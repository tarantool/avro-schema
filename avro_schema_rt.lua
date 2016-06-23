local ffi        = require('ffi')
local bit        = require('bit')
local msgpacklib = require('msgpack')

local find, format = string.find, string.format
local byte, sub = string.byte, string.sub
local concat, insert = table.concat, table.insert
local remove = table.remove
local bor, band = bit.bor, bit.band

local ffi_cast = ffi.cast
local ffi_string = ffi.string
local msgpacklib_encode = msgpacklib and msgpacklib.encode
local msgpacklib_decode = msgpacklib and msgpacklib.decode

ffi.cdef[[

struct schema_rt_Value {
    union {
        void          *p;
        int64_t        ival;
        uint64_t       uval;
        double         dval;
        struct {
            uint32_t   xlen;
            uint32_t   xoff;
        };
    };
};

struct schema_rt_Regs {
    ssize_t                   rc;
    union {
        uint8_t              *t;
        uint8_t              *t_[1];
    };
    union {
        struct schema_rt_Value
                             *v;
        struct schema_rt_Value
                             *v_[1];
    };
    const uint8_t            *b1;
    union {
        const uint8_t        *b2;
        const uint16_t       *b2_16;
        const uint32_t       *b2_32;
    };
    uint8_t                  *ot;
    struct schema_rt_Value
                             *ov;
};

ssize_t
parse_msgpack(const uint8_t *msgpack_in,
              size_t         msgpack_size,
              size_t         stock_buf_size_or_hint,
              uint8_t       *stock_typeid_buf,
              struct schema_rt_Value
                            *stock_value_buf,
              uint8_t      **typeid_out,
              struct schema_rt_Value
                           **value_out);

ssize_t
unparse_msgpack(size_t            nitems,
                const uint8_t    *typeid,
                const struct schema_rt_Value
                                 *value,
                const uint8_t    *bank1,
                const uint8_t    *bank2,
                size_t            stock_buf_size_or_hint,
                uint8_t          *stock_buf,
                uint8_t         **msgpack_out);

int32_t
create_hash_func(int n, const char *strings[],
                 const char *random, size_t size_random);

int32_t
eval_hash_func(int32_t func, const unsigned char *str, size_t len);

int32_t
eval_fnv1a_func(int32_t seed, const unsigned char *str, size_t len);

int
schema_rt_key_eq(const char *key, const char *str, size_t klen, size_t len);

int32_t
schema_rt_search8(const void *tab, int32_t k, size_t n);

int32_t
schema_rt_search16(const void *tab, int32_t k, size_t n);

int32_t
schema_rt_search32(const void *tab, int32_t k, size_t n);

/* phf ***************************************************************/

struct schema_rt_phf {
    bool                      nodiv;
    int32_t                   seed;
    size_t                    r;
    size_t                    m;
    void                     *g;
    size_t                    d_max;
    int                       g_op;
    intptr_t                  reserved;
};

int
phf_init_uint32(struct schema_rt_phf *phf,
                const int32_t *k,
                size_t n,
                size_t lambda,/* displacement table compaction factor (try 4) */
                size_t alpha, /* hash table load factor (in percent, try 80%) */
                int32_t seed,
                bool nodiv);

void
phf_compact(struct schema_rt_phf *phf);

int32_t
phf_hash_uint32(struct schema_rt_phf *phf, int32_t k);

void
phf_destroy(struct schema_rt_phf *phf);

int32_t
phf_hash_uint32_band_raw8(const void *g, int32_t k, int32_t seed, size_t r, size_t m);

int32_t
phf_hash_uint32_band_raw16(const void *g, int32_t k, int32_t seed, size_t r, size_t m);

int32_t
phf_hash_uint32_band_raw32(const void *g, int32_t k, int32_t seed, size_t r, size_t m);

/* libc **************************************************************/

void *malloc(size_t);
void  free(void *);
int   memcmp(const void *, const void *, size_t);

]]

local null        = ffi_cast('void *', 0)
local rt_C_path   = package.searchpath('avro_schema_rt_c',
                                     package.cpath)
local rt_C        = ffi.load(rt_C_path)

--
-- vis_msgpack
--

local function esc(s)
    if find(s, '[A-Za-z0-9_]') then
        return s
    else
        return format('\\%0d', string.byte(s))
    end
end

local typenames = {
    [1] = 'NIL', [2] = 'FALSE', [3] = 'TRUE', [4] = 'LONG', [5] = 'ULONG',
    [6] = 'FLOAT', [7] = 'DOUBLE', [8] = 'STR', [9] = 'BIN',
    [10] = 'EXT', [11] = 'ARRAY', [12] = 'MAP'
}

local valuevis = {
    [4] = function(i, val)
        return nil, format(' %4s', val.ival)
    end,
    [5] = function(i, val)
        return nil, format(' %4s', val.uval)
    end,
    [6] = function(i,val)
        return nil, format(' %4s', val.dval)
    end,
    [7] = function(i, val)
        return nil, format(' %4s', val.dval)
    end,
    [8] = function(i, val, bank)
        local sample
        if type(bank) == 'string' then
            sample = {}
            local i = #bank - val.xoff + 1
            while #sample < val.xlen do
                if #sample == 10 then
                    sample[8], sample[9], sample[10] = '.', '.', '.'
                    break
                end
                insert(sample, esc(sub(bank, i, i)))
                i = i + 1
            end
            sample = concat(sample)
        else
            sample = format('-%d', val.xoff)
        end
        return nil, format(' %4s %s', val.xlen, sample or '')
    end,
    [11] = function(i, val)
        return val.xlen, format(
            ' %4s ->%05d', val.xlen, i+val.xoff)
    end,
    [12] = function(i, val)
        return val.xlen * 2, format(
            ' %4s ->%05d', val.xlen, i+val.xoff)
    end
}

local function vis_msgpack(input)
    local typeid_out = ffi.new('uint8_t *[1]');
    local value_out  = ffi.new('struct schema_rt_Value *[1]')
    
    local rc = rt_C.parse_msgpack(
        input, #input, 0, null, null, typeid_out, value_out)

    if rc < 0 then
        error('schema_rt_C.parse_msgpack: -1')
    end

    local st, res = pcall(function()

        local typeid = typeid_out[0]
        local value  = value_out[0]

        local output = {}
        local todos = {}
        local todo = 1
        for i = 0, tonumber(rc) - 1 do
            local indent
            local xid, xval = typeid[i], value[i]
            local vis = valuevis[xid]
            local len, info
            if vis then
                len, info = vis(i, xval, input)
            end
            local line = format(
                '%05d%s %-06s%s', i, string.rep('....', #todos),
                typenames[xid] or '???', info or '')
            insert(output, line)
            todo = todo - 1
            if len then
                insert(todos, todo)
                todo = len
            end
            while todo == 0 and todos[1] do
                todo = remove(todos)
            end
        end

        insert(output, format('%05d', tonumber(rc)))
        return concat(output, '\n')

    end)

    ffi.C.free(typeid_out[0])
    ffi.C.free(value_out[0])

    if not st then
        error(res)
    end
    return res
end

local regs = ffi.new('struct schema_rt_Regs')
regs.t  = ffi.C.malloc(4096)
regs.v  = ffi.C.malloc(4096*8)
regs.ot = ffi.C.malloc(512)
regs.ov = ffi.C.malloc(512*8)

local function msgpack_decode(r, s)
    local r = regs
    if rt_C.parse_msgpack(s, #s, 4096, r.t, r.v, r.t_, r.v_) < 0 then
        error('Malformed msgpack data', 0)
    end
    r.b1 = ffi_cast("const uint8_t *", s) + #s
end

local function msgpack_encode(r, n)
    r.rc = rt_C.unparse_msgpack(n, r.ot, r.ov, r.b1, r.b2, 4096, r.t, r.t_)
    if r.rc < 0 then
        error('Internal error', 0)
    end
    return ffi_string(r.t, r.rc)
end

local function universal_decode(r, s)
    local r = regs
    if type(s) ~= 'string' then
        s = msgpacklib_encode(s)
    end
    if rt_C.parse_msgpack(s, #s, 4096, r.t, r.v, r.t_, r.v_) < 0 then
        error('Malformed msgpack data', 0)
    end
    r.b1 = ffi_cast("const uint8_t *", s) + #s
end

local function lua_encode(r, n)
    r.rc = rt_C.unparse_msgpack(n, r.ot, r.ov, r.b1, r.b2, 4096, r.t, r.t_)
    if r.rc < 0 then
        error('Internal error', 0)
    end
    return msgpacklib_decode(ffi_string(r.t, r.rc))
end

local extract_location
extract_location = function(r, pos)
    if pos == 0 then
        return '', false
    end
    local res = {}
    local iter = 1
    local counter = 1
    local ismap = r.t[0] == 12
    while true do
        local t = r.t[iter]
        local xoff = t < 11 and 1 or r.v[iter].xoff
        if iter + xoff > pos then
            if not ismap then
                insert(res, counter)
            elseif counter % 2 == 0 and r.t[iter-1] == 8 then
                insert(res, ffi_string(r.b1 - r.v[iter-1].xoff, r.v[iter-1].xlen))
            else
                return res[1] and format('%s: ', concat(res, '/')) or
                       '', true -- bad key
            end
            if iter == pos then
                return res[1] and format('%s: ', concat(res, '/')) or
                       '', false
            end
            iter = iter + 1
            counter = 1
            ismap = t == 12
        else
            iter = iter + xoff
            counter = counter + 1
        end
    end
end

local etype2typename = {
    [0xea] = 'BOOL', [0xeb] = 'INT', [0xec] = 'FLOAT', [0xed] = 'DOUBLE',
    [0xee] = 'LONG', [0xef] = 'STR', [0xf0] = 'BIN',   [0xf1] = 'ARRAY',
    [0xf2] = 'MAP',  [0xf3] = 'NIL'
}

local function err_type(r, pos, etype)
    -- T==4(LONG) and (etype==0xec(ISFLOAT) or etype==0xed(ISDOUBLE))
    -- due to T range (1..12) and etype-s coding (234 + (0..9))
    -- this check is robust
    if r.t[pos] * band(etype, 0xfe) == 0x3b0 then
        r.t[pos] = 7 + band(etype, 1) -- long 2 float / double
        r.v[pos].dval = r.v[pos].ival
        return
    end
    local location, iskerror = extract_location(r, pos)
    if iskerror then
        error(format('%sNon-string key', location), 0)
    elseif etype == 0xeb and r.t[pos] == 4 then
        error(format('%sValue exceeds INT range: %s',
                     location, r.v[pos].ival), 0)
    else
        error(format('%sExpected %s, encountered %s',
                     location, etype2typename[etype],
                     typenames[r.t[pos]]), 0)
    end
end

local function err_length(r, pos, elength)
    local location = extract_location(r, pos)
    local t = typenames[r.t[pos]]
    error(format('%sExpecting %s of length %d. Encountered %s of length %d.',
                 location, t, elength, t, r.v[pos].xlen), 0)
end

local function err_missing(r, pos, missing_name)
    local location = extract_location(r, pos)
    error(format('%sKey missing: %s', location, missing_name), 0)
end

local function err_duplicate(r, pos)
    error('duplicate', 0)
end

-- err_value() is used to report:
--   - unknown key in a JSON record;
--   - bad ENUM value (both integer and string forms);
--   - bad UNION discriminator.
-- ver_error == true iff the value is correct according to the source
-- schema, but no conversion into destination schema exist.
local function err_value(r, pos, ver_error)
    local tag = ver_error and ' (schema versioning)' or ''
    local location, iskerror = extract_location(r, pos)
    if iskerror and r.t[pos] == 8 then
        error(format('%sUnknown key: %q%s',
                     location,
                     ffi_string(r.b1-r.v[pos].xoff, r.v[pos].xlen), tag), 0)
    end
    local val = 'TBD'
    local t = r.t[pos]
    if t == 4 then
        val = tonumber(r.v[pos].ival)
        val = val == r.v[pos].ival and val or r.v[pos].ival
    elseif t == 6 or t == 7 then
        val = r.v[pos].dval
    elseif t == 8 then
        val = format('%q', ffi_string(r.b1 - r.v[pos].xoff, r.v[pos].xlen))
    end
    error(format('%sBad value: %s%s', location, val, tag), 0)
end

return {
    -- don't expose C library (unsafe),
    -- but let module user to load it herself (if she can)
    C_path           = rt_C_path,

    vis_msgpack      = vis_msgpack,
    regs             = regs,
    msgpack_encode   = msgpack_encode,
    msgpack_decode   = msgpack_decode,
    lua_encode       = lua_encode,
    universal_decode = universal_decode,
    err_type         = err_type,
    err_length       = err_length,
    err_missing      = err_missing,
    err_duplicate    = err_duplicate,
    err_value        = err_value
}
