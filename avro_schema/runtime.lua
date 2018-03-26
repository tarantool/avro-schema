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

-- pipeline -----------------------------------------------------------
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

struct schema_rt_State {
    size_t                    t_capacity;
    size_t                    ot_capacity;
    size_t                    res_capacity;
    size_t                    res_size;
    uint8_t                  *res;
    const uint8_t            *b1;
    union {
        const uint8_t        *b2;
        const uint16_t       *b2_16;
        const uint32_t       *b2_32;
    };
    uint8_t                  *t;
    struct schema_rt_Value   *v;
    uint8_t                  *ot;
    struct schema_rt_Value   *ov;
    int32_t                   k;
};

int
parse_msgpack(struct schema_rt_State *state,
              const uint8_t          *msgpack_in,
              size_t                  msgpack_size);

int
unparse_msgpack(struct schema_rt_State *state,
                size_t                  nitems);

int
schema_rt_buf_grow(struct schema_rt_State *state,
                   size_t                  min_capacity);

int schema_rt_extract_location(struct schema_rt_State *state,
                               intptr_t                pos);

void schema_rt_xflatten_done(struct schema_rt_State *state,
                             size_t len);

]]

-- hash ---------------------------------------------------------------
ffi.cdef[[
int32_t
create_hash_func(int n, const char *strings[],
                 const char *random, size_t size_random);

int32_t
eval_hash_func(int32_t func, const unsigned char *str, size_t len);

int32_t
eval_fnv1a_func(int32_t seed, const unsigned char *str, size_t len);
]]

-- misc ---------------------------------------------------------------
ffi.cdef[[
int
schema_rt_key_eq(const char *key, const char *str, size_t klen, size_t len);

int32_t
schema_rt_search8(const void *tab, int32_t k, size_t n);

int32_t
schema_rt_search16(const void *tab, int32_t k, size_t n);

int32_t
schema_rt_search32(const void *tab, int32_t k, size_t n);
]]

-- phf ----------------------------------------------------------------
ffi.cdef[[
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
]]

local rt_C_path   = package.search('avro_schema_rt_c') or
                    package.searchpath('avro_schema_rt_c', package.cpath) or
                    error('Failed to load avro_schema_rt_c.so, check LUA_CPATH.')
local rt_C        = ffi.load(rt_C_path)
local regs        = ffi.new('struct schema_rt_State')

local function buf_grow(r, min_capacity)
    if rt_C.schema_rt_buf_grow(r, min_capacity) ~= 0 then
        error('Out of memory', 0)
    end
end

-- Buf has space for at least 128 items.
buf_grow(regs, 128)

local function msgpack_decode(r, s)
    if rt_C.parse_msgpack(r, s, #s) ~= 0 then
        error(ffi.string(r.res, r.res_size), 0)
    end
    return tonumber(r.res_size)
end

local function msgpack_encode(r, n)
    if rt_C.unparse_msgpack(r, n) ~= 0 then
        error(ffi.string(r.res, r.res_size), 0)
    end
    return ffi_string(r.res, r.res_size)
end

local function universal_decode(r, s)
    local r = regs
    if type(s) ~= 'string' then
        s = msgpacklib_encode(s)
    end
    if rt_C.parse_msgpack(r, s, #s) ~= 0 then
        error(ffi.string(r.res, r.res_size), 0)
    end
    return tonumber(r.res_size)
end

local function lua_encode(r, n)
    if rt_C.unparse_msgpack(r, n) ~= 0 then
        error(ffi.string(r.res, r.res_size), 0)
    end
    return msgpacklib_decode(ffi_string(r.res, r.res_size))
end

--
-- vis_msgpack
--

local typenames = {
    [1] = 'NIL', [2] = 'FALSE', [3] = 'TRUE', [4] = 'LONG', [5] = 'ULONG',
    [6] = 'FLOAT', [7] = 'DOUBLE', [8] = 'STR', [9] = 'BIN',
    [10] = 'EXT', [11] = 'ARRAY', [12] = 'MAP'
}

local vis_value_funcs = {
    [4] = function(r, i)
        return format(' %4s', r.v[i].ival)
    end,
    [5] = function(r, i)
        return format(' %4s', r.v[i].uval)
    end,
    [6] = function(r, i)
        return format(' %4s', r.v[i].dval)
    end,
    [7] = function(r, i)
        return format(' %4s', r.v[i].dval)
    end,
    [8] = function(r, i)
        local v = r.v[i]
        return format(' %q', ffi_string(r.b1-v.xoff, v.xlen))
    end,
    [11] = function(r, i)
        local v = r.v[i]
        return format(
            ' %4s ->%05d', v.xlen, i+v.xoff), v.xlen
    end,
    [12] = function(r, i)
        local v = r.v[i]
        return format(
            ' %4s ->%05d', v.xlen, v.xoff), 2*v.len
    end
}

local function vis_value(r, i)
    local t = r.t[i]
    local tname = typenames[t]
    local func = vis_value_funcs[t]
    if func then return tname, func(r, i) end
    return (tname or '????'), ''
end

local function vis_msgpack(input)

    local n = universal_decode(regs, input)
    local typeid = regs.t
    local value  = regs.v

    local output = {}
    local todos = {}
    local todo = 1
    for i = 0, n - 1 do
        local a, b, len = vis_value(regs, i)
        insert(output, format('%05d%s %-06s%s',
                              i, string.rep('....', #todos), a, b))
        todo = todo - 1
        if len then
            insert(todos, todo)
            todo = len
        end
        while todo == 0 and todos[1] do
            todo = remove(todos)
        end
    end
    insert(output, format('%05d', n))
    return concat(output, '\n')
end

--
-- err_*
--

local extract_location
extract_location = function(r, pos)
    local key_error = rt_C.schema_rt_extract_location(r, pos)
    return ffi.string(r.res, r.res_size), key_error ~= 0
end

local etype2typename = {
    [0xec] = 'BOOL', [0xed] = 'INT', [0xee] = 'FLOAT', [0xef] = 'DOUBLE',
    [0xf0] = 'LONG', [0xf1] = 'STR', [0xf2] = 'BIN',   [0xf3] = 'ARRAY',
    [0xf4] = 'MAP',  [0xf5] = 'NIL', [0xf6] = 'NIL or MAP'
}

local function err_type(r, pos, etype)
    -- T==4(LONG) and (etype==0xee(ISFLOAT) or etype==0xef(ISDOUBLE))
    -- due to T range (1..12) and etype-s coding (236 + (0..9))
    -- this check is robust
    if r.t[pos] * band(etype, 0xfe) == 0x3b8 then
        r.t[pos] = 7 + band(etype, 1) -- long 2 float / double
        r.v[pos].dval = r.v[pos].ival
        return
    end
    local location, iskerror = extract_location(r, pos)
    if iskerror then
        error(format('%sNon-string key', location), 0)
    elseif etype == 0xed and r.t[pos] == 4 then
        error(format('%sValue exceeds INT range: %s',
                     location, r.v[pos].ival), 0)
    else
        error(format('%sExpecting %s, encountered %s',
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
    error(format('%sKey missing: %q', location, missing_name), 0)
end

local function err_duplicate(r, pos)
    error(format('%sDuplicate Key', extract_location(r, pos)), 0)
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
    local t = r.t[pos]
    local val
    if t == 4 then
        val = tonumber(r.v[pos].ival)
        val = val == r.v[pos].ival and val or r.v[pos].ival
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
    buf_grow         = buf_grow,
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
