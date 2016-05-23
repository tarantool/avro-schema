local ffi        = require('ffi')
local msgpacklib = require('msgpack')

local find, format = string.find, string.format
local byte, sub = string.byte, string.sub
local concat, insert = table.concat, table.insert
local remove = table.remove

local ffi_cast = ffi.cast
local msgpacklib_encode = msgpacklib and msgpacklib.encode
local msgpacklib_decode = msgpacklib and msgpacklib.decode

ffi.cdef[[

struct schema_rt_Value {
    union {
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
    const uint8_t            *b2;
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

void *malloc(size_t);
void  free(void *);
int   memcmp(const void *, const void *, size_t);

]]

local null        = ffi_cast('void *', 0)
local schema_rt_C = ffi.load(package.searchpath('avro_schema_rt_c',
                                                package.cpath))

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
    
    local rc = schema_rt_C.parse_msgpack(
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
    if schema_rt_C.parse_msgpack(s, #s, 4096, r.t, r.v, r.t_, r.v_) < 0 then
        error('Malformed msgpack data', 0)
    end
    r.b1 = ffi_cast("const uint8_t *", s) + #s
end

local function msgpack_encode(r, n)
    r.rc = schema_rt_C.unparse_msgpack(n, r.ot, r.ov, r.b1, r.b2, 4096, r.t, r.t_)
    if r.rc < 0 then
        error('Internal error', 0)
    end
    return ffi.string(r.t, r.rc)
end

local function universal_decode(r, s)
    local r = regs
    if type(s) ~= 'string' then
        s = msgpacklib_encode(s)
    end
    if schema_rt_C.parse_msgpack(s, #s, 4096, r.t, r.v, r.t_, r.v_) < 0 then
        error('Malformed msgpack data', 0)
    end
    r.b1 = ffi_cast("const uint8_t *", s) + #s
end

local function lua_encode(r, n)
    r.rc = schema_rt_C.unparse_msgpack(n, r.ot, r.ov, r.b1, r.b2, 4096, r.t, r.t_)
    if r.rc < 0 then
        error('Internal error', 0)
    end
    return msgpacklib_decode(ffi.string(r.t, r.rc))
end

local extract_path
extract_path = function(r, pos)
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
                insert(res, ffi.string(r.b1 - r.v[iter-1].xoff, r.v[iter-1].xlen))
            else
                return concat(res, '/'), true -- bad key
            end
            if iter == pos then
                return concat(res, '/'), false
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

local function err_type(r, pos, etype)
    local path, iskerror = extract_path(r, pos)
    if iskerror then
        error(format('/%s: Non-string key', path), 0)
    else
        error(format('/%s: Expected %d, encountered %d',
                     path, etype, r.t[pos]), 0)
    end
end

local function err_length(r, pos, elength)
    local path = extract_path(r, pos)
    local t = r.t[pos]
    error(format('/%s: Expecting a %d of length %d. Encountered %s of length %d.',
                 path, t, elength, t, r.v[pos].xlen), 0)
end

local function err_missing(r, pos, missing_name)
    local path = extract_path(r, pos)
    error(format('/%s: Key missing: %s', path, missing_name), 0)
end

local function err_duplicate(r, pos)
    error('duplicate', 0)
end

local function err_value(r, pos)
    local path, iskerror = extract_path(r, pos)
    if iskerror and r.t[pos] == 8 then
        error(format('/%s: Unknown Key: %s',
                     path,
                     ffi.string(r.b1-r.v[pos].xoff, r.v[pos].xlen)), 0)
    end
    error('value', 0)
end

return {
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
