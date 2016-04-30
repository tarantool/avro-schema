local digest = require('digest')
local front  = require('avro_schema_front')
local c      = require('avro_schema_c')
local lir    = require('avro_schema_lir')
local rt     = require('avro_schema_rt')

local format, find, gsub = string.format, string.find, string.gsub
local insert, remove, concat = table.insert, table.remove, table.concat

local base64_encode       = digest.base64_encode
local f_create_schema     = front.create_schema
local f_validate_data     = front.validate_data
local f_create_ir         = front.create_ir
local c_emit_code         = c.emit_code
local lir_lua_lir         = lir.lua_lir
local lir_lua_lir_to_code = lir.lua_lir_to_code
local rt_msgpack_encode   = rt.msgpack_encode
local rt_msgpack_decode   = rt.msgpack_decode
local rt_lua_encode       = rt.lua_encode
local rt_universal_decode = rt.universal_decode

-- We give away a handle but we never expose schema data.
local schema_by_handle = setmetatable( {}, { __mode = 'k' } )

local function get_schema(handle)
    local schema = schema_by_handle[handle]
    if not schema then
        error(format('Not a schema: %s', handle), 0)
    end
    return schema
end

local function is_schema(schema_handle)
    return not not schema_by_handle[schema_handle]
end

-- IR-s are cached
local ir_by_key        = setmetatable( {}, { __mode = 'v' } )

local function get_ir(from_schema, to_schema, inverse)
    k = format('%s%p.%p', inverse and '-' or '', from_schema, to_schema)
    ir = ir_by_key[k]
    if ir then
        if type(ir) == 'table' and ir[1] == 'ERR' then
            return false, ir[2]
        else
            return true, ir
        end
    else
        local err
        ir, err = f_create_ir(from_schema, to_schema, inverse)
        if not ir then
            ir_by_key[k] = { 'ERR', err }
            return false, err
        else
            ir_by_key[k] = ir
            return true, ir
        end
    end
end

local function schema_to_string(handle)
    local schema = schema_by_handle[handle]
    return format('Schema (%s)',
                  handle[1] or (type(schema) ~= 'table' and schema) or
                  schema.name or schema.type or 'union')
end

local schema_handle_mt = {
    __tostring  = schema_to_string,
    __serialize = schema_to_string
}

local function create(raw_schema)
    local ok, schema = pcall(f_create_schema, raw_schema)
    if not ok then
        return false, schema
    end
    local schema_handle = setmetatable({}, schema_handle_mt)
    schema_by_handle[schema_handle] = schema
    return true, schema_handle
end

local function validate(schema_handle, data)
    return f_validate_data(get_schema(schema_handle), data)
end

local function are_compatible(schema_h1, schema_h2, opt_mode)
    local ok, extra = get_ir(get_schema(schema_h1), get_schema(schema_h2),
                             opt_mode == 'downgrade')
    if ok then
        return true -- never leak IR
    else
        return false, extra
    end
end

local function gen_lua_code(ir)
    local lir = lir_lua_lir()
    local code = lir_lua_lir_to_code(lir, c_emit_code(lir, ir))
    local cpool = lir.cpool_data()
    return concat({[[
local ffi        = require('ffi')
local digest     = require('digest')
local bit        = require('bit')
local schema_rt  = require('avro_schema_rt')
local type       = type
local xpcall     = xpcall
local ffi_C      = ffi.C
local ffi_cast   = ffi.cast
local regs       = schema_rt.regs
local eh_handler = schema_rt.eh_handler
local linker
local cpool     = digest.base64_decode([[]],
            base64_encode(cpool),
        ']])',
        code, [[
linker = function(decode_proc, encode_proc)
    decode_proc = decode_proc or schema_rt.msgpack_decode
    encode_proc = encode_proc or schema_rt.msgpack_encode
    local function flatten(data)
        local _ = linker
        local r = regs
        decode_proc(r, data)
        r.b2 = ffi_cast("const uint8_t *", cpool) + #cpool
        return encode_proc(r, f001(r, 0, 0))
    end
    local function unflatten(data)
        local _ = linker
        local r = regs
        decode_proc(r, data)
        r.b2 = ffi_cast("const uint8_t *", cpool) + #cpool
        return encode_proc(r, f002(r, 0, 0))
    end
    local function xflatten(data)
        local _ = linker
        local r = regs
        decode_proc(r, data)
        r.b2 = ffi_cast("const uint8_t *", cpool) + #cpool
        return encode_proc(r, f003(r, 0, 0))
    end
    return {
        flatten  = function(data)
            return xpcall(flatten, eh_handler, data)
        end,
        unflatten  = function(data, path)
            return xpcall(unflatten, eh_handler, data, path)
        end,
        xflatten  = function(data, path)
            return xpcall(xflatten, eh_handler, data, path)
        end
    }
end
return linker
]]
    }, '\n')
end

-- compile(schema)
-- compile(schema1, schema2)
-- compile({schema1, schema2, downgrade = true, extra_fields = { ... }})
-- --> { deflate = , inflate = , xdeflate = , convert_deflated = , convert_inflated = }
local function compile(...)
    local n = select('#', ...)
    local args = { ... }
    local ok, ir
    if n == 1 and not is_schema(args[1]) then
        if type(args[1]) ~= 'table' then
            error('Expecting a schema or a table', 0)
        end
        n = select('#', unpack(args[1]))
        args = args[1]
    end
    local list = {}
    for i = 1, n do
        insert(list, get_schema(args[i]))
    end
    if #list == 0 then
        error('Expecting a schema', 0)
    elseif #list == 1 then
        ok, ir = get_ir(list[1], list[1])
    elseif #list == 2 then
        ok, ir = get_ir(list[1], list[2], args.downgrade)
    else
        assert(false, 'NYI: chain')
    end
    if not ok then
        return false, ir
    else
        local dump_src = args.dump_src
        local code = gen_lua_code(ir)
        if dump_src then
            local file = io.open(dump_src, 'w+')
            file:write(code)
            file:close()
        end
        local linker  = loadstring(code, '@<schema-jit>') ()
        local msgpack = linker(rt_universal_decode, rt_msgpack_encode)
        local lua     = linker(rt_universal_decode, rt_lua_encode)
        return true, {
            flatten           = lua.flatten,
            unflatten         = lua.unflatten,
            xflatten          = lua.xflatten,
            flatten_msgpack   = msgpack.flatten,
            unflatten_msgpack = msgpack.unflatten,
            xflatten_msgpack  = msgpack.xflatten
        }
    end
end

local get_names_helper
get_names_helper = function(res, pos, names, rec)
    local fields = rec.fields
    for i = 1, #fields do
        local ftype = fields[i].type
        insert(names, fields[i].name)
        if type(ftype) == 'string' then
            res[pos] = concat(names, '.')
            pos = pos + 1
        elseif ftype.type == 'record' then
            pos = get_names_helper(res, pos, names, ftype)
        elseif not ftype.type then -- union
            local path = concat(names, '.')
            res[pos] = path .. '.$type$'
            res[pos + 1] = path
            pos = pos + 2
        else
            res[pos] = concat(names, '.')
            pos = pos + 1
        end
        remove(names)
    end
    return pos
end

local function get_names(schema_h)
    local schema = get_schema(schema_h)
    if type(schema) == 'table' and schema.type == 'record' then
        local res = {}
        local names = {}
        get_names_helper(res, 1, names, schema)
        return res
    else
        return {}
    end
end

local get_types_helper
get_types_helper = function(res, pos, rec)
    local fields = rec.fields
    for i = 1, #fields do
        local ftype = fields[i].type
        if type(ftype) == 'string' then
            res[pos] = ftype
            pos = pos + 1
        elseif ftype.type == 'record' then
            pos = get_types_helper(res, pos, ftype)
        elseif not ftype.type then -- union
            pos = pos + 2
        else
            res[pos] = ftype.name or ftype.type
            pos = pos + 1
        end
    end
    return pos
end

local function get_types(schema_h)
    local schema = get_schema(schema_h)
    if type(schema) == 'table' and schema.type == 'record' then
        local res = {}
        get_types_helper(res, 1, schema)
        return res
    else
        return {}
    end
end

return {
    are_compatible = are_compatible,
    create         = create,
    compile        = compile,
    get_names      = get_names,
    get_types      = get_types,
    is             = is_schema,
    validate       = validate
}
