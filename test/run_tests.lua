package.path  = package.path ..';avro/?.lua'
package.cpath = package.cpath..';avro/?.so'

local math           = require('math')
local io             = require('io')
local digest         = require('digest')
local debug          = require('debug')
local json           = require('json')
local fio            = require('fio')
local schema         = require('avro_schema')
local max            = math.max
local base64_encode  = digest.base64_encode
local format, gsub   = string.format, string.gsub
local insert, concat = table.insert, table.concat
local sort           = table.sort

-- order-preserving JSON<->msgpack conversion, via external tool
local function msgpack_helper(data, opts)
    if data=='' then error('Data empty') end
    local cmd = format("./msgpack_helper.py %s '%s'", opts or '',
                       base64_encode(data))
    local handle = io.popen(cmd, 'r')
    local res = handle:read('*a')
    handle:close()
    return res
end

local cache = setmetatable({}, {__mode='v'})
local function memoize(key, func, ...)
    local ok, res = nil, cache[key]
    if res then return true, res end
    ok, res = func(...)
    if ok then cache[key] = res end
    return ok, res
end

local function create_from_json(data)
    return schema.create(json.decode(data))
end

--  schema / schema1 / schema2 (JSON)
--  create_error   - if create failed, error message
--  create_only
local function create_stage(test, args)
    local s = {}
    test.schema = s
    insert(s, args.schema)
    insert(s, args.schema1)
    insert(s, args.schema2)
    if #s == 0 then
        test.FAILED = 'schema/schema1/schema2 missing'
        return
    end
    test.schema_key = concat(s, ';')
    for i = 1,#s do
        local ok, schema      = memoize(s[i], create_from_json, s[i])
        local status          = ok and '<OK>' or schema  
        local expected_status = args.create_error or '<OK>'  
        if status ~= expected_status then
            test.FAILED = format('schema.create: %q instead of %q',
                                 status, expected_status)
            return
        end
        s[i] = schema
    end
    if args.create_only or args.create_error then test.PASSED = true end
end

--  service_fields - service fields in compile 
--  downgrade      - downgrade flag
--  compile_error  - if compile failed, error message
--  compile_only
local function compile_stage(test, args)
    local service_fields = args.service_fields or {}
    local downgrade      = args.downgrade or false
    local compile_error  = args.compile_error

    local key = format('%s;%s;%s',
                       downgrade, concat(service_fields, ';'),
                       test.schema_key)
    local compile_opts          = test.schema
    compile_opts.service_fields = service_fields
    compile_opts.downgrade      = downgrade
    local ok, schema_c          = memoize(key, schema.compile, compile_opts)
    local status          = ok and '<OK>' or schema_c
    local expected_status = args.compile_error or '<OK>'  
    if status ~= expected_status then
        test.FAILED = format('schema.compile: %q instead of %q',
                             status, expected_status)
        return
    end
    if args.compile_only then test.PASSED = true end
    test.schema_c = schema_c
end

local function res_wrap(ok, ...) return ok, {...} end
local function esc(v) return type(v)=='string' and format('%q', v) or v end

--  func:  flatten/unflatten/xflatten
--  input
--  output
--  error
local function convert_stage(test, args)
    local func   = args.func
    local input  = args.input
    local output = args.output
    if not func or not input or not (output or args.error) then
        test.FAILED = 'func/input/output/error missing'
        return
    end
    local call_func = test.schema_c[func .. '_msgpack']
    if not call_func then
        test.FAILED = 'unknown function '..func
    end
    if type(input) ~= 'table' then
        input = { input }
    end
    local input_1 = input[1]
    input[1] = msgpack_helper(input_1) -- JSON->msgpack
    local ok, result = res_wrap(call_func(unpack(input)))
    local status          = ok and '<OK>' or result[1]
    local expected_status = args.error or '<OK>'  
    if status ~= expected_status then
        test.FAILED = format('%s: %q instead of %q',
                             func, status, expected_status)
        return
    end
    if ok then
        if type(output) ~= 'table' then output = { output } end
        local n = max(#result, #output)
        for i = 1,n do
            local result_i = result[i]
            local output_i = output[i]
            if i == 1 then
                output_i = msgpack_helper(output_i)
            end
            if result_i ~= output_i then
                if i == 1 then
                    result_i = msgpack_helper(result_i, '-D')
                    output_i = msgpack_helper(output_i, '-D')
                else
                    result_i = esc(result_i)
                    output_i = esc(output_i)
                end
                test.FAILED = format('%s, result[%d]: %s instead of %s',
                                     func, i, result_i, output_i)
                return
            end
        end
    end
    test.PASSED = true
end

local stages = {
    create_stage,
    compile_stage,
    convert_stage,
    -- the last stage always fails
    function(test) test.FAILED = 'auto-failer' end
}

-- test-id is <file-name>-<line>
local test_name, test_env
local test_env_ignore = {_G = true, t = true, skip_t = true}
local function test_id(caller)
    local keys = {}
    for k in pairs(test_env) do
        if not test_env_ignore[k] then insert(keys, k) end
    end
    sort(keys)
    local res = { test_name }
    for _, k in pairs(keys) do
        insert(res, format('%s_%s', k, test_env[k]))
    end
    insert(res, caller.currentline)
    return format('%32s', concat(res, '/'))
end

local function t(args)
    local id = test_id(debug.getinfo(2, 'lS'))
    local test = { id = test_id }
    for i = 1, #stages do
        stages[i](test, args)
        if test.PASSED then
            print(format('%s: PASSED', id))
            return
        end
        if test.FAILED then
            print(format('%s: FAILED (%s)', id, test.FAILED))
            return
        end
    end
end

for _, path in pairs(fio.glob('suite/*.lua')) do
    local result, extra = loadfile(path)
    if not result then error(extra) end
    local test = result
    test_env = { t = t, skip_t = function() end}
    test_env._G = test_env
    setfenv(test, test_env)
    test_name = gsub(gsub(path, '.*/', ''), '%.lua$', '')
    test()
end
