local schema =  require('avro_schema')
local tap    =  require('tap')
local json   =  require('json')
local msgpack = require('msgpack')

local test = tap.test('api-tests')

test:plan(17)

-- Schema evolution: extend a schema with a record field of type
-- union or record with a default value.

local default_record_1 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "bar", "type": "string" }
    ]
}
]])
local default_record_2 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record", "name": "default_record", "fields":[
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "int"} ]},
                 "default": { "f1": 1, "f2": 2}},
        { "name": "foo_nullable", "type": "default_record",
                 "default": { "f1": 1, "f2": 2}},
        { "name": "bar", "type": "string" }
    ]
}
]])
local ok, handle_1 = schema.create(default_record_1)
local ok, handle_2 = schema.create(default_record_2)
local ok, compiled = schema.compile({handle_1, handle_2})
assert(ok, compiled)
local ok, data = compiled.unflatten({"asd"})
test:is_deeply(data, {foo={f1=1,f2=2}, foo_nullable={f1=1,f2=2},bar="asd"},
    'evolution unflatten record')

local default_union_1 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "bar", "type": "string" }
    ]
}
]])

local default_union_2 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record*", "name": "default_record", "fields":[
                {"name": "f1", "type": ["int", "null"]},
                {"name": "f2", "type": ["null", "int"]} ]},
                 "default": { "f1": {"int": 1}}},
        { "name": "bar", "type": "string" }
    ]
}
]])

local ok, handle_1 = schema.create(default_union_1)
local ok, handle_2 = schema.create(default_union_2)
local ok, compiled = schema.compile({handle_1, handle_2})
local ok, data = compiled.unflatten({"asd"})
test:is_deeply(data,
    json.decode([[{"foo":{"f2":null,"f1":{"int":1}},"bar":"asd"}]]),
    'evolution: add default union && unflatten')

-- Add nullable record.

local evolution_1 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "bar", "type": "string" }
    ]
}
]])
local evolution_2 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record*", "name": "default_record", "fields":[
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "int"} ]},
                 "default": { "f1": 1, "f2": 2}},
        { "name": "bar", "type": "string" }
    ]
}
]])
local ok, handle_1 = schema.create(evolution_1)
local ok, handle_2 = schema.create(evolution_2)
local ok, compiled = schema.compile({handle_1, handle_2})
local ok, data = compiled.flatten({bar="asd"})
test:is_deeply(data, {{1,2},"asd"},
    'evolution: add nullable record && flatten')
local ok, data = compiled.unflatten({"asd"})
test:is_deeply(data, {foo={f1=1, f2=2}, bar="asd"},
    'evolution: add nullable record && unflatten')

-- Record become nullable.

local evolution_1 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record", "name": "default_record", "fields":[
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "int"} ]},
                 "default": { "f1": 1, "f2": 2}},
        { "name": "bar", "type": "string" }
    ]
}
]])
local evolution_2 = json.decode([[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record*", "name": "default_record", "fields":[
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "int"} ]},
                 "default": { "f1": 1, "f2": 2}},
        { "name": "bar", "type": "string" }
    ]
}
]])
local ok, handle_1 = schema.create(evolution_1)
local ok, handle_2 = schema.create(evolution_2)
local ok, compiled = schema.compile({handle_1, handle_2})
local ok, data = compiled.unflatten({1,2,"asd"})
test:is_deeply(data, {foo={f1=1,f2=2},bar="asd"},
    'evolution: made record nullable && unflatten')
local ok, data = compiled.flatten({foo={f1=1,f2=2}, bar="asd"})
test:is_deeply(data, {{1,2},"asd"},
    'evolution: made record nullable && flatten')

-- Non record become nullable.

local evolution_1 = json.decode([[
{
    "type": "record",
    "name": "X",
    "fields": [
        { "type": "int", "name": "f1" },
        { "type": {"type": "array", "items": "int"}, "name": "f2" },
        { "type": {"type": "map", "values": "string"}, "name": "f3" }
    ]
}
]])
local evolution_2 = json.decode([[
{
    "type": "record",
    "name": "X",
    "fields": [
        { "type": "int*", "name": "f1" },
        { "type": {"type": "array*", "items": "int*"}, "name": "f2" },
        { "type": {"type": "map*", "values": "string*"}, "name": "f3" }
    ]
}
]])
local ok, handle_1 = schema.create(evolution_1)
local ok, handle_2 = schema.create(evolution_2)
local ok, compiled = schema.compile({handle_1, handle_2})
local ok, data = compiled.unflatten({1,{2,3},{a="4"}})
test:is_deeply(data, {f1=1, f2={2, 3}, f3={a="4"}},
    'evolution: made non-record nullable && unflatten')
local ok, data = compiled.flatten({f1=1, f2={2, 3}, f3={a="4"}})
test:is_deeply(data, {1,{2,3},{a="4"}},
    'evolution: made non-record nullable && flatten')

-- nullable -> non-nullable.

local evolution_to_nonnull = {
    type = "record",
    name = "X",
    fields = {
        -- Different fields would be inserted here.
    }
}

local testcases = {
    { "int*", "int" },
    { "string*", "string" },
    { {type="array*", items = "int"}, {type="array", items = "int"} },
    { {type="array", items = "int*"}, {type="array", items = "int"} },
    { {type="map*", values = "int"}, {type="map", values = "int"} },
    { {type="map", values = "int*"}, {type="map", values = "int"} },
    { {type="fixed*", size = 4, name = "f2"},
      {type="fixed", size = 4, name = "f2"} },
    { {type="enum*", symbols = {"a", "b"}, name = "f2"},
      {type="enum", symbols = {"a", "b"}, name = "f2"}},
    { {type="record*", name = "Y", fields = {
        {name = "f1", type = "int"}}},
      {type="record", name = "Y", fields = {
        {name = "f1", type = "int"}}}
    }
}

for _, testcase in pairs(testcases) do
    local evolution_1 = table.deepcopy(evolution_to_nonnull)
    evolution_1.fields[1] = {name = "f1", type = testcase[1]}
    local evolution_2 = table.deepcopy(evolution_to_nonnull)
    evolution_2.fields[1] = {name = "f1", type = testcase[2]}
    local typename = type(testcase[2]) == "string" and testcase[2] or
        testcase[2].type
    local ok, handle_1 = schema.create(evolution_1)
    local ok, handle_2 = schema.create(evolution_2)
    local ok, compiled = schema.compile({handle_1, handle_2})
    assert(not ok, typename)
    test:like(compiled, "Types incompatible:",
        "nullable -> non-nullable " .. typename)
end

test:check()
os.exit(test.planned == test.total and test.failed == 0 and 0 or -1)
