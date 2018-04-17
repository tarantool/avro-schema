local schema =  require('avro_schema')
local tap    =  require('tap')
local json   =  require('json')
local msgpack = require('msgpack')

local test = tap.test('api-tests')

test:plan(26)

-- nested records, union, reference to earlier declared type
local foobar_decl = {
    name = 'FooBar',
    type = 'record',
    fields = {
        { name = 'A', type = {
             name = 'nested',
             type = 'record',
             fields = {
                { name = 'X', type = 'double' },
                { name = 'Y', type = 'double' }
             }
        }},
        { name = 'B', type = 'nested' },
        { name = 'C', type = {'null', 'int'}},
        { name = 'D', type = 'string'}
    }
}
local _, foobar = schema.create(foobar_decl)
test:is_deeply(schema.export(foobar), foobar_decl, 'export (FooBar)')

for _, type in ipairs(
        {
            "int", "string", "null", "boolean", "long",
            "float", "double", "bytes"
        }) do
    res = {schema.create({type=type})}
    test:is_deeply(schema.export(res[2]), type, 'schema normalization '..type)
end

-- fingerprint tests
local fingerprint_testcases = {
    {
        schema = [[
            {
              "name": "Pet",
              "type": "record",
              "fields":
                [{"name": "kind", "type":
                    {"name": "Kind", "type": "enum",
                    "symbols": ["CAT", "DOG"]}},
                {"name": "name", "type": "string"}
              ]
            }
        ]],
        fingerprint = "42620f01b34833f1e70cf2a9567fc4d3b9cf8b74afba64af0e9dce9a148b1e90"
    },
    {
        schema = [[{"type": "fixed", "name": "Id", "size": 4}]],
        fingerprint = "ecd9e5c6039fe40543f95176d664e1b9b56dddf1e8b1e3a6d87a6402b12e305d"
    },
    {
        schema = [[
            {
              "type": "record",
              "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
              "fields": [
                {"name": "match",
                 "type": {"type": "enum", "name": "HandshakeMatch",
                          "symbols": ["BOTH", "CLIENT", "NONE"]}},
                {"name": "serverProtocol", "type":
                    ["null", "string"]},
                {"name": "serverHash", "type":
                    ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
                {"name": "meta", "type":
                    ["null", {"type": "map", "values": "bytes"}]}
              ]
            }
        ]],
        fingerprint = "a303cbbfe13958f880605d70c521a4b7be34d9265ac5a848f25916a67b11d889"
    },
    {
        schema = [[
            {
              "type": "record",
              "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
              "fields": [
                {"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta", "type":
                    ["null", {"type": "map", "values": "bytes"}]}
              ]
            }
        ]],
        fingerprint = "2b2f7a9b22991fe0df9134cb6b5ff7355343e797aaea337e0150e20f3a35800e"
    },
}

function string.tohex(str)
    return (str:gsub('.', function (c)
        return string.format('%02X', string.byte(c))
    end))
end

for i, testcase in ipairs(fingerprint_testcases) do
    local _, schema_handler = schema.create(json.decode(testcase.schema))
    local fingerprint = schema.fingerprint(schema_handler, "sha256", 32)
    test:is(string.lower(string.tohex(fingerprint)), testcase.fingerprint,
        "Fingerprint testcase "..i)
end

local schema_preserve_fields_testcases = {
    {
        name = "1",
        schema = {
            type="int",
            extra_field="extra_field"
        },
        options = {},
        ast = "int"
    },
    {
        name = "2",
        schema = {
            type="int",
            extra_field="extra_field"
        },
        options = {preserve_in_ast={"extra_field"}},
        ast = {
            type="int",
            extra_field="extra_field"
        }
    },
    {
        name = "3-complex",
        schema = {
            type="int",
            extra_field={extra_field={"extra_field"}}
        },
        options = {preserve_in_ast={"extra_field"}},
        ast = {
            type="int",
            extra_field={extra_field={"extra_field"}}
        }
    },
}

for _, testcase in ipairs(schema_preserve_fields_testcases) do
    res = {schema.create(testcase.schema, testcase.options)}
    test:is_deeply(schema.export(res[2]), testcase.ast,
        'schema extra fields ' .. testcase.name)
end

test:is_deeply(
        {schema.create("int", {
                                preserve_in_ast={},
                                preserve_in_fingerprint={"extra_field"},
                             })},
        {false, "fingerprint should contain only fields from AST"},
        'preserve_in_fingerprint contains more fields than AST')

local fingerprint
res = {schema.create(
        {
            type = "record",
            name = "test",
            extra_field = "extra_field",
            fields = {
                { name = "bar", type = "null", default = msgpack.NULL,
                  extra_field = "extra" },
                { name = "foo", type = {"null", "int"},
                  default = msgpack.NULL },
            }
        }, nil)}
fingerprint = schema.fingerprint(res[2], "sha256", 32)
test:is(string.lower(string.tohex(fingerprint)),
        "a64098ee437e9020923c6005db88f37a234ed60daae23b26e33d8ae1bf643356",
        "Fingerprint extra fields 1")

res = {schema.create(
        {
            type = "record",
            name = "test",
            extra_field = "extra_field",
            fields = {
                { name = "bar", type = "null", default = msgpack.NULL,
                  extra_field = "extra" },
                { name = "foo", type = {"null", "int"},
                  default = msgpack.NULL },
            }
        }, {preserve_in_ast={"extra_field"},
            preserve_in_fingerprint={"extra_field"}})}
fingerprint = schema.fingerprint(res[2], "sha256", 32)
test:is(string.lower(string.tohex(fingerprint)),
        "70bd295335daafff0a4512cadc39a4298cd81c460defec530c7372bdd1ec6f44",
        "Fingerprint extra fields 2")

res = {schema.create(
        {
            type = "int",
            extra_field = "extra_field",
        }, {preserve_in_ast={"extra_field"}})}
fingerprint = schema.fingerprint(res[2], "sha256", 32)
test:is_deeply(schema.export(res[2]),
    {type = "int", extra_field = "extra_field"},
    "Prevent primitive type collapse by extra field")

-- avro_json is used for fingerprint
fingerprint = require("avro_schema.fingerprint")
test:is(fingerprint.avro_json({field1="1"}), "{}", "avro_json 1")
test:is(fingerprint.avro_json({field1="1"}, {"field1"}),
    '{"field1":"1"}', "avro_json 2")
test:is(fingerprint.avro_json({field2="1", field1="1"}, {"field2", "field1"}),
        '{"field1":"1","field2":"1"}', "avro_json 3 order")

local nullable_orig = [[ {
"name": "outer", "type": "record", "fields":
    [{ "name": "r1", "type":
        {"name": "tr1", "type": "record", "fields":
            [{"name": "v1", "type": "int"} ,
            {"name": "v2", "type": "string*"} ] } },
    { "name": "r2", "type": "tr1*"},
    { "name": "dummy", "type": {
         "name": "td", "type": "array", "items": "int" }},
    { "name": "r3", "type": {
          "name": "tr2", "type": "record*", "fields": [
                {"name": "v1", "type": "string"} ,
                {"name": "v2", "type": "int*"} ] } },
   { "name": "r4", "type": "tr2" }]
}]]

-- TODO: the `nullable_orig` should be used after #74
local nullable_exported = [[
{"type":"record","fields":
    [{"name":"r1","type":
        {"type":"record","fields":
            [{"name":"v1","type":"int"},
            {"name":"v2","type":{"type":"string*"}}],
        "name":"tr1"}},
    {"name":"r2","type":"tr1*"},
    {"name":"dummy","type":{"type":"array","items":"int"}},
    {"name":"r3","type":
        {"type":"record*","name":"tr2","fields":
            [{"name":"v1","type":"string"},
            {"name":"v2","type":{"type":"int*"}}]}},
    {"name":"r4","type":"tr2"}],
"name":"outer"}
]]
res = {schema.create(json.decode(nullable_orig))}
test:is(res[1], true, "Schema created successfully")
res = schema.export(res[2])
test:is_deeply(res, json.decode(nullable_exported), "Exported schema is valid.")

-- check if nullable reference is not exported as a definition
local nullable_reference = {
    name = "X",
    type = "record",
    fields = {
        {
            name = "first",
            type = {
                name = "first",
                type = "fixed",
                size = 16
            }
        },
        {
            name = "second",
            type = "first*"
        }
    }
}
res = {schema.create(nullable_reference)}
res = schema.export(res[2])
test:is_deeply(res, nullable_reference,
    "Export nullable reference")

test:check()
os.exit(test.planned == test.total and test.failed == 0 and 0 or -1)
