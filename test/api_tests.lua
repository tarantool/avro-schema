local schema =  require('avro_schema')
local tap    =  require('tap')
local json   =  require('json')
local msgpack = require('msgpack')

local test = tap.test('api-tests')

test:plan(69)

test:is_deeply({schema.create()}, {false, 'Unknown Avro type: nil'},
               'error unknown type')

local res = {schema.create('int') }
test:is_deeply(res, {true, {}}, 'schema handle is a table (1)')
local int = res[2]

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
local res = {schema.create(foobar_decl)}
test:is_deeply(res, {true, {}}, 'schema handle is a table (2)')
local foobar = res[2]

local ok, sch = schema.create({
    type = "record",
    name = "test",
    fields = {
      { name = "foo", type = {"int", "null"}, default = msgpack.NULL },
    }
})

test:ok(not ok, 'default non-NULL compiles not ok')
test:diag("error: %s", json.encode(sch))

local ok, sch = schema.create({
    type = "record",
    name = "test",
    fields = {
      { name = "bar", type = "null", default = msgpack.NULL },
      { name = "foo", type = {"null", "int"}, default = msgpack.NULL },
    }
})

test:ok(ok, 'default NULL compiles ok')

local ok, err = schema.validate(sch, {})
test:diag('output: %s', json.encode(err))
if test:ok(ok, 'default NULL validates ok') then
    test:is(err.bar, msgpack.NULL, 'checking default value')
    test:is(err.foo, msgpack.NULL, 'checking default value')
end

local ok, err = schema.validate(sch, { foo = { int  = 5 }})
test:diag('output: %s', json.encode(err))
if test:ok(ok, 'default NULL validates ok') then
    test:is(err.bar, msgpack.NULL, 'checking default value')
    test:is(err.foo.int, 5, 'checking non-default value')
end

-- XXX expects a schema
test:is_deeply({pcall(schema.are_compatible, int, 42)},
               {false, 'Not a schema: 42'},
               'are_compatible expects a schema (1)')
test:is_deeply({pcall(schema.are_compatible, 42, int)},
               {false, 'Not a schema: 42'},
               'are_compatible expects a schema (2)')
test:is_deeply({pcall(schema.compile, 42)},
               {false, 'Expecting a schema or a table'},
               'compile expects a schema (1)')
test:is_deeply({pcall(schema.compile, {42})},
               {false, 'Not a schema: 42'},
               'compile expects a schema (2)')
test:is_deeply({pcall(schema.compile, {int,42})},
               {false, 'Not a schema: 42'},
               'compile expects a schema (3)')
test:is_deeply({pcall(schema.get_names, 42)},
               {false, 'Not a schema: 42'},
               'get_names expects a schema')
test:is_deeply({pcall(schema.get_types, 42)},
               {false, 'Not a schema: 42'},
               'get_types expects a schema')
test:is_deeply({pcall(schema.validate, 42)},
               {false, 'Not a schema: 42'},
               'validate expects a schema')
test:is_deeply({pcall(schema.export, 42)},
               {false, 'Not a schema: 42'},
               'export expects a schema')

-- schema handle serialization
test:is(tostring(int),       'Schema (int)',      'tostring (1)')
test:is(json.encode(int),    '"Schema (int)"',    'serialize (1)')
test:is(tostring(foobar),    'Schema (FooBar)',   'tostring (2)')
test:is(json.encode(foobar), '"Schema (FooBar)"', 'serialize (2)')

-- are_compatible
local barfoo_decl = {
    name = 'BarFoo', type = 'record', aliases = {'FooBar'},
    fields = {
        { name = 'A', type = {
             name = 'nested', type = 'record', fields = {
                { name = 'X', type = 'double' },
                { name = 'Y', type = 'double' }
             }
        }},
        { name = 'B', type = 'nested' },
        { name = 'C', type = {'null', 'int'}},
        { name = 'D', type = 'string'}
    }
}
local _, barfoo = schema.create(barfoo_decl)

test:is(schema.are_compatible(foobar, barfoo), true, 'are_compatible (1)')
test:is(schema.are_compatible(barfoo, foobar), false, 'are_compatible (2)')
test:is(schema.are_compatible(barfoo, foobar, 'downgrade'), true,
        'are_compatible (3)')

-- create, infinite loop
local inf_loop_union_decl = { 'null' }
inf_loop_union_decl[2] = inf_loop_union_decl
test:is_deeply({schema.create(inf_loop_union_decl)},
               {false, '<union>/<branch-2>: Infinite loop detected in the data'},
               'create / infinite loop (union)')
-- compile
local res = {schema.compile(int)}
test:test("compile / int", function(test)
    test:plan(18)
    test:is(res[1], true, '(1)')
    test:istable(res[2], '(2)')
    local m = res[2]
    local flatten, unflatten = m.flatten, m.unflatten
    local flatten_msgpack    = m.flatten_msgpack
    local unflatten_msgpack  = m.unflatten_msgpack
    test:is(type(flatten), 'function', '(3)')
    test:is(type(unflatten), 'function', '(4)')
    test:is(type(flatten_msgpack), 'function', '(5)')
    test:is(type(unflatten_msgpack), 'function', '(6)')
    test:is_deeply({flatten(42)}, {true, {42}}, '(7)')
    test:is_deeply({flatten(msgpack.encode(42))}, {true, {42}}, '(8)')
    test:is_deeply({unflatten({42})}, {true, 42}, '(9)')
    test:is_deeply({unflatten(msgpack.encode({42}))}, {true, 42}, '(10)')
    test:is_deeply({flatten_msgpack(42)}, {true, msgpack.encode({42})}, '(11)')
    test:is_deeply({flatten_msgpack(msgpack.encode(42))}, {true, msgpack.encode({42})}, '(12)')
    test:is_deeply({unflatten_msgpack({42})}, {true, msgpack.encode(42)}, '(13)')
    test:is_deeply({unflatten_msgpack(msgpack.encode({42}))}, {true, msgpack.encode(42)}, '(14)')
    test:is_deeply({flatten('')}, {false, 'Truncated data'}, '(15)')
    test:is_deeply({unflatten('')}, {false, 'Truncated data'}, '(16)')
    test:is_deeply({flatten_msgpack('')}, {false, 'Truncated data'}, '(17)')
    test:is_deeply({unflatten_msgpack('')}, {false, 'Truncated data'}, '(18)')
end)

-- get_names
test:is_deeply(schema.get_names(int), {}, 'get_names (int)')
test:is_deeply(schema.get_names(foobar),
               {'A.X','A.Y','B.X','B.Y','C.$type$','C','D'},
               'get_names (FooBar)')

-- get_types
test:is_deeply(schema.get_types(int), {}, 'get_types (int)')
test:is_deeply(schema.get_types(foobar),
               {'double','double','double','double',nil,nil,'string'},
               'get_types (FooBar)')

-- is
test:is(schema.is(int), true, 'schema handle is a schema (1)')
test:is(schema.is(foobar), true, 'schema handle is a schema (1)')
test:is(schema.is(), false, 'nil is not a schema')
test:is(schema.is({}), false, 'table is not a schema')

-- validate
local inf_loop_foobar = {
    A = {X = 1, Y = 2},
    C = {int = 42},
    D = 'Hello, world!'
}
inf_loop_foobar.B = inf_loop_foobar
test:is_deeply({schema.validate(foobar, inf_loop_foobar)},
               {false, 'B: Infinite loop detected in the data'},
               'validate / infinite loop')

-- export
test:is(schema.export(int), 'int', 'export (int)')
test:is_deeply(schema.export(foobar), foobar_decl, 'export (FooBar)')

-- large strings
local _, strm = schema.compile(select(2, schema.create('string')))
local s260 = string.rep('@', 260)
local s65400 = string.rep('@', 65400)
test:is_deeply({strm.unflatten({s260})}, {true, s260}, 'large string 260')
test:is_deeply({strm.unflatten({s65400})}, {true, s65400}, 'large string 65400')

for _, type in ipairs({"int", "string", "null", "boolean", "long", "float", "double", "bytes"}) do
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
              "fields": [
                {"name": "kind", "type": {"name": "Kind", "type": "enum", "symbols": ["CAT", "DOG"]}},
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
                {"name": "serverProtocol",
                 "type": ["null", "string"]},
                {"name": "serverHash",
                 "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}
              ]
            }
        ]],
        fingerprint = "a303cbbfe13958f880605d70c521a4b7be34d9265ac5a848f25916a67b11d889"
    },
    -- in case of type reuse, it should not be copied. It should only contain type name
    -- {"name": "serverHash", "type": "MD5"}, -- > {"name":"serverHash","type":{"name":"org.apache.avro.ipc.MD5","type":"fixed","size":16}}!!!
    -- correct fingerprint is "2b2f7a9b22991fe0df9134cb6b5ff7355343e797aaea337e0150e20f3a35800e"
    {
        schema = [[
            {
              "type": "record",
              "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
              "fields": [
                {"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
              ]
            }
        ]],
        fingerprint = "ef17a5460289684db839c86a0c2cdcfe69da9dd0a3047e6a91f6d6bc37f76314"

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
    test:is(string.lower(string.tohex(fingerprint)), testcase.fingerprint, "Fingerprint testcase "..i)
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
    }
}

for _, testcase in ipairs(schema_preserve_fields_testcases) do
    res = {schema.create(testcase.schema, testcase.options)}
    test:is_deeply(schema.export(res[2]), testcase.ast, 'schema extra fields '..testcase.name)
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
                { name = "bar", type = "null", default = msgpack.NULL, extra_field = "extra" },
                { name = "foo", type = {"null", "int"}, default = msgpack.NULL },
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
                { name = "bar", type = "null", default = msgpack.NULL, extra_field = "extra" },
                { name = "foo", type = {"null", "int"}, default = msgpack.NULL },
            }
        }, {preserve_in_ast={"extra_field"}, preserve_in_fingerprint={"extra_field"}})}
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
test:is_deeply(schema.export(res[2]), {type = "int", extra_field = "extra_field"},
        "Prevent primitive type collapse by extra field")

-- avro_json is used for fingerprint
fingerprint = require("avro_schema.fingerprint")
test:is(fingerprint.avro_json({field1="1"}), "{}", "avro_json 1")
test:is(fingerprint.avro_json({field1="1"}, {"field1"}), '{"field1":"1"}', "avro_json 2")
test:is(fingerprint.avro_json({field2="1", field1="1"}, {"field2", "field1"}),
        '{"field1":"1","field2":"1"}', "avro_json 3 order")

-- check that schema which uses any type cannot be compiled
res = {schema.create({
    name = "foo",
    type = "record",
    fields = {
        {
            name = "X",
            type = "any"
        }
    }
})}
test:is(true, res[1], "Schema created successfully")
res = {schema.compile(res[2])}
test:is(false, res[1], "Schema cannot be compiled")
test:like(res[2], "ANY: not supported in compiled schemas",
    "any: compile error message")

-- check `forward_reference` option
local forward_orig = {
    {
        name = "X",
        type = "record",
        fields = {
            {
                name = "reference_second",
                type = "second"
            },
            {
                name = "f2",
                type = {
                    name = "second",
                    type = "record",
                    fields = {
                        {
                            name = "f1",
                            type = "double"
                        },
                        {
                            name = "reference_first",
                            type = "first"
                        }
                    }
                }
            }
        }
    },
    {
        name = "first",
        type = "fixed",
        size = 16
    }
}
local forward_canonical = {
    {
        name = "X",
        type = "record",
        fields = {
            {
                name = "reference_second",
                type = {
                    type = "record",
                    name = "second",
                    fields = {
                        {
                            name = "f1",
                            type = "double"
                        },
                        {
                            name = "reference_first",
                            type = {
                                name = "first",
                                type = "fixed",
                                size = 16
                            }
                        }
                    }
                }
            },
            {
                name = "f2",
                type = "second"
            }
        }
    },
    "first"
}
res = {schema.create(forward_orig, {forward_reference = true})}
test:is(res[1], true, "Schema created successfully")
res = schema.export(res[2])
test:is_deeply(res, forward_canonical,
    "Exported schema should be canonical.")

test:check()
os.exit(test.planned == test.total and test.failed == 0 and 0 or -1)
