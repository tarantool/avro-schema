local schema =  require('avro_schema')
local tap    =  require('tap')
local json   =  require('json')
local msgpack = require('msgpack')

local test = tap.test('api-tests')

test:plan(46)

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
inf_loop_union_decl[2] = {
    type = "record",
    name = "infinite_union",
    fields = {
        {
            name = "f1",
            type = inf_loop_union_decl
        }
    }
}
test:is_deeply({schema.create(inf_loop_union_decl)},
   {false, '<union>/infinite_union/f1: Infinite loop detected in the data'},
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

-- large strings
local _, strm = schema.compile(select(2, schema.create('string')))
local s260 = string.rep('@', 260)
local s65400 = string.rep('@', 65400)
test:is_deeply({strm.unflatten({s260})}, {true, s260}, 'large string 260')
test:is_deeply({strm.unflatten({s65400})}, {true, s65400}, 'large string 65400')

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

-- Due to e719015 the nullable and non-nullable types are two different tables.
-- As only one table is initialized during copy_schema call, there is a time
-- when the other table is blank. This is a regression test for #77
local deferred_type_initialization_for_nullable_type = {
    {
        name = "Foo",
        type = "record*",
        fields = {
            {
                name = "foo",
                type = "float"
            }
        }
    }
}
res = {schema.create(deferred_type_initialization_for_nullable_type)}
test:is(res[1], true, "deferred initialization of nullable/non-nullable types")

test:check()
os.exit(test.planned == test.total and test.failed == 0 and 0 or -1)
