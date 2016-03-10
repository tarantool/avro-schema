#!/usr/bin/env tarantool
for _, dir in pairs({ "..", ".", os.getenv("BINARY_DIR") }) do
    package.cpath = package.cpath .. ";" .. dir .. "/avro/?.so;" .. dir .. "/avro/?.dylib"
end

local tap = require('tap')
local avro = require('avro')

local create_schema = avro.create_schema
local flatten       = avro.flatten
local unflatten     = avro.unflatten
local is_compatible = avro.schema_is_compatible

local test = tap.test('Avro module')
test:plan(7)


-- hook GC methods to produce log
local gc_log
local function wrap_gc(orig_gc)
    return function(arg)
        if gc_log then 
            table.insert(gc_log, tostring(arg))
        end
        orig_gc(arg)
    end
end
for _,M in pairs({ avro._get_metatables() }) do
    M.__gc = wrap_gc(M.__gc)
end

-- a few stock schema prototypes
local int_schema_p          = { type = "int" }
local long_schema_p         = { type = "long" }
local float_schema_p        = { type = "float" }
local double_schema_p       = { type = "double" }
local string_schema_p       = { type = "string" }
local int_array_schema_p    = { type = "array", items = "int" }
local string_array_schema_p = { type = "array", items = "string" }
local frob_v1_schema_p      = {
    type = "record",
    name = "X.Frob",
    fields = {
        { name = "A", type = "int" },
        { name = "B", type = "int" },
        { name = "C", type = "int" }
    }
}
local frob_v2_schema_p    = {
    type = "record",
    name = "X.Frob",
    fields = {
        { name = "A", type = "int" },
        { name = "B", type = "int" },
        { name = "C", type = "int" },
        { name = "D", type = "string" }
    }
}
local frob_v1_array_schema_p = { type = "array", items = frob_v1_schema_p }
local complex_schema_p = {
    type = "record",
    name = "X.Complex",
    fields = {
        { name = "A", type = "int" },
        { name = "B", type = "int" },
        { name = "C", type = "int" },
        { name = "D", type = {
            type = "record",
            name = "X.Nested",
            fields = {
                { name = "E", type = "int" },
                { name = "F", type = "int" },
                { name = "G", type = "int" }
            }
        }}
    }
}

--
-- load-good-schema
--
test:test('load-good-schema', function(test)

    local tests = {
        {'load-int-schema',           int_schema_p,           'Avro schema (int)'},
        {'load-long-schema',          long_schema_p,          'Avro schema (long)'},
        {'load-float-schema',         float_schema_p,         'Avro schema (float)'},
        {'load-double-schema',        double_schema_p,        'Avro schema (double)'},
        {'load-string-schema',        string_schema_p,        'Avro schema (string)'},
        {'load-int-array-schema',     int_array_schema_p,     'Avro schema (array)'},
        {'load-string-array-schema',  string_array_schema_p,  'Avro schema (array)'},
        {'load-frob-v1-schema',       frob_v1_schema_p,       'Avro schema (X.Frob)'},
        {'load-frob-v2-schema',       frob_v2_schema_p,       'Avro schema (X.Frob)'},
        {'load-frob-v1-array-schema', frob_v1_array_schema_p, 'Avro schema (array)'},
        {'load-complex-schema',       complex_schema_p,       'Avro schema (X.Complex)'}
    }
 
    test:plan(#tests)

    for _,v in pairs(tests) do
        local ok, schema = create_schema(v[2])
        test:is_deeply({ok, tostring(schema)}, {true, v[3]}, v[1])
    end
end)

--
-- load-bad-schema
--
test:test('load-bad-schema', function(test)

    test:plan(7)

    test:is_deeply({create_schema('')}, {false, 'Error parsing JSON: unexpected token near end of file'}, 'bad1')
    test:is_deeply({create_schema({})}, {false, 'Union type must have at least one branch'}, 'bad2')
    test:is_deeply({create_schema({ type = 'broccoli' })}, {false, 'Unknown Avro "type": broccoli'}, 'bad3')
    test:is_deeply({create_schema({ type = 'array' })}, {false, 'Array type must have "items"'}, 'bad4')
    test:is_deeply({create_schema({ type = 'record' })}, {false, 'Record type must have a "name"'}, 'bad5')
    test:is_deeply({create_schema({ type = 'record', name = 'X' })}, {false, 'Record type must have "fields"'}, 'bad6')
    test:is_deeply(
        { create_schema({ type = 'record', name = 'X', fields = {}}) },
        { false, 'Record type must have at least one field' },
        'bad7')
end)

--
-- resolver cache
--
test:test('resolver-cache', function(test)
    local _, frob_v1_schema = create_schema(frob_v1_schema_p)
    local _, frob_v2_schema = create_schema(frob_v2_schema_p)
    local cache = avro._get_resolver_cache()

    test:plan(5)
    test:is_deeply(cache, {}, 'resolver-cache-initially-empty')
    test:is_deeply(avro._create_resolver(frob_v2_schema, frob_v1_schema), nil, 'resolver-created')
    test:istable(cache[frob_v2_schema], 'resolver-was-cached-1')
    test:is(tostring(cache[frob_v2_schema][frob_v1_schema]), 'Avro schema resolver', 'resolver-was-cached-2')

    frob_v1_schema = nil
    frob_v2_schema = nil
    collectgarbage()
    collectgarbage()
    test:is_deeply(cache, {}, 'resolver-cache-auto-pruned')
end)

--
-- objects properly GC-ed
--
test:test('gc', function(test)

    local _, frob_v1_schema = create_schema(frob_v1_schema_p)
    collectgarbage()
    test:plan(6)

    gc_log = {}
    create_schema(frob_v1_schema_p)
    collectgarbage()
    test:is_deeply(gc_log, {'Avro schema (X.Frob)'}, 'gc-schema')

    gc_log = {}
    do
        local _, frob_v1_schema = create_schema(frob_v1_schema_p)
        local _, frob_v2_schema = create_schema(frob_v2_schema_p)
        avro._create_resolver(frob_v2_schema, frob_v1_schema)
    end
    collectgarbage()
    collectgarbage()
    collectgarbage()
    table.sort(gc_log)
    test:is_deeply(gc_log, {'Avro schema (X.Frob)', 'Avro schema (X.Frob)', 'Avro schema resolver'}, 'gc-resolver')

    gc_log = {}
    flatten({ A = 1, B = 2, C = 3 }, frob_v1_schema)
    collectgarbage()
    test:is_deeply(gc_log, {'Avro xform ctx'}, 'gc-flatten-xform-ctx')

    gc_log = {}
    flatten({ A = '', B = 2, C = 3 }, frob_v1_schema)
    collectgarbage()
    test:is_deeply(gc_log, {'Avro xform ctx'}, 'gc-flatten-error-xform-ctx')

    gc_log = {}
    unflatten({ 1, 2, 3 }, frob_v1_schema)
    collectgarbage()
    test:is_deeply(gc_log, {'Avro xform ctx'}, 'gc-unflatten-xform-ctx')

    gc_log = {}
    unflatten({ '', 2, 3 }, frob_v1_schema)
    collectgarbage()
    test:is_deeply(gc_log, {'Avro xform ctx'}, 'gc-unflatten-error-xform-ctx')

    gc_log = nil
end)

--
-- flatten
--
test:test('flatten', function(test)
    local _, frob_v1_schema = create_schema(frob_v1_schema_p)
    local _, frob_v2_schema = create_schema(frob_v2_schema_p)
    local _, frob_v1_array_schema = create_schema(frob_v1_array_schema_p)
    local _, complex_schema = create_schema(complex_schema_p)
    local ABC = { A = 1, B = 2, C = 3 }
    local flat_ABC = { 1, 2, 3 }
    local ABCD = { A = 1, B = 2, C = 3, D = 'test' }
    local flat_ABCD = { 1, 2, 3, 'test' }
    local ABCDEFG = { A = 1, B = 2, C = 3, D = { E = 4, F = 5, G = 6 } }
    local flat_ABCDEFG = { 1, 2, 3, 4, 5, 6 }

    test:plan(7)

    test:is_deeply(
        { flatten(ABC, frob_v1_schema) },
        { true, flat_ABC },
        'flatten-frob-v1')

    test:is_deeply(
        { flatten({ ABC, ABC, ABC }, frob_v1_array_schema) },
        { true, { flat_ABC, flat_ABC, flat_ABC } },
        'flatten-frob-v1-array')

    test:is_deeply(
        { flatten(ABCD, frob_v2_schema) },
        { true, flat_ABCD },
        'flatten-frob-v2')

    test:is_deeply(
        { flatten(ABCD, frob_v2_schema, frob_v1_schema) },
        { true, flat_ABC },
        'flatten-frob-v2-as-frob-v1')

    test:is_deeply(
        { flatten(ABCDEFG, complex_schema) },
        { true, flat_ABCDEFG },
        'flatten-complex')

    test:is_deeply(
        { flatten({ A = '', B = 2, C = 3 }, frob_v1_schema) },
        { false, 'type mismatch' },
        'flatten-error-type-mismatch')

    local T = {}
    T[1] = T
    test:is_deeply(
        { flatten(T, frob_v1_array_schema) },
        { false, 'circular ref' },
        'flatten-error-circular-ref')
end)

--
-- unflatten
--
test:test('unflatten', function(test)
    local _, frob_v1_schema = create_schema(frob_v1_schema_p)
    local _, frob_v2_schema = create_schema(frob_v2_schema_p)
    local _, frob_v1_array_schema = create_schema(frob_v1_array_schema_p)
    local _, complex_schema = create_schema(complex_schema_p)
    local ABC = { A = 1, B = 2, C = 3 }
    local flat_ABC = { 1, 2, 3 }
    local ABCD = { A = 1, B = 2, C = 3, D = 'test' }
    local flat_ABCD = { 1, 2, 3, 'test' }
    local ABCDEFG = { A = 1, B = 2, C = 3, D = { E = 4, F = 5, G = 6 } }
    local flat_ABCDEFG = { 1, 2, 3, 4, 5, 6 }

    test:plan(7)

    test:is_deeply(
        { unflatten(flat_ABC, frob_v1_schema) },
        { true, ABC },
        'unflatten-frob-v1')

    test:is_deeply(
        { unflatten({ flat_ABC, flat_ABC, flat_ABC }, frob_v1_array_schema) },
        { true, { ABC, ABC, ABC } },
        'unflatten-frob-v1-array')

    test:is_deeply(
        { unflatten(flat_ABCD, frob_v2_schema) },
        { true, ABCD },
        'unflatten-frob-v2')

    test:is_deeply(
        { unflatten(flat_ABCD, frob_v2_schema, frob_v1_schema) },
        { true, ABC },
        'unflatten-frob-v2-as-frob-v1')

    test:is_deeply(
        { unflatten(flat_ABCDEFG, complex_schema) },
        { true, ABCDEFG },
        'unflatten-complex')

    test:is_deeply(
        { unflatten({ '', 2, 3 }, frob_v1_schema) },
        { false, 'type mismatch' },
        'flatten-error-type-mismatch')

    local T = {}
    T[1] = T
    test:is_deeply(
        { unflatten(T, frob_v1_array_schema) },
        { false, 'circular ref' },
        'flatten-error-circular-ref')
end)

--
-- schema compatibility
--
test:test('is-compatible', function(test)
    local _, frob_v1_schema = create_schema(frob_v1_schema_p)
    local _, frob_v2_schema = create_schema(frob_v2_schema_p)

    test:plan(2)

    test:is_deeply(
        { is_compatible(frob_v1_schema, frob_v2_schema) },
        { false, 'Reader field D doesn\'t appear in writer' },
        'upgrade')

    test:is_deeply(
        { is_compatible(frob_v2_schema, frob_v1_schema) },
        { true },
        'downgrade')

end)

test:check()

os.exit(test.planned == test.total and test.failed == 0 and 0 or -1)
