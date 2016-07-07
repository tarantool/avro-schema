local union_v1 = '["null", "int", "double"]'
local union_v2 = '["null", "string", "int"]' -- different int id

-- flatten

t {
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'flatten', input = 'null', output = '[0, null]'
}

t {
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'flatten', input = '{"int": 42}', output = '[2, 42]'
}

t {
    error   = 'Unknown key: "string"',
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'flatten', input = '{"string": "42"}'
}

t {
    error   = 'Unknown key: "double" (schema versioning)',
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'flatten', input = '{"double": "42"}'
}

--

t {
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'flatten', input = 'null', output = '[0, null]'
}

t {
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'flatten', input = '{"int": 42}', output = '[1, 42]'
}

t {
    error   = 'Unknown key: "string" (schema versioning)',
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'flatten', input = '{"string": "42"}'
}

t {
    error   = 'Unknown key: "double"',
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'flatten', input = '{"double": "42"}'
}

-- unflatten

t {
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'unflatten', input = '[0, null]', output = 'null'
}

t {
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'unflatten', input = '[1, 42]', output = '{"int": 42}'
}

t {
    error   = '1: Bad value: 2 (schema versioning)',
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'unflatten', input = '[2, 42.0]'
}

--

t {
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'unflatten', input = '[0, null]', output = 'null'
}

t {
    error   = '1: Bad value: 1 (schema versioning)',
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'unflatten', input = '[1, 42]'
}

t {
    schema1 = union_v2,
    schema2 = union_v1,
    func = 'unflatten', input = '[2, 42]', output = '{"int": 42}'
}
