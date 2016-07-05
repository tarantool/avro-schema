local union_v1 = '["null", "int"]'
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
    error   = '',
    schema1 = union_v1,
    schema2 = union_v2,
    func = 'flatten', input = '{"string": "42"}'
}

-- TODO
