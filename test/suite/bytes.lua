t {
    schema = '"bytes"',
    func = 'flatten', input = '{"$binary": "FFFF"}',
    output = '[{"$binary": "FFFF"}]'
}

t {
    schema = '"bytes"',
    func = 'flatten', input = '{"$binary": ""}',
    output = '[{"$binary": ""}]'
}

t {
    schema = '"bytes"',
    func = 'flatten',
    input  =  '{"$binary": "CAFEBABE00000000DEAD0000BEEF00FF010203040506070809559A"}',
    output = '[{"$binary": "CAFEBABE00000000DEAD0000BEEF00FF010203040506070809559A"}]'
}

-- validation errors
t {
    error = 'Expecting BIN, encountered NIL',
    schema = '"bytes"',
    func = 'flatten', input = 'null'
}

t {
    error = 'Expecting BIN, encountered FALSE',
    schema = '"bytes"',
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting BIN, encountered TRUE',
    schema = '"bytes"',
    func = 'flatten', input = 'true'
}

t {
    error = 'Expecting BIN, encountered LONG',
    schema = '"bytes"',
    func = 'flatten', input = '42'
}

-- ! is a hack to enable single-precision floating point
t {
    error = 'Expecting BIN, encountered FLOAT',
    schema = '"bytes"',
    func = 'flatten', input = '! 1.0'
}

t {
    error = 'Expecting BIN, encountered DOUBLE',
    schema = '"bytes"',
    func = 'flatten', input = '1.0'
}

t {
    error = 'Expecting BIN, encountered STR',
    schema = '"bytes"',
    func = 'flatten', input = '"Loads of oranges!"'
}

t {
    error = 'Expecting BIN, encountered ARRAY',
    schema = '"bytes"',
    func = 'flatten', input = '[]'
}

t {
    error = 'Expecting BIN, encountered MAP',
    schema = '"bytes"',
    func = 'flatten', input = '{}'
}
