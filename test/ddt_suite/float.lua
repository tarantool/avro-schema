-- ! is a hack to encode msgpack using single precision floats
t {
    schema = '"float"',
    func = 'flatten', input = '! 99.25', output = '! [99.25]'
}

t {
    schema = '"float"',
    func = 'flatten', input = '99.25', output = '! [99.25]'
}

-- auto long->float conversion
t {
    schema = '"float"',
    func = 'flatten', input = '42', output = '! [42.0]'
}

t {
    schema = '"float"',
    func = 'flatten', input = '-9000000', output = '! [-9000000.0]'
}

-- validation errors
t {
    error = 'Expecting FLOAT, encountered NIL',
    schema = '"float"',
    func = 'flatten', input = 'null'
}

t {
    error = 'Expecting FLOAT, encountered FALSE',
    schema = '"float"',
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting FLOAT, encountered TRUE',
    schema = '"float"',
    func = 'flatten', input = 'true'
}

t {
    error = 'Expecting FLOAT, encountered STR',
    schema = '"float"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error = 'Expecting FLOAT, encountered BIN',
    schema = '"float"',
    func = 'flatten', input = '{"$binary": "CAFEBABE"}'
}

t {
    error = 'Expecting FLOAT, encountered ARRAY',
    schema = '"float"',
    func = 'flatten', input = '[]'
}

t {
    error = 'Expecting FLOAT, encountered MAP',
    schema = '"float"',
    func = 'flatten', input = '{}'
}
