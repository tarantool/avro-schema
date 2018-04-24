-- ! is a hack to encode msgpack using single precision doubles
t {
    schema = '"double"',
    func = 'flatten', input = '99.25', output = '[99.25]'
}

t {
    schema = '"double"',
    func = 'flatten', input = '! 99.25', output = '[99.25]'
}

-- auto long->double conversion
t {
    schema = '"double"',
    func = 'flatten', input = '42', output = '[42.0]'
}

t {
    schema = '"double"',
    func = 'flatten', input = '-9000000', output = '[-9000000.0]'
}

-- validation errors
t {
    error = 'Expecting DOUBLE, encountered NIL',
    schema = '"double"',
    func = 'flatten', input = 'null'
}

t {
    error = 'Expecting DOUBLE, encountered FALSE',
    schema = '"double"',
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting DOUBLE, encountered TRUE',
    schema = '"double"',
    func = 'flatten', input = 'true'
}

t {
    error = 'Expecting DOUBLE, encountered STR',
    schema = '"double"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error = 'Expecting DOUBLE, encountered BIN',
    schema = '"double"',
    func = 'flatten', input = '{"$binary": "CAFEBABE"}'
}

t {
    error = 'Expecting DOUBLE, encountered ARRAY',
    schema = '"double"',
    func = 'flatten', input = '[]'
}

t {
    error = 'Expecting DOUBLE, encountered MAP',
    schema = '"double"',
    func = 'flatten', input = '{}'
}

t {
    schema = '"double*"',
    func = 'flatten', input = '42', output = '[42.0]'
}

t {
    schema = '"double*"',
    func = 'flatten', input = 'null', output = '[null]'
}

t {
    schema = '"double*"',
    func = 'unflatten', input = '[42]', output = '42.0'
}

t {
    schema = '"double*"',
    func = 'unflatten', input = '[null]', output = 'null'
}
