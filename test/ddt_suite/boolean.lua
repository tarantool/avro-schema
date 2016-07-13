t {
    schema = '"boolean"',
    func = 'flatten', input = 'false', output = '[false]'
}

t {
    schema = '"boolean"',
    func = 'flatten', input = 'true', output = '[true]'
}

t {
    error  = 'Expecting BOOL, encountered NIL',
    schema = '"boolean"',
    func = 'flatten', input = 'null'
}

t {
    error  = 'Expecting BOOL, encountered LONG',
    schema = '"boolean"',
    func = 'flatten', input = '42'
}

-- ! is a hack to enable single-precision floating point
t {
    error  = 'Expecting BOOL, encountered FLOAT',
    schema = '"boolean"',
    func = 'flatten', input = '! 42.0'
}

t {
    error  = 'Expecting BOOL, encountered DOUBLE',
    schema = '"boolean"',
    func = 'flatten', input = '42.0'
}

t {
    error  = 'Expecting BOOL, encountered STR',
    schema = '"boolean"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error = 'Expecting BOOL, encountered BIN',
    schema = '"boolean"',
    func = 'flatten', input = '{"$binary": "DEADBEEF"}'
}

t {
    error  = 'Expecting BOOL, encountered ARRAY',
    schema = '"boolean"',
    func = 'flatten', input = '[42]'
}

t {
    error  = 'Expecting BOOL, encountered MAP',
    schema = '"boolean"',
    func = 'flatten', input = '{"key": 42}'
}
