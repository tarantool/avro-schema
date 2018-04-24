t {
    schema = '"int"',
    func = 'flatten', input = '42', output = '[42]'
}

t {
    schema = '"int"',
    func = 'flatten', input = '-9000', output = '[-9000]'
}

t {
    schema = '"int"',
    func = 'flatten', input = '9000000', output = '[9000000]'
}

t {
    schema = '"int"',
    func = 'flatten', input = '-9000000', output = '[-9000000]'
}

t {
    schema = '"int"',
    func = 'flatten', input = '2147483647', output = '[2147483647]'
}

t {
    schema = '"int"',
    func = 'flatten', input = '-2147483648', output = '[-2147483648]'
}

-- validation errors
t {
    error = 'Expecting INT, encountered NIL',
    schema = '"int"',
    func = 'flatten', input = 'null'
}

t {
    error = 'Expecting INT, encountered FALSE',
    schema = '"int"',
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting INT, encountered TRUE',
    schema = '"int"',
    func = 'flatten', input = 'true'
}

-- ! is a hack to enable single-precision floating point
t {
    error = 'Expecting INT, encountered FLOAT',
    schema = '"int"',
    func = 'flatten', input = '! 1.0'
}

t {
    error = 'Expecting INT, encountered DOUBLE',
    schema = '"int"',
    func = 'flatten', input = '1.0'
}

t {
    error = 'Expecting INT, encountered STR',
    schema = '"int"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error = 'Expecting INT, encountered BIN',
    schema = '"int"',
    func = 'flatten', input = '{"$binary": "CAFEBABE"}'
}

t {
    error = 'Expecting INT, encountered ARRAY',
    schema = '"int"',
    func = 'flatten', input = '[]'
}

t {
    error = 'Expecting INT, encountered MAP',
    schema = '"int"',
    func = 'flatten', input = '{}'
}

t {
    error = 'Value exceeds INT range: 2147483648LL',
    schema = '"int"',
    func = 'flatten', input = '2147483648'
}

t {
    error = 'Value exceeds INT range: -2147483649LL',
    schema = '"int"',
    func = 'flatten', input = '-2147483649'
}

t {
    schema = '"int*"',
    func = 'flatten', input = '42', output = '[42]'
}

t {
    schema = '"int*"',
    func = 'flatten', input = 'null', output = '[null]'
}

t {
    schema = '"int*"',
    func = 'unflatten', input = '[42]', output = '42'
}

t {
    schema = '"int*"',
    func = 'unflatten', input = '[null]', output = 'null'
}
