t {
    schema = '"string"',
    func = 'flatten', input = '"Hello, world!"',
    output = '["Hello, world!"]'
}

t {
    schema = '"string"',
    func = 'flatten', input = '""',
    output = '[""]'
}

t {
    schema = '"string"',
    func = 'flatten',
    input =   '"123456789.123456789.123456789.1234567890"',
    output = '["123456789.123456789.123456789.1234567890"]'
}

-- validation errors
t {
    error = 'Expecting STR, encountered NIL',
    schema = '"string"',
    func = 'flatten', input = 'null'
}

t {
    error = 'Expecting STR, encountered FALSE',
    schema = '"string"',
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting STR, encountered TRUE',
    schema = '"string"',
    func = 'flatten', input = 'true'
}

t {
    error = 'Expecting STR, encountered LONG',
    schema = '"string"',
    func = 'flatten', input = '42'
}

-- ! is a hack to enable single-precision floating point
t {
    error = 'Expecting STR, encountered FLOAT',
    schema = '"string"',
    func = 'flatten', input = '! 1.0'
}

t {
    error = 'Expecting STR, encountered DOUBLE',
    schema = '"string"',
    func = 'flatten', input = '1.0'
}

t {
    error = 'Expecting STR, encountered BIN',
    schema = '"string"',
    func = 'flatten', input = '{"$binary": "CAFEBABE"}'
}

t {
    error = 'Expecting STR, encountered ARRAY',
    schema = '"string"',
    func = 'flatten', input = '[]'
}

t {
    error = 'Expecting STR, encountered MAP',
    schema = '"string"',
    func = 'flatten', input = '{}'
}

t {
    schema = '"string*"',
    func = 'flatten', input = '"kek"', output = '[1, "kek"]'
}

t {
    schema = '"string*"',
    func = 'flatten', input = 'null', output = '[0, null]'
}

t {
    schema = '"string*"',
    func = 'unflatten', input = '[1, "kek"]', output = '"kek"'
}

t {
    schema = '"string*"',
    func = 'unflatten', input = '[0, null]', output = 'null'
}
