t {
    schema = '"null"',
    func = 'flatten', input = 'null', output = '[null]'
}

t {
    error = 'Expecting NIL, encountered FALSE',
    schema = '"null"',
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting NIL, encountered TRUE',
    schema = '"null"',
    func = 'flatten', input = 'true'
}

t {
    error = 'Expecting NIL, encountered LONG',
    schema = '"null"',
    func = 'flatten', input = '1'
}

-- ! is a hack to enable single-precision floating point
t {
    error = 'Expecting NIL, encountered FLOAT',
    schema = '"null"',
    func = 'flatten', input = '! 1.1'
}

t {
    error = 'Expecting NIL, encountered DOUBLE',
    schema = '"null"',
    func = 'flatten', input = '1.1'
}

t {
    error = 'Expecting NIL, encountered STR',
    schema = '"null"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error = 'Expecting NIL, encountered BIN',
    schema = '"null"',
    func = 'flatten', input = '{"$binary": "DEADBEEF"}'
}

t {
    error = 'Expecting NIL, encountered ARRAY',
    schema = '"null"',
    func = 'flatten', input = '[42]'
}

t {
    error = 'Expecting NIL, encountered MAP',
    schema = '"null"',
    func = 'flatten', input = '{"key": 42}'
}

t { -- test nullable fixed
    schema = [[
        {"type":"record","name":"X","fields":
                [{"name":"f1","type":{"type":"fixed*","name":"ff","size":4}},
                {"name":"f2","type":"int"}]}]],
    validate = '{"f2":1}',
    func = 'flatten',
    input = '{"f2":1}',
    output = '[0, null, 1]'
}
