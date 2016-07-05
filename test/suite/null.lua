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
    error = 'Expecting NIL, encountered ARRAY',
    schema = '"null"',
    func = 'flatten', input = '[42]'
}

t {
    error = 'Expecting NIL, encountered MAP',
    schema = '"null"',
    func = 'flatten', input = '{"key": 42}'
}
