t {
    schema = '"long"',
    func = 'flatten', input = '42', output = '[42]'
}

t {
    error  = 'Expecting LONG, encountered NIL',
    schema = '"long"',
    func = 'flatten', input = 'null'
}

t {
    error  = 'Expecting LONG, encountered FALSE',
    schema = '"long"',
    func = 'flatten', input = 'false'
}

t {
    error  = 'Expecting LONG, encountered TRUE',
    schema = '"long"',
    func = 'flatten', input = 'true'
}

t {
    error  = 'Expecting LONG, encountered DOUBLE',
    schema = '"long"',
    func = 'flatten', input = '42.0'
}

t {
    error  = 'Expecting LONG, encountered STR',
    schema = '"long"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error  = 'Expecting LONG, encountered ARRAY',
    schema = '"long"',
    func = 'flatten', input = '[42]' 
}

t {
    error  = 'Expecting LONG, encountered MAP',
    schema = '"long"',
    func = 'flatten', input = '{"key": 42}' 
}
