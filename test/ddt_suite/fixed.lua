local fixed4 = '{"type": "fixed", "size":4, "name": "fixed4"}'
local fixed8 = '{"type": "fixed", "size":8, "name": "fixed8"}'

t {
    schema = fixed4,
    func = 'flatten',
    input  =  '{"$binary": "89ABCDEF"}',
    output = '[{"$binary": "89ABCDEF"}]',
}

t {
    schema = fixed8,
    func = 'flatten',
    input  =  '{"$binary": "0102030405060700"}',
    output = '[{"$binary": "0102030405060700"}]',
}

-- validation errors
t {
    error = 'Expecting BIN, encountered NIL',
    schema = fixed8,
    func = 'flatten', input = 'null'
}

t {
    error = 'Expecting BIN, encountered FALSE',
    schema = fixed8,
    func = 'flatten', input = 'false'
}

t {
    error = 'Expecting BIN, encountered TRUE',
    schema = fixed8,
    func = 'flatten', input = 'true'
}

t {
    error = 'Expecting BIN, encountered LONG',
    schema = fixed8,
    func = 'flatten', input = '42'
}

-- ! is a hack to enable single-precision floating point
t {
    error = 'Expecting BIN, encountered FLOAT',
    schema = fixed8,
    func = 'flatten', input = '! 1.0'
}

t {
    error = 'Expecting BIN, encountered DOUBLE',
    schema = fixed8,
    func = 'flatten', input = '1.0'
}

t {
    error = 'Expecting BIN, encountered STR',
    schema = fixed8,
    func = 'flatten', input = '"Chunky bacon!"'
}

t {
    error = 'Expecting BIN, encountered ARRAY',
    schema = fixed8,
    func = 'flatten', input = '[]'
}

t {
    error = 'Expecting BIN, encountered MAP',
    schema = fixed8,
    func = 'flatten', input = '{}'
}

t {
    error = 'Expecting BIN of length 8. Encountered BIN of length 2.',
    schema = fixed8,
    func = 'flatten', input = '{"$binary": "FFFF"}'
}

t {
    error = 'Expecting BIN of length 8. Encountered BIN of length 0.',
    schema = fixed8,
    func = 'flatten', input = '{"$binary": ""}'
}

t {
    error = 'Expecting BIN of length 4. Encountered BIN of length 7.',
    schema = fixed4,
    func = 'flatten', input = '{"$binary": "FF00FF11AA22CC"}'
}

t {
    error = 'Expecting BIN of length 4. Encountered BIN of length 1.',
    schema = fixed4,
    func = 'flatten', input = '{"$binary": "55"}'
}
