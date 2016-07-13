t {
    schema1 = '"string"', schema2 = '"bytes"',
    func = 'flatten', input = '"Hello, world!"',
    output = '[{"$binary": "48656c6c6f2c20776f726c6421"}]'
}

t {
    error = "Expecting STR, encountered BIN",
    schema1 = '"string"', schema2 = '"bytes"',
    func = 'flatten',
    input = '{"$binary": "48656c6c6f2c20776f726c6421"}'
}

t {
    error = "Expecting STR, encountered NIL",
    schema1 = '"string"', schema2 = '"bytes"',
    func = 'flatten',
    input = 'null'
}
