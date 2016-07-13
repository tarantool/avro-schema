t {
    schema1 = '"bytes"', schema2 = '"string"',
    func = 'flatten', input = '{"$binary": "48656c6c6f2c20776f726c6421"}',
    output = '["Hello, world!"]'
}

t {
    error = "Expecting BIN, encountered STR",
    schema1 = '"bytes"', schema2 = '"string"',
    func = 'flatten', input = '"Hello, world!"'
}

t {
    error = "Expecting BIN, encountered NIL",
    schema1 = '"bytes"', schema2 = '"string"',
    func = 'flatten', input = 'null'
}
