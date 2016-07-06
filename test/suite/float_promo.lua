t {
    schema1 = '"float"', schema2 = '"double"',
    func = 'flatten', input = '42.25', output = '[42.25]'
}

t {
    schema1 = '"float"', schema2 = '"double"',
    func = 'flatten', input = '! 42.25', output = '[42.25]'
}

t {
    schema1 = '"float"', schema2 = '"double"',
    func = 'flatten', input = '42', output = '[42.0]'
}

--

t {
    error = "Expecting FLOAT, encountered NIL",
    schema1 = '"float"', schema2 = '"double"',
    func = 'flatten', input = 'null'
}

t {
    compile_error = 'Types incompatible: double and float',
    schema1 = '"double"', schema2 = '"float"'
}
