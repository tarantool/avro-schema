t {
    schema1 = '"long"', schema2 = '"float"',
    func = 'flatten', input = '9999', output = '![9999.0]'
}

t {
    schema1 = '"long"', schema2 = '"double"',
    func = 'flatten', input = '9999', output = '[9999.0]'
}

--

t {
    compile_error = 'Types incompatible: float and long',
    schema1 = '"float"', schema2 = '"long"'
}

t {
    compile_error = 'Types incompatible: double and long',
    schema1 = '"double"', schema2 = '"long"'
}

--

local schemas = {'"float"', '"double"'}

for other_schema = 1,#schemas do

    _G['other_schema'] = other_schema

    t {
        error = 'Expecting LONG, encountered FLOAT',
        schema1 = '"long"', schema2 = schemas[other_schema],
        func = 'flatten', input = '!9999.1'
    }

    t {
        error = 'Expecting LONG, encountered DOUBLE',
        schema1 = '"long"', schema2 = schemas[other_schema],
        func = 'flatten', input = '9999.1'
    }

    t {
        error = 'Expecting LONG, encountered NIL',
        schema1 = '"long"', schema2 = schemas[other_schema],
        func = 'flatten', input = 'null'
    }

end
