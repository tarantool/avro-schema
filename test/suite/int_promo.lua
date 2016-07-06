t {
    schema1 = '"int"', schema2 = '"long"',
    func = 'flatten', input = '42', output = '[42]'
}

t {
    schema1 = '"int"', schema2 = '"long"',
    func = 'flatten', input = '9000000', output = '[9000000]'
}

t {
    schema1 = '"int"', schema2 = '"long"',
    func = 'flatten', input = '-9000000', output = '[-9000000]'
}

t {
    schema1 = '"int"', schema2 = '"long"',
    func = 'flatten', input = '2147483647', output = '[2147483647]'
}

t {
    schema1 = '"int"', schema2 = '"long"',
    func = 'flatten', input = '-2147483648', output = '[-2147483648]'
}

--

t {
    schema1 = '"int"', schema2 = '"float"',
    func = 'flatten', input = '42', output = '![42.0]'
}

t {
    schema1 = '"int"', schema2 = '"float"',
    func = 'flatten', input = '9000000', output = '![9000000.0]'
}

t {
    schema1 = '"int"', schema2 = '"float"',
    func = 'flatten', input = '-9000000', output = '![-9000000.0]'
}

t {
    schema1 = '"int"', schema2 = '"float"',
    func = 'flatten', input = '2147483647', output = '![2147483647.0]'
}

t {
    schema1 = '"int"', schema2 = '"float"',
    func = 'flatten', input = '-2147483648', output = '![-2147483648.0]'
}

--

t {
    schema1 = '"int"', schema2 = '"double"',
    func = 'flatten', input = '42', output = '[42.0]'
}

t {
    schema1 = '"int"', schema2 = '"double"',
    func = 'flatten', input = '9000000', output = '[9000000.0]'
}

t {
    schema1 = '"int"', schema2 = '"double"',
    func = 'flatten', input = '-9000000', output = '[-9000000.0]'
}

t {
    schema1 = '"int"', schema2 = '"double"',
    func = 'flatten', input = '2147483647', output = '[2147483647.0]'
}

t {
    schema1 = '"int"', schema2 = '"double"',
    func = 'flatten', input = '-2147483648', output = '[-2147483648.0]'
}

--

t {
    compile_error = 'Types incompatible: long and int',
    schema1 = '"long"', schema2 = '"int"'
}

t {
    compile_error = 'Types incompatible: float and int',
    schema1 = '"float"', schema2 = '"int"'
}

t {
    compile_error = 'Types incompatible: double and int',
    schema1 = '"double"', schema2 = '"int"'
}

--

local schemas = {'"long"', '"float"', '"double"'}

for other_schema = 1,#schemas do

    _G['other_schema'] = other_schema

    t {
        error = 'Expecting INT, encountered FLOAT',
        schema1 = '"int"', schema2 = schemas[other_schema],
        func = 'flatten', input = '!9999.1'
    }

    t {
        error = 'Expecting INT, encountered DOUBLE',
        schema1 = '"int"', schema2 = schemas[other_schema],
        func = 'flatten', input = '9999.1'
    }

    t {
        error = 'Expecting INT, encountered NIL',
        schema1 = '"int"', schema2 = schemas[other_schema],
        func = 'flatten', input = 'null'
    }

    t {
        error = 'Value exceeds INT range: 2147483648LL',
        schema1 = '"int"', schema2 = schemas[other_schema],
        func = 'flatten', input = '2147483648'
    }

    t {
        error = 'Value exceeds INT range: -2147483649LL',
        schema1 = '"int"', schema2 = schemas[other_schema],
        func = 'flatten', input = '-2147483649'
    }

end
