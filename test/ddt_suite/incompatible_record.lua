t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ]
    }]],
    schema2 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"string"}
        ]
    }]],
    compile_error = 'foo/A: Types incompatible: int and string'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ]
    }]],
    schema2 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"B", "type":"string"}
        ]
    }]],
    compile_error = 'foo: Field B is missing in source schema, and no default value was provided'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ]
    }]],
    schema2 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"B", "type":"string", "aliases":["A"]}
        ]
    }]],
    compile_error = 'foo/(A aka B): Types incompatible: int and string'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int", "aliases":["B"]}
        ]
    }]],
    schema2 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"B", "type":"string"}
        ]
    }]],
    compile_error = 'foo: Field B is missing in source schema, and no default value was provided'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int", "aliases":["B"]}
        ]
    }]],
    schema2 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"B", "type":"string"}
        ]
    }]],
    compile_downgrade = true,
    compile_error = 'foo/(A aka B): Types incompatible: int and string'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ]
    }]],
    schema2 = [[{
        "name": "xfoo", "type": "record", "fields": [
            {"name":"A", "type":"string"}
        ]
    }]],
    compile_error = 'Types incompatible: foo and xfoo'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ]
    }]],
    schema2 = [[{
        "name": "xfoo", "type": "record", "fields": [
            {"name":"A", "type":"string"}
        ], "aliases": ["foo"]
    }]],
    compile_error = '(foo aka xfoo)/A: Types incompatible: int and string'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ], "aliases": ["xfoo"]
    }]],
    schema2 = [[{
        "name": "xfoo", "type": "record", "fields": [
            {"name":"A", "type":"string"}
        ]
    }]],
    compile_error = 'Types incompatible: foo and xfoo'
}

t {
    schema1 = [[{
        "name": "foo", "type": "record", "fields": [
            {"name":"A", "type":"int"}
        ], "aliases": ["xfoo"]
    }]],
    schema2 = [[{
        "name": "xfoo", "type": "record", "fields": [
            {"name":"A", "type":"string"}
        ]
    }]],
    compile_error = '(foo aka xfoo)/A: Types incompatible: int and string',
    compile_downgrade = true
}
