local foo = [[{
    "name": "foo",
    "type": "record",
    "fields": [
        {"name": "A", "type": "string"},
        {"name": "B", "type": ["null", "string"]},
        {"name": "C", "type": "int"}
    ]
}]]

t {
    schema = foo,
    func = 'flatten',
    input = '{"A":"Hello, world!","B":null,"C":42}',
    output = '["Hello, world!", 0, null, 42]'
}

t {
    schema = foo,
    func = 'flatten',
    input = '{"A":"Hello, world!","B":{"string":"OLOLO"},"C":42}',
    output = '["Hello, world!", 1, "OLOLO", 42]'
}

--

t {
    schema = foo,
    func = 'unflatten',
    input = '["Hello, world!", 0, null, 42]',
    output = '{"A":"Hello, world!","B":null,"C":42}'
}

t {
    schema = foo,
    func = 'unflatten',
    input = '["Hello, world!", 1, "OLOLO", 42]',
    output = '{"A":"Hello, world!","B":{"string":"OLOLO"},"C":42}'
}

--

t {
    schema = foo,
    func = 'xflatten',
    input = '{"A":"Hello, world!"}',
    output = '[["=",1,"Hello, world!"]]'
}

t {
    schema = foo,
    func = 'xflatten',
    input = '{"B":null}',
    output = '[["=",2,0],["=",3,null]]'
}

t {
    schema = foo,
    func = 'xflatten',
    input = '{"B":{"string":"OLOLO"}}',
    output = '[["=",2,1],["=",3,"OLOLO"]]'
}

t {
    schema = foo,
    func = 'xflatten',
    input = '{"C":42}',
    output = '[["=",4,42]]'
}

-----------------------------------------------------------------------

local foo = [[{
    "name": "foo",
    "type": "record",
    "fields": [
        {"name": "A", "type": "string"},
        {"name": "B", "type": {
            "name": "nested", "type": "record", "fields": [
                {"name":"_","type":["null", "string"]}
            ]
        }},
        {"name": "C", "type": "int"}
    ]
}]]

t {
    schema = foo,
    func = 'flatten',
    input = '{"A":"Hello, world!","B":{"_":null},"C":42}',
    output = '["Hello, world!", 0, null, 42]'
}

t {
    schema = foo,
    func = 'flatten',
    input = '{"A":"Hello, world!","B":{"_":{"string":"OLOLO"}},"C":42}',
    output = '["Hello, world!", 1, "OLOLO", 42]'
}

--

t {
    schema = foo,
    func = 'unflatten',
    input = '["Hello, world!", 0, null, 42]',
    output = '{"A":"Hello, world!","B":{"_":null},"C":42}'
}

t {
    schema = foo,
    func = 'unflatten',
    input = '["Hello, world!", 1, "OLOLO", 42]',
    output = '{"A":"Hello, world!","B":{"_":{"string":"OLOLO"}},"C":42}'
}

--

t {
    schema = foo,
    func = 'xflatten',
    input = '{"A":"Hello, world!"}',
    output = '[["=",1,"Hello, world!"]]'
}

t {
    schema = foo,
    func = 'xflatten',
    input = '{"B":{"_":null}}',
    output = '[["=",2,0],["=",3,null]]'
}

t {
    schema = foo,
    func = 'xflatten',
    input = '{"B":{"_":{"string":"OLOLO"}}}',
    output = '[["=",2,1],["=",3,"OLOLO"]]'
}

t {
    schema = foo,
    func = 'xflatten',
    input = '{"C":42}',
    output = '[["=",4,42]]'
}
