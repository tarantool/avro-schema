local foo = [[{
    "name": "foo",
    "type": "record",
    "fields": [
        {"name": "A", "type": "int", "default": 1001},
        {"name": "B", "type": "int", "default": 1002},
        {"name": "C", "type": "int", "default": 1003},
        {"name": "D", "type": "int", "default": 1004}
    ]
}]]

local foo_reversed = [[{
    "name": "foo",
    "type": "record",
    "fields": [
        {"name": "D", "type": "int", "default": 1005},
        {"name": "C", "type": "int", "default": 1006},
        {"name": "B", "type": "int", "default": 1007},
        {"name": "A", "type": "int", "default": 1008}
    ]
}]]

local foo_reduced = [[{
    "name": "foo",
    "type": "record",
    "fields": [
        {"name": "A", "type": "int", "default": 1001},
        {"name": "B", "type": "int", "default": 1002}
    ]
}]]

t {
    schema1 = foo, schema2 = foo_reversed,
    func = 'flatten',
    input = '{"A":1, "B":2, "C":3, "D":4}',
    output = '[4,3,2,1]'
}

t {
    schema1 = foo, schema2 = foo_reversed,
    func = 'unflatten',
    input = '[1,2,3,4]',
    output = '{"D":4, "C":3, "B":2, "A":1}'
}

--

t {
    schema1 = foo_reduced, schema2 = foo,
    func = 'flatten',
    input = '{"A":1, "B":2}',
    output = '[1,2,1003,1004]'
}

t {
    schema1 = foo_reduced, schema2 = foo_reversed,
    func = 'flatten',
    input = '{"A":1, "B":2}',
    output = '[1005,1006,2,1]'
}

t {
    schema1 = foo, schema2 = foo_reduced,
    func = 'flatten',
    input = '{"A":1, "B":2, "C":3, "D":4}',
    output = '[1,2]'
}

t {
    error = 'C: Expecting INT, encountered STR',
    schema1 = foo, schema2 = foo_reduced,
    func = 'flatten',
    input = '{"A":1, "B":2, "C":"Hello, world!"}'
}

--

t {
    schema1 = foo_reduced, schema2 = foo,
    func = 'unflatten',
    input = '[1,2]',
    output = '{"A":1, "B":2, "C":1003, "D":1004}'
}

t {
    schema1 = foo_reduced, schema2 = foo_reversed,
    func = 'unflatten',
    input = '[1,2]',
    output = '{"D":1005, "C":1006, "B":2, "A":1}'
}

t {
    schema1 = foo, schema2 = foo_reduced,
    func = 'unflatten',
    input = '[1,2,3,4]',
    output = '{"A":1, "B":2}'
}

t {
    error = '4: Expecting INT, encountered STR',
    schema1 = foo, schema2 = foo_reduced,
    func = 'unflatten',
    input = '[1,2,3,"Hello, world!"]'
}

--

t {
    schema1 = foo, schema2 = foo_reversed,
    func = 'xflatten',
    input = '{"A":100, "B":200, "C":300, "D":400}',
    output = '[["=",4,100],["=",3,200],["=",2,300],["=",1,400]]'
}

t {
    schema1 = foo, schema2 = foo_reduced,
    func = 'xflatten',
    input = '{"A":100, "B":200, "C":300, "D":400}',
    output = '[["=",1,100],["=",2,200]]'
}


t {
    schema1 = foo_reversed, schema2 = foo_reduced,
    func = 'xflatten',
    input = '{"A":100, "B":200, "C":300, "D":400}',
    output = '[["=",1,100],["=",2,200]]'
}

t {
    schema1 = foo_reduced, schema2 = foo,
    func = 'xflatten',
    input = '{"A":100, "B":200}',
    output = '[["=",1,100],["=",2,200]]'
}

t {
    schema1 = foo_reduced, schema2 = foo_reversed,
    func = 'xflatten',
    input = '{"A":100, "B":200}',
    output = '[["=",4,100],["=",3,200]]'
}
