local int_array = [[{
    "type": "array",
    "items": "int"
}]]

local string_array = [[{
    "type": "array",
    "items": "string"
}]]

local string_array_array = [[{
    "type": "array",
    "items": {
        "type": "array",
        "items": "string"
    }
}]]

local string_array_nullable = [[{
    "type": "array*",
    "items": "string"
}]]

local string_array_items_nullable = [[{
    "type": "array",
    "items": "string*"
}]]
-----------------------------------------------------------------------

t {
    schema = int_array,
    func = 'flatten',
    input = '[]', output = '[[]]'
}

t {
    schema = int_array,
    func = 'flatten',
    input  = '[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]',
    output = '[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]'
}

t {
    schema = string_array,
    func = 'flatten',
    input = '[]', output = '[[]]'
}

t {
    schema = string_array,
    func = 'flatten',
    input  =  '["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]',
    output = '[["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]]'
}

t {
    schema = string_array_array,
    func = 'flatten',
    input = '[]', output = '[[]]'
}

t {
    schema = string_array_array,
    func = 'flatten',
    input  =  '[[], ["1"], ["2", "3"], ["4", "5", "6"], ["7"], ["8", "9", "10"]]',
    output = '[[[], ["1"], ["2", "3"], ["4", "5", "6"], ["7"], ["8", "9", "10"]]]'
}

-----------------------------------------------------------------------

t {
    error  = 'Expecting ARRAY, encountered NIL',
    schema = int_array,
    func = 'flatten', input = 'null'
}

t {
    error  = 'Expecting ARRAY, encountered FALSE',
    schema = int_array,
    func = 'flatten', input = 'false'
}

t {
    error  = 'Expecting ARRAY, encountered TRUE',
    schema = int_array,
    func = 'flatten', input = 'true'
}

t {
    error  = 'Expecting ARRAY, encountered LONG',
    schema = int_array,
    func = 'flatten', input = '42'
}

t {
    error  = 'Expecting ARRAY, encountered FLOAT',
    schema = int_array,
    func = 'flatten', input = '! 42.0'
}

t {
    error  = 'Expecting ARRAY, encountered DOUBLE',
    schema = int_array,
    func = 'flatten', input = '42.0'
}

t {
    error  = 'Expecting ARRAY, encountered STR',
    schema = int_array,
    func = 'flatten', input = '"Hello, array!"'
}

t {
    error  = 'Expecting ARRAY, encountered BIN',
    schema = int_array,
    func = 'flatten', input = '{"$binary": "FFFF0055"}'
}

t {
    error  = 'Expecting ARRAY, encountered MAP',
    schema = int_array,
    func = 'flatten', input = '{"key": 42}'
}

t {
    error  = '1: Expecting INT, encountered DOUBLE',
    schema = int_array,
    func = 'flatten', input = '[42.0]'
}

t {
    error  = '5: Expecting INT, encountered DOUBLE',
    schema = int_array,
    func = 'flatten', input = '[1, 2, 3, 4, 5.1]'
}

t {
    error  = '1: Expecting STR, encountered LONG',
    schema = string_array,
    func = 'flatten', input = '[42]'
}

t {
    error  = '5: Expecting STR, encountered LONG',
    schema = string_array,
    func = 'flatten', input = '["1", "2", "3", "4", 5]'
}


t {
    error = '1: Expecting ARRAY, encountered LONG',
    schema = string_array_array,
    func = 'flatten', input = '[1]'
}

t {
    error = '1/1: Expecting STR, encountered LONG',
    schema = string_array_array,
    func = 'flatten', input = '[[1]]'
}

t {
    error = '5: Expecting ARRAY, encountered LONG',
    schema = string_array_array,
    func = 'flatten', input = '[[],[],[],[],1]'
}

t {
    error = '5/1: Expecting STR, encountered LONG',
    schema = string_array_array,
    func = 'flatten', input = '[[],[],[],[],[1]]'
}


t {
    schema = string_array_nullable,
    func = 'flatten',
    input = '["hello", "world"]', output = '[["hello", "world"]]'
}

t {
    schema = string_array_nullable,
    func = 'flatten',
    input = 'null', output = '[null]'
}

t {
    schema = string_array_nullable,
    func = 'unflatten',
    input = '[["hello", "world"]]', output = '["hello", "world"]'
}

t {
    schema = string_array_nullable,
    func = 'unflatten',
    input = '[null]', output = 'null'
}

t {
    schema = string_array_items_nullable,
    func = 'flatten',
    input = '[null, null]', output = '[[null, null]]'
}

t {
    schema = string_array_items_nullable,
    func = 'unflatten',
    input = '[[null, null]]', output = '[null, null]'
}

t {
    schema = string_array_items_nullable,
    func = 'flatten',
    input = '[null, "hello"]', output = '[[null, "hello"]]'
}

t {
    schema = string_array_items_nullable,
    func = 'unflatten',
    input = '[[null, "hello"]]', output = '[null, "hello"]'
}
