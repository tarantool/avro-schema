local int_map = [[{
    "type": "map",
    "values": "int"
}]]

local string_map = [[{
    "type": "map",
    "values": "string"
}]]

local string_map_map = [[{
    "type": "map",
    "values": {
        "type": "map",
        "values": "string"
    }
}]]

local int_map_nullable = [[{
  "type": "map*",
  "values": "int"
}]]

-----------------------------------------------------------------------

t {
    schema = int_map,
    func = 'flatten',
    input = '{}', output = '[{}]'
}

t {
    schema = int_map,
    func = 'flatten',
    input  =  '{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10}',
    output = '[{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10}]',
}

t {
    schema = string_map,
    func = 'flatten',
    input = '{}', output = '[{}]'
}

t {
    schema = string_map,
    func = 'flatten',
    input  =  [[{
        "a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6",
        "g": "7", "h": "8", "i": "9", "j": "10"
    }]],
    output  =  [=[[{
        "a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6",
        "g": "7", "h": "8", "i": "9", "j": "10"
    }]]=]
}

t {
    schema = string_map_map,
    func = 'flatten',
    input = '{}', output = '[{}]'
}

t {
    schema = string_map_map,
    func = 'flatten',
    input  =  [[{
        "1":{}, "2":{"a": "1"}, "3":{"b": "2", "c": "3"},
        "4":{"d": "4", "e": "5", "f": "6"},
        "5":{"g": "7"}, "6":{"h": "8", "i": "9", "j": "10"}
    }]],
    output  = [=[[{
        "1":{}, "2":{"a": "1"}, "3":{"b": "2", "c": "3"},
        "4":{"d": "4", "e": "5", "f": "6"},
        "5":{"g": "7"}, "6":{"h": "8", "i": "9", "j": "10"}
    }]]=]
}

-----------------------------------------------------------------------

t {
    error  = 'Expecting MAP, encountered NIL',
    schema = int_map,
    func = 'flatten', input = 'null'
}

t {
    error  = 'Expecting MAP, encountered FALSE',
    schema = int_map,
    func = 'flatten', input = 'false'
}

t {
    error  = 'Expecting MAP, encountered TRUE',
    schema = int_map,
    func = 'flatten', input = 'true'
}

t {
    error  = 'Expecting MAP, encountered LONG',
    schema = int_map,
    func = 'flatten', input = '42'
}

t {
    error  = 'Expecting MAP, encountered FLOAT',
    schema = int_map,
    func = 'flatten', input = '! 42.0'
}

t {
    error  = 'Expecting MAP, encountered DOUBLE',
    schema = int_map,
    func = 'flatten', input = '42.0'
}

t {
    error  = 'Expecting MAP, encountered STR',
    schema = int_map,
    func = 'flatten', input = '"Hello, MAP!"'
}

t {
    error  = 'Expecting MAP, encountered BIN',
    schema = int_map,
    func = 'flatten', input = '{"$binary": "FFFF0055"}'
}

t {
    error  = 'Expecting MAP, encountered ARRAY',
    schema = int_map,
    func = 'flatten', input = '[42]'
}

t {
    error  = 'a: Expecting INT, encountered DOUBLE',
    schema = int_map,
    func = 'flatten', input = '{"a": 42.0}'
}

t {
    error  = 'e: Expecting INT, encountered DOUBLE',
    schema = int_map,
    func = 'flatten', input = '{"a":1, "b":2, "c":3, "d":4, "e":5.1}'
}

t {
    error  = 'a: Expecting STR, encountered LONG',
    schema = string_map,
    func = 'flatten', input = '{"a":42}'
}

t {
    error  = 'e: Expecting STR, encountered LONG',
    schema = string_map,
    func = 'flatten',
    input = '{"a":"1", "b":"2", "c":"3", "d":"4", "e":5}'
}

t {
    error = 'a: Expecting MAP, encountered LONG',
    schema = string_map_map,
    func = 'flatten', input = '{"a":1}'
}

t {
    error = 'a/b: Expecting STR, encountered LONG',
    schema = string_map_map,
    func = 'flatten', input = '{"a":{"b":1}}'
}

t {
    error = 'e: Expecting MAP, encountered LONG',
    schema = string_map_map,
    func = 'flatten', input = '{"a":{}, "b":{}, "c":{}, "d":{}, "e":1}'
}

t {
    error = 'e/f: Expecting STR, encountered LONG',
    schema = string_map_map,
    func = 'flatten', input = '{"a":{}, "b":{}, "c":{}, "d":{}, "e":{"f": 1}}'
}

t {
    schema = int_map_nullable,
    func = 'flatten',
    input  =  '{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10}',
    output = '[{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10}]'
}

t {
    schema = int_map_nullable,
    func = 'flatten',
    input  =  'null',
    output = '[null]'
}

t {
    schema = int_map_nullable,
    func = 'unflatten',
    input = '[{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10}]',
    output  =  '{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10}'
}

t {
    schema = int_map_nullable,
    func = 'unflatten',
    input = '[null]',
    output  =  'null'
}
