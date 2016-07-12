local array =  '{"type":"array","items":"int"}'
local map =    '{"type":"map", "values":"int"}'
local fixed =  '{"name":"foo", "type":"fixed", "size":16}'
local record = '{"name":"foo", "type":"record", "fields":[{"name":"_","type":"int"}]}'
local enum   = '{"name":"foo", "type":"enum", "symbols":["_"]}'

-- null vs. X

t {
    schema1 = '"null"', schema2 = '"null"',
    compile_only = true
}

t {
    schema1 = '"null"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: null and boolean'
}

t {
    schema1 = '"null"', schema2 = '"int"',
    compile_error = 'Types incompatible: null and int'
}

t {
    schema1 = '"null"', schema2 = '"long"',
    compile_error = 'Types incompatible: null and long'
}

t {
    schema1 = '"null"', schema2 = '"float"',
    compile_error = 'Types incompatible: null and float'
}

t {
    schema1 = '"null"', schema2 = '"double"',
    compile_error = 'Types incompatible: null and double'
}

t {
    schema1 = '"null"', schema2 = '"string"',
    compile_error = 'Types incompatible: null and string'
}

t {
    schema1 = '"null"', schema2 = '"bytes"',
    compile_error = 'Types incompatible: null and bytes'
}

t {
    schema1 = '"null"', schema2 = array,
    compile_error = 'Types incompatible: null and array'
}

t {
    schema1 = '"null"', schema2 = map,
    compile_error = 'Types incompatible: null and map'
}

t {
    schema1 = '"null"', schema2 = fixed,
    compile_error = 'Types incompatible: null and foo'
}

t {
    schema1 = '"null"', schema2 = record,
    compile_error = 'Types incompatible: null and foo'
}

t {
    schema1 = '"null"', schema2 = enum,
    compile_error = 'Types incompatible: null and foo'
}

t {
    schema1 = '"null"', schema2 = '["int","string"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"null"', schema2 = '["null","string"]',
    compile_only = true
}

-- boolean vs. X

t {
    schema1 = '"boolean"', schema2 = '"null"',
    compile_error = 'Types incompatible: boolean and null'
}

t {
    schema1 = '"boolean"', schema2 = '"boolean"',
    compile_only = true
}

t {
    schema1 = '"boolean"', schema2 = '"int"',
    compile_error = 'Types incompatible: boolean and int'
}

t {
    schema1 = '"boolean"', schema2 = '"long"',
    compile_error = 'Types incompatible: boolean and long'
}

t {
    schema1 = '"boolean"', schema2 = '"float"',
    compile_error = 'Types incompatible: boolean and float'
}

t {
    schema1 = '"boolean"', schema2 = '"double"',
    compile_error = 'Types incompatible: boolean and double'
}

t {
    schema1 = '"boolean"', schema2 = '"string"',
    compile_error = 'Types incompatible: boolean and string'
}

t {
    schema1 = '"boolean"', schema2 = '"bytes"',
    compile_error = 'Types incompatible: boolean and bytes'
}

t {
    schema1 = '"boolean"', schema2 = array,
    compile_error = 'Types incompatible: boolean and array'
}

t {
    schema1 = '"boolean"', schema2 = map,
    compile_error = 'Types incompatible: boolean and map'
}

t {
    schema1 = '"boolean"', schema2 = fixed,
    compile_error = 'Types incompatible: boolean and foo'
}

t {
    schema1 = '"boolean"', schema2 = record,
    compile_error = 'Types incompatible: boolean and foo'
}

t {
    schema1 = '"boolean"', schema2 = enum,
    compile_error = 'Types incompatible: boolean and foo'
}

t {
    schema1 = '"boolean"', schema2 = '["int","string"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"boolean"', schema2 = '["null","boolean"]',
    compile_only = true
}

-- int vs. X

t {
    schema1 = '"int"', schema2 = '"null"',
    compile_error = 'Types incompatible: int and null'
}

t {
    schema1 = '"int"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: int and boolean'
}

t {
    schema1 = '"int"', schema2 = '"int"',
    compile_only = true
}

t {
    schema1 = '"int"', schema2 = '"long"',
    compile_only = true
}

t {
    schema1 = '"int"', schema2 = '"float"',
    compile_only = true
}

t {
    schema1 = '"int"', schema2 = '"double"',
    compile_only = true
}

t {
    schema1 = '"int"', schema2 = '"string"',
    compile_error = 'Types incompatible: int and string'
}

t {
    schema1 = '"int"', schema2 = '"bytes"',
    compile_error = 'Types incompatible: int and bytes'
}

t {
    schema1 = '"int"', schema2 = array,
    compile_error = 'Types incompatible: int and array'
}

t {
    schema1 = '"int"', schema2 = map,
    compile_error = 'Types incompatible: int and map'
}

t {
    schema1 = '"int"', schema2 = fixed,
    compile_error = 'Types incompatible: int and foo'
}

t {
    schema1 = '"int"', schema2 = record,
    compile_error = 'Types incompatible: int and foo'
}

t {
    schema1 = '"int"', schema2 = enum,
    compile_error = 'Types incompatible: int and foo'
}

t {
    schema1 = '"int"', schema2 = '["null","string"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"int"', schema2 = '["int","string"]',
    compile_only = true
}

-- long vs. X

t {
    schema1 = '"long"', schema2 = '"null"',
    compile_error = 'Types incompatible: long and null'
}

t {
    schema1 = '"long"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: long and boolean'
}

t {
    schema1 = '"long"', schema2 = '"int"',
    compile_error = 'Types incompatible: long and int'
}

t {
    schema1 = '"long"', schema2 = '"long"',
    compile_only = true
}

t {
    schema1 = '"long"', schema2 = '"float"',
    compile_only = true
}

t {
    schema1 = '"long"', schema2 = '"double"',
    compile_only = true
}

t {
    schema1 = '"long"', schema2 = '"string"',
    compile_error = 'Types incompatible: long and string'
}

t {
    schema1 = '"long"', schema2 = '"bytes"',
    compile_error = 'Types incompatible: long and bytes'
}

t {
    schema1 = '"long"', schema2 = array,
    compile_error = 'Types incompatible: long and array'
}

t {
    schema1 = '"long"', schema2 = map,
    compile_error = 'Types incompatible: long and map'
}

t {
    schema1 = '"long"', schema2 = fixed,
    compile_error = 'Types incompatible: long and foo'
}

t {
    schema1 = '"long"', schema2 = record,
    compile_error = 'Types incompatible: long and foo'
}

t {
    schema1 = '"long"', schema2 = enum,
    compile_error = 'Types incompatible: long and foo'
}

t {
    schema1 = '"long"', schema2 = '["null","string"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"long"', schema2 = '["null","long"]',
    compile_only = true
}

-- float vs. X

t {
    schema1 = '"float"', schema2 = '"null"',
    compile_error = 'Types incompatible: float and null'
}

t {
    schema1 = '"float"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: float and boolean'
}

t {
    schema1 = '"float"', schema2 = '"int"',
    compile_error = 'Types incompatible: float and int'
}

t {
    schema1 = '"float"', schema2 = '"long"',
    compile_error = 'Types incompatible: float and long'
}

t {
    schema1 = '"float"', schema2 = '"float"',
    compile_only = true
}

t {
    schema1 = '"float"', schema2 = '"double"',
    compile_only = true
}

t {
    schema1 = '"float"', schema2 = '"string"',
    compile_error = 'Types incompatible: float and string'
}

t {
    schema1 = '"float"', schema2 = '"bytes"',
    compile_error = 'Types incompatible: float and bytes'
}

t {
    schema1 = '"float"', schema2 = array,
    compile_error = 'Types incompatible: float and array'
}

t {
    schema1 = '"float"', schema2 = map,
    compile_error = 'Types incompatible: float and map'
}

t {
    schema1 = '"float"', schema2 = fixed,
    compile_error = 'Types incompatible: float and foo'
}

t {
    schema1 = '"float"', schema2 = record,
    compile_error = 'Types incompatible: float and foo'
}

t {
    schema1 = '"float"', schema2 = enum,
    compile_error = 'Types incompatible: float and foo'
}

t {
    schema1 = '"float"', schema2 = '["int","string"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"float"', schema2 = '["null","float"]',
    compile_only = true
}

-- double vs. X

t {
    schema1 = '"double"', schema2 = '"null"',
    compile_error = 'Types incompatible: double and null'
}

t {
    schema1 = '"double"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: double and boolean'
}

t {
    schema1 = '"double"', schema2 = '"int"',
    compile_error = 'Types incompatible: double and int'
}

t {
    schema1 = '"double"', schema2 = '"long"',
    compile_error = 'Types incompatible: double and long'
}

t {
    schema1 = '"double"', schema2 = '"float"',
    compile_error = 'Types incompatible: double and float'
}

t {
    schema1 = '"double"', schema2 = '"double"',
    compile_only = true
}

t {
    schema1 = '"double"', schema2 = '"string"',
    compile_error = 'Types incompatible: double and string'
}

t {
    schema1 = '"double"', schema2 = '"bytes"',
    compile_error = 'Types incompatible: double and bytes'
}

t {
    schema1 = '"double"', schema2 = array,
    compile_error = 'Types incompatible: double and array'
}

t {
    schema1 = '"double"', schema2 = map,
    compile_error = 'Types incompatible: double and map'
}

t {
    schema1 = '"double"', schema2 = fixed,
    compile_error = 'Types incompatible: double and foo'
}

t {
    schema1 = '"double"', schema2 = record,
    compile_error = 'Types incompatible: double and foo'
}

t {
    schema1 = '"double"', schema2 = enum,
    compile_error = 'Types incompatible: double and foo'
}

t {
    schema1 = '"double"', schema2 = '["int","string"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"double"', schema2 = '["null","double"]',
    compile_only = true
}

-- string vs. X

t {
    schema1 = '"string"', schema2 = '"null"',
    compile_error = 'Types incompatible: string and null'
}

t {
    schema1 = '"string"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: string and boolean'
}

t {
    schema1 = '"string"', schema2 = '"int"',
    compile_error = 'Types incompatible: string and int'
}

t {
    schema1 = '"string"', schema2 = '"long"',
    compile_error = 'Types incompatible: string and long'
}

t {
    schema1 = '"string"', schema2 = '"float"',
    compile_error = 'Types incompatible: string and float'
}

t {
    schema1 = '"string"', schema2 = '"double"',
    compile_error = 'Types incompatible: string and double'
}

t {
    schema1 = '"string"', schema2 = '"string"',
    compile_only = true
}

t {
    schema1 = '"string"', schema2 = '"bytes"',
    compile_only = true
}

t {
    schema1 = '"string"', schema2 = array,
    compile_error = 'Types incompatible: string and array'
}

t {
    schema1 = '"string"', schema2 = map,
    compile_error = 'Types incompatible: string and map'
}

t {
    schema1 = '"string"', schema2 = fixed,
    compile_error = 'Types incompatible: string and foo'
}

t {
    schema1 = '"string"', schema2 = record,
    compile_error = 'Types incompatible: string and foo'
}

t {
    schema1 = '"string"', schema2 = enum,
    compile_error = 'Types incompatible: string and foo'
}

t {
    schema1 = '"string"', schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"string"', schema2 = '["null","string"]',
    compile_only = true
}

-- bytes vs. X

t {
    schema1 = '"bytes"', schema2 = '"null"',
    compile_error = 'Types incompatible: bytes and null'
}

t {
    schema1 = '"bytes"', schema2 = '"boolean"',
    compile_error = 'Types incompatible: bytes and boolean'
}

t {
    schema1 = '"bytes"', schema2 = '"int"',
    compile_error = 'Types incompatible: bytes and int'
}

t {
    schema1 = '"bytes"', schema2 = '"long"',
    compile_error = 'Types incompatible: bytes and long'
}

t {
    schema1 = '"bytes"', schema2 = '"float"',
    compile_error = 'Types incompatible: bytes and float'
}

t {
    schema1 = '"bytes"', schema2 = '"double"',
    compile_error = 'Types incompatible: bytes and double'
}

t {
    schema1 = '"bytes"', schema2 = '"string"',
    compile_only = true
}

t {
    schema1 = '"bytes"', schema2 = '"bytes"',
    compile_only = true
}

t {
    schema1 = '"bytes"', schema2 = array,
    compile_error = 'Types incompatible: bytes and array'
}

t {
    schema1 = '"bytes"', schema2 = map,
    compile_error = 'Types incompatible: bytes and map'
}

t {
    schema1 = '"bytes"', schema2 = fixed,
    compile_error = 'Types incompatible: bytes and foo'
}

t {
    schema1 = '"bytes"', schema2 = record,
    compile_error = 'Types incompatible: bytes and foo'
}

t {
    schema1 = '"bytes"', schema2 = enum,
    compile_error = 'Types incompatible: bytes and foo'
}

t {
    schema1 = '"bytes"', schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '"bytes"', schema2 = '["int","bytes"]',
    compile_only = true
}

-- array vs. X

t {
    schema1 = array, schema2 = '"null"',
    compile_error = 'Types incompatible: array and null'
}

t {
    schema1 = array, schema2 = '"boolean"',
    compile_error = 'Types incompatible: array and boolean'
}

t {
    schema1 = array, schema2 = '"int"',
    compile_error = 'Types incompatible: array and int'
}

t {
    schema1 = array, schema2 = '"long"',
    compile_error = 'Types incompatible: array and long'
}

t {
    schema1 = array, schema2 = '"float"',
    compile_error = 'Types incompatible: array and float'
}

t {
    schema1 = array, schema2 = '"double"',
    compile_error = 'Types incompatible: array and double'
}

t {
    schema1 = array, schema2 = '"string"',
    compile_error = 'Types incompatible: array and string'
}

t {
    schema1 = array, schema2 = '"bytes"',
    compile_error = 'Types incompatible: array and bytes'
}

t {
    schema1 = array, schema2 = array,
    compile_only = true
}

t {
    schema1 = array, schema2 = map,
    compile_error = 'Types incompatible: array and map'
}

t {
    schema1 = array, schema2 = fixed,
    compile_error = 'Types incompatible: array and foo'
}

t {
    schema1 = array, schema2 = record,
    compile_error = 'Types incompatible: array and foo'
}

t {
    schema1 = array, schema2 = enum,
    compile_error = 'Types incompatible: array and foo'
}

t {
    schema1 = array, schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = array, schema2 = '["null",'..array..']',
    compile_only = true
}

-- map vs. X

t {
    schema1 = map, schema2 = '"null"',
    compile_error = 'Types incompatible: map and null'
}

t {
    schema1 = map, schema2 = '"boolean"',
    compile_error = 'Types incompatible: map and boolean'
}

t {
    schema1 = map, schema2 = '"int"',
    compile_error = 'Types incompatible: map and int'
}

t {
    schema1 = map, schema2 = '"long"',
    compile_error = 'Types incompatible: map and long'
}

t {
    schema1 = map, schema2 = '"float"',
    compile_error = 'Types incompatible: map and float'
}

t {
    schema1 = map, schema2 = '"double"',
    compile_error = 'Types incompatible: map and double'
}

t {
    schema1 = map, schema2 = '"string"',
    compile_error = 'Types incompatible: map and string'
}

t {
    schema1 = map, schema2 = '"bytes"',
    compile_error = 'Types incompatible: map and bytes'
}

t {
    schema1 = map, schema2 = array,
    compile_error = 'Types incompatible: map and array'
}

t {
    schema1 = map, schema2 = map,
    compile_only = true
}

t {
    schema1 = map, schema2 = fixed,
    compile_error = 'Types incompatible: map and foo'
}

t {
    schema1 = map, schema2 = record,
    compile_error = 'Types incompatible: map and foo'
}

t {
    schema1 = map, schema2 = enum,
    compile_error = 'Types incompatible: map and foo'
}

t {
    schema1 = map, schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = map, schema2 = '["null",'..map..']',
    compile_only = true
}

-- fixed vs. X

t {
    schema1 = fixed, schema2 = '"null"',
    compile_error = 'Types incompatible: foo and null'
}

t {
    schema1 = fixed, schema2 = '"boolean"',
    compile_error = 'Types incompatible: foo and boolean'
}

t {
    schema1 = fixed, schema2 = '"int"',
    compile_error = 'Types incompatible: foo and int'
}

t {
    schema1 = fixed, schema2 = '"long"',
    compile_error = 'Types incompatible: foo and long'
}

t {
    schema1 = fixed, schema2 = '"float"',
    compile_error = 'Types incompatible: foo and float'
}

t {
    schema1 = fixed, schema2 = '"double"',
    compile_error = 'Types incompatible: foo and double'
}

t {
    schema1 = fixed, schema2 = '"string"',
    compile_error = 'Types incompatible: foo and string'
}

t {
    schema1 = fixed, schema2 = '"bytes"',
    compile_error = 'Types incompatible: foo and bytes'
}

t {
    schema1 = fixed, schema2 = array,
    compile_error = 'Types incompatible: foo and array'
}

t {
    schema1 = fixed, schema2 = map,
    compile_error = 'Types incompatible: foo and map'
}

t {
    schema1 = fixed, schema2 = fixed,
    compile_only = true
}

t {
    schema1 = fixed, schema2 = record,
    compile_error = 'Types incompatible: foo and foo'
}

t {
    schema1 = fixed, schema2 = enum,
    compile_error = 'Types incompatible: foo and foo'
}

t {
    schema1 = fixed, schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = fixed, schema2 = '["null",'..fixed..']',
    compile_only = true
}

-- enum vs. X

t {
    schema1 = enum, schema2 = '"null"',
    compile_error = 'Types incompatible: foo and null'
}

t {
    schema1 = enum, schema2 = '"boolean"',
    compile_error = 'Types incompatible: foo and boolean'
}

t {
    schema1 = enum, schema2 = '"int"',
    compile_error = 'Types incompatible: foo and int'
}

t {
    schema1 = enum, schema2 = '"long"',
    compile_error = 'Types incompatible: foo and long'
}

t {
    schema1 = enum, schema2 = '"float"',
    compile_error = 'Types incompatible: foo and float'
}

t {
    schema1 = enum, schema2 = '"double"',
    compile_error = 'Types incompatible: foo and double'
}

t {
    schema1 = enum, schema2 = '"string"',
    compile_error = 'Types incompatible: foo and string'
}

t {
    schema1 = enum, schema2 = '"bytes"',
    compile_error = 'Types incompatible: foo and bytes'
}

t {
    schema1 = enum, schema2 = array,
    compile_error = 'Types incompatible: foo and array'
}

t {
    schema1 = enum, schema2 = map,
    compile_error = 'Types incompatible: foo and map'
}

t {
    schema1 = enum, schema2 = fixed,
    compile_error = 'Types incompatible: foo and foo'
}

t {
    schema1 = enum, schema2 = record,
    compile_error = 'Types incompatible: foo and foo'
}

t {
    schema1 = enum, schema2 = enum,
    compile_only = true
}

t {
    schema1 = enum, schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = enum, schema2 = '["null",'..enum..']',
    compile_only = true
}

-- record vs. X

t {
    schema1 = record, schema2 = '"null"',
    compile_error = 'Types incompatible: foo and null'
}

t {
    schema1 = record, schema2 = '"boolean"',
    compile_error = 'Types incompatible: foo and boolean'
}

t {
    schema1 = record, schema2 = '"int"',
    compile_error = 'Types incompatible: foo and int'
}

t {
    schema1 = record, schema2 = '"long"',
    compile_error = 'Types incompatible: foo and long'
}

t {
    schema1 = record, schema2 = '"float"',
    compile_error = 'Types incompatible: foo and float'
}

t {
    schema1 = record, schema2 = '"double"',
    compile_error = 'Types incompatible: foo and double'
}

t {
    schema1 = record, schema2 = '"string"',
    compile_error = 'Types incompatible: foo and string'
}

t {
    schema1 = record, schema2 = '"bytes"',
    compile_error = 'Types incompatible: foo and bytes'
}

t {
    schema1 = record, schema2 = array,
    compile_error = 'Types incompatible: foo and array'
}

t {
    schema1 = record, schema2 = map,
    compile_error = 'Types incompatible: foo and map'
}

t {
    schema1 = record, schema2 = fixed,
    compile_error = 'Types incompatible: foo and foo'
}

t {
    schema1 = record, schema2 = record,
    compile_only = true
}

t {
    schema1 = record, schema2 = enum,
    compile_error = 'Types incompatible: foo and foo'
}

t {
    schema1 = record, schema2 = '["null","int"]',
    compile_error = '<union>: No common types'
}

t {
    schema1 = record, schema2 = '["null",'..record..']',
    compile_only = true
}

-- union vs. X

t {
    schema1 = '["int"]', schema2 = '"null"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","int"]', schema2 = '"null"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"boolean"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","boolean"]', schema2 = '"boolean"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"int"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","int"]', schema2 = '"int"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"long"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","long"]', schema2 = '"long"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"float"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","float"]', schema2 = '"float"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"double"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","double"]', schema2 = '"double"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"string"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","string"]', schema2 = '"string"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = '"bytes"',
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null","bytes"]', schema2 = '"bytes"',
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = array,
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null",'..array..']', schema2 = array,
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = map,
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null",'..map..']', schema2 = map,
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = fixed,
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null",'..fixed..']', schema2 = fixed,
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = record,
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null",'..record..']', schema2 = record,
    compile_only = true
}

t {
    schema1 = '["null"]', schema2 = enum,
    compile_error = '<union>: No common types'
}

t {
    schema1 = '["null",'..enum..']', schema2 = enum,
    compile_only = true
}
