t {
    schema = '"42"',
    create_error = 'Unknown Avro type: 42'
}

t {
    schema = '"bad_type"',
    create_error = 'Unknown Avro type: bad_type'
}

t {
    schema = '{"type": "bad_type"}',
    create_error = 'Unknown Avro type: bad_type'
}

t {
    schema = '{"name": "foo"}',
    create_error = 'Must have a "type"'
}

-- verbose notation for primitives, seldom used

t {
    schema = '{"type": "null"}',
    create_only = true
}

t {
    schema = '{"type": "boolean"}',
    create_only = true
}

t {
    schema = '{"type": "int"}',
    create_only = true
}

t {
    schema = '{"type": "long"}',
    create_only = true
}

t {
    schema = '{"type": "float"}',
    create_only = true
}

t {
    schema = '{"type": "double"}',
    create_only = true
}

t {
    schema = '{"type": "string"}',
    create_only = true
}

t {
    schema = '{"type": "bytes"}',
    create_only = true
}

-- fixed

t {
    schema = '{"type": "fixed"}',
    create_error = '<fixed>: Must have a "name"'
}

t {
    schema = '{"type": "fixed", "name": 42}',
    create_error = '<fixed>: Bad type name: 42'
}

t {
    schema = '{"type": "fixed", "name": "42"}',
    create_error = '<fixed>: Bad type name: 42'
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": 16, "aliases": 42}',
    create_error = 'blob: Property "aliases" must be a list'
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": 16, "aliases": [ 42 ]}',
    create_error = 'blob: Bad type name: 42'
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": 16, "aliases": ["blob"]}',
    create_error = 'blob: Alias type name already defined: blob'
}

t {
    schema = '{"type": "fixed", "name": "blob"}',
    create_error = 'blob: Fixed type must have "size"'
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": ""}',
    create_error = 'blob: Bad fixed type size: '
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": false}',
    create_error = 'blob: Bad fixed type size: false'
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": 4.1}',
    create_error = 'blob: Bad fixed type size: 4.1'
}

t {
    schema = '{"type": "fixed", "name": "blob", "size": -10}',
    create_error = 'blob: Bad fixed type size: -10'
}

-- enum

t {
    schema = '{"type": "enum"}',
    create_error = '<enum>: Must have a "name"'
}

t {
    schema = '{"type": "enum", "name": 42}',
    create_error = '<enum>: Bad type name: 42'
}

t {
    schema = '{"type": "enum", "name": "42"}',
    create_error = '<enum>: Bad type name: 42'
}

t {
    schema = '{"type": "enum", "name": "test"}',
    create_error = 'test: Enum type must have "symbols"'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": true}',
    create_error = 'test: Enum "symbols" must be a list'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": []}',
    create_error = 'test: Enum type must contain at least one symbol'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": ["APPLES", -1]}',
    create_error = 'test: Bad enum symbol name: -1'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": ["APPLES", "1"]}',
    create_error = 'test: Bad enum symbol name: 1'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": ["APPLES", "ORANGES", "APPLES"]}',
    create_error = 'test: Enum contains symbol APPLES twice'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": ["APPLES"], "aliases": 42}',
    create_error = 'test: Property "aliases" must be a list'
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": ["APPLES"], "aliases": ["test1"]}',
    compile_only = true
}

t {
    schema = '{"type": "enum", "name": "test", "symbols": ["APPLES"], "aliases": ["test"]}',
    create_error = "test: Alias type name already defined: test"
}

-- array

t {
    schema = '{"type": "array"}',
    create_error = '<array>: Array type must have "items"'
}

t {
    schema = '{"type": "array", "items": 42}',
    create_error = '<array>: Unknown Avro type: 42'
}

-- map

t {
    schema = '{"type": "map"}',
    create_error = '<map>: Map type must have "values"'
}

t {
    schema = '{"type": "map", "values": 42}',
    create_error = '<map>: Unknown Avro type: 42'
}

-- union

t {
    schema = '[]',
    create_error = 'Union type must have at least one branch'
}

t {
    schema = '["int", "int"]',
    create_error = '<union>/<branch-2>: Union contains int twice'
}

t {
    schema = '[ {"type": "array", "items": "int"}, {"type": "array", "items": "float"} ]',
    create_error = '<union>/<branch-2>: Union contains array twice'
}

-- record

t {
    schema = '{"type":"record"}',
    create_error = '<record>: Must have a "name"'
}

t {
    schema = '{"type":"record", "name":42}',
    create_error = '<record>: Bad type name: 42'
}

t {
    schema = '{"type":"record", "name":"42"}',
    create_error = '<record>: Bad type name: 42'
}

t {
    schema = '{"type":"record", "name":"FooBar"}',
    create_error = 'FooBar: Record type must have "fields"'
}

t {
    schema = '{"type":"record", "name":"FooBar", "fields": 42}',
    create_error = 'FooBar: Record "fields" must be a list'
}

t {
    schema = '{"type":"record", "name":"FooBar", "fields": []}',
    create_error = 'FooBar: Record type must have at least one field'
}

t {
    schema = '{"type":"record", "name":"FooBar", "fields": [42]}',
    create_error = 'FooBar/<field-1>: Record field must be a list'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {}
    ]}]],
    create_error = 'FooBar/<field-1>: Record field must have a "name"'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": 42}
    ]}]],
    create_error = 'FooBar/<field-1>: Bad record field name: 42'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "42"}
    ]}]],
    create_error = 'FooBar/<field-1>: Bad record field name: 42'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A"}
    ]}]],
    create_error = 'FooBar/A: Record field must have a "type"'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        42
    ]}]],
    create_error = 'FooBar/<field-2>: Record field must be a list'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "A", "type": "int"}
    ]}]],
    create_error = 'FooBar/<field-2>: Record contains field A twice'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": "FooBar"}
    ]}]],
    create_error = 'Record FooBar contains itself via B'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": {
            "type": "record", "name": "Nested1", "fields": [
                { "name": "X", "type": "FooBar" }
            ]
        }}
    ]}]],
    create_error = 'Record FooBar contains itself via B/X'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": {
            "type": "record", "name": "Nested1", "fields": [
                { "name": "X", "type": {
                    "type": "record", "name": "Nested2", "fields": [
                        {"name": "Y", "type": "FooBar"}
                    ]
                }}
            ]
        }}
    ]}]],
    create_error = 'Record FooBar contains itself via B/X/Y'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": ["int", "FooBar"]}
    ]}]],
    create_only = true
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": {"type": "array", "items": "FooBar"}}
    ]}]],
    create_only = true
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": {"type": "map", "values": "FooBar"}}
    ]}]],
    create_only = true
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int", "aliases": 42}
    ]}]],
    create_error = 'FooBar/A: Property "aliases" must be a list'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int", "aliases": [42]}
    ]}]],
    create_error = 'FooBar/A: Bad field alias name: 42'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int", "aliases": ["B"]},
        {"name": "B", "type": "int"}
    ]}]],
    create_error = 'FooBar/<field-2>: Record contains field B twice'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": "int", "aliases": ["A"]}
    ]}]],
    create_error = 'FooBar/B: Alias field name already defined: A'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"}
    ], "aliases": 42}]],
    create_error = 'FooBar: Property "aliases" must be a list'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A"}
    ], "aliases": [42]}]],
    create_error = 'FooBar: Bad type name: 42'
}

t {
    schema = [[{"type":"record", "name":"FooBar", "fields": [
        {"name": "A", "type": "int"}
    ], "aliases": ["FooBar"]}]],
    create_error = 'FooBar: Alias type name already defined: FooBar'
}
