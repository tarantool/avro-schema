t {
    schema = [[{
        "type": "record",
        "name": "Y",
        "namespace": "X"
    }]],
    create_error = 'X.Y: Record type must have "fields"'
}

t {
    schema = [[{
        "type": "record",
        "name": "X.Y",
        "namespace": "Z"
    }]],
    create_error = 'X.Y: Record type must have "fields"'
}

t {
    schema = [[{
        "type": "record",
        "name": "Y",
        "namespace": "X",
        "fields": [{"name": "field1", "type": "W"}]
    }]],
    create_error = 'X.Y/field1: Unknown Avro type: X.W'
}

t {
    schema = [[{
        "type": "record",
        "name": "X.Y",
        "namespace": "Z",
        "fields": [{"name": "field1", "type": "W"}]
    }]],
    create_error = 'X.Y/field1: Unknown Avro type: X.W'
}

t {
    schema = [[{
        "type": "record",
        "name": "Y",
        "namespace": "X",
        "fields": [{"name": "field1", "type": "Z.W"}]
    }]],
    create_error = 'X.Y/field1: Unknown Avro type: Z.W'
}
