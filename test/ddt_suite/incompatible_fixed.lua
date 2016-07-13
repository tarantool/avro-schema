t {
    schema1 = '{"name":"blob", "type":"fixed", "size": 16}',
    schema2 = '{"name":"blob", "type":"fixed", "size": 17}',
    compile_error = 'blob: Size mismatch: 16 vs 17'
}

t {
    schema1 = '{"name":"blob", "type":"fixed", "size": 16}',
    schema2 = '{"name":"xblob", "type":"fixed", "size": 16}',
    compile_error = 'Types incompatible: blob and xblob'
}

t {
    schema1 = '{"name":"blob", "type":"fixed", "size": 16}',
    schema2 = '{"name":"xblob", "type":"fixed", "size": 16, "aliases":["blob"]}',
    compile_only = true
}

t {
    schema1 = '{"name":"blob", "type":"fixed", "size": 16, "aliases":["xblob"]}',
    schema2 = '{"name":"xblob", "type":"fixed", "size": 16}',
    compile_error = 'Types incompatible: blob and xblob'
}

t {
    schema1 = '{"name":"blob", "type":"fixed", "size": 16, "aliases":["xblob"]}',
    schema2 = '{"name":"xblob", "type":"fixed", "size": 16}',
    compile_downgrade = true,
    compile_only = true
}
