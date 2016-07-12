t {
    schema1 = '{"name":"foo", "type":"enum", "symbols":["A","B","C"]}', 
    schema2 = '{"name":"foo", "type":"enum", "symbols":["D"]}', 
    compile_error = 'foo: No common symbols'
}

t {
    schema1 = '{"name":"foo", "type":"enum", "symbols":["A","B","C"]}', 
    schema2 = '{"name":"xfoo", "type":"enum", "symbols":["B"]}',
    compile_error = 'Types incompatible: foo and xfoo'
}

t {
    schema1 = '{"name":"foo", "type":"enum", "symbols":["A","B","C"]}', 
    schema2 = '{"name":"xfoo", "type":"enum", "symbols":["B"], "aliases":["foo"]}',
    compile_only = true
}

t {
    schema1 = '{"name":"foo", "type":"enum", "symbols":["A","B","C"], "aliases":["xfoo"]}', 
    schema2 = '{"name":"xfoo", "type":"enum", "symbols":["B"]}', 
    compile_error = 'Types incompatible: foo and xfoo'
}

t {
    schema1 = '{"name":"foo", "type":"enum", "symbols":["A","B","C"], "aliases":["xfoo"]}', 
    schema2 = '{"name":"xfoo", "type":"enum", "symbols":["B"]}',
    compile_downgrade = true,
    compile_only = true
}
