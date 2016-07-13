local record_array = [[{
    "type": "array",
    "items": {
        "name": "foo",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int", "default":1},
            {"name":"B", "type":"int", "default":2},
            {"name":"C", "type":"int", "default":3}
        ]
    }
}]]

local record_array2 = [[{
    "type": "array",
    "items": {
        "name": "foo",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int", "default":1},
            {"name":"B", "type":"int", "default":2},
            {"name":"C", "type":{"type":"array", "items":"int"}}
        ]
    }
}]]

t {
    schema = record_array,
    func = 'flatten',
    input = '[]', output='[[]]'
}

t {
    schema = record_array,
    func = 'flatten',
    input = '[{},{"A":100},{"B":200},{"C":300}]',
    output='[[[1,2,3],[100,2,3],[1,200,3],[1,2,300]]]'
}

t {
    schema = record_array,
    func = 'unflatten',
    input = '[[]]', output='[]'
}

t {
    schema = record_array,
    func = 'unflatten',
    input='[[[1,2,3],[100,2,3],[1,200,3],[1,2,300]]]',
    output = [=[[
        {"A":1,"B":2,"C":3},
        {"A":100,"B":2,"C":3},
        {"A":1,"B":200,"C":3},
        {"A":1,"B":2,"C":300}]
    ]=]
}

--

t {
    schema = record_array2,
    func = 'flatten',
    input = '[]', output='[[]]'
}

t {
    schema = record_array2,
    func = 'flatten',
    input = '[{"C":[]},{"A":100,"C":[1,2,3,4]},{"B":200,"C":[5,6,7,8]}]',
    output='[[[1,2,[]],[100,2,[1,2,3,4]],[1,200,[5,6,7,8]]]]'
}

t {
    schema = record_array2,
    func = 'unflatten',
    input = '[[]]', output='[]'
}

t {
    schema = record_array2,
    func = 'unflatten',
    input='[[[1,2,[]],[100,2,[1,2,3,4]],[1,200,[5,6,7,8]]]]',
    output = '[{"A":1,"B":2,"C":[]},{"A":100,"B":2,"C":[1,2,3,4]},{"A":1,"B":200,"C":[5,6,7,8]}]'
}
