--
-- VLO fields are handled differently i.r.t. flatten/defaults
--

local vlo1 = [[{
    "type": "record",
    "name": "vlo1",
    "fields": [
        {"name": "A", "type": "int", "default": 100},
        {"name": "B", "type": "int", "default": 101},
        {"name": "VLO", "type": { "type": "array", "items": "int"}}
    ]
}]]

local vlo2 = [[{
    "type": "record",
    "name": "vlo2",
    "fields": [
        {"name": "VLO", "type": { "type": "array", "items": "int"}},
        {"name": "A", "type": "int", "default": 100},
        {"name": "B", "type": "int", "default": 101}
    ]
}]]

local vlo3 = [[{
    "type": "record",
    "name": "vlo3",
    "fields": [
        {"name": "VL1", "type": { "type": "array", "items": "int"}},
        {"name": "A", "type": "int", "default": 100},
        {"name": "B", "type": "int", "default": 101},
        {"name": "VL2", "type": { "type": "array", "items": "int"}}
    ]
}]]

--

t {
    schema = vlo1,
    func = 'flatten', input = '{"VLO": [1,2,3,4]}', output = '[100, 101, [1,2,3,4]]'
}

t {
    schema = vlo1,
    func = 'flatten', input = '{"A":1, "VLO": [1,2,3]}', output = '[1, 101, [1,2,3]]'
}

t {
    schema = vlo1,
    func = 'flatten', input = '{"B":2, "VLO": [1,2,3]}', output = '[100, 2, [1,2,3]]'
}

t {
    schema = vlo1,
    func = 'flatten', input = '{"A":1, "B":2, "VLO": [1,2,3]}', output = '[1, 2, [1,2,3]]'
}

--

t {
    schema = vlo2,
    func = 'flatten', input = '{"VLO": [1,2,3,4]}', output = '[[1,2,3,4], 100, 101]'
}

t {
    schema = vlo2,
    func = 'flatten', input = '{"A":1, "VLO": [1,2,3]}', output = '[[1,2,3], 1, 101]'
}

t {
    schema = vlo2,
    func = 'flatten', input = '{"B":2, "VLO": [1,2,3]}', output = '[[1,2,3], 100, 2]'
}

t {
    schema = vlo2,
    func = 'flatten', input = '{"A":1, "B":2, "VLO": [1,2,3]}', output = '[[1,2,3], 1, 2]'
}

--

t {
    schema = vlo3,
    func = 'flatten', input = '{"VL1": [1,2,3], "VL2": [4,5,6]}', output = '[[1,2,3],100,101,[4,5,6]]'
}

t {
    schema = vlo3,
    func = 'flatten', input = '{"A":1, "VL1": [1,2,3], "VL2": [4,5,6]}', output = '[[1,2,3],1,101,[4,5,6]]'
}

t {
    schema = vlo3,
    func = 'flatten', input = '{"B":2, "VL1": [1,2,3], "VL2": [4,5,6]}', output = '[[1,2,3],100,2,[4,5,6]]'
}

t {
    schema = vlo3,
    func = 'flatten', input = '{"A":1, "B":2, "VL1": [1,2,3], "VL2": [4,5,6]}', output = '[[1,2,3],1,2,[4,5,6]]'
}

