t {
    schema = [[{
        "name": "hidden",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int", "hidden":true},
            {"name":"B", "type":"int", "hidden":true},
            {"name":"C", "type":"int", "hidden":true},
            {"name":"D", "type":"int"}
        ]
    }]],
    func = 'unflatten', input = '[100,200,300,400]',
    output = '{"D":400}'
}

t {
    schema = [[{
        "name": "hidden",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int", "hidden":true},
            {"name":"B", "type":"int", "hidden":true},
            {"name":"C", "type":"int"},
            {"name":"D", "type":"int", "hidden":true}
        ]
    }]],
    func = 'unflatten', input = '[100,200,300,400]',
    output = '{"C":300}'
}

t {
    schema = [[{
        "name": "hidden",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int", "hidden":true},
            {"name":"B", "type":"int"},
            {"name":"C", "type":"int", "hidden":true},
            {"name":"D", "type":"int", "hidden":true}
        ]
    }]],
    func = 'unflatten', input = '[100,200,300,400]',
    output = '{"B":200}'
}

t {
    schema = [[{
        "name": "hidden",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int"},
            {"name":"B", "type":"int"},
            {"name":"C", "type":"int", "hidden":true},
            {"name":"D", "type":"int", "hidden":true}
        ]
    }]],
    func = 'unflatten', input = '[100,200,300,400]',
    output = '{"A":100,"B":200}'
}

--

t {
    schema = [[{
        "name": "hidden",
        "type": "record",
        "fields": [
            {"name":"A", "type":"int"},
            {"name":"B", "type": {
                "name": "nested", "type": "record", "fields": [
                    {"name":"X", "type":"int"},
                    {"name":"Y", "type":"int"}
                ]
            }, "hidden": true},
            {"name":"C", "type":"int"}
        ]
    }]],
    func = 'unflatten', input = '[100,200,300,400]',
    output = '{"A":100,"C":400}'
}
