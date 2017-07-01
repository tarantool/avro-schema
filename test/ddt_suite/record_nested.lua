local user = [[{
    "type": "record",
    "name": "user",
    "fields": [
        {"name": "uid", "type": "long"},
        {"name": "p1", "type": "long"},
        {"name": "p2", "type": "long"},
        {
            "name": "nested",
            "type": {
                "type": "record",
                "name": "nested",
                "fields": [
                    {"name": "x", "type": "long"},
                    {"name": "y", "type": "long"},
                    {"name": "points", "type": {
                        "type": "array",
                        "items": {
                            "name": "point",
                            "type": "record",
                            "fields": [
                                    {"name": "x", "type": "long"},
                                    {"name": "y", "type": "long"}
                            ]
                        }
                    }}
                ]
            }
        }
    ]
}]]

t {
    schema = user,
    func = 'flatten',
    input = [[{
        "p2": 79031234569, "p1": 79031234568,
        "uid": 79031234567,
        "nested": {"y": 2, "x": 1, "points": [
            {"y": 20, "x": 10},
            {"y": 22, "x": 12}]}
    }]],
    output = '[79031234567, 79031234568, 79031234569, 1, 2, [[10, 20], [12, 22]]]'
}

t {
    schema = user,
    func = 'unflatten',
    output = [[{
        "uid": 79031234567,
        "p1": 79031234568,
        "p2": 79031234569,
        "nested": {"x": 1, "y": 2, "points": [
            {"x": 10, "y": 20},
            {"x": 12, "y": 22}]}
    }]],
    input = '[79031234567, 79031234568, 79031234569, 1, 2, [[10, 20], [12, 22]]]'
}

t {
    schema = user,
    func = 'xflatten',
    input = [[{
        "p1": 79031234568,
        "nested": {
            "x": 1,
            "y": 2
        }
    }]],
    output = '[["=", 2, 79031234568], ["=", 4, 1], ["=", 5, 2]]'
}
