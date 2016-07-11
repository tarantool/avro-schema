local large = [[{
    "name": "large",
    "type": "record",
    "fields": [
        {"name": "f01", "type": "int", "default": 1001},
        {"name": "f02", "type": "int", "default": 1002},
        {"name": "f03", "type": "int", "default": 1003},
        {"name": "f04", "type": "int", "default": 1004},
        {"name": "f05", "type": "int", "default": 1005},
        {"name": "f06", "type": "int", "default": 1006},
        {"name": "f07", "type": "int", "default": 1007},
        {"name": "f08", "type": "int", "default": 1008},
        {"name": "f09", "type": "int", "default": 1009},
        {"name": "f10", "type": "int", "default": 1010},
        {"name": "f11", "type": "int", "default": 1011},
        {"name": "f12", "type": "int", "default": 1012},
        {"name": "f13", "type": "int", "default": 1013},
        {"name": "f14", "type": "int", "default": 1014},
        {"name": "f15", "type": "int", "default": 1015},
        {"name": "f16", "type": "int", "default": 1016},
        {"name": "f17", "type": "int", "default": 1017},
        {"name": "f18", "type": "int", "default": 1018},
        {"name": "f19", "type": "int", "default": 1019},
        {"name": "f20", "type": "int", "default": 1020},
        {"name": "f21", "type": "int", "default": 1021},
        {"name": "f22", "type": "int", "default": 1022},
        {"name": "f23", "type": "int", "default": 1023},
        {"name": "f24", "type": "int", "default": 1024},
        {"name": "f25", "type": "int", "default": 1025}
    ]
}]]

t {
    schema = large,
    func = 'flatten',
    input = '{}', output = [=[[
       1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010,
       1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020,
       1021, 1022, 1023, 1024, 1025
    ]]=]
}

t {
    schema = large,
    func = 'flatten',
    input = [[{
        "f01": 1, "f02": 2, "f03": 3, "f04": 4, "f05": 5,
        "f06": 6, "f07": 7, "f08": 8, "f09": 9, "f10": 10,
        "f11": 11, "f12": 12, "f13": 13, "f14": 14, "f15": 15,
        "f16": 16, "f17": 17, "f18": 18, "f19": 19, "f20": 20,
        "f21": 21, "f22": 22, "f23": 23, "f24": 24, "f25": 25
    
    }]], output = [=[[
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    ]]=]
}

t {
    schema = large,
    func = 'flatten',
    input = [[{
        "f01": 1, "f03": 3, "f05": 5,
        "f07": 7, "f09": 9,
        "f11": 11, "f13": 13, "f15": 15,
        "f17": 17, "f19": 19,
        "f21": 21, "f23": 23, "f25": 25
    
    }]], output = [=[[
        1,1002,3,1004,5,1006,7,1008,9,1010,11,1012,13,1014,15,
        1016,17,1018,19,1020,21,1022,23,1024,25
    ]]=]
}


t {
    schema = large,
    func = 'unflatten',
    output = [[{
        "f01": 1, "f02": 2, "f03": 3, "f04": 4, "f05": 5,
        "f06": 6, "f07": 7, "f08": 8, "f09": 9, "f10": 10,
        "f11": 11, "f12": 12, "f13": 13, "f14": 14, "f15": 15,
        "f16": 16, "f17": 17, "f18": 18, "f19": 19, "f20": 20,
        "f21": 21, "f22": 22, "f23": 23, "f24": 24, "f25": 25
    
    }]], input = [=[[
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    ]]=]
}
