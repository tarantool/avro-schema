local vehicle_v1 = [[{
    "name": "vehicle", "type": "enum", "symbols": [
        "CAR", "BUS", "TRICYCLE", "TRUCK"
    ]
}]]

-- TRICYCLE removed, TRUCK changes ID, SCOOTER added
local vehicle_v2 = [[{
    "name": "vehicle", "type": "enum", "symbols": [
        "CAR", "BUS", "TRUCK", "SCOOTER"
    ]
}]]

-----------------------------------------------------------------------

t {
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'flatten', input = '"CAR"', output = '[0]'
}

t {
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'flatten', input = '"CAR"', output = '[0]'
}

t {
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'unflatten', input = '[0]', output = '"CAR"'
}

t {
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'unflatten', input = '[0]', output = '"CAR"'
}

-----------------------------------------------------------------------

t {
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'flatten', input = '"BUS"', output = '[1]'
}

t {
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'flatten', input = '"BUS"', output = '[1]'
}

t {
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'unflatten', input = '[1]', output = '"BUS"'
}

t {
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'unflatten', input = '[1]', output = '"BUS"'
}

-----------------------------------------------------------------------

t {
    error = 'Bad value: "TRICYCLE" (schema versioning)',
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'flatten', input = '"TRICYCLE"'
}

t {
    error = 'Bad value: "TRICYCLE"',
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'flatten', input = '"TRICYCLE"'
}

t {
    error = '1: Bad value: 2 (schema versioning)',
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'unflatten', input = '[2]'
}

-----------------------------------------------------------------------

t {
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'flatten', input = '"TRUCK"', output = '[2]'
}

t {
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'flatten', input = '"TRUCK"', output = '[3]'
}

t {
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'unflatten', input = '[3]', output = '"TRUCK"'
}

t {
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'unflatten', input = '[2]', output = '"TRUCK"'
}

-----------------------------------------------------------------------

t {
    error = 'Bad value: "SCOOTER"',
    schema1 = vehicle_v1, schema2 = vehicle_v2,
    func = 'flatten', input = '"SCOOTER"'
}

t {
    error = 'Bad value: "SCOOTER" (schema versioning)',
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'flatten', input = '"SCOOTER"'
}

t {
    error = '1: Bad value: 3 (schema versioning)',
    schema1 = vehicle_v2, schema2 = vehicle_v1,
    func = 'unflatten', input = '[3]'
}
