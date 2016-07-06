local person = [[{
    "name": "person", "type": "record", "fields": [
        { "name": "FirstName",   "type": "string" },
        { "name": "LastName",    "type": "string" },
        { "name": "Age",         "type": "int"    },
        { "name": "Sex",         "type": "int"    },
        { "name": "PhoneNumber", "type": "string" },
        { "name": "HomeAddress", "type": "string" },
        { "name": "Occupation",  "type": "string" }
    ]
}]]

local person_default = [[{
    "name": "person", "type": "record", "fields": [
        { "name": "FirstName",   "type": "string" },
        { "name": "LastName",    "type": "string", "default": "" },
        { "name": "Age",         "type": "int"    },
        { "name": "Sex",         "type": "int"    },
        { "name": "PhoneNumber", "type": "string", "default": "" },
        { "name": "HomeAddress", "type": "string", "default": "" },
        { "name": "Occupation",  "type": "string", "default": "" }
    ]
}]]

local schemas = { person, person_default }

-----------------------------------------------------------------------

t {
    schema = person,
    func = 'flatten',
    input = [[{
        "FirstName": "John", "LastName": "Doe", "Age":33, "Sex":1,
        "PhoneNumber": "+7 999 1234567",
        "HomeAddress": "Long Street, 1", "Occupation": "Engineer"
    }]],
    output = '["John", "Doe", 33, 1, "+7 999 1234567", "Long Street, 1", "Engineer"]'
}

-- check different key orders
t {
    schema = person,
    func = 'flatten',
    input = [[{
        "HomeAddress": "Long Street, 1", "Occupation": "Engineer",
        "FirstName": "John", "LastName": "Doe", "Age":33, "Sex":1,
        "PhoneNumber": "+7 999 1234567"
    }]],
    output = '["John", "Doe", 33, 1, "+7 999 1234567", "Long Street, 1", "Engineer"]'
}

t {
    schema = person,
    func = 'flatten',
    input = [[{
        "PhoneNumber": "+7 999 1234567",
        "FirstName": "John", "LastName": "Doe",
        "Age":33,
        "HomeAddress": "Long Street, 1",
        "Sex":1,
        "Occupation": "Engineer"
    }]],
    output = '["John", "Doe", 33, 1, "+7 999 1234567", "Long Street, 1", "Engineer"]'
}

-- unflatten
t {
    schema = person,
    func = 'unflatten',
    input = '["Jane", "Doe", 21, 0, "+7 999 1234567", "Long Street, 1", "Engineer"]',
    output = [[{
        "FirstName": "Jane", "LastName": "Doe", "Age":21, "Sex":0,
        "PhoneNumber": "+7 999 1234567",
        "HomeAddress": "Long Street, 1", "Occupation": "Engineer"
    }]]
}

-- flatten, defaults
t {
    schema = person_default,
    func = 'flatten',
    input = [[{
        "FirstName": "John", "Age":33, "Sex":1,
        "PhoneNumber": "+7 999 1234567",
        "HomeAddress": "Long Street, 1"
    }]],
    output = '["John", "", 33, 1, "+7 999 1234567", "Long Street, 1", ""]'
}

t {
    schema = person_default,
    func = 'flatten',
    input = [[{
        "FirstName": "John", "LastName": "Doe", "Age":33, "Sex":1,
        "Occupation": "Engineer"
    }]],
    output = '["John", "Doe", 33, 1, "", "", "Engineer"]'
}

-- validation errors
t {
    error = 'Expecting MAP, encountered LONG',
    schema = person,
    func = 'flatten',
    input = '42'
}

t {
    error = 'Unknown key: "InvalidKey"',
    schema = person,
    func = 'flatten',
    input = '{"InvalidKey": 42}'
}

--
local funcs = { 'flatten', 'xflatten'}
for schema = 1,#schemas do

    _G['schema'] = schema

    for func = 1,#funcs do

        _G['func'] = func

        t {
            error = 'FirstName: Expecting STR, encountered LONG',
            schema = schemas[ schema ],
            func   = funcs  [ func   ],
            input = '{"FirstName": 42}'
        }

        t {
            error = 'LastName: Expecting STR, encountered LONG',
            schema = schemas[ schema ],
            func   = funcs  [ func   ],
            input = '{"LastName": 42}'
        }

        t {
            error = 'Age: Expecting INT, encountered DOUBLE',
            schema = schemas[ schema ],
            func   = funcs  [ func   ],
            input = '{"Age": 42.0}'
        }

        t {
            error = 'PhoneNumber: Expecting STR, encountered LONG',
            schema = schemas[ schema ],
            func   = funcs  [ func   ],
            input = '{"PhoneNumber": 42}'
        }

        t {
            error = 'HomeAddress: Expecting STR, encountered LONG',
            schema = schemas[ schema ],
            func   = funcs  [ func   ],
            input = '{"HomeAddress": 42}'
        }

        t {
            error = 'Occupation: Expecting STR, encountered LONG',
            schema = schemas[ schema ],
            func   = funcs  [ func   ],
            input = '{"Occupation": 42}'
        }
    end
end

_G['schema'] = nil
_G['func'] = nil

--

t {
    error = 'Key missing: "FirstName"',
    schema = person,
    func = 'flatten',
    input = '{}'
}

t {
    error = 'Key missing: "FirstName"',
    schema = person_default,
    func = 'flatten',
    input = '{}'
}

t {
    error = 'Key missing: "LastName"',
    schema = person,
    func = 'flatten',
    input = '{"FirstName": "Jane"}'
}

t {
    error = 'Key missing: "Age"',
    schema = person,
    func = 'flatten',
    input = '{"FirstName": "Jane", "LastName": "Doe"}'
}

t {
    error = 'Key missing: "Age"',
    schema = person_default,
    func = 'flatten',
    input = '{"FirstName": "Jane"}'
}

t {
    error = 'Key missing: "Sex"',
    schema = person,
    func = 'flatten',
    input = '{"FirstName": "Jane", "LastName": "Doe", "Age": 21}'
}

t {
    error = 'Key missing: "Sex"',
    schema = person_default,
    func = 'flatten',
    input = '{"FirstName": "Jane", "Age": 21}'
}

t {
    error = 'Key missing: "PhoneNumber"',
    schema = person,
    func = 'flatten',
    input = '{"FirstName": "Jane", "LastName": "Doe", "Age": 21, "Sex": 0}'
}

t {
    error = 'Key missing: "HomeAddress"',
    schema = person,
    func = 'flatten',
    input = [[{
        "FirstName": "Jane", "LastName": "Doe", "Age": 21, "Sex": 0,
        "PhoneNumber": "+7 ???"
    }]]
}

t {
    error = 'Key missing: "Occupation"',
    schema = person,
    func = 'flatten',
    input = [[{
        "FirstName": "Jane", "LastName": "Doe", "Age": 21, "Sex": 0,
        "PhoneNumber": "+7 ???", "HomeAddress": "Long St., 1"
    }]]
}

--

for schema = 1,#schemas do

    _G['schema'] = schema

    t {
        error = 'Expecting ARRAY, encountered LONG',
        schema = schemas[schema],
        func = 'unflatten', input = '42'
    }

    t {
        error = 'Expecting ARRAY of length 7. Encountered ARRAY of length 1.',
        schema = schemas[schema],
        func = 'unflatten', input = '[42]'
    }

    t {
        error = 'Expecting ARRAY of length 7. Encountered ARRAY of length 8.',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", "", 33, 1, "+7 999 1234567", "Long Street, 1", "", 42]'
    }

    t {
        error = '1: Expecting STR, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '[null, "", 33, 1, "+7 999 1234567", "Long Street, 1", ""]'
    }

    t {
        error = '2: Expecting STR, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", null, 33, 1, "+7 999 1234567", "Long Street, 1", ""]'
    }

    t {
        error = '3: Expecting INT, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", "", null, 1, "+7 999 1234567", "Long Street, 1", ""]'
    }

    t {
        error = '4: Expecting INT, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", "", 33, null, "+7 999 1234567", "Long Street, 1", ""]'
    }

    t {
        error = '5: Expecting STR, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", "", 33, 1, null, "Long Street, 1", ""]'
    }

    t {
        error = '6: Expecting STR, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", "", 33, 1, "+7 999 1234567", null, ""]'
    }

    t {
        error = '7: Expecting STR, encountered NIL',
        schema = schemas[schema],
        func = 'unflatten',
        input = '["John", "", 33, 1, "+7 999 1234567", "Long Street, 1", null]'
    }

end

_G['schema'] = nil

-- xflatten

for schema = 1,#schemas do 

    _G['schema'] = schema

    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"FirstName": "John"}',
        output = '[["=", 1, "John"]]'
    }

    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"LastName": "Doe"}',
        output = '[["=", 2, "Doe"]]'
    }

    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"Age": 33}',
        output = '[["=", 3, 33]]'
    }

    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"Sex": 1}',
        output = '[["=", 4, 1]]'
    }

    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"PhoneNumber": "+7 999 1234567"}',
        output = '[["=", 5, "+7 999 1234567"]]'
    }
    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"HomeAddress": "Long St., 1"}',
        output = '[["=", 6, "Long St., 1"]]'
    }
    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"Occupation": "Engineer"}',
        output = '[["=", 7, "Engineer"]]'
    }

    --

    t {
        schema = schemas[schema], func = 'xflatten',
        input = '{"FirstName": "John", "LastName": "Doe", "Age": 33}',
        output = '[["=", 1, "John"], ["=", 2, "Doe"], ["=", 3, 33]]'
    }

end

_G['schema'] = nil

