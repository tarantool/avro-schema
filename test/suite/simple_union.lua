-- flatten/unflatten for the schemas below, + most common validation errors 

local schema = {
    '["int", "string", "double"]',
    '["int", "string", "double", "null"]'
}

for i = 1,2 do

    -- included in test label
    _G['i'] = i

    -- flatten
    t {
        schema = schema[i],
        func   = 'flatten', input  = '{"int": 42}', output = '[0, 42]'
    }

    t {
        schema = schema[i],
        func   = 'flatten',
        input  = '{"string": "Hello, world!"}',
        output = '[1, "Hello, world!"]'
    }

    t {
        schema = '["int", "string", "double"]',
        func   = 'flatten', input  = '{"double": 99.1}', output = '[2, 99.1]'
    }

    -- flatten, errors
    t {
        error  = 'Unknown key: "!!!"',
        schema = schema[i],
        func   = 'flatten', input  = '{"!!!": 42}'
    }

    t {
        error  = 'Expecting MAP of length 1. Encountered MAP of length 0.',
        schema = schema[i],
        func   = 'flatten', input  = '{}'
    }

    t {
        error  = 'int: Expecting INT, encountered STR',
        schema = schema[i],
        func   = 'flatten', input  = '{"int": "42"}'
    }

    t {
        error  = 'string: Expecting STR, encountered LONG',
        schema = schema[i],
        func   = 'flatten', input  = '{"string": 42}'
    }

    t {
        error  = 'double: Expecting DOUBLE, encountered STR',
        schema = schema[i],
        func   = 'flatten', input  = '{"double": "42"}'
    }

    -- unflatten
    t {
        schema = schema[i],
        func   = 'unflatten', input  = '[0, 42]', output = '{"int": 42}'
    }

    t {
        schema = schema[i],
        func   = 'unflatten', input  = '[1, "Hello, world!"]',
        output = '{"string": "Hello, world!"}'
    }

    t {
        schema = schema[i],
        func   = 'unflatten', input  = '[2, 99.8]',
        output = '{"double": 99.8}'
    }

    -- unflatten, errors
    t {
        error  = 'Expecting ARRAY, encountered LONG',
        schema = schema[i],
        func   = 'unflatten', input = '42'
    }

    t {
        error  = 'Expecting ARRAY of length 2. Encountered ARRAY of length 3.',
        schema = schema[i],
        func   = 'unflatten', input = '[0, 42, 42]'
    }

    t {
        error  = '1: Expecting INT, encountered STR',
        schema = schema[i],
        func   = 'unflatten', input = '["1", 42]'
    }

    t {
        error  = '1: Bad value: -1',
        schema = schema[i],
        func   = 'unflatten', input = '[-1, 42]'
    }

    t {
        error  = '1: Bad value: 123',
        schema = schema[i],
        func   = 'unflatten', input = '[123, 42]'
    }

    t {
        error  = '2: Expecting INT, encountered STR',
        schema = schema[i],
        func   = 'unflatten', input = '[0, "42"]'
    }

    skip_t {
        error  = '2: Expecting STR, encountered LONG',
        schema = schema[i],
        func   = 'unflatten', input = '[1, 42]'
    }

    skip_t {
        error  = '2: Expecting DOUBLE, encountered STR',
        schema = schema[i],
        func   = 'unflatten', input = '[2, "42"]'
    }

end -- for i = 1,2 do

-- schema-specific test cases, hence not in the loop above
_G['i'] = nil

-- flatten
t {
    schema = '["int", "string", "double", "null"]',
    func   = 'flatten', input  = 'null', output = '[3, null]'
}

-- unflatten
t {
    schema = '["int", "string", "double", "null"]',
    func   = 'unflatten', input  = '[3, null]', output = 'null'
}

-- flatten, errors
t {
    error  = 'Expecting MAP, encountered STR',
    schema = '["int", "string", "double"]',
    func   = 'flatten', input  = '"!!!"'
}

t {
    error  = 'Expecting NIL or MAP, encountered STR',
    schema = '["int", "string", "double", "null"]',
    func   = 'flatten', input  = '"!!!"'
}

-- unflatten, errors
t {
    error  = '1: Bad value: 3',
    schema = '["int", "string", "double"]',
    func   = 'unflatten', input = '[3, 42]'
}

t {
    error  = '1: Bad value: 4',
    schema = '["int", "string", "double", "null"]',
    func   = 'unflatten', input = '[4, 42]'
}

t {
    error  = '2: Expecting NIL, encountered LONG',
    schema = '["int", "string", "double", "null"]',
    func   = 'unflatten', input  = '[3, 42]', output = 'null'
}
