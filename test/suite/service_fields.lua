local simple = [[{
    "name": "simple", "type": "record", "fields":[
        {"name":"A", "type":"string", "default":"Simple "},
        {"name":"B", "type":"int", "default":1234}
    ]
}]]

-- boolean

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'flatten',
    input = {'{}', true}, output = '[true, "Simple ", 1234]'
}

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'flatten',
    input = {'{}', false}, output = '[false, "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'unflatten',
    input = '[false, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', false}, 
}

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', true}, 
}

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'boolean'}, func = 'unflatten',
    input = '[42, "Hello, world!", 42]',
    error = "1: Expecting BOOL, encountered LONG"
}

-- int

t {
    schema = simple,
    service_fields = {'int'}, func = 'flatten',
    input = {'{}', 19}, output = '[19, "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'int'}, func = 'unflatten',
    input = '[29, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 29}, 
}

t {
    schema = simple,
    service_fields = {'int'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'int'}, func = 'unflatten',
    input = '[31, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'int'}, func = 'unflatten',
    input = '["31", "Hello, world!", 42]',
    error = "1: Expecting INT, encountered STR"
}

-- long

t {
    schema = simple,
    service_fields = {'long'}, func = 'flatten',
    input = {'{}', 100500}, output = '[100500, "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'long'}, func = 'unflatten',
    input = '[100501, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 100501}, 
}

t {
    schema = simple,
    service_fields = {'long'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'long'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'long'}, func = 'unflatten',
    input = '["42", "Hello, world!", 42]',
    error = "1: Expecting LONG, encountered STR"
}

-- float

t {
    schema = simple,
    service_fields = {'float'}, func = 'flatten',
    input = {'{}', 3.14}, output = '![3.14, "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'float'}, func = 'unflatten',
    input = '![3.125, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 3.125}, 
}

t {
    schema = simple,
    service_fields = {'float'}, func = 'unflatten',
    input = '[3.14, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 3.14}, 
}

t {
    schema = simple,
    service_fields = {'float'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'float'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'float'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42]',
    error = "1: Expecting FLOAT, encountered TRUE"
}

-- double

t {
    schema = simple,
    service_fields = {'double'}, func = 'flatten',
    input = {'{}', 99.1}, output = '[99.1, "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'double'}, func = 'unflatten',
    input = '[99.1, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 99.1}, 
}

t {
    schema = simple,
    service_fields = {'double'}, func = 'unflatten',
    input = '![99.125, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 99.125}, 
}

t {
    schema = simple,
    service_fields = {'double'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'double'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'double'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42]',
    error = "1: Expecting DOUBLE, encountered TRUE"
}

-- string

t {
    schema = simple,
    service_fields = {'string'}, func = 'flatten',
    input = {'{}', "Hello, world!"}, output = '["Hello, world!", "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'string'}, func = 'unflatten',
    input = '["ABCDEF", "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', "ABCDEF"}, 
}

t {
    schema = simple,
    service_fields = {'string'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'string'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'string'}, func = 'unflatten',
    input = '[42, "Hello, world!", 42]',
    error = "1: Expecting STR, encountered LONG"
}

-- bytes

t {
    schema = simple,
    service_fields = {'bytes'}, func = 'flatten',
    input = {'{}', "Hello!"}, output = '[{"$binary": "48656c6c6f21"}, "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'bytes'}, func = 'unflatten',
    input = '[{"$binary": "48656c6c6f2f"}, "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 'Hello/'}, 
}

t {
    schema = simple,
    service_fields = {'bytes'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'bytes'}, func = 'unflatten',
    input = '[true, "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 3. Encountered ARRAY of length 4."
}

t {
    schema = simple,
    service_fields = {'bytes'}, func = 'unflatten',
    input = '[42, "Hello, world!", 42]',
    error = "1: Expecting BIN, encountered LONG"
}

-- int, str

t {
    schema = simple,
    service_fields = {'int', 'string'}, func = 'flatten',
    input = {'{}', 42, "Hello!"}, output = '[42, "Hello!", "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'int', 'string'}, func = 'unflatten',
    input = '[42, "World!", "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 42, "World!"}, 
}

t {
    schema = simple,
    service_fields = {'int', 'string'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 4. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'int','string'}, func = 'unflatten',
    input = '[0, "", "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 4. Encountered ARRAY of length 5."
}

t {
    schema = simple,
    service_fields = {'int','string'}, func = 'unflatten',
    input = '[null,"", "Hello, world!", 42]',
    error = "1: Expecting INT, encountered NIL"
}

t {
    schema = simple,
    service_fields = {'int','string'}, func = 'unflatten',
    input = '[0, null, "Hello, world!", 42]',
    error = "2: Expecting STR, encountered NIL"
}

-- int, str, str

t {
    schema = simple,
    service_fields = {'int', 'string', 'string'}, func = 'flatten',
    input = {'{}', 42, "Hello, ", "world!"}, output = '[42, "Hello, ", "world!", "Simple ", 1234]'
}

--

t {
    schema = simple,
    service_fields = {'int', 'string', 'string'}, func = 'unflatten',
    input = '[99, "Kill ", "all humans!", "Hello, world!", 42]',
    output = {'{"A":"Hello, world!", "B":42}', 99, "Kill ", "all humans!"}, 
}

t {
    schema = simple,
    service_fields = {'int', 'string', 'string'}, func = 'unflatten',
    input = '["Hello, world!", 42]',
    error = "Expecting ARRAY of length 5. Encountered ARRAY of length 2."
}

t {
    schema = simple,
    service_fields = {'int','string', 'string'}, func = 'unflatten',
    input = '[0, "", "", "Hello, world!", 42, ""]',
    error = "Expecting ARRAY of length 5. Encountered ARRAY of length 6."
}

t {
    schema = simple,
    service_fields = {'int','string','string'}, func = 'unflatten',
    input = '[null,"", "", "Hello, world!", 42]',
    error = "1: Expecting INT, encountered NIL"
}

t {
    schema = simple,
    service_fields = {'int','string','string'}, func = 'unflatten',
    input = '[0, null, "", "Hello, world!", 42]',
    error = "2: Expecting STR, encountered NIL"
}

t {
    schema = simple,
    service_fields = {'int','string','string'}, func = 'unflatten',
    input = '[0, "", null, "Hello, world!", 42]',
    error = "3: Expecting STR, encountered NIL"
}

-- xflatten, basic
local types_to_check={'boolean','int','long','float','double','string','bytes'}
for to_check = 1,#types_to_check do
    _G['to_check'] = to_check
    t {
        schema = simple,
        service_fields = {types_to_check[to_check]}, func = 'xflatten',
        input = '{"A":"Hello, world!"}',
        output = '[["=",2,"Hello, world!"]]'
    }

    t {
        schema = simple,
        service_fields = {types_to_check[to_check]}, func = 'xflatten',
        input = '{"B":42}',
        output = '[["=",3,42]]'
    }
end
_G['to_check'] = nil

-- xflatten, 2 service fields
t {
    schema = simple,
    service_fields = {'int','string'}, func = 'xflatten',
    input = '{"A":"Hello, world!"}',
    output = '[["=",3,"Hello, world!"]]'
}

t {
    schema = simple,
    service_fields = {'int','string'}, func = 'xflatten',
    input = '{"B":42}',
    output = '[["=",4,42]]'
}

-- xflatten, 3 service fields
t {
    schema = simple,
    service_fields = {'int','string','double'}, func = 'xflatten',
    input = '{"A":"Hello, world!"}',
    output = '[["=",4,"Hello, world!"]]'
}

t {
    schema = simple,
    service_fields = {'int','string','double'}, func = 'xflatten',
    input = '{"B":42}',
    output = '[["=",5,42]]'
}
