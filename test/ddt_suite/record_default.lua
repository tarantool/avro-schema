-- Default values + nullable type.

local nullable_default = [[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type": "int*", "default": 42 },
        { "name": "bar", "type": "string" }
    ]
}
]]

t {
    schema = nullable_default,
    func = "flatten",
    input = [[ {"bar": "str"} ]],
    output = [=[ [42, "str" ] ]=],
}

t {
    schema = nullable_default,
    func = "unflatten",
    input = [[ [null, "str" ] ]],
    output = [[ {"foo": null, "bar": "str"} ]],
}

local nullable_default_null = [[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type": "int*", "default": null },
        { "name": "bar", "type": "string" }
    ]
}
]]

t {
    schema = nullable_default_null,
    func = "flatten",
    input = [[ {"bar": "str"} ]],
    output = [[ [null, "str" ] ]],
}

t {
    schema = nullable_default_null,
    func = "unflatten",
    input = [[ [42, "str" ] ]],
    output = [[ {"foo": 42, "bar": "str"} ]],
}

local nullable_default_record = [[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record*", "name": "default_record", "fields":[
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "int"},
                {"name": "f3", "type":
                    {"type": "record*", "name": "default_record_2", "fields":[
                        {"name": "f3_1", "type": "int*"}
                    ]}
                }
            ]}, "default": { "f1": 1, "f2": 2}},
        { "name": "bar", "type": "string" }
    ]
}
]]

t {
    schema = nullable_default_record,
    func = "flatten",
    input = [[ {"bar": "str"} ]],
    output = [=[ [[1, 2, null], "str" ] ]=],
}

t {
    schema = nullable_default_record,
    func = "unflatten",
    input = [[ [null, "str" ] ]],
    output = [[ {"foo": null, "bar": "str"} ]],
}

local nullable_default_record_null = [[
{
    "type": "record",
    "name": "Frob",
    "fields": [
        { "name": "foo", "type":
            { "type": "record*", "name": "default_record", "fields":[
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "int"},
                {"name": "f3", "type":
                    {"type": "record*", "name": "default_record_2", "fields":[
                        {"name": "f3_1", "type": "int*"}
                    ]}
                }
            ]}, "default": null},
        { "name": "bar", "type": "string" }
    ]
}
]]

t {
    schema = nullable_default_record_null,
    func = "flatten",
    input = [[ {"bar": "str"} ]],
    output = [=[ [null, "str" ] ]=],
}

t {
    schema = nullable_default_record_null,
    func = "unflatten",
    input = [[ [[1, 2, null], "str" ] ]],
    output = [[ {"foo": {"f1": 1, "f2": 2, "f3": null}, "bar": "str"} ]],
}
