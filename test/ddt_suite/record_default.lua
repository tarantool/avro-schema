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

local default_inside_of_default = [[
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
                    ]},
                "default": {"f3_1": 3}}
            ]}, "default": { "f1": 1, "f2": 2}},
        { "name": "bar", "type": "string" }
    ]
}
]]

t {
    schema = default_inside_of_default,
    func = "flatten",
    input = [[ {"bar": "str"} ]],
    output = [=[ [[1, 2, [3]], "str" ] ]=],
}

t {
    schema = default_inside_of_default,
    func = "flatten",
    input = [[ {"foo": {"f1":5, "f2":7}, "bar": "str"} ]],
    output = [=[ [[5, 7, [3]], "str" ] ]=],
}

t {
    schema = default_inside_of_default,
    func = "flatten",
    input = [[ {"foo": {"f1":5, "f2":7, "f3": null}, "bar": "str"} ]],
    output = [=[ [[5, 7, null], "str" ] ]=],
}

local default_inside_of_default_2 = [[
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
                        {"name": "f3_1", "type": "int*", "default": 3}
                    ]}
                }
            ]}, "default": { "f1": 1, "f2": 2, "f3": null}},
        { "name": "bar", "type": "string" }
    ]
}
]]

t {
    schema = default_inside_of_default_2,
    func = "flatten",
    input = [[ {"bar": "str"} ]],
    output = [=[ [[1, 2, null], "str" ] ]=],
}

t {
    schema = default_inside_of_default_2,
    func = "flatten",
    input = [[ {"foo": {"f1":5, "f2":7, "f3": {}}, "bar": "str"} ]],
    output = [=[ [[5, 7, [3]], "str" ] ]=],
}
