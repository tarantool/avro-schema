local recursive = [[{
    "name": "node",
    "type": "record",
    "fields": [
        {"name":"next", "type":["null", "node"]},
        {"name":"label", "type":"string"}
    ]
}]]

t {
    schema = recursive,
    func = 'flatten', input = '{"label":"L1", "next":null}',
    output = '[0, null, "L1"]'
}

t {
    schema = recursive,
    func = 'flatten', input = '{"label":"L1", "next":{"node":{"label":"L2", "next":null}}}',
    output = '[1, [0, null, "L2"], "L1"]'
}

t {
    schema = recursive,
    func = 'flatten', input = [[{
        "label":"L1", "next":{"node":{
            "label":"L2", "next":{"node":
                {"label":"L3", "next":null}
            }
        }}
    }]],
    output = '[1, [1, [0, null, "L3"], "L2"], "L1"]'
}

--

t {
    schema = recursive,
    func = 'unflatten', input = '[0, null, "L1"]',
    output = '{"next":null, "label":"L1"}',
}

t {
    schema = recursive,
    func = 'unflatten', input = '[1, [0, null, "L2"], "L1"]',
    output = '{"next":{"node":{"next":null, "label":"L2"}},"label":"L1"}'
}

t {
    schema = recursive,
    func = 'unflatten',
    input = '[1, [1, [0, null, "L3"], "L2"], "L1"]',
    output = [[{
        "next":{"node":{
            "next":{"node":
                {"next":null, "label":"L3"}
            },
            "label":"L2"
        }},
        "label":"L1"
    }]]
}

--

t {
    schema = recursive,
    func = 'xflatten', input = '{"label": "LABEL"}',
    output = '[["=",3,"LABEL"]]'
}

t {
    schema = recursive,
    func = 'xflatten', input = '{"next": null}',
    output = '[["=",1,0],["=",2,null]]'
}

t {
    schema = recursive,
    func = 'xflatten', input = '{"next": {"node":{"label":"LABEL", "next":null}}}',
    output = '[["=",1,1],["=",2,[0,null,"LABEL"]]]'
}

t {
    schema = recursive,
    func = 'xflatten', input = [[{
        "next": {"node":{"label":"LABEL1", "next":{"node":{"label":"LABEL2", "next":null}}}}
    }]],
    output = '[["=",1,1],["=",2,[1,[0,null,"LABEL2"],"LABEL1"]]]'
}
