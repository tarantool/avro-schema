t {
    schema1 = [[{
        "name":"user",
        "type":"record",
        "fields":[
            {"type":"long","name":"uid"},
            {"type":"long","name":"p1"},
            {"type":"long","name":"p2"},
            {"name":"nested", "type":{
                "name": "nested", "type":"record","fields":[
                    {"type":"long","name":"x"},
                    {"type":"long","name":"y"},
                    {"name":"points", "type":{"type":"array","items":{
                        "name":"point",
                        "type":"record",
                        "fields":[
                            {"type":"long","name":"x"},
                            {"type":"long","name":"y"}
                        ]
                    }}}
                ]
            }}
        ]
    }]],
    schema2 = [[{
        "name":"user",
        "type":"record",
        "fields":[
            {"type":"long","name":"uid"},
            {"type":"long","name":"p1"},
            {"type":"long","name":"p2"},
            {"type":"string","name":"p3","default":"test avro default"},
            {"name":"nested", "type":{
                "name": "nested", "type":"record","fields":[
                    {"type":"long","name":"x"},
                    {"type":"long","name":"y"},
                    {"name":"points", "type":{"type":"array","items":{
                        "name":"point",
                        "type":"record",
                        "fields":[
                            {"type":"long","name":"x"},
                            {"type":"long","name":"y"}
                        ]
                    }}}
                ]
            }}
        ]
    }]],
    compile_only = true
}
