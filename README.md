# tarantool-avro
Apache Avro bindings for Tarantool

```lua
avro = require ('avro')
```

##Defining a Schema
```lua
ok, schema = avro.create_schema[[
  {
    "type": "record",
    "name": "Frob",
    "fields": [
      { "name": "foo", "type": "int"},
      { "name": "bar", "type": "string"}
    ]
  }
]]
```

##Removing Redundant Information from the Data
The schema is necessary to restore it to the pristine condition.
```lua
avro.flatten({foo = 42, bar = "Hello!"}, schema) --> true, {42, "Hello"}
```

##Going back to Square One
```lua
avro.unflatten({42, "Hello"}, schema) --> true, {foo = 42, bar = "Hello!"}
```

##On the fly Conversion
```lua
_, new_schema = avro.create_schema[[
  {
    "type": "record",
    "name": "Frob",
    "fields": [
      { "name": "foo", "type": "int"},
      { "name": "pi",  "type": {"type": "array", "items": "int"} },
      { "name": "bar", "type": "string"}
    ]
  }
]]

new_data = {42, {3, 1, 4, 1, 5, 9, 2, 6, 5}, "Hello"}
avro.unflatten(new_data, new_schema, schema) --> true, {foo = 42, bar = "Hello!"}
```

Enjoy!
