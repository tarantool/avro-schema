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

# Internals

Internally, Apache Avro C library is being used.
The library provides facilities for parsing schemas, exploring schemas programmatically and data presentation
(storing data in a DOM-like structure).
Data presentation supports transparent conversion between schema versions.

The module opperates rawly as follows.
Avro data presentation is built from the input.
This step includes optional unflattening, i.e. the data undergo certain transformations before insertion in the DOM.
For performance reasons string copies are shallow.
Then, output is built from the DOM. At this step, flattening may happen.

It is suggested to cache schema objects,
to avoid creating a new schema instance for every transformation.

When transformation involves conversion between schema versions a resolver object is necessary.
Resolvers are created automatically and cached.
A cached resolver is purged as soon as either the source or the destination schema object is destroyed.
