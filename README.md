# avro-schema [![Build Status](https://travis-ci.org/tarantool/avro-schema.svg?branch=master)](https://travis-ci.org/tarantool/avro-schema)
[Apache Avro](http://avro.apache.org/docs/1.8.0/spec.html) schema tools for Tarantool, implemented from scratch in Lua

Notable features:
 * Avro defaults;
 * Avro aliases;
 * data transformations are fast due to runtime code generation;
 * extensions such as built-in nullable types.

```lua
avro_schema = require('avro_schema')
```

## Creating a Schema
```lua
ok, schema = avro_schema.create {
    type = "record",
    name = "Frob",
    fields = {
      { name = "foo", type = "int", default = 42 },
      { name = "bar", type = "string" }
    }
  }
```
Creates a schema object (`ok == true`). If there was a syntax error, returns `false` and the error message.

## Validating and Normalizing Data with a Schema
```lua
ok, normalized_data_copy = avro_schema.validate(schema, { bar = "Hello, world!" })
```
Returns `true` if the data was valid. Otherwise, returns `false` and the error message.

The `avro_schema.validate()` function creates a normalized copy of the data. Normalization implies
filling in default values for missing fields.
For example, because the "foo" field has a default value = 42,
the result from the above example will be { foo = 42, bar = "Hello, world!" }.

## Checking if Schemas are Compatible
To facilitate data evolution Avro defines certain schema mapping rules.
If schemas `A` and `B` are compatible, then one can convert data from `A` to `B`.
```lua
ok = avro_schema.are_compatible(schema1, schema2)
ok = avro_schema.are_compatible(schema2, schema1, "downgrade")
```
Allowed modifications include:
  1. renaming types and record fields (provided that `aliases` are correctly set);
  2. extending records with new fields (these fields are initialized with default values, which are mandatory);
  3. removing fields (contents are simply removed during conversion);
  4. modifying unions and enums (provided that type definitions retain some similarity);
  5. type promotions are allowed (e.g. `int` is compatible with `long` but not vice versa).

Let's assume that `B` is newer than `A`. `A` defines `Apple` (a record type). `B` renames it to `Banana`.
Upgrading data from `A` to `B` works, since `Banana` is marked as an alias of `Apple`.
However, downgrading data from `B` to `A` does not work, since in `A` the record type `Apple` has no aliases.

To make it work we implement `downgrade` mode. In downgrade mode, name mapping rules
take into account the aliases in the source schema,
and ignore the aliases in the target schem.
 
## Checking if an object is a schema object

```lua
avro_schema.is(object)
```

## Querying a schema's field names or field types

```lua
avro_schema.get_names(schema [, service-fields])
```

```lua
avro_schema.get_types(schema [, service-fields])
```

The first argument must be a schema object, such as the one created in the ``Creating a Schema`` example above.
The optional second argument is a table with names of types, such as {'string', 'int'}.
The result will be a Lua table of field names (for the get_names method)
or a Lua table of field types (for the get_types method).
The order will match the field order in the flat representation.

## Compiling Schemas
Compiling a schema creates optimized data conversion routines (runtime code generation).
```lua
ok, methods = avro_schema.compile(schema)
ok, methods = avro_schema.compile({schema1, schema2})
```
If two schemas are provided, then the generated routines consume data in `schema1` and produce results in `schema2`.

What if the schema1 source and the schema2 destination are not adjacent revisions, i.e. there were some revisions in between?
While going from source to destination directly is fast, sometimes it alters the results. Performing conversion
step by step, using all the in-between revisions, always yields correct results but it is slow.

There is a third option: let `compile` generate routines that are fast yet produce the correct results.

## Compile Options
A few options affecting compilation are recognized.

Enabling `downgrade` mode (see `avro_schema.are_compatible` for details):
```lua
ok, methods = avro_schema.compile({schema1, schema2, downgrade = true})
```

Dumping generated code for inspection:
```lua
ok, methods = avro_schema.compile({schema1, schema2, dump_src = "output.lua"})
```

Troubleshooting code generation issues:
```lua
ok, methods = avro_schema.compile({schema1, schema2, debug = true, dump_il = "output.il"})
```

Add service fields (which are part of a tuple, but are not part of an object):

```lua
ok, methods = avro_schema.compile({schema, service_fields = {'string', 'int'}})
```

## Generated Routines
`Compile` produces the following routines (returned in a Lua table):
  * `flatten`
  * `unflatten`
  * `xflatten`
  * `flatten_msgpack`
  * `unflatten_msgpack`
  * `xflatten_msgpack`
  * `get_types`
  * `get_names`

Here is an example which uses the avro schema that we described in
the section `Creating a Schema`, a Tarantool database space, and
the methods that `compile` produces. This is a script that you
can paste into a client of a Tarantool server; the comments explain
what the results look like and what they mean.

```lua
-- Create a Tarantool database, an index, and a tuple
box.schema.space.create('T')
box.space.T:create_index('I')
box.space.T:insert{1, 'string-value'}
-- Let tuple_1 = a tuple from the database space
tuple_1 = box.space.T:get(1)
-- Load the module
avro_schema = require('avro_schema')
-- Load avro_schema and create a schema as described earlier
ok, schema = avro_schema.create {
    type = "record",
    name = "Frob",
    fields = {
      { name = "foo", type = "int", default = 42 },
      { name = "bar", type = "string" }
    }
  }
-- Compile, so that "methods" will have the generated routines
ok, methods = avro_schema.compile(schema)
-- Invoke unflatten(). The result will look like this:
-- - {'foo': 1, 'bar': 'string-value'}
-- That is: unflattening can turn tuples into avro-schema objects.
ok, result = methods.unflatten(tuple_1)
result
-- Make a new Lua table with an integer and a string component
-- table_1 = {42, 'string-value-2'}
-- Invoke flatten(). The result can be inserted into the database.
-- The value of the newly inserted tuple will look like this:
-- - [1, 'string-value']
-- That is, flattening can turn avro-schema objects into tuples.
ok, tuple_2 = methods.flatten(result)
box.space.T:truncate()
box.space.T:insert(tuple_2)
-- Make an avro_schema object with {foo=2, bar='Hello, World!'}
ok, normalized_data_copy = avro_schema.validate(schema, { bar = "Hello, world!" })
-- Invoke xflatten(). The result will look like this:
-- - [['=', 1, 42], ['=', 2, 'Hello, world!']]
ok, result = methods.xflatten(normalized_data_copy)
result
-- That is, the format of an xflatten() result is exactly
-- what a Tarantool "update" request looks like.
-- Therefore let's put it in an update request ...
box.space.T:update({42},result)
-- And the result looks like:
-- -- - [1, 'Hello, world!']
```

So: with `flatten()` for inserting, `xflatten()` for updating,
`unflatten()`
for getting, we have ways to use avro_schema objects as tuples in
Tarantool databases.

With the other three methods that work with transformations of
avro_schema objects  -- `flatten_msgpack()` and `xflatten_msgpack()` and
`unflatten_msgpack()` --  we have similar functionality,
except that the transformations are to
and from MsgPack objects.
(The ..._msgpack() methods are usually faster because
they do not need to encode or decode internally.)

The final two methods -- `get_types()` and `get_names()` -- have almost the
same effect as `get_types()` and `get_names()` described in the earlier
section "Querying a schema's field names or field types".
(The main difference is that the optional "service_fields" argument
is unnecessary if `methods` is the result of a compile done with
the "service_fields =" option.) For example:

```lua
tarantool> methods.get_names()
---
- - foo
  - bar
...
tarantool> methods.get_types()
---
- - int
  - string
...
```

## References

Named types are ones that have mandatory `name` fields in their definitions:
record, fixed, enum.

Named types can be referenced after the first definition (in depth-first,
left-to-right traversal).

Example:

```
{
    name = 'user',
    type = 'record',
    fields = {
        {name = 'uid', type = 'long'},
        {
            name = 'nested',
            type = {
                type = 'record',
                name = 'nested_record',
                fields = {
                    {name = 'x', type = 'long'},
                    {name = 'y', type = 'long'}
                }
            }
        },
        {
            name = 'another_nested',
            type = 'nested_record'
        }
    }
}
```

Notes:

* A reference is a usage of a type (not a value), so the effect is as if you
  define the same type with an a different name.
* A field of a record also has a name, but it is not a type, so you cannot
  reference a field by its name.
* A record can be referenced from within itself only as part of a union or an
  array.
* An array and a map are unnamed and cannot be referenced by a name, consider
  related discussions below.

### Related discussions

* [[Avro-user] Why Array and Map are not named type ?][1].
* [AEP 102 - Named Unions][2].

## Nullability (extension)

The problem: in database management systems NULL is a value, not a type.
So it should be possible, for example, to have a "long integer" type that
can contain both NULL and integers.
One can try to handle this with a union such as `{'null', 'long'}` which
can have both `null` and `{long = 42}`. What really is necessary, though,
is that a single field, whose name determines the type, can contain both
`null` and `42` as valid values (see the [JSON Encoding][3]
section of the avro-schema standard). This problem -- expressing a single
type that accepts both  `null` and `42` -- is the problem that the
nullability extension solves.

A type can be marked as nullable by adding an asterisk ("*") at the end of the type name:

```lua
{
    name = 'user',
    type = 'record',
    fields = {
        {name = 'uid', type = 'long'},
        {name = 'first_name', type = 'string'},
        {name = 'middle_name', type = 'string*'},
        {name = 'last_name', type = 'string'}
    }
}
```

The following types can be marked as nullable:

* All primitive types: null, boolean, int, long, float, double, bytes, string.
* All named complex types: record, fixed, enum.
* Almost all unnamed complex types: array, map (but not union).

Notes:

* A type reference can be non-nullable or nullable (asterisk-marked)
  independently of the original type definition.
* Use standard `{'null', ...}` without an asterisk to make a union nullable type.
* The xflatten method is not designed to work with complex nullable types.

...

[1]: http://grokbase.com/t/avro/user/108svyaz63/why-array-and-map-are-not-named-type
[2]: https://cwiki.apache.org/confluence/display/AVRO/AEP+102+-+Named+Unions
[3]: http://avro.apache.org/docs/1.8.2/spec.html#json_encoding

