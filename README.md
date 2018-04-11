# avro-schema [![Build Status](https://travis-ci.org/tarantool/avro-schema.svg?branch=master)](https://travis-ci.org/tarantool/avro-schema)
[Apache Avro](http://avro.apache.org/docs/1.8.0/spec.html) schema tools for Tarantool, implemented from scratch in Lua

Notable features:
 * Avro defaults;
 * Avro aliases;
 * data transformations are fast due to runtime code generation.

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

## Validating Data by a Schema
```lua
ok, normalized_data_copy = avro_schema.validate(schema, { bar = "Hello, world!" })
```
Returns `true` if the data was valid. If not, returns `false` and the error message.

The function creates a normalized copy of the data. Normalization implies
filling in default values for missing fields.

## Checking if Schemas are Compatible
To facilitate data evolution Avro defines certain schema mapping rules.
If schemas `A` and `B` are compatible, one can convert data from `A` to `B`.
```lua
ok = avro_schema.are_compatible(schema1, schema2)
ok = avro_schema.are_compatible(schema2, schema1, "downgrade")
```
Allowed modifications include:
  1. renaming types and record fields (provided that `aliases` are correctly set);
  2. extending records with new fields (these fields are initialized with default values, which are mandatory);
  3. removing fields (data simply dropped during conversion);
  4. modifying unions and enums (provided that type definitions retain some similarity);
  5. type promotions are allowed (e.x. `int` is compatible with `long` but not vice versa).

Let's assume that `B` is newer than `A`. `A` defines `Apple` (a record type). `B` renames it to `Banana`.
Upgrading data from `A` to `B` is possible, since `Banana` is marked as an alias of the `Apple`.
Unfortunately, downgrading doesn't work since in `A` the record type `Apple` has no aliases.

To make it work we implement `downgrade` mode. In the downgrade mode, name mapping rules consider
aliases in the source schema (while ignoring aliases in the target schema).

## Compiling Schemas
Compiling a schema creates optimized data conversion routines (runtime code generation).
```lua
ok, methods = avro_schema.compile(schema)
ok, methods = avro_schema.compile({schema1, schema2})
```
If two schemas were provided, generated routines consume data in `schema1` and produce results in `schema2`.

What if the source and destination schemas aren't adjacent revisions, i.e. there were some revisions in-between?
While going from source to destination directly is fast, sometimes it alters the results. Performing conversion
step by step always yields correct results but it is slow.

```lua
ok, methods = avro_schema.compile({schema1, ... schemaN})
```

There is the third option: let `compile` build routines that are fast yet produce the correct results.

## Compile Options
A few options affecting compilation are recognized.

Enabling `downgrade` mode (see `avro_schema.are_compatible` for details):
```lua
avro_schema.compile({schema1, schema2, downgrade = true})
```

Dumping generated code for inspection:
```lua
avro_schema.compile({schema1, schema2, dump_src = "output.lua"})
```

Troubleshooting codegen issues:
```lua
avro_schema.compile({schema1, schema2, debug = true, dump_il = "output.il"})
```

## Generated Routines
`Compile` produces the following routines (returned in a Lua table):
  * `flatten`
  * `unflatten`
  * `flatten_msgpack`
  * `unflatten_msgpack`

## Miscelania
Checking if an object is a schema:
```lua
avro_schema.is(object)
```

Quering schema field names (the order matches the field order in the flat representation):
```lua
avro_schema.get_names(schema)
```

Quering schema field types (the order matches `get_names`):
```lua
avro_schema.get_types(schema)
```

## References

Named types are ones that have mandatory `name` field in the definition:
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

* A reference is a usage of a type (not a value), so the effect is like you
  define the same type with an another name.
* A field of a record also has a name, but it is not a type, so you cannot
  reference a field by a name.
* A record can be referenced inside of itself only as part of a union or an
  array.
* An array and a map are unnamed and cannot be referenced by a name, consider
  related discussions below.

### Related discussions

* [[Avro-user] Why Array and Map are not named type ?][1].
* [AEP 102 - Named Unions][2].

## Nullability (extension)

The problem statement: the union like `{'null', 'long'}` assumed valid values
like `null` and `{long = 42}`. In other words, valid values are `null` and an
object with one field, whose name determines the type (see [JSON Encoding][3]
section of the avro-schema standard). We cannot express a type that accepts
`null` or `42`. That is the problem that is solved by the nullability
extension.

A type can be marked as nullable using asterisk symbol after the type:

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

The following types can be marked as a nullable:

* All primitive types: null, boolean, int, long, float, double, bytes, string.
* All named complex types: record, fixed, enum.
* Almost all unnamed complex types: array, map (except union).

Notes:

* A type reference can be non-nullable or nullable (asterisk-marked)
  independently of the original type definition.
* Use standard `{'null', ...}` to make a union nullable type.

[1]: http://grokbase.com/t/avro/user/108svyaz63/why-array-and-map-are-not-named-type
[2]: https://cwiki.apache.org/confluence/display/AVRO/AEP+102+-+Named+Unions
[3]: http://avro.apache.org/docs/1.8.2/spec.html#json_encoding
