# Testing

There are several kinds of tests:

 1. DDT tests;
 2. API tests;
 3. IL optimizer tests.
 
**So-called DDT**, or *data-driven-tests*, cover the majority of features.
Every test in this category performs the same sequence of steps:

 * create a schema;
 * validate data using a schema object (optional);
 * compile schema;
 * convert data using a compiled schema.
 
The reason it is called *data-driven* is because these tests are configured with *data*,
ex:

```lua
t {
    schema = '["int", "string", "double"]',
    func   = 'unflatten', input = '[3, 42]'
    error  = '1: Bad value: 3',
}
```

**API tests** While DDT tests cover the majority of *semantic* features, they all use
virtually the same set of API calls. Auxiliary calls and incorect API usage aren't covered.
This is where *API tests* come into play.

**IL optimizer tests** ensure that code transformations in the IL optimizer are correct.

## DDT Tests

### Schema Creation

`invalid.lua` — all sorts of misspelled schemas. Valid schemas are implicitly covered by other tests.

### Data Validation with a Schema Object

`validate.lua`

### Schema Mapping (Versions)

`incompatible.lua` 
`incompatible_array.lua` 
`incompatible_enum.lua` 
`incompatible_fixed.lua` 
`incompatible_map.lua` 
`incompatible_record.lua`

Valid schema mappings are implicitly covered by other suites.
For named type, `aliases` feature is tested, including `downgrade` mode.

### Generated Code: Basic Types

`array.lua`
`boolean.lua`
`bytes.lua`
`double.lua`
`fixed.lua`
`float.lua`
`int.lua`
`long.lua`
`string.lua`
`map.lua`
`null.lua`

Basic types in runtime and generated code, including type mismatches.

### Generated Code: Type Promotions

`bytes_promo.lua`
`float_promo.lua`
`int_promo.lua`
`long_promo.lua`
`string_promo.lua`

Type promotions in runtime and generated code, including type mismatches.

### Generated Code: Enum

`enum.lua`
`enum_versions.lua`

Enums: type mismatches, on-the-fly conversion from one schema revision to another.

`enum_large.lua`

Ridiculously large enum.

### Generated Code: Record

`record_array.lua`
`record_hidden.lua`
`record_large.lua`
`record.lua`
`record_nested.lua`
`record_union.lua`
`record_version.lua`
`record_vlo.lua`

Records in generated code, flattening nested records and enums, `xflatten`,
on-the-fly conversion from one schema revision to another. Hidden fields.
Large record.

### Generated Code: Union

`union.lua`
`union_versions.lua`

### Misc Tests

`recursive.lua` — recursive schemas.

`service_fields.lua`

## DDT Tests: Implementation Details and Usage

Any file matching `ddt_suite/*.lua` pattern is a test bundle.

Each file invokes `t()` several times, which is the testing function.
The function receives a dictionary. It knows about the following keys:

  * `schema`, `schema1`, `schema2` — schema(s) definition, JSON string;
  * `create_error` — expected error message in schema create, a string;
  * `create_only` — don't run further steps;
  * `validate` — data to validate, JSON string or a value as-is;
  * `validate_error` — expected validation failure message, a string;
  * `validate_only` — stop here;
  * `compile_downgrade`, `service_fields` — compilation options;
  * `compile_dump` — dump compilation artefacts (compiler troubleshooting);
  * `compile_error` — expected compilation error, a string;
  * `compile_only` — stop now;
  * `func` — a function to invoke, `"flatten"` / `"unflatten"` / `"xflatten"`;
  * `input`, `output` — one value or a list of values (for service fields);
  * `error`
  
### Test Names

Each test is automatically given a name after the containing bundle and the corresponding line that called `t()`, ex:

`record_array/40`

Sometimes, it is handy to perform the same test repeatedly in a loop with a parameter varying.
Having this use case in mind, we automatically include `key=value` piece in a name for each global variable
defined in a test bundle, ex:

`enum_large/i_1/36`

Translation: the test lives in `enum_large.lua`, line 36. This particular test instance was invoked with `i=1`.

### Input and Output

Input and output are either a single value or a list of values. 
That single value or a first value in a list is an extended JSON string, converted to MsgPack
before passing to `flatten` / `unflatten` / `xflatten` or comparing with the result.
Second and further values are used as is.

JSON extensions:
 * leading `!` to encode using floats instead of doubles, ex: `!42.0`;
 * MongoDB-inspired `{"$binary": "DEADBEAF"}` for binary data.
 
The key order in the resulting MsgPack data is exactly the same as in the JSON string.
`42` encodes as `int` while `42.0` encodes as `double`/`float`.

Stock JSON/MsgPack modules lack the features necessary to implement the encoding defined above.
For this reason, the conversion is implemented with an external tool, `msgpack_helper.py` (Python 3).
The results are cached in `.ddt_cache` (added to the repository) to improve performance
and to make it possible to run tests in an environment without Python 3.
