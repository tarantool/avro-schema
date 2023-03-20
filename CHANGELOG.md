# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2023-03-20
### Added
- [Add versioning support.](https://github.com/tarantool/avro-schema/pull/147)
### Changed
- [Use git+https:// for git repository in rockspec](https://github.com/tarantool/avro-schema/pull/143)
- [RPM build to support Fedora 33+](https://github.com/tarantool/avro-schema/issues/148)


## [3.0.6] - 2021-09-24
### Changed
- [Fixed compatibility with tarantool-2.5.1](https://github.com/tarantool/avro-schema/issues/135),
  where `table.clear()` should be required explicitly (it is tarantool's bug, but anyway)
- [Don't reconfigure default JSON serializer](https://github.com/tarantool/avro-schema/issues/140)
- [Don't accept a string value for a field of 'long' type in a schema](https://github.com/tarantool/avro-schema/issues/133)
- [Improved `validate()` performance.](https://github.com/tarantool/avro-schema/pull/134)
  (It speeds up on near 6% on our benchmark.)


## [3.0.5] - 2020-02-27
### Changed
- [Full hotreload support](https://github.com/tarantool/avro-schema/issues/34)


## [3.0.4] - 2019-11-12
### Changed
- [Support hotreload (partially)](https://github.com/tarantool/avro-schema/issues/34)
- [Fix compilation of a large schema](https://github.com/tarantool/avro-schema/issues/124)


## [3.0.3] - 2018-11-12
### Changed
- [Fix wrong array validation](https://github.com/tarantool/avro-schema/issues/117)


## [3.0.2] - 2018-10-14
### Changed
- [Treat nil as box.NULL when validate a union](https://github.com/tarantool/avro-schema/issues/113)


## [3.0.1] - 2018-09-28
### Added
- Allow default values for records, nullable records, unions
  ([#99](https://github.com/tarantool/avro-schema/issues/99),
  [595fe703](https://github.com/tarantool/avro-schema/commit/595fe703b5c2ce6624e8f3dfa752e787c97d0462),
  [9d853a79](https://github.com/tarantool/avro-schema/commit/9d853a795c78259d27986db2fb16a349f2457b7c))
- Comments to codebase
### Changed
- [Fix temp_msgpack_gc/flatten race](https://github.com/tarantool/avro-schema/issues/109)
- [Fix stack restore for `validate` error handling](https://github.com/tarantool/avro-schema/issues/11)
- [Fix schema evolution for nullable fields](https://github.com/tarantool/avro-schema/issues/76)
- [Fix installation via luarocks](https://github.com/tarantool/avro-schema/commit/6fbd4d6092f96a2dfad254a89eb85d829d89938d)
- Code refactoring
- Deleted unused register (ripv)
- Extend documentation


## [3.0.0] - 2018-05-08
### Added
- Error opcode for runtime exceptions
### Changed
- [Change nullable flatten/unflatten/xflatten](https://github.com/tarantool/avro-schema/issues/63)
  - scalar nullable types are encoded with null or value
  - nullable record encoded with null or array of field values
  - xflatten for nullable record is in alpha stage
- `get_names`, `get_types` changed
  ([#58](https://github.com/tarantool/avro-schema/issues/58),
  [#56](https://github.com/tarantool/avro-schema/issues/56))
  - add nullable type support
    - scalars are exported as `string*`
    - nullable record is exported just as `record*` string
  - api changes
    - fixed field is exported as `fixed` (instead of its name)
    - union is exported as `union_type`, `union_value`
    - support `service_fields`
    - add `get_*` methods to `compiled` object
- Give variables the same names in IR and in resulting Lua
- Fix nullable field is not mandatory in flatten
- Fix flatten for variable size types


## [2.3.2] - 2018-05-04
### Changed
- Fix boolean field validation


## [2.3.1] - 2018-04-19
### Changed
- [Fix one of null/non-null type tables is not initialized](https://github.com/tarantool/avro-schema/issues/77)
- [Fix `preserve_in_ast` for record fields](https://github.com/tarantool/avro-schema/issues/78)
- [Fix collapse nullable scalar on export](https://github.com/tarantool/avro-schema/issues/74)


## [2.3.0] - 2018-04-19
### Added
- [Forward type reference](https://github.com/tarantool/avro-schema/issues/48)
### Changed
- Improve benchmark script
- [Fix nullable type reference export](https://github.com/tarantool/avro-schema/issues/49)
- [Fix nullable type name export](https://github.com/tarantool/avro-schema/issues/38)
- [Fix fingerprints for type references](https://github.com/tarantool/avro-schema/issues/52)
- [Make `preserve_in_ast` work at any place of a schema](https://github.com/tarantool/avro-schema/issues/73)


## [2.2.3] - 2018-04-16
### Changed
- [Fix nullability for `fixed` type](https://github.com/tarantool/avro-schema/issues/55)
- [Treat lack of value for union field as null for `validate`](https://github.com/tarantool/avro-schema/issues/64)
- [Make float validation rules stricter](https://github.com/tarantool/avro-schema/issues/60)


## [2.2.2] - 2018-04-06
### Added
- [Tests for any type](https://github.com/tarantool/avro-schema/issues/47)
### Changed
- [Fix nullable float type validation](https://github.com/tarantool/avro-schema/issues/50)


## [2.2.1] - 2018-03-26
### Changed
- Fixed OSX support


## [2.2.0] - 2018-03-26
### Added
- Support for UTF-8 Enum value
### Changed
- [Fix nullable types inside of arrays](https://github.com/tarantool/avro-schema/issues/37)


## [2.1.0] - 2018-02-24
### Added
- Allow NIL values for nullable types
- Introduced model fingerprint
- Allow to preserve extra fields in AST and fingerprint
