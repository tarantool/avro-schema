// We need a function to initialize AVRO data IR from various input
// formats (e.g. Lua data or msgpack or JSON).  This function is
// implemented using a template parametrized with 'parser_context' type,
// the later encapsulating the specifics of a particular input format.
//
// Below we declare a fictional 'example_parser', consider it the
// interface documentation.

class example_parser;
class example_array_parser;
class example_map_parser;
class example_union_parser;
class example_verbose_record_parser;
class example_terse_record_parser;

// Encapsulates the parser state. Sets up the necessary parsing
// machinery in ctor and tears it down in dtor.  Provides typedefs for
// various parsers used to digest the content.
class example_parser_context: private noncopyable
{
public:
	typedef example_parser                parser;
	typedef example_array_parser          array_parser;
	typedef example_map_parser            map_parser;
	typedef example_union_parser          union_parser;
	typedef example_verbose_record_parser verbose_record_parser;
	typedef example_terse_record_parser   terse_record_parser;
};

// Base parser class. All methods advance the input position hence
// 'consume' moniker.
//
// Parsers are non-copyable due to setup/teardown works performed in
// ctor/dtor. Errors are reported with exceptions.
//
// Parsers for the nested objects are initialized with a parser context
// obtained from the parent parser. While child parser is active parent
// parsers MUST remain inactive.
//
// To adopt to the inevitable differences in input formats some methods
// signatures are 'flexible' allowing different return types.
// E.g. consume_enum() returns Bytes if the format stores
// enums literally or int otherwise. User bridges the gap using
// overloaded functions. For instanse, resolve_enum(int) is a no-op
// while resolve_enum(Bytes) looks up the string in a name table.
class example_parser: private noncopyable
{
public:
	example_parser(example_parser_context &);
	// perform aditional validation before teardown
	// and whatever cleanup that is to be skipped in the case of
	// exception thrown
	void    kill();

	// skip the current value, whatever it is
	void    consume_any();

	// primitive types defined in AVRO spec
	void    consume_null();
	bool    consume_boolean();
	int32_t consume_int();
	int64_t consume_long();
	float   consume_float();
	double  consume_double();
	Bytes   consume_bytes();
	Bytes   consume_fixed(size_t);

	// Bytes
	Bytes   consume_string();

	// Bytes or int
	Bytes   consume_enum();

	example_parser_context& context();
};

// Inspired by cursor concept, hence the next() method.
class example_array_parser: public example_parser
{
public:
	example_array_parser(example_parser_context &);
	bool next();
};

class example_map_parser: public example_parser
{
public:
	example_map_parser(example_parser_context &);
	bool next();
	// Bytes
	Bytes key();
};

class example_union_parser: public example_parser
{
public:
	example_union_parser(example_parser &);
	// Bytes or int
	Bytes tag();
};

// Verbose record parser deals with records that are stored with
// explicit field tags or names.
class example_verbose_record_parser: public example_parser
{
public:
	example_verbose_record_parser(example_parser &);
	bool next();
	// Bytes or int
	Bytes key();
};

// Terse record parser deals with records that store fields in the order
// defined by the schema, no names or tags.
class example_terse_record_parser: public example_parser
{
public:
	example_terse_record_parser(example_parser &);
	void next();
};
