#include <cassert>
#include <stdexcept>
#define MP_SOURCE
#include <msgpuck.h>

struct noncopyable
{
	noncopyable() = default;
	noncopyable(const noncopyable &) = delete;
	void operator = (const noncopyable &) = delete;
};

// Opaque blob
struct Bytes
{
	Bytes() = default;
	Bytes(const void *p_, size_t length_):
		p(reinterpret_cast<const uint8_t *>(p_)),
		length(length_)
	{}
	const uint8_t *p;
	size_t         length;
};

struct CStringBuf
{
	char buf[128];
	operator const char * () const { return buf; }
};

CStringBuf bytes_to_cstring(const Bytes &b)
{
	CStringBuf res;
	size_t len = std::min((sizeof res.buf) - 1, b.length); // XXX
	memcpy(res.buf, b.p, len);
	res.buf[len] = '\0';
	return res;
}

avro_wrapped_buffer_t bytes_to_avro_buffer(const Bytes &b)
{
	avro_wrapped_buffer_t res = {
		b.p, b.length, nullptr, nullptr, nullptr, nullptr
	};
	return res;
}

#define PARSER_ABORT_FUNC_ATTRIBUTES \
	__attribute__((__noreturn__, __cold__, __noinline__))

void internal_error() PARSER_ABORT_FUNC_ATTRIBUTES;
void parse_error()    PARSER_ABORT_FUNC_ATTRIBUTES;
void type_mismatch()  PARSER_ABORT_FUNC_ATTRIBUTES;
void name_unknown()   PARSER_ABORT_FUNC_ATTRIBUTES;
void circular_ref()   PARSER_ABORT_FUNC_ATTRIBUTES;
void stack_overflow() PARSER_ABORT_FUNC_ATTRIBUTES;

void internal_error()
{
	throw std::runtime_error("internal error");
}

void parse_error()
{
	throw std::runtime_error("parse error");
}

void type_mismatch()
{
	throw std::runtime_error("type mismatch");
}

void name_unknown()
{
	throw std::runtime_error("name unknown");
}

void circular_ref()
{
	throw std::runtime_error("circular ref");
}

void stack_overflow()
{
	throw std::runtime_error("stack overflow");
}

void record_index_error(const avro_schema_t, int)
		      PARSER_ABORT_FUNC_ATTRIBUTES;
void union_tag_error(const avro_schema_t, int)
		      PARSER_ABORT_FUNC_ATTRIBUTES;
void enum_value_error(avro_value_t *, int)
		      PARSER_ABORT_FUNC_ATTRIBUTES;

// flags controlling things
enum {
	PARSER_ASSUME_NUL_TERM_STRINGS   = 1,
	PARSER_ASSUME_NON_NUL_CHARS      = 2,
	PARSER_ASSUME_UTF8_STRINGS       = 4,
	PARSER_ASSUME_NO_DUP_MAP_KEYS    = 8,
	PARSER_ENABLE_FAST_SKIP          = 16,
	PARSER_ENABLE_VERBOSE_RECORDS    = 32,
	PARSER_ENABLE_TERSE_RECORDS      = 64,
	PARSER_ENABLE_COLLAPSE_NESTED    = 128,
	PARSER_TERSE_RECORDS_IMPLY_COLLAPSE_NESTED = 256
};

void check_utf8_string(int flags, const Bytes &b)
{
	if (flags & PARSER_ASSUME_UTF8_STRINGS)
		return;
	// XXX
	(void)b;
}

int resolve_record_field(int flags, const avro_schema_t schema, int index)
{
	(void)flags;
	(void)schema;
	return index;
}

int resolve_record_field(int flags, const avro_schema_t schema, const Bytes &b)
{
	int index;
	if (flags & PARSER_ASSUME_NUL_TERM_STRINGS) {
		// XXX embeded NULs
		index = avro_schema_record_field_get_index(
			schema, reinterpret_cast<const char*>(b.p));
	} else {
		index = avro_schema_record_field_get_index(
			schema, bytes_to_cstring(b));
	}
	if (index < 0)
		name_unknown();
	return index;
}

int resolve_union_tag(int flags, const avro_schema_t schema, int index)
{
	(void)flags;
	(void)schema;
	return index;
}

int resolve_union_tag(int flags, const avro_schema_t schema, const Bytes &b)
{
	int index;
	if (flags & PARSER_ASSUME_NUL_TERM_STRINGS) {
		// XXX embeded NULs
		if (!avro_schema_union_branch_by_name(
				schema, &index,
				reinterpret_cast<const char*>(b.p)))
			name_unknown();
	} else {
		if (!avro_schema_union_branch_by_name(
				schema, &index, bytes_to_cstring(b)))
			name_unknown();
	}
	return index;
}

int resolve_enum_value(int flags, const avro_schema_t schema, int index)
{
	(void)flags;
	(void)schema;
	return index;
}

int resolve_enum_value(int flags, const avro_schema_t schema, const Bytes &b)
{
	int index;
	if (flags & PARSER_ASSUME_NUL_TERM_STRINGS) {
		// XXX embeded NULs
		index = avro_schema_enum_get_by_name(
			schema, reinterpret_cast<const char*>(b.p));
	} else {
		index = avro_schema_enum_get_by_name(
			schema, bytes_to_cstring(b));
	}
	if (index < 0)
		name_unknown();
	return index;
}

// Enable optimizer to drop a call if the result is unused.
avro_schema_t pure_value_get_schema(avro_value_t *)
	__attribute__((__pure__));

avro_schema_t pure_value_get_schema(avro_value_t *v)
{
	return avro_value_get_schema(v);
}

avro_schema_t pure_schema_record_field_get_by_index(const avro_schema_t, int)
	__attribute__((__pure__));

avro_schema_t pure_schema_record_field_get_by_index(const avro_schema_t s, int i)
{
	return avro_schema_record_field_get_by_index(s, i);
}

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

//
// Lua parser
//

class lua_parser;
class lua_array_parser;
class lua_map_parser;
class lua_union_parser;
class lua_terse_record_parser;

class lua_parser_context: private noncopyable
{
	friend class lua_parser;
public:
	typedef lua_parser                  parser;
	typedef lua_array_parser            array_parser;
	typedef lua_map_parser              map_parser;
	typedef lua_union_parser            union_parser;
	typedef lua_map_parser              verbose_record_parser;
	typedef lua_terse_record_parser     terse_record_parser;

	lua_parser_context(struct lua_State *L, int root_value_index)
		: L_(L)
	{
		lua_pushvalue(L, root_value_index);
		crct_index_ = lua_gettop(L);
		lua_newtable(L);
		lua_insert(L, -2);
	}
	~lua_parser_context()
	{
		lua_pop(L_, 1);
	}
private:
	struct lua_State *L_;
	int               crct_index_; // circular ref checker table
};

class lua_parser: private noncopyable
{
public:
	typedef lua_parser_context context_type;
	lua_parser(lua_parser_context &context)
		: context_(context), L_(context.L_)
	{
		if (!lua_checkstack(L(), 4))
			stack_overflow();
	}
	void    kill()
	{
	}
	void    on_before_consume()
	{
	}
	void    consume_any()
	{
		lua_pop(L(), 1);
	}
	void    consume_null()
	{
		// XXX fixme
		type_mismatch();
	}
	bool    consume_boolean()
	{
		if (!lua_isboolean(L(), -1))
			type_mismatch();
		bool b = lua_toboolean(L(), -1);
		lua_pop(L(), 1);
		return b;
	}
	int32_t consume_int() { return consume_long(); }
	int64_t consume_long()
	{
		if (!lua_isnumber(L(), -1))
			type_mismatch();
		lua_Integer i = lua_tointeger(L(), -1);
		lua_pop(L(), 1);
		return i;
	}
	float   consume_float() { return consume_double(); }
	double  consume_double()
	{
		if (!lua_isnumber(L(), -1))
			type_mismatch();
		lua_Number n = lua_tonumber(L(), -1);
		lua_pop(L(), 1);
		return n;
	}
	Bytes   consume_bytes()
	{
		if (!lua_isstring(L(), -1))
			type_mismatch();
		size_t len;
		const char *s = lua_tolstring(L(), -1, &len);
		lua_pop(L(), 1);
		return Bytes(s, len);
	}
	Bytes   consume_fixed(size_t len)
	{
		Bytes bytes = consume_bytes();
		if (bytes.length != len)
			type_mismatch();
		return bytes;
	}
	Bytes   consume_string() { return consume_bytes(); }
	Bytes   consume_enum() { return consume_string(); }

	lua_parser_context& context() { return context_; }
protected:
	struct lua_State *L() { return L_; }
	void note_table();
	void forget_table();
private:
	lua_parser_context &context_;
	struct lua_State   *L_;
};

lua_parser &erase_type(lua_parser &parser) { return parser; }

class lua_array_parser: public lua_parser
{
public:
	lua_array_parser(lua_parser_context &context)
		: lua_parser(context), index_(1)
	{
		note_table();
	}
	void kill()
	{
		// assert(at_end());
		forget_table();
		lua_pop(L(), 1);
		lua_parser::kill();
	}
	bool next()
	{
		assert(!at_end());
		lua_rawgeti(L(), -1, index_);
		if (lua_isnil(L(), -1)) {
			lua_pop(L(), 1);
			index_ = -1;
			return false;
		}
		index_ += 1;
		return true;
	}
protected:
	bool at_end() { return index_ == (size_t)-1; }
private:
	size_t index_;
};

class lua_map_parser: public lua_parser
{
public:
	lua_map_parser(lua_parser_context &context)
		: lua_parser(context), at_end_(false)
	{
		note_table();
		lua_pushnil(L());
	}
	void kill()
	{
		assert(at_end_);
		forget_table();
		lua_pop(L(), 1);
		lua_parser::kill();
	}
	bool next()
	{
		assert(!at_end_);
		if (lua_next(L(), -2) == 0) {
			at_end_ = true;
			return false;
		}
		// lua_isstring returns true for numbers with tolstring
		// subsequently converting number to string inplace,
		// confusing lua_next
		if (lua_type(L(), -2) != LUA_TSTRING)
			type_mismatch();
		size_t len;
		const char *s = lua_tolstring(L(), -2, &len);
		key_ = Bytes(s, len);
		return true;

	}
	Bytes key() { return key_; }
private:
	Bytes key_;
	bool  at_end_;
};

class lua_union_parser: public lua_array_parser
{
public:
	lua_union_parser(lua_parser_context &context)
		: lua_array_parser(context)
	{
		// expecting an array consisting of exactly 2 elements
		if (!next() || !next() || next() || !lua_isstring(L(), -2))
			type_mismatch();
		size_t len;
		const char *s = lua_tolstring(L(), -2, &len);
		tag_ = Bytes(s, len);
	}
	void kill()
	{
		lua_pop(L(), 1); // pop tag
		lua_array_parser::kill();
	}
	Bytes tag() { return tag_; }
private:
	Bytes tag_;
};

class lua_terse_record_parser: public lua_array_parser
{
public:
	lua_terse_record_parser(lua_parser_context &context)
		: lua_array_parser(context)
	{
	}
	void kill()
	{
		// if (lua_array_parser::next())
		//	type_mismatch();
		lua_array_parser::kill();
	}
	void next() {
		if (!lua_array_parser::next())
			type_mismatch();
	}
};

void lua_parser::note_table()
{
	if (!lua_istable(L(), -1))
		type_mismatch();
	lua_pushvalue(L(), -1);
	lua_rawget(L(), context_.crct_index_);
	if (!lua_isnil(L(), -1))
		circular_ref();
	lua_pop(L(), 1);
	lua_pushvalue(L(), -1);
	lua_pushboolean(L(), 1);
	lua_rawset(L(), context_.crct_index_);
}

void lua_parser::forget_table()
{
	lua_pushvalue(L(), -1);
	lua_pushnil(L());
	lua_rawset(L(), context_.crct_index_);
}

template<bool Validate> struct mpk_parsers {

class mpk_parser;
class mpk_array_parser;
class mpk_map_parser;
class mpk_union_parser;
class mpk_terse_record_parser;

class mpk_parser_context: noncopyable
{
	friend class mpk_parser;
public:
	typedef mpk_parser                  parser;
	typedef mpk_array_parser            array_parser;
	typedef mpk_map_parser              map_parser;
	typedef mpk_union_parser            union_parser;
	typedef mpk_map_parser              verbose_record_parser;
	typedef mpk_terse_record_parser     terse_record_parser;

	mpk_parser_context(const Bytes &b)
		: pos_(reinterpret_cast<const char *>(b.p)),
		  end_(pos_ + b.length) {}

private:
	const char *pos_;
	const char *end_;
};

class mpk_parser: noncopyable
{
public:
	typedef mpk_parser_context context_type;
	mpk_parser(mpk_parser_context &context)
		: context_(context) {}

	void    kill()
	{
	}
	void    on_before_consume()
	{
		if (Validate && pos() == end())
			parse_error();
	}
	void    consume_any()
	{
		if (mp_check(&pos(), end()) > 0)
			parse_error();
	}
	void    consume_null()
	{
		if (*pos() != 0xc0)
			type_mismatch();
		mp_decode_nil(&pos());
	}
	bool    consume_boolean()
	{
		bool val;
		switch (static_cast<uint8_t>(*pos())) {
		default:
			type_mismatch();
		case 0xc3:
			val = true; break;
		case 0xc2:
			val = false; break;
		}
		mp_decode_bool(&pos());
		return val;
	}
	int32_t consume_int() { return consume_long(); }
	int64_t consume_long()
	{
		switch (mp_typeof(*pos())) {
		default:
			type_mismatch();
		case MP_INT:
			if (Validate && mp_check_int(pos(), end()) > 0)
				parse_error();
			return mp_decode_int(&pos());
		case MP_UINT:
			if (Validate && mp_check_uint(pos(), end()) > 0)
				parse_error();
			return mp_decode_uint(&pos());
		}
	}
	float   consume_float() { return consume_double(); }
	double  consume_double()
	{
		switch (mp_typeof(*pos())) {
		default:
			type_mismatch();
		case MP_FLOAT:
			if (Validate && mp_check_float(pos(), end()) > 0)
				parse_error();
			return mp_decode_float(&pos());
		case MP_DOUBLE:
			if (Validate && mp_check_double(pos(), end()) > 0)
				parse_error();
			return mp_decode_double(&pos());
		}
	}
	Bytes   consume_bytes()
	{
		const char *b;
		uint32_t len;
		switch (mp_typeof(*pos())) {
		default:
			type_mismatch();
		case MP_BIN:
			if (Validate && mp_check_binl(pos(), end()) > 0)
				parse_error();
			b = mp_decode_bin(&pos(), &len);
			return Bytes(b, len);
		case MP_STR:
			if (Validate && mp_check_strl(pos(), end()) > 0)
				parse_error();
			b = mp_decode_str(&pos(), &len);
			return Bytes(b, len);
		}
	}
	Bytes   consume_fixed(size_t len)
	{
		Bytes bytes = consume_bytes();
		if (bytes.length != len)
			type_mismatch();
		return bytes;
	}
	Bytes   consume_string() { return consume_bytes(); }
	Bytes   consume_enum() { return consume_string(); }

	mpk_parser_context& context() { return context_; }
protected:
	const char *&pos() { return context_.pos_; }
	const char *end() { return context_.end_; }
private:
	mpk_parser_context &context_;
};

class mpk_array_parser: public mpk_parser
{
public:
	mpk_array_parser(mpk_parser_context &context)
		: mpk_parser(context)
	{
		if (Validate && pos() == end())
			parse_error();
		if (mp_typeof(*pos()) != MP_ARRAY)
			type_mismatch();
		if (Validate && mp_check_array(pos(), end()) > 0)
			parse_error();
		items_left_ = mp_decode_array(&pos());
	}
	void kill()
	{
		while (items_left_ != 0) {
			// XXX: I want mp_check() accepting the number
			// of items to skip
			if (mp_check(&pos(), end()) > 0)
				parse_error();
			items_left_ --;
		}
		mpk_parser::kill();
	}
	bool next()
	{
		if (items_left_ == 0)
			return false;
		items_left_ --;
		return true;
	}
	using mpk_parser::pos;
	using mpk_parser::end;
private:
	uint32_t items_left_;
};

class mpk_map_parser: public mpk_parser
{
public:
	mpk_map_parser(mpk_parser_context &context)
		: mpk_parser(context)
	{
		if (Validate && pos() == end())
			parse_error();
		if (mp_typeof(*pos()) != MP_MAP)
			type_mismatch();
		if (Validate && mp_check_map(pos(), end()) > 0)
			parse_error();
		items_left_ = mp_decode_map(&pos());
	}
	void kill()
	{
		assert(items_left_ == 0);
		mpk_parser::kill();
	}
	bool next()
	{
		if (items_left_ == 0)
			return false;
		items_left_ --;
		on_before_consume();
		key_ = consume_string();
		return true;
	}
	Bytes key() { return key_; }
	using mpk_parser::pos;
	using mpk_parser::end;
	using mpk_parser::on_before_consume;
	using mpk_parser::consume_string;
private:
	uint32_t items_left_;
	Bytes    key_;
};

class mpk_union_parser: public mpk_array_parser
{
public:
	mpk_union_parser(mpk_parser_context &context)
		: mpk_array_parser(context)
	{
		if (!next())
			type_mismatch();
		on_before_consume();
		tag_ = consume_string();
		if (!next())
			type_mismatch();
	}
	void kill()
	{
		if (next())
			type_mismatch();
		mpk_array_parser::kill();
	}
	Bytes tag() { return tag_; }
	using mpk_parser::pos;
	using mpk_parser::end;
	using mpk_parser::on_before_consume;
	using mpk_parser::consume_string;
	using mpk_array_parser::next;
private:
	Bytes tag_;
};

class mpk_terse_record_parser: public mpk_array_parser
{
public:
	mpk_terse_record_parser(mpk_parser_context &context)
		: mpk_array_parser(context)
	{
	}
	void next()
	{
		if (!mpk_array_parser::next())
			type_mismatch();
	}
};

}; // mpk_parsers

typedef mpk_parsers<true>  mpk_safe_parsers;
typedef mpk_parsers<false> mpk_fast_parsers;

mpk_safe_parsers::mpk_parser &erase_type(mpk_safe_parsers::mpk_parser &parser)
{
	return parser;
}

mpk_fast_parsers::mpk_parser &erase_type(mpk_fast_parsers::mpk_parser &parser)
{
	return parser;
}

//
// IR builder
//
template <int Flags>
class ir_builder
{
public:
	ir_builder()
		: use_terse_records_(false), collapse_nested_(false)
	{}
	void set_use_terse_records(bool yes_no) { use_terse_records_ = yes_no; }
	void set_collapse_nested(bool yes_no) { collapse_nested_ = yes_no; }

	template <typename Parser>
	void build_value(Parser &p, avro_value_t *v)
	{
		build_value(p, v, avro_value_get_type(v));
	}

	template <typename Parser>
	void build_value(Parser &, avro_value_t *, avro_type_t);

	template <typename Parser>
	void build_array_value(Parser &, avro_value_t *);

	template <typename Parser>
	void build_map_value(Parser &, avro_value_t *);

	template <typename Parser>
	void build_verbose_record_value(Parser &, avro_value_t *);

	template <typename Parser>
	void build_terse_record_value(Parser &, avro_value_t *);

	template <typename Parser>
	void build_union_value(Parser &, avro_value_t *);

	template <typename Parser>
	void skip_value(Parser &, const avro_schema_t);

	bool use_terse_records()
	{
		if (Flags & PARSER_ENABLE_TERSE_RECORDS) {
			if (Flags & PARSER_ENABLE_VERBOSE_RECORDS)
				return use_terse_records_;
			else
				return true;
		} else {
			return false;
		}
	}

	bool collapse_nested()
	{
		if (Flags & PARSER_ENABLE_COLLAPSE_NESTED) {
			if (Flags & PARSER_TERSE_RECORDS_IMPLY_COLLAPSE_NESTED)
				return use_terse_records();
			else
				return use_terse_records() && collapse_nested_;
		} else {
			return false;
		}
	}

private:
	bool use_terse_records_;
	bool collapse_nested_;
};

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::build_value(
	Parser &parser, avro_value_t *dest, avro_type_t type)
{
	parser.on_before_consume();

	switch (type) {
	case AVRO_BOOLEAN:
		if (avro_value_set_boolean(dest, parser.consume_boolean()) != 0)
			internal_error();
		return;
	case AVRO_BYTES:
		{
			Bytes b = parser.consume_bytes();
			avro_wrapped_buffer_t buf = bytes_to_avro_buffer(b);
			if (avro_value_give_bytes(dest, &buf) != 0)
				internal_error();
		}
		return;
	case AVRO_DOUBLE:
		if (avro_value_set_double(dest, parser.consume_double()) != 0)
			internal_error();
		return;
	case AVRO_FLOAT:
		if (avro_value_set_float(dest, parser.consume_float() != 0))
			internal_error();
		return;
	case AVRO_INT32:
		if (avro_value_set_int(dest, parser.consume_int()) != 0)
			internal_error();
		return;
	case AVRO_INT64:
		if (avro_value_set_long(dest, parser.consume_long()) != 0)
			internal_error();
		return;
	case AVRO_NULL:
		parser.consume_null();
		if (avro_value_set_null(dest) != 0)
			internal_error();
		return;
	case AVRO_STRING:
		{
			Bytes s = parser.consume_string();
			check_utf8_string(Flags, s);
			avro_wrapped_buffer_t buf = bytes_to_avro_buffer(s);
			if (avro_value_give_string_len(dest, &buf) != 0)
				internal_error();
		}
		return;
	case AVRO_ARRAY:
		{
			 typename Parser::context_type::array_parser ap(
				 parser.context());
			 build_array_value(ap, dest);
		}
		return;
	case AVRO_ENUM:
		{
			int val = resolve_enum_value(
				Flags,
				pure_value_get_schema(dest),
				parser.consume_enum());
			if (avro_value_set_enum(dest, val) != 0)
				enum_value_error(dest, val);
		}
		return;
	case AVRO_FIXED:
		{
			avro_schema_t schema = avro_value_get_schema(dest);
			Bytes f = parser.consume_fixed(
				avro_schema_fixed_size(schema));
			avro_wrapped_buffer_t buf = bytes_to_avro_buffer(f);
			if (avro_value_give_fixed(dest, &buf) != 0)
				internal_error();
		}
		return;
	case AVRO_MAP:
		{
			typename Parser::context_type::map_parser mp(
				parser.context());
			build_map_value(mp, dest);
			mp.kill();
		}
		return;
	case AVRO_RECORD:
		if (use_terse_records()) {
			typename Parser::context_type::terse_record_parser rp(
				parser.context());
			build_terse_record_value(rp, dest);
			rp.kill();
		} else {
			typename Parser::context_type::verbose_record_parser rp(
				parser.context());
			build_verbose_record_value(rp, dest);
			rp.kill();
		}
		return;
	case AVRO_UNION:
		{
			typename Parser::context_type::union_parser up(
				parser.context());
			build_union_value(up, dest);
			up.kill();
		}
		return;
	default:
		internal_error();
	}
}

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::build_array_value(Parser &parser, avro_value_t *dest)
{
	while (parser.next()) {
		avro_value_t child;
		if (avro_value_append(dest, &child, NULL) != 0)
			internal_error();
		build_value(erase_type(parser), &child);
	}
}

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::build_map_value(Parser &parser, avro_value_t *dest)
{
	while (parser.next()) {
		int rc;
		avro_value_t child;
		check_utf8_string(Flags, parser.key());
		if (Flags & PARSER_ASSUME_NUL_TERM_STRINGS) {
			rc = avro_value_add(
				dest,
				reinterpret_cast<const char*>(parser.key().p),
				&child,
				NULL,
				NULL);
		} else {
			rc = avro_value_add(
				dest,
				bytes_to_cstring(parser.key()),
				&child,
				NULL,
				NULL);
		}
		// XXX dup keys
		if (rc != 0)
			internal_error();
		build_value(erase_type(parser), &child);
	}
}

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::build_verbose_record_value(Parser &parser, avro_value_t *dest)
{
	avro_schema_t schema = avro_value_get_schema(dest);
	while (parser.next()) {
		avro_value_t  field;
		int index = resolve_record_field(Flags, schema, parser.key());
		// XXX check no dup fields, all fields were set
		// XXX maintain a bitmask and use it later to create
		//     UPDATE statement
		if (avro_value_get_by_index(dest, index, &field, NULL) != 0)
			record_index_error(schema, index);

		if (field.iface) {
			build_value(erase_type(parser), &field);
		} else {
			// on the fly schema conversion;
			// excluded from target IR
			skip_value(
				erase_type(parser),
				pure_schema_record_field_get_by_index(schema, index));
		}
	}
}

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::build_terse_record_value(Parser &parser, avro_value_t *dest)
{
	const bool collapse = collapse_nested();
	avro_schema_t schema = avro_value_get_schema(dest);
	size_t i, field_count = avro_schema_record_size(schema);
	for (i = 0; i < field_count; i++) {
		avro_value_t  field;
		avro_type_t   type;
		if (avro_value_get_by_index(dest, i, &field, NULL) != 0)
			internal_error();
		if (field.iface != NULL) {
			type = avro_value_get_type(&field);
			if (collapse && type == AVRO_RECORD) {
				build_terse_record_value(parser, &field);
			} else {
				parser.next();
				build_value(erase_type(parser), &field, type);
			}
		} else {
			// on the fly schema conversion;
			// excluded from target IR
			parser.next();
			skip_value(
				erase_type(parser),
				pure_schema_record_field_get_by_index(schema, i));
		}
	}
}

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::build_union_value(Parser &parser, avro_value_t *dest)
{
	avro_schema_t schema = avro_value_get_schema(dest);
	int           tag    = resolve_union_tag(Flags, schema, parser.tag());
	avro_value_t  branch;
	if (avro_value_set_branch(dest, tag, &branch) != 0)
		union_tag_error(schema, tag);
	build_value(erase_type(parser), &branch);
}

template <int Flags>
template <typename Parser>
void
ir_builder<Flags>::skip_value(Parser &parser, const avro_schema_t schema)
{
	if (Flags & PARSER_ENABLE_FAST_SKIP) {
		parser.on_before_consume();
		parser.consume_any();
	} else {
		// XXX
		(void)schema;
		internal_error();
	}
}

void record_index_error(const avro_schema_t schema, int index)
{
	// we postpone index validation until after the error
	if (index < 0 || (size_t)index >= avro_schema_record_size(schema))
		name_unknown();
	internal_error();
}

void union_tag_error(const avro_schema_t schema, int tag)
{
	// we postpone index validation until after the error
	if (tag < 0 || (size_t)tag >= avro_schema_union_size(schema))
		name_unknown();
	internal_error();
}

void enum_value_error(avro_value_t *e, int v)
{
	(void)e;
	(void)v;
	internal_error(); // XXX
}
