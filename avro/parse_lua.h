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
