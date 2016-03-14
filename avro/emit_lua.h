class lua_emitter;
class lua_array_emitter;
class lua_map_emitter;
class lua_union_emitter;

class lua_emitter_context: private noncopyable
{
public:
	friend class lua_emitter;
	typedef lua_emitter       emitter;
	typedef lua_array_emitter array_emitter;
	typedef lua_map_emitter   map_emitter;
	typedef lua_union_emitter union_emitter;
	typedef lua_map_emitter   verbose_record_emitter;
	typedef lua_array_emitter terse_record_emitter;
	lua_emitter_context(struct lua_State *L): L_(L)
	{
	}
private:
	struct lua_State *L_;
};

class lua_emitter: private noncopyable
{
public:
	typedef lua_emitter_context context_type;

	lua_emitter(lua_emitter_context &context)
		: context_(context), L_(context.L_)
	{
		if (!lua_checkstack(L(), 4))
			stack_overflow();
	}
	void emit_null()
	{
		// XXX
		internal_error();
	}
	void emit_boolean(bool v)
	{
		lua_pushboolean(L(), v);
	}
	void emit_int(int32_t v) { emit_long(v); }
	void emit_long(int64_t v)
	{
		lua_pushinteger(L(), v);
	}
	void emit_float(float v) { emit_double(v); }
	void emit_double(double v)
	{
		lua_pushnumber(L(), v);
	}
	void emit_bytes(const Bytes &b)
	{
		lua_pushlstring(
			L(),
			reinterpret_cast<const char *>(b.p),
			b.length);
	}
	void emit_fixed(const Bytes &b) { emit_bytes(b); }
	void emit_string(const Bytes &b) { emit_bytes(b); }
	void emit_enum(int iv, const char *v)
	{
		(void)iv;
		lua_pushstring(L(), v);
	}

	lua_emitter_context &context() { return context_; }
protected:
	struct lua_State *L() { return L_; }
private:
	lua_emitter_context &context_;
	struct lua_State    *L_;

};

class lua_array_emitter: public lua_emitter
{
public:
	lua_array_emitter(lua_emitter_context &context, int length)
		: lua_emitter(context), i(1)
	{
		lua_createtable(L(), length, 0);
	}
	void kill() {}
	void begin_item(int) {}
	void end_item(int)
	{
		lua_rawseti(L(), -2, i++);
	}
private:
	lua_Integer i;
};

class lua_map_emitter: public lua_emitter
{
public:
	lua_map_emitter(lua_emitter_context &context, int length)
		: lua_emitter(context)
	{
		lua_createtable(L(), 0, length);
	}
	void kill() {}
	void begin_item(int index, const char *name)
	{
		(void)index;
		lua_pushstring(L(), name);
	}
	void end_item(int index, const char *name)
	{
		(void)index;
		(void)name;
		lua_rawset(L(), -3);
	}
};

class lua_union_emitter: public lua_emitter
{
public:
	lua_union_emitter(lua_emitter_context &context, int itag, const char *tag)
		: lua_emitter(context)
	{
		(void)itag;
		lua_createtable(L(), 2, 0);
		lua_pushstring(L(), tag);
		lua_rawseti(L(), -2, 1);
	}
	void kill()
	{
		lua_rawseti(L(), -2, 2);
	}
};

lua_emitter &erase_type(lua_emitter &e) { return e; }
