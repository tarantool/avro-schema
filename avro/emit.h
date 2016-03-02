enum {
	EMITTER_ENABLE_VERBOSE_RECORDS = 32,
	EMITTER_ENABLE_TERSE_RECORDS   = 64,
};

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
		: lua_emitter(context)
	{
		lua_createtable(L(), length, 0);
	}
	void kill() {}
	void begin_item(int) {}
	void end_item(int i)
	{
		lua_rawseti(L(), -2, i+1);
	}
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

const char *pure_union_branch_name(avro_value_t *val, int tag)
	__attribute__((__pure__));

const char *pure_union_branch_name(avro_value_t *val)
{
	avro_schema_t schema = avro_value_get_schema(val);
	return avro_schema_type_name(schema);
}

const char *pure_enum_value_name(avro_value_t *val, int v)
	__attribute__((__pure__));

const char *pure_enum_value_name(avro_value_t *val, int v)
{
	return avro_schema_enum_get(avro_value_get_schema(val), v);
}

template <int Flags>
class ir_visitor
{
public:
	ir_visitor(): use_terse_records_(false) {}
	void set_use_terse_records(bool yes_no) { use_terse_records_ = yes_no; }

	bool use_terse_records()
	{
		if (Flags & EMITTER_ENABLE_TERSE_RECORDS) {
			if (Flags & EMITTER_ENABLE_VERBOSE_RECORDS)
				return use_terse_records_;
			else
				return true;
		} else {
			return false;
		}
	}

	template <typename Emitter>
	void visit_value(Emitter &, avro_value_t *);

	template <typename Emitter>
	void visit_array_value(Emitter &, avro_value_t *, size_t);

	template <typename Emitter>
	void visit_map_value(Emitter &, avro_value_t *, size_t);

private:
	bool use_terse_records_;
};

template <int Flags>
template <typename Emitter>
void
ir_visitor<Flags>::visit_value(Emitter &emitter, avro_value_t *val)
{
	switch (avro_value_get_type(val)) {
	case AVRO_BOOLEAN:
		{
			int v;
			if (avro_value_get_boolean(val, &v) != 0)
				internal_error();
			emitter.emit_boolean(v);
		}
		return;
	case AVRO_BYTES:
		{
			const void *buf;
			size_t len;
			if (avro_value_get_bytes(val, &buf, &len) != 0)
				internal_error();
			emitter.emit_bytes(Bytes(buf, len));
		}
		return;
	case AVRO_DOUBLE:
		{
			double v;
			if (avro_value_get_double(val, &v) != 0)
				internal_error();
			emitter.emit_double(v);
		}
		return;
	case AVRO_FLOAT:
		{
			float v;
			if (avro_value_get_float(val, &v) != 0)
				internal_error();
			emitter.emit_float(v);
		}
		return;
	case AVRO_INT32:
		{
			int32_t v;
			if (avro_value_get_int(val, &v) != 0)
				internal_error();
			emitter.emit_int(v);
		}
		return;
	case AVRO_INT64:
		{
			int64_t v;
			if (avro_value_get_long(val, &v) != 0)
				internal_error();
			emitter.emit_long(v);
		}
		return;
	case AVRO_NULL:
		{
			if (avro_value_get_null(val) != 0)
				internal_error();
			emitter.emit_null();
		}
		return;
	case AVRO_STRING:
		{
			const char *str;
			size_t len;
			if (avro_value_get_string(val, &str, &len) != 0)
				internal_error();
			emitter.emit_string(Bytes(str, len));
		}
		return;
	case AVRO_ARRAY:
		{
			size_t count;
			if (avro_value_get_size(val, &count) != 0)
				internal_error();
			typename Emitter::context_type::array_emitter ae(
				emitter.context(),
				static_cast<int>(count));
			visit_array_value(ae, val, count);
			ae.kill();
		}
		return;
	case AVRO_ENUM:
		{
			int v;
			if (avro_value_get_enum(val, &v) != 0)
				internal_error();
			emitter.emit_enum(v, pure_enum_value_name(val, v));
		}
		return;
	case AVRO_FIXED:
		{
			const void *buf;
			size_t len;
			if (avro_value_get_fixed(val, &buf, &len) != 0)
				internal_error();
			emitter.emit_fixed(Bytes(buf, len));
		}
		return;
	case AVRO_MAP:
		{
			size_t count;
			if (avro_value_get_size(val, &count) != 0)
				internal_error();
			typename Emitter::context_type::map_emitter me(
				emitter.context(),
				static_cast<int>(count));
			visit_map_value(me, val, count);
			me.kill();
		}
		return;
	case AVRO_RECORD:
		{
			size_t count;
			if (avro_value_get_size(val, &count) != 0)
				internal_error();
			if (use_terse_records()) {
				typename Emitter::context_type::terse_record_emitter re(
					emitter.context(),
					static_cast<int>(count));
				visit_array_value(re, val, count);
				re.kill();
			} else {
				typename Emitter::context_type::verbose_record_emitter re(
					emitter.context(),
					static_cast<int>(count));
				visit_map_value(re, val, count);
				re.kill();
			}
		}
		return;
	case AVRO_UNION:
		{
			int tag;
			avro_value_t branch;
			if (avro_value_get_discriminant(val, &tag) != 0)
				internal_error();
			if (avro_value_get_current_branch(val, &branch) != 0)
				internal_error();
			typename Emitter::context_type::union_emitter ue(
				emitter.context(),
				tag,
				pure_union_branch_name(&branch));
			visit_value(erase_type(ue), &branch);
			ue.kill();
		}
		return;
	default:
		internal_error();
	}
}

template <int Flags>
template <typename Emitter>
void
ir_visitor<Flags>::visit_array_value(
	Emitter &emitter, avro_value_t *val, size_t count)
{
	size_t i;
	for (i = 0; i < count; i++) {
		avro_value_t item;
		if (avro_value_get_by_index(val, i, &item, NULL) != 0)
			internal_error();
		emitter.begin_item(static_cast<int>(i));
		visit_value(erase_type(emitter), &item);
		emitter.end_item(static_cast<int>(i));
	}
}

template <int Flags>
template <typename Emitter>
void
ir_visitor<Flags>::visit_map_value(
	Emitter &emitter, avro_value_t *val, size_t count)
{
	size_t i;
	for (i = 0; i < count; i++) {
		avro_value_t item;
		const char *key;
		if (avro_value_get_by_index(val, i, &item, &key) != 0)
			internal_error();
		emitter.begin_item(static_cast<int>(i), key);
		visit_value(erase_type(emitter), &item);
		emitter.end_item(static_cast<int>(i), key);
	}
}
