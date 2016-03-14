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
