enum {
	EMITTER_ENABLE_VERBOSE_RECORDS = 32
};

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

template <int Flags, typename Options = processing_options>
class ir_visitor
{
public:
	ir_visitor(const Options &options): options_(options) {}

	template <typename Emitter>
	void visit_value(Emitter &e, avro_value_t *v)
	{
		visit_value(e, v, avro_value_get_type(v));
	}

	template <typename Emitter>
	void visit_value(Emitter &, avro_value_t *, avro_type_t);

	template <typename Emitter>
	void visit_array_value(Emitter &, avro_value_t *, size_t);

	template <typename Emitter>
	void visit_map_value(Emitter &, avro_value_t *, size_t);

	template <typename Emitter>
	void visit_terse_record_value(Emitter &, avro_value_t *, size_t);

	template <typename Emitter>
	void visit_alt_union_value(Emitter &, avro_value_t *);

	template <typename Emitter>
	void visit_value_for_update(
		Emitter &, avro_value_t *, const uint64_t *);

	template <typename Emitter>
	void visit_x_verbose_record_value(
		Emitter &, avro_value_t *, const uint64_t *,
		size_t base_index,
		size_t bit_offset);

private:
	const Options options_;
};

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_value(Emitter &emitter, avro_value_t *val, avro_type_t type)
{
	switch (type) {
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

			if ((Flags & ASSUME_STRING_ENUM_CODING) &&
				options_.use_integer_enum_coding()) {

				emitter.emit_int(v);
			} else {
				emitter.emit_enum(v, pure_enum_value_name(val, v));
			}
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
			typedef typename Emitter::context_type::terse_record_emitter
				terse_record_emitter;
			size_t count;
			if (avro_value_get_size(val, &count) != 0)
				internal_error();
			if (!(Flags & EMITTER_ENABLE_VERBOSE_RECORDS) ||
				options_.use_terse_records()) {
				// XXX count is incorrect if collapse_nested is in
				// effect
				terse_record_emitter re(
					emitter.context(),
					static_cast<int>(count));
				visit_terse_record_value(re, val, count);
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
		if ((Flags & ASSUME_STRING_UNION_TAG_CODING) &&
			options_.use_integer_union_tag_coding()) {

			typename Emitter::context_type::terse_record_emitter ue(
				emitter.context(), 2);
			visit_alt_union_value(ue, val);
			ue.kill();
		} else {
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

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_array_value(
	Emitter &emitter, avro_value_t *val, size_t count)
{
	size_t i;
	for (i = 0; i < count; i++) {
		avro_value_t item;
		if (avro_value_get_by_index(val, i, &item, NULL) != 0)
			internal_error();
		emitter.begin_item();
		visit_value(erase_type(emitter), &item);
		emitter.end_item();
	}
}

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_map_value(
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

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_terse_record_value(
	Emitter &emitter, avro_value_t *val, size_t count)
{
	const bool collapse = options_.collapse_nested();
	size_t i;
	for (i = 0; i < count; i++) {
		avro_value_t item;
		avro_type_t  type;
		if (avro_value_get_by_index(val, i, &item, NULL) != 0)
			internal_error();
		type = avro_value_get_type(&item);
		if (collapse && type == AVRO_RECORD) {
			size_t count;
			if (avro_value_get_size(&item, &count) != 0)
				internal_error();
			visit_terse_record_value(emitter, &item, count);
		} else if (collapse && type == AVRO_UNION) {
			visit_alt_union_value(emitter, &item);
		} else {
			emitter.begin_item();
			visit_value(erase_type(emitter), &item, type);
			emitter.end_item();
		}
	}
}

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_alt_union_value(
	Emitter &emitter, avro_value_t *val)
{
	int tag;
	avro_value_t branch;
	if (avro_value_get_discriminant(val, &tag) != 0)
		internal_error();
	if (avro_value_get_current_branch(val, &branch) != 0)
		internal_error();
	emitter.begin_item();
	emitter.emit_int(tag);
	emitter.end_item();
	emitter.begin_item();
	visit_value(erase_type(emitter), &branch);
	emitter.end_item();
}

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_value_for_update(
	Emitter &emitter, avro_value_t *val, const uint64_t *bitmap)
{
	typedef typename Emitter::context_type::array_emitter array_emitter;

	if (avro_value_get_type(val) != AVRO_RECORD)
		internal_error();

	array_emitter ae(emitter.context(), 0 /* XXX */);

	visit_x_verbose_record_value(ae, val, bitmap, 1, 0);

	ae.kill();
}

template <int Flags, typename Options>
template <typename Emitter>
void
ir_visitor<Flags, Options>::visit_x_verbose_record_value(
	Emitter &emitter, avro_value_t *val,
	const uint64_t *bitmap,
	size_t base_index,
	size_t bit_offset)
{
	typedef typename Emitter::context_type::array_emitter array_emitter;

	avro_schema_t schema = avro_value_get_schema(val);

	size_t count = avro_schema_record_size(schema);

	annotation *a = static_cast<annotation *>(
		avro_schema_record_annotation(schema));

	size_t *iof =
		get_flattened_item_offsets(a, count);

	size_t *bof =
		get_nested_bitmap_offsets(a, count);

	const uint64_t *p = bitmap + bit_offset / 64;
	size_t index_adjust = -(bit_offset & 63);
	uint64_t v = *p & (UINT64_C(-1) << (bit_offset & 63));

	// for each field if the corresponding bit is set
repeat:
	while (v) {
		int pos = __builtin_ctzll(v);
		size_t i = index_adjust + pos;
		if (i >= count)
			return;
		v ^= UINT64_C(1) << pos;

		avro_value_t item;
		if (avro_value_get_by_index(val, i, &item, NULL) != 0)
			internal_error();

		avro_type_t type = avro_value_get_type(&item);
		size_t update_item_index = base_index + iof[i];

		switch (type) {
		case AVRO_RECORD:
			visit_x_verbose_record_value(
				emitter, &item, bitmap,
				update_item_index,
				bit_offset + bof[i]);
			break;
		case AVRO_UNION:
			{
				emitter.begin_item();

				array_emitter ae(emitter.context(), 3);
				ae.begin_item();
				ae.emit_string(Bytes("=", 1));
				ae.end_item();
				ae.begin_item();
				ae.emit_int(update_item_index);
				ae.end_item();
				ae.begin_item();

				int tag;
				if (avro_value_get_discriminant(
						val, &tag) != 0)
					internal_error();

				ae.emit_int(tag);
				ae.end_item();
				ae.kill();

				emitter.end_item();

				avro_value_t copy = item;
				if (avro_value_get_current_branch(
						&copy, &item) != 0)
					internal_error();

				update_item_index += 1;
				type = avro_value_get_type(val);
			}
			/* fallthrough */
		default:
			{
				emitter.begin_item();

				array_emitter ae(emitter.context(), 3);
				ae.begin_item();
				ae.emit_string(Bytes("=", 1));
				ae.end_item();
				ae.begin_item();
				ae.emit_int(update_item_index);
				ae.end_item();
				ae.begin_item();
				visit_value(emitter, &item, type);
				ae.end_item();
				ae.kill();

				emitter.end_item();
			}
			break;
		}
	}
	index_adjust += 64;
	if (index_adjust >= count)
		return;
	v = *++p;
	goto repeat;
}
