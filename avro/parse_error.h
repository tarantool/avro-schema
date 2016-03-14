#define PARSER_ABORT_FUNC_ATTRIBUTES \
	__attribute__((__noreturn__, __cold__, __noinline__))

void internal_error() PARSER_ABORT_FUNC_ATTRIBUTES;
void parse_error()    PARSER_ABORT_FUNC_ATTRIBUTES;
void type_mismatch()  PARSER_ABORT_FUNC_ATTRIBUTES;
void name_unknown()   PARSER_ABORT_FUNC_ATTRIBUTES;
void circular_ref()   PARSER_ABORT_FUNC_ATTRIBUTES;
void stack_overflow() PARSER_ABORT_FUNC_ATTRIBUTES;

void record_index_error(const avro_schema_t, int)
		      PARSER_ABORT_FUNC_ATTRIBUTES;
void union_tag_error(const avro_schema_t, int)
		      PARSER_ABORT_FUNC_ATTRIBUTES;
void enum_value_error(avro_value_t *, int)
		      PARSER_ABORT_FUNC_ATTRIBUTES;


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
	if (v < 0 || v >= avro_schema_enum_size(avro_value_get_schema(e)))
		name_unknown();
	internal_error();
}
