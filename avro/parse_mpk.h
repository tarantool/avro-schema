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
