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


enum processing_mode
{
	CONSUME_REGULAR = 1,
	CONSUME_FLATTENED = 2,
	EMIT_REGULAR = 1,
	EMIT_FLATTENED = 2
};

class processing_options
{
public:
	processing_options(processing_mode mode): mode_(mode) {}
	bool use_terse_records() const       { return enable_flattening(); }
	bool collapse_nested() const         { return enable_flattening(); }
	bool use_integer_enum_coding() const { return enable_flattening(); }
	bool use_integer_union_tag_coding() const { return enable_flattening(); }
private:
	bool enable_flattening() const
	{
		return mode_ == CONSUME_FLATTENED || mode_ == EMIT_FLATTENED;
	}
	processing_mode mode_;
};


enum {
	ASSUME_NUL_TERM_STRINGS         = 1,
	ASSUME_NON_NUL_CHARS            = 2,
	ASSUME_UTF8_STRINGS             = 4,
	ASSUME_NO_DUP_MAP_KEYS          = 8,
	ENABLE_FAST_SKIP                = 16,
	ENABLE_VERBOSE_RECORDS          = 32,
	ASSUME_STRING_ENUM_CODING       = 64,
	ASSUME_STRING_UNION_TAG_CODING  = 128
};
