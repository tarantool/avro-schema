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
