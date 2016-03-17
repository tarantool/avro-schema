#define __STDC_LIMIT_MACROS
#define __STDC_CONSTANT_MACROS
#define MP_SOURCE

#include <tarantool/module.h>
extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
// missing parts of tarantool pub api
struct tuple *lua_istuple(struct lua_State *, int)
	__attribute__((__visibility__("default")));
}

#include <avro/errors.h>
#include <avro/value.h>
#include <avro/schema.h>
#include <avro/resolver.h>
#include <avro/generic.h>

#include <msgpuck.h>

#include <cassert>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "util.h"
#include "annotate.h"
#include "parse_error.h"
#include "parse.h"
#include "parse_lua.h"
#include "parse_mpk.h"
#include "emit.h"
#include "emit_lua.h"

enum cache_mode {
	DO_CACHE,
	DONT_CACHE
};

struct schema_plus
{
	avro_schema_t        schema;
	avro_value_iface_t  *gclass;
};

// data transformation context - we use it to attach a finalizer to our
// state because transformation may abort prematurely due to a Lua error
struct xform_ctx
{
	avro_value_t         src;
	avro_value_t         dest;
	std::vector<uint64_t>bitmap;
};

static void *resolver_cache_registry_key;
static const char schema_typename[]    = "avro.schema"; // struct schema_plus
static const char resolver_typename[]  = "avro.resolver"; // avro_value_iface_t*
static const char xform_ctx_typename[] = "avro.xform-ctx"; // struct xfrom_ctx

static int
schema_gc(struct lua_State *L)
{
	struct schema_plus *schema;
	schema = (struct schema_plus *)
		luaL_checkudata(L, 1, schema_typename);
	if (schema->gclass) {
		avro_value_iface_decref(schema->gclass);
	}
	avro_schema_decref(schema->schema);
	return 0;
}

static int
schema_to_string(struct lua_State *L)
{
	struct schema_plus *schema;
	const char *ns;
	const char *name;
	schema = (struct schema_plus *)
		luaL_checkudata(L, 1, schema_typename);
	ns = avro_schema_namespace(schema->schema);
	name = avro_schema_type_name(schema->schema);
	if (ns != NULL) {
		lua_pushfstring(L, "Avro schema (%s.%s)", ns, name);
	} else {
		lua_pushfstring(L, "Avro schema (%s)", name);
	}
	return 1;
}

static int
resolver_gc(struct lua_State *L)
{
	avro_value_iface_t **wrapper;
	wrapper = (avro_value_iface_t **)
		luaL_checkudata(L, 1, resolver_typename);
	if (*wrapper != NULL) {
		avro_value_iface_decref(*wrapper);
	}
	return 0;
}

static int
resolver_to_string(struct lua_State *L)
{
	avro_value_iface_t **wrapper;
	wrapper = (avro_value_iface_t **)
		luaL_checkudata(L, 1, resolver_typename);
	(void)wrapper;
	lua_pushliteral(L, "Avro schema resolver");
	return 1;
}

static struct xform_ctx *
create_xform_ctx(struct lua_State *L)
{
	struct xform_ctx *xform_ctx;
	xform_ctx = (struct xform_ctx *)
		lua_newuserdata(L, sizeof(*xform_ctx));
	xform_ctx->src.iface = NULL;
	xform_ctx->src.self = NULL;
	xform_ctx->dest.iface = NULL;
	xform_ctx->dest.self = NULL;
	new (&xform_ctx->bitmap) std::vector<uint64_t>;
	luaL_getmetatable(L, xform_ctx_typename);
	lua_setmetatable(L, -2);
	return xform_ctx;
}

static void
finalize_xform_ctx(struct xform_ctx *xform_ctx)
{
	// can be called multiple times with the same object
	if (xform_ctx->src.iface != NULL) {
		avro_value_decref(&xform_ctx->src);
	}
	if (xform_ctx->dest.iface != NULL) {
		avro_value_decref(&xform_ctx->dest);
	}
	std::vector<uint64_t> empty;
	xform_ctx->bitmap.swap(empty);
}

static int
xform_ctx_gc(struct lua_State *L)
{
	struct xform_ctx *xform_ctx;
	xform_ctx = (struct xform_ctx *)
		luaL_checkudata(L, 1, xform_ctx_typename);
	finalize_xform_ctx(xform_ctx);
	return 0;
}

static int
xform_ctx_to_string(struct lua_State *L)
{
	struct xform_ctx *xform_ctx;
	xform_ctx = (struct xform_ctx *)
		luaL_checkudata(L, 1, xform_ctx_typename);
	(void)xform_ctx;
	lua_pushliteral(L, "Avro xform ctx");
	return 1;
}

const char get_json_encode[] =
	"local json = require(\"json\")\n"
	"return json.encode";

static int
create_schema(struct lua_State *L)
{
	const char *str;
	size_t len;
	struct schema_plus *schema;

	if (lua_type(L, 1) != LUA_TSTRING) {
		// use json.encode (retrieved once and cached in upvalue)
		lua_pushvalue(L, lua_upvalueindex(1));
		if (lua_isnil(L, -1)) {
			lua_pop(L, 1);
			luaL_loadstring(L, get_json_encode);
			lua_call(L, 0, 1);
			lua_pushvalue(L, -1);
			lua_replace(L, lua_upvalueindex(1));
		}
		lua_pushvalue(L, 1);
		lua_call(L, 1, 1);
		lua_replace(L, 1);
	}
	luaL_argcheck(L, lua_isstring(L, 1), 1, "`string' expected");
	str = lua_tolstring(L, 1, &len);
	schema = (struct schema_plus *)lua_newuserdata(L, sizeof(*schema));
	if (avro_schema_from_json_length(str, len, &schema->schema) != 0) {
		lua_pop(L, 1);
		lua_pushboolean(L, 0);
		lua_pushstring(L, avro_strerror());
		return 2;
	}
	try
	{
		annotate(schema->schema);
	}
	catch (std::exception &e)
	{
		avro_schema_decref(schema->schema);
		lua_pop(L, 1);
		lua_pushboolean(L, 0);
		lua_pushstring(L, e.what());
		return 2;
	}
	schema->gclass = NULL;
	luaL_getmetatable(L, schema_typename);
	lua_setmetatable(L, -2);
	lua_pushboolean(L, 1);
	lua_insert(L, -2); /* swap 2 top elements */
	return 2;
}

static int
get_resolver_cache(struct lua_State *L)
{
	// using a global table __mode = 'k', mapping <src> -> { ... }
	// nested table is __mode = 'k', maps <dest> -> resolver
	// resolver wrapped in schema_plus
	lua_pushlightuserdata(L, &resolver_cache_registry_key);
	lua_gettable(L, LUA_REGISTRYINDEX);
	if (lua_istable(L, -1)) {
		return 1;
	}
	lua_pop(L, 1);
	lua_newtable(L);
	lua_pushlightuserdata(L, &resolver_cache_registry_key);
	lua_pushvalue(L, -2);
	lua_settable(L, LUA_REGISTRYINDEX);
	lua_newtable(L);
	lua_pushliteral(L, "k");
	lua_setfield(L, -2, "__mode");
	lua_setmetatable(L, -2);
	return 1;
}

static avro_value_iface_t *
create_resolver(struct lua_State *L,
		int src_index, int dest_index, enum cache_mode cache_mode)
{
	avro_value_iface_t **wrapper;
	avro_value_iface_t *resolver;

	// lookup existing resolver
	get_resolver_cache(L);
	lua_pushvalue(L, src_index);
	lua_gettable(L, -2);
	if (!lua_istable(L, -1)) {
		lua_pop(L, 1); // keep toplevel table on stack
	} else {
		lua_pushvalue(L, dest_index);
		lua_gettable(L, -2);
		// lua stack: toplevel/nested/w
		if (lua_isuserdata(L, -1)) {
			wrapper = (avro_value_iface_t **)
				luaL_checkudata(L, -1, resolver_typename);
			lua_pop(L, 3);
			resolver = *wrapper;
			avro_value_iface_incref(resolver);
			return resolver;
		}
		lua_pop(L, 2); // keep toplevel table on stack
	}

	if (cache_mode == DO_CACHE) {
		wrapper = (avro_value_iface_t **)
			lua_newuserdata(L, sizeof(*wrapper));
		*wrapper = NULL;
		luaL_getmetatable(L, resolver_typename);
		lua_setmetatable(L, -2);
		// lua stack: toplevel/w
	}

	resolver = avro_resolved_writer_new(
		((struct schema_plus *)lua_touserdata(L, src_index))->schema,
		((struct schema_plus *)lua_touserdata(L, dest_index))->schema);

	if (resolver == NULL) {
		if (cache_mode == DO_CACHE) {
			lua_pop(L, 2);
		}
		return NULL;
	}

	if (cache_mode == DO_CACHE) {
		// prevent leak in case of Lua error
		*wrapper = resolver;
		lua_pushvalue(L, src_index);
		// lua stack: toplevel/w/src
		lua_gettable(L, -3);
		if (!lua_istable(L, -1)) {
			// create nested table
			lua_pop(L, 1);
			lua_newtable(L);
			// lua stack: toplevel/w/nested
			lua_getmetatable(L, -3);
			// lua stack: toplevel/w/nested/toplevel-metatable
			lua_setmetatable(L, -2);
			lua_pushvalue(L, src_index);
			lua_pushvalue(L, -2);
			// lua stack: toplevel/w/nested/src/nested
			lua_settable(L, -5);
		}
		// lua stack: toplevel/w/nested
		lua_pushvalue(L, dest_index);
		// lua stack: toplevel/w/nested/dest
		lua_pushvalue(L, -3);
		// lua stack: toplevel/w/nested/dest/w
		lua_settable(L, -3);
		// lua stack: toplevel/w/nested
		lua_pop(L, 3);
		// after Lua is done
		avro_value_iface_incref(resolver);
	}
	return resolver;
}

static int
create_resolver(struct lua_State *L)
{
	/* used in tests, primarily */
	avro_value_iface_t *resolver;
	luaL_checkudata(L, 1, schema_typename);
	luaL_checkudata(L, 2, schema_typename);
	resolver = create_resolver(L, 1, 2, DO_CACHE);
	if (resolver == NULL) {
		lua_pushstring(L, avro_strerror());
		return 1;
	} else {
		avro_value_iface_decref(resolver);
	}
	return 0;
}

static int
schema_is_compatible(struct lua_State *L)
{
	avro_value_iface_t *resolver;

	luaL_checkudata(L, 1, schema_typename);
	luaL_checkudata(L, 2, schema_typename);

	resolver = create_resolver(L, 1, 2, DONT_CACHE);
	if (resolver) {
		avro_value_iface_decref(resolver);
		lua_pushboolean(L, 1);
		return 1;
	}
	lua_pushboolean(L, 0);
	lua_pushstring(L, avro_strerror());
	return 2;
}

// returns the number of elements pushed into the Lua stack,
// nonzero iff error
static int
prepare_xform(struct lua_State *L, struct xform_ctx **xform_ctx)
{
	struct schema_plus *src_schema;
	struct schema_plus *dest_schema = NULL;

	src_schema = (struct schema_plus *)
		luaL_checkudata(L, 2, schema_typename);

	if (lua_isnoneornil(L, 3)) {
		dest_schema = src_schema;
	} else {
		dest_schema = (struct schema_plus *)
			luaL_checkudata(L, 3, schema_typename);
	}

	if (dest_schema->gclass == NULL) {
		dest_schema->gclass =
			avro_generic_class_from_schema(dest_schema->schema);
		if (dest_schema->gclass == NULL) {
			goto avro_error;
		}
	}

	*xform_ctx = create_xform_ctx(L);

	if (avro_generic_value_new(
			dest_schema->gclass, &(*xform_ctx)->dest) != 0) {
		goto avro_error;
	}

	if (dest_schema == src_schema) {
		avro_value_copy_ref(&(*xform_ctx)->src, &(*xform_ctx)->dest);
	} else {
		avro_value_iface_t *resolver;
		resolver = create_resolver(L, 2, 3, DO_CACHE);
		if (resolver == NULL) {
			lua_pushboolean(L, 0);
			lua_pushfstring(
				L, "schemas incompatible: %s",
				avro_strerror());
			return 2;
		}
		// already retained by the cache
		avro_value_iface_decref(resolver);
		if (avro_resolved_writer_new_value(
				resolver, &(*xform_ctx)->src) != 0) {
			goto avro_error;
		}
		avro_resolved_writer_set_dest(
			&(*xform_ctx)->src, &(*xform_ctx)->dest);
	}

	return 0;
avro_error:
	lua_pushboolean(L, 0);
	lua_pushstring(L, avro_strerror());
	return 2;
}

const int lua_ir_b_options =
	ASSUME_NUL_TERM_STRINGS |
	ASSUME_NO_DUP_MAP_KEYS |
	ENABLE_FAST_SKIP |
	ENABLE_VERBOSE_RECORDS |
	ASSUME_STRING_ENUM_CODING |
	ASSUME_STRING_UNION_TAG_CODING;

const int lua_ir_v_options =
	ENABLE_VERBOSE_RECORDS |
	ASSUME_STRING_ENUM_CODING |
	ASSUME_STRING_UNION_TAG_CODING;

const int mpk_ir_b_options =
	ENABLE_FAST_SKIP |
	ENABLE_VERBOSE_RECORDS |
	ASSUME_STRING_ENUM_CODING |
	ASSUME_STRING_UNION_TAG_CODING;

static int
flatten(struct lua_State *L)
{
	struct xform_ctx *xform_ctx;
	int st;
	st = prepare_xform(L, &xform_ctx);
	if (st != 0) {
		goto out;
	}
	try
	{
		{
			lua_parser_context          pc(L, 1);
			lua_parser                  parser(pc);

			ir_builder<lua_ir_b_options>
				builder (CONSUME_REGULAR, &xform_ctx->bitmap);

			builder.build_value(parser, &xform_ctx->src);
		}
		{
			lua_emitter_context         ec(L);
			lua_emitter                 emitter(ec);

			ir_visitor<lua_ir_v_options>
				visitor (EMIT_FLATTENED);

			visitor.visit_value(emitter, &xform_ctx->dest);
		}
		lua_pushboolean(L, 1);
		lua_insert(L, -2);
		st  = 2;
		goto out;
	}
	catch (std::exception &e)
	{
		lua_pushboolean(L, 0);
		lua_pushstring(L, e.what());
		st = 2;
		goto out;
	}
out:
	finalize_xform_ctx(xform_ctx);
	return st;
}

static int
xflatten(struct lua_State *L)
{
	struct xform_ctx *xform_ctx;
	int st;
	st = prepare_xform(L, &xform_ctx);
	if (st != 0) {
		goto out;
	}
	try
	{
		{
			lua_parser_context          pc(L, 1);
			lua_parser                  parser(pc);

			ir_builder<lua_ir_b_options>
				builder (CONSUME_REGULAR, &xform_ctx->bitmap);

			builder.build_value_for_update(parser, &xform_ctx->src);
		}
		{
			lua_emitter_context         ec(L);
			lua_emitter                 emitter(ec);

			ir_visitor<lua_ir_v_options>
				visitor (EMIT_FLATTENED);

			visitor.visit_value_for_update(
				emitter, &xform_ctx->dest,
				&xform_ctx->bitmap[0]);
		}
		lua_pushboolean(L, 1);
		lua_insert(L, -2);
		st  = 2;
		goto out;
	}
	catch (std::exception &e)
	{
		lua_pushboolean(L, 0);
		lua_pushstring(L, e.what());
		st = 2;
		goto out;
	}
out:
	finalize_xform_ctx(xform_ctx);
	return st;
}

static int
unflatten(struct lua_State *L)
{
	struct xform_ctx *xform_ctx;
	int st;
	st = prepare_xform(L, &xform_ctx);
	if (st != 0) {
		goto out;
	}
	try
	{
		box_tuple_t *t = NULL;
		if ((t = lua_istuple(L, 1)) != NULL) {

			// XXX no box_tuple_data()
			const uint8_t *p =
				reinterpret_cast<const uint8_t *>(t) + 12;

			mpk_fast_parsers::mpk_parser_context
				pc(Bytes(p, box_tuple_bsize(t)));

			ir_builder<mpk_ir_b_options>
				builder(CONSUME_FLATTENED, &xform_ctx->bitmap);

			avro_type_t t =
				avro_value_get_type(&xform_ctx->src);

			if (t == AVRO_RECORD) {
				mpk_fast_parsers::mpk_terse_record_parser
					parser(pc);

				builder.build_terse_record_value(
					parser, &xform_ctx->src);
				// parser.kill() intentionally omited
			} else {
				mpk_fast_parsers::mpk_parser parser(pc);

				builder.build_value(
					parser, &xform_ctx->src, t);
			}
		} else {
			lua_parser_context          pc(L, 1);
			lua_parser                  parser(pc);

			ir_builder<lua_ir_b_options>
				builder (CONSUME_FLATTENED,
					 &xform_ctx->bitmap);

			builder.build_value(parser, &xform_ctx->src);
		}
		{
			lua_emitter_context         ec(L);
			lua_emitter                 emitter(ec);
			ir_visitor<lua_ir_v_options>visitor(EMIT_REGULAR);
			visitor.visit_value(emitter, &xform_ctx->dest);
		}
		lua_pushboolean(L, 1);
		lua_insert(L, -2);
		st  = 2;
		goto out;
	}
	catch (std::exception &e)
	{
		lua_pushboolean(L, 0);
		lua_pushstring(L, e.what());
		st = 2;
		goto out;
	}
out:
	finalize_xform_ctx(xform_ctx);
	return st;
}

static int
get_metatables(struct lua_State *L)
{
	// used in tests to hook __gc
	luaL_getmetatable(L, schema_typename);
	luaL_getmetatable(L, resolver_typename);
	luaL_getmetatable(L, xform_ctx_typename);
	return 3;
}

static size_t
register_schema_names(
	struct lua_State *L, avro_schema_t rec,
	size_t out_index, int tab_index)
{
	bool toplevel = (tab_index == lua_gettop(L));
	size_t n = avro_schema_record_size(rec);
	for (size_t i = 0; i < n; i++) {

		const char *name = avro_schema_record_field_name(rec, i);
		avro_schema_t field = avro_schema_record_field_get_by_index(
			rec, i);

		if (toplevel)
			lua_pushstring(L, name);
		else
			lua_pushfstring(
				L, "%s.%s", lua_tostring(L, -1), name);

		switch (avro_typeof(field)) {
		case AVRO_RECORD:
			out_index = register_schema_names(
				L, field, out_index, tab_index);
			lua_pop(L, 1);
			break;
		case AVRO_UNION:
			lua_pushfstring(
				L, "%s.$type$", lua_tostring(L, -1), name);
			lua_rawseti(L, tab_index, out_index++);
			/* fallthrough */
		default:
			lua_rawseti(L, tab_index, out_index++);
			break;
		}
	}
	return out_index;
}

static int
get_schema_names(struct lua_State *L)
{
	struct schema_plus *schema;
	schema = (struct schema_plus *)
		luaL_checkudata(L, 1, schema_typename);
	lua_createtable(L, 0, 0);
	int top = lua_gettop(L);
	if (avro_typeof(schema->schema) == AVRO_RECORD)
		register_schema_names(L, schema->schema, 1, top);
	return 1;
}

static size_t
register_schema_types(
	struct lua_State *L, avro_schema_t rec, size_t out_index)
{
	size_t n = avro_schema_record_size(rec);
	for (size_t i = 0; i < n; i++) {
		avro_schema_t field = avro_schema_record_field_get_by_index(
			rec, i);
		switch (avro_typeof(field)) {
		case AVRO_UNION:
			out_index += 2;
			break;
		case AVRO_RECORD:
			out_index = register_schema_types(L, field, out_index);
			break;
		default:
			{
				const char *ns;
				const char *name;
				ns = avro_schema_namespace(field);
				name = avro_schema_type_name(field);
				if (ns != NULL) {
					lua_pushfstring(L, "%s.%s", ns, name);
				} else {
					lua_pushstring(L, name);
				}
				lua_rawseti(L, -2, out_index++);
			}
			break;
		}
	}
	return out_index;
}

static int
get_schema_types(struct lua_State *L)
{
	struct schema_plus *schema;
	schema = (struct schema_plus *)
		luaL_checkudata(L, 1, schema_typename);
	lua_createtable(L, 0, 0);
	if (avro_typeof(schema->schema) == AVRO_RECORD)
		register_schema_types(L, schema->schema, 1);
	return 1;
}

extern "C" {

LUA_API int
luaopen_avro(lua_State *L)
	__attribute__((__visibility__("default")));

LUA_API int
luaopen_avro(lua_State *L)
{
	static const struct luaL_reg lib [] = {
		{"schema_is_compatible", schema_is_compatible},
		{"flatten",              flatten},
		{"unflatten",            unflatten},
		{"xflatten",             xflatten},
		{"get_schema_names",     get_schema_names},
		{"get_schema_types",     get_schema_types},
		{NULL, NULL}
	};
	// avro.schema
	luaL_newmetatable(L, schema_typename);
	lua_pushcclosure(L, schema_gc, 0);
	lua_setfield(L, -2, "__gc");
	lua_pushcclosure(L, schema_to_string, 0);
	lua_setfield(L, -2, "__tostring");
	// avro.resolver
	luaL_newmetatable(L, resolver_typename);
	lua_pushcclosure(L, resolver_gc, 0);
	lua_setfield(L, -2, "__gc");
	lua_pushcclosure(L, resolver_to_string, 0);
	lua_setfield(L, -2, "__tostring");
	// avvo.xform-ctx
	luaL_newmetatable(L, xform_ctx_typename);
	lua_pushcclosure(L, xform_ctx_gc, 0);
	lua_setfield(L, -2, "__gc");
	lua_pushcclosure(L, xform_ctx_to_string, 0);
	lua_setfield(L, -2, "__tostring");

	lua_newtable(L);
	luaL_register(L, NULL, lib);
	lua_pushnil(L);
	lua_pushcclosure(L, create_schema, 1);
	lua_setfield(L, -2, "create_schema");
	return 1;
}

LUA_API int
luaopen_avrotest(lua_State *L)
	__attribute__((__visibility__("default")));

// Alt entry point, exposes internals
LUA_API int
luaopen_avrotest(lua_State *L)
{
	static const struct luaL_reg lib [] = {
		{"_get_resolver_cache",  get_resolver_cache},
		{"_create_resolver",     create_resolver},
		{"_get_metatables",      get_metatables},
		{NULL, NULL}
	};
	luaopen_avro(L);
	luaL_register(L, NULL, lib);
	return 1;
}

}
