#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

enum TypeId {
    NilValue         = 1,
    FalseValue       = 2,
    TrueValue        = 3,
    LongValue        = 4,
    UlongValue       = 5, /* parser prefers LongValue */
    FloatValue       = 6,
    DoubleValue      = 7,
    StringValue      = 8,
    BinValue         = 9,
    ExtValue         = 10,

    ArrayValue       = 11,
    MapValue         = 12,

    CStringValue     = 18,
    CBinValue        = 19,
    CopyCommand      = 20 /* Copy N bytes verbatim from data bank.
                           * Provides complex default values. Also
                           * strings during unflatten.
                           */
};

struct Value {
    union {
        void          *p;
        int64_t        ival;
        uint64_t       uval;
        double         dval;
        struct {
            uint32_t   xlen;
            uint32_t   xoff;
        };
    };
};

/*
 * TypeId-s and Value-s live in two parallel arrays.
 *
 * NilValue         - (value allocated but unused)
 * FalseValue       - (value allocated but unused)
 * TrueValue        - (value allocated but unused)
 * LongValue        - ival
 * UlongValue       - uval
 * FloatValue       - dval
 * DoubleValue      - dval
 * StringValue      - xlen, xoff
 * BinValue         - xlen, xoff
 * ExtValue         - xlen, xoff
 * ArrayValue       - xlen, xoff
 * MapValue         - xlen, xoff
 */

ssize_t
parse_msgpack(const uint8_t *msgpack_in,
              size_t         msgpack_size,
              size_t         stock_buf_size_or_hint,
              uint8_t       *stock_typeid_buf,
              struct Value  *stock_value_buf,
              uint8_t      **typeid_out,
              struct Value **value_out

) __attribute__((__visibility__("default")));

ssize_t
unparse_msgpack(size_t             nitems,
               const uint8_t     *typeid,
               const struct Value*value,
               const uint8_t     *bank1,
               const uint8_t     *bank2,
               size_t             stock_buf_size_or_hint,
               uint8_t           *stock_buf,
               uint8_t          **msgpack_out

) __attribute__((__visibility__("default")));

uint32_t
create_hash_func(int n, const unsigned char *strings[],
                 const unsigned char *random, size_t size_random
) __attribute__((__visibility__("default")));

uint32_t
eval_hash_func(uint32_t func, const unsigned char *str, size_t len
) __attribute__((__visibility__("default")));

uint32_t
eval_fnv1a_func(uint32_t seed, const unsigned char *str, size_t len
) __attribute__((__visibility__("default")));

static int
collisions_found(uint32_t func, int n, const unsigned char *strings[],
                 void *mem);

static uint32_t
create_fnv_func(int n, const unsigned char *strings[],
                const unsigned char *random, size_t size_random,
                void *mem);

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define net2host16(v) __builtin_bswap16(v)
#define net2host32(v) __builtin_bswap32(v)
#define net2host64(v) __builtin_bswap64(v)
#define host2net16(v) __builtin_bswap16(v)
#define host2net32(v) __builtin_bswap32(v)
#define host2net64(v) __builtin_bswap64(v)
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define net2host16(v) (v)
#define net2host32(v) (v)
#define net2host64(v) (v)
#define host2net16(v) (v)
#define host2net32(v) (v)
#define host2net64(v) (v)
#else
#error Unsupported __BYTE_ORDER__
#endif

#define unaligned(p) ((struct unaligned_storage *)(p))

struct unaligned_storage
{
    union {
        uint16_t u16;
        uint32_t u32;
        uint64_t u64;
        float    f32;
        double   f64;
    };
}
__attribute__((__packed__));

static void *realloc_wrap(void *buf, size_t size,
                          void *stock_buf, size_t old_size)
{
    if (buf != stock_buf)
        return realloc(buf, size);
    buf = malloc(size);
    if (buf == NULL)
        return NULL;
    return memcpy(buf, stock_buf, old_size);
}

ssize_t parse_msgpack(const uint8_t * restrict mi,
                      size_t        ms,
                      size_t        sz_or_hint,
                      uint8_t      *stock_typeid_buf,
                      struct Value *stock_value_buf,
                      uint8_t     **typeid_out,
                      struct Value **value_out)
{
    const uint8_t *me = mi + ms;
    uint8_t       * restrict typeid, *typeid_max, *typeid_buf;
    struct Value  * restrict value, *value_buf = NULL;
    uint32_t       todo = 1, patch = -1;
    uint32_t       auto_stack_buf[32];
    uint32_t      *stack_buf = auto_stack_buf, *stack_max = auto_stack_buf + 32;
    uint32_t      *stack = stack_buf;
    uint32_t       len;

    if (stock_typeid_buf != NULL && stock_value_buf != NULL) {
        typeid = typeid_buf = stock_typeid_buf;
        typeid_max = stock_typeid_buf + sz_or_hint * sizeof(typeid[0]);
        value = value_buf = stock_value_buf;
    } else {
        size_t ic = 32; /* initial capacity */
        if (sz_or_hint > ic)
            ic = sz_or_hint;

        typeid_max = (typeid = typeid_buf = malloc(ic * sizeof(typeid[0]))) + ic;
        if (typeid_buf == NULL)
            goto error_alloc;

        value = value_buf = malloc(ic * sizeof(value[0]));
        if (value_buf == NULL)
            goto error_alloc;
    }

    if (0) {
repeat:
        value++; typeid++;
    }

    while (todo -- == 0) {
        struct Value *fixit;

        if (stack == stack_buf)
            goto done;

        todo = *--stack;
        fixit = value_buf + patch;
        patch = fixit->xoff;
        fixit->xoff = value - fixit;
    }

    if (mi == me)
        goto error_underflow;

    /* ensure output has capacity for 1 more item */
    if (__builtin_expect(typeid == typeid_max, 0)) {
        size_t          capacity = typeid_max - typeid_buf;
        size_t          new_capacity = capacity + capacity / 2;
        uint8_t        *new_typeid_buf;
        struct Value   *new_value_buf;

        new_typeid_buf = realloc_wrap(typeid_buf, new_capacity * sizeof(typeid[0]),
                                      stock_typeid_buf, capacity * sizeof(typeid[0]));
        if (new_typeid_buf == NULL)
            goto error_alloc;

        typeid     = new_typeid_buf + capacity;
        typeid_buf = new_typeid_buf;
        typeid_max = new_typeid_buf + new_capacity;

        new_value_buf = realloc_wrap(value_buf, new_capacity * sizeof(value[0]),
                                     stock_value_buf, capacity * sizeof(value[0]));
        if (new_value_buf == NULL)
            goto error_alloc;

        value     = new_value_buf + capacity;
        value_buf = new_value_buf;
    }

    switch (*mi) {
    case 0x00 ... 0x7f:
        /* positive fixint */
        *typeid = LongValue;
        value->ival = *mi++;
        goto repeat;
    case 0x80 ... 0x8f:
        /* fixmap */
        len = *mi++ - 0x80;
        *typeid = MapValue;
        value->xlen = len;
        len *= 2;
        goto setup_nested;
    case 0x90 ... 0x9f:
        /* fixarray */
        len = *mi++ - 0x90;
        *typeid = ArrayValue;
        value->xlen = len;
setup_nested:
        value->xoff = patch;
        patch = value - value_buf;
        if (__builtin_expect(stack == stack_max, 0)) {
            size_t      capacity = stack_max - stack_buf;
            size_t      new_capacity = capacity + capacity/2;
            uint32_t   *new_stack_buf;

            new_stack_buf = realloc_wrap(
                stack_buf, new_capacity * sizeof(stack[0]),
                auto_stack_buf, capacity * sizeof(stack[0]));

            if (new_stack_buf == NULL)
                goto error_alloc;

            stack     = new_stack_buf + capacity;
            stack_buf = new_stack_buf;
            stack_max = new_stack_buf + new_capacity;
        }
        *stack++ = todo;
        todo = len;
        goto repeat;
    case 0xa0 ... 0xbf:
        /* fixstr */
        len = *mi - 0xa0;
        *typeid = StringValue;
        /* string, bin and ext jumps here */
do_xdata:
        if (mi + len + 1 > me)
            goto error_underflow;
        value->xlen = len;
        /* offset relative to blob end! (saves a reg) */
        value->xoff = (me - mi - 1);
        mi += len + 1;
        goto repeat;
    case 0xc0:
        *typeid = NilValue;
        mi++;
        goto repeat;
    case 0xc1:
        /* invalid */
        goto error_c1;
    case 0xc2:
        /* false */
        *typeid = FalseValue;
        mi++;
        goto repeat;
    case 0xc3:
        /* true */
        *typeid = TrueValue;
        mi++;
        goto repeat;
    case 0xc4:
        /* bin 8 */
        if (mi + 2 > me)
            goto error_underflow;
        *typeid = BinValue;
        len = mi[1];
        mi += 1;
        goto do_xdata;
    case 0xc5:
        /* bin 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = BinValue;
        len = net2host16(unaligned(mi + 1)->u16);
        mi += 2;
        goto do_xdata;
    case 0xc6:
        /* bin 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = BinValue;
        len = net2host32(unaligned(mi + 1)->u32);
        mi += 4;
        goto do_xdata;
    case 0xc7:
        /* ext 8 */
        if (mi + 2 > me)
            goto error_underflow;
        *typeid = ExtValue;
        len = mi[1] + 1;
        mi += 1;
        goto do_xdata;
    case 0xc8:
        /* ext 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = ExtValue;
        len = net2host16(unaligned(mi + 1)->u16);
        mi += 2;
        goto do_xdata;
    case 0xc9:
        /* ext 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = ExtValue;
        len = net2host32(unaligned(mi + 1)->u32);
        mi += 4;
        goto do_xdata;
    case 0xca: {
        /* float 32 */
        struct unaligned_storage ux;
        if (mi + 5 > me)
            goto error_underflow;
        ux.u32 = net2host32(unaligned(mi + 1)->u32);
        *typeid = FloatValue;
        value->dval = ux.f32;
        mi += 5;
        goto repeat;
    }
    case 0xcb: {
        /* float 64 */
        struct unaligned_storage ux;
        if (mi + 9 > me)
            goto error_underflow;
        ux.u64 = net2host64(unaligned(mi + 1)->u64);
        *typeid = DoubleValue;
        value->dval = ux.f64;
        mi += 9;
        goto repeat;
    }
    case 0xcc:
        /* uint 8 */
        if (mi + 2 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = mi[1];
        mi += 2;
        goto repeat;
    case 0xcd:
        /* uint 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = net2host16(unaligned(mi + 1)->u16);
        mi += 3;
        goto repeat;
    case 0xce:
        /* uint 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = net2host32(unaligned(mi + 1)->u32);
        mi += 5;
        goto repeat;
    case 0xcf: {
        /* uint 64 */
        uint64_t v;
        if (mi + 9 > me)
            goto error_underflow;
        v = net2host64(unaligned(mi + 1)->u64);
        if (v > (uint64_t)INT64_MAX) {
            *typeid = UlongValue;
            value->uval = v;
            mi += 9;
            goto repeat;
        }
        *typeid = LongValue;
        value->ival = v;
        mi += 9;
        goto repeat;
    }
    case 0xd0:
        /* int 8 */
        if (mi + 2 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = (int8_t)mi[1];
        mi += 2;
        goto repeat;
    case 0xd1:
        /* int 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = (int16_t)net2host16(unaligned(mi + 1)->u16);
        mi += 3;
        goto repeat;
    case 0xd2:
        /* int 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = (int32_t)net2host32(unaligned(mi + 1)->u32);
        mi += 5;
        goto repeat;
    case 0xd3:
        /* int 64 */
        if (mi + 9 > me)
            goto error_underflow;
        *typeid = LongValue;
        value->ival = (int64_t)net2host64(unaligned(mi + 1)->u64);
        mi += 9;
        goto repeat;
    case 0xd4:
    case 0xd5:
        /* fixext 1, 2 */
        len = *mi - 0xd3;
        *typeid = ExtValue;
        goto do_xdata;
    case 0xd6:
        /* fixext 4 */
        len = 5;
        *typeid = ExtValue;
        goto do_xdata;
    case 0xd7:
        /* fixext 8 */
        len = 9;
        *typeid = ExtValue;
        goto do_xdata;
    case 0xd8:
        /* fixext 16 */
        len = 17;
        *typeid = ExtValue;
        goto do_xdata;
    case 0xd9:
        /* str 8 */
        if (mi + 2 > me)
            goto error_underflow;
        *typeid = StringValue;
        len = mi[1];
        mi += 1;
        goto do_xdata;
    case 0xda:
        /* str 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = StringValue;
        len = net2host16(unaligned(mi + 1)->u16);
        mi += 2;
        goto do_xdata;
    case 0xdb:
        /* str 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = StringValue;
        len = net2host32(unaligned(mi + 1)->u32);
        mi += 4;
        goto do_xdata;
    case 0xdc:
        /* array 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = ArrayValue;
        len = net2host16(unaligned(mi + 1)->u16);
        mi += 3;
        value->xlen = len;
        goto setup_nested;
    case 0xdd:
        /* array 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = ArrayValue;
        len = net2host32(unaligned(mi + 1)->u32);
        mi += 5;
        value->xlen = len;
        goto setup_nested;
    case 0xde: /* map 16 */
        if (mi + 3 > me)
            goto error_underflow;
        *typeid = MapValue;
        len = net2host16(unaligned(mi + 1)->u16);
        mi += 3;
        value->xlen = len;
        len *= 2;
        goto setup_nested;
    case 0xdf: /* map 32 */
        if (mi + 5 > me)
            goto error_underflow;
        *typeid = MapValue;
        len = net2host32(unaligned(mi + 1)->u32);
        mi += 5;
        value->xlen = len;
        goto setup_nested;
    case 0xe0 ... 0xff:
        /* negative fixint */
        *typeid = LongValue;
        value->ival = (int8_t)*mi++;
        goto repeat;
    }

done:
    if (stack_buf != auto_stack_buf)
        free(stack_buf);
    *typeid_out = typeid_buf;
    *value_out = value_buf;
    return typeid - typeid_buf;

error_underflow:
error_c1:
error_alloc:
    if (stack_buf != auto_stack_buf)
        free(stack_buf);
    if (typeid_buf != stock_typeid_buf)
        free(typeid_buf);
    if (value_buf != stock_value_buf);
        free(value_buf);
    return -1;
}

ssize_t unparse_msgpack(size_t nitems,
                        const uint8_t * restrict typeid,
                        const struct Value * restrict value,
                        const uint8_t * restrict bank1,
                        const uint8_t * restrict bank2,
                        size_t    sz_or_hint,
                        uint8_t  *stock_buf,
                        uint8_t **msgpack_out)
{
    const uint8_t *typeid_max = typeid + nitems;
    uint8_t * restrict out, *out_max, *out_buf;
    const uint8_t * restrict copy_from = bank1;

    if (stock_buf != NULL) {
        out = out_buf = stock_buf;
        out_max = stock_buf + sz_or_hint;
    } else {
        size_t initial_capacity = nitems > 128 ? nitems : 128;
        if (sz_or_hint > initial_capacity)
            initial_capacity = sz_or_hint;
        out_buf = malloc(initial_capacity);
        if (out_buf == NULL)
            goto error_alloc;
        out = out_buf;
        out_max = out_buf + initial_capacity;
    }

    for (; typeid != typeid_max; typeid++, value++) {

        /* precondition: at least 10 bytes avail in out */

        switch (*typeid) {
        default:
            goto error_badcode;
        case NilValue:
            *out ++ = 0xc0;
            goto check_buf;
        case FalseValue:
            *out ++ = 0xc2;
            goto check_buf;
        case TrueValue:
            *out ++ = 0xc3;
            goto check_buf;
        case LongValue:
            /*
             * Note: according to the MsgPack spec, signed and unsigned
             * integer families are different 'presentations' of
             * Integer type (i.e. signedness isn't a core value property
             * worth preserving).
             * It's faster to encode it the way we do, i.e. to use signed
             * presentations for negative values only.
             * Also, Tarantool friendly (can't index signed integers).
             * Assuming 2-complement signed integers.
             */
            if (value->uval > (uint64_t)INT64_MAX /* negative val */) {
                if (value->uval >= (uint64_t)-0x20) {
                    *out++ = (uint8_t)value->uval;
                    goto check_buf;
                }
                if (value->uval >= (uint64_t)INT8_MIN) {
                    out[0] = 0xd0;
                    out[1] = (uint8_t)value->uval;
                    out += 2;
                    goto check_buf;
                }
                if (value->uval >= (uint64_t)INT16_MIN) {
                    out[0] = 0xd1;
                    unaligned(out + 1)->u16 = host2net16((uint16_t)value->uval);
                    out += 3;
                    goto check_buf;
                }
                if (value->uval >= (uint64_t)INT32_MIN) {
                    out[0] = 0xd2;
                    unaligned(out + 1)->u32 = host2net32((uint32_t)value->uval);
                    out += 5;
                    goto check_buf;
                }
                out[0] = 0xd3;
                unaligned(out + 1)->u64 = host2net64(value->uval);
                out += 9;
                goto check_buf;
            }
            /* fallthrough */
        case UlongValue:
            if (value->uval <= 0x7f) {
                *out++ = (uint8_t)value->uval;
                goto check_buf;
            }
            if (value->uval <= UINT8_MAX) {
                out[0] = 0xcc;
                out[1] = (uint8_t)value->uval;
                out += 2;
                goto check_buf;
            }
            if (value->uval <= UINT16_MAX) {
                out[0] = 0xcd;
                unaligned(out + 1)->u16 = host2net16((uint16_t)value->uval);
                out += 3;
                goto check_buf;
            }
            if (value->uval <= UINT32_MAX) {
                out[0] = 0xce;
                unaligned(out + 1)->u32 = host2net32((uint32_t)value->uval);
                out += 5;
                goto check_buf;
            }
            out[0] = 0xcf;
            unaligned(out + 1)->u64 = host2net64(value->uval);
            out += 9;
            goto check_buf;
        case FloatValue: {
            struct unaligned_storage ux;
            ux.f32 = (float)value->dval;
            out[0] = 0xca;
            unaligned(out + 1)->u32 = host2net32(ux.u32);
            out += 5;
            goto check_buf;
        }
        case DoubleValue: {
            struct unaligned_storage ux;
            ux.f64 = value->dval;
            out[0] = 0xcb;
            unaligned(out + 1)->u64 = host2net64(ux.u64);
            out += 9;
            goto check_buf;
        }
        case CStringValue:
            copy_from = bank2;
            /* fallthrough */
        case StringValue:
            if (value->xlen <= 31) {
                *out++ = 0xa0 + (uint8_t)value->xlen;
                goto copy_data;
            }
            if (value->xlen <= UINT8_MAX) {
                out[0] = 0xd9;
                out[1] = (uint8_t)value->xlen;
                out += 2;
                goto copy_data;
            }
            if (value->xlen <= UINT16_MAX) {
                out[1] = 0xda;
                unaligned(out+1)->u16 = host2net16((uint16_t)value->xlen);
                out += 3;
                goto copy_data;
            }
            out[1] = 0xdb;
            unaligned(out+1)->u32 = host2net32(value->xlen);
            out += 5;
            goto copy_data;
        case CBinValue:
            copy_from = bank2;
            /* fallthrough */
        case BinValue:
            if (value->xlen <= UINT8_MAX) {
                out[0] = 0xc4;
                out[1] = (uint8_t)value->xlen;
                out += 2;
                goto copy_data;
            }
            if (value->xlen <= UINT16_MAX) {
                out[0] = 0xc5;
                unaligned(out + 1)->u16 = host2net16((uint16_t)value->xlen);
                out += 3;
                goto copy_data;
            }
            out[0] = 0xc6;
            unaligned(out + 1)->u32 = host2net32(value->xlen);
            out += 5;
            goto copy_data;
        case ExtValue:
            switch (value->xlen) {
            case 2:
                /* fixext 1 */
                out[0] = 0xd4;
                unaligned(out + 1)->u16 = unaligned(copy_from - value->xoff)->u16;
                out += 3;
                goto check_buf;
            case 3:
                /* fixext 2 */
                out[0] = 0xd5;
                out[1] = (copy_from - value->xoff)[0];
                unaligned(out + 2)->u16 = unaligned(copy_from - value->xoff + 1)->u16;
                out += 4;
                goto check_buf;
            case 5:
                /* fixext 4 */
                out[0] = 0xd6;
                out[1] = (copy_from - value->xoff)[0];
                unaligned(out + 2)->u32 = unaligned(copy_from - value->xoff + 1)->u32;
                out += 6;
                goto check_buf;
            case 9:
                /* fixext 8 */
                out[0] = 0xd5;
                out[1] = (copy_from - value->xoff)[0];
                unaligned(out + 2)->u64 = unaligned(copy_from - value->xoff + 1)->u64;
                out += 10;
                goto check_buf;
            case 17:
                /* fixext 16 */
                *out++ = 0xd8;
                goto copy_data;
            }
            if (value->xlen - 1 <= UINT8_MAX) {
                out[0] = 0xc7;
                out[1] = (uint8_t)(value->xlen - 1);
                out += 2;
                goto copy_data;
            }
            if (value->xlen - 1 <= UINT16_MAX) {
                out[0] = 0xc8;
                unaligned(out + 1)->u16 = host2net16((uint16_t)(value->xlen - 1));
                out += 3;
                goto copy_data;
            }
            out[0] = 0xc9;
            unaligned(out + 1)->u32 = host2net32(value->xlen - 1);
            out += 5;
            goto copy_data;
        case ArrayValue:
            if (value->xlen <= 15) {
                *out++ = 0x90 + (uint8_t)value->xlen;
                goto check_buf;
            }
            if (value->xlen <= UINT16_MAX) {
                out[0] = 0xdc;
                unaligned(out + 1)->u16 = host2net16((uint16_t)value->xlen);
                out += 3;
                goto check_buf;
            }
            out[0] = 0xdd;
            unaligned(out + 1)->u32 = host2net32(value->xlen);
            out += 5;
            goto check_buf;
        case MapValue:
            if (value->xlen <= 15) {
                *out++ = 0x80 + (uint8_t)value->xlen;
                goto check_buf;
            }
            if (value->xlen <= UINT16_MAX) {
                out[0] = 0xde;
                unaligned(out + 1)->u16 = host2net16((uint16_t)value->xlen);
                out += 3;
                goto check_buf;
            }
            out[0] = 0xdf;
            unaligned(out + 1)->u32 = host2net32(value->xlen);
            out += 5;
            goto check_buf;
        case CopyCommand:
            copy_from = bank2;
            goto copy_data;
        }

check_buf:
        /*
         * Restore invariant: at least 10 bytes available in out_buf.
         * Almost every switch branch ends up jumping here.
         */
        if (out + 10 > out_max) {
            size_t capacity = out_max - out_buf;
            size_t new_capacity = capacity + capacity / 2;
            uint8_t *new_out_buf = realloc_wrap(out_buf, new_capacity,
                                                stock_buf, capacity);
            if (new_out_buf == NULL)
                goto error_alloc;
            out     = new_out_buf + (out - out_buf);
            out_buf = new_out_buf;
            out_max = new_out_buf + new_capacity;
        }
        continue;

copy_data:
        /*
         * Ensure we have a room fom value->xlen bytes in out_buf, plus
         * 10 more bytes for the next iteration.
         * Some switch branches end up jumping here.
         */
        if (out + value->xlen + 10 > out_max) {
            size_t capacity = out_max - out_buf;
            size_t new_capacity = capacity + capacity / 2;
            uint8_t *new_out_buf = realloc_wrap(out_buf, new_capacity,
                                                stock_buf, capacity);
            if (new_out_buf == NULL)
                goto error_alloc;
            out     = new_out_buf + (out - out_buf);
            out_buf = new_out_buf;
            out_max = new_out_buf + new_capacity;
        }
        if (__builtin_expect(value->xoff == UINT32_MAX, 0)) {
            /* Offset is too big; next item contains explicit ptr. */
            memcpy(out, value[1].p, value->xlen);
            out += value->xlen;
            value++;
            typeid++;
        } else {
            memcpy(out, copy_from - value->xoff, value->xlen);
            out += value->xlen;
        }
        copy_from = bank1;
        continue;
    }

    *msgpack_out = out_buf;
    return out - out_buf;

error_alloc:
error_badcode:
    if (out_buf != stock_buf)
        free(out_buf);
    return -1;
}

/*
 * create_hash_func - creates a function mapping a string to
 *                    an (unsigned) integer with no collisions
 *                    on the given string set
 * @returns
 *
 * 0          - failed to create a perfect hash func
 * 0x???????? - FNV1A + a 4 byte random prefix (MSB > 0xf)
 *
 * 0x01p1     - sample specified positions, combine with '+'
 * 0x02p1p2     positions must not exceed the length of the shortest
 * 0x03p1p2p3   string in the set
 * 0x04       - same as above, but include length as well
 * 0x05p1
 * 0x06p1p2
 * 0x07p1p2p3
 *
 * 0x09p1     - length? and up to 3 positions, result is 32bit
 * 0x0ap1p2     see eval_hash_func()
 * 0x0bp1p2p3
 * 0x0c
 * 0x0dp1
 * 0x0ep1p2
 * 0x0fp1p2p3
 *
 * A chunk of random data is passed explicitly (i.e. random/size_random.)
 * The function gets n ASCII-Z strings.
 */
uint32_t create_hash_func(int n, const unsigned char *strings[],
                          const unsigned char *random, size_t size_random)
{
    /*
     * Select sampling positions with a simple greedy algorithm:
     * 1) initially, all strings are in the same collision domain;
     * 2) for each valid position, count collisions -
     *    eg: let strings be { 'March', 'May' },
     *        pos #0 yields 1 collision ('M'/'M'),
     *        pos #2 yields no collisions ('r'/'y')
     *    Note: elements in distinct domains don't collide.
     * 3) pick a position yielding the min number of collisions;
     * 4) split collision domain(s) based on the characters in selected
     *    position;
     * 5) repeat step #2.
     */
    enum {
        DOMAIN_END_BIT = 0x80000000,
        IDX_MASK       = 0x7fffffff
    };

    void *mem;
    int use_len = 0, sample_count = 0, sample_pos[4] = {256, 256, 256, 256};
    uint32_t gen;
    int best_pos, collisions_min;
    int i, pos;

    if (n == 0) return 0;

    /*
     * mem: int32_t probes[128] | n*2 int32_t slots (sel. sampling pos-s)
     * mem: n*2 int32_t slots | bitmap              (collisions_found?)
     */
#define BITMAP_SIZE(n) \
    (sizeof(uint64_t) * ((n) + 63) / 64)

    size_t bitmap_size = BITMAP_SIZE(n * 2);
    size_t probes_size = 128 * sizeof(int32_t);
    mem = malloc((n * 2) * sizeof(int32_t) +
                 (bitmap_size > probes_size ? bitmap_size : probes_size));
    if (mem == NULL)
        return 0;
    uint32_t * const probes  = mem;
    uint32_t * const slots   = probes + 128;
    uint32_t *       indices = slots;

    /* semi-arbitrary limit, hard max is MAX_INT32 / 257
     * (larger size causes generation counter to wrap)
     * Note: it's highly unlikely we'll ever get a huge string set;
     *       if we do, it makes sense to have character *COLUMNS*
     *       in continuous memory (aka transpose) for improved memory
     *       access pattern, not implemented.
     * */
    if (n > 1000)
        return create_fnv_func(n, strings, random, size_random, mem);

    for (i = 0; i < n; i++)
        indices[i] = i;
    indices[n-1] = DOMAIN_END_BIT | (n - 1);

    memset(probes, 0, 128 * sizeof probes[0]);
pick_next_sample:
    gen = 1;
    collisions_min = n + 1;
    /* don't consider len again if already using it */
    for (pos = use_len - 1; pos < 256; pos++) {
        int collisions = 0;
        for (i = 0; i < n; i++) {
            uint32_t  idx = indices[i];
            const char *str = strings[idx & IDX_MASK];
            unsigned probe;

            if (pos == -1) {
                probe = 0x7f & strlen(str);
            } else {
                probe = str[pos];
                if (str[pos] == 0) goto save_best_pos;
            }

            if (probes[probe] == gen)
                collisions++;
            else
                probes[probe] = gen;

            gen += (idx >> 31); /* end of a collision domain? */
        }
        /* did we improve? */
        if (collisions < collisions_min) {
            collisions_min = collisions;
            best_pos = pos;
        }
    }

save_best_pos:
    /* save the best pos */
    if (best_pos == -1)
        use_len = 1;
    else
        sample_pos[sample_count++] = best_pos;

    if (collisions_min == 0) {
        uint32_t func;
        /* Found a solution, sort sample_pos[] first */
sort_sample_pos:
        if (sample_pos[0] > sample_pos[1]) {
            int temp = sample_pos[0];
            sample_pos[0] = sample_pos[1];
            sample_pos[1] = temp;
        }
        if (sample_pos[1] > sample_pos[2]) {
            int temp = sample_pos[1];
            sample_pos[1] = sample_pos[2];
            sample_pos[2] = temp;
            goto sort_sample_pos;
        }

        /* encode func */
        func = (sample_count << 24) |
               ((sample_pos[0] & 255) << 16) |
               ((sample_pos[1] & 255) << 8 ) |
               (sample_pos[2] & 255);

        if (use_len)
            func |= 0x04000000;

        /* check if we can get away with a simple func */
        if (collisions_found(func, n, strings, mem))
            func |= 0x08000000;

        free(mem);
        return func;
    }

    if (sample_count == 4) {
        /* too many samples, yet no solution */
        return create_fnv_func(n, strings, random, size_random, mem);
    }

    /* rebuild collision domains...
     * it starts here and spans till the function's end */
    uint32_t *next_indices = (indices == slots ? slots + n : slots);

    /* reuse probes for collision counters */
    memset(probes, 0, 128 * sizeof probes[0]); gen = 1;
    for (i = 0; i < n; ) {
        int j, end;
        uint64_t map, map_copy;
        if (indices[i] & DOMAIN_END_BIT) {
            /* 1-element collision domain */
            next_indices[i] = indices[i]; i++;
            continue;
        }
        /* estimate new collision domains' sizes;
         * (bit)map helps to avoid considering the entire probes[]
         * in subsequent steps */
        map = 0;
        for (j = i; ; j++) {
            const uint32_t idx = indices[j];
            const char * const str = strings[idx & IDX_MASK];
            unsigned probe = (best_pos == -1 ?
                              (0x7f & strlen(str)) : str[best_pos]);
            map |= (uint64_t)1 << (probe / 2);
            probes[probe]++;
            if (idx & DOMAIN_END_BIT) {
                /* the end of the original collision domain */
                if (probes[probe] != 1) indices[j] = idx & IDX_MASK;
                break;
            }
            /* New collision domain *begins*. We will be putting
             * elements in reverse order, so this element will come
             * at the end. Add domain-end marker now since that's
             * convenient. */
            if (probes[probe] == 1) indices[j] = DOMAIN_END_BIT | idx;
        }
        end = j + 1; j = i;
        /* assign output positions for new collision domains */
        map_copy = map;
        while (map_copy) {
            int pos = 2 * (unsigned)__builtin_ctzll(map_copy);
            i = probes[pos] += i;
            i = probes[pos+1] += i;
            map_copy &= map_copy - 1;
        }
        /* copy */
        for (; j != end; j++) {
            const uint32_t idx = indices[j];
            const char * const str = strings[idx & IDX_MASK];
            unsigned probe = (best_pos == -1 ?
                              (0x7f & strlen(str)) : str[best_pos]);
            next_indices[--probes[probe]] = idx;
        }
        /* zero out entries we touched */
        while (map) {
            int pos = 2 * (unsigned)__builtin_ctzll(map);
            probes[pos] = probes[pos+1] = 0;
            map &= map - 1;
        }
    }
    indices = next_indices;
    goto pick_next_sample;
}

static uint32_t create_fnv_func(int n, const unsigned char *strings[],
                                const unsigned char *random, size_t size_random,
                                void *mem)
{
    const unsigned char *last_random;
    uint32_t func = 0;
    if (size_random < sizeof(uint32_t)) goto done;
    for (last_random = random + size_random - sizeof(uint32_t);
         random <= last_random;
         random++) {

        uint32_t v;
        memcpy(&v, random, sizeof(v));
        v = net2host32(v);
        if (!collisions_found(v, n, strings, mem)) {
            /* technically, it may be not a FNV1A, yet no collisions */
            func = v;
            goto done;
        }
    }
done:
    free(mem);
    return func;
}

uint32_t
eval_hash_func(uint32_t func, const unsigned char *str, size_t len)
{
    int family = func >> 24, a, b, c;
    if (family > 0xf) {
        uint32_t prefix = host2net32(func);
        uint32_t seed = eval_fnv1a_func(0x811c9dc5,
                                        (const unsigned char *)&prefix,
                                        sizeof(prefix));
        return eval_fnv1a_func(seed, str, len);
    }

    a = 0xff & (func >> 16);
    b = 0xff & (func >> 8);
    c = 0xff & func;

    switch (family) {
    default:
        return 0;
    case 0x1:
        return str[a];
    case 0x2:
        return str[a] + str[b];
    case 0x3:
        return str[a] + str[b] + str[c];
    case 0x4:
        return len;
    case 0x5:
        return len + str[a];
    case 0x6:
        return len + str[a] + str[b];
    case 0x7:
        return len + str[a] + str[b] + str[c];
    case 0x9:
        return str[a];
    case 0xa:
        return (str[a] << 8) | str[b];
    case 0xb:
        return (str[a] << 16) | (str[b] << 8) | str[c];
    case 0xc:
        return len;
    case 0xd:
        return (len << 8) | str[a];
    case 0xe:
        return (len << 16) | (str[a] << 8) | str[b];
    case 0xf:
        return (len << 24) | (str[a] << 16) | (str[b] << 8) | str[c];
    }
}

uint32_t
eval_fnv1a_func(uint32_t seed, const unsigned char *str, size_t len)
{
    uint32_t res = seed;
    const unsigned char *i, *e;
    for (i = str, e = str + len; i < e; i++) {
        res ^= *i;
        res *= 0x1000193;
    }
    return res;
}

static int
collisions_found(uint32_t func, int n, const unsigned char *strings[],
                 void *mem)
{
    int i;
    unsigned bucket_count = 1;

    if (n < 2) return 0;

    /* bucket_count = 2 ** K, important! */
    while (bucket_count <= n)
        bucket_count *= 2;

    assert(bucket_count <= n * 2); /* mem has capacity for n * 2 buckets */

    uint32_t * const buckets = mem;
    uint64_t * const bitmap  = (void *)(buckets + bucket_count);
    memset(bitmap, 0, BITMAP_SIZE(bucket_count));

    for (i = 0; i < n; i++) {
        uint32_t hash = eval_hash_func(func, strings[i],
                                       (0xf4000000 & func) ?
                                       strlen(strings[i]) : 0);

        uint32_t j = 0;
        uint32_t perturb = hash;
        while (1) {
            /* that's how Python does it */
            j = j * 5 + 1 + perturb;
            perturb >>= 5;

            unsigned index = j & (bucket_count - 1);
            uint64_t mask = (uint64_t)1 << (index & 63);
            if (bitmap[index / 64] & mask) {
                /* bucket used; maybe a collision */
                if (buckets[index] == hash) return 1;
            } else {
                /* mark bucket as used and store hash */
                bitmap[index / 64] |= mask;
                buckets[index] = hash;
                break;
            }
        }
    }
    return 0;
}
