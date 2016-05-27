#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
