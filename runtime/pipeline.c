#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdio.h>

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

    CDummyValue      = 17, /* skipped */
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

struct State {
    size_t             t_capacity;   // capacity of t/v   bufs (items)
    size_t             ot_capacity;  // capacity of ot/ov bufs (items)
    size_t             res_capacity; // capacity of res   buf
    size_t             res_size;
    uint8_t           *res;      // filled by unparse_msgpack, others
    const uint8_t     *b1;       // bank1: input data
    const uint8_t     *b2;       // bank2: program constants
    uint8_t           *t;        // filled by parse_msgpack
    struct Value      *v;        // .......................
    uint8_t           *ot;       // consumed by unparse_msgpack
    struct Value      *ov;       // ...........................
};

#if !(C_HAVE_BSWAP16)
static inline uint16_t __builtin_bswap16(uint16_t a)
{
    return (a << 8) | ( a >> 8);
}
#endif

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

static inline size_t next_capacity(size_t min_capacity)
{
    size_t capacity = 128;
    while (capacity < min_capacity)
        capacity = capacity + capacity / 2;
    return capacity;
}

static int buf_grow(uint8_t **t,
                    size_t *capacity,
                    size_t new_capacity)
{
    uint8_t      *new_t;

    new_t = realloc(*t, new_capacity * sizeof(new_t[0]));
    if (new_t == NULL)
        return -1;

    *t = new_t;
    *capacity = new_capacity;
    return 0;
}

static int buf_grow_tv(uint8_t **t,
                       struct Value **v,
                       size_t *capacity,
                       size_t new_capacity)
{
    struct Value *new_v;

    new_v = realloc(*v, new_capacity * sizeof(new_v[0]));
    if (new_v == NULL)
        return -1;
    *v = new_v;

    return buf_grow(t, capacity, new_capacity);
}

static int set_error(struct State *state,
                     const char *msg)
{
    size_t len = strlen(msg);
    if (state->res_capacity < len &&
        buf_grow(&state->res, &state->res_capacity, next_capacity(len)) != 0) {

        state->res_size = 0;
        return -1;
    }
    state->res_size = len;
    memcpy(state->res, msg, len);
    return -1; /* always returns -1, see invocation */
}

int parse_msgpack(struct State *state,
                  const uint8_t * restrict mi,
                  size_t        ms)
{
    const uint8_t *me = mi + ms;
    uint8_t       * restrict typeid;
    struct Value  * restrict value, *value_max, *value_buf;
    uint32_t       todo = 1, patch = -1;
    uint32_t      * restrict stack, *stack_max, *stack_buf;
    uint32_t       len;

#if 0
    /* Debug  */
    fprintf(stderr, "parse_msgpack; s: ");
    for (int i = 0; i < ms; ++i)
        fprintf(stderr, "%02X ", mi[i]);
    fprintf(stderr, "\b\n");
#endif

    /* Initialising ptrs with NULL-s is correct, but that would
     * harm branch prediction accuracy. Not checking the buf capacity,
     * because that would hurt performance (there's enough capacity,
     * except for the very first call). */
    typeid    = state->t;
    value     = state->v;
    value_max = state->v + state->t_capacity;
    value_buf = state->v;
    /* reusing ov for the stack */
    stack     = (void *)(state->ov);
    stack_max = (void *)(state->ov + state->ot_capacity);
    stack_buf = (void *)(state->ov);

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
    if (__builtin_expect(value == value_max, 0)) {

        size_t old_capacity = state->t_capacity;

        if (buf_grow_tv(&state->t, &state->v, &state->t_capacity,
                        next_capacity(old_capacity + 1)) != 0)
            goto error_alloc;

        typeid    = state->t + old_capacity;
        value     = state->v + old_capacity;
        value_max = state->v + state->t_capacity;
        value_buf = state->v;
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

            size_t old_capacity = state->ot_capacity;

            if (buf_grow_tv(&state->ot, &state->ov, &state->ot_capacity,
                            next_capacity(old_capacity + 1)) != 0)
                goto error_alloc;

            /* reusing ov for the stack */
            stack     = (void *)(state->ov + old_capacity);
            stack_max = (void *)(state->ov + state->ot_capacity);
            stack_buf = (void *)(state->ov);
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
    state->res_size = value - state->v;
    state->b1 = me;
    return 0;

error_underflow:
    return set_error(state, "Truncated data");
error_c1:
    return set_error(state, "Invalid data");
error_alloc:
    return set_error(state, "Out of memory");
}

int unparse_msgpack(struct State *state,
                    size_t        nitems)
{
    //nitems--;
    const uint8_t      * restrict typeid = state->ot - 1;
    const struct Value * restrict value = state->ov - 1;
    const uint8_t      * restrict bank1 = state->b1;
    const uint8_t      * restrict bank2 = state->b2;
    const uint8_t      * typeid_max = state->ot + nitems;
    uint8_t            * restrict out, *out_max;
    const uint8_t      * restrict copy_from = bank1;

    out = state->res;
    out_max = state->res + state->res_capacity;

    const uint8_t * typeid2 = typeid;
    const struct Value * value2 = value;
#if 0
    /* Debug  */
    for (; typeid2 != typeid_max; typeid2++, value2++) {
        const uint8_t *cmdname =
            (*typeid2 == 18) ? "PUTSTRC" :
            (*typeid2 == 12) ? "PUTMAP" :
            (*typeid2 == 11) ? "PUTARRAYC" :
            (*typeid2 == 4) ? "PUTINT / PUTLONG" :
            (*typeid2 == 8) ? "PUTSTR" :
            (*typeid2 == 0) ? "(zero)" : "unknown";
        fprintf(stderr, "unparse_msgpack; *typeid: 0x%02X (%d) -- %s; value: %d\n", *typeid2, *typeid2, cmdname, *value2);
    }
#endif
    int i = 0;

    goto check_buf;

    for (; typeid != typeid_max; typeid++, value++) {
        /* precondition: at least 10 bytes avail in out */

#if 0
	    /* Debug  */
	const uint8_t *cmdname =
            (*typeid == 18) ? "PUTSTRC" :
            (*typeid == 12) ? "PUTMAP" :
            (*typeid == 11) ? "PUTARRAYC" :
            (*typeid == 4) ? "PUTINT / PUTLONG" :
            (*typeid == 8) ? "PUTSTR" :
            (*typeid == 0) ? "(zero)" : "unknown";
        fprintf(stderr, "%02d; unparse_msgpack; in for; *typeid: 0x%02X (%d) -- %s; value: %d\n", i, *typeid, *typeid, cmdname, *value);
#endif
        ++i;

        switch (*typeid) {
        default:
            goto error_badcode;
        case CDummyValue:
            continue;
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
                out[0] = 0xda;
                unaligned(out+1)->u16 = host2net16((uint16_t)value->xlen);
                out += 3;
                goto copy_data;
            }
            out[0] = 0xdb;
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
        if (__builtin_expect(out + 10 > out_max, 0)) {
            uint8_t *old_res = state->res;
            if (buf_grow(&state->res, &state->res_capacity,
                         next_capacity(state->res_capacity + 10)) != 0)
                goto error_alloc;
            out = state->res + (out - old_res);
            out_max = state->res + state->res_capacity;
        }
        continue;

copy_data:
        /*
         * Ensure we have a room fom value->xlen bytes in out_buf, plus
         * 10 more bytes for the next iteration.
         * Some switch branches end up jumping here.
         */
        if (__builtin_expect(out + value->xlen + 10 > out_max, 0)) {
            uint8_t *old_res = state->res;
            size_t old_capacity = state->res_capacity;
            if (buf_grow(&state->res, &state->res_capacity,
                         next_capacity(old_capacity + value->xlen + 10)) != 0)
                goto error_alloc;
            out = state->res + (out - old_res);
            out_max = state->res + state->res_capacity;
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

    state->res_size = out - state->res;
    return 0;

error_alloc:
    return set_error(state, "Out of memory");
error_badcode:
#if 0
    {
	    char *c = malloc(1024);
	    sprintf(c, "Internal error: unknown code (%x)", *typeid);
	    return set_error(state, c);
    }
#endif
    return set_error(state, "Internal error: unknown code");
}

int schema_rt_buf_grow(struct State *state,
                       size_t min_capacity)
{
    if (min_capacity <= state->ot_capacity)
        return 0;
    return buf_grow_tv(&state->ot, &state->ov, &state->ot_capacity,
                       next_capacity(min_capacity));
}

/*
 * Render location info in res buf.
 * *Pos* is the posiotion of offending element.
 * Ex: "Foo/Bar/32: "
 *
 * @returns 1 if the position is a map key, 0 otherwise.
 */
int schema_rt_extract_location(struct State *state,
                               intptr_t pos)
{
    intptr_t i = 1;
    int      ismap, need_sep = 0;
    uint32_t counter = 1;

    state->res_size = 0;
    if (pos == 0) return 0; /* the very root element */

    ismap = state->t[0] == MapValue;
    while (1) {
        char         buf[16]; /* PRId32 */
        const  void *item;
        size_t       item_size;
        int          type = state->t[i];
        intptr_t     next = i + (type == ArrayValue || type == MapValue ?
                                 state->v[i].xoff : 1);

        if (next <= pos) { /* skip it */
            i = next; counter++; continue;
        }
        if (need_sep) { /* invariant: there's space for sep in buf */
            state->res[state->res_size] = '/';
            state->res_size++;
        }
        if (ismap) {
            if ((counter & 1) || state->t[i-1] != StringValue) {
                /* the issue was with a key */
                if (need_sep) { /* overwrite /, hence -1 */
                    memcpy(state->res + state->res_size - 1, ": ", 2);
                    state->res_size++;
                }
                return 1;
            }
            item = state->b1 - state->v[i-1].xoff;
            item_size = state->v[i-1].xlen;
        } else {
            item = buf;
            item_size = sprintf(buf, "%"PRIu32, counter);
        }
        /* maintain invariant: there's space for sep in buf */
        if (state->res_capacity < state->res_size + item_size + 2 &&
            buf_grow(&state->res, &state->res_capacity,
                     next_capacity(state->res_size + item_size + 2)) != 0) {

            /* allocation failure (unlikely); discard incomplete message */
            state->res_size = 0;
            return 0;
        }
        memcpy(state->res + state->res_size, item, item_size);
        state->res_size += item_size;
        if (i == pos) {
            memcpy(state->res + state->res_size, ": ", 2);
            state->res_size += 2;
            return 0;
        }
        /* descent into map or array */
        need_sep = 1;
        i++;
        counter = 1;
        ismap = type == MapValue;
    }
}

void schema_rt_xflatten_done(struct State *state,
                             size_t len)
{
    uint32_t array_len = 0, countdown = 1;
    size_t i;
    for (i = 1; i < len; i++) {
        switch (state->ot[i]) {
        case ArrayValue:
            countdown += state->ov[i].xlen;
            break;
        case MapValue:
            countdown += state->ov[i].xlen * 2;
            break;
        case CDummyValue:
            continue;
        }
        if (--countdown == 0) {
            array_len ++;
            countdown = 1;
        }
    }
    state->ot[0] = ArrayValue;
    state->ov[0].xlen = array_len;
}
