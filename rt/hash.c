#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

uint32_t
eval_fnv1a_func(uint32_t seed, const char *str, size_t len);

static int
collisions_found(uint32_t func, int n, const char *strings[],
                 void *mem);

static uint32_t
create_fnv_func(int n, const char *strings[],
                const char *random, size_t size_random,
                void *mem);

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
uint32_t create_hash_func(int n, const char *strings[],
                          const char *random, size_t size_random)
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
    int n_active, i, pos, o, max_len = 256;

    if (n == 0) return 0;

    /*
     * mem: int32_t probes[128] | int32_t slots[n*2] (sel. sampling pos-s)
     * mem:  int32_t slots[n*2] | bitmap             (collisions_found?)
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
    n_active = n;
pick_next_sample:
    gen = 1;
    collisions_min = n_active + 1; best_pos = 0;
    /* don't consider len again if already using it */
    for (pos = use_len - 1; pos < max_len; pos++) {
        int collisions = 0;
        for (i = 0; i < n_active; i++) {
            uint32_t  idx = indices[i];
            const char *str = strings[idx & IDX_MASK];
            unsigned probe;

            if (pos == -1) {
                probe = 0x7f & strlen(str);
            } else {
                probe = str[pos];
                if (str[pos] == 0) {
                    /* we may drop the string when splitting domains */
                    max_len = pos;
                    goto save_best_pos;
                }
            }

            if (probes[probe] == gen)
                collisions++;
            else
                probes[probe] = gen;

            /* end of a collision domain? */
            gen += (idx >> __builtin_ctzl(DOMAIN_END_BIT));
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
    uint32_t *next_indices = (indices == slots ? slots + n_active : slots);

    /* reuse probes for collision counters */
    memset(probes, 0, 128 * sizeof probes[0]);
    o = 0;
    for (i = 0; i < n_active; ) {
        int j, end;
        uint64_t map, map_copy;
        /* estimate new collision domains' sizes;
         * (bit)map helps to avoid considering the entire probes[]
         * in subsequent steps */
        map = 0;
        for (j = i; ; j++) {
            const uint32_t idx = indices[j];
            const char * const str = strings[idx & IDX_MASK];
            unsigned probe = (best_pos == -1 ?
                              (0x7f & strlen(str)) : (unsigned)str[best_pos]);
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
        end = j + 1;
        /* assign output positions for new collision domains;
         * drop 1-element collision domains */
        map_copy = map;
        while (map_copy) {
            int pos = 2 * (unsigned)__builtin_ctzll(map_copy);
            probes[pos+0] =
                (probes[pos+0] > 1 ? (o += probes[pos+0]) : n_active);
            probes[pos+1] =
                (probes[pos+1] > 1 ? (o += probes[pos+1]) : n_active);
            map_copy &= map_copy - 1;
        }
        /* copy */
        for (j = i; j != end; j++) {
            const uint32_t idx = indices[j];
            const char * const str = strings[idx & IDX_MASK];
            unsigned probe = (best_pos == -1 ?
                              (0x7f & strlen(str)) : (unsigned)str[best_pos]);
            next_indices[--probes[probe]] = idx;
        }
        i = end;
        /* zero out entries we touched */
        while (map) {
            int pos = 2 * (unsigned)__builtin_ctzll(map);
            probes[pos] = probes[pos+1] = 0;
            map &= map - 1;
        }
    }
    indices = next_indices;
    n_active = o;
    goto pick_next_sample;
}

static uint32_t create_fnv_func(int n, const char *strings[],
                                const char *random, size_t size_random,
                                void *mem)
{
    const char *last_random;
    uint32_t func = 0;
    if (size_random < sizeof(uint32_t)) goto done;
    for (last_random = random + size_random - sizeof(uint32_t);
         random <= last_random;
         random++) {

        uint32_t v;
        memcpy(&v, random, sizeof(v));
        if (v > 0xf000000 && !collisions_found(v, n, strings, mem)) {
            func = v;
            goto done;
        }
    }
done:
    free(mem);
    return func;
}

uint32_t
eval_hash_func(uint32_t func, const char *str, size_t len)
{
    int family = func >> 24, a, b, c;
    if (family > 0xf) {
        uint32_t prefix = func;
        uint32_t seed = eval_fnv1a_func(0x811c9dc5,
                                        (const char *)&prefix,
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
eval_fnv1a_func(uint32_t seed, const char *str, size_t len)
{
    uint32_t res = seed;
    const unsigned char *i, *e;
    for (i = (const unsigned char *)str, e = i + len; i < e; i++) {
        res ^= *i;
        res *= 0x1000193;
    }
    return res;
}

static int
collisions_found(uint32_t func, int n, const char *strings[],
                 void *mem)
{
    int i;
    unsigned bucket_count = 1;

    if (n < 2) return 0;

    /* bucket_count = 2 ** K, important! */
    while ((int)bucket_count <= n)
        bucket_count *= 2;

    assert((int)bucket_count <= n * 2); /* mem has capacity for n * 2 buckets */

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
