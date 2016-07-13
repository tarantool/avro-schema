#include <stdint.h>
#include <string.h>

int
schema_rt_key_eq(const char *key, const char *str, size_t klen, size_t len)
{
    return klen == 0 || klen != len ? -1 : memcmp(key, str, klen);
}

uint32_t
schema_rt_search8(const uint8_t *tab, uint32_t k, size_t n)
#define SCHEMA_RT_SEARCH_BODY \
    uint32_t i = 0; \
    while (i != n - 1 && tab[i] != k) i++; \
    return i;
{ SCHEMA_RT_SEARCH_BODY }

uint32_t
schema_rt_search16(const uint16_t *tab, uint32_t k, size_t n)
{ SCHEMA_RT_SEARCH_BODY }

uint32_t
schema_rt_search32(const uint32_t *tab, uint32_t k, size_t n)
{ SCHEMA_RT_SEARCH_BODY }

