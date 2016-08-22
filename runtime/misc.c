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

__asm__(
    "\t.text\n"
#if __APPLE
    "\t.globl _schema_rt_setjmp\n"
    "_schema_rt_setjmp:\n"
#else
    "\t.globl schema_rt_setjmp\n"
    "schema_rt_setjmp:\n"
#endif
    "\tmovq (%rsp), %rax\n"
    "\tmovq %r12,  0(%rdi)\n"
    "\tmovq %r13,  8(%rdi)\n"
    "\tmovq %r14, 16(%rdi)\n"
    "\tmovq %r15, 24(%rdi)\n"
    "\tmovq %rbx, 32(%rdi)\n"
    "\tmovq %rsp, 40(%rdi)\n"
    "\tmovq %rbp, 48(%rdi)\n"
    "\tmovq %rax, 56(%rdi)\n"
    "\txor  %eax, %eax\n"
    "\tret\n"

#if __APPLE
    "\t.globl _schema_rt_longjmp\n"
    "_schema_rt_longjmp:\n"
#else
    "\t.globl schema_rt_longjmp\n"
    "schema_rt_longjmp:\n"
#endif
    "\tmovq  0(%rdi), %r12\n"
    "\tmovq  8(%rdi), %r13\n"
    "\tmovq 16(%rdi), %r14\n"
    "\tmovq 24(%rdi), %r15\n"
    "\tmovq 56(%rdi), %rdx\n"
    "\tmovq 32(%rdi), %rbx\n"
    "\tmovq 40(%rdi), %rsp\n"
    "\tmovq 48(%rdi), %rbp\n"
    "\tmovl $1, %eax\n"
    "\taddq $8, %rsp\n"
    "\tjmpq *%rdx\n"
);
