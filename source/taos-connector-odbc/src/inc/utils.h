/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _utils_h_
#define _utils_h_

#include "macros.h"

#include "iconv_wrapper.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

EXTERN_C_BEGIN

#define TOD_SAFE_FREE(_p) if (_p) { free(_p); _p = NULL; }

typedef struct string_s                    string_t;
struct string_s {
  const char           *charset; // NOTE: no ownership
  const char           *str;     // NOTE: no ownership
  size_t                bytes;
};

typedef struct fixed_buf_s           fixed_buf_t;
struct fixed_buf_s {
  char              *buf;
  size_t             cap;
  size_t             nr;
};

#define fixed_buf_sprintf(_n, _buf, _fmt, ...) do {       \
  fixed_buf_t *_fixed_buf = _buf;                         \
  char *__buf = _fixed_buf->buf;                          \
  size_t _nn = 0;                                         \
  if (__buf) {                                            \
    __buf += _fixed_buf->nr;                              \
    _nn = _fixed_buf->cap - _fixed_buf->nr;               \
  }                                                       \
  _n = snprintf(__buf, _nn, _fmt, ##__VA_ARGS__);         \
  if (__buf) {                                            \
    if (_n>0 && (size_t)_n < _nn) {                       \
      _fixed_buf->nr += _n;                               \
    } else {                                              \
      _fixed_buf->nr += _nn - 1;                          \
    }                                                     \
  }                                                       \
} while (0)

typedef struct static_pool_s                   static_pool_t;

static_pool_t* static_pool_create(size_t cap) FA_HIDDEN;
void static_pool_destroy(static_pool_t *pool) FA_HIDDEN;
void static_pool_reset(static_pool_t *pool) FA_HIDDEN;
unsigned char* static_pool_malloc(static_pool_t *pool, size_t sz) FA_HIDDEN;
unsigned char* static_pool_calloc(static_pool_t *pool, size_t sz) FA_HIDDEN;
unsigned char* static_pool_malloc_align(static_pool_t *pool, size_t sz, size_t align) FA_HIDDEN;
unsigned char* static_pool_calloc_align(static_pool_t *pool, size_t sz, size_t align) FA_HIDDEN;

typedef struct mem_s               mem_t;

struct mem_s {
  unsigned char        *base;
  size_t                cap;
  size_t                nr;
};

void mem_release(mem_t *mem) FA_HIDDEN;
void mem_reset(mem_t *mem) FA_HIDDEN;
void mem_memset(mem_t *mem, int c) FA_HIDDEN;
int mem_expand(mem_t *mem, size_t delta) FA_HIDDEN;
int mem_keep(mem_t *mem, size_t cap) FA_HIDDEN;
int mem_conv(mem_t *mem, iconv_t cnv, const char *src, size_t len) FA_HIDDEN;
int mem_conv_ex(mem_t *mem, const string_t *src, const char *dst_charset) FA_HIDDEN;
int mem_iconv(mem_t *mem, const char *fromcode, const char *tocode, const char *src, size_t len) FA_HIDDEN;
int mem_copy(mem_t *mem, const char *src) FA_HIDDEN;
int mem_copy_bin(mem_t *mem, const unsigned char *src, size_t len) FA_HIDDEN;

int iconv_calc(iconv_t cnv, const char *in, size_t len, size_t *outbytes) FA_HIDDEN;

typedef struct buf_s               buf_t;
struct buf_s {
  char               *base;
  size_t              cap;
};

void buf_release(buf_t *buf) FA_HIDDEN;
void* buf_realloc(buf_t *buf, size_t sz) FA_HIDDEN;

typedef struct buffers_s           buffers_t;
struct buffers_s {
  buf_t             **bufs;
  size_t              cap;
};

void buffers_release(buffers_t *buffers) FA_HIDDEN;
void* buffers_realloc(buffers_t *buffers, size_t idx, size_t sz) FA_HIDDEN;

typedef struct wildex_s           wildex_t;
int wildcomp(wildex_t **pwild, const string_t *wildex) FA_HIDDEN;
int wildexec(wildex_t *wild, const string_t *str, int *matched) FA_HIDDEN;

void wildfree(wildex_t *wild) FA_HIDDEN;

#define WILD_SAFE_FREE(wild)                 if (wild) { wildfree(wild); wild = NULL; }

int table_type_parse(const char *table_type, int *table, int *stable) FA_HIDDEN;

typedef struct buffer_s           buffer_t;
struct buffer_s {
  char        *base;
  size_t       sz;
  size_t       nr;
};

void buffer_reset(buffer_t *str) FA_HIDDEN;
void buffer_release(buffer_t *str) FA_HIDDEN;
int buffer_expand(buffer_t *str, size_t nr) FA_HIDDEN;

int buffer_concat_n(buffer_t *str, const char *s, size_t len) FA_HIDDEN;
static inline int buffer_concat(buffer_t *str, const char *s)
{
  return buffer_concat_n(str, s, strlen(s));
}
int buffer_vconcat(buffer_t *str, const char *fmt, va_list ap) FA_HIDDEN;
#ifdef _WIN32
int buffer_concat_fmt_x(buffer_t *str, const char *fmt, ...) FA_HIDDEN;
// NOTE: make MSVC to do fmt-string-checking with printf during compile-time
#define buffer_concat_fmt(str, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : buffer_concat_fmt_x(str, fmt, ##__VA_ARGS__))
#else
int buffer_concat_fmt_x(buffer_t *str, const char *fmt, ...) __attribute__ ((format (printf, 2, 3))) FA_HIDDEN;
#define buffer_concat_fmt buffer_concat_fmt_x
#endif
// replace "'" in s with "''"
int buffer_concat_replacement_n(buffer_t *str, const char *s, size_t len) FA_HIDDEN;
static inline int buffer_concat_replacement(buffer_t *str, const char *s)
{
  return buffer_concat_replacement_n(str, s, strlen(s));
}

int buffer_copy_n(buffer_t *str, const unsigned char *mem, size_t len) FA_HIDDEN;
static inline int buffer_copy(buffer_t *str, const char *s)
{
  str->nr = 0;
  return buffer_copy_n(str, (const unsigned char*)s, strlen(s));
}


void trim_left(const char *src, size_t nr, const char **first) FA_HIDDEN;
void trim_right(const char *src, size_t nr, const char **last) FA_HIDDEN;
void trim_string(const char *src, size_t nr, const char **start, const char **end) FA_HIDDEN;
void trim_spaces(const char *src, size_t len, char *dst, size_t n) FA_HIDDEN;
void get_kv(const char *kv, char *k, size_t kn, char *v, size_t vn) FA_HIDDEN;

typedef struct kv_s                      kv_t;
struct kv_s {
  char            *key;
  char            *val;
};
void kv_release(kv_t *kv);
int kv_set(kv_t *kv, const char *k, size_t kn, const char *v, size_t vn) FA_HIDDEN;

typedef struct kvs_s                     kvs_t;
struct kvs_s {
  kv_t                  *kvs;
  size_t                 cap;
  size_t                 nr;
};
void kvs_reset(kvs_t *kvs);
void kvs_release(kvs_t *kvs);
void kvs_transfer(kvs_t *from, kvs_t *to);
int kvs_append(kvs_t *kvs, const char *k, size_t kn, const char *v, size_t vn) FA_HIDDEN;


// 1-based row/col
void locate_str(const char *s, int row0, int col0, int row1, int col1, const char **start, const char **end) FA_HIDDEN;

typedef struct hash_table_s             hash_table_t;
hash_table_t* hash_table_new(void (*release_cb)(hash_table_t *hash_table, void *val, void *arg), void *arg) FA_HIDDEN;
void hash_table_release(hash_table_t *hash_table) FA_HIDDEN;
int hash_table_set(hash_table_t *hash_table, const char *key, void *val) FA_HIDDEN;
void hash_table_get(hash_table_t *hash_table, const char *key, void **val) FA_HIDDEN;


typedef struct str_s                    str_t;
typedef struct strs_s                   strs_t;

struct str_s {
  char               *str;
  size_t              cap; // NOTE: null-terminator-exclusive
  size_t              nr;
};

struct strs_s {
  str_t              *strs;
  size_t              cap;
  size_t              nr;
};

void str_reset(str_t *str) FA_HIDDEN;
void str_release(str_t *str) FA_HIDDEN;
int str_keep(str_t *str, size_t cap) FA_HIDDEN;
int str_append(str_t *str, const char *s, size_t n) FA_HIDDEN;

void strs_reset(strs_t *strs) FA_HIDDEN;
void strs_release(strs_t *strs) FA_HIDDEN;
int strs_keep(strs_t *strs, size_t cap) FA_HIDDEN;
int strs_flat(strs_t *strs, str_t *str) FA_HIDDEN;


EXTERN_C_END

#endif // _utils_h_

