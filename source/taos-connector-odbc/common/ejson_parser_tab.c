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

#include "ejson_parser.h"

#include "helpers.h"
#include "logger.h"

#include <errno.h>
#include <math.h>
#include <string.h>

typedef struct _ejson_str_s             _ejson_str_t;
typedef struct _ejson_kv_s              _ejson_kv_t;
typedef struct _ejson_bin_s             _ejson_bin_t;
typedef struct _ejson_internal_s        _ejson_internal_t;

struct _ejson_str_s {
  char                      *str;
  size_t                     cap;
  size_t                     nr;

  parser_loc_t               loc;
};

struct _ejson_bin_s {
  unsigned char             *bin;
  size_t                     cap;
  size_t                     nr;

  parser_loc_t               loc;
};

struct _ejson_kv_s {
  _ejson_str_t               key;
  ejson_t                   *val;

  parser_loc_t               loc;
};

typedef struct ejson_parser_token_s             ejson_parser_token_t;
struct ejson_parser_token_s {
  const char      *text;
  size_t           leng;
};

struct _ejson_internal_s {
  char            *buf;
  size_t           nr;
  size_t           cap;
};

static void _ejson_internal_reset(_ejson_internal_t *internal)
{
  if (!internal) return;
  internal->nr = 0;
  if (internal->buf) internal->buf[0] = '\0';
}

static void _ejson_internal_release(_ejson_internal_t *internal)
{
  if (!internal) return;
  _ejson_internal_reset(internal);
  if (internal->buf) {
    free(internal->buf);
    internal->buf = NULL;
  }
  internal->cap = 0;
}

void ejson_parser_param_release(ejson_parser_param_t *param)
{
  if (!param) return;
  param->ctx.err_msg[0] = '\0';
  param->ctx.bad_token.first_line = 0;
  if (param->ejson) {
    ejson_dec_ref(param->ejson);
    param->ejson = NULL;
  }
  if (param->internal) {
    _ejson_internal_t *internal = (_ejson_internal_t*)param->internal;
    _ejson_internal_release(internal);
    free(param->internal);
    param->internal = NULL;
  }
}

static void _ejson_str_reset(_ejson_str_t *str)
{
  if (!str) return;
  if (str->str) str->str[0] = '\0';
  str->nr = 0;
}

static void _ejson_bin_reset(_ejson_bin_t *bin)
{
  if (!bin) return;
  if (bin->bin) bin->bin[0] = '\0';
  bin->nr = 0;
}

static void _ejson_str_release(_ejson_str_t *str)
{
  if (!str) return;
  _ejson_str_reset(str);
  if (str->str) {
    free(str->str);
    str->str = NULL;
  }
  str->cap = 0;
}

static void _ejson_bin_release(_ejson_bin_t *bin)
{
  if (!bin) return;
  _ejson_bin_reset(bin);
  if (bin->bin) {
    free(bin->bin);
    bin->bin = NULL;
  }
  bin->cap = 0;
}

static int _ejson_str_append(_ejson_str_t *str, const char *v, size_t n)
{
  size_t len = strnlen(v, n);
  if (len == 0) return 0;
  size_t cap = str->nr + len;
  if (cap > str->cap) {
    cap = (cap + 15) / 16 * 16;
    char *p = (char*)realloc(str->str, cap + 1);
    if (!p) return -1;
    str->str = p;
    str->cap = cap;
  }
  strncpy(str->str + str->nr, v, len);
  str->nr += len;
  str->str[str->nr] = '\0';
  return 0;
}

static int _ejson_bin_append(_ejson_bin_t *bin, const unsigned char *v, size_t n)
{
  if (n == 0) return 0;
  size_t cap = bin->nr + n;
  if (cap > bin->cap) {
    cap = (cap + 15) / 16 * 16;
    unsigned char *p = (unsigned char*)realloc(bin->bin, cap + 1);
    if (!p) return -1;
    bin->bin = p;
    bin->cap = cap;
  }
  memcpy(bin->bin + bin->nr, v, n);
  bin->nr += n;
  bin->bin[bin->nr] = '\0'; // NOTE: this is just for safe
  return 0;
}

static int _ejson_bin_append_hex(_ejson_bin_t *bin, const char *v, size_t n)
{
  if (n == 0) return 0;
  if (n & 1) {
    errno = EINVAL;
    return -1;
  }
  while (n > 0) {
    unsigned char buf[1024]; *buf = '\0';
    size_t        hn = sizeof(buf) * 2;
    if (hn > n) hn = n;
    tod_hex2bytes_unsafe(v, hn, buf);
    int r = _ejson_bin_append(bin, buf, hn / 2);
    if (r) return -1;

    v += hn;
    n -= hn;
  }

  return 0;
}

static int _ejson_bin_append_str(_ejson_bin_t *bin, const char *v, size_t n)
{
  if (n == 0) return 0;
  size_t cap = bin->nr + n;
  if (cap > bin->cap) {
    cap = (cap + 15) / 16 * 16;
    unsigned char *p = (unsigned char*)realloc(bin->bin, cap + 1);
    if (!p) return -1;
    bin->bin = p;
    bin->cap = cap;
  }
  memcpy(bin->bin + bin->nr, v, n);
  bin->nr += n;
  bin->bin[bin->nr] = '\0'; // NOTE: this is just for safe
  return 0;
}

static int _ejson_str_append_uni(_ejson_str_t *str, const char *v, size_t n)
{
  if (1) return _ejson_str_append(str, v, n);
  size_t len = strnlen(v, n);
  if (len == 0) return 0;
  size_t cap = str->nr + len;
  if (cap > str->cap) {
    cap = (cap + 15) / 16 * 16;
    char *p = (char*)realloc(str->str, cap + 1);
    if (!p) return -1;
    str->str = p;
    str->cap = cap;
  }
  strncpy(str->str + str->nr, v, len);
  str->nr += len;
  str->str[str->nr] = '\0';
  return 0;
}

static void _ejson_str_init(_ejson_str_t *str)
{
  memset(str, 0, sizeof(*str));
}

static void _ejson_bin_init(_ejson_bin_t *bin)
{
  memset(bin, 0, sizeof(*bin));
}

static void _ejson_kv_release(_ejson_kv_t *kv)
{
  if (!kv) return;
  _ejson_str_release(&kv->key);
  if (kv->val) {
    ejson_dec_ref(kv->val);
    kv->val = NULL;
  }
}


static ejson_t* _ejson_new_num(const char *s, size_t n);
static ejson_t* _ejson_new_str(_ejson_str_t *str);
static ejson_t* _ejson_new_bin(_ejson_bin_t *bin);
static ejson_t* _ejson_new_obj(_ejson_kv_t *kv);
static int _ejson_obj_set(ejson_t *ejson, _ejson_kv_t *kv);

// static int ejson_str_append(ejson_t *ejson, const char *v, size_t n);
// static int ejson_obj_set(ejson_t *ejson, const char *k, size_t n, ejson_t *v);

typedef struct ejson_num_s              ejson_num_t;
typedef struct ejson_obj_s              ejson_obj_t;
typedef struct ejson_arr_s              ejson_arr_t;


struct ejson_num_s {
  double                     dbl;
};

struct ejson_obj_s {
  _ejson_kv_t               *kvs;
  size_t                     cap;
  size_t                     nr;
};

struct ejson_arr_s {
  ejson_t                 **vals;
  size_t                    cap;
  size_t                    nr;
};

struct ejson_s {
  ejson_type_t               type;
  union {
    ejson_num_t              _num;
    _ejson_str_t             _str;
    ejson_obj_t              _obj;
    ejson_arr_t              _arr;
    _ejson_bin_t             _bin;
  };

  parser_loc_t               loc;
  int                        refc;
};

// utf-16be: 4/8-hexdigits
// cnv:      from `ejson_parser_iconv_open()`
// eg.:      "4eba" -> "人"
static int _ejson_parser_iconv_char_unsafe(const char *utf16be,
    char *utf8, size_t nr, iconv_t cnv)
{
  char buf[64]; *buf = '\0';
  size_t len = strlen(utf16be);

  tod_hex2bytes_unsafe(utf16be, len, (unsigned char*)buf);

  char        *inbuf            = buf;
  size_t       inbytesleft      = len / 2;
  char        *outbuf           = utf8;
  size_t       outbytesleft     = nr;

  size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  *outbuf = '\0';
  if (inbytesleft || n) {
    if (errno == 0) errno = EINVAL;
    return -1;
  }
  return 0;
}

#include "ejson_parser.tab.h"
#include "ejson_parser.lex.c"

#include "ejson_parser.lex.h"
#undef yylloc
#undef yylval
#include "ejson_parser.tab.c"


ejson_t* ejson_new_null(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_NULL;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_true(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_TRUE;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_false(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_FALSE;
  ejson->refc = 1;
  return ejson;
}

static void _ejson_num_release(ejson_num_t *num)
{
  if (!num) return;
}

static void _ejson_obj_reset(ejson_obj_t *obj)
{
  if (!obj) return;
  for (size_t i=0; i<obj->nr; ++i) {
    _ejson_kv_t *kv = obj->kvs + i;
    _ejson_kv_release(kv);
  }
  obj->nr = 0;
}

static void _ejson_obj_release(ejson_obj_t *obj)
{
  if (!obj) return;
  _ejson_obj_reset(obj);
  if (obj->kvs) {
    free(obj->kvs);
    obj->kvs = NULL;
  }
  obj->cap = 0;
}

static void _ejson_inc_ref(ejson_t *ejson)
{
  if (!ejson) return;

  ejson->refc += 1;
}

static int _ejson_obj_keep(ejson_obj_t *obj, size_t cap)
{
  if (cap >= obj->cap) {
    cap = (cap + 15) / 16 * 16;
    _ejson_kv_t *kvs = (_ejson_kv_t*)realloc(obj->kvs, cap * sizeof(*kvs));
    if (!kvs) return -1;
    obj->kvs = kvs;
    obj->cap = cap;
  }
  return 0;
}

// static int _ejson_kv_set(_ejson_kv_t *kv, const char *k, size_t n, ejson_t *v)
// {
//   size_t len = strnlen(k, n);
//   int r = _ejson_str_init(&kv->key, k, len);
//   if (r) return -1;
// 
//   kv->val = v;
//   _ejson_inc_ref(v);
// 
//   return 0;
// }

// static int _ejson_obj_append(ejson_obj_t *obj, const char *k, size_t n, ejson_t *v)
// {
//   int r = _ejson_obj_keep(obj, obj->nr + 1);
//   if (r) return -1;
// 
//   _ejson_kv_t *kv = obj->kvs + obj->nr;
//   memset(kv, 0, sizeof(*kv));
//   r = _ejson_kv_set(kv, k, n, v);
//   if (r) return -1;
// 
//   ++obj->nr;
// 
//   return 0;
// }

static void _ejson_arr_reset(ejson_arr_t *arr)
{
  if (!arr) return;
  for (size_t i=0; i<arr->nr; ++i) {
    ejson_t *v = arr->vals[i];
    ejson_dec_ref(v);
    arr->vals[i] = NULL;
  }
  arr->nr = 0;
}

static void _ejson_arr_release(ejson_arr_t *arr)
{
  if (!arr) return;
  _ejson_arr_reset(arr);
  if (arr->vals) {
    free(arr->vals);
    arr->vals = NULL;
  }
  arr->cap = 0;
}

static int _ejson_arr_append(ejson_arr_t *arr, ejson_t *v)
{
  if (arr->nr == arr->cap) {
    size_t cap = arr->cap + 16;
    ejson_t **vals = (ejson_t**)realloc(arr->vals, cap * sizeof(*vals));
    if (!vals) return -1;
    arr->vals = vals;
    arr->cap  = cap;
  }

  arr->vals[arr->nr++] = v;
  _ejson_inc_ref(v);

  return 0;
}

ejson_t* ejson_new_num(double v)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson_num_t *num = &ejson->_num;
  num->dbl = v;
  ejson->type = EJSON_NUM;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_str(const char *v, size_t n)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  _ejson_str_init(&ejson->_str);
  int r = _ejson_str_append(&ejson->_str, v, n);
  if (r) {
    free(ejson);
    return NULL;
  }
  ejson->type = EJSON_STR;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_obj(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_OBJ;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_arr(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_ARR;
  ejson->refc = 1;
  return ejson;
}

void ejson_inc_ref(ejson_t *ejson)
{
  if (!ejson) return;
  ++ejson->refc;
}

static void _ejson_release(ejson_t *ejson)
{
  switch (ejson->type) {
    case EJSON_NULL:
    case EJSON_TRUE:
    case EJSON_FALSE:
      break;
    case EJSON_NUM:
      _ejson_num_release(&ejson->_num);
      break;
    case EJSON_STR:
      _ejson_str_release(&ejson->_str);
      break;
    case EJSON_BIN:
      _ejson_bin_release(&ejson->_bin);
      break;
    case EJSON_OBJ:
      _ejson_obj_release(&ejson->_obj);
      break;
    case EJSON_ARR:
      _ejson_arr_release(&ejson->_arr);
      break;
    default: return;
  }
}

void ejson_dec_ref(ejson_t *ejson)
{
  if (!ejson) return;
  if (ejson->refc > 1) {
    --ejson->refc;
    return;
  }
  --ejson->refc;
  _ejson_release(ejson);
  free(ejson);
}

// static int ejson_str_append(ejson_t *ejson, const char *v, size_t n)
// {
//   if (ejson->type != EJSON_STR) return -1;
//   return _ejson_str_append(&ejson->_str, v, n);
// }

// static int ejson_obj_set(ejson_t *ejson, const char *k, size_t n, ejson_t *v)
// {
//   if (ejson->type != EJSON_OBJ) return -1;
//   return _ejson_obj_append(&ejson->_obj, k, n, v);
// }

int ejson_arr_append(ejson_t *ejson, ejson_t *v)
{
  if (ejson->type != EJSON_ARR) {
    errno = EINVAL;
    return -1;
  }
  return _ejson_arr_append(&ejson->_arr, v);
}

static ejson_t* _ejson_new_num(const char *s, size_t n)
{
  char buf[128];
  snprintf(buf, sizeof(buf), "%.*s", (int)n, s);
  double dbl = 0;
  sscanf(buf, "%lg", &dbl);
  return ejson_new_num(dbl);
}

static ejson_t* _ejson_new_str(_ejson_str_t *str)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  memcpy(&ejson->_str, str, sizeof(*str));
  memset(str, 0, sizeof(*str));
  ejson->type = EJSON_STR;
  ejson->refc = 1;
  return ejson;
}

static ejson_t* _ejson_new_bin(_ejson_bin_t *bin)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  memcpy(&ejson->_bin, bin, sizeof(*bin));
  memset(bin, 0, sizeof(*bin));
  ejson->type = EJSON_BIN;
  ejson->refc = 1;
  return ejson;
}

static int _ejson_obj_set(ejson_t *ejson, _ejson_kv_t *kv)
{
  ejson_obj_t *obj = &ejson->_obj;
  int r = _ejson_obj_keep(obj, obj->nr + 1);
  if (r) return -1;

  _ejson_kv_t *_kv = obj->kvs + obj->nr;
  if (_kv == kv) return 0;

  memcpy(_kv, kv, sizeof(*kv));
  memset(kv, 0, sizeof(*kv));

  ++obj->nr;

  return 0;
}

static ejson_t* _ejson_new_obj(_ejson_kv_t *kv)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;

  ejson->type = EJSON_OBJ;
  ejson->refc = 1;

  int r = _ejson_obj_set(ejson, kv);
  if (r) {
    ejson_dec_ref(ejson);
    return NULL;
  }

  return ejson;
}


int ejson_is_null(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_NULL) ? 1 : 0;
}

int ejson_is_true(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_TRUE) ? 1 : 0;
}

int ejson_is_false(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_FALSE) ? 1 : 0;
}

int ejson_is_str(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_STR) ? 1 : 0;
}

int ejson_is_bin(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_BIN) ? 1 : 0;
}

int ejson_is_num(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_NUM) ? 1 : 0;
}

int ejson_is_obj(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_OBJ) ? 1 : 0;
}

int ejson_is_arr(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_ARR) ? 1 : 0;
}

const char* ejson_str_get(ejson_t *ejson)
{
  if (!ejson_is_str(ejson)) {
    errno = EINVAL;
    return NULL;
  }
  if (!ejson->_str.str) return "";
  return ejson->_str.str;
}

const unsigned char* ejson_bin_get(ejson_t *ejson, size_t *len)
{
  if (!ejson_is_bin(ejson)) {
    errno = EINVAL;
    return NULL;
  }
  *len = ejson->_bin.nr;
  if (!ejson->_bin.bin) return (const unsigned char*)"";
  return ejson->_bin.bin;
}

int ejson_num_get(ejson_t *ejson, double *v)
{
  if (!ejson_is_num(ejson)) {
    errno = EINVAL;
    return -1;
  }
  *v = ejson->_num.dbl;
  return 0;
}

ejson_t* ejson_obj_get(ejson_t *ejson, const char *k)
{
  if (!ejson_is_obj(ejson)) {
    errno = EINVAL;
    return NULL;
  }
  for (size_t i=0; i<ejson->_obj.nr; ++i) {
    _ejson_kv_t *kv = ejson->_obj.kvs + i;
    if (strcmp(k, kv->key.str)) continue;
    return kv->val;
  }
  /* errno = 0; */
  return NULL;
}

size_t ejson_obj_count(ejson_t *ejson)
{
  if (!ejson_is_obj(ejson)) return 0;
  return ejson->_obj.nr;
}

ejson_t* ejson_obj_idx(ejson_t *ejson, size_t idx, const char **k)
{
  if (!ejson_is_obj(ejson)) {
    errno = EINVAL;
    return NULL;
  }
  if (idx >= ejson->_obj.nr) {
    errno = ERANGE;
    return NULL;
  }
  _ejson_kv_t *kv = ejson->_obj.kvs + idx;
  *k = kv->key.str;
  return kv->val;
}

size_t ejson_arr_count(ejson_t *ejson)
{
  if (!ejson_is_arr(ejson)) {
    // FIXME: return (size_t)-1;
    return 0;
  }
  return ejson->_arr.nr;
}

ejson_t* ejson_arr_get(ejson_t *ejson, size_t idx)
{
  if (!ejson_is_arr(ejson)) {
    errno = EINVAL;
    return NULL;
  }
  if (idx >= ejson->_arr.nr) {
    errno = ERANGE;
    return NULL;
  }
  return ejson->_arr.vals[idx];
}

static int _ejson_kv_cmp(_ejson_kv_t *l, _ejson_kv_t *r)
{
  int rr = strcmp(l->key.str, r->key.str);
  if (rr) return rr;
  return ejson_cmp(l->val, r->val);
}

static int _ejson_obj_cmp(ejson_obj_t *l, ejson_obj_t *r)
{
  for (size_t i=0; i<l->nr && i<r->nr; ++i) {
    int rr = _ejson_kv_cmp(l->kvs + i, r->kvs + i);
    if (rr) return rr;
  }
  if (l->nr < r->nr) return -1;
  if (l->nr > r->nr) return 1;
  return 0;
}

static int _ejson_arr_cmp(ejson_arr_t *l, ejson_arr_t *r)
{
  for (size_t i=0; i<l->nr && i<r->nr; ++i) {
    int rr = ejson_cmp(l->vals[i], r->vals[i]);
    if (rr) return rr;
  }
  if (l->nr < r->nr) return -1;
  return 1;
}

static int _dbl_cmp(double l, double r)
{
  double epsilon = 1e-6;
  if(fabs(l-r) < epsilon) return 0;
  return (l<r) ? -1 : 1;
}

static int _ejson_bin_cmp(_ejson_bin_t *l, _ejson_bin_t *r)
{
  if (l->nr < r->nr) return -1;
  if (l->nr > r->nr) return 1;
  if (l->nr == 0) return 0;
  return memcmp(l->bin, r->bin, l->nr);
}

int ejson_cmp(ejson_t *l, ejson_t *r)
{
  if (l == r) return 0;
  if (l->type < r->type) return -1;
  if (l->type > r->type) return 1;
  switch (l->type) {
    case EJSON_NULL:
    case EJSON_FALSE:
    case EJSON_TRUE:
      return 0;
    case EJSON_NUM:
      return _dbl_cmp(l->_num.dbl, r->_num.dbl);
    case EJSON_STR:
      return strcmp(l->_str.str, r->_str.str);
    case EJSON_BIN:
      return _ejson_bin_cmp(&l->_bin, &r->_bin);
      break;
    case EJSON_OBJ:
      return _ejson_obj_cmp(&l->_obj, &r->_obj);
    case EJSON_ARR:
      return _ejson_arr_cmp(&l->_arr, &r->_arr);
    default: /* errno = EINVAL; */ return -1;
  }
}

#if 1        /* { */
#define _ADJUST() do {                               \
  if (n < 0) { errno = EINVAL; return -1; }          \
  count += n;                                        \
  p += n;                                            \
  len -= n;                                          \
  if ((size_t)n >= len) {                            \
    p = NULL;                                        \
    len = 0;                                         \
  }                                                  \
} while (0)

#define _SNPRINTF(fmt, ...) do {                     \
  n = snprintf(p, len, fmt, ##__VA_ARGS__);          \
  _ADJUST();                                         \
} while (0)

static int _str_serialize(const char *str, size_t nr, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  _SNPRINTF("%c", '"');

  if (str && nr > 0) {
    const char *escape1 = "\\\"";
    const char *escape2         = "\b\f\r\n\t";
    const char *escape2_replace = "bfrnt";
    const char *prev = str;
    for (size_t i=0; i<nr; ++i) {
      const char *curr = str + i;
      const char c = *curr;
      if (strchr(escape1, c)) {
        if (curr > prev) {
          _SNPRINTF("%.*s", (int)(curr - prev), prev);
        }
        _SNPRINTF("\\%c", c);
        prev = ++curr;
        continue;
      }
      const char *pp = strchr(escape2, c);
      if (pp) {
        if (curr > prev) {
          _SNPRINTF("%.*s", (int)(curr - prev), prev);
        }
        _SNPRINTF("\\%c", escape2_replace[pp-escape2]);
        prev = ++curr;
        continue;
      }
    }
    if (*prev) {
      _SNPRINTF("%s", prev);
    }
  }

  _SNPRINTF("%c", '"');

  return count;
}

static int _bin_serialize(const unsigned char *bin, size_t nr,
    char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  _SNPRINTF("b%c", '"');

  if (bin && nr > 0) {
    _SNPRINTF("\\x");
    for (size_t i=0; i<nr; ++i) {
      const unsigned char *curr = bin + i;
      const unsigned char c = *curr;
      _SNPRINTF("%02x", c);
    }
  }

  _SNPRINTF("%c", '"');

  return count;
}

static int _ejson_str_serialize(_ejson_str_t *str, char *buf, size_t len)
{
  return _str_serialize(str->str, str->nr, buf, len);
}

static int _ejson_bin_serialize(_ejson_bin_t *bin, char *buf, size_t len)
{
  return _bin_serialize(bin->bin, bin->nr, buf, len);
}

static int _ejson_kv_serialize(_ejson_kv_t *kv, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  n = _str_serialize(kv->key.str, kv->key.nr, buf, len);
  _ADJUST();
  _SNPRINTF(":");

  n = ejson_serialize(kv->val, p, len);
  _ADJUST();

  return count;
}

static int _ejson_obj_serialize(ejson_obj_t *obj, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  _SNPRINTF("{");
  for (size_t i=0; i<obj->nr; ++i) {
    _ejson_kv_t *kv = obj->kvs + i;
    if (i) _SNPRINTF(",");
    n = _ejson_kv_serialize(kv, p, len);
    _ADJUST();
  }
  _SNPRINTF("}");

  return count;
}

static int _ejson_arr_serialize(ejson_arr_t *arr, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  _SNPRINTF("[");
  for (size_t i=0; i<arr->nr; ++i) {
    ejson_t *val = arr->vals[i];
    if (i) _SNPRINTF(",");
    n = ejson_serialize(val, p, len);
    _ADJUST();
  }
  _SNPRINTF("]");

  return count;
}

#undef _ADJUST
#undef _SNPRINTF
#endif       /* } */

int ejson_serialize(ejson_t *ejson, char *buf, size_t len)
{
  if (buf && len) *buf = '\0';
  if (!ejson) return 1;

  switch (ejson->type) {
    case EJSON_NULL:
      return snprintf(buf, len, "null");
    case EJSON_FALSE:
      return snprintf(buf, len, "false");
    case EJSON_TRUE:
      return snprintf(buf, len, "true");
    case EJSON_NUM:
      return snprintf(buf, len, "%lg", ejson->_num.dbl);
    case EJSON_STR:
      return _ejson_str_serialize(&ejson->_str, buf, len);
    case EJSON_BIN:
      return _ejson_bin_serialize(&ejson->_bin, buf, len);
    case EJSON_OBJ:
      return _ejson_obj_serialize(&ejson->_obj, buf, len);
    case EJSON_ARR:
      return _ejson_arr_serialize(&ejson->_arr, buf, len);
    default: errno = EINVAL; return -1;
  }
}

const parser_loc_t* ejson_get_loc(ejson_t *ejson)
{
  if (!ejson) {
    errno = EINVAL;
    return NULL;
  }
  return &ejson->loc;
}

iconv_t ejson_parser_iconv_open(void)
{
  const char *to           = EJSON_PARSER_TO;
  const char *from         = EJSON_PARSER_FROM;
  return iconv_open(to, from);
}

void ejson_parser_iconv_close(iconv_t cnv)
{
  if (cnv == (iconv_t)-1) return;
  iconv_close(cnv);
}

