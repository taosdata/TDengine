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

#include "test_config.h"
#include "test_helper.h"

#include "logger.h"

#include "iconv_wrapper.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int json_get_by_path(cJSON *root, const char *path, cJSON **json)
{
  const char *t = path;
  const char *s = path;
  cJSON *j = root;
  char buf[1024];
  buf[0] = '\0';
  int n;

  while (j) {
    if (!*s) break;
    while (*s == '/') ++s;
    if (!*s) break;
    t = s + 1;
    while (*t != '/' && *t) ++t;

    n = snprintf(buf, sizeof(buf), "%.*s", (int)(t-s), s);
    if (n<0 || (size_t)n>=sizeof(buf)) {
      W("buffer too small(%zd)", sizeof(buf));
      return -1;
    }
    s = t;

    if (!cJSON_IsArray(j) && !cJSON_IsObject(j)) {
      j = NULL;
      break;
    }

    char *end = NULL;
    long int i = strtol(buf, &end, 0);
    if (end && *end) {
      if (!cJSON_IsObject(j)) {
        j = NULL;
      } else {
        j = cJSON_GetObjectItem(j, buf);
      }
    } else if (!cJSON_IsArray(j)) {
      j = NULL;
    } else if (i>=cJSON_GetArraySize(j)) {
      j = NULL;
    } else {
      j = cJSON_GetArrayItem(j, i);
    }
  }

  if (json) *json = j;
  return 0;
}

int json_get_by_item(cJSON *root, int item, cJSON **json)
{
  if (!cJSON_IsArray(root)) return -1;
  cJSON *v = NULL;
  if (item >= 0 && item < cJSON_GetArraySize(root)) {
    v = cJSON_GetArrayItem(root, item);
  }
  if (json) *json = v;
  return 0;
}

const char* json_to_string(cJSON *json)
{
  if (!cJSON_IsString(json)) return NULL;
  return cJSON_GetStringValue(json);
}

const char* json_object_get_string(cJSON *json, const char *key)
{
  cJSON *val = NULL;
  int r = json_get_by_path(json, key, &val);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }

  if (!val) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }
  if (!cJSON_IsString(val)) {
    char *t1 = cJSON_PrintUnformatted(json);
    char *t2 = cJSON_PrintUnformatted(val);
    W("item (%s) in ==%s== is not string ==%s==", t1, key, t2);
    free(t1);
    free(t2);
    return NULL;
  }

  return cJSON_GetStringValue(val);
}

int json_get_number(cJSON *json, double *v)
{
  if (!cJSON_IsNumber(json)) return -1;
  if (v) *v = cJSON_GetNumberValue(json);
  return 0;
}

int json_object_get_number(cJSON *json, const char *key, double *v)
{
  cJSON *val = NULL;
  int r = json_get_by_path(json, key, &val);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return -1;
  }

  if (!val) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return -1;
  }
  if (!cJSON_IsNumber(val)) {
    char *t1 = cJSON_PrintUnformatted(json);
    char *t2 = cJSON_PrintUnformatted(val);
    W("item (%s) in ==%s== is not string ==%s==", t1, key, t2);
    free(t1);
    free(t2);
    return -1;
  }

  if (v) *v = cJSON_GetNumberValue(val);

  return 0;
}

cJSON* json_object_get_array(cJSON *json, const char *key)
{
  cJSON *val = NULL;
  int r = json_get_by_path(json, key, &val);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }

  if (!val) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }
  if (!cJSON_IsArray(val)) {
    char *t1 = cJSON_PrintUnformatted(json);
    char *t2 = cJSON_PrintUnformatted(val);
    W("item (%s) in ==%s== is not array ==%s==", t1, key, t2);
    free(t1);
    free(t2);
    return NULL;
  }

  return val;
}

static inline cJSON* load_json_from_file(const char *json_file, FILE *fn, const char *fromcode, const char *tocode)
{
  int r, e;
  r = fseek(fn, 0, SEEK_END);
  if (r) {
    e = errno;
    W("fseek file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }
  long len = ftell(fn);
  if (len == -1) {
    e = errno;
    W("ftell file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }
  r = fseek(fn, 0, SEEK_SET);
  if (r) {
    e = errno;
    W("fseek file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }

  char *buf = malloc(len + 1);
  if (!buf) {
    W("out of memory when processing file [%s]", json_file);
    return NULL;
  }

  size_t bytes = fread(buf, 1, len, fn);
  if (bytes != (size_t)len) {
    e = errno;
    W("fread file [%s] failed: [%d]%s", json_file, e, strerror(e));
    free(buf);
    return NULL;
  }

  buf[bytes] = '\0';
  const char *next = NULL;
  const char *p = buf;

  iconv_t cnv = iconv_open(tocode, fromcode);
  if (cnv == (iconv_t)-1) {
    W("no charset conversion between `%s` <=> `%s`", fromcode, tocode);
    free(buf);
    return NULL;
  }
  char *gb = NULL;
  do {
    size_t sz = bytes * 3 + 3;
    gb = malloc(sz);
    if (!gb) {
      W("out of memory");
      break;
    }
    gb[0] = '\0';
    char          *inbuf               = buf;
    size_t         inbytesleft         = bytes;
    char          *outbuf              = gb;
    size_t         outbytesleft        = sz;
    size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    if (n == (size_t)-1) {
      int e = errno;
      W("conversion from `%s` to `%s` failed:[%d]%s", fromcode, tocode, e, strerror(e));
      free(gb);
      gb = NULL;
      break;
    }
    A(inbytesleft == 0, "");
    A(outbytesleft >= 1, "");
    outbuf[0] = '\0';
    p = gb;
  } while (0);
  iconv_close(cnv);
  if (!gb) {
    free(buf);
    return NULL;
  }

  if (0) {
    iconv_t cnv = ejson_parser_iconv_open();
    if (cnv == (iconv_t)-1) {
      E("no convertion found for %s -> %s",
          EJSON_PARSER_FROM, EJSON_PARSER_TO);
      return NULL;
    }

    ejson_parser_param_t param = {0};
    // param.ctx.debug_flex = 1;
    // param.ctx.debug_bison = 1;

    r = ejson_parser_parse(p, strlen(p), &param, cnv);
    if (r) {
      parser_loc_t *loc = &param.ctx.bad_token;
      E("parsing @[%s]:%s", json_file, p);
      E("location:(%d,%d)->(%d,%d)",
          loc->first_line, loc->first_column, loc->last_line, loc->last_column);
      E("failed:%s", param.ctx.err_msg);
      free(gb);
      free(buf);
    }
    ejson_parser_param_release(&param);

    ejson_parser_iconv_close(cnv);
    if (r) return NULL;
  }

  cJSON *json = cJSON_ParseWithOpts(p, &next, true);
  free(gb);
  gb = NULL;

  if (!json) {
    W("parsing file [%s] failed: bad syntax: @[%s]", json_file, next);
    free(buf);
    return NULL;
  }

  free(buf);

  return json;
}

cJSON* load_json_file(const char *json_file, char *buf, size_t bytes, const char *fromcode, const char *tocode)
{
  int r = 0;

  FILE *fn = fopen(json_file, "rb");
  if (!fn) {
    int e = errno;
    W("open file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }

  cJSON *json = load_json_from_file(json_file, fn, fromcode, tocode);
  if (buf) {
    char *p = tod_dirname(json_file, buf, bytes);
    if (!p) r = -1;
  }

  if (r) {
    cJSON_Delete(json);
    json = NULL;
  }

  fclose(fn);

  return json;
}

////////////////////////////////////////////////////////////////////////////////
int ejson_get_by_path(ejson_t *ejson, const char *path, ejson_t **v)
{
  const char *t = path;
  const char *s = path;
  ejson_t *j = ejson;
  char buf[1024];
  buf[0] = '\0';
  int n;

  while (j) {
    if (!*s) break;
    while (*s == '/') ++s;
    if (!*s) break;
    t = s + 1;
    while (*t != '/' && *t) ++t;

    n = snprintf(buf, sizeof(buf), "%.*s", (int)(t-s), s);
    if (n<0 || (size_t)n>=sizeof(buf)) {
      W("buffer too small(%zd)", sizeof(buf));
      return -1;
    }
    s = t;

    if (!ejson_is_arr(j) && !ejson_is_obj(j)) {
      j = NULL;
      break;
    }

    char *end = NULL;
    long int i = strtol(buf, &end, 0);
    if (end && *end) {
      if (!ejson_is_obj(j)) {
        j = NULL;
      } else {
        j = ejson_obj_get(j, buf);
      }
    } else if (!ejson_is_arr(j)) {
      j = NULL;
    } else if (i<0 || (size_t)i>=ejson_arr_count(j)) {
      j = NULL;
    } else {
      j = ejson_arr_get(j, (size_t)i);
    }
  }

  if (v) *v = j;
  return 0;
}

int ejson_get_by_item(ejson_t *ejson, int item, ejson_t **v)
{
  if (!ejson_is_arr(ejson)) return -1;
  if (item >= 0 && (size_t)item < ejson_arr_count(ejson)) {
    *v = ejson_arr_get(ejson, (size_t)item);
    return 0;
  }
  *v = NULL;
  return 0;
}

const char* ejson_to_string(ejson_t *ejson)
{
  return ejson_str_get(ejson);
}

const char* ejson_object_get_string(ejson_t *ejson, const char *key)
{
  ejson_t *val = NULL;
  int r = ejson_get_by_path(ejson, key, &val);
  if (r || !val) return NULL;

  return ejson_to_string(val);
}

int ejson_get_number(ejson_t *ejson, double *v)
{
  return ejson_num_get(ejson, v);
}

int ejson_object_get_number(ejson_t *ejson, const char *key, double *v)
{
  ejson_t *val = NULL;
  int r = ejson_get_by_path(ejson, key, &val);
  if (r || !val) return -1;

  return ejson_num_get(val, v);
}

ejson_t* ejson_object_get_array(ejson_t *ejson, const char *key)
{
  ejson_t *val = NULL;
  int r = ejson_get_by_path(ejson, key, &val);
  if (r || !val) return NULL;
  if (!ejson_is_arr(val)) return NULL;

  return val;
}

static ejson_t* load_ejson_from_file(const char *json_file, FILE *fn, const char *fromcode, const char *tocode)
{
  int r, e;
  r = fseek(fn, 0, SEEK_END);
  if (r) {
    e = errno;
    W("fseek file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }
  long len = ftell(fn);
  if (len == -1) {
    e = errno;
    W("ftell file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }
  r = fseek(fn, 0, SEEK_SET);
  if (r) {
    e = errno;
    W("fseek file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }

  char *buf = malloc(len + 1);
  if (!buf) {
    W("out of memory when processing file [%s]", json_file);
    return NULL;
  }

  size_t bytes = fread(buf, 1, len, fn);
  if (bytes != (size_t)len) {
    e = errno;
    W("fread file [%s] failed: [%d]%s", json_file, e, strerror(e));
    free(buf);
    return NULL;
  }

  buf[bytes] = '\0';
  const char *p = buf;

  iconv_t cnv = iconv_open(tocode, fromcode);
  if (cnv == (iconv_t)-1) {
    W("no charset conversion between `%s` <=> `%s`", fromcode, tocode);
    free(buf);
    return NULL;
  }
  char *gb = NULL;
  do {
    size_t sz = bytes * 3 + 3;
    gb = malloc(sz);
    if (!gb) {
      W("out of memory");
      break;
    }
    gb[0] = '\0';
    char          *inbuf               = buf;
    size_t         inbytesleft         = bytes;
    char          *outbuf              = gb;
    size_t         outbytesleft        = sz;
    size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    if (n == (size_t)-1) {
      int e = errno;
      W("conversion from `%s` to `%s` failed:[%d]%s", fromcode, tocode, e, strerror(e));
      free(gb);
      gb = NULL;
      break;
    }
    A(inbytesleft == 0, "");
    A(outbytesleft >= 1, "");
    outbuf[0] = '\0';
    p = gb;
  } while (0);

  if (!gb) {
    free(buf);
    iconv_close(cnv);
    return NULL;
  }

  ejson_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;

  ejson_t *v = NULL;

  r = ejson_parser_parse(p, strlen(p), &param, cnv);
  if (r) {
    parser_loc_t *loc = &param.ctx.bad_token;
    E("parsing @[%s]:%s", json_file, p);
    E("location:(%d,%d)->(%d,%d)",
        loc->first_line, loc->first_column, loc->last_line, loc->last_column);
    E("failed:%s", param.ctx.err_msg);
  } else {
    v = param.ejson;
    param.ejson = NULL;
  }

  ejson_parser_param_release(&param);

  free(gb);
  free(buf);

  iconv_close(cnv);

  return v;
}

ejson_t* load_ejson_file(const char *json_file, char *buf, size_t bytes, const char *fromcode, const char *tocode)
{
  int r = 0;

  FILE *fn = fopen(json_file, "rb");
  if (!fn) {
    int e = errno;
    W("open file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }

  ejson_t *ejson = load_ejson_from_file(json_file, fn, fromcode, tocode);
  if (buf) {
    char *p = tod_dirname(json_file, buf, bytes);
    if (!p) r = -1;
  }

  if (r) {
    ejson_dec_ref(ejson);
    ejson = NULL;
  }

  fclose(fn);

  return ejson;
}






