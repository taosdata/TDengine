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

#include "taosws_helpers.h"

#include "../test_helper.h"
#include "test_config.h"

#include "iconv_wrapper.h"
#include <locale.h>
#include <stdio.h>

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef enum stage_s          stage_t;
enum stage_s {
  STAGE_INITED,
  STAGE_CONNECTED,
  STAGE_STATEMENT,
};

typedef struct arg_s                arg_t;

typedef int (*regress_f)(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt);

struct arg_s {
  const char           *url;

  const char           *name;

  const char           *regname;
  regress_f             regress;
};

typedef struct sql_s                     sql_t;
struct sql_s {
  const char            *sql;
  int                    __line__;
};

static char          _c_charset[4096] = "UTF-8";

static void _init_charset(void)
{
#ifdef _WIN32
  UINT acp = GetACP();
  switch (acp) {
    case 437:
    case 850:
    case 858:
      snprintf(_c_charset, sizeof(_c_charset), "CP850");
      break;
    case 936:
      snprintf(_c_charset, sizeof(_c_charset), "GB18030");
      break;
    case 65001:
      snprintf(_c_charset, sizeof(_c_charset), "UTF-8");
      break;
    default:
      snprintf(_c_charset, sizeof(_c_charset), "CP%d", acp);
      break;
  }
#else
  const char *locale = setlocale(LC_CTYPE, "");
  if (!locale) return;

  const char *p = strchr(locale, '.');
  p = p ? p + 1 : locale;
  snprintf(_c_charset, sizeof(_c_charset), "%s", p);
#endif
}

static int _charset_conv(const char *src, size_t slen, char *dst, size_t dlen, const char *fromcode, const char *tocode)
{
  iconv_t cnv = iconv_open(tocode, fromcode);
  if (cnv == (iconv_t)-1) {
    E("no conversion from `%s` to `%s`", fromcode, tocode);
    return -1;
  }

  char       *inbuf        = (char*)src;
  char       *outbuf       = dst;
  size_t      inbytesleft  = slen;
  size_t      outbytesleft = dlen - 1;

  size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  if (n || inbytesleft) {
    E("conversion from `%s` to `%s` failed", fromcode, tocode);
    return -1;
  }

  *outbuf = '\0';

  iconv_close(cnv);

  return 0;
}

static int _dummy(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stage;
  (void)taos;
  (void)stmt;
  return 0;
}

static int _query(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{

  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage != STAGE_CONNECTED) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 0},
    {"create table foo.t (ts timestamp, name varchar(20))", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), ?)", __LINE__, 0},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    char utf8[4096]; utf8[0] = '\0';
    r = _charset_conv(sql, strlen(sql), utf8, sizeof(utf8), _c_charset, "UTF-8");
    if (r) return -1;

    WS_RES *res = CALL_ws_query(taos, utf8);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

  return 0;
}

typedef struct prepare_case_s                  prepare_case_t;
struct prepare_case_s {
    int                        __line__;
    const char                *sql;

    int                        errCode;

    int                        nr_fields;
    int                        nr_tags;

    uint8_t                    req_tbname:1;
    uint8_t                    is_insert:1;
};

static int _prepare(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) {
    r = _query(arg, stage, taos, stmt);
  } else if (stage == STAGE_STATEMENT) {
    const char *sql = "show databases";

    char utf8[4096]; utf8[0] = '\0';
    r = _charset_conv(sql, strlen(sql), utf8, sizeof(utf8), _c_charset, "UTF-8");
    if (r) return -1;

    r = CALL_ws_stmt_prepare(stmt, utf8, (unsigned long)strlen(utf8));
  }

  return r;
}

static int _fetch(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage != STAGE_CONNECTED) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 0},
    {"create table foo.t (ts timestamp, name varchar(20))", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), null)", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    char utf8[4096]; utf8[0] = '\0';
    r = _charset_conv(sql, strlen(sql), utf8, sizeof(utf8), _c_charset, "UTF-8");
    if (r) return -1;

    WS_RES *res = CALL_ws_query(taos, utf8);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

  // insert into foo.t (ts, name) values (now(), null)
  const char *sql = "select name from foo.t";
  char utf8[4096]; utf8[0] = '\0';
  r = _charset_conv(sql, strlen(sql), utf8, sizeof(utf8), _c_charset, "UTF-8");
  if (r) return -1;

  WS_RES *res = CALL_ws_query(taos, utf8);
  int e = ws_errno(res);
  if (!!e) {
    E("failed:[%d]%s", e, ws_errstr(res));
    r = -1;
  }
  if (res) {
    const void *ptr = NULL;
    int32_t rows = 0;
    r = CALL_ws_fetch_raw_block(res, &ptr, &rows);
    E("r:%d;ptr:%p;rows:%d", r, ptr, rows);
  }
  if (res) {
    uint8_t ty = 0;
    uint32_t len = 0;
    const void *p = NULL;
    int row = 0, col = 0;

    ty = 0; len = 0; p = NULL;
    p = CALL_ws_get_value_in_block(res, row, col, &ty, &len);
    if (p) {
      E("expected null, but got ==(%d,%d):p:%p;ty:%d;len:%d==", row, col, p, ty, len);
      r = -1;
    }
  }
  if (res) CALL_ws_free_result(res);
  if (r) return -1;

  return 0;
}

static int _charset(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage != STAGE_CONNECTED) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 0},
    {"create table foo.t (ts timestamp, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"insert into foo.t (ts, name, mark) values (now(), '你好', '世界')", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    char utf8[4096]; utf8[0] = '\0';
    r = _charset_conv(sql, strlen(sql), utf8, sizeof(utf8), _c_charset, "UTF-8");
    if (r) return -1;

    WS_RES *res = CALL_ws_query(taos, utf8);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

  const char *sql = "select name, mark from foo.t";
  WS_RES *res = CALL_ws_query(taos, sql);
  int e = ws_errno(res);
  if (!!e) {
    E("failed:[%d]%s", e, ws_errstr(res));
    r = -1;
  }
  if (res) {
    const void *ptr = NULL;
    int32_t rows = 0;
    r = CALL_ws_fetch_raw_block(res, &ptr, &rows);
    E("r:%d;ptr:%p;rows:%d", r, ptr, rows);
  }
  if (res) {
    char buf[4096]; buf[0] = '\0';
    uint8_t ty = 0;
    uint32_t len = 0;
    const void *p = NULL;
    int row = 0, col = 0;

    r = 0;
    if (r == 0) {
      row = 0, col = 0, ty = 0; len = 0; p = NULL;
      p = CALL_ws_get_value_in_block(res, row, col, &ty, &len);
      r = -1;
      if (p && ty == TSDB_DATA_TYPE_VARCHAR) r = _charset_conv(p, len, buf, sizeof(buf), "UTF-8", _c_charset);
      E("buf:[%s]", buf);
      if (r == 0) r = strcmp("你好", buf);
      if (r) {
        E("expected 'varchar:你好', but got ==(%d,%d):p:%p;ty:%d;len:%d==", row, col, p, ty, len);
        r = -1;
      }
    }
    if (r == 0) {
      row = 0, col = 1, ty = 0; len = 0; p = NULL;
      p = CALL_ws_get_value_in_block(res, row, col, &ty, &len);
      r = -1;
      if (p && ty == TSDB_DATA_TYPE_NCHAR) r = _charset_conv(p, len, buf, sizeof(buf), "UTF-8", _c_charset);
      E("buf:[%s]", buf);
      if (r == 0) r = strcmp("世界", buf);
      if (r) {
        fprintf(stderr, "\n=======================\n");
        fprintf(stderr, "0x");
        for (size_t i=0; i<len; ++i) {
          const unsigned char c = ((const unsigned char*)p)[i];
          fprintf(stderr, "%02x", c);
        }
        fprintf(stderr, "\n=======================\n");
        E("expected 'nchar:世界', but got ==(%d,%d):p:%p;ty:%d;len:%d==", row, col, p, ty, len);
#ifdef _WIN32         /* { */
        // NOTE: waiting taosws to update
        r = 0;
#else                 /* }{ */
        r = -1;
#endif                /* } */
      }
    }
  }
  if (res) CALL_ws_free_result(res);
  if (r) return -1;

  return 0;
}

static int _prepare_insert(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    char utf8[4096]; utf8[0] = '\0';
    r = _charset_conv(sql, strlen(sql), utf8, sizeof(utf8), _c_charset, "UTF-8");
    if (r) return -1;

    WS_RES *res = CALL_ws_query(taos, utf8);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

  // NOTE: create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))
  const char *sql = "insert into foo.t (ts, v, name, mark) values (?, ?, ?, ?)";
  // const char *sql = "insert into foo.t (ts, v, name) values (?, ?, ?)";
  r = CALL_ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_ws_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  int64_t               ts            = 1665025866843L;
  const int32_t         ts_len        = (int32_t)sizeof(ts);
  const char            ts_is_null    = (char)0;

  int                   v             = 223;
  const int32_t         v_len         = (int32_t)sizeof(v);
  const char            v_is_null     = (char)0;


  const char           *s_name        = "中国";
  char name[4096]; name[0] = '\0';
  tod_conv(_c_charset, "UTF-8", s_name, strlen(s_name), name, sizeof(name));
  const int32_t         name_len      = (int32_t)strlen(name);
  const char            name_is_null  = (char)0;
  A(name_len == 6, "");

  const char           *s_mark        = "苏州";
  char mark[4096]; mark[0] = '\0';
  tod_conv(_c_charset, "UTF-8", s_mark, strlen(s_mark), mark, sizeof(mark));
  const int32_t         mark_len      = (int32_t)strlen(mark);
  A(mark_len == 6, "");
  const char            mark_is_null  = (char)0;

  WS_MULTI_BIND mbs[4] = {0};
  mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
  mbs[0].buffer             = &ts;
  mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
  mbs[0].length             = &ts_len;
  mbs[0].is_null            = &ts_is_null;
  mbs[0].num = 1;

  mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
  mbs[1].buffer             = &v;
  mbs[1].buffer_length      = (uintptr_t)sizeof(v);
  mbs[1].length             = &v_len;
  mbs[1].is_null            = &v_is_null;
  mbs[1].num = 1;

  mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
  mbs[2].buffer             = name;
  mbs[2].buffer_length      = (uintptr_t)strlen(name);
  mbs[2].length             = &name_len;
  mbs[2].is_null            = &name_is_null;
  mbs[2].num = 1;

  mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
  mbs[3].buffer             = (void*)mark;
  mbs[3].buffer_length      = (uintptr_t)mark_len;
  mbs[3].length             = (int32_t*)&mark_len;
  mbs[3].is_null            = (char*)&mark_is_null;
  mbs[3].num = 1;

  r = CALL_ws_stmt_bind_param_batch(stmt, &mbs[0], sizeof(mbs)/sizeof(mbs[0]));
  if (r) return -1;

  r = CALL_ws_stmt_add_batch(stmt);
  if (r) return -1;

  int32_t affected_rows = 0;
  r = CALL_ws_stmt_execute(stmt, &affected_rows);
  if (r) return -1;
  A(affected_rows == 1, "");

  return 0;
}

static int _prepare_insert_many(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(21), mark nchar(20))", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = CALL_ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into foo.t (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_ws_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  int64_t               ts[2]                 = {1665025866843L, 1665025866844L};
  const int32_t         ts_len[2]             = {(int32_t)sizeof(ts[0]), (int32_t)sizeof(ts[1])};
  uintptr_t             ts_buf_len            = (uintptr_t)sizeof(ts[0]);
  A(ts_buf_len == 8, "");
  const char            ts_is_null[2]         = {(char)0, (char)0};

  int                   v[2]                  = {223, 224};
  const int32_t         v_len[2]              = {(int32_t)sizeof(v[0]), (int32_t)sizeof(v[1])};
  uintptr_t             v_buf_len             = (uintptr_t)sizeof(v[0]);
  const char            v_is_null[2]          = {(char)0, (char)0};

  const char            name[2][40]           = {"中国","中华人民共和国"};
  const int32_t         name_len[2]           = {(int32_t)strlen(name[0]), (int32_t)strlen(name[1])};
  uintptr_t             name_buf_len          = (int32_t)(sizeof(name[0])/sizeof(name[0][0]));
  A(name_buf_len == 40, "");
  const char            name_is_null[2]       = {(char)0, (char)0};

  const char            mark[2][40]           = {"苏州","无锡"};
  const int32_t         mark_len[2]           = {(int32_t)strlen(mark[0]), (int32_t)strlen(mark[1])};
  uintptr_t             mark_buf_len          = (int32_t)(sizeof(mark[0])/sizeof(mark[0][0]));
  A(mark_buf_len == 40, "");
  const char            mark_is_null[2]       = {(char)0, (char)0};

  WS_MULTI_BIND mbs[4] = {0};
  mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
  mbs[0].buffer             = (void*)&ts[0];
  mbs[0].buffer_length      = ts_buf_len;
  mbs[0].length             = (int32_t*)&ts_len[0];
  mbs[0].is_null            = (char*)&ts_is_null[0];
  mbs[0].num = 2;

  mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
  mbs[1].buffer             = (void*)&v[0];
  mbs[1].buffer_length      = v_buf_len;
  mbs[1].length             = (int32_t*)&v_len[0];
  mbs[1].is_null            = (char*)&v_is_null[0];
  mbs[1].num = 2;

  mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
  mbs[2].buffer             = (void*)&name[0];
  A((void*)&name[0] == (void*)&name[0][0], "");
  mbs[2].buffer_length      = name_buf_len;
  mbs[2].length             = (int32_t*)&name_len[0];
  mbs[2].is_null            = (char*)&name_is_null[0];
  mbs[2].num = 2;

  mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
  mbs[3].buffer             = (void*)&mark[0];
  mbs[3].buffer_length      = mark_buf_len;
  mbs[3].length             = (int32_t*)&mark_len[0];
  mbs[3].is_null            = (char*)&mark_is_null[0];
  mbs[3].num = 2;

  r = CALL_ws_stmt_bind_param_batch(stmt, &mbs[0], sizeof(mbs)/sizeof(mbs[0]));
  if (r) return -1;

  r = CALL_ws_stmt_add_batch(stmt);
  if (r) return -1;

  int32_t affected_rows = 0;
  r = CALL_ws_stmt_execute(stmt, &affected_rows);
  if (r) return -1;
  A(affected_rows == 2, "");

  return 0;
}

static int _prepare_insert_normal_dynamic(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = CALL_ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into ? (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_ws_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  int64_t               ts            = 1665025866843L;
  const int32_t         ts_len        = (int32_t)sizeof(ts);
  const char            ts_is_null    = (char)0;

  int                   v             = 223;
  const int32_t         v_len         = (int32_t)sizeof(v);
  const char            v_is_null     = (char)0;

  const char           *name          = "中国";
  const int32_t         name_len      = (int32_t)strlen(name);
  const char            name_is_null  = (char)0;

  const char           *mark          = "苏州";
  const int32_t         mark_len      = (int32_t)strlen(mark);
  const char            mark_is_null  = (char)0;

  WS_MULTI_BIND mbs[4] = {0};
  mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
  mbs[0].buffer             = (void*)&ts;
  mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
  mbs[0].length             = (int32_t*)&ts_len;
  mbs[0].is_null            = (char*)&ts_is_null;
  mbs[0].num = 1;

  mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
  mbs[1].buffer             = (void*)&v;
  mbs[1].buffer_length      = (uintptr_t)sizeof(v);
  mbs[1].length             = (int32_t*)&v_len;
  mbs[1].is_null            = (char*)&v_is_null;
  mbs[1].num = 1;

  mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
  mbs[2].buffer             = (void*)name;
  mbs[2].buffer_length      = (uintptr_t)name_len;
  mbs[2].length             = (int32_t*)&name_len;
  mbs[2].is_null            = (char*)&name_is_null;
  mbs[2].num = 1;

  mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
  mbs[3].buffer             = (void*)mark;
  mbs[3].buffer_length      = (uintptr_t)mark_len;
  mbs[3].length             = (int32_t*)&mark_len;
  mbs[3].is_null            = (char*)&mark_is_null;
  mbs[3].num = 1;

  const char *tbl = "foo.t"; // NOTE: must specify db, why?
  r = CALL_ws_stmt_set_tbname(stmt, tbl);
  if (r) return -1;

  r = CALL_ws_stmt_bind_param_batch(stmt, &mbs[0], sizeof(mbs)/sizeof(mbs[0]));
  if (r) return -1;

  r = CALL_ws_stmt_add_batch(stmt);
  if (r) return -1;

  int32_t affected_rows = 0;
  r = CALL_ws_stmt_execute(stmt, &affected_rows);
  if (r) return -1;
  A(affected_rows == 1, "");

  return 0;
}

static int _prepare_insert_normals_dynamic(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"create table foo.m (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = CALL_ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into ? (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_ws_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  {
    int64_t               ts            = 1665025866843L;
    const int32_t         ts_len        = (int32_t)sizeof(ts);
    const char            ts_is_null    = (char)0;

    int                   v             = 223;
    const int32_t         v_len         = (int32_t)sizeof(v);
    const char            v_is_null     = (char)0;

    char                  name[]        = "中国";
    const int32_t         name_len      = (int32_t)strlen(name);
    const char            name_is_null  = (char)0;

    char                  mark[]        = "苏州";
    const int32_t         mark_len      = (int32_t)strlen(mark);
    const char            mark_is_null  = (char)0;

    WS_MULTI_BIND mbs[4] = {0};
    mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
    mbs[0].buffer             = (void*)&ts;
    mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
    mbs[0].length             = (int32_t*)&ts_len;
    mbs[0].is_null            = (char*)&ts_is_null;
    mbs[0].num = 1;

    mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
    mbs[1].buffer             = (void*)&v;
    mbs[1].buffer_length      = (uintptr_t)sizeof(v);
    mbs[1].length             = (int32_t*)&v_len;
    mbs[1].is_null            = (char*)&v_is_null;
    mbs[1].num = 1;

    mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
    mbs[2].buffer             = (void*)name;
    mbs[2].buffer_length      = (uintptr_t)name_len;
    mbs[2].length             = (int32_t*)&name_len;
    mbs[2].is_null            = (char*)&name_is_null;
    mbs[2].num = 1;

    mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
    mbs[3].buffer             = (void*)mark;
    mbs[3].buffer_length      = (uintptr_t)mark_len;
    mbs[3].length             = (int32_t*)&mark_len;
    mbs[3].is_null            = (char*)&mark_is_null;
    mbs[3].num = 1;

    const char *tbl = "foo.t"; // NOTE: must specify db, why?
    r = CALL_ws_stmt_set_tbname(stmt, tbl);
    if (r) return -1;

    r = CALL_ws_stmt_bind_param_batch(stmt, &mbs[0], sizeof(mbs)/sizeof(mbs[0]));
    if (r) return -1;

    r = CALL_ws_stmt_add_batch(stmt);
    if (r) return -1;

    memset(mbs, 0, sizeof(mbs));
    memset(name, 0, sizeof(name));
    memset(mark, 0, sizeof(mark));
  }
  {
    int64_t               ts            = 1665025866844L;
    const int32_t         ts_len        = (int32_t)sizeof(ts);
    const char            ts_is_null    = (char)0;

    int                   v             = 224;
    const int32_t         v_len         = (int32_t)sizeof(v);
    const char            v_is_null     = (char)0;

    const char           *name          = "中国x";
    const int32_t         name_len      = (int32_t)strlen(name);
    const char            name_is_null  = (char)0;

    const char           *mark          = "苏州x";
    const int32_t         mark_len      = (int32_t)strlen(mark);
    const char            mark_is_null  = (char)0;

    WS_MULTI_BIND mbs[4] = {0};
    mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
    mbs[0].buffer             = (void*)&ts;
    mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
    mbs[0].length             = (int32_t*)&ts_len;
    mbs[0].is_null            = (char*)&ts_is_null;
    mbs[0].num = 1;

    mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
    mbs[1].buffer             = (void*)&v;
    mbs[1].buffer_length      = (uintptr_t)sizeof(v);
    mbs[1].length             = (int32_t*)&v_len;
    mbs[1].is_null            = (char*)&v_is_null;
    mbs[1].num = 1;

    mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
    mbs[2].buffer             = (void*)name;
    mbs[2].buffer_length      = (uintptr_t)name_len;
    mbs[2].length             = (int32_t*)&name_len;
    mbs[2].is_null            = (char*)&name_is_null;
    mbs[2].num = 1;

    mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
    mbs[3].buffer             = (void*)mark;
    mbs[3].buffer_length      = (uintptr_t)mark_len;
    mbs[3].length             = (int32_t*)&mark_len;
    mbs[3].is_null            = (char*)&mark_is_null;
    mbs[3].num = 1;

    const char *tbl = "foo.m";
    r = CALL_ws_stmt_set_tbname(stmt, tbl);
    if (r) return -1;

    r = CALL_ws_stmt_bind_param_batch(stmt, &mbs[0], sizeof(mbs)/sizeof(mbs[0]));
    if (r) return -1;

    r = CALL_ws_stmt_add_batch(stmt);
    if (r) return -1;
  }

  int32_t affected_rows = 0;
  r = CALL_ws_stmt_execute(stmt, &affected_rows);
  if (r) return -1;
  A(affected_rows == 2, "affected_rows:%d", affected_rows);

  return 0;
}

static int _prepare_normal_get_col_fields(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(21), mark nchar(20))", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = CALL_ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

#define RECORD(...)       {__LINE__, ##__VA_ARGS__}
  struct {
    int                line;
    const char        *sql;
    int                nr_cols;
  } _inserts[] = {
    // NOTE: must specify db, why?
    RECORD("insert into foo.t (ts, v, name, mark) values (?, ?, ?, ?)", 4),
    RECORD("insert into foo.t values (?, ?, ?, ?)", 4),
    RECORD("insert into foo.t (ts, v, mark) values (?, ?, ?)", 3),
  };
  size_t _nr_inserts = sizeof(_inserts)/sizeof(_inserts[0]);
#undef RECORD

  for (size_t i=0; i<_nr_inserts; ++i) {
    int          line             = _inserts[i].line;
    const char  *sql              = _inserts[i].sql;
    int          nr_cols          = _inserts[i].nr_cols;

    r = CALL_ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
    if (r) return -1;

    int insert = 0;
    r = CALL_ws_stmt_is_insert(stmt, &insert);
    if (r) return -1;
    if (!insert) return -1;

    int colNum = 0;
    struct StmtField *cols = NULL;
    r = CALL_ws_stmt_get_col_fields(stmt, &colNum, &cols);
    if (cols) {
      CALL_ws_stmt_reclaim_fields(stmt, &cols, colNum); cols = NULL;
    }
    if (r) return -1;
    if (colNum != nr_cols) {
      E("@%dL:cols expected %d, but got ==%d==", line, nr_cols, colNum);
      return -1;
    }
  }

  return 0;
}

static int _prepare_get_tag_col_fields(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.st (ts timestamp, v int, name varchar(21), mark nchar(20)) tags (val int, feat varchar(20))", __LINE__, 1},
    {"insert into foo.t using foo.st (val, feat) tags (3, 'f3') (ts, v, name, mark) values (1665025866843, 4, 'n4', 'm4')", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = CALL_ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_ws_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

#define RECORD(...)       {__LINE__, ##__VA_ARGS__}
  struct {
    int                line;
    const char        *sql;
    int                nr_tags;
    int                nr_cols;
  } _inserts[] = {
    // NOTE: get_tag_fields seems trigger abortion in taosadapter->taosc if the statement is not subtble-related!!!!!!!
    // RECORD("insert into foo.t (ts, v, name, mark) values (?, ?, ?, ?)", 0, 4),
    // RECORD("insert into foo.t values (?, ?, ?, ?)", 0, 4),
    // RECORD("insert into foo.t (ts, v, mark) values (?, ?, ?)", 0, 3),
    // RECORD("insert into foo.t (ts) values (?)", 0, 1),
    // RECORD("insert into foo.t (ts) values (1665025866843)", 0, 1), // NOTE: weird!!!, literal col value, but get_col_fields still reports!!!
    // RECORD("insert into foo.t (ts, v) values (now(), 3)", 0, 2), // NOTE: weird!!!, literal col value, but get_col_fields still reports!!!
    // RECORD("insert into foo.t (ts, v) values (1665025866843, 3)", 0, 2), // NOTE: weird!!!, mixing literal and parameter-markers!!!
    // RECORD("insert into foo.t (ts, v) values (now(), ?)", 0, 2), // NOTE: weird!!!, mixing function and parameter-markers!!!
    // RECORD("insert into foo.t (ts, v) values (1665025866843, ?)", 0, 2), // NOTE: weird!!!, mixing literal and parameter-markers!!!
    // RECORD("insert into foo.t (ts, v, mark) values (1665025866843, ?, ?)", 0, 3), // NOTE: weird!!!, mixing literal and parameter-markers!!!
    // RECORD("insert into foo.t values (1665025866843, ?, ?, ?)", 0, 4), // NOTE: weird!!!, mixing literal and parameter-markers!!!
    RECORD("insert into foo.t using foo.st (val, feat) tags (?, ?) (ts, v, mark) values (?, ?, ?)", 2, 3),
    RECORD("insert into foo.t using foo.st (val) tags (?) (ts, v, mark) values (?, ?, ?)", 1, 3),
    RECORD("insert into foo.t using foo.st (val) tags (4) (ts, v, mark) values (?, ?, ?)", 1, 3),  // NOTE: weird!!!, literal tag value, but get_tag_fields still reports!!!
  };
  size_t _nr_inserts = sizeof(_inserts)/sizeof(_inserts[0]);
#undef RECORD

  for (size_t i=0; i<1 && i<_nr_inserts; ++i) {
    int          line             = _inserts[i].line;
    const char  *sql              = _inserts[i].sql;
    int          nr_tags          = _inserts[i].nr_tags;
    int          nr_cols          = _inserts[i].nr_cols;

    r = CALL_ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
    if (r) return -1;

    int insert = 0;
    r = CALL_ws_stmt_is_insert(stmt, &insert);
    if (r) return -1;
    if (!insert) return -1;

    int tagNum = 0;
    struct StmtField *tags = NULL;
    r = CALL_ws_stmt_get_tag_fields(stmt, &tagNum, &tags);
    if (tags) {
      CALL_ws_stmt_reclaim_fields(stmt, &tags, tagNum); tags = NULL;
    }
    if (r) return -1;
    if (tagNum != nr_tags) {
      E("@%dL:tags expected %d, but got ==%d==", line, nr_tags, tagNum);
      return -1;
    }

    int colNum = 0;
    struct StmtField *cols = NULL;
    r = CALL_ws_stmt_get_col_fields(stmt, &colNum, &cols);
    if (cols) {
      CALL_ws_stmt_reclaim_fields(stmt, &cols, colNum); cols = NULL;
    }
    if (r) return -1;
    if (colNum != nr_cols) {
      E("@%dL:cols expected %d, but got ==%d==", line, nr_cols, colNum);
      return -1;
    }
  }

  return 0;
}

#define RECORD(x) {x, #x}

static struct {
  regress_f             func;
  const char           *name;
} _regressions[] = {
  RECORD(_dummy),
  RECORD(_query),
  RECORD(_prepare),
  RECORD(_fetch),
  RECORD(_charset),
  RECORD(_prepare_insert),
  RECORD(_prepare_insert_many),
  RECORD(_prepare_insert_normal_dynamic),
  RECORD(_prepare_insert_normals_dynamic),
  RECORD(_prepare_normal_get_col_fields),
  RECORD(_prepare_get_tag_col_fields),
};

static int on_statement(const arg_t *arg, WS_TAOS *taos, WS_STMT *stmt)
{
  int r = 0;

  r = arg->regress(arg, STAGE_STATEMENT, taos, stmt);
  if (r) return -1;

  return 0;
}

static int on_connected(const arg_t *arg, WS_TAOS *taos)
{
  int r = 0;

  r = arg->regress(arg, STAGE_CONNECTED, taos, NULL);
  if (r) return -1;

  WS_STMT *stmt = CALL_ws_stmt_init(taos);
  if (!stmt) E("%d:%s", ws_errno(NULL), ws_errstr(NULL));
  if (!stmt) return -1;

  r = on_statement(arg, taos, stmt);

  CALL_ws_stmt_close(stmt);

  return r ? -1 : 0;
}

static int on_init(const arg_t *arg)
{
  int r = 0;

  r = arg->regress(arg, STAGE_INITED, NULL, NULL);
  if (r) return -1;

  WS_TAOS *taos = CALL_ws_connect(arg->url);
  if (!taos) E("%d:%s", ws_errno(NULL), ws_errstr(NULL));
  if (!taos) return -1;

  r = on_connected(arg, taos);

  CALL_ws_close(taos);

  return r ? -1 : 0;
}

static int run_regress(const arg_t *arg)
{
  int r = 0;

  r = on_init(arg);

  return r ? -1 : 0;
}

static int start_by_arg(arg_t *arg)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);
  for (size_t i=0; i<nr; ++i) {
    arg->regname = _regressions[i].name;
    arg->regress = _regressions[i].func;
    if ((!arg->name) || tod_strcasecmp(arg->name, arg->regname) == 0) {
      r = run_regress(arg);
      if (r) return -1;
    }
  }

  return 0;
}

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s [--url <URL>] [name]", arg0);
  DUMP("");
  DUMP("supported regression names:");
  for (size_t i=0; i<sizeof(_regressions)/sizeof(_regressions[0]); ++i) {
    DUMP("  %s", _regressions[i].name);
  }
}

static int start(int argc, char *argv[])
{
  int r = 0;

  arg_t arg = {0};
  arg.url = "http://" WS_FOR_TEST;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--url") == 0) {
      ++i;
      if (i>=argc) break;
      arg.url = argv[i];
      continue;
    }

    arg.name = argv[i];
    r = start_by_arg(&arg);
    if (r) return -1;
  }

  if (arg.name) return 0;

  return start_by_arg(&arg);
}

int main(int argc, char *argv[])
{
  int r;

  _init_charset();

  r = start(argc, argv);

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

