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

#ifndef _taos_helpers_h_
#define _taos_helpers_h_

#include "helpers.h"

#include <taos.h>

#include <string.h>
#include <taoserror.h>

#define LOGD_TAOS(file, line, func, fmt, ...) do {                                                \
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define LOGE_TAOS(file, line, func, fmt, ...) do {                                                \
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define diag_res_impl(file, line, func, res) do {                    \
  TAOS_RES *__res = res;                                             \
  int _e         = taos_errno(__res);                                \
  if (!_e) break;                                                    \
  const char *_s = taos_errstr(__res);                               \
  LOGE_TAOS(file, line, func, "taos_errno/str(res:%p) => [%d/0x%x]%s%s%s", __res, _e, _e, color_red(), _s, color_reset());     \
} while (0)

#define diag_stmt_impl(file, line, func, stmt) do {                                       \
  TAOS_STMT *__stmt = stmt;                                                               \
  int _e          = taos_errno(NULL);                                                     \
  if (!_e) break;                                                                         \
  const char *_s1 = taos_errstr(NULL);                                                    \
  const char *_s2 = taos_stmt_errstr(__stmt);                                             \
  LOGE_TAOS(file, line, func,                                                             \
      "taos_errno/str(stmt:%p) => [%d/0x%x]%s%s%s;stmt_errstr:%s%s%s",                    \
      __stmt, _e, _e, color_red(), _s1, color_reset(), color_red(), _s2, color_reset());  \
} while (0)

#define diag_res(x_res)          diag_res_impl(file, line, func, x_res)
#define diag_stmt(x_stmt)        diag_stmt_impl(file, line, func, x_stmt)

static inline void call_taos_cleanup(const char *file, int line, const char *func)
{
  LOGD_TAOS(file, line, func, "taos_cleanup() ...");
  taos_cleanup();
  LOGD_TAOS(file, line, func, "taos_cleanup() => void");
}

static inline setConfRet call_taos_set_config(const char *file, int line, const char *func, const char *config)
{
  LOGD_TAOS(file, line, func, "taos_set_config(config:%s) ...", config);
  setConfRet r = taos_set_config(config);
  LOGD_TAOS(file, line, func, "taos_set_config(config:%s) => (%d,%.*s)", config, r.retCode, (int)sizeof(r.retMsg), r.retMsg);
  return r;
}

static inline int call_taos_init(const char *file, int line, const char *func)
{
  LOGD_TAOS(file, line, func, "taos_init() ...");
  int r = taos_init();
  LOGD_TAOS(file, line, func, "taos_init() => %d", r);
  return r;
}

static inline TAOS* call_taos_connect(const char *file, int line, const char *func, const char *ip, const char *user, const char *pass, const char *db, uint16_t port)
{
  LOGD_TAOS(file, line, func, "taos_connect(ip:%s,user:%s,pass:%s,db:%s,port:%d) ...", ip, user, pass, db, port);
  TAOS *taos = taos_connect(ip, user, pass, db, port);
  if (!taos) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_connect(ip:%s,user:%s,pass:%s,db:%s,port:%d) => %p", ip, user, pass, db, port, taos);
  return taos;
}

static inline TAOS* call_taos_connect_auth(const char *file, int line, const char *func, const char *ip, const char *user, const char *auth, const char *db, uint16_t port)
{
  LOGD_TAOS(file, line, func, "taos_connect_auth(ip:%s,user:%s,auth:%s,db:%s,port:%d) ...", ip, user, auth, db, port);
  TAOS *taos = taos_connect_auth(ip, user, auth, db, port);
  if (!taos) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_connect_auth(ip:%s,user:%s,auth:%s,db:%s,port:%d) => %p", ip, user, auth, db, port, taos);
  return taos;
}

static inline void call_taos_close(const char *file, int line, const char *func, TAOS *taos)
{
  LOGD_TAOS(file, line, func, "taos_close(taos:%p) ...", taos);
  taos_close(taos);
  LOGD_TAOS(file, line, func, "taos_close(taos:%p) => void", taos);
}

static inline const char* call_taos_data_type(const char *file, int line, const char *func, int type)
{
  (void)file;
  (void)line;
  (void)func;
  return taos_data_type(type);
}

static inline TAOS_STMT* call_taos_stmt_init(const char *file, int line, const char *func, TAOS *taos)
{
  LOGD_TAOS(file, line, func, "taos_stmt_init(taos:%p) ...", taos);
  TAOS_STMT *stmt = taos_stmt_init(taos);
  if (!stmt) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_init(taos:%p) => %p", taos, stmt);
  return stmt;
}

static inline int call_taos_stmt_prepare(const char *file, int line, const char *func, TAOS_STMT *stmt, const char *sql, unsigned long length)
{
  int n = (int)(length ? length : (sql ? strlen(sql) : 0));
  LOGD_TAOS(file, line, func, "taos_stmt_prepare(stmt:%p,sql:%.*s,length:%ld) ...", stmt, n, sql, length);
  int r = taos_stmt_prepare(stmt, sql, length);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_prepare(stmt:%p,sql:%.*s,length:%ld) => %d", stmt, n, sql, length, r);
  return r;
}

static inline int call_taos_stmt_set_tbname_tags(const char *file, int line, const char *func, TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags)
{
  LOGD_TAOS(file, line, func, "taos_stmt_set_tbname_tags(stmt:%p,name:%s,tags:%p) ...", stmt, name, tags);
  int r = taos_stmt_set_tbname_tags(stmt, name, tags);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_set_tbname_tags(stmt:%p,name:%s,tags:%p) => %d", stmt, name, tags, r);
  return r;
}

static inline int call_taos_stmt_set_tbname(const char *file, int line, const char *func, TAOS_STMT *stmt, const char *name)
{
  LOGD_TAOS(file, line, func, "taos_stmt_set_tbname(stmt:%p,name:%s) ...", stmt, name);
  int r = taos_stmt_set_tbname(stmt, name);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_set_tbname(stmt:%p,name:%s) => %d", stmt, name, r);
  return r;
}

static inline int call_taos_stmt_set_tags(const char *file, int line, const char *func, TAOS_STMT *stmt, TAOS_MULTI_BIND *tags)
{
  LOGD_TAOS(file, line, func, "taos_stmt_set_tags(stmt:%p,tags:%p) ...", stmt, tags);
  int r = taos_stmt_set_tags(stmt, tags);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_set_tags(stmt:%p,tags:%p) => %d", stmt, tags, r);
  return r;
}

static inline int call_taos_stmt_set_sub_tbname(const char *file, int line, const char *func, TAOS_STMT *stmt, const char *name)
{
  LOGD_TAOS(file, line, func, "taos_stmt_set_sub_tbname(stmt:%p,name:%s) ...", stmt, name);
  int r = taos_stmt_set_sub_tbname(stmt, name);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_set_sub_tbname(stmt:%p,name:%s) => %d", stmt, name, r);
  return r;
}

static inline int call_taos_stmt_get_tag_fields(const char *file, int line, const char *func, TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields)
{
  LOGD_TAOS(file, line, func, "taos_stmt_get_tag_fields(stmt:%p,fieldNum:%p,fields:%p) ...", stmt, fieldNum, fields);
  int r = taos_stmt_get_tag_fields(stmt, fieldNum, fields);
  if (r) diag_stmt(stmt);
  int n = fieldNum ? *fieldNum : 0;
  TAOS_FIELD_E *p = fields ? *fields : NULL;
  LOGD_TAOS(file, line, func, "taos_stmt_get_tag_fields(stmt:%p,fieldNum:%p(%d),fields:%p(%p)) => %d", stmt, fieldNum, n, fields, p, r);
  return r;
}

static inline int call_taos_stmt_get_col_fields(const char *file, int line, const char *func, TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields)
{
  LOGD_TAOS(file, line, func, "taos_stmt_get_col_fields(stmt:%p,fieldNum:%p,fields:%p) ...", stmt, fieldNum, fields);
  int r = taos_stmt_get_col_fields(stmt, fieldNum, fields);
  if (r) diag_stmt(stmt);
  int n = fieldNum ? *fieldNum : 0;
  TAOS_FIELD_E *p = fields ? *fields : NULL;
  LOGD_TAOS(file, line, func, "taos_stmt_get_col_fields(stmt:%p,fieldNum:%p(%d),fields:%p(%p)) => %d", stmt, fieldNum, n, fields, p, r);
  return r;
}

static inline void call_taos_stmt_reclaim_fields(const char *file, int line, const char *func, TAOS_STMT *stmt, TAOS_FIELD_E *fields)
{
  LOGD_TAOS(file, line, func, "taos_stmt_reclaim_fields(stmt:%p,fields:%p) ...", stmt, fields);
  taos_stmt_reclaim_fields(stmt, fields);
  LOGD_TAOS(file, line, func, "taos_stmt_reclaim_fields(stmt:%p,fields:%p) => void", stmt, fields);
}

static inline int call_taos_stmt_is_insert(const char *file, int line, const char *func, TAOS_STMT *stmt, int *insert)
{
  LOGD_TAOS(file, line, func, "taos_stmt_is_insert(stmt:%p,insert:%p) ...", stmt, insert);
  int r = taos_stmt_is_insert(stmt, insert);
  if (r) diag_stmt(stmt);
  int n = insert ? *insert : 0;
  LOGD_TAOS(file, line, func, "taos_stmt_is_insert(stmt:%p,insert:%p(%d)) => %d", stmt, insert, n, r);
  return r;
}

static inline int call_taos_stmt_num_params(const char *file, int line, const char *func, TAOS_STMT *stmt, int *nums)
{
  LOGD_TAOS(file, line, func, "taos_stmt_num_params(stmt:%p,nums:%p) ...", stmt, nums);
  int r = taos_stmt_num_params(stmt, nums);
  if (r) diag_stmt(stmt);
  int n = nums ? *nums : 0;
  LOGD_TAOS(file, line, func, "taos_stmt_num_params(stmt:%p,nums:%p(%d)) => %d", stmt, nums, n, r);
  return r;
}

static inline int call_taos_stmt_get_param(const char *file, int line, const char *func, TAOS_STMT *stmt, int idx, int *type, int *bytes)
{
  if (type) *type = 0;
  if (bytes) *bytes = 0;
  LOGD_TAOS(file, line, func, "taos_stmt_get_param(stmt:%p,idx:%d,type:%p,bytes:%p) ...", stmt, idx, type, bytes);
  int r = taos_stmt_get_param(stmt, idx, type, bytes);
  if (r) diag_stmt(stmt);
  int a = type ? *type : 0;
  int b = bytes ? *bytes : 0;
  LOGD_TAOS(file, line, func, "taos_stmt_get_param(stmt:%p,idx:%d,type:%p(%d),bytes:%p(%d)) => %d", stmt, idx, type, a, bytes, b, r);
  return r;
}

static inline int call_taos_stmt_bind_param(const char *file, int line, const char *func, TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)
{
  LOGD_TAOS(file, line, func, "taos_stmt_bind_param(stmt:%p,bind:%p) ...", stmt, bind);
  int r = taos_stmt_bind_param(stmt, bind);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_bind_param(stmt:%p,bind:%p) => %d", stmt, bind, r);
  return r;
}

static inline int call_taos_stmt_bind_param_batch(const char *file, int line, const char *func, TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)
{
  LOGD_TAOS(file, line, func, "taos_stmt_bind_param_batch(stmt:%p,bind:%p) ...", stmt, bind);
  int r = taos_stmt_bind_param_batch(stmt, bind);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_bind_param_batch(stmt:%p,bind:%p) => %d", stmt, bind, r);
  return r;
}

static inline int call_taos_stmt_bind_single_param_batch(const char *file, int line, const char *func, TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx)
{
  LOGD_TAOS(file, line, func, "taos_stmt_bind_single_param_batch(stmt:%p,bind:%p,colIdx:%d) ...", stmt, bind, colIdx);
  int r = taos_stmt_bind_single_param_batch(stmt, bind, colIdx);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_bind_single_param_batch(stmt:%p,bind:%p,colIdx:%d) => %d", stmt, bind, colIdx, r);
  return r;
}

static inline int call_taos_stmt_add_batch(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  LOGD_TAOS(file, line, func, "taos_stmt_add_batch(stmt:%p) ...", stmt);
  int r = taos_stmt_add_batch(stmt);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_add_batch(stmt:%p) => %d", stmt, r);
  return r;
}

static inline int call_taos_stmt_execute(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  LOGD_TAOS(file, line, func, "taos_stmt_execute(stmt:%p) ...", stmt);
  int r = taos_stmt_execute(stmt);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_execute(stmt:%p) => %d", stmt, r);
  return r;
}

static inline TAOS_RES* call_taos_stmt_use_result(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  LOGD_TAOS(file, line, func, "taos_stmt_use_result(stmt:%p) ...", stmt);
  TAOS_RES *res = taos_stmt_use_result(stmt);
  diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_use_result(stmt:%p) => %p", stmt, res);
  return res;
}

static inline int call_taos_stmt_close(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  LOGD_TAOS(file, line, func, "taos_stmt_close(stmt:%p) ...", stmt);
  int r = taos_stmt_close(stmt);
  if (r) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_stmt_close(stmt:%p) => %d", stmt, r);
  return r;
}

static inline char* call_taos_stmt_errstr(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  (void)file;
  (void)line;
  (void)func;
  return taos_stmt_errstr(stmt);
}

static inline int call_taos_stmt_affected_rows(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  LOGD_TAOS(file, line, func, "taos_stmt_affected_rows(stmt:%p) ...", stmt);
  int r = taos_stmt_affected_rows(stmt);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_affected_rows(stmt:%p) => %d", stmt, r);
  return r;
}

static inline int call_taos_stmt_affected_rows_once(const char *file, int line, const char *func, TAOS_STMT *stmt)
{
  LOGD_TAOS(file, line, func, "taos_stmt_affected_rows_once(stmt:%p) ...", stmt);
  int r = taos_stmt_affected_rows_once(stmt);
  if (r) diag_stmt(stmt);
  LOGD_TAOS(file, line, func, "taos_stmt_affected_rows_once(stmt:%p) => %d", stmt, r);
  return r;
}

static inline TAOS_RES* call_taos_query(const char *file, int line, const char *func, TAOS *taos, const char *sql)
{
  LOGD_TAOS(file, line, func, "taos_query(taos:%p,sql:%s) ...", taos, sql);
  TAOS_RES *res = taos_query(taos, sql);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_query(taos:%p,sql:%s) => %p", taos, sql, res);
  return res;
}

static inline TAOS_ROW call_taos_fetch_row(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_fetch_row(res:%p) ...", res);
  TAOS_ROW row = taos_fetch_row(res);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_fetch_row(res:%p) => %p", res, row);
  return row;
}

static inline int call_taos_result_precision(const char *file, int line, const char *func, TAOS_RES *res)  // get the time precision of result
{
  LOGD_TAOS(file, line, func, "taos_result_precision(res:%p) ...", res);
  int r = taos_result_precision(res);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_result_precision(res:%p) => %d", res, r);
  return r;
}

static inline void call_taos_free_result(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_free_result(res:%p) ...", res);
  taos_free_result(res);
  if (0) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_free_result(res:%p) => void", res);
}

static inline void call_taos_kill_query(const char *file, int line, const char *func, TAOS *taos)
{
  LOGD_TAOS(file, line, func, "taos_kill_query(taos:%p) ...", taos);
  taos_kill_query(taos);
  diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_kill_query(taos:%p) => void", taos);
}

static inline int call_taos_field_count(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_field_count(res:%p) ...", res);
  int r = taos_field_count(res);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_field_count(res:%p) => %d", res, r);
  return r;
}

static inline int call_taos_affected_rows(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_affected_rows(res:%p) ...", res);
  int r = taos_affected_rows(res);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_affected_rows(res:%p) => %d", res, r);
  return r;
}

static inline TAOS_FIELD* call_taos_fetch_fields(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_fetch_fields(res:%p) ...", res);
  TAOS_FIELD *p = taos_fetch_fields(res);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_fetch_fields(res:%p) => %p", res, p);
  return p;
}

static inline int call_taos_select_db(const char *file, int line, const char *func, TAOS *taos, const char *db)
{
  LOGD_TAOS(file, line, func, "taos_select_db(taos:%p,db:%s) ...", taos, db);
  int r = taos_select_db(taos, db);
  if (r) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_select_db(taos:%p,db:%s) => %d", taos, db, r);
  return r;
}

static inline int call_taos_print_row(const char *file, int line, const char *func, char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields)
{
  (void)file;
  (void)line;
  (void)func;
  return taos_print_row(str, row, fields, num_fields);
}

static inline void call_taos_stop_query(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_stop_query(res:%p) ...", res);
  taos_stop_query(res);
  diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_stop_query(res:%p) => void", res);
}

static inline bool call_taos_is_null(const char *file, int line, const char *func, TAOS_RES *res, int32_t row, int32_t col)
{
  LOGD_TAOS(file, line, func, "taos_is_null(res:%p,row:%d,col:%d) ...", res, row, col);
  bool b = taos_is_null(res, row, col);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_is_null(res:%p,row:%d,col:%d) => %s", res, row, col, b ? "true" : "false");
  return b;
}

static inline bool call_taos_is_update_query(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_is_update_query(res:%p) ...", res);
  bool b = taos_is_update_query(res);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_is_update_query(res:%p) => %s", res, b ? "true" : "false");
  return b;
}

static inline int call_taos_fetch_block(const char *file, int line, const char *func, TAOS_RES *res, TAOS_ROW *rows)
{
  LOGD_TAOS(file, line, func, "taos_fetch_block(res:%p,rows:%p) ...", res, rows);
  int r = taos_fetch_block(res, rows);
  if (r) diag_res(res);
  TAOS_ROW p = rows ? *rows : NULL;
  LOGD_TAOS(file, line, func, "taos_fetch_block(res:%p,rows:%p(%p)) => %d", res, rows, p, r);
  return r;
}

static inline int call_taos_fetch_block_s(const char *file, int line, const char *func, TAOS_RES *res, int *numOfRows, TAOS_ROW *rows)
{
  LOGD_TAOS(file, line, func, "taos_fetch_block_s(res:%p,rnumOfRows:%p,rows:%p) ...", res, numOfRows, rows);
  int r = taos_fetch_block_s(res, numOfRows, rows);
  if (r) diag_res(res);
  int n = numOfRows ? *numOfRows : 0;
  TAOS_ROW p = rows ? *rows : NULL;
  LOGD_TAOS(file, line, func, "taos_fetch_block_s(res:%p,rnumOfRows:%p(%d),rows:%p(%p)) => %d", res, numOfRows, n, rows, p, r);
  return r;
}

static inline int call_taos_fetch_raw_block(const char *file, int line, const char *func, TAOS_RES *res, int *numOfRows, void **pData)
{
  LOGD_TAOS(file, line, func, "taos_fetch_raw_block(res:%p,rnumOfRows:%p,pData:%p) ...", res, numOfRows, pData);
  int r = taos_fetch_raw_block(res, numOfRows, pData);
  if (r) diag_res(res);
  int n = numOfRows ? *numOfRows : 0;
  void *p = pData ? *pData : NULL;
  LOGD_TAOS(file, line, func, "taos_fetch_raw_block(res:%p,rnumOfRows:%p(%d),pData:%p(%p)) => %d", res, numOfRows, n, pData, p, r);
  return r;
}

static inline int* call_taos_get_column_data_offset(const char *file, int line, const char *func, TAOS_RES *res, int columnIndex)
{
  LOGD_TAOS(file, line, func, "taos_get_column_data_offset(res:%p,columnIndex:%d) ...", res, columnIndex);
  int *p = taos_get_column_data_offset(res, columnIndex);
  if (!p) diag_res(res);
  LOGD_TAOS(file, line, func, "taos_get_column_data_offset(res:%p,columnIndex:%d) => %p", res, columnIndex, p);
  return p;
}

static inline int call_taos_validate_sql(const char *file, int line, const char *func, TAOS *taos, const char *sql)
{
  LOGD_TAOS(file, line, func, "taos_validate_sql(taos:%p,sql:%s) ...", taos, sql);
  int r = taos_validate_sql(taos, sql);
  if (r) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_validate_sql(taos:%p,sql:%s) => %d", taos, sql, r);
  return r;
}

static inline void call_taos_reset_current_db(const char *file, int line, const char *func, TAOS *taos)
{
  LOGD_TAOS(file, line, func, "taos_reset_current_db(taos:%p) ...", taos);
  taos_reset_current_db(taos);
  LOGD_TAOS(file, line, func, "taos_reset_current_db(taos:%p) => void", taos);
}

static inline int* call_taos_fetch_lengths(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_fetch_lengths(res:%p) ...", res);
  int *p = taos_fetch_lengths(res);
  if (!p) diag_res(res);
  LOGD_TAOS(file, line, func, "taos_fetch_lengths(res:%p) => %p", res, p);
  return p;
}

static inline TAOS_ROW* call_taos_result_block(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_result_block(res:%p) ...", res);
  TAOS_ROW *p = taos_result_block(res);
  if (!p) diag_res(res);
  LOGD_TAOS(file, line, func, "taos_result_block(res:%p) => %p", res, p);
  return p;
}

static inline const char* call_taos_get_server_info(const char *file, int line, const char *func, TAOS *taos)
{
  LOGD_TAOS(file, line, func, "taos_get_server_info(taos:%p) ...", taos);
  const char *s = taos_get_server_info(taos);
  if (!s) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_get_server_info(taos:%p) => %s", taos, s);
  return s;
}

static inline const char* call_taos_get_client_info(const char *file, int line, const char *func)
{
  LOGD_TAOS(file, line, func, "taos_get_client_info() ...");
  const char *s = taos_get_client_info();
  if (!s) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_get_client_info() => %s", s);
  return s;
}

static inline const char* call_taos_errstr(const char *file, int line, const char *func, TAOS_RES *res)
{
  (void)file;
  (void)line;
  (void)func;
  return taos_errstr(res);
}

static inline int call_taos_errno(const char *file, int line, const char *func, TAOS_RES *res)
{
  (void)file;
  (void)line;
  (void)func;
  return taos_errno(res);
}

static inline void call_taos_query_a(const char *file, int line, const char *func, TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param)
{
  LOGD_TAOS(file, line, func, "taos_query_a(taos:%p,sql:%s,fp:%p,param:%p) ...", taos, sql, fp, param);
  taos_query_a(taos, sql, fp, param);
  diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_query_a(taos:%p,sql:%s,fp:%p,param:%p) => void", taos, sql, fp, param);
}

static inline void call_taos_fetch_rows_a(const char *file, int line, const char *func, TAOS_RES *res, __taos_async_fn_t fp, void *param)
{
  LOGD_TAOS(file, line, func, "taos_fetch_rows_a(res:%p,fp:%p,param:%p) ...", res, fp, param);
  taos_fetch_rows_a(res, fp, param);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_fetch_rows_a(res:%p,fp:%p,param:%p) => void", res, fp, param);
}

static inline void call_taos_fetch_raw_block_a(const char *file, int line, const char *func, TAOS_RES *res, __taos_async_fn_t fp, void *param)
{
  LOGD_TAOS(file, line, func, "taos_fetch_raw_block_a(res:%p,fp:%p,param:%p) ...", res, fp, param);
  taos_fetch_raw_block_a(res, fp, param);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_fetch_raw_block_a(res:%p,fp:%p,param:%p) => void", res, fp, param);
}

static inline const void* call_taos_get_raw_block(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "taos_get_raw_block(res:%p) ...", res);
  const void *p = taos_get_raw_block(res);
  if (!p) diag_res(res);
  LOGD_TAOS(file, line, func, "taos_get_raw_block(res:%p) => %p", res, p);
  return p;
}

static inline int call_taos_load_table_info(const char *file, int line, const char *func, TAOS *taos, const char *tableNameList)
{
  LOGD_TAOS(file, line, func, "taos_load_table_info(taos:%p,tableNameList:%s) ...", taos, tableNameList);
  int r = taos_load_table_info(taos, tableNameList);
  diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_load_table_info(taos:%p,tableNameList:%s) => %d", taos, tableNameList, r);
  return r;
}

static inline TAOS_RES* call_taos_schemaless_insert(const char *file, int line, const char *func, TAOS *taos, char *lines[], int numLines, int protocol, int precision)
{
  LOGD_TAOS(file, line, func, "taos_schemaless_insert(taos:%p,lines:%p,numLines:%d,protocol:%d,precision:%d) ...", taos, lines, numLines, protocol, precision);
  TAOS_RES *res = taos_schemaless_insert(taos, lines, numLines, protocol, precision);
  diag_res(res);
  LOGD_TAOS(file, line, func, "taos_schemaless_insert(taos:%p,lines:%p,numLines:%d,protocol:%d,precision:%d) => %p", taos, lines, numLines, protocol, precision, res);
  return res;
}

static inline tmq_list_t* call_tmq_list_new(const char *file, int line, const char *func)
{
  LOGD_TAOS(file, line, func, "tmq_list_new() ...");
  tmq_list_t *r = tmq_list_new();
  LOGD_TAOS(file, line, func, "tmq_list_new() => %p", r);
  return r;
}

static inline int32_t     call_tmq_list_append(const char *file, int line, const char *func, tmq_list_t *topic_list, const char *name)
{
  LOGD_TAOS(file, line, func, "tmq_list_append(topic_list:%p, name:%s) ...", topic_list, name);
  int32_t r = tmq_list_append(topic_list, name);
  LOGD_TAOS(file, line, func, "tmq_list_append(topic_list:%p, name:%s) => %d", topic_list, name, r);
  return r;
}

static inline void        call_tmq_list_destroy(const char *file, int line, const char *func, tmq_list_t *topic_list)
{
  LOGD_TAOS(file, line, func, "tmq_list_destroy(topic_list:%p) ...", topic_list);
  tmq_list_destroy(topic_list);
  LOGD_TAOS(file, line, func, "tmq_list_destroy(topic_list:%p) => void", topic_list);
}

static inline int32_t     call_tmq_list_get_size(const char *file, int line, const char *func, const tmq_list_t *topic_list)
{
  LOGD_TAOS(file, line, func, "tmq_list_get_size(topic_list:%p) ...", topic_list);
  int32_t r = tmq_list_get_size(topic_list);
  LOGD_TAOS(file, line, func, "tmq_list_get_size(topic_list:%p) => %d", topic_list, r);
  return r;
}

static inline char**      call_tmq_list_to_c_array(const char *file, int line, const char *func, const tmq_list_t *topic_list)
{
  LOGD_TAOS(file, line, func, "tmq_list_to_c_array(topic_list:%p) ...", topic_list);
  char **r = tmq_list_to_c_array(topic_list);
  LOGD_TAOS(file, line, func, "tmq_list_to_c_array(topic_list:%p) => %p", topic_list, r);
  return r;
}

static inline tmq_t*      call_tmq_consumer_new(const char *file, int line, const char *func, tmq_conf_t *conf, char *errstr, int32_t errstrLen)
{
  LOGD_TAOS(file, line, func, "tmq_consumer_new(conf:%p,errstr:%p,errstrLen:%d) ...", conf, errstr, errstrLen);
  tmq_t *r = tmq_consumer_new(conf, errstr, errstrLen);
  LOGD_TAOS(file, line, func, "tmq_consumer_new(conf:%p,errstr:%p[%.*s],errstrLen:%d) => %p", conf, errstr, errstrLen, errstr, errstrLen, r);
  return r;
}

static inline const char* call_tmq_err2str(const char *file, int line, const char *func, int32_t code)
{
  (void)file;
  (void)line;
  (void)func;
  return tmq_err2str(code);
}

static inline int32_t   call_tmq_subscribe(const char *file, int line, const char *func, tmq_t *tmq, const tmq_list_t *topic_list)
{
  LOGD_TAOS(file, line, func, "tmq_subscribe(tmq:%p,topic_list:%p) ...", tmq, topic_list);
  int32_t r = tmq_subscribe(tmq, topic_list);
  LOGD_TAOS(file, line, func, "tmq_subscribe(tmq:%p,topic_list:%p) => %d", tmq, topic_list, r);
  return r;
}

static inline int32_t   call_tmq_unsubscribe(const char *file, int line, const char *func, tmq_t *tmq)
{
  LOGD_TAOS(file, line, func, "tmq_unsubscribe(tmq:%p) ...", tmq);
  int32_t r = tmq_unsubscribe(tmq);
  LOGD_TAOS(file, line, func, "tmq_unsubscribe(tmq:%p) => %d", tmq, r);
  return r;
}

static inline int32_t   call_tmq_subscription(const char *file, int line, const char *func, tmq_t *tmq, tmq_list_t **topics)
{
  LOGD_TAOS(file, line, func, "tmq_subscription(tmq:%p,topics:%p) ...", tmq, topics);
  int32_t r = tmq_subscription(tmq, topics);
  LOGD_TAOS(file, line, func, "tmq_subscription(tmq:%p,topics:%p) => %d", tmq, topics, r);
  return r;
}

static inline TAOS_RES* call_tmq_consumer_poll(const char *file, int line, const char *func, tmq_t *tmq, int64_t timeout)
{
  LOGD_TAOS(file, line, func, "tmq_consumer_poll(tmq:%p,timeout:%" PRId64 ") ...", tmq, timeout);
  TAOS_RES *r = tmq_consumer_poll(tmq, timeout);
  LOGD_TAOS(file, line, func, "tmq_consumer_poll(tmq:%p,timeout:%" PRId64 ") => %p", tmq, timeout, r);
  return r;
}

static inline int32_t   call_tmq_consumer_close(const char *file, int line, const char *func, tmq_t *tmq)
{
  LOGD_TAOS(file, line, func, "tmq_consumer_close(tmq:%p) ...", tmq);
  int32_t r = tmq_consumer_close(tmq);
  LOGD_TAOS(file, line, func, "tmq_consumer_close(tmq:%p) => %d", tmq, r);
  return r;
}

static inline int32_t   call_tmq_commit_sync(const char *file, int line, const char *func, tmq_t *tmq, const TAOS_RES *msg)
{
  LOGD_TAOS(file, line, func, "tmq_commit_sync(tmq:%p,msg:%p) ...", tmq, msg);
  int32_t r = tmq_commit_sync(tmq, msg);
  LOGD_TAOS(file, line, func, "tmq_commit_sync(tmq:%p,msg:%p) => %d", tmq, msg, r);
  return r;
}

static inline void      call_tmq_commit_async(const char *file, int line, const char *func, tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param)
{
  LOGD_TAOS(file, line, func, "tmq_commit_async(tmq:%p,msg:%p,cb:%p,param:%p) ...", tmq, msg, cb, param);
  tmq_commit_async(tmq, msg, cb, param);
  LOGD_TAOS(file, line, func, "tmq_commit_async(tmq:%p,msg:%p,cb:%p,param:%p) => void", tmq, msg, cb, param);
}

static inline tmq_conf_t*    call_tmq_conf_new(const char *file, int line, const char *func)
{
  LOGD_TAOS(file, line, func, "tmq_conf_new() ...");
  tmq_conf_t *r = tmq_conf_new();
  LOGD_TAOS(file, line, func, "tmq_conf_new() => %p", r);
  return r;
}

static inline tmq_conf_res_t call_tmq_conf_set(const char *file, int line, const char *func, tmq_conf_t *conf, const char *key, const char *value)
{
  LOGD_TAOS(file, line, func, "tmq_conf_set(conf:%p,key:%s,value:%s) ...", conf, key, value);
  tmq_conf_res_t r = tmq_conf_set(conf, key, value);
  LOGD_TAOS(file, line, func, "tmq_conf_set(conf:%p,key:%s,value:%s) => %d", conf, key, value, r);
  return r;
}

static inline void           call_tmq_conf_destroy(const char *file, int line, const char *func, tmq_conf_t *conf)
{
  LOGD_TAOS(file, line, func, "tmq_conf_destroy(conf:%p) ...", conf);
  tmq_conf_destroy(conf);
  LOGD_TAOS(file, line, func, "tmq_conf_destroy(conf:%p) => void", conf);
}

static inline void           call_tmq_conf_set_auto_commit_cb(const char *file, int line, const char *func, tmq_conf_t *conf, tmq_commit_cb *cb, void *param)
{
  LOGD_TAOS(file, line, func, "tmq_conf_set_auto_commit_cb(conf:%p,cb:%p,param:%p) ...", conf, cb, param);
  tmq_conf_set_auto_commit_cb(conf, cb, param);
  LOGD_TAOS(file, line, func, "tmq_conf_set_auto_commit_cb(conf:%p,cb:%p,param:%p) => void", conf, cb, param);
}

static inline const char* call_tmq_get_topic_name(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "tmq_get_topic_name(res:%p) ...", res);
  const char *r = tmq_get_topic_name(res);
  LOGD_TAOS(file, line, func, "tmq_get_topic_name(res:%p) => %s", res, r);
  return r;
}

static inline const char* call_tmq_get_db_name(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "tmq_get_db_name(res:%p) ...", res);
  const char *r = tmq_get_db_name(res);
  LOGD_TAOS(file, line, func, "tmq_get_db_name(res:%p) => %s", res, r);
  return r;
}

static inline int32_t     call_tmq_get_vgroup_id(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "tmq_get_vgroup_id(res:%p) ...", res);
  int32_t r = tmq_get_vgroup_id(res);
  LOGD_TAOS(file, line, func, "tmq_get_vgroup_id(res:%p) => %d", res, r);
  return r;
}

static inline const char* call_tmq_get_table_name(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "tmq_get_table_name(res:%p) ...", res);
  const char *r = tmq_get_table_name(res);
  LOGD_TAOS(file, line, func, "tmq_get_table_name(res:%p) => %s", res, r);
  return r;
}

static inline tmq_res_t   call_tmq_get_res_type(const char *file, int line, const char *func, TAOS_RES *res)
{
  LOGD_TAOS(file, line, func, "tmq_get_res_type(res:%p) ...", res);
  tmq_res_t r = tmq_get_res_type(res);
  LOGD_TAOS(file, line, func, "tmq_get_res_type(res:%p) => %d", res, r);
  return r;
}

/*
static inline int32_t     call_tmq_get_raw(const char *file, int line, const char *func, TAOS_RES *res, tmq_raw_data *raw);
static inline int32_t     call_tmq_write_raw(const char *file, int line, const char *func, TAOS *taos, tmq_raw_data raw);
static inline int         call_taos_write_raw_block(const char *file, int line, const char *func, TAOS *taos, int numOfRows, char *pData, const char *tbname);
static inline void        call_tmq_free_raw(const char *file, int line, const char *func, tmq_raw_data raw);

static inline char* call_tmq_get_json_meta(const char *file, int line, const char *func, TAOS_RES *res);
static inline void  call_tmq_free_json_meta(const char *file, int line, const char *func, char *jsonMeta);
*/

static inline TSDB_SERVER_STATUS call_taos_check_server_status(const char *file, int line, const char *func, const char *fqdn, int port, char *details, int maxlen)
{
  LOGD_TAOS(file, line, func, "taos_check_server_status(fqdn:%s,port:%d,details:%p,maxlen:%d) ...", fqdn, port, details, maxlen);
  TSDB_SERVER_STATUS r = taos_check_server_status(fqdn, port, details, maxlen);
  LOGD_TAOS(file, line, func, "taos_check_server_status(fqdn:%s,port:%d,details:%p(%.*s),maxlen:%d) => %d", fqdn, port, details, maxlen, details, maxlen, r);
  return r;
}

static inline int call_taos_get_current_db(const char *file, int line, const char *func, TAOS *taos, char *database, int len, int *required)
{
  LOGD_TAOS(file, line, func, "taos_get_current_db(taos:%p,database:%p,len:%d,required:%p) ...", taos, database, len, required);
  int r = taos_get_current_db(taos, database, len, required);
  if (r) diag_res(NULL);
  LOGD_TAOS(file, line, func, "taos_get_current_db(taos:%p,database:%p(%.*s),len:%d,required:%p(%d)) => %d",
      taos, database, len, database, len, required, required ? *required : 0,
      r);
  return r;
}

#define CALL_taos_cleanup(...) call_taos_cleanup(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
// #define CALL_taos_options(...) call_taos_options(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_set_config(...) call_taos_set_config(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_init(...) call_taos_init(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_connect(...) call_taos_connect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_connect_auth(...) call_taos_connect_auth(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_close(...) call_taos_close(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_data_type(...) call_taos_data_type(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_stmt_init(...) call_taos_stmt_init(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_prepare(...) call_taos_stmt_prepare(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_set_tbname_tags(...) call_taos_stmt_set_tbname_tags(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_set_tbname(...) call_taos_stmt_set_tbname(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_set_tags(...) call_taos_stmt_set_tags(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_set_sub_tbname(...) call_taos_stmt_set_sub_tbname(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_get_tag_fields(...) call_taos_stmt_get_tag_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_get_col_fields(...) call_taos_stmt_get_col_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_reclaim_fields(...) call_taos_stmt_reclaim_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_stmt_is_insert(...) call_taos_stmt_is_insert(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_num_params(...) call_taos_stmt_num_params(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_get_param(...) call_taos_stmt_get_param(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_bind_param(...) call_taos_stmt_bind_param(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_bind_param_batch(...) call_taos_stmt_bind_param_batch(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_bind_single_param_batch(...) call_taos_stmt_bind_single_param_batch(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_add_batch(...) call_taos_stmt_add_batch(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_execute(...) call_taos_stmt_execute(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_use_result(...) call_taos_stmt_use_result(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_close(...) call_taos_stmt_close(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_errstr(...) call_taos_stmt_errstr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_affected_rows(...) call_taos_stmt_affected_rows(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stmt_affected_rows_once(...) call_taos_stmt_affected_rows_once(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_query(...) call_taos_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_fetch_row(...) call_taos_fetch_row(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_result_precision(...) call_taos_result_precision(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_free_result(...) call_taos_free_result(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_kill_query(...) call_taos_kill_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_field_count(...) call_taos_field_count(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_affected_rows(...) call_taos_affected_rows(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_fetch_fields(...) call_taos_fetch_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_select_db(...) call_taos_select_db(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_print_row(...) call_taos_print_row(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_stop_query(...) call_taos_stop_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_is_null(...) call_taos_is_null(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_is_update_query(...) call_taos_is_update_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_fetch_block(...) call_taos_fetch_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_fetch_block_s(...) call_taos_fetch_block_s(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_fetch_raw_block(...) call_taos_fetch_raw_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_get_column_data_offset(...) call_taos_get_column_data_offset(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_validate_sql(...) call_taos_validate_sql(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_reset_current_db(...) call_taos_reset_current_db(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_fetch_lengths(...) call_taos_fetch_lengths(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_result_block(...) call_taos_result_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_get_server_info(...) call_taos_get_server_info(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_get_client_info(...) call_taos_get_client_info(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_errstr(...) call_taos_errstr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_errno(...) call_taos_errno(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_query_a(...) call_taos_query_a(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_fetch_rows_a(...) call_taos_fetch_rows_a(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_fetch_raw_block_a(...) call_taos_fetch_raw_block_a(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_get_raw_block(...) call_taos_get_raw_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_load_table_info(...) call_taos_load_table_info(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_schemaless_insert(...) call_taos_schemaless_insert(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_list_new(...) call_tmq_list_new(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_list_append(...) call_tmq_list_append(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_list_destroy(...) call_tmq_list_destroy(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_list_get_size(...) call_tmq_list_get_size(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_list_to_c_array(...) call_tmq_list_to_c_array(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_consumer_new(...) call_tmq_consumer_new(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_err2str(...) call_tmq_err2str(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_subscribe(...) call_tmq_subscribe(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_unsubscribe(...) call_tmq_unsubscribe(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_subscription(...) call_tmq_subscription(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_consumer_poll(...) call_tmq_consumer_poll(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_consumer_close(...) call_tmq_consumer_close(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_commit_sync(...) call_tmq_commit_sync(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_commit_async(...) call_tmq_commit_async(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_conf_new(...) call_tmq_conf_new(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_conf_set(...) call_tmq_conf_set(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_conf_destroy(...) call_tmq_conf_destroy(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_conf_set_auto_commit_cb(...) call_tmq_conf_set_auto_commit_cb(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_get_topic_name(...) call_tmq_get_topic_name(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_get_db_name(...) call_tmq_get_db_name(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_get_vgroup_id(...) call_tmq_get_vgroup_id(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_get_table_name(...) call_tmq_get_table_name(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_get_res_type(...) call_tmq_get_res_type(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_get_raw(...) call_tmq_get_raw(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_write_raw(...) call_tmq_write_raw(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_taos_write_raw_block(...) call_taos_write_raw_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_free_raw(...) call_tmq_free_raw(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_tmq_get_json_meta(...) call_tmq_get_json_meta(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_tmq_free_json_meta(...) call_tmq_free_json_meta(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_check_server_status(...) call_taos_check_server_status(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define CALL_taos_get_current_db(...) call_taos_get_current_db(__FILE__, __LINE__, __func__, ##__VA_ARGS__)


EXTERN_C_BEGIN

typedef struct tsdb_data_s               tsdb_data_t;
struct tsdb_data_s {
  int8_t                type;
  union {
    uint8_t             b;
    int8_t              i8;
    uint8_t             u8;
    int16_t             i16;
    uint16_t            u16;
    int32_t             i32;
    uint32_t            u32;
    int64_t             i64;
    uint64_t            u64;
    float               flt;
    double              dbl;
    struct {
      const char *str;
      size_t      len;
      const char *encoder;
    }                   str;
    struct {
      int64_t     ts;
      int         precision;
    }                   ts;
    struct {
      const unsigned char *bin;
      size_t               len;
    }                   bin;
    struct {
      const unsigned char *geo;
      size_t               len;
    }                   geo;
  };

  uint8_t               is_null:1;
};

int helper_get_tsdb(TAOS_RES *res, int block, TAOS_FIELD *fields, int time_precision, TAOS_ROW rows, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len) FA_HIDDEN;

int helper_get_tsdb_ws(int time_precision, const char *name, uint8_t col_type, const void *col_data, uint32_t col_len, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len) FA_HIDDEN;

EXTERN_C_END

#endif // _taos_helpers_h_

