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

#ifndef _taosws_helpers_h_
#define _taosws_helpers_h_

#include "helpers.h"

#include <taos.h>
#include <taosws.h>

#include <string.h>
#include <taoserror.h>

#define LOGD_TAOSWS(file, line, func, fmt, ...) do {                                              \
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define LOGE_TAOSWS(file, line, func, fmt, ...) do {                                              \
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define diag_ws_res_impl(file, line, func, res) do {                 \
  WS_RES *__res  = res;                                              \
  int _e         = ws_errno(__res);                                  \
  const char *_s = ws_errstr(__res);                                 \
  if (!_s || !*_s) break;                                            \
  LOGE_TAOSWS(file, line, func, "ws_errno/str(res:%p) => [%d/0x%x]%s%s%s", __res, _e, _e, color_red(), _s, color_reset());     \
} while (0)

#define diag_ws_stmt_impl(file, line, func, stmt) do {                                    \
  WS_STMT *__stmt = stmt;                                                                 \
  int _e          = ws_errno(NULL);                                                       \
  const char *_s1 = ws_errstr(NULL);                                                      \
  const char *_s2 = ws_stmt_errstr(__stmt);                                               \
  if (!_s2 || !*_s2) break;                                                               \
  LOGE_TAOSWS(file, line, func,                                                           \
      "ws_errno/str(stmt:%p) => [%d/0x%x]%s%s%s;ws_stmt_errstr:%s%s%s",                   \
      __stmt, _e, _e, color_red(), _s1, color_reset(), color_red(), _s2, color_reset());  \
} while (0)

#define diag_ws_res(x_res)          diag_ws_res_impl(file, line, func, x_res)
#define diag_ws_stmt(x_stmt)        diag_ws_stmt_impl(file, line, func, x_stmt)

static inline int32_t call_ws_enable_log(const char *file, int line, const char *func, const char *log_level)
{
  LOGD_TAOSWS(file, line, func, "ws_enable_log(log_level:%s) ...", log_level);
  int32_t r = ws_enable_log(log_level);
  if (r) diag_ws_res(NULL);
  LOGD_TAOSWS(file, line, func, "ws_enable_log(log_level:%s) => %d", log_level, r);
  return r;
}

static inline WS_TAOS *call_ws_connect(const char *file, int line, const char *func, const char *dsn)
{
  LOGD_TAOSWS(file, line, func, "ws_connect(dsn:%s) ...", dsn);
  WS_TAOS *ws_taos = ws_connect(dsn);
  if (!ws_taos) diag_ws_res(NULL);
  LOGD_TAOSWS(file, line, func, "ws_connect(dsn:%s) => %p", dsn, ws_taos);
  return ws_taos;
}

static inline const char *call_ws_get_server_info(const char *file, int line, const char *func, WS_TAOS *taos)
{
  LOGD_TAOSWS(file, line, func, "ws_get_server_info(ws_taos:%p) ...", taos);
  const char *s = ws_get_server_info(taos);
  LOGD_TAOSWS(file, line, func, "ws_get_server_info(ws_taos:%p) => %s", taos, s);
  return s;
}

static inline const char *call_ws_get_client_info(const char *file, int line, const char *func)
{
  LOGD_TAOSWS(file, line, func, "ws_get_client_info() ...");
  const char *s = ws_get_client_info();
  LOGD_TAOSWS(file, line, func, "ws_get_client_info() => %s", s);
  return s;
}

static inline int call_ws_close(const char *file, int line, const char *func, WS_TAOS *taos)
{
  LOGD_TAOSWS(file, line, func, "ws_close(ws_taos:%p) ...", taos);
  int r = ws_close(taos);
  if (r) diag_ws_res(NULL);
  LOGD_TAOSWS(file, line, func, "ws_close(ws_taos:%p) => %d", taos, r);
  return r;
}

static inline WS_RES *call_ws_query(const char *file, int line, const char *func, WS_TAOS *taos, const char *sql)
{
  LOGD_TAOSWS(file, line, func, "ws_query(ws_taos:%p, sql:%s) ...", taos, sql);
  WS_RES *res = ws_query(taos, sql);
  diag_ws_res(res);
  LOGD_TAOSWS(file, line, func, "ws_query(ws_taos:%p, sql:%s) => %p", taos, sql, res);
  return res;
}

// static inline int32_t call_ws_stop_query(const char *file, int line, const char *func, WS_RES *rs)
// static inline WS_RES *call_ws_query_timeout(const char *file, int line, const char *func, WS_TAOS *taos, const char *sql, uint32_t seconds)
// static inline int64_t call_ws_take_timing(const char *file, int line, const char *func, WS_RES *rs)
static inline int32_t call_ws_errno(const char *file, int line, const char *func, WS_RES *rs)
{
  (void)file;
  (void)line;
  (void)func;
  return ws_errno(rs);
}

static inline const char *call_ws_errstr(const char *file, int line, const char *func, WS_RES *rs)
{
  (void)file;
  (void)line;
  (void)func;
  return ws_errstr(rs);
}

static inline int32_t call_ws_affected_rows(const char *file, int line, const char *func, const WS_RES *rs)
{
  LOGD_TAOSWS(file, line, func, "ws_affected_rows(rs:%p) ...", rs);
  int32_t r = ws_affected_rows(rs);
  diag_ws_res((WS_RES*)rs);
  LOGD_TAOSWS(file, line, func, "ws_affected_rows(rs:%p) => %d", rs, r);
  return r;
}

static inline int32_t call_ws_field_count(const char *file, int line, const char *func, const WS_RES *rs)
{
  LOGD_TAOSWS(file, line, func, "ws_field_count(rs:%p) ...", rs);
  int32_t r = ws_field_count(rs);
  diag_ws_res((WS_RES*)rs);
  LOGD_TAOSWS(file, line, func, "ws_field_count(rs:%p) => %d", rs, r);
  return r;
}

static inline bool call_ws_is_update_query(const char *file, int line, const char *func, const WS_RES *rs)
{
  LOGD_TAOSWS(file, line, func, "ws_is_update_query(rs:%p) ...", rs);
  bool r = ws_is_update_query(rs);
  diag_ws_res((WS_RES*)rs);
  LOGD_TAOSWS(file, line, func, "ws_is_update_query(rs:%p) => %s", rs, r ? "true" : "false");
  return r;
}

static inline const struct WS_FIELD *call_ws_fetch_fields(const char *file, int line, const char *func, WS_RES *rs)
{
  LOGD_TAOSWS(file, line, func, "ws_fetch_fields(rs:%p) ...", rs);
  const WS_FIELD *r = ws_fetch_fields(rs);
  diag_ws_res(rs);
  LOGD_TAOSWS(file, line, func, "ws_fetch_fields(rs:%p) => %p", rs, r);
  return r;
}

// static inline const struct WS_FIELD_V2 *call_ws_fetch_fields_v2(const char *file, int line, const char *func, WS_RES *rs)

static inline int32_t call_ws_fetch_raw_block(const char *file, int line, const char *func, WS_RES *rs, const void **ptr, int32_t *rows)
{
  LOGD_TAOSWS(file, line, func, "ws_fetch_raw_block(rs:%p, ptr:%p, rows:%p) ...", rs, ptr, rows);
  int32_t r = ws_fetch_raw_block(rs, ptr, rows);
  diag_ws_res(rs);
  LOGD_TAOSWS(file, line, func, "ws_fetch_raw_block(rs:%p, ptr:%p(%p), rows:%p(%d)) => %d",
      rs, ptr, ptr ? *ptr : NULL, rows, rows ? *rows : -1, r);
  return r;
}

static inline int32_t call_ws_free_result(const char *file, int line, const char *func, WS_RES *rs)
{
  LOGD_TAOSWS(file, line, func, "ws_free_result(rs:%p) ...", rs);
  int32_t r = ws_free_result(rs);
  if (r) diag_ws_res(NULL);
  LOGD_TAOSWS(file, line, func, "ws_free_result(rs:%p) => %d", rs, r);
  return r;
}

static inline int32_t call_ws_result_precision(const char *file, int line, const char *func, const WS_RES *rs)
{
  LOGD_TAOSWS(file, line, func, "ws_result_precision(rs:%p) ...", rs);
  int32_t r = ws_result_precision(rs);
  diag_ws_res((WS_RES*)rs);
  LOGD_TAOSWS(file, line, func, "ws_result_precision(rs:%p) => %d", rs, r);
  return r;
}

static inline const void *call_ws_get_value_in_block(const char *file, int line, const char *func, WS_RES *rs, int32_t row, int32_t col, uint8_t *ty, uint32_t *len)
{
  // NOTE: attentionally direct call without loggin
  (void)file;
  (void)line;
  (void)func;
  return ws_get_value_in_block(rs, row, col, ty, len);
}

// static inline void call_ws_timestamp_to_rfc3339(const char *file, int line, const char *func, uint8_t *dest, int64_t raw, int32_t precision, bool use_z)

static inline WS_STMT *call_ws_stmt_init(const char *file, int line, const char *func, const WS_TAOS *taos)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_init(ws_taos:%p) ...", taos);
  WS_STMT *r = ws_stmt_init(taos);
  diag_ws_stmt(r);
  LOGD_TAOSWS(file, line, func, "ws_stmt_init(ws_taos:%p) => %p", taos, r);
  return r;
}

static inline int call_ws_stmt_prepare(const char *file, int line, const char *func, WS_STMT *stmt, const char *sql, unsigned long len)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_prepare(stmt:%p,sql:%.*s,len:%d) ...", stmt, (int)len, sql, (int)len);
  int r = ws_stmt_prepare(stmt, sql, len);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_prepare(stmt:%p) => %d", stmt, r);
  return r;
}

static inline int call_ws_stmt_set_tbname(const char *file, int line, const char *func, WS_STMT *stmt, const char *name)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_set_tbname(stmt:%p,name:%s) ...", stmt, name);
  int r = ws_stmt_set_tbname(stmt, name);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_set_tbname(stmt:%p,name:%s) => %d", stmt, name, r);
  return r;
}

static inline int call_ws_stmt_set_tbname_tags(const char *file, int line, const char *func, WS_STMT *stmt, const char *name, const WS_MULTI_BIND *bind, uint32_t len)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_set_tbname_tags(stmt:%p,name:%s,bind:%p,len:%u) ...", stmt, name, bind, len);
  int r = ws_stmt_set_tbname_tags(stmt, name, bind, len);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_set_tbname_tags(stmt:%p,name:%s,bind:%p,len:%u) => %d", stmt, name, bind, len, r);
  return r;
}

static inline int call_ws_stmt_get_tag_fields(const char *file, int line, const char *func, WS_STMT *stmt, int *fieldNum, struct StmtField **fields)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_get_tag_fields(stmt:%p,fieldNum:%p,fields:%p) ...", stmt, fieldNum, fields);
  int r = ws_stmt_get_tag_fields(stmt, fieldNum, fields);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_get_tag_fields(stmt:%p,fieldNum:%p(%d),fields:%p(%p)) => %d",
      stmt,
      fieldNum, fieldNum ? *fieldNum : -1,
      fields, fields ? *fields : NULL,
      r);
  return r;
}

static inline int call_ws_stmt_get_col_fields(const char *file, int line, const char *func, WS_STMT *stmt, int *fieldNum, struct StmtField **fields)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_get_col_fields(stmt:%p,fieldNum:%p,fields:%p) ...", stmt, fieldNum, fields);
  int r = ws_stmt_get_col_fields(stmt, fieldNum, fields);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_get_col_fields(stmt:%p,fieldNum:%p(%d),fields:%p(%p)) => %d",
      stmt,
      fieldNum, fieldNum ? *fieldNum : -1,
      fields, fields ? *fields : NULL,
      r);
  return r;
}

static inline int call_ws_stmt_reclaim_fields(const char *file, int line, const char *func, WS_STMT *stmt, struct StmtField **fields, int fieldNum)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_reclaim_fields(stmt:%p,fields:%p,fieldNum:%d) ...", stmt, fields, fieldNum);
  int r = ws_stmt_reclaim_fields(stmt, fields, fieldNum);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_reclaim_fields(stmt:%p,fields:%p,fieldNum:(%d)) => %d", stmt, fields, fieldNum, r);
  return r;
}

static inline int call_ws_stmt_is_insert(const char *file, int line, const char *func, WS_STMT *stmt, int *insert)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_is_insert(stmt:%p,insert:%p) ...", stmt, insert);
  int r = ws_stmt_is_insert(stmt, insert);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_is_insert(stmt:%p,insert:%p(%d)) => %d", stmt, insert, insert ? *insert : -1, r);
  return r;
}

static inline int call_ws_stmt_set_tags(const char *file, int line, const char *func, WS_STMT *stmt, const WS_MULTI_BIND *bind, uint32_t len)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_set_tags(stmt:%p,bind:%p,len:%u) ...", stmt, bind, len);
  int r = ws_stmt_set_tags(stmt, bind, len);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_set_tags(stmt:%p,bind:%p,len:%u) => %d", stmt, bind, len, r);
  return r;
}

static inline int call_ws_stmt_bind_param_batch(const char *file, int line, const char *func, WS_STMT *stmt, const WS_MULTI_BIND *bind, uint32_t len)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_bind_param_batch(stmt:%p,bind:%p,len:%u) ...", stmt, bind, len);
  int r = ws_stmt_bind_param_batch(stmt, bind, len);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_bind_param_batch(stmt:%p,bind:%p,len:%u) => %d", stmt, bind, len, r);
  return r;
}

static inline int call_ws_stmt_add_batch(const char *file, int line, const char *func, WS_STMT *stmt)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_add_batch(stmt:%p) ...", stmt);
  int r = ws_stmt_add_batch(stmt);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_add_batch(stmt:%p) => %d", stmt, r);
  return r;
}

static inline int call_ws_stmt_execute(const char *file, int line, const char *func, WS_STMT *stmt, int32_t *affected_rows)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_execute(stmt:%p, affected_rows:%p) ...", stmt, affected_rows);
  int r = ws_stmt_execute(stmt, affected_rows);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_execute(stmt:%p, affected_rows:%p(%d)) => %d", stmt, affected_rows, affected_rows ? *affected_rows : -1, r);
  return r;
}

static inline int call_ws_stmt_affected_rows(const char *file, int line, const char *func, WS_STMT *stmt)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_affected_rows(stmt:%p) ...", stmt);
  int r = ws_stmt_affected_rows(stmt);
  if (r <= 0) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_affected_rows(stmt:%p) => %d", stmt, r);
  return r;
}

static inline const char *call_ws_stmt_errstr(const char *file, int line, const char *func, WS_STMT *stmt)
{
  (void)file;
  (void)line;
  (void)func;
  return ws_stmt_errstr(stmt);
}

static inline int32_t call_ws_stmt_close(const char *file, int line, const char *func, WS_STMT *stmt)
{
  LOGD_TAOSWS(file, line, func, "ws_stmt_close(stmt:%p) ...", stmt);
  int32_t r = ws_stmt_close(stmt);
  if (r) diag_ws_stmt(stmt);
  LOGD_TAOSWS(file, line, func, "ws_stmt_close(stmt:%p) => %d", stmt, r);
  return r;
}



#define CALL_ws_enable_log(...)                          call_ws_enable_log(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_connect(...)                             call_ws_connect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_get_server_info(...)                     call_ws_get_server_info(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_get_client_info(...)                     call_ws_get_client_info(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_close(...)                               call_ws_close(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_query(...)                               call_ws_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stop_query(...)                          call_ws_stop_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_query_timeout(...)                       call_ws_query_timeout(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_take_timing(...)                         call_ws_take_timing(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_errno(...)                               call_ws_errno(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_errstr(...)                              call_ws_errstr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_affected_rows(...)                       call_ws_affected_rows(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_field_count(...)                         call_ws_field_count(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_is_update_query(...)                     call_ws_is_update_query(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_fetch_fields(...)                        call_ws_fetch_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_fetch_fields_v2(...)                     call_ws_fetch_fields_v2(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_fetch_raw_block(...)                     call_ws_fetch_raw_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_free_result(...)                         call_ws_free_result(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_result_precision(...)                    call_ws_result_precision(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_get_value_in_block(...)                  call_ws_get_value_in_block(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_timestamp_to_rfc3339(...)                call_ws_timestamp_to_rfc3339(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_init(...)                           call_ws_stmt_init(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_prepare(...)                        call_ws_stmt_prepare(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_set_tbname(...)                     call_ws_stmt_set_tbname(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_set_tbname_tags(...)                call_ws_stmt_set_tbname_tags(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_get_tag_fields(...)                 call_ws_stmt_get_tag_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_get_col_fields(...)                 call_ws_stmt_get_col_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_reclaim_fields(...)                 call_ws_stmt_reclaim_fields(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_is_insert(...)                      call_ws_stmt_is_insert(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_set_tags(...)                       call_ws_stmt_set_tags(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_bind_param_batch(...)               call_ws_stmt_bind_param_batch(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_add_batch(...)                      call_ws_stmt_add_batch(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_execute(...)                        call_ws_stmt_execute(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_affected_rows(...)                  call_ws_stmt_affected_rows(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_errstr(...)                         call_ws_stmt_errstr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_ws_stmt_close(...)                          call_ws_stmt_close(__FILE__, __LINE__, __func__, ##__VA_ARGS__)








EXTERN_C_BEGIN

EXTERN_C_END

#endif // _taosws_helpers_h_


