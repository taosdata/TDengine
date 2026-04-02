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

#include "internal.h"

#include "charset.h"
#include "columns.h"
#include "conn.h"
#include "desc.h"
#include "errs.h"
#include "log.h"
#include "conn_parser.h"
#include "ext_parser.h"
#include "sqls_parser.h"
#include "primarykeys.h"
#include "stmt.h"
#include "tables.h"
#include "taos_helpers.h"
#ifdef HAVE_TAOSWS           /* { */
#include "taosws_helpers.h"
#endif                       /* } */
#include "tls.h"
#include "topic.h"
#include "tsdb.h"
#include "ts_parser.h"
#include "typesinfo.h"

#include <errno.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

static void _param_bind_meta_reset(param_bind_meta_t *param_bind_meta)
{
  if (!param_bind_meta) return;
  param_bind_meta->check    = NULL;
  param_bind_meta->guess    = NULL;
  param_bind_meta->get_sqlc = NULL;
  param_bind_meta->adjust   = NULL;
  param_bind_meta->conv     = NULL;
}

static void _param_bind_meta_release(param_bind_meta_t *param_bind_meta)
{
  if (!param_bind_meta) return;
  _param_bind_meta_reset(param_bind_meta);
}

static void _params_bind_meta_reset(params_bind_meta_t *params_bind_meta)
{
  if (!params_bind_meta) return;
  for (size_t i=0; i<params_bind_meta->nr; ++i) {
    param_bind_meta_t *p = params_bind_meta->base + i;
    _param_bind_meta_reset(p);
  }

  params_bind_meta->nr = 0;
}

static void _params_bind_meta_release(params_bind_meta_t *params_bind_meta)
{
  if (!params_bind_meta) return;
  _params_bind_meta_reset(params_bind_meta);
  for (size_t i=0; i<params_bind_meta->cap; ++i) {
    param_bind_meta_t *p = params_bind_meta->base + i;
    _param_bind_meta_release(p);
  }

  params_bind_meta->cap = 0;
  TOD_SAFE_FREE(params_bind_meta->base);
}

static param_bind_meta_t* _params_bind_meta_get(params_bind_meta_t *params_bind_meta, size_t i_param)
{
  if (i_param >= params_bind_meta->nr) return NULL;
  return params_bind_meta->base + i_param;
}

static int _params_bind_meta_keep(params_bind_meta_t *params_bind_meta, size_t cap)
{
  if (cap <= params_bind_meta->cap) return 0;

  cap = (cap + 15) / 16 * 16;
  param_bind_meta_t *p = (param_bind_meta_t*)realloc(params_bind_meta->base, cap * sizeof(*p));
  if (!p) return -1;
  memset(p + params_bind_meta->nr, 0, sizeof(*p) * (cap - params_bind_meta->nr));

  params_bind_meta->base = p;
  params_bind_meta->cap  = cap;

  return 0;
}

static int _params_bind_meta_set_check(params_bind_meta_t *params_bind_meta, size_t i_param, param_f check)
{
  if (_params_bind_meta_keep(params_bind_meta, i_param + 1)) return -1;

  params_bind_meta->base[i_param].check = check;
  if (i_param >= params_bind_meta->nr) {
    params_bind_meta->nr = i_param + 1;
  }

  return 0;
}

static int _params_bind_meta_set_guess(params_bind_meta_t *params_bind_meta, size_t i_param, param_f guess)
{
  if (_params_bind_meta_keep(params_bind_meta, i_param + 1)) return -1;

  params_bind_meta->base[i_param].guess = guess;
  if (i_param >= params_bind_meta->nr) {
    params_bind_meta->nr = i_param + 1;
  }

  return 0;
}

static int _params_bind_meta_set_get_sqlc(params_bind_meta_t *params_bind_meta, size_t i_param, param_f get_sqlc)
{
  if (_params_bind_meta_keep(params_bind_meta, i_param + 1)) return -1;

  params_bind_meta->base[i_param].get_sqlc = get_sqlc;
  if (i_param >= params_bind_meta->nr) {
    params_bind_meta->nr = i_param + 1;
  }

  return 0;
}

static int _params_bind_meta_set_adjust(params_bind_meta_t *params_bind_meta, size_t i_param, param_f adjust)
{
  if (_params_bind_meta_keep(params_bind_meta, i_param + 1)) return -1;

  params_bind_meta->base[i_param].adjust = adjust;
  if (i_param >= params_bind_meta->nr) {
    params_bind_meta->nr = i_param + 1;
  }

  return 0;
}

static int _params_bind_meta_set_conv(params_bind_meta_t *params_bind_meta, size_t i_param, param_f conv)
{
  if (_params_bind_meta_keep(params_bind_meta, i_param + 1)) return -1;

  params_bind_meta->base[i_param].conv = conv;
  if (i_param >= params_bind_meta->nr) {
    params_bind_meta->nr = i_param + 1;
  }

  return 0;
}

static int _params_bind_meta_set(params_bind_meta_t *params_bind_meta, size_t i_param, param_f check, param_f guess, param_f get_sqlc)
{
  if (_params_bind_meta_set_check(params_bind_meta, i_param, check))    return -1;
  if (_params_bind_meta_set_guess(params_bind_meta, i_param, guess))    return -1;
  if (_params_bind_meta_set_get_sqlc(params_bind_meta, i_param, get_sqlc)) return -1;
  return 0;
}

static int _params_bind_meta_set_adjust_conv(params_bind_meta_t *params_bind_meta, size_t i_param, param_f adjust, param_f conv)
{
  if (_params_bind_meta_set_adjust(params_bind_meta, i_param, adjust)) return -1;
  if (_params_bind_meta_set_conv(params_bind_meta, i_param, conv)) return -1;
  return 0;
}

static void _sqls_reset(sqls_t *sqls)
{
  if (!sqls) return;
  sqls->nr     = 0;
  sqls->pos    = 0;
  sqls->failed = 0;
}

static void _sqls_release(sqls_t *sqls)
{
  if (!sqls) return;
  _sqls_reset(sqls);
  TOD_SAFE_FREE(sqls->sqls);
  sqls->cap = 0;
}

static void _sqlc_data_reset(sqlc_data_t *sqlc)
{
  if (!sqlc) return;
  mem_reset(&sqlc->mem);
  sqlc->is_null = 0;
}

static void _sqlc_data_release(sqlc_data_t *sqlc)
{
  if (!sqlc) return;
  mem_release(&sqlc->mem);
}

static void _sql_data_reset(sql_data_t *data)
{
  if (!data) return;
  mem_reset(&data->mem);
  data->is_null = 0;
}

static void _sql_data_release(sql_data_t *data)
{
  if (!data) return;
  mem_release(&data->mem);
}

static void _param_state_reset(param_state_t *param_state)
{
  if (!param_state) return;
  mem_reset(&param_state->tmp);
  _sqlc_data_reset(&param_state->sqlc_data);
  _sql_data_reset(&param_state->sql_data);

  param_state->nr_batch_size    = 0;
  param_state->i_batch_offset   = 0;

  param_state->nr_tsdb_fields   = 0;

  param_state->i_row            = 0;
  param_state->i_param          = 0;
  param_state->APD_record       = NULL;
  param_state->IPD_record       = NULL;

  param_state->tsdb_field       = NULL;

  param_state->param_column     = NULL;
  param_state->tsdb_bind        = NULL;
}

static void _param_state_release(param_state_t *param_state)
{
  if (!param_state) return;
  _param_state_reset(param_state);
  mem_release(&param_state->tmp);
  _sqlc_data_release(&param_state->sqlc_data);
  _sql_data_release(&param_state->sql_data);
}

static void _stmt_release_descriptors(stmt_t *stmt)
{
  descriptor_release(&stmt->APD);
  descriptor_release(&stmt->IPD);
  descriptor_release(&stmt->ARD);
  descriptor_release(&stmt->IRD);
}

static void _stmt_init_descriptors(stmt_t *stmt)
{
  descriptor_init(&stmt->APD);
  descriptor_init(&stmt->IPD);
  descriptor_init(&stmt->ARD);
  descriptor_init(&stmt->IRD);

  stmt->current_APD = &stmt->APD;
  stmt->current_ARD = &stmt->ARD;

  INIT_TOD_LIST_HEAD(&stmt->associated_APD_node);
  INIT_TOD_LIST_HEAD(&stmt->associated_ARD_node);
}

static void _stmt_init(stmt_t *stmt, conn_t *conn)
{
  stmt->conn = conn_ref(conn);

  stmt->strict   = 1;
  stmt->no_total = 0;

  errs_init(&stmt->errs);
  stmt->errs.connected_conn = conn;

  _stmt_init_descriptors(stmt);

  tsdb_stmt_init(&stmt->tsdb_stmt, stmt);
  tables_init(&stmt->tables, stmt);
  columns_init(&stmt->columns, stmt);
  typesinfo_init(&stmt->typesinfo, stmt);
  primarykeys_init(&stmt->primarykeys, stmt);
  topic_init(&stmt->topic, stmt);

  stmt->base = &stmt->tsdb_stmt.base;
  stmt->cursor_type = SQL_CURSOR_FORWARD_ONLY;
  if (stmt->conn->cfg.customproduct == CUSTP_ADO) {
    stmt->concurrency_attr = SQL_CONCUR_LOCK;
  } else {
    stmt->concurrency_attr = SQL_CONCUR_READ_ONLY;
  }
  stmt->refc = 1;
}

static void _get_data_ctx_reset(get_data_ctx_t *ctx)
{
  if (!ctx) return;
  _sqlc_data_reset(&ctx->sqlc);

  ctx->buf[0] = '\0';
  mem_reset(&ctx->mem);
  ctx->pos = NULL;
  ctx->nr  = 0;

  // NOTE: we don't support bookmark yet, thus `0` means unset yet
  ctx->Col_or_Param_Num = 0;
}

static void _get_data_ctx_release(get_data_ctx_t *ctx)
{
  if (!ctx) return;
  _get_data_ctx_reset(ctx);
  _sqlc_data_release(&ctx->sqlc);
}

descriptor_t* stmt_APD(stmt_t *stmt)
{
  return stmt->current_APD;
}

descriptor_t* stmt_IPD(stmt_t *stmt)
{
  return &stmt->IPD;
}

static descriptor_t* _stmt_IRD(stmt_t *stmt)
{
  return &stmt->IRD;
}

static descriptor_t* _stmt_ARD(stmt_t *stmt)
{
  return stmt->current_ARD;
}

static SQLULEN* _stmt_get_rows_fetched_ptr(stmt_t *stmt)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  return IRD_header->DESC_ROWS_PROCESSED_PTR;
}

static void _stmt_reset_result(stmt_t *stmt)
{
  _get_data_ctx_reset(&stmt->get_data_ctx);

  tsdb_res_reset(&stmt->tsdb_stmt.res);
  tables_reset(&stmt->tables);
  columns_reset(&stmt->columns);
  typesinfo_reset(&stmt->typesinfo);
  primarykeys_reset(&stmt->primarykeys);
  topic_reset(&stmt->topic);

  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
}

static void _stmt_release_result(stmt_t *stmt)
{
  _get_data_ctx_release(&stmt->get_data_ctx);

  tsdb_res_release(&stmt->tsdb_stmt.res);
  tables_release(&stmt->tables);
  columns_release(&stmt->columns);
  typesinfo_release(&stmt->typesinfo);
  primarykeys_release(&stmt->primarykeys);
  topic_release(&stmt->topic);

  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
}

static void _stmt_reset_tables(stmt_t *stmt)
{
  tables_reset(&stmt->tables);
}

static void _stmt_reset_columns(stmt_t *stmt)
{
  columns_reset(&stmt->columns);
}

static void _stmt_reset_typesinfo(stmt_t *stmt)
{
  typesinfo_reset(&stmt->typesinfo);
}

static void _stmt_reset_primarykeys(stmt_t *stmt)
{
  primarykeys_reset(&stmt->primarykeys);
}

#ifdef USE_TICK_TO_DEBUG                 /* { */
static void _ticks_reset(ticks_t *ticks)
{
  ticks->idx = 0;

  ticks->max_delta = INT64_MIN;
  ticks->min_delta = INT64_MAX;
  ticks->total_delta = 0;

  ticks->max_interval = INT64_MIN;
  ticks->min_interval = INT64_MAX;
  ticks->total_interval = 0;
}

static void _stmt_reset_ticks(stmt_t *stmt)
{
  _ticks_reset(&stmt->ticks_for_SQLGetData);
  _ticks_reset(&stmt->ticks_for_fetch_block);
}

static void _ticks_release(ticks_t *ticks)
{
  TOD_SAFE_FREE(ticks->ticks)
  ticks->sz = 0;
  ticks->idx = 0;
}

static void _stmt_release_ticks(stmt_t *stmt)
{
  _ticks_release(&stmt->ticks_for_SQLGetData);
  _ticks_release(&stmt->ticks_for_fetch_block);
}

static void _ticks_init(ticks_t *ticks)
{
  ticks->idx = 0;

  ticks->max_delta = INT64_MIN;
  ticks->min_delta = INT64_MAX;
  ticks->total_delta = 0;

  ticks->max_interval = INT64_MIN;
  ticks->min_interval = INT64_MAX;
  ticks->total_interval = 0;

  QueryPerformanceFrequency(&ticks->freq);
}

static int _stmt_init_ticks(stmt_t *stmt)
{
  const size_t ticks = 102600001;

  stmt->ticks_for_SQLGetData.ticks = (tick_t*)calloc(ticks, sizeof(*stmt->ticks_for_SQLGetData.ticks));
  if (!stmt->ticks_for_SQLGetData.ticks) return -1;
  stmt->ticks_for_SQLGetData.sz = ticks;
  _ticks_init(&stmt->ticks_for_SQLGetData);

  stmt->ticks_for_fetch_block.ticks = (tick_t*)calloc(ticks, sizeof(*stmt->ticks_for_SQLGetData.ticks));
  if (!stmt->ticks_for_fetch_block.ticks) return -1;
  stmt->ticks_for_fetch_block.sz = ticks;
  _ticks_init(&stmt->ticks_for_fetch_block);

  return 0;
}

static int _ticks_enter(ticks_t *ticks)
{
  if (ticks->idx >= ticks->sz) return -1;
  tick_t *tick = ticks->ticks + ticks->idx;
#ifdef _WIN32               /* { */
  QueryPerformanceCounter(&tick->enter);
#else                       /* }{ */
  clock_gettime(CLOCK_MONOTONIC_RAW, &tick->enter);
#endif                      /* } */
  return 0;
}

#ifdef _WIN32               /* { */
static int64_t _ticks_delta(ticks_t *ticks, LARGE_INTEGER *t0, LARGE_INTEGER *t1)
{
  int64_t delta = 0;
  delta  = t1->QuadPart - t0->QuadPart;
  delta *= 1000000000;
  delta /= ticks->freq.QuadPart;
  return delta;
}
#else                       /* }{ */
static int64_t _ticks_delta(ticks_t *ticks, struct timespec *t0, struct timespec *t1)
{
  int64_t delta = 0;
  delta = t1->tv_sec - t0->tv_sec;
  delta *= 1000000000;
  delta += t1->tv_nsec - t0->tv_nsec;
  return delta;
}
#endif                      /* } */

static void _ticks_leave(ticks_t *ticks)
{
  if (ticks->idx >= ticks->sz) return;
  tick_t *tick = ticks->ticks + ticks->idx;

#ifdef _WIN32               /* { */
  QueryPerformanceCounter(&tick->leave);
#else                       /* }{ */
  clock_gettime(CLOCK_MONOTONIC_RAW, &tick->leave);
#endif                      /* } */

  int64_t delta = _ticks_delta(ticks, &tick->enter, &tick->leave);
  if (delta > ticks->max_delta) ticks->max_delta = delta;
  if (delta < ticks->min_delta) ticks->min_delta = delta;
  ticks->total_delta += delta;

  ticks->idx += 1;
  if (ticks->idx > 1) {
    delta = _ticks_delta(ticks, &(tick-1)->enter, &tick->enter);
    if (delta > ticks->max_interval) ticks->max_interval = delta;
    if (delta < ticks->min_interval) ticks->min_interval = delta;
    ticks->total_interval += delta;
  }
}

static int _stmt_enter_SQLGetData(stmt_t *stmt)
{
  return _ticks_enter(&stmt->ticks_for_SQLGetData);
}

static void _stmt_leave_SQLGetData(stmt_t *stmt)
{
  _ticks_leave(&stmt->ticks_for_SQLGetData);
}

int stmt_enter_fetch_block(stmt_t *stmt)
{
  return _ticks_enter(&stmt->ticks_for_fetch_block);
}

void stmt_leave_fetch_block(stmt_t *stmt)
{
  _ticks_leave(&stmt->ticks_for_fetch_block);
}

static void ticks_report(ticks_t *ticks, const char *title, size_t minimum)
{
  if (ticks->idx < minimum) return;
  OE("%s:   delta:max[%10" PRId64 "ns];min[%10" PRId64 "ns];nr[%10" PRId64 "];avg:[%10" PRId64 "]",
    title, ticks->max_delta, ticks->min_delta, ticks->idx,
    (ticks->total_delta) / ticks->idx);
  OE("%s:interval:max[%10" PRId64 "ns];min[%10" PRId64 "ns];nr[%10" PRId64 "];avg:[%10" PRId64 "]",
    title, ticks->max_interval, ticks->min_interval, ticks->idx,
    (ticks->total_interval) / ticks->idx);
  OE("\n");
}

static void _stmt_report_ticks(stmt_t *stmt)
{
  ticks_report(&stmt->ticks_for_SQLGetData,  "      SQLGetData", 102400);
  ticks_report(&stmt->ticks_for_fetch_block, "taos_fetch_block", 20);
  _stmt_reset_ticks(stmt);
}
#endif                                   /* } */

static void _stmt_close_result(stmt_t *stmt)
{
#ifdef USE_TICK_TO_DEBUG                 /* { */
  _stmt_report_ticks(stmt);
#endif                                   /* } */
  _stmt_reset_result(stmt);
  stmt->base = &stmt->tsdb_stmt.base;
}

static void _stmt_unbind_cols(stmt_t *stmt)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_COUNT = 0;
  memset(ARD->records, 0, sizeof(*ARD->records) * ARD->cap);
}

static void _stmt_reset_params(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_COUNT = 0;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_COUNT = 0;

  _params_bind_meta_reset(&stmt->params_bind_meta);
  tsdb_paramset_reset(&stmt->tsdb_paramset);
}

static void _stmt_release_stmt(stmt_t *stmt)
{
  _stmt_close_result(stmt);
  tsdb_stmt_release(&stmt->tsdb_stmt);
}

void stmt_dissociate_ARD(stmt_t *stmt)
{
  if (stmt->associated_ARD == NULL) return;

  tod_list_del(&stmt->associated_ARD_node);
  desc_unref(stmt->associated_ARD);
  stmt->associated_ARD = NULL;
  stmt->current_ARD = &stmt->ARD;
}

static SQLRETURN _stmt_associate_ARD(stmt_t *stmt, desc_t *desc)
{
  if (stmt->associated_ARD == desc) return SQL_SUCCESS;

  if (desc->conn != stmt->conn) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor not on the same connection as that of the statement");
    return SQL_ERROR;
  }

  // FIXME:
  if (stmt->associated_APD == desc) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor already associated as statement's APD");
    return SQL_ERROR;
  }

  stmt_dissociate_ARD(stmt);

  tod_list_add_tail(&stmt->associated_ARD_node, &desc->associated_stmts_as_ARD);
  desc_ref(desc);
  stmt->associated_ARD = desc;
  stmt->current_ARD = &desc->descriptor;

  return SQL_SUCCESS;
}

void stmt_dissociate_APD(stmt_t *stmt)
{
  if (stmt->associated_APD == NULL) return;

  descriptor_reclaim_buffers(&stmt->associated_APD->descriptor);

  tod_list_del(&stmt->associated_APD_node);
  desc_unref(stmt->associated_APD);
  stmt->associated_APD = NULL;
  stmt->current_APD = &stmt->APD;
}

static SQLRETURN _stmt_associate_APD(stmt_t *stmt, desc_t *desc)
{
  if (stmt->associated_APD == desc) return SQL_SUCCESS;

  if (desc->conn != stmt->conn) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor not on the same connection as that of the statement");
    return SQL_ERROR;
  }

  // FIXME:
  if (stmt->associated_ARD == desc) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor already associated as statement's ARD");
    return SQL_ERROR;
  }

  stmt_dissociate_APD(stmt);

  tod_list_add_tail(&stmt->associated_APD_node, &desc->associated_stmts_as_APD);
  desc_ref(desc);
  stmt->associated_APD = desc;
  stmt->current_APD = &desc->descriptor;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_row_desc(stmt_t *stmt, SQLPOINTER ValuePtr)
{
  if (ValuePtr == SQL_NULL_DESC) {
    stmt_dissociate_ARD(stmt);
    return SQL_SUCCESS;
  }

  desc_t *desc = (desc_t*)(SQLHANDLE)ValuePtr;
  return _stmt_associate_ARD(stmt, desc);
}

static SQLRETURN _stmt_set_param_desc(stmt_t *stmt, SQLPOINTER ValuePtr)
{
  if (ValuePtr == SQL_NULL_DESC) {
    stmt_dissociate_APD(stmt);
    return SQL_SUCCESS;
  }

  desc_t *desc = (desc_t*)(SQLHANDLE)ValuePtr;
  return _stmt_associate_APD(stmt, desc);
}

static void _stmt_release_field_arrays(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_reclaim_buffers(APD);
}

// static void _tsdb_param_column_reset(tsdb_param_column_t *pa)
// {
//   mem_reset(&pa->mem);
// }

static void _stmt_release(stmt_t *stmt)
{
  _stmt_release_result(stmt);

  _stmt_release_field_arrays(stmt);

  stmt_dissociate_APD(stmt);

  _stmt_release_stmt(stmt);

  stmt_dissociate_ARD(stmt);

  _stmt_release_descriptors(stmt);

  mem_release(&stmt->raw);
  mem_release(&stmt->tsdb_sql);

  errs_release(&stmt->errs);
  mem_release(&stmt->mem);
  tsdb_paramset_release(&stmt->tsdb_paramset);
  tsdb_binds_release(&stmt->tsdb_binds);
  _sqls_release(&stmt->sqls);
  _param_state_release(&stmt->param_state);
  _params_bind_meta_release(&stmt->params_bind_meta);
#ifdef USE_TICK_TO_DEBUG                 /* { */
  _stmt_release_ticks(stmt);
#endif                                   /* } */

  conn_unref(stmt->conn);
  stmt->conn = NULL;

  return;
}

stmt_t* stmt_create(conn_t *conn)
{
  stmt_t *stmt = (stmt_t*)calloc(1, sizeof(*stmt));
  if (!stmt) {
    conn_oom(conn);
    return NULL;
  }
#ifdef USE_TICK_TO_DEBUG                 /* { */
  int r = 0;
  if (r == 0) r = _stmt_init_ticks(stmt);
  if (r) {
    _stmt_release_ticks(stmt);
    TOD_SAFE_FREE(stmt);
    conn_oom(conn);
    return NULL;
  }
#endif                                   /* } */

  tod_list_add_tail(&stmt->node, &conn->stmts);
  conn->nr_stmts += 1;

  _stmt_init(stmt, conn);

  return stmt;
}

stmt_t* stmt_ref(stmt_t *stmt)
{
  int prev = atomic_fetch_add(&stmt->refc, 1);
  OA_ILE(prev>0);
  return stmt;
}

stmt_t* stmt_unref(stmt_t *stmt)
{
  int prev = atomic_fetch_sub(&stmt->refc, 1);
  if (prev>1) return stmt;
  OA_ILE(prev==1);

  tod_list_del(&stmt->node);
  stmt->conn->nr_stmts -= 1;
  _stmt_release(stmt);
  free(stmt);

  return NULL;
}

SQLRETURN stmt_free(stmt_t *stmt)
{
  stmt_unref(stmt);
  return SQL_SUCCESS;
}

static int _stmt_time_precision(stmt_t *stmt)
{
  int time_precision = 0;
  if (stmt->base == &stmt->tsdb_stmt.base) {
    time_precision = stmt->tsdb_stmt.res.time_precision;
  }
  return time_precision;
}

static col_bind_map_t _col_bind_map[] = {
  {
    .tsdb_type      = TSDB_DATA_TYPE_TIMESTAMP,
    .type_name      = "TIMESTAMP",
    .sql_type       = SQL_TYPE_TIMESTAMP,
    .sql_promoted   = SQL_TYPE_TIMESTAMP,
    .prefix         = "'",
    .suffix         = "'",
    .length         = 0,
    .octet_length   = 8,
    .precision      = 0,
    .scale          = 0,
    .display_size   = 8,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_SEARCHABLE,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_BOOL,
    .type_name      = "BOOL",
    .sql_type       = SQL_BIT,
    .sql_promoted   = SQL_BIT,
    .prefix         = "",
    .suffix         = "",
    .length         = 1,
    .octet_length   = 1,
    .precision      = 1,
    .scale          = 0,
    .display_size   = 1,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_TINYINT,
    .type_name      = "TINYINT",
    .sql_type       = SQL_TINYINT,
    .sql_promoted   = SQL_SMALLINT,
    .prefix         = "",
    .suffix         = "",
    .length         = 3,
    .octet_length   = 1,
    .precision      = 3,
    .scale          = 0,
    .display_size   = 4,
    .num_prec_radix = 10,
    .unsigned_      = SQL_FALSE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_SMALLINT,
    .type_name      = "SMALLINT",
    .sql_type       = SQL_SMALLINT,
    .sql_promoted   = SQL_SMALLINT,
    .prefix         = "",
    .suffix         = "",
    .length         = 5,
    .octet_length   = 2,
    .precision      = 5,
    .scale          = 0,
    .display_size   = 6,
    .num_prec_radix = 10,
    .unsigned_      = SQL_FALSE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_INT,
    .type_name      = "INT",
    .sql_type       = SQL_INTEGER,
    .sql_promoted   = SQL_INTEGER,
    .prefix         = "",
    .suffix         = "",
    .length         = 10,
    .octet_length   = 4,
    .precision      = 10,
    .scale          = 0,
    .display_size   = 11,
    .num_prec_radix = 10,
    .unsigned_      = SQL_FALSE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_BIGINT,
    .type_name      = "BIGINT",
    .sql_type       = SQL_BIGINT,
    .sql_promoted   = SQL_BIGINT,
    .prefix         = "",
    .suffix         = "",
    .length         = 19,
    .octet_length   = 8,
    .precision      = 19,
    .scale          = 0,
    .display_size   = 20,
    .num_prec_radix = 10,
    .unsigned_      = SQL_FALSE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_FLOAT,
    .type_name      = "FLOAT",
    .sql_type       = SQL_REAL,
    .sql_promoted   = SQL_REAL,
    .prefix         = "",
    .suffix         = "",
    .length         = 7,
    .octet_length   = 4,
    .precision      = 24,
    .scale          = 0,
    .display_size   = 14,
    .num_prec_radix = 2,
    .unsigned_      = SQL_FALSE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_DOUBLE,
    .type_name      = "DOUBLE",
    .sql_type       = SQL_DOUBLE,
    .sql_promoted   = SQL_DOUBLE,
    .prefix         = "",
    .suffix         = "",
    .length         = 15,
    .octet_length   = 8,
    .precision      = 53,
    .scale          = 0,
    .display_size   = 24,
    .num_prec_radix = 2,
    .unsigned_      = SQL_FALSE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_VARCHAR,
    .type_name      = "VARCHAR",
    .sql_type       = SQL_VARCHAR,
    .sql_promoted   = SQL_VARCHAR,
    .prefix         = "'",
    .suffix         = "'",
    .length         = -1,
    .octet_length   = -1,
    .precision      = -1,
    .scale          = 0,
    .display_size   = -1,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_SEARCHABLE,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_NCHAR,
    .type_name      = "NCHAR",
    .sql_type       = SQL_WVARCHAR,
    .sql_promoted   = SQL_WVARCHAR,
    .prefix         = "'",
    .suffix         = "'",
    .length         = -1,
    .octet_length   = -2,
    .precision      = -1,
    .scale          = 0,
    .display_size   = -2,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_SEARCHABLE,
  },{
    // FIXME:
    .tsdb_type      = TSDB_DATA_TYPE_JSON,
    .type_name      = "JSON",
    .sql_type       = SQL_WVARCHAR,
    .sql_promoted   = SQL_WVARCHAR,
    .prefix         = "'",
    .suffix         = "'",
    .length         = -1,
    .octet_length   = -2,
    .precision      = -1,
    .scale          = 0,
    .display_size   = -2,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_SEARCHABLE,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_UTINYINT,
    .type_name      = "TINYINT UNSIGNED",
    .sql_type       = SQL_TINYINT,
    .sql_promoted   = SQL_TINYINT,
    .prefix         = "",
    .suffix         = "",
    .length         = 3,
    .octet_length   = 1,
    .precision      = 3,
    .scale          = 0,
    .display_size   = 3,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_USMALLINT,
    .type_name      = "SMALLINT UNSIGNED",
    .sql_type       = SQL_SMALLINT,
    .sql_promoted   = SQL_INTEGER,
    .prefix         = "",
    .suffix         = "",
    .length         = 5,
    .octet_length   = 2,
    .precision      = 5,
    .scale          = 0,
    .display_size   = 5,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_UINT,
    .type_name      = "INT UNSIGNED",
    .sql_type       = SQL_INTEGER,
    .sql_promoted   = SQL_BIGINT,
    .prefix         = "",
    .suffix         = "",
    .length         = 10,
    .octet_length   = 4,
    .precision      = 10,
    .scale          = 0,
    .display_size   = 10,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },{
    .tsdb_type      = TSDB_DATA_TYPE_UBIGINT,
    .type_name      = "BIGINT UNSIGNED",
    .sql_type       = SQL_BIGINT,
    .sql_promoted   = SQL_BIGINT,
    .prefix         = "",
    .suffix         = "",
    .length         = 20,
    .octet_length   = 8,
    .precision      = 20,
    .scale          = 0,
    .display_size   = 20,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },{
    // TODO & FIXME:
    .tsdb_type      = TSDB_DATA_TYPE_VARBINARY,
    .type_name      = "VARBINARY",
    .sql_type       = SQL_VARBINARY,
    .sql_promoted   = SQL_C_BINARY,
    .prefix         = "'\\x",
    .suffix         = "'",
    .length         = -1,
    .octet_length   = -1,
    .precision      = -1,
    .scale          = 0,
    .display_size   = -1,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },{
    // TODO & FIXME:
    .tsdb_type      = TSDB_DATA_TYPE_GEOMETRY,
    .type_name      = "GEOMETRY",  // FIXME:
    .sql_type       = SQL_VARBINARY,
    .sql_promoted   = SQL_C_BINARY,
    .prefix         = "'\\x",      // FIXME:
    .suffix         = "'",         // FIXME:
    .length         = -1,
    .octet_length   = -1,
    .precision      = -1,
    .scale          = 0,
    .display_size   = -1,
    .num_prec_radix = 10,
    .unsigned_      = SQL_TRUE,
    .searchable     = SQL_PRED_BASIC,
  },
};

static SQLRETURN _stmt_col_DESC_TYPE(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  for (size_t i=0; i<sizeof(_col_bind_map)/sizeof(_col_bind_map[0]); ++i) {
    if (col->type != _col_bind_map[i].tsdb_type) continue;
    if (stmt->conn->cfg.unsigned_promotion) {
      *NumericAttributePtr = _col_bind_map[i].sql_promoted;
    } else {
      *NumericAttributePtr = _col_bind_map[i].sql_type;
    }
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = SQL_WVARCHAR;
      } else {
        *NumericAttributePtr = SQL_DATETIME;
      }
    }
    return SQL_SUCCESS;
  }

  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_CONCISE_TYPE(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLLEN               *NumericAttributePtr)
{
  if (stmt->conn->cfg.unsigned_promotion) {
    *NumericAttributePtr = _map->sql_promoted;
  } else {
    *NumericAttributePtr = _map->sql_type;
  }
  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!stmt->conn->cfg.timestamp_as_is) {
      *NumericAttributePtr = SQL_WVARCHAR;
      return SQL_SUCCESS;
    }
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_OCTET_LENGTH(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    int32_t               bytes,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!stmt->conn->cfg.timestamp_as_is) {
      *NumericAttributePtr = (20 + (time_precision + 1) * 3) * 2;
      return SQL_SUCCESS;
    }
  }
  if (_map->octet_length > 0 && _map->octet_length != bytes) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:octet length for `%s` is expected to be %d, but got ==%d==",
        taos_data_type(_map->tsdb_type), _map->octet_length, bytes);
    return SQL_ERROR;
  }
  if (_map->octet_length < 0) {
    *NumericAttributePtr = 0 - bytes * _map->octet_length;
  } else {
    *NumericAttributePtr = _map->octet_length;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_PRECISION(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    int32_t               bytes,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!stmt->conn->cfg.timestamp_as_is) {
      *NumericAttributePtr = 20 + (time_precision + 1) * 3;
    } else {
      *NumericAttributePtr = (time_precision + 1) * 3;
    }
    return SQL_SUCCESS;
  }
  if (_map->precision == -1) {
    *NumericAttributePtr = bytes;
  } else {
    *NumericAttributePtr = _map->precision;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_SCALE(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!stmt->conn->cfg.timestamp_as_is) {
      *NumericAttributePtr = _map->scale;
    } else {
      *NumericAttributePtr = (time_precision + 1) * 3;
    }
    return SQL_SUCCESS;
  }
  *NumericAttributePtr = _map->scale;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_DISPLAY_SIZE(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    int32_t               bytes,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!stmt->conn->cfg.timestamp_as_is) {
      *NumericAttributePtr = 20 + (time_precision + 1) * 3;
      return SQL_SUCCESS;
    }
  }
  if (_map->display_size < 0) {
    *NumericAttributePtr = 0 - bytes * _map->display_size;
  } else {
    *NumericAttributePtr = _map->display_size;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_SEARCHABLE(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLLEN               *NumericAttributePtr)
{
  (void)stmt;

  *NumericAttributePtr = _map->searchable;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_NAME(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  int n;
  n = snprintf(CharacterAttributePtr, BufferLength, "%.*s", (int)sizeof(col->name), col->name);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_TYPE_NAME(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  const char *name = _map->type_name;
  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!stmt->conn->cfg.timestamp_as_is) {
      name = "NCHAR";
    }
  }
  int n = snprintf(CharacterAttributePtr, BufferLength, "%s", name);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_LENGTH(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    int32_t               bytes,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  if (_map->tsdb_type == TSDB_DATA_TYPE_TIMESTAMP) {
    *NumericAttributePtr = 20 + (time_precision + 1) * 3;
    return SQL_SUCCESS;
  }
  if (_map->length == -1) {
    *NumericAttributePtr = bytes;
  } else {
    *NumericAttributePtr = _map->length;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_NUM_PREC_RADIX(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLLEN               *NumericAttributePtr)
{
  (void)stmt;
  *NumericAttributePtr = _map->num_prec_radix;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_UNSIGNED(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLLEN               *NumericAttributePtr)
{
  (void)stmt;

  *NumericAttributePtr = _map->unsigned_;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_LITERAL_PREFIX(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  const char *prefix = _map->prefix;
  int n = snprintf(CharacterAttributePtr, BufferLength, "%s", prefix);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_LITERAL_SUFFIX(
    stmt_t               *stmt,
    col_bind_map_t       *_map,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  const char *suffix = _map->suffix;
  int n = snprintf(CharacterAttributePtr, BufferLength, "%s", suffix);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_set_empty_string(
    stmt_t               *stmt,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  int n;
  n = snprintf(CharacterAttributePtr, BufferLength, "%s", "");
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_copy_string(
    stmt_t         *stmt,
    const SQLCHAR  *name,
    size_t          name_bytes,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  int n = 0;
  n = snprintf((char*)CharacterAttributePtr, BufferLength, "%.*s", (int)name_bytes, name);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_fill_IRD(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  TAOS_FIELD *fields;
  size_t nr;
  sr = stmt->base->get_col_fields(stmt->base, &fields, &nr);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  sr = descriptor_keep(IRD, stmt, nr);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  IRD_header->DESC_COUNT = (SQLUSMALLINT)nr;

  for (size_t i=0; i<IRD_header->DESC_COUNT; ++i) {
    desc_record_t *IRD_record = IRD->records + i;
    TAOS_FIELD *col = fields + i;

    SQLSMALLINT StringLength = 0;

    IRD_record->tsdb_type = col->type;

    IRD_record->DESC_AUTO_UNIQUE_VALUE = SQL_FALSE;

    col_bind_map_t *_map = NULL;
    for (size_t i=0; i<sizeof(_col_bind_map)/sizeof(_col_bind_map[0]); ++i) {
      if (col->type != _col_bind_map[i].tsdb_type) continue;
      _map = _col_bind_map + i;
      break;
    }
    if (!_map) {
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s`[%d] not supported yet", taos_data_type(col->type), col->type);
      return SQL_ERROR;
    }

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_BASE_COLUMN_NAME, sizeof(IRD_record->DESC_BASE_COLUMN_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_BASE_TABLE_NAME, sizeof(IRD_record->DESC_BASE_TABLE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_CASE_SENSITIVE = SQL_FALSE;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_CATALOG_NAME, sizeof(IRD_record->DESC_CATALOG_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_CONCISE_TYPE(stmt, _map, &IRD_record->DESC_CONCISE_TYPE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_DATA_PTR = NULL;
    IRD_record->DESC_COUNT = IRD_header->DESC_COUNT; // FIXME:

    sr = _stmt_col_DESC_DISPLAY_SIZE(stmt, _map, col->bytes, &IRD_record->DESC_DISPLAY_SIZE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_FIXED_PREC_SCALE = 0;

    sr = _stmt_col_DESC_NAME(stmt, col, IRD_record->DESC_LABEL, sizeof(IRD_record->DESC_LABEL), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_LENGTH(stmt, _map, col->bytes, &IRD_record->DESC_LENGTH);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_LITERAL_PREFIX(stmt, _map, IRD_record->DESC_LITERAL_PREFIX, sizeof(IRD_record->DESC_LITERAL_PREFIX), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_LITERAL_SUFFIX(stmt, _map, IRD_record->DESC_LITERAL_SUFFIX, sizeof(IRD_record->DESC_LITERAL_SUFFIX), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_TYPE_NAME(stmt, _map, IRD_record->DESC_LOCAL_TYPE_NAME, sizeof(IRD_record->DESC_LOCAL_TYPE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_NAME(stmt, col, IRD_record->DESC_NAME, sizeof(IRD_record->DESC_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_NULLABLE = SQL_NULLABLE_UNKNOWN;
    if (i == 0 && col->type == TSDB_DATA_TYPE_TIMESTAMP) IRD_record->DESC_NULLABLE = SQL_NO_NULLS;

    sr = _stmt_col_DESC_NUM_PREC_RADIX(stmt, _map, &IRD_record->DESC_NUM_PREC_RADIX);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_OCTET_LENGTH(stmt, _map, col->bytes, &IRD_record->DESC_OCTET_LENGTH);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_PRECISION(stmt, _map, col->bytes, &IRD_record->DESC_PRECISION);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_SCALE(stmt, _map, &IRD_record->DESC_SCALE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_SCHEMA_NAME, sizeof(IRD_record->DESC_SCHEMA_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_SEARCHABLE(stmt, _map, &IRD_record->DESC_SEARCHABLE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_TABLE_NAME, sizeof(IRD_record->DESC_TABLE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_TYPE(stmt, col, &IRD_record->DESC_TYPE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_TYPE_NAME(stmt, _map, IRD_record->DESC_TYPE_NAME, sizeof(IRD_record->DESC_TYPE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_UNNAMED = (col->name[0]) ? SQL_NAMED : SQL_UNNAMED;

    sr = _stmt_col_DESC_UNSIGNED(stmt, _map, &IRD_record->DESC_UNSIGNED);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    if (stmt->conn->cfg.customproduct == CUSTP_ADO) {
      IRD_record->DESC_UPDATABLE = SQL_ATTR_WRITE;
    } else {
      IRD_record->DESC_UPDATABLE = SQL_ATTR_READONLY;
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_rows_fetched_ptr(stmt_t *stmt, SQLULEN *rows_fetched_ptr)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  IRD_header->DESC_ROWS_PROCESSED_PTR = rows_fetched_ptr;

  return SQL_SUCCESS;
}

static void _stmt_unprepare(stmt_t *stmt)
{
  _stmt_close_result(stmt);
  tsdb_stmt_unprepare(&stmt->tsdb_stmt);
  // _tsdb_binds_reset(&stmt->tsdb_binds);
  _param_state_reset(&stmt->param_state);
}

static SQLULEN _stmt_get_row_array_size(stmt_t *stmt)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  return ARD_header->DESC_ARRAY_SIZE;
}

static SQLRETURN _stmt_set_paramset_size(stmt_t *stmt, SQLULEN paramset_size)
{
  if (paramset_size == 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`0` for `SQL_ATTR_PARAMSET_SIZE` is substituted by current value");
    return SQL_SUCCESS_WITH_INFO;
  }

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_ARRAY_SIZE = paramset_size;

  if (paramset_size != 1) {
    if (stmt->tsdb_stmt.prepared && !stmt->tsdb_stmt.is_insert_stmt) {
      // NOTE: we still keep it in APD_header->DESC_ARRAY_SIZE, in case the caller does not check return code
      stmt_append_err(stmt, "HY000", 0, "General error:taosc currently does not support batch execution for non-insert-statement");
      return SQL_ERROR;
    }
  }

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_paramset_size(stmt_t *stmt)
// {
//   descriptor_t *APD = stmt_APD(stmt);
//   desc_header_t *APD_header = &APD->header;
//   return APD_header->DESC_ARRAY_SIZE;
// }

static SQLRETURN _stmt_set_row_status_ptr(stmt_t *stmt, SQLUSMALLINT *row_status_ptr)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  IRD_header->DESC_ARRAY_STATUS_PTR = row_status_ptr;
  return SQL_SUCCESS;
}

// static SQLUSMALLINT* stmt_get_row_status_ptr(stmt_t *stmt)
// {
//   descriptor_t *IRD = _stmt_IRD(stmt);
//   desc_header_t *IRD_header = &IRD->header;
//   return IRD_header->DESC_ARRAY_STATUS_PTR;
// }

static SQLRETURN _stmt_set_param_status_ptr(stmt_t *stmt, SQLUSMALLINT *param_status_ptr)
{
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_ARRAY_STATUS_PTR = param_status_ptr;
  return SQL_SUCCESS;
}

// static SQLUSMALLINT* _stmt_get_param_status_ptr(stmt_t *stmt)
// {
//   descriptor_t *IPD = stmt_IPD(stmt);
//   desc_header_t *IPD_header = &IPD->header;
//   return IPD_header->DESC_ARRAY_STATUS_PTR;
// }

SQLRETURN stmt_get_row_count(stmt_t *stmt, SQLLEN *row_count_ptr)
{
  return stmt->base->row_count(stmt->base, row_count_ptr);
}

SQLRETURN stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr)
{
  return stmt->base->get_num_cols(stmt->base, col_count_ptr);
}

static SQLRETURN _stmt_set_row_bind_type(stmt_t *stmt, SQLULEN row_bind_type)
{
  if (row_bind_type != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_BIND_TYPE = row_bind_type;

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_row_bind_type(stmt_t *stmt)
// {
//   descriptor_t *ARD = _stmt_ARD(stmt);
//   desc_header_t *ARD_header = &ARD->header;
//   return ARD_header->DESC_BIND_TYPE;
// }

static SQLRETURN _stmt_set_param_bind_type(stmt_t *stmt, SQLULEN param_bind_type)
{
  if (param_bind_type != SQL_PARAM_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_PARAM_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_BIND_TYPE = param_bind_type;

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_param_bind_type(stmt_t *stmt)
// {
//   descriptor_t *APD = stmt_APD(stmt);
//   desc_header_t *APD_header = &APD->header;
//   return APD_header->DESC_BIND_TYPE;
// }

static SQLRETURN _stmt_set_params_processed_ptr(stmt_t *stmt, SQLULEN *params_processed_ptr)
{
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_ROWS_PROCESSED_PTR = params_processed_ptr;

  return SQL_SUCCESS;
}

// static SQLULEN* _stmt_get_params_processed_ptr(stmt_t *stmt)
// {
//   descriptor_t *IPD = stmt_IPD(stmt);
//   desc_header_t *IPD_header = &IPD->header;
//   return IPD_header->DESC_ROWS_PROCESSED_PTR;
// }

static SQLRETURN _stmt_set_max_length(stmt_t *stmt, SQLULEN max_length)
{
  if (max_length != 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`%u` for `SQL_ATTR_MAX_LENGTH` is substituted by `0`");
    return SQL_SUCCESS_WITH_INFO;
  }
  // stmt->attrs.ATTR_MAX_LENGTH = max_length;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_row_bind_offset_ptr(stmt_t *stmt, SQLULEN *row_bind_offset_ptr)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_BIND_OFFSET_PTR = row_bind_offset_ptr;

  return SQL_SUCCESS;
}

// static SQLULEN* _stmt_get_row_bind_offset_ptr(stmt_t *stmt)
// {
//   descriptor_t *ARD = _stmt_ARD(stmt);
//   desc_header_t *ARD_header = &ARD->header;
//   return ARD_header->DESC_BIND_OFFSET_PTR;
// }

SQLRETURN stmt_describe_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *ColumnSizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr)
{
  if (ColumnNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  SQLRETURN sr = SQL_SUCCESS;

  SQLLEN NumericAttribute;

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_record_t *IRD_record = IRD->records + ColumnNumber - 1;

  sr = _stmt_col_copy_string(stmt, IRD_record->DESC_NAME, sizeof(IRD_record->DESC_NAME), ColumnName, BufferLength, NameLengthPtr);
  if (sr != SQL_SUCCESS) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:`SQL_DESC_NAME` for `%s` not supported yet",
        taos_data_type(IRD_record->tsdb_type));
    return SQL_ERROR;
  }

  if (DataTypePtr) {
    NumericAttribute = IRD_record->DESC_CONCISE_TYPE;
    *DataTypePtr = (SQLSMALLINT)NumericAttribute;
  }

  if (ColumnSizePtr) {
    NumericAttribute = IRD_record->DESC_LENGTH;
    *ColumnSizePtr = (SQLULEN)NumericAttribute;
  }

  if (DecimalDigitsPtr) {
    NumericAttribute = IRD_record->DESC_SCALE;
    *DecimalDigitsPtr = (SQLSMALLINT)NumericAttribute;
  }

  if (NullablePtr) {
    NumericAttribute = IRD_record->DESC_NULLABLE;
    *NullablePtr = (SQLSMALLINT)NumericAttribute;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  return descriptor_bind_col(ARD, stmt, ColumnNumber, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

SQLRETURN stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  if (ColumnNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  // OW("Column%d:%s",  ColumnNumber, sqlc_data_type(TargetType));
  return _stmt_bind_col(stmt, ColumnNumber, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLPOINTER _stmt_get_address(stmt_t *stmt, SQLPOINTER ptr, SQLULEN octet_length, size_t i_row, desc_header_t *header)
{
  (void)stmt;
  if (ptr == NULL) return ptr;

  char *base = (char*)ptr;
  if (header->DESC_BIND_OFFSET_PTR) base += *header->DESC_BIND_OFFSET_PTR;

  char *dest = base + octet_length * i_row;

  return dest;
}

SQLRETURN stmt_get_diag_rec(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&stmt->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

static SQLRETURN _stmt_get_data_prepare_ctx(stmt_t *stmt, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  sr = stmt->base->get_data(stmt->base, args->Col_or_Param_Num, tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (tsdb->is_null) return SQL_SUCCESS;

  int target_is_fix = 1;

  switch (args->TargetType) {
    case SQL_C_CHAR:
    case SQL_C_WCHAR:
    case SQL_C_BINARY:
      target_is_fix = 0;
      break;
    default:
      break;
  }

  if (target_is_fix) return SQL_SUCCESS;

  switch(tsdb->type) {
    case TSDB_DATA_TYPE_BOOL:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%s", tsdb->b ? "true" : "false");
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%d", tsdb->i8);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_UTINYINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%u", tsdb->u8);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%d", tsdb->i16);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_USMALLINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%u", tsdb->u16);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%d", tsdb->i32);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_UINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%u", tsdb->u32);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%" PRId64 "", tsdb->i64);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_UBIGINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%" PRIu64 "", tsdb->u64);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%g", (double)tsdb->flt);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%g", tsdb->dbl);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_VARCHAR:
      ctx->nr = tsdb->str.len;
      ctx->pos = tsdb->str.str;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        ctx->nr = tsdb_timestamp_to_string(tsdb->ts.ts, tsdb->ts.precision, (char*)ctx->buf, sizeof(ctx->buf));
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      ctx->nr = tsdb->str.len;
      ctx->pos = tsdb->str.str;
      break;
    case TSDB_DATA_TYPE_JSON:
      ctx->nr = tsdb->str.len;
      ctx->pos = tsdb->str.str;
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      ctx->nr  = tsdb->bin.len;
      ctx->pos = (const char*)tsdb->bin.bin;
      break;
    case TSDB_DATA_TYPE_GEOMETRY:
      ctx->nr  = tsdb->geo.len;
      ctx->pos = (const char*)tsdb->geo.geo;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static void _dump_iconv(
    const char *fromcode, const char *tocode,
    const char *inbuf, size_t inbytes, size_t inbytesleft,
    const char *outbuf, size_t outbytes, size_t outbytesleft)
{
  char ix[4096+1]; ix[0] = '\0';
  char ox[4096+1]; ox[0] = '\0';
  for (size_t i=0; i<inbytes && i<sizeof(ix)/2; ++i) {
    snprintf(ix + i * 2, 3, "%02x", (const unsigned char)inbuf[i]);
  }
  for (size_t i=0; i<outbytes-outbytesleft && i<sizeof(ox)/2; ++i) {
    snprintf(ox + i * 2, 3, "%02x", (const unsigned char)outbuf[i]);
  }
  char buf[16384]; buf[0] = '\0';
  snprintf(buf, sizeof(buf), "%s=>%s:%zd,%zd;%zd,%zd;0x%.*s;0x%.*s",
      fromcode, tocode, inbytes, inbytesleft, outbytes, outbytesleft,
      (int)(inbytes*2), ix,
      (int)(outbytes-outbytesleft)*2, ox);
  OW("%s\n", buf);
}

static SQLRETURN _stmt_conv_one_character(stmt_t *stmt, const char *fromcode, const char *tocode, char **in, size_t *inlen, char **out, size_t *outlen)
{
  const char *utf_32le = "UTF-32LE";

  charset_conv_t *to_utf_32le   = tls_get_charset_conv(fromcode, utf_32le);
  if (!to_utf_32le) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, utf_32le);
    return SQL_ERROR;
  }
  charset_conv_t *from_utf_32le = tls_get_charset_conv(utf_32le, tocode);
  if (!from_utf_32le) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", utf_32le, tocode);
    return SQL_ERROR;
  }

  int e = 0;
  char buf[4]; *buf   = '\0';
  size_t inlenlen     = 1;
  size_t n            = 0;

  char          *outbuf              = buf;
  size_t         outbytesleft        = sizeof(buf);
  char          *inbuf               = *in;
  size_t         inbytesleft         = 1;

again:

  outbuf         = buf;
  outbytesleft   = sizeof(buf);
  inbuf          = *in;
  inlenlen       = inbytesleft;

  if (inbytesleft > *inlen) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
        to_utf_32le->from, to_utf_32le->to, e, strerror(e));
    return SQL_ERROR;
  }

  n = CALL_iconv(to_utf_32le->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  e = errno;
  iconv(to_utf_32le->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    ++inbytesleft;
    goto again;
  }

  size_t converted = inlenlen - inbytesleft;

  outbuf       = buf;
  outbytesleft = sizeof(buf);

  n = CALL_iconv(from_utf_32le->cnv, &outbuf, &outbytesleft, out, outlen);
  e = errno;
  iconv(from_utf_32le->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
        from_utf_32le->from, from_utf_32le->to, e, strerror(e));
    return SQL_ERROR;
  }

  *in        += converted;
  *inlen     -= converted;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_return_SQL_SUCCESS_WITH_INFO(stmt_t *stmt, stmt_get_data_args_t *args, size_t nr_remains)
{
  if (stmt->no_total) {
    if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
    if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
  } else {
    if (args->IndPtr) *args->IndPtr = 0;
    if (args->StrLenPtr) *args->StrLenPtr = nr_remains;
  }
  stmt_append_err(stmt, "01004", 0, "String data, right truncated");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN _stmt_get_data_copy_buf_to_char(stmt_t *stmt, stmt_get_data_args_t *args)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;

  const char *fromcode = conn_get_tsdb_charset(stmt->conn);
  const char *tocode   = conn_get_sqlc_charset(stmt->conn);
  if (1) {
    // FIXME:
    if (tsdb->type != TSDB_DATA_TYPE_NCHAR && tsdb->type != TSDB_DATA_TYPE_JSON) {
      tocode = conn_get_sqlc_charset_for_col_bind(stmt->conn);
    }
  }

  if (tsdb->type == TSDB_DATA_TYPE_NCHAR && tsdb->str.encoder) {
    fromcode = tsdb->str.encoder;
  }

  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  // NOTE: calculate remaining bytes in whole-characters
  size_t nr_remains = 0;  // NOTE: store as StrLen_or_Ind when SQL_SUCCESS_WITH_INFO
  r = iconv_calc(cnv->cnv, ctx->pos, ctx->nr, &nr_remains);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:[iconv]Character set conversion for `%s` to `%s` failed",
        cnv->from, cnv->to);
    return SQL_ERROR;
  }

  size_t      curr        = ctx->curr;
  size_t      end         = ctx->end;
  uint8_t    *target_ptr  = (uint8_t*)args->TargetValuePtr;
  SQLLEN      buffer_len  = args->BufferLength;
  const char *residual    = ctx->residual;

  nr_remains += end - curr;

  if (buffer_len <= 1) {
    if (buffer_len) ((char*)args->TargetValuePtr)[0] = '\0';
    return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
  }

  size_t nr_copied_residual = 0;

  if (curr < end) {
    // NOTE: flushing residual
    int n = snprintf((char*)target_ptr, buffer_len, "%.*s", (int)(end - curr), residual + curr);
    if (n < 0) {
      stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
      return SQL_ERROR;
    }

    nr_copied_residual = n;
    if (n >= buffer_len) {
      // NOTE: too big to flush all
      nr_copied_residual = buffer_len - 1; // NOTE: null-terminator exclusive
      ctx->curr += nr_copied_residual;
      target_ptr[nr_copied_residual] = 0;
      return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
    }

    curr            += nr_copied_residual;
    target_ptr      += nr_copied_residual;
    buffer_len      -= nr_copied_residual;
    target_ptr[0]    = 0;
  }

  // NOTE: convert remainings
  const size_t     inbytes             = ctx->nr;
  const size_t     outbytes            = buffer_len - 1; // NOTE: leaving one byte for null-terminator

  char            *inbuf               = (char*)ctx->pos;
  size_t           inbytesleft         = inbytes;
  char            *outbuf              = (char*)target_ptr;
  size_t           outbytesleft        = outbytes;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  if (0) _dump_iconv(fromcode, tocode, (char*)ctx->pos, inbytes, inbytesleft, (char*)target_ptr, outbytes, outbytesleft);
  // OW("[%.*s]", (int)(outbytes - outbytesleft), (char*)args->TargetValuePtr);
  int e = errno;
  iconv(cnv->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    if (e != E2BIG) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
          cnv->from, cnv->to, e, strerror(e));
      return SQL_ERROR;
    }
  }

  *outbuf = '\0';

  if (inbytesleft) {
    // NOTE: convert next valid unicode-character and store it as residual
    char       *out                 = ctx->residual;
    size_t      outlen              = sizeof(ctx->residual);
    sr = _stmt_conv_one_character(stmt, fromcode, tocode, &inbuf, &inbytesleft, &out, &outlen);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    ctx->curr = 0;
    ctx->end  = sizeof(ctx->residual) - outlen;

    size_t n = ctx->end - ctx->curr;
    if (n > outbytesleft) n = outbytesleft;
    memcpy(outbuf, ctx->residual, n);
    outbuf    += n;
    ctx->curr  = n;
  } else {
    ctx->curr = ctx->end;
  }

  n = inbytes - inbytesleft;
  ctx->pos  += n;
  ctx->nr   -= n;

  size_t nr_converted = outbuf - (char*)args->TargetValuePtr;

  *outbuf   = '\0';

  if (ctx->nr || ctx->curr < ctx->end) {
    return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
  }

  ctx->pos = (const char*)-1;
  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = nr_converted;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_buf_to_wchar(stmt_t *stmt, stmt_get_data_args_t *args)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;

  const char *fromcode = conn_get_tsdb_charset(stmt->conn);
  const char *tocode   = "UTF-16LE";

  if (tsdb->type == TSDB_DATA_TYPE_NCHAR && tsdb->str.encoder) {
    fromcode = tsdb->str.encoder;
  }

  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  // NOTE: calculate remaining bytes in whole-characters
  size_t nr_remains = 0;  // NOTE: store as StrLen_or_Ind when SQL_SUCCESS_WITH_INFO
  r = iconv_calc(cnv->cnv, ctx->pos, ctx->nr, &nr_remains);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:[iconv]Character set conversion for `%s` to `%s` failed",
        cnv->from, cnv->to);
    return SQL_ERROR;
  }

  size_t      curr        = ctx->curr;
  size_t      end         = ctx->end;
  uint8_t    *target_ptr  = (uint8_t*)args->TargetValuePtr;
  SQLLEN      buffer_len  = args->BufferLength;
  const char *residual    = ctx->residual;

  nr_remains += end - curr;

  if (buffer_len <= 2) {
    if (buffer_len == 2) ((uint16_t*)args->TargetValuePtr)[0] = 0;
    return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
  }

  size_t nr_copied_residual = 0;

  if (curr < end) {
    // NOTE: flushing residual
    size_t n = end - curr;
    if (n > (size_t)buffer_len - 2) n = (buffer_len - 2) / 2 * 2; // NOTE: null-terminator exclusive
    memcpy(target_ptr, residual + curr, n);

    nr_copied_residual = n;
    if (end - curr > (size_t)buffer_len - 2) {
      // NOTE: too big to flush all
      ctx->curr += nr_copied_residual;
      *(uint16_t*)(target_ptr+nr_copied_residual) = 0;
      return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
    }

    curr            += nr_copied_residual;
    target_ptr      += nr_copied_residual;
    buffer_len      -= nr_copied_residual;
    *(uint16_t*)target_ptr    = 0;
  }

  // NOTE: convert remainings
  const size_t     inbytes             = ctx->nr;
  const size_t     outbytes            = buffer_len - 2; // NOTE: leaving one byte for null-terminator

  char            *inbuf               = (char*)ctx->pos;
  size_t           inbytesleft         = inbytes;
  char            *outbuf              = (char*)target_ptr;
  size_t           outbytesleft        = outbytes;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  if (0) _dump_iconv(fromcode, tocode, (char*)ctx->pos, inbytes, inbytesleft, (char*)target_ptr, outbytes, outbytesleft);
  // OW("[%.*s]", (int)(outbytes - outbytesleft), (char*)args->TargetValuePtr);
  int e = errno;
  iconv(cnv->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    if (e != E2BIG) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
          cnv->from, cnv->to, e, strerror(e));
      return SQL_ERROR;
    }
  }

  *(uint16_t*)outbuf = 0;

  if (inbytesleft) {
    // NOTE: convert next valid unicode-character and store it as residual
    char       *out                 = ctx->residual;
    size_t      outlen              = sizeof(ctx->residual);
    sr = _stmt_conv_one_character(stmt, fromcode, tocode, &inbuf, &inbytesleft, &out, &outlen);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    ctx->curr = 0;
    ctx->end  = sizeof(ctx->residual) - outlen;

    size_t n = ctx->end - ctx->curr;
    if (n > outbytesleft / 2 * 2) n = outbytesleft / 2 * 2;
    memcpy(outbuf, ctx->residual, n);
    outbuf    += n;
    ctx->curr  = n;
  } else {
    ctx->curr = ctx->end;
  }

  n = inbytes - inbytesleft;
  ctx->pos  += n;
  ctx->nr   -= n;

  size_t nr_converted = outbuf - (char*)args->TargetValuePtr;

  *(uint16_t*)outbuf   = 0;

  if (ctx->nr || ctx->curr < ctx->end) {
    return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
  }

  ctx->pos = (const char*)-1;
  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = nr_converted;
  return SQL_SUCCESS;

























  // const size_t     inbytes             = ctx->nr;
  // const size_t     outbytes            = args->BufferLength < 2 ? 0 : args->BufferLength - 2;

  // char            *inbuf               = (char*)ctx->pos;
  // size_t           inbytesleft         = inbytes;
  // char            *outbuf              = (char*)args->TargetValuePtr;
  // size_t           outbytesleft        = outbytes;

  // size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  // if (0) _dump_iconv(fromcode, tocode, (char*)ctx->pos, inbytes, inbytesleft, (char*)args->TargetValuePtr, outbytes, outbytesleft);
  // int e = errno;
  // iconv(cnv->cnv, NULL, NULL, NULL, NULL);
  // if (n == (size_t)-1) {
  //   if (e != E2BIG) {
  //     stmt_append_err_format(stmt, "HY000", 0,
  //         "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
  //         cnv->from, cnv->to, e, strerror(e));
  //     return SQL_ERROR;
  //   }
  // }
  // if (args->BufferLength > 1) *(int16_t*)outbuf = 0;

  // if (inbytesleft) {
  //   if (stmt->no_total) {
  //     if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
  //     if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
  //   } else {
  //     if (args->IndPtr) *args->IndPtr = 0;
  //     if (args->StrLenPtr) *args->StrLenPtr = ctx->nr;
  //   }
  //   stmt_append_err_format(stmt, "01004", 0,
  //       "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
  //       cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
  //   ctx->pos = inbuf;
  //   ctx->nr  = inbytesleft;
  //   return SQL_SUCCESS_WITH_INFO;
  // }

  // if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  // if (args->StrLenPtr) *args->StrLenPtr = outbytes - outbytesleft;
  // ctx->pos = (const char*)-1;
  // ctx->nr  = inbytesleft;
  // return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_buf_to_binary(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;
  if (args->BufferLength < 1) {
    stmt_append_err(stmt, "01004", 0, "String data, right truncated");
    return SQL_ERROR;
  }

  size_t nr_remains = ctx->nr;

  size_t n = args->BufferLength;
  if (ctx->nr < n) n = ctx->nr;

  memcpy(args->TargetValuePtr, ctx->pos, n);

  size_t nr_copied = n;

  ctx->nr -= n;

  if (ctx->nr) {
    ctx->pos += n;
    return _stmt_return_SQL_SUCCESS_WITH_INFO(stmt, args, nr_remains);
  }

  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = nr_copied;
  ctx->pos = (const char*)-1;
  return SQL_SUCCESS;
}

static const char* _stmt_varchar_cache(stmt_t *stmt, const char *s, size_t nr)
{
  int r = 0;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  mem_t *tsdb_cache = &ctx->mem;

  r = mem_keep(tsdb_cache, nr + 1);
  if (r) return NULL;

  memcpy(tsdb_cache->base, s, nr);
  tsdb_cache->base[nr] = '\0';

  return (const char*)tsdb_cache->base;
}

static SQLRETURN _stmt_varchar_to_int64(stmt_t *stmt, const char *s, size_t nr, int64_t *v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  if (s[nr]) {
    s = _stmt_varchar_cache(stmt, s, nr);
    if (!s) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:invalid character[0x%02x]",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  *v = ll;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_uint64(stmt_t *stmt, const char *s, size_t nr, uint64_t *v, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  int64_t i64;
  sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (memchr(s, '-', nr)) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:signedness conflict",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  *v = (uint64_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_float(stmt_t *stmt, const char *s, size_t nr, float *v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  if (s[nr]) {
    s = _stmt_varchar_cache(stmt, s, nr);
    if (!s) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  char *end = NULL;
  float flt = strtof(s, &end);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:invalid character[0x%02x]",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType, *end);
    return SQL_ERROR;
  }
  if (fabsf(flt) == HUGE_VALF) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  if (fpclassify(flt) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  *v = flt;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_double(stmt_t *stmt, const char *s, size_t nr, double *v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  if (s[nr]) {
    s = _stmt_varchar_cache(stmt, s, nr);
    if (!s) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  char *end = NULL;
  double dbl = strtod(s, &end);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:invalid character[0x%02x]",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType, *end);
    return SQL_ERROR;
  }
  if (fabs(dbl) == HUGE_VAL) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  if (fpclassify(dbl) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  *v = dbl;
  return SQL_SUCCESS;
}


static SQLRETURN _stmt_get_data_copy_varchar(stmt_t *stmt, const char *s, size_t nr, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  int64_t i64 = 0;
  uint64_t u64 = 0;
  float flt = 0.;
  double dbl = 0.;

  switch (args->TargetType) {
    case SQL_C_BIT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 != 0 && i64 != 1) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int8_t*)args->TargetValuePtr = !!i64;
      break;
    case SQL_C_STINYINT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 < INT8_MIN || i64 > INT8_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int8_t*)args->TargetValuePtr = (int8_t)i64;
      break;
    case SQL_C_UTINYINT:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (u64 > UINT8_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(uint8_t*)args->TargetValuePtr = (uint8_t)u64;
      break;
    case SQL_C_SSHORT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 < INT16_MIN || i64 > INT16_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int16_t*)args->TargetValuePtr = (int16_t)i64;
      break;
    case SQL_C_USHORT:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (u64 > UINT16_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(uint16_t*)args->TargetValuePtr = (uint16_t)u64;
      break;
    case SQL_C_SLONG:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 < INT32_MIN || i64 > INT32_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int32_t*)args->TargetValuePtr = (int32_t)i64;
      break;
    case SQL_C_ULONG:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (u64 > UINT32_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(uint32_t*)args->TargetValuePtr = (uint32_t)u64;
      break;
    case SQL_C_SBIGINT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(int64_t*)args->TargetValuePtr = i64;
      break;
    case SQL_C_UBIGINT:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(uint64_t*)args->TargetValuePtr = u64;
      break;
    case SQL_C_FLOAT:
      sr = _stmt_varchar_to_float(stmt, s, nr, &flt, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(float*)args->TargetValuePtr = flt;
      break;
    case SQL_C_DOUBLE:
      sr = _stmt_varchar_to_double(stmt, s, nr, &dbl, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(double*)args->TargetValuePtr = dbl;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_nchar(stmt_t *stmt, const char *s, size_t nr, stmt_get_data_args_t *args)
{
  return _stmt_get_data_copy_varchar(stmt, s, nr, args);
}

static SQLRETURN _stmt_get_data_copy_json(stmt_t *stmt, const char *s, size_t nr, stmt_get_data_args_t *args)
{
  return _stmt_get_data_copy_nchar(stmt, s, nr, args);
}

static SQLRETURN _stmt_get_data_copy_varbinary_to_binary(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;
  if (args->BufferLength < 0) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:buffer too small",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  size_t n = args->BufferLength;
  if (ctx->nr < n) n = ctx->nr;

  memcpy(args->TargetValuePtr, ctx->pos, n);

  ctx->nr = ctx->nr - n;

  if (ctx->nr) {
    if (stmt->no_total) {
      if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
      if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
    } else {
      if (args->IndPtr) *args->IndPtr = 0;
      if (args->StrLenPtr) *args->StrLenPtr = ctx->nr;
    }
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:Column[%d], #%zd out of #%zd bytes consumed",
        args->Col_or_Param_Num, n, n + ctx->nr);
    ctx->pos += n;
    return SQL_SUCCESS_WITH_INFO;
  }

  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = n;
  ctx->pos = (const char*)-1;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_geometry_to_binary(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;
  if (args->BufferLength < 0) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:buffer too small",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  size_t n = args->BufferLength;
  if (ctx->nr < n) n = ctx->nr;

  memcpy(args->TargetValuePtr, ctx->pos, n);

  ctx->nr = ctx->nr - n;

  if (ctx->nr) {
    if (stmt->no_total) {
      if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
      if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
    } else {
      if (args->IndPtr) *args->IndPtr = 0;
      if (args->StrLenPtr) *args->StrLenPtr = ctx->nr;
    }
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:Column[%d], #%zd out of #%zd bytes consumed",
        args->Col_or_Param_Num, n, n + ctx->nr);
    ctx->pos += n;
    return SQL_SUCCESS_WITH_INFO;
  }

  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = n;
  ctx->pos = (const char*)-1;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_varbinary(stmt_t *stmt, const unsigned char *s, size_t nr, stmt_get_data_args_t *args)
{
  (void)s;
  (void)nr;
  // SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BINARY:
      return _stmt_get_data_copy_varbinary_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_geometry(stmt_t *stmt, const unsigned char *s, size_t nr, stmt_get_data_args_t *args)
{
  (void)s;
  (void)nr;
  // SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BINARY:
      return _stmt_get_data_copy_geometry_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_timestamp(stmt_t *stmt, int64_t v, int precision, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!v;
      break;
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_SHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    case SQL_C_TYPE_TIMESTAMP:
      if (tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(v, precision, (SQL_TIMESTAMP_STRUCT*)args->TargetValuePtr)) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_int64(stmt_t *stmt, int64_t v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!v;
      break;
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SHORT:
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_LONG:
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    case SQL_C_TYPE_TIMESTAMP:
      if (tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(v, 0, (SQL_TIMESTAMP_STRUCT*)args->TargetValuePtr)) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_uint64(stmt_t *stmt, uint64_t v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!v;
      break;
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SHORT:
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_LONG:
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_double(stmt_t *stmt, double v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!(uint8_t)v;
      break;
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = (int64_t)v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = (uint64_t)v;
      break;
    case SQL_C_FLOAT:
      *(float*)args->TargetValuePtr = (float)v;
      break;
    case SQL_C_DOUBLE:
      *(double*)args->TargetValuePtr = v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  if (tsdb->is_null) {
    if (args->IndPtr) {
      *args->IndPtr = SQL_NULL_DATA;
      return SQL_SUCCESS;
    }
    stmt_append_err_format(stmt, "22002", 0, "Indicator variable required but not supplied:#%d Column_or_Param", args->Col_or_Param_Num);
    return SQL_ERROR;
  }

  switch(tsdb->type) {
    case TSDB_DATA_TYPE_BOOL:
      return _stmt_get_data_copy_uint64(stmt, tsdb->b, args);
    case TSDB_DATA_TYPE_TINYINT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i8, args);
    case TSDB_DATA_TYPE_UTINYINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u8, args);
    case TSDB_DATA_TYPE_SMALLINT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i16, args);
    case TSDB_DATA_TYPE_USMALLINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u16, args);
    case TSDB_DATA_TYPE_INT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i32, args);
    case TSDB_DATA_TYPE_UINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u32, args);
    case TSDB_DATA_TYPE_BIGINT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i64, args);
    case TSDB_DATA_TYPE_UBIGINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u64, args);
    case TSDB_DATA_TYPE_FLOAT:
      return _stmt_get_data_copy_double(stmt, tsdb->flt, args);
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_get_data_copy_double(stmt, tsdb->dbl, args);
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_get_data_copy_varchar(stmt, tsdb->str.str, tsdb->str.len, args);
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_get_data_copy_timestamp(stmt, tsdb->ts.ts, tsdb->ts.precision, args);
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_get_data_copy_nchar(stmt, tsdb->str.str, tsdb->str.len, args);
    case TSDB_DATA_TYPE_JSON:
      return _stmt_get_data_copy_json(stmt, tsdb->str.str, tsdb->str.len, args);
    case TSDB_DATA_TYPE_VARBINARY:
      return _stmt_get_data_copy_varbinary(stmt, tsdb->bin.bin, tsdb->bin.len, args);
    case TSDB_DATA_TYPE_GEOMETRY:
      return _stmt_get_data_copy_geometry(stmt, tsdb->geo.geo, tsdb->geo.len, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_get_data_x(stmt_t *stmt, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  if (ctx->Col_or_Param_Num != args->Col_or_Param_Num) {
    ctx->Col_or_Param_Num = args->Col_or_Param_Num;
    ctx->TargetType       = args->TargetType;

    sr = _stmt_get_data_prepare_ctx(stmt, args);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
  }

  if (ctx->TargetType != args->TargetType) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:TargetType changes in successive SQLGetData call for #%d Column_or_Param", args->Col_or_Param_Num);
    return SQL_ERROR;
  }

  return _stmt_get_data_copy(stmt, args);
}

SQLRETURN _stmt_get_data_impl(
    stmt_t        *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (Col_or_Param_Num == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  SQLSMALLINT ColumnCount;
  sr = stmt_get_col_count(stmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (Col_or_Param_Num < 1 || Col_or_Param_Num > ColumnCount) {
    stmt_append_err_format(stmt, "07009", 0, "Invalid descriptor index:#%d Col_or_Param, %d ColumnCount", Col_or_Param_Num, ColumnCount);
    return SQL_ERROR;
  }

  size_t row_array_size = _stmt_get_row_array_size(stmt);
  if (row_array_size > 1) {
    stmt_append_err(stmt, "HY109", 0, "Invalid cursor position:The cursor was a forward-only cursor, and the rowset size was greater than one");
    return SQL_ERROR;
  }

  stmt_get_data_args_t args = {
    .Col_or_Param_Num           = Col_or_Param_Num,
    .TargetType                 = TargetType,
    .TargetValuePtr             = TargetValuePtr,
    .BufferLength               = BufferLength,
    .StrLenPtr                  = StrLen_or_IndPtr,
    .IndPtr                     = StrLen_or_IndPtr,
  };

  sr = _stmt_get_data_x(stmt, &args);
  return sr;
}

SQLRETURN stmt_get_data(
    stmt_t        *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  SQLRETURN sr = SQL_SUCCESS;

#ifdef USE_TICK_TO_DEBUG                 /* { */
  int r = 0;
  r = _stmt_enter_SQLGetData(stmt);
  if (r) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }
#endif                                   /* } */

  sr = _stmt_get_data_impl(stmt, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
#ifdef USE_TICK_TO_DEBUG                 /* { */
  _stmt_leave_SQLGetData(stmt);
#endif                                   /* } */

  return sr;
}

static SQLRETURN _stmt_fetch_row(stmt_t *stmt)
{
  return stmt->base->fetch_row(stmt->base);
}

static SQLRETURN _stmt_fill_col(stmt_t *stmt, size_t i_row, size_t i_col)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (i_col >= ARD_header->DESC_COUNT) return SQL_SUCCESS;

  desc_record_t *ARD_record = ARD->records + i_col;
  if (ARD_record->DESC_DATA_PTR == NULL) return SQL_SUCCESS;

  char *dest = _stmt_get_address(stmt, ARD_record->DESC_DATA_PTR, ARD_record->DESC_OCTET_LENGTH, i_row, ARD_header);
  SQLLEN *StrLenPtr = _stmt_get_address(stmt, ARD_record->DESC_OCTET_LENGTH_PTR, sizeof(SQLLEN), i_row, ARD_header);
  SQLLEN *IndPtr = _stmt_get_address(stmt, ARD_record->DESC_INDICATOR_PTR, sizeof(SQLLEN), i_row, ARD_header);

  SQLSMALLINT    TargetType       = (SQLSMALLINT)ARD_record->DESC_CONCISE_TYPE;
  SQLPOINTER     TargetValuePtr   = dest;
  SQLLEN         BufferLength     = ARD_record->DESC_OCTET_LENGTH;

  stmt_get_data_args_t args = {
    .Col_or_Param_Num           = (SQLUSMALLINT)i_col + 1,
    .TargetType                 = TargetType,
    .TargetValuePtr             = TargetValuePtr,
    .BufferLength               = BufferLength,
    .StrLenPtr                  = StrLenPtr,
    .IndPtr                     = IndPtr,
  };

  return _stmt_get_data_x(stmt, &args);
}

static SQLRETURN _stmt_fill_row(stmt_t *stmt, size_t i_row)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  int with_info = 0;

  for (int i_col = 0; (size_t)i_col < ARD->cap; ++i_col) {
    if (i_col >= ARD_header->DESC_COUNT) continue;
    desc_record_t *ARD_record = ARD->records + i_col;
    if (!ARD_record->bound) continue;
    if (ARD_record->DESC_DATA_PTR == NULL) continue;

    sr = _stmt_fill_col(stmt, i_row, i_col);

    _get_data_ctx_reset(&stmt->get_data_ctx);

    if (sr == SQL_SUCCESS) continue;
    with_info = 1;
    if (sr == SQL_SUCCESS_WITH_INFO) continue;
    return SQL_ERROR;
  }

  return with_info ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

static SQLRETURN _stmt_fetch_rows(stmt_t *stmt, const size_t row_array_size, size_t *nr_rows)
{
  SQLRETURN sr = SQL_SUCCESS;
  SQLRETURN sr_row = SQL_ROW_SUCCESS;

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  size_t i_row = 0;

  *nr_rows = 0;

again:

  sr = _stmt_fetch_row(stmt);
  if (sr == SQL_NO_DATA) {
    if (*nr_rows == 0) return SQL_NO_DATA;
    return SQL_SUCCESS;
  }
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = _stmt_fill_row(stmt, i_row);
  switch (sr) {
    case SQL_SUCCESS:
      sr_row = SQL_ROW_SUCCESS;
      break;
    case SQL_SUCCESS_WITH_INFO:
      sr_row = SQL_ROW_SUCCESS_WITH_INFO;
      break;
    default:
      sr_row = SQL_ROW_ERROR;
      break;
  }

  if (IRD_header->DESC_ARRAY_STATUS_PTR) {
    IRD_header->DESC_ARRAY_STATUS_PTR[i_row] = sr_row;
  }

  *nr_rows = ++i_row;

  if (i_row < row_array_size) goto again;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_fetch_x(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  _get_data_ctx_reset(&stmt->get_data_ctx);

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  size_t row_array_size = _stmt_get_row_array_size(stmt);
  if (row_array_size == 0) row_array_size = 1;

  size_t nr_rows = 0;
  sr = _stmt_fetch_rows(stmt, row_array_size, &nr_rows);

  if (IRD_header->DESC_ROWS_PROCESSED_PTR) *IRD_header->DESC_ROWS_PROCESSED_PTR = nr_rows;

  if (IRD_header->DESC_ARRAY_STATUS_PTR) {
    for (size_t i=nr_rows; i<row_array_size; ++i) {
      IRD_header->DESC_ARRAY_STATUS_PTR[i] = SQL_ROW_NOROW;
    }
  }

  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr == SQL_SUCCESS) return SQL_SUCCESS;
  if (sr == SQL_SUCCESS_WITH_INFO) return SQL_SUCCESS_WITH_INFO;
  return SQL_ERROR;
}

static SQLRETURN _stmt_fetch(stmt_t *stmt)
{
  _get_data_ctx_reset(&stmt->get_data_ctx);

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (ARD_header->DESC_BIND_TYPE != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  return _stmt_fetch_x(stmt);
}

SQLRETURN stmt_fetch_scroll(stmt_t *stmt,
    SQLSMALLINT   FetchOrientation,
    SQLLEN        FetchOffset)
{
  _get_data_ctx_reset(&stmt->get_data_ctx);

  switch (FetchOrientation) {
    case SQL_FETCH_NEXT:
      (void)FetchOffset;
      return _stmt_fetch(stmt);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[%s] not implemented yet",
          sql_fetch_orientation(FetchOrientation));
      return SQL_ERROR;
  }
}

SQLRETURN stmt_fetch(stmt_t *stmt)
{
  return stmt_fetch_scroll(stmt, SQL_FETCH_NEXT, 0);
}

static SQLRETURN _stmt_prepare(stmt_t *stmt)
{
  return stmt->base->prepare(stmt->base, &stmt->current_sql);
}

static SQLRETURN _stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr)
{
  return stmt->base->get_num_params(stmt->base, ParameterCountPtr);
}

SQLRETURN stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr)
{
  return _stmt_get_num_params(stmt, ParameterCountPtr);
}

static SQLRETURN _stmt_describe_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  return stmt->base->describe_param(stmt->base, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
}

SQLRETURN stmt_describe_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (ParameterNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  SQLSMALLINT    nr_params;
  sr = _stmt_get_num_params(stmt, &nr_params);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  int nr = nr_params;
  if (ParameterNumber > nr) {
    stmt_append_err_format(stmt, "07009", 0,
        "Invalid descriptor index:"
        "The value specified for the argument ParameterNumber[%d] was greater than the number of parameters[%d] in the associated SQL statement",
        ParameterNumber, nr);
    return SQL_ERROR;
  }

  return _stmt_describe_param(stmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
}

static SQLRETURN _stmt_sql_c_char_to_tsdb_timestamp(stmt_t *stmt, const char *s, int64_t *timestamp)
{
  char *end;
  errno = 0;
  long long v = strtoll(s, &end, 0);
  int e = errno;
  if (e == ERANGE && (v == LONG_MAX || v == LONG_MIN)) {
    stmt_append_err_format(stmt, "22008", 0,
        "Datetime field overflow:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:[strtol]%s", strerror(e));
    return SQL_ERROR;
  }
  if (e != 0) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:[strtol]%s", strerror(e));
    return SQL_ERROR;
  }
  if (end == s) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:no digits at all");
    return SQL_ERROR;
  }
  if (end && *end) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:string following digits[%s]",
        end);
    return SQL_ERROR;
  }

  *timestamp = v;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_char_to_tsdb_timestamp_x(stmt_t *stmt, const char *src, size_t len, int precision, int64_t *dst)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;

  int64_t v = 0;
  // OW("len:%zd", len);

  char *end = NULL;
  strtol(src, &end, 0);
  if (end && !*end) {
    // TODO: precision
    sr = _stmt_sql_c_char_to_tsdb_timestamp(stmt, src, &v);
    if (sr == SQL_ERROR) return SQL_ERROR;
  } else {
    int64_t tz_default = tod_get_local_timezone();

    ts_parser_param_t param = {0};
    // param.ctx.debug_flex = 1;
    // param.ctx.debug_bison = 1;
    r = ts_parser_parse(src, len, &param, tz_default);
    if (r) {
      stmt_append_err_format(stmt, "22007", 0,
          "Invalid datetime format:timestamp is required, but got ==[%.*s]==", (int)len, src);
      return SQL_ERROR;
    }

    time_t             tm_utc0        = param.tm_utc0;
    unsigned long long frac_nano      = param.frac_nano;

    ts_parser_param_release(&param);

    switch (precision) {
      case 0:       // ms
        v = tm_utc0 * 1000 + frac_nano / 1000000;
        break;
      case 1:       // us
        v = tm_utc0 * 1000000 + frac_nano / 1000;
        break;
      default:      // ns
        v = tm_utc0 * 1000000000 + frac_nano;
        break;
    }
  }

  *dst = v;
  return SQL_SUCCESS;
}

static int _stmt_sql_found(sqls_parser_param_t *param, size_t start, size_t end, int32_t qms, void *arg)
{
  (void)param;

  stmt_t *stmt = (stmt_t*)arg;
  sqls_t *sqls = &stmt->sqls;
  --end;

  if (sqls->nr >= sqls->cap) {
    size_t cap = sqls->cap + 16;
    sqls_parser_nterm_t *nterms = (sqls_parser_nterm_t*)realloc(sqls->sqls, sizeof(*nterms) * cap);
    if (!nterms) {
      stmt_oom(stmt);
      return -1;
    }
    sqls->sqls = nterms;
    sqls->cap  = cap;
  }

  sqls->sqls[sqls->nr].start  = start;
  sqls->sqls[sqls->nr].end    = end;
  sqls->sqls[sqls->nr].qms    = qms;
  sqls->nr += 1;

  return 0;
}

static SQLRETURN _stmt_cache_and_parse(stmt_t *stmt, sqls_parser_param_t *param, const char *sql, size_t len)
{
  int r = 0;

  mem_reset(&stmt->raw);
  _sqls_reset(&stmt->sqls);

  r = mem_keep(&stmt->raw, len + 1);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  memcpy(stmt->raw.base, sql, len);
  stmt->raw.nr = len;
  stmt->raw.base[len] = '\0';

  r = sqls_parser_parse(sql, len, param);
  if (r) {
    parser_loc_t *loc = &param->ctx.bad_token;
    E("location:(%d,%d)->(%d,%d)",
        loc->first_line, loc->first_column, loc->last_line, loc->last_column);
    E("failed:%s", param->ctx.err_msg);
    stmt_append_err_format(stmt, "HY000", 0, "General error:parsing:%.*s", (int)len, sql);
    stmt_append_err_format(stmt, "HY000", 0, "General error:location:(%d,%d)->(%d,%d)",
        loc->first_line, loc->first_column, loc->last_line, loc->last_column);
    stmt_append_err_format(stmt, "HY000", 0, "General error:failed:%.*s", (int)strlen(param->ctx.err_msg), param->ctx.err_msg);

    return SQL_ERROR;
  } else if (stmt->sqls.failed) {
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_next_sql(stmt_t *stmt)
{
  int r = 0;

  sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;

  memset(&stmt->current_sql, 0, sizeof(stmt->current_sql));

  sqls_t *sqls = &stmt->sqls;
  if (sqls->pos + 1 > sqls->nr) return SQL_NO_DATA;

  sqls_parser_nterm_t *nterms = stmt->sqls.sqls + stmt->sqls.pos;
  sqlc_tsdb->sqlc             = (const char*)stmt->raw.base + nterms->start;
  sqlc_tsdb->sqlc_bytes       = nterms->end - nterms->start;
  sqlc_tsdb->qms              = nterms->qms;

  if (sqls->nr > 1) {
    if (sqlc_tsdb->qms > 0) {
      stmt_append_err(stmt, "HY000", 0, "General error:parameterized-statement in batch-statements not supported yet");
      return SQL_ERROR;
    }
  }

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  string_t src = {
    .charset             = fromcode,
    .str                 = sqlc_tsdb->sqlc,
    .bytes               = sqlc_tsdb->sqlc_bytes,
  };
  mem_reset(&stmt->tsdb_sql);
  r = mem_conv_ex(&stmt->tsdb_sql, &src, tocode);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory or conversion failed", fromcode, tocode);
    memset(&stmt->current_sql, 0, sizeof(stmt->current_sql));
    return SQL_ERROR;
  }

  sqlc_tsdb->tsdb        = (const char*)stmt->tsdb_sql.base;
  sqlc_tsdb->tsdb_bytes  = stmt->tsdb_sql.nr;

  ++sqls->pos;

  stmt->tsdb_stmt.params.qms = sqlc_tsdb->qms;

  const char *start = sqlc_tsdb->tsdb;
  const char *end   = sqlc_tsdb->tsdb + sqlc_tsdb->tsdb_bytes;
  if (end > start && start[0] == '!') {
    stmt->tsdb_stmt.is_ext = 1;
  } else {
    stmt->tsdb_stmt.is_ext = 0;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_double_sql_timestamp(stmt_t *stmt, const double dbl, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t *IPD_record = param_state->IPD_record;
  sql_data_t    *data       = &param_state->sql_data;

  data->ts.sec = (int64_t)dbl;
  data->ts.nsec = (int64_t)((dbl - data->ts.sec) * 1000000000);
  data->ts.is_i64 = 0;
  data->type    = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_sqlc_char_sql_varchar(stmt_t *stmt, const char *s, size_t n, param_state_t *param_state)
{
  int r = 0;

  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *IPD_record = param_state->IPD_record;
  sql_data_t    *data       = &param_state->sql_data;
  mem_t         *mem        = &data->mem;

  if (0 && n > (size_t)IPD_record->DESC_LENGTH) {
    // FIXME: flaw found by `go` on windows
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:param[%d,%d]:[%zd]%.*s, but %s(%zd) was specified previously by SQLBindParameter",
        i_row+1, i_param+1,
        n, (int)n, s,
        sql_data_type(IPD_record->DESC_CONCISE_TYPE), (size_t)IPD_record->DESC_LENGTH);
    return SQL_ERROR;
  }

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  data->type    = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  data->bin.bin = mem->base;
  data->bin.len = mem->nr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_sqlc_binary_sql_varbinary(stmt_t *stmt, const unsigned char *s, size_t n, param_state_t *param_state)
{
  int r = 0;

  desc_record_t *IPD_record = param_state->IPD_record;
  sql_data_t    *data       = &param_state->sql_data;
  mem_t         *mem        = &data->mem;

  r = mem_copy_bin(mem, s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  data->type    = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  data->str.str = (const char*)mem->base;
  data->str.len = mem->nr;
  OA_NIY(data->str.str[data->str.len] == '\0');

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_sqlc_char_sql_wvarchar(stmt_t *stmt, const char *s, size_t n, param_state_t *param_state)
{
  int r = 0;

  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *IPD_record = param_state->IPD_record;
  sql_data_t    *data       = &param_state->sql_data;
  mem_t         *mem        = &data->mem;

  charset_conv_t *cnv  = param_state->charset_convs.cnv_from_sqlc_charset_for_param_bind_to_wchar;
  const char *fromcode = cnv->from;
  const char *tocode   = cnv->to;

  r = mem_conv(mem, cnv->cnv, s, n);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:failed to convert `%.*s` from `%s` to `%s`",
        (int)n, s, fromcode, tocode);
    return SQL_ERROR;
  }

  if ((mem->nr/2) > (size_t)IPD_record->DESC_LENGTH) {
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:param[%d,%d]:[%zd]%.*s, but %s(%zd) was specified previously by SQLBindParameter",
        i_row+1, i_param+1,
        n, (int)n, s,
        sql_data_type(IPD_record->DESC_CONCISE_TYPE), (size_t)IPD_record->DESC_LENGTH);
    return SQL_ERROR;
  }

  data->type      = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  data->wstr.wstr = (const char*)mem->base;
  data->wstr.wlen = mem->nr / 2;
  int16_t *p = (int16_t*)data->wstr.wstr;
  OA_NIY(p[data->wstr.wlen] == 0);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_sqlc_wchar_sql_wvarchar(stmt_t *stmt, const char *wstr, size_t wlen, param_state_t *param_state)
{
  int r = 0;

  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *IPD_record = param_state->IPD_record;
  sql_data_t    *data       = &param_state->sql_data;
  mem_t         *mem        = &data->mem;

  charset_conv_t *cnv  = param_state->charset_convs.cnv_from_wchar_to_wchar;
  const char *fromcode = cnv->from;
  const char *tocode   = cnv->to;

  r = mem_conv(mem, cnv->cnv, wstr, wlen * 2);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:failed to convert param[%d,%d] from `%s` to `%s`",
        i_row+1, i_param+1, fromcode, tocode);
    return SQL_ERROR;
  }

  if ((mem->nr/2) > (size_t)IPD_record->DESC_LENGTH) {
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:param[%d,%d]:[%zd] larger than %s(%zd) specified previously by SQLBindParameter",
        i_row+1, i_param+1, (mem->nr/2),
        sql_data_type(IPD_record->DESC_CONCISE_TYPE), (size_t)IPD_record->DESC_LENGTH);
    return SQL_ERROR;
  }

  data->type      = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  data->wstr.wstr = (const char*)mem->base;
  data->wstr.wlen = mem->nr / 2;
  int16_t *p = (int16_t*)data->wstr.wstr;
  OA_NIY(p[data->wstr.wlen] == 0);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_sqlc_wchar_sql_varchar(stmt_t *stmt, const char *wstr, size_t wlen, param_state_t *param_state)
{
  int r = 0;

  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *IPD_record = param_state->IPD_record;
  sql_data_t    *data       = &param_state->sql_data;
  mem_t         *mem        = &data->mem;

  charset_conv_t *cnv  = param_state->charset_convs.cnv_from_wchar_to_sqlc;
  const char *fromcode = cnv->from;
  const char *tocode   = cnv->to;

  r = mem_conv(mem, cnv->cnv, wstr, wlen * 2);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:failed to convert param[%d,%d] from `%s` to `%s`",
        i_row+1, i_param+1, fromcode, tocode);
    return SQL_ERROR;
  }

  if (0 && mem->nr > (size_t)IPD_record->DESC_LENGTH) {
    // FIXME: flaw found by `go` on windows
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:param[%d,%d]:[%zd] larger than %s(%zd) specified previously by SQLBindParameter",
        i_row+1, i_param+1, mem->nr,
        sql_data_type(IPD_record->DESC_CONCISE_TYPE), (size_t)IPD_record->DESC_LENGTH);
    return SQL_ERROR;
  }

  data->type      = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  data->str.str   = (const char*)mem->base;
  data->str.len   = mem->nr;
  OA_NIY(data->str.str[data->str.len] == '\0');

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_copy_to_sql_timestamp(stmt_t *stmt, const char *src, size_t len, param_state_t *param_state)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024];
  int n = snprintf(buf, sizeof(buf), "%.*s", (int)len, src);
  if (n < 0 || (size_t)n >= sizeof(buf)) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is too long, but got ==[%.*s]==", (int)len, src);
    return SQL_ERROR;
  }
  src = buf;
  len = (size_t)n;

  sql_data_t    *data       = &param_state->sql_data;

  data->type = SQL_TYPE_TIMESTAMP;

  int64_t v = 0;

  char *end = NULL;
  strtol(src, &end, 0);
  if (end && !*end) {
    // TODO: precision
    sr = _stmt_sql_c_char_to_tsdb_timestamp(stmt, src, &v);
    if (sr == SQL_ERROR) return SQL_ERROR;
    data->ts.i64 = v;
    data->ts.is_i64 = 1;
    return SQL_SUCCESS;
  }

  int64_t tz_default = tod_get_local_timezone();

  ts_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  r = ts_parser_parse(src, len, &param, tz_default);
  if (r) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%.*s]==", (int)len, src);
    return SQL_ERROR;
  }

  time_t             tm_utc0        = param.tm_utc0;
  unsigned long long frac_nano      = param.frac_nano;

  ts_parser_param_release(&param);

  data->ts.sec    = tm_utc0;
  data->ts.nsec   = frac_nano;
  data->ts.is_i64 = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_dummy(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;
  (void)param_state;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_sbigint_sql_integer(stmt_t *stmt, param_state_t *param_state)
{
  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *APD_record = param_state->APD_record;
  desc_record_t *IPD_record = param_state->IPD_record;
  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  int64_t v = sqlc_data->i64;
  if (v > INT_MAX || v < INT_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1,
        v);
    return SQL_ERROR;
  }

  data->i32 = (int32_t)v;

  param_state->sql_data.type = SQL_INTEGER;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_sbigint_sql_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *APD_record = param_state->APD_record;
  desc_record_t *IPD_record = param_state->IPD_record;
  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  int64_t v = sqlc_data->i64;
  if (v > INT16_MAX || v < INT16_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1,
        v);
    return SQL_ERROR;
  }

  data->type = SQL_SMALLINT;
  data->i16  = (int16_t)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_sbigint_sql_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int            i_row      = param_state->i_row;
  int            i_param    = param_state->i_param;
  desc_record_t *APD_record = param_state->APD_record;
  desc_record_t *IPD_record = param_state->IPD_record;
  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  int64_t v = sqlc_data->i64;
  if (v > INT8_MAX || v < INT8_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1,
        v);
    return SQL_ERROR;
  }

  data->type = SQL_TINYINT;
  data->i8   = (int8_t)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_sbigint_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  int64_t v = sqlc_data->i64;
  char buf[64];
  snprintf(buf, sizeof(buf), "%" PRId64 "", v);

  sr = _stmt_param_copy_sqlc_char_sql_varchar(stmt, buf, strlen(buf), param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  data->type = SQL_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_tinyint_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  int8_t v = sqlc_data->i8;
  char buf[64];
  snprintf(buf, sizeof(buf), "%d", v);

  sr = _stmt_param_copy_sqlc_char_sql_varchar(stmt, buf, strlen(buf), param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  data->type = SQL_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_slong_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  int8_t v = sqlc_data->i32;
  char buf[64];
  snprintf(buf, sizeof(buf), "%d", v);

  sr = _stmt_param_copy_sqlc_char_sql_varchar(stmt, buf, strlen(buf), param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  data->type = SQL_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  sr = _stmt_param_copy_sqlc_char_sql_varchar(stmt, s, n, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  param_state->sql_data.type = SQL_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_binary_sql_varbinary(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  const unsigned char *s = param_state->sqlc_data.bin.bin;
  size_t               n = param_state->sqlc_data.bin.len;

  sr = _stmt_param_copy_sqlc_binary_sql_varbinary(stmt, s, n, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  param_state->sql_data.type = SQL_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_wvarchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  sr = _stmt_param_copy_sqlc_char_sql_wvarchar(stmt, s, n, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  param_state->sql_data.type = SQL_WVARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  return _stmt_param_copy_to_sql_timestamp(stmt, s, n, param_state);
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  char *end = NULL;
  long long ll = strtoll((const char*)mem->base, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_BIGINT` failed:invalid character[0x%02x]",
        (const char*)mem->base, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_BIGINT` failed:overflow or underflow occurs",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_BIGINT;
  param_state->sql_data.i64  = ll;
  param_state->sql_data.unsigned_ = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_integer(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  char *end = NULL;
  long long ll = strtoll((const char*)mem->base, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_INTEGER` failed:invalid character[0x%02x]",
        (const char*)mem->base, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_INTEGER` failed:overflow or underflow occurs",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  if (ll > UINT_MAX) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:conversion from `%s` to `SQL_INTEGER` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }
  if (ll < INT_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:conversion from `%s` to `SQL_INTEGER` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_INTEGER;
  if (ll > INT_MAX) {
    param_state->sql_data.u32  = (uint32_t)ll;
    param_state->sql_data.unsigned_ = 1;
  } else {
    param_state->sql_data.i32  = (int32_t)ll;
    param_state->sql_data.unsigned_ = 0;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  char *end = NULL;
  long long ll = strtoll((const char*)mem->base, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_SMALLINT` failed:invalid character[0x%02x]",
        (const char*)mem->base, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_SMALLINT` failed:overflow or underflow occurs",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  if (ll > UINT16_MAX) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:conversion from `%s` to `SQL_SMALLINT` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }
  if (ll < INT16_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:conversion from `%s` to `SQL_SMALLINT` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_SMALLINT;
  if (ll > INT16_MAX) {
    param_state->sql_data.u16  = (uint16_t)ll;
    param_state->sql_data.unsigned_ = 1;
  } else {
    param_state->sql_data.i16  = (int16_t)ll;
    param_state->sql_data.unsigned_ = 0;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  char *end = NULL;
  long long ll = strtoll((const char*)mem->base, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_TINYINT` failed:invalid character[0x%02x]",
        (const char*)mem->base, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_TINYINT` failed:overflow or underflow occurs",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  if (ll > UINT8_MAX) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:conversion from `%s` to `SQL_TINYINT` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }
  if (ll < INT8_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:conversion from `%s` to `SQL_TINYINT` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_TINYINT;
  if (ll > INT8_MAX) {
    param_state->sql_data.u8  = (uint8_t)ll;
    param_state->sql_data.unsigned_ = 1;
  } else {
    param_state->sql_data.i8  = (int8_t)ll;
    param_state->sql_data.unsigned_ = 0;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_bit(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  char *end = NULL;
  long long ll = strtoll((const char*)mem->base, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_BIT` failed:invalid character[0x%02x]",
        (const char*)mem->base, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_BIT` failed:overflow or underflow occurs",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_BIT;
  param_state->sql_data.b  = !!ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_double(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  double v = 0.;
  r = sscanf((const char*)mem->base, "%lg", &v);
  if (r != 1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_DOUBLE` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_DOUBLE;
  param_state->sql_data.dbl = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_char_sql_float(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *s = param_state->sqlc_data.str.str;
  size_t      n = param_state->sqlc_data.str.len;

  mem_t *mem = &param_state->tmp;

  r = mem_copy_bin(mem, (const unsigned char*)s, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  float v = 0.;
  r = sscanf((const char*)mem->base, "%g", &v);
  if (r != 1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `SQL_FLOAT` failed",
        (const char*)mem->base);
    return SQL_ERROR;
  }

  param_state->sql_data.type = SQL_FLOAT;
  param_state->sql_data.flt = (float)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_double_sql_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;

  double v = sqlc_data->dbl;
  char buf[128];
  snprintf(buf, sizeof(buf), "%" PRId64 "", (int64_t)v);

  sr = _stmt_param_copy_double_sql_timestamp(stmt, v, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  param_state->sql_data.type = SQL_TYPE_TIMESTAMP;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_double_sql_double(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  data->dbl  = sqlc_data->dbl;
  data->type = SQL_DOUBLE;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_float_sql_real(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  data->flt  = sqlc_data->flt;
  data->type = SQL_FLOAT;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_double_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;

  double v = sqlc_data->dbl;
  char buf[128];
  snprintf(buf, sizeof(buf), "%lg", v);

  sr = _stmt_param_copy_sqlc_char_sql_varchar(stmt, buf, strlen(buf), param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  param_state->sql_data.type = SQL_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_double_sql_real(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t   *sqlc_data  = &param_state->sqlc_data;
  sql_data_t    *data       = &param_state->sql_data;

  double v = sqlc_data->dbl;

  if (v > FLT_MAX || v < -FLT_MAX) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `SQL_DOUBLE` to `SQL_FLOAT` failed,numeric value out of range for SQL_REAL,value:%lf", v);
    return SQL_ERROR;
  }

  if ((v != 0) && (fabs(v) < FLT_MIN)) {
    stmt_append_err_format(stmt, "HY000", 0,
        "Warning: value too small for SQL_REAL, will be rounded to 0,value:%lf", v);
    v = 0.0;
  }

  data->flt  = (float)v;
  data->type = SQL_REAL;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_wchar_sql_wvarchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *wstr = param_state->sqlc_data.wstr.wstr;
  size_t      wlen = param_state->sqlc_data.wstr.wlen;

  sr = _stmt_param_copy_sqlc_wchar_sql_wvarchar(stmt, wstr, wlen, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_wchar_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *wstr = param_state->sqlc_data.wstr.wstr;
  size_t      wlen = param_state->sqlc_data.wstr.wlen;

  sr = _stmt_param_copy_sqlc_wchar_sql_varchar(stmt, wstr, wlen, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_sqlc_wchar_sql_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  const char *wstr = param_state->sqlc_data.wstr.wstr;
  size_t      wlen = param_state->sqlc_data.wstr.wlen;

  const char *fromcode = "UTF-16LE";
  const char *tocode   = "UTF-8";

  mem_t *mem = &param_state->tmp;
  r = mem_iconv(mem, fromcode, tocode, wstr, wlen*2);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  const char *s = (const char*)mem->base;
  size_t      n = mem->nr;

  return _stmt_param_copy_to_sql_timestamp(stmt, s, n, param_state);
}

static SQLRETURN _stmt_guess_tsdb_params_for_sql_c_char(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  // const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  // const char *tocode   = conn_get_tsdb_charset(stmt->conn);

  size_t BufferLength = (size_t)APD_record->DESC_OCTET_LENGTH;

  tsdb_field->bytes = (int32_t)BufferLength * 8 + 2;   // *8: hard-coded
  if (BufferLength == 0 && param_state->nr_batch_size == 1) {
    // NOTE: this was discovered via `rust odbc`
    size_t ColumnSize   = (size_t)IPD_record->DESC_LENGTH;
    tsdb_field->bytes = (int32_t)ColumnSize * 8 + 2; // +2: consistent with taosc
  }
  tsdb_field->type = TSDB_DATA_TYPE_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_params_for_sql_c_wchar(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  // FIXME: more test cases to pass
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  // const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  // const char *tocode   = conn_get_tsdb_charset(stmt->conn);

  size_t BufferLength = (size_t)APD_record->DESC_OCTET_LENGTH;

  tsdb_field->bytes = (int32_t)BufferLength * 8 + 2;   // *8: hard-coded
  if (BufferLength == 0 && param_state->nr_batch_size == 1) {
    // NOTE: this was discovered via `rust odbc`
    size_t ColumnSize   = (size_t)IPD_record->DESC_LENGTH;
    tsdb_field->bytes = (int32_t)ColumnSize * 8 + 2; // +2: consistent with taosc
  }
  tsdb_field->type = TSDB_DATA_TYPE_VARCHAR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_sbigint(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type = TSDB_DATA_TYPE_BIGINT;
  tsdb_field->bytes = sizeof(int64_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_char(stmt_t *stmt, param_state_t *param_state)
{
  return _stmt_guess_tsdb_params_for_sql_c_char(stmt, param_state);
}

static SQLRETURN _stmt_param_guess_sqlc_binary(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type = TSDB_DATA_TYPE_VARBINARY;
  tsdb_field->bytes = sizeof(int64_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_double(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type      = TSDB_DATA_TYPE_DOUBLE;
  tsdb_field->bytes     = sizeof(double);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_float(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type      = TSDB_DATA_TYPE_FLOAT;
  tsdb_field->bytes     = sizeof(float);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_slong(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type = TSDB_DATA_TYPE_INT;
  tsdb_field->bytes = sizeof(int32_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_long(stmt_t *stmt, param_state_t *param_state)
{
  return _stmt_param_guess_sqlc_slong(stmt, param_state);
}

static SQLRETURN _stmt_param_guess_sqlc_short(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type = TSDB_DATA_TYPE_SMALLINT;
  tsdb_field->bytes = sizeof(int16_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->type = TSDB_DATA_TYPE_SMALLINT;
  tsdb_field->bytes = sizeof(int8_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess_sqlc_wchar(stmt_t *stmt, param_state_t *param_state)
{
  return _stmt_guess_tsdb_params_for_sql_c_wchar(stmt, param_state);
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_char(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = BufferLength;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_binary(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = BufferLength;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_sbigint(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 8;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_double(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 8;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_float(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 4;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_slong(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 4;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_wchar(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = BufferLength;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_long(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 4;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_short(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 2;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_APD_record_sqlc_tinyint(stmt_t* stmt,
    desc_record_t  *APD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     ValueType,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)BufferLength;

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = 1;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_varchar(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = ColumnSize;
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_varbinary(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = ColumnSize;
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_timestamp(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;

  if ( (!(ColumnSize == 16 && DecimalDigits == 0)) &&
       (!(ColumnSize == 19 && DecimalDigits == 0)) &&
       (!((DecimalDigits == 3 || DecimalDigits == 6 || DecimalDigits == 9) && (ColumnSize == 20 + (size_t)DecimalDigits))) )
  {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:#%d Parameter[SQL_TYPE_TIMESTAMP(%zd.%d)] not implemented yet",
        ParameterNumber, ColumnSize, DecimalDigits);
    return SQL_ERROR;
  }

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = SQL_DATETIME;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = ColumnSize;
  IPD_record->DESC_PRECISION           = DecimalDigits;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_wvarchar(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = ColumnSize;
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_bigint(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)ColumnSize;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = 19; // FIXME: 19 (if signed) or 20 (if unsigned)
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_integer(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)ColumnSize;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = 10;
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_double(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)ColumnSize;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = 15;
  IPD_record->DESC_PRECISION           = DecimalDigits; // FIXME:

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_real(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)ColumnSize;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = 7;
  IPD_record->DESC_PRECISION           = DecimalDigits; // FIXME:

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_smallint(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)ColumnSize;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = 5;
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_bind_set_IPD_record_sql_tinyint(stmt_t* stmt,
    desc_record_t  *IPD_record,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits)
{
  (void)stmt;
  (void)ParameterNumber;
  (void)ColumnSize;
  (void)DecimalDigits;

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  IPD_record->DESC_LENGTH              = 3;
  IPD_record->DESC_PRECISION           = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_char(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;
  size_t         len           = param_state->sqlc_len;

  sqlc_data->str.str = base;
  sqlc_data->str.len = len;
  if (sqlc_data->str.len == (size_t)SQL_NTS) sqlc_data->str.len = strlen(sqlc_data->str.str);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_binary(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t          *sqlc_data  = &param_state->sqlc_data;
  const unsigned char  *base       = (const unsigned char*)param_state->sqlc_base;
  size_t                len        = param_state->sqlc_len;

  sqlc_data->bin.bin = base;
  sqlc_data->bin.len = len;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_sbigint(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->i64 = *(int64_t*)base;
  // OW("sbigint:%" PRId64 "", sqlc_data->i64);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_double(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->dbl = *(double*)base;
  // OW("double:%lg", sqlc_data->dbl);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_float(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->flt = *(float*)base;
  // OW("float:%lg", sqlc_data->flt);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_slong(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->i32 = *(int32_t*)base;
  // OW("slong:%d", sqlc_data->i32);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_wchar(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;
  size_t         len           = param_state->sqlc_len;

  sqlc_data->wstr.wstr = base;
  sqlc_data->wstr.wlen = len;
  if (sqlc_data->wstr.wlen == (size_t)SQL_NTS) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:SQL_C_WCHAR length [%zd] invalid", sqlc_data->wstr.wlen);
    return SQL_ERROR;
  }
  if (sqlc_data->wstr.wlen % 2) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:SQL_C_WCHAR length [%zd] invalid", sqlc_data->wstr.wlen);
    return SQL_ERROR;
  }
  sqlc_data->wstr.wlen /= 2;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_long(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->i32 = *(int32_t*)base;
  // OW("long:%d", sqlc_data->i32);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_short(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->i16 = *(int16_t*)base;
  // OW("short:%d", sqlc_data->i16);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_get_sqlc_tinyint(stmt_t* stmt, param_state_t *param_state)
{
  (void)stmt;

  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  const char    *base          = param_state->sqlc_base;

  sqlc_data->i8 = *(int8_t*)base;
  // OW("tinyint:%d", sqlc_data->i8);

  return SQL_SUCCESS;
}

static const sqlc_sql_map_t          _sqlc_sql_map[] = {
  {SQL_C_CHAR, SQL_VARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_varchar,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_varchar,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_WVARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_wvarchar,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_wvarchar,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_TYPE_TIMESTAMP,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_timestamp,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_timestamp,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_BIGINT,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_bigint,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_INTEGER,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_integer,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_SMALLINT,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_smallint,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_TINYINT,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_tinyint,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_BIT,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_bit,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_DOUBLE,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_double,
    _stmt_param_guess_sqlc_char},
  {SQL_C_CHAR, SQL_FLOAT,
    _stmt_param_bind_set_APD_record_sqlc_char,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_char,
    _stmt_param_check_sqlc_char_sql_float,
    _stmt_param_guess_sqlc_char},


  {SQL_C_WCHAR, SQL_WVARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_wchar,
    _stmt_param_bind_set_IPD_record_sql_wvarchar,
    _stmt_param_get_sqlc_wchar,
    _stmt_param_check_sqlc_wchar_sql_wvarchar,
    _stmt_param_guess_sqlc_wchar},
  {SQL_C_WCHAR, SQL_VARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_wchar,
    _stmt_param_bind_set_IPD_record_sql_varchar,
    _stmt_param_get_sqlc_wchar,
    _stmt_param_check_sqlc_wchar_sql_varchar,
    _stmt_param_guess_sqlc_wchar},
  {SQL_C_WCHAR, SQL_TYPE_TIMESTAMP,
    _stmt_param_bind_set_APD_record_sqlc_wchar,
    _stmt_param_bind_set_IPD_record_sql_timestamp,
    _stmt_param_get_sqlc_wchar,
    _stmt_param_check_sqlc_wchar_sql_timestamp,
    _stmt_param_guess_sqlc_wchar},

  {SQL_C_SBIGINT, SQL_TYPE_TIMESTAMP,
    _stmt_param_bind_set_APD_record_sqlc_sbigint,
    _stmt_param_bind_set_IPD_record_sql_timestamp,
    _stmt_param_get_sqlc_sbigint,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_sbigint},
  {SQL_C_SBIGINT, SQL_BIGINT,
    _stmt_param_bind_set_APD_record_sqlc_sbigint,
    _stmt_param_bind_set_IPD_record_sql_bigint,
    _stmt_param_get_sqlc_sbigint,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_sbigint},
  {SQL_C_SBIGINT, SQL_INTEGER,
    _stmt_param_bind_set_APD_record_sqlc_sbigint,
    _stmt_param_bind_set_IPD_record_sql_integer,
    _stmt_param_get_sqlc_sbigint,
    _stmt_param_check_sqlc_sbigint_sql_integer,
    _stmt_param_guess_sqlc_sbigint},
  {SQL_C_SBIGINT, SQL_VARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_sbigint,
    _stmt_param_bind_set_IPD_record_sql_varchar,
    _stmt_param_get_sqlc_sbigint,
    _stmt_param_check_sqlc_sbigint_sql_varchar,
    _stmt_param_guess_sqlc_sbigint},
  {SQL_C_SBIGINT, SQL_SMALLINT,
    _stmt_param_bind_set_APD_record_sqlc_sbigint,
    _stmt_param_bind_set_IPD_record_sql_smallint,
    _stmt_param_get_sqlc_sbigint,
    _stmt_param_check_sqlc_sbigint_sql_smallint,
    _stmt_param_guess_sqlc_sbigint},
  {SQL_C_SBIGINT, SQL_TINYINT,
    _stmt_param_bind_set_APD_record_sqlc_sbigint,
    _stmt_param_bind_set_IPD_record_sql_tinyint,
    _stmt_param_get_sqlc_sbigint,
    _stmt_param_check_sqlc_sbigint_sql_tinyint,
    _stmt_param_guess_sqlc_sbigint},

  {SQL_C_SLONG, SQL_INTEGER,
    _stmt_param_bind_set_APD_record_sqlc_slong,
    _stmt_param_bind_set_IPD_record_sql_integer,
    _stmt_param_get_sqlc_slong,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_slong},
  {SQL_C_LONG, SQL_INTEGER,
    _stmt_param_bind_set_APD_record_sqlc_long,
    _stmt_param_bind_set_IPD_record_sql_integer,
    _stmt_param_get_sqlc_long,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_long},
  {SQL_C_SLONG, SQL_VARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_slong,
    _stmt_param_bind_set_IPD_record_sql_varchar,
    _stmt_param_get_sqlc_slong,
    _stmt_param_check_sqlc_slong_sql_varchar,
    _stmt_param_guess_sqlc_slong},

  {SQL_C_SHORT, SQL_SMALLINT,
    _stmt_param_bind_set_APD_record_sqlc_short,
    _stmt_param_bind_set_IPD_record_sql_smallint,
    _stmt_param_get_sqlc_short,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_short},

  {SQL_C_SHORT, SQL_BIT,
    _stmt_param_bind_set_APD_record_sqlc_short,
    _stmt_param_bind_set_IPD_record_sql_tinyint,
    _stmt_param_get_sqlc_short,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_short},  

  {SQL_C_STINYINT, SQL_TINYINT,
    _stmt_param_bind_set_APD_record_sqlc_tinyint,
    _stmt_param_bind_set_IPD_record_sql_tinyint,
    _stmt_param_get_sqlc_tinyint,
    _stmt_param_check_dummy,
    _stmt_param_guess_sqlc_tinyint},
  {SQL_C_STINYINT, SQL_VARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_tinyint,
    _stmt_param_bind_set_IPD_record_sql_varchar,
    _stmt_param_get_sqlc_tinyint,
    _stmt_param_check_sqlc_tinyint_sql_varchar,
    _stmt_param_guess_sqlc_tinyint},

  {SQL_C_DOUBLE, SQL_TYPE_TIMESTAMP,
    _stmt_param_bind_set_APD_record_sqlc_double,
    _stmt_param_bind_set_IPD_record_sql_timestamp,
    _stmt_param_get_sqlc_double,
    _stmt_param_check_sqlc_double_sql_timestamp,
    _stmt_param_guess_sqlc_double},
  {SQL_C_DOUBLE, SQL_DOUBLE,
    _stmt_param_bind_set_APD_record_sqlc_double,
    _stmt_param_bind_set_IPD_record_sql_double,
    _stmt_param_get_sqlc_double,
    _stmt_param_check_sqlc_double_sql_double,
    _stmt_param_guess_sqlc_double},
  {SQL_C_DOUBLE, SQL_VARCHAR,
    _stmt_param_bind_set_APD_record_sqlc_double,
    _stmt_param_bind_set_IPD_record_sql_varchar,
    _stmt_param_get_sqlc_double,
    _stmt_param_check_sqlc_double_sql_varchar,
    _stmt_param_guess_sqlc_double},
  {SQL_C_DOUBLE, SQL_REAL,
    _stmt_param_bind_set_APD_record_sqlc_double,
    _stmt_param_bind_set_IPD_record_sql_real,
    _stmt_param_get_sqlc_double,
    _stmt_param_check_sqlc_double_sql_real,
    _stmt_param_guess_sqlc_double},

  {SQL_C_FLOAT, SQL_REAL,
    _stmt_param_bind_set_APD_record_sqlc_float,
    _stmt_param_bind_set_IPD_record_sql_real,
    _stmt_param_get_sqlc_float,
    _stmt_param_check_sqlc_float_sql_real,
    _stmt_param_guess_sqlc_float},

  {SQL_C_BINARY, SQL_VARBINARY,
    _stmt_param_bind_set_APD_record_sqlc_binary,
    _stmt_param_bind_set_IPD_record_sql_varbinary,
    _stmt_param_get_sqlc_binary,
    _stmt_param_check_sqlc_binary_sql_varbinary,
    _stmt_param_guess_sqlc_binary},
};

static SQLRETURN _stmt_bind_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  switch (InputOutputType) {
    case SQL_PARAM_INPUT:
      break;
    case SQL_PARAM_INPUT_OUTPUT:
      break;
    case SQL_PARAM_OUTPUT:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_OUTPUT not supported yet by taos");
      return SQL_ERROR;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_PARAM_INPUT_OUTPUT_STREAM:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_INPUT_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
    case SQL_PARAM_OUTPUT_STREAM:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
#endif                       /* } */
    default:
      stmt_append_err(stmt, "HY000", 0, "unknown InputOutputType for `SQLBindParameter`");
      return SQL_ERROR;
  }

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  sr = descriptor_keep(APD, stmt, ParameterNumber);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  sr = descriptor_keep(IPD, stmt, ParameterNumber);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  desc_record_t *APD_record = APD->records + ParameterNumber - 1;
  desc_record_t *IPD_record = IPD->records + ParameterNumber - 1;

  for (size_t i=0; i<sizeof(_sqlc_sql_map)/sizeof(_sqlc_sql_map[0]); ++i) {
    const sqlc_sql_map_t *map = _sqlc_sql_map + i;
    if (map->ValueType     != ValueType) continue;
    if (map->ParameterType != ParameterType) continue;

    sr = map->set_APD_record(stmt, APD_record, ParameterNumber, ValueType, ParameterValuePtr, BufferLength, StrLen_or_IndPtr);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = map->set_IPD_record(stmt, IPD_record, ParameterNumber, InputOutputType, ParameterType, ColumnSize, DecimalDigits);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    if (map->check == NULL) break;
    if (map->guess == NULL) break;
    if (map->get_sqlc == NULL) break;

    r = _params_bind_meta_set(&stmt->params_bind_meta, ParameterNumber-1, map->check, map->guess, map->get_sqlc);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }

    if (ParameterNumber > APD_header->DESC_COUNT) APD_header->DESC_COUNT = ParameterNumber;
    if (ParameterNumber > IPD_header->DESC_COUNT) IPD_header->DESC_COUNT = ParameterNumber;

    APD_record->bound = 1;
    IPD_record->bound = 1;

    return SQL_SUCCESS;
  }
  stmt_append_err_format(stmt, "HY000", 0,
      "General error:#%d Parameter converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
      ParameterNumber,
      sqlc_data_type(ValueType), ValueType, ValueType,
      sql_data_type(ParameterType), ParameterType, ParameterType);
  return SQL_ERROR;
}

SQLRETURN stmt_bind_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  if (ParameterNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  return _stmt_bind_param(stmt, ParameterNumber, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_param_get_sqlc(stmt_t *stmt, param_state_t *param_state)
{
  param_bind_meta_t *meta = _params_bind_meta_get(&stmt->params_bind_meta, param_state->i_param);
  if (!meta || !meta->get_sqlc) {
    int            i_row      = param_state->i_row;
    int            i_param    = param_state->i_param;
    desc_record_t *APD_record = param_state->APD_record;
    desc_record_t *IPD_record = param_state->IPD_record;

    SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
    SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

    stmt_append_err_format(stmt, "HY000", 0,
        "General error:no param get_sqlcer found for parameter(#%d,#%d) [%s/%s]",
        i_row + 1, i_param + 1,
        sqlc_data_type(ValueType), sql_data_type(ParameterType));
    return SQL_ERROR;
  }

  return meta->get_sqlc(stmt, param_state);
}

static SQLRETURN _stmt_param_get(stmt_t *stmt, param_state_t *param_state)
{
  size_t irow                  = param_state->i_row;
  size_t iparam                = param_state->i_param;
  sqlc_data_t *sqlc_data       = &param_state->sqlc_data;
  desc_record_t *APD_record    = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  const SQLLEN BufferLength = APD_record->DESC_OCTET_LENGTH;

  sqlc_data->type = ValueType;

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  SQLLEN *len_arr = APD_record->DESC_OCTET_LENGTH_PTR;
  SQLLEN *ind_arr = APD_record->DESC_INDICATOR_PTR;

  if (ind_arr && ind_arr[irow] == SQL_NULL_DATA) {
    sqlc_data->is_null = 1;
    return SQL_SUCCESS;
  }

  sqlc_data->is_null = 0;
  const char *base    = buffer ? buffer + APD_record->DESC_OCTET_LENGTH * irow : NULL;
  size_t len = BufferLength;

  switch (ValueType) {
    case SQL_C_BIT:
    case SQL_C_STINYINT:
    case SQL_C_TINYINT:
    case SQL_C_UTINYINT:
    case SQL_C_SSHORT:
    case SQL_C_SHORT:
    case SQL_C_USHORT:
    case SQL_C_SLONG:
    case SQL_C_LONG:
    case SQL_C_ULONG:
    case SQL_C_SBIGINT:
    case SQL_C_UBIGINT:
    case SQL_C_FLOAT:
    case SQL_C_DOUBLE:
      break;
    case SQL_C_CHAR:
      if (!len_arr) {
        len = base ? strnlen(base, BufferLength) : 0;
      } else if (len_arr[irow] == SQL_NTS) {
        len = base ? strlen(base) : 0;
      } else {
        len = len_arr[irow];
#ifndef TODBC_X86
        // NOTE: this is to hacking common_lisp plain-odbc `feature`
        if (len_arr && len >> 32) {
          len = (int32_t)len;
        }
#endif
      }
      if (base[len]) {
        int r = mem_copy_bin(&sqlc_data->mem, (const unsigned char*)base, len);
        if (r) {
          stmt_append_err_format(stmt, "HY000", 0,
              "General error:col(%zd,%zd) of [%d/%x]%s not null terminated and out of memory when caching",
              irow + 1, iparam + 1, ValueType, ValueType, sqlc_data_type(ValueType));
          return SQL_ERROR;
        }
        base = (const char*)sqlc_data->mem.base;
      }
      break;
    case SQL_C_WCHAR:
      if (!len_arr) {
        len = base ? tod_wstrnlen(base, BufferLength) : 0;
      } else if (len_arr[irow] == SQL_NTS) {
        len = base ? tod_wstrlen(base) : 0;
      } else {
        len = len_arr[irow];
#ifndef TODBC_X86
        // NOTE: this is to hacking common_lisp plain-odbc `feature`
        if (len_arr && len >> 32) {
          len = (int32_t)len;
        }
#endif
      }
      if (base[len] || base[len+1]) {
        int r = mem_copy_bin(&sqlc_data->mem, (const unsigned char*)base, len);
        if (r) {
          stmt_append_err_format(stmt, "HY000", 0,
              "General error:col(%zd,%zd) of [%d/%x]%s not null terminated and out of memory when caching",
              irow + 1, iparam + 1, ValueType, ValueType, sqlc_data_type(ValueType));
          return SQL_ERROR;
        }
        base = (const char*)sqlc_data->mem.base;
      }
      break;
    case SQL_C_BINARY:
      if (len_arr) {
        len = len_arr[irow];
      }
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:param#%zd of [%d/%x]%s not supported yet",
          iparam + 1, ValueType, ValueType, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  param_state->sqlc_base = base;
  param_state->sqlc_len  = len;

  return _stmt_param_get_sqlc(stmt, param_state);
}

static SQLRETURN _stmt_param_adjust_reuse_sqlc_sbigint(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int64_t);

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  if (buffer) {
    buffer = buffer + tsdb_bind->buffer_length * param_state->i_batch_offset;
  }
  tsdb_bind->buffer = buffer;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_reuse_sqlc_double(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(double);

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  if (buffer) {
    buffer = buffer + tsdb_bind->buffer_length * param_state->i_batch_offset;
  }
  tsdb_bind->buffer = buffer;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_reuse_sqlc_float(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(float);

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  if (buffer) {
    buffer = buffer + tsdb_bind->buffer_length * param_state->i_batch_offset;
  }
  tsdb_bind->buffer = buffer;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_reuse_sqlc_long(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int32_t);

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  if (buffer) {
    buffer = buffer + tsdb_bind->buffer_length * param_state->i_batch_offset;
  }
  tsdb_bind->buffer = buffer;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_reuse_sqlc_short(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int16_t);

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  if (buffer) {
    buffer = buffer + tsdb_bind->buffer_length * param_state->i_batch_offset;
  }
  tsdb_bind->buffer = buffer;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_reuse_sqlc_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  if (buffer) {
    buffer = buffer + tsdb_bind->buffer_length * param_state->i_batch_offset;
  }
  tsdb_bind->buffer = buffer;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_utinyint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(uint8_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int16_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_usmallint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(uint16_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int32_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_uint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(uint32_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int64_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_ubigint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(uint64_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(float);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_double(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(double);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust(stmt_t *stmt, param_state_t *param_state)
{
  param_bind_meta_t *meta = _params_bind_meta_get(&stmt->params_bind_meta, param_state->i_param);
  if (!meta || !meta->adjust) {
    int            i_param    = param_state->i_param;
    desc_record_t *APD_record = param_state->APD_record;
    desc_record_t *IPD_record = param_state->IPD_record;
    TAOS_FIELD_E  *tsdb_field = param_state->tsdb_field;

    SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
    SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
    int8_t      tsdb_type     = tsdb_field->type;

    stmt_append_err_format(stmt, "HY000", 0,
        "General error:no param adjuster found for #%d Parameter `%s[0x%x/%d]:%s[0x%x/%d]:%s[0x%x/%d]`",
        i_param + 1,
        sqlc_data_type(ValueType), ValueType, ValueType,
        sql_data_type(ParameterType), ParameterType, ParameterType,
        taos_data_type(tsdb_type), tsdb_type, tsdb_type);
    return SQL_ERROR;
  }

  return meta->adjust(stmt, param_state);
}

static SQLRETURN _stmt_param_tsdb_array_adjust(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  int i_param                             = param_state->i_param;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  if (i_param == 0 && stmt->tsdb_stmt.is_insert_stmt && stmt->tsdb_stmt.params.subtbl_required) {
    return SQL_SUCCESS;
  }

  int r = 0;

  r = mem_keep(&param_column->mem_is_null, sizeof(char)*nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->is_null = (char*)param_column->mem_is_null.base;
  tsdb_bind->num = nr_batch_size;

  return _stmt_param_adjust(stmt, param_state);
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  charset_conv_t *cnv  = param_state->charset_convs.cnv_from_sqlc_charset_for_param_bind_to_tsdb;

  char *tsdb_varchar = tsdb_bind->buffer;
  tsdb_varchar += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;
  size_t tsdb_varchar_len = tsdb_bind->buffer_length;

  size_t         inbytes             = len;
  size_t         outbytes            = tsdb_varchar_len;

  size_t         inbytesleft         = inbytes;
  size_t         outbytesleft        = outbytes;

  char          *inbuf               = (char*)s;
  char          *outbuf              = (char*)tsdb_varchar;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  int e = errno;
  iconv(cnv->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    if (e != E2BIG) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
          cnv->from, cnv->to, e, strerror(e));
      return SQL_ERROR;
    }
  }

  if (tsdb_bind->length) {
    tsdb_bind->length[i_row - param_state->i_batch_offset] = (int32_t)(outbytes - outbytesleft);
  }

  if (inbytesleft) {
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_binary_tsdb_varbinary(stmt_t *stmt, param_state_t *param_state, const unsigned char *s, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  unsigned char *tsdb_varbinary = tsdb_bind->buffer;
  tsdb_varbinary += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;
  size_t tsdb_varbinary_len = tsdb_bind->buffer_length;

  size_t n = len;
  if (n > tsdb_varbinary_len) n = tsdb_varbinary_len;
  memcpy(tsdb_varbinary, s, n);

  if (tsdb_bind->length) {
    tsdb_bind->length[i_row - param_state->i_batch_offset] = (int32_t)n;
  }

  if (len > tsdb_varbinary_len) {
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated: varbinary too large (#%zd > #%zd)",
        len, tsdb_varbinary_len);
    return SQL_SUCCESS_WITH_INFO; // FIXME: or SQL_ERROR?
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_binary_tsdb_geometry(stmt_t *stmt, param_state_t *param_state, const unsigned char *s, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  unsigned char *tsdb_geometry = tsdb_bind->buffer;
  tsdb_geometry += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;
  size_t tsdb_geometry_len = tsdb_bind->buffer_length;

  size_t n = len;
  if (n > tsdb_geometry_len) n = tsdb_geometry_len;
  memcpy(tsdb_geometry, s, n);

  if (tsdb_bind->length) {
    tsdb_bind->length[i_row - param_state->i_batch_offset] = (int32_t)n;
  }

  if (len > tsdb_geometry_len) {
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated: geometry too large (#%zd > #%zd)",
        len, tsdb_geometry_len);
    return SQL_SUCCESS_WITH_INFO; // FIXME: or SQL_ERROR?
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *IPD_record        = param_state->IPD_record;

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  sql_data_t           *data              = &param_state->sql_data;


  char *tsdb_timestamp = tsdb_bind->buffer;
  tsdb_timestamp += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;

  int64_t v = 0;
  if (IPD_record->DESC_CONCISE_TYPE == SQL_TYPE_TIMESTAMP) {
    if (data->ts.is_i64) {
      v = data->ts.i64;
    } else {
      v = data->ts.sec;
      switch (tsdb_field->precision) {
        case 0:
          v = v * 1000 + data->ts.nsec / 1000000;
          break;
        case 1:
          v = v * 1000000 + data->ts.nsec / 1000;
          break;
        case 2:
          v = v * 1000000000 + data->ts.nsec;
          break;
        default:
          stmt_append_err_format(stmt, "HY000", 0,
              "General error:bad tsdb_timestamp precision:%d for param[%d,%d]",
              tsdb_field->precision, i_row+1, i_param+1);
          return SQL_ERROR;
      }
    }
    *(int64_t*)tsdb_timestamp = v;
    return SQL_SUCCESS;
  }

  return _stmt_conv_sql_c_char_to_tsdb_timestamp_x(stmt, s, len, tsdb_field->precision, (int64_t*)tsdb_timestamp);
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_wchar_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, const char *wstr, size_t wlen)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  charset_conv_t *cnv  = param_state->charset_convs.cnv_from_wchar_to_tsdb;

  char *tsdb_varchar = tsdb_bind->buffer;
  tsdb_varchar += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;
  size_t tsdb_varchar_len = tsdb_bind->buffer_length;

  size_t         inbytes             = wlen * 2;
  size_t         outbytes            = tsdb_varchar_len;

  size_t         inbytesleft         = inbytes;
  size_t         outbytesleft        = outbytes;

  char          *inbuf               = (char*)wstr;
  char          *outbuf              = (char*)tsdb_varchar;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  int e = errno;
  iconv(cnv->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    if (e != E2BIG) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
          cnv->from, cnv->to, e, strerror(e));
      return SQL_ERROR;
    }
  }
  if (inbytesleft) {
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }

  if (tsdb_bind->length) {
    tsdb_bind->length[i_row - param_state->i_batch_offset] = (int32_t)(outbytes - outbytesleft);
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_guess(stmt_t *stmt, param_state_t *param_state)
{
  param_bind_meta_t *meta = _params_bind_meta_get(&stmt->params_bind_meta, param_state->i_param);
  if (!meta || !meta->guess) {
    int            i_row      = param_state->i_row;
    int            i_param    = param_state->i_param;
    desc_record_t *APD_record = param_state->APD_record;
    desc_record_t *IPD_record = param_state->IPD_record;

    SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
    SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

    stmt_append_err_format(stmt, "HY000", 0,
        "General error:no param guesser found for parameter(#%d,#%d) [%s/%s]",
        i_row + 1, i_param + 1,
        sqlc_data_type(ValueType), sql_data_type(ParameterType));
    return SQL_ERROR;
  }

  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;

  return meta->guess(stmt, param_state);
}

static SQLRETURN _stmt_param_adjust_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int64_t);

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_column->mem_length, sizeof(*tsdb_bind->length) * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->length = (int32_t*)param_column->mem_length.base;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->buffer_length = tsdb_field->bytes - 2;

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_varbinary(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_column->mem_length, sizeof(*tsdb_bind->length) * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->length = (int32_t*)param_column->mem_length.base;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->buffer_length = tsdb_field->bytes - 2;

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_geometry(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_column->mem_length, sizeof(*tsdb_bind->length) * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->length = (int32_t*)param_column->mem_length.base;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->buffer_length = tsdb_field->bytes - 2;

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  int nr_batch_size                       = param_state->nr_batch_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_column->mem_length, sizeof(*tsdb_bind->length) * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = (int32_t*)param_column->mem_length.base;

  if (stmt->tsdb_stmt.is_insert_stmt) {
    // NOTE: tsdb_field->bytes: nchar(20): => 82 == 20 * 4 + 2;
    tsdb_bind->buffer_length = tsdb_field->bytes - 2 /*+ cnv->nr_to_terminator*/;
  } else {
    OA(0, "tsdb_field->bytes:%d", tsdb_field->bytes);
    tsdb_bind->buffer_length = tsdb_field->bytes + 8;
  }

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_batch_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_adjust_tsdb_json(stmt_t *stmt, param_state_t *param_state)
{
  return _stmt_param_adjust_tsdb_nchar(stmt, param_state);
}

static SQLRETURN _stmt_param_conv_dummy(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;
  (void)param_state;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_integer_to_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  int64_t i64 = 0;
  if (param_state->sql_data.unsigned_) {
    i64 = param_state->sql_data.u32;
  } else {
    i64 = param_state->sql_data.i32;
  }

  if (i64 > INT32_MAX || i64 < INT32_MIN) {
    OA_NIY(0);
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int32_t *v = (int32_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int32_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_smallint_to_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  int64_t i64 = 0;
  if (param_state->sql_data.unsigned_) {
    i64 = param_state->sql_data.u16;
  } else {
    i64 = param_state->sql_data.i16;
  }

  if (i64 > INT16_MAX || i64 < INT16_MIN) {
    OA_NIY(0);
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int16_t *v = (int16_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int16_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_tinyint_to_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  int64_t i64 = 0;
  if (param_state->sql_data.unsigned_) {
    i64 = param_state->sql_data.u8;
  } else {
    i64 = param_state->sql_data.i8;
  }

  if (i64 > INT8_MAX || i64 < INT8_MIN) {
    OA_NIY(0);
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int8_t *v = (int8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int8_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_tinyint_to_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  int64_t i64 = 0;
  if (param_state->sql_data.unsigned_) {
    i64 = param_state->sql_data.u8;
  } else {
    i64 = param_state->sql_data.i8;
  }

  if (i64 > INT8_MAX || i64 < INT8_MIN) {
    OA_NIY(0);
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int8_t *v = (int8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int8_t)!!i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_timestamp_to_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  sql_data_t           *data              = &param_state->sql_data;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  int64_t v = 0;
  if (data->ts.is_i64) {
    v = data->ts.i64;
  } else {
    switch (tsdb_field->precision) {
      case 0: v = data->ts.sec * 1000 + (int64_t)round((((double)data->ts.nsec) / 1000000)); break;
      case 1: v = data->ts.sec * 1000000 + (int64_t)round((((double)data->ts.nsec) / 1000)); break;
      case 2: v = data->ts.sec * 1000000000 + data->ts.nsec;     break;
      default: {
        stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
        return SQL_ERROR;
      }
    }
  }

  int64_t *tsdb_timestamp = (int64_t*)tsdb_bind->buffer;
  tsdb_timestamp[i_row - param_state->i_batch_offset] = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_double_to_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  sql_data_t           *data              = &param_state->sql_data;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  int64_t v = 0;
  switch (tsdb_field->precision) {
    case 0: v = (int64_t)(data->dbl * 1000);       break;
    case 1: v = (int64_t)(data->dbl * 1000000);    break;
    case 2: v = (int64_t)(data->dbl * 1000000000); break;
    default: {
      stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
      return SQL_ERROR;
    }
  }

  int64_t *tsdb_timestamp = (int64_t*)tsdb_bind->buffer;
  tsdb_timestamp[i_row - param_state->i_batch_offset] = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_double_to_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  sql_data_t           *data              = &param_state->sql_data;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  double dbl = data->dbl;

  if (dbl > FLT_MAX) {
    stmt_append_err(stmt, "HY000", 0, "General error:Too big a number");
    return SQL_ERROR;
  }

  if (dbl < FLT_MIN) {
    stmt_append_err(stmt, "HY000", 0, "General error:Too small a number");
    return SQL_ERROR;
  }

  float *v = (float*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (float)dbl;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_double_to_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  sql_data_t           *data              = &param_state->sql_data;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  double dbl = data->dbl;
  switch (fpclassify(dbl)) {
    case FP_NAN:
      stmt_append_err(stmt, "HY000", 0, "General error:Not a Number");
      return SQL_ERROR;
    case FP_INFINITE:
      stmt_append_err(stmt, "HY000", 0, "General error:A infinity number");
      return SQL_ERROR;
    default:
      break;
  }

  if (dbl > INT32_MAX) {
    stmt_append_err(stmt, "HY000", 0, "General error:Too big a number");
    return SQL_ERROR;
  }

  if (dbl < INT32_MIN) {
    stmt_append_err(stmt, "HY000", 0, "General error:Too small a number");
    return SQL_ERROR;
  }

  int32_t *v = (int32_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int32_t)dbl;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sql_real_to_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  sql_data_t           *data              = &param_state->sql_data;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  float *v = (float*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = data->flt;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  const char *s = param_state->sqlc_data.str.str;
  size_t len = param_state->sqlc_data.str.len;

  return _stmt_conv_param_data_from_sqlc_char_tsdb_varchar(stmt, param_state, s, len);
}

static SQLRETURN _stmt_param_conv_sqlc_binary_to_tsdb_varbinary(stmt_t *stmt, param_state_t *param_state)
{
  const unsigned char *s   = param_state->sqlc_data.bin.bin;
  size_t               len = param_state->sqlc_data.bin.len;

  return _stmt_conv_param_data_from_sqlc_binary_tsdb_varbinary(stmt, param_state, s, len);
}

static SQLRETURN _stmt_param_conv_sqlc_binary_to_tsdb_geometry(stmt_t *stmt, param_state_t *param_state)
{
  const unsigned char *s   = param_state->sqlc_data.bin.bin;
  size_t               len = param_state->sqlc_data.bin.len;

  return _stmt_conv_param_data_from_sqlc_binary_tsdb_geometry(stmt, param_state, s, len);
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  const char *s = param_state->sqlc_data.str.str;
  size_t len = param_state->sqlc_data.str.len;

  return _stmt_conv_param_data_from_sqlc_char_tsdb_varchar(stmt, param_state, s, len);
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_json(stmt_t *stmt, param_state_t *param_state)
{
  return _stmt_param_conv_sqlc_char_to_tsdb_nchar(stmt, param_state);
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  const char *s = param_state->sqlc_data.str.str;
  size_t len = param_state->sqlc_data.str.len;

  return _stmt_conv_param_data_from_sqlc_char_tsdb_timestamp(stmt, param_state, s, len);
}

static SQLRETURN _stmt_param_conv_sqlc_wchar_to_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  const char *wstr = param_state->sqlc_data.wstr.wstr;
  size_t      wlen = param_state->sqlc_data.wstr.wlen;
  return _stmt_conv_param_data_from_sqlc_wchar_tsdb_varchar(stmt, param_state, wstr, wlen);
}

static SQLRETURN _stmt_param_conv_sqlc_wchar_to_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  const char *wstr = param_state->sqlc_data.wstr.wstr;
  size_t      wlen = param_state->sqlc_data.wstr.wlen;
  return _stmt_conv_param_data_from_sqlc_wchar_tsdb_varchar(stmt, param_state, wstr, wlen);
}

static SQLRETURN _stmt_param_conv_sqlc_wchar_to_tsdb_json(stmt_t *stmt, param_state_t *param_state)
{
  return _stmt_param_conv_sqlc_wchar_to_tsdb_nchar(stmt, param_state);
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int8_t *v = (int8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int8_t)!!ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  if (ll > INT8_MAX || ll < INT8_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1, (int64_t)ll);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int8_t *v = (int8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int8_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_utinyint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  if (ll > UINT8_MAX || ll < 0) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1, (int64_t)ll);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  uint8_t *v = (uint8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (uint8_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  if (ll > INT16_MAX || ll < INT16_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1, (int64_t)ll);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int16_t *v = (int16_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int16_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_usmallint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  if (ll > UINT16_MAX || ll < 0) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1, (int64_t)ll);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  uint16_t *v = (uint16_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (uint16_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  if (ll > INT32_MAX || ll < INT32_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1, (int64_t)ll);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int32_t *v = (int32_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int32_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_uint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  if (ll > UINT32_MAX || ll < 0) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:`%s/%s` for param[%d,%d]:%" PRId64 "",
        sqlc_data_type(ValueType), sql_data_type(ParameterType),
        i_row+1, i_param+1, (int64_t)ll);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  uint32_t *v = (uint32_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (uint32_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int64_t *v = (int64_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (int64_t)ll;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_ubigint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;


  const char *s = data->str.str;

  char *end = NULL;
  unsigned long long ull = strtoull(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  uint64_t *v = (uint64_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (uint64_t)ull;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  const char *s = data->str.str;

  char *end = NULL;
  float flt = strtof(s, &end);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (fabsf(flt) == HUGE_VALF) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
          i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
      return SQL_ERROR;
    }
  }

  if (fpclassify(flt) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
          i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
      return SQL_ERROR;
    }
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  float *v = (float*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = flt;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_conv_sqlc_char_to_tsdb_double(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  sql_data_t           *data              = &param_state->sql_data;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  const char *s = data->str.str;

  char *end = NULL;
  double dbl = strtod(s, &end);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Param[%d,%d] conversion from `%s` to `%s` failed:invalid character[0x%02x]",
        i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType), *end);
    return SQL_ERROR;
  }
  if (fabs(dbl) == HUGE_VALF) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
          i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
      return SQL_ERROR;
    }
  }

  if (fpclassify(dbl) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Param[%d,%d] conversion from `%s` to `%s` failed:overflow or underflow occurs",
          i_row+1, i_param+1, taos_data_type(tsdb_type), sqlc_data_type(ValueType));
      return SQL_ERROR;
    }
  }

  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  double *v = (double*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = dbl;

  return SQL_SUCCESS;
}

static const param_bind_map_t _param_bind_map[] = {
  {SQL_C_SBIGINT, SQL_VARCHAR, TSDB_DATA_TYPE_BIGINT,
    _stmt_param_adjust_reuse_sqlc_sbigint,
    _stmt_param_conv_dummy},
  {SQL_C_SBIGINT, SQL_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_reuse_sqlc_sbigint,
    _stmt_param_conv_dummy},
  {SQL_C_SBIGINT, SQL_TYPE_TIMESTAMP, TSDB_DATA_TYPE_BIGINT,
    _stmt_param_adjust_reuse_sqlc_sbigint,
    _stmt_param_conv_dummy},
  {SQL_C_SBIGINT, SQL_BIGINT, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_reuse_sqlc_sbigint,
    _stmt_param_conv_dummy},
  {SQL_C_SBIGINT, SQL_BIGINT, TSDB_DATA_TYPE_BIGINT,
    _stmt_param_adjust_reuse_sqlc_sbigint,
    _stmt_param_conv_dummy},
  {SQL_C_SBIGINT, SQL_INTEGER, TSDB_DATA_TYPE_INT,
    _stmt_param_adjust_tsdb_int,
    _stmt_param_conv_sql_integer_to_tsdb_int},
  {SQL_C_SBIGINT, SQL_SMALLINT, TSDB_DATA_TYPE_SMALLINT,
    _stmt_param_adjust_tsdb_smallint,
    _stmt_param_conv_sql_smallint_to_tsdb_smallint},
  {SQL_C_SBIGINT, SQL_TINYINT, TSDB_DATA_TYPE_TINYINT,
    _stmt_param_adjust_tsdb_tinyint,
    _stmt_param_conv_sql_tinyint_to_tsdb_tinyint},
  {SQL_C_SBIGINT, SQL_TINYINT, TSDB_DATA_TYPE_BOOL,
    _stmt_param_adjust_tsdb_bool,
    _stmt_param_conv_sql_tinyint_to_tsdb_bool},

  {SQL_C_DOUBLE,  SQL_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_tsdb_timestamp,
    _stmt_param_conv_sql_timestamp_to_tsdb_timestamp},
  {SQL_C_DOUBLE,  SQL_DOUBLE, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_reuse_sqlc_double,
    _stmt_param_conv_sql_double_to_tsdb_timestamp},
  {SQL_C_DOUBLE,  SQL_DOUBLE, TSDB_DATA_TYPE_DOUBLE,
    _stmt_param_adjust_reuse_sqlc_double,
    _stmt_param_conv_dummy},
  {SQL_C_DOUBLE,  SQL_DOUBLE, TSDB_DATA_TYPE_FLOAT,
    _stmt_param_adjust_reuse_sqlc_double,
    _stmt_param_conv_sql_double_to_tsdb_float},
  {SQL_C_DOUBLE,  SQL_DOUBLE, TSDB_DATA_TYPE_INT,
    _stmt_param_adjust_reuse_sqlc_double,
    _stmt_param_conv_sql_double_to_tsdb_int},
  {SQL_C_DOUBLE,  SQL_VARCHAR, TSDB_DATA_TYPE_DOUBLE,
    _stmt_param_adjust_reuse_sqlc_double,
    _stmt_param_conv_dummy},
  {SQL_C_DOUBLE,  SQL_REAL, TSDB_DATA_TYPE_FLOAT,
    _stmt_param_adjust_tsdb_float,
    _stmt_param_conv_sql_real_to_tsdb_float},

  {SQL_C_FLOAT,  SQL_REAL, TSDB_DATA_TYPE_FLOAT,
    _stmt_param_adjust_reuse_sqlc_float,
    _stmt_param_conv_dummy},

  {SQL_C_CHAR, SQL_TYPE_TIMESTAMP, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_tsdb_timestamp,
    _stmt_param_conv_sql_timestamp_to_tsdb_timestamp},
  {SQL_C_CHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_tsdb_timestamp,
    _stmt_param_conv_sqlc_char_to_tsdb_timestamp},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_TIMESTAMP,
    _stmt_param_adjust_tsdb_timestamp,
    _stmt_param_conv_sqlc_char_to_tsdb_timestamp},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_VARCHAR,
    _stmt_param_adjust_tsdb_varchar,
    _stmt_param_conv_sqlc_char_to_tsdb_varchar},
  {SQL_C_CHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_VARCHAR,
    _stmt_param_adjust_tsdb_varchar,
    _stmt_param_conv_sqlc_char_to_tsdb_varchar},
  {SQL_C_CHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_NCHAR,
    _stmt_param_adjust_tsdb_nchar,
    _stmt_param_conv_sqlc_char_to_tsdb_nchar},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_NCHAR,
    _stmt_param_adjust_tsdb_nchar,
    _stmt_param_conv_sqlc_char_to_tsdb_nchar},
  {SQL_C_CHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_JSON,
    _stmt_param_adjust_tsdb_json,
    _stmt_param_conv_sqlc_char_to_tsdb_json},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_JSON,
    _stmt_param_adjust_tsdb_json,
    _stmt_param_conv_sqlc_char_to_tsdb_json},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_BOOL,
    _stmt_param_adjust_tsdb_bool,
    _stmt_param_conv_sqlc_char_to_tsdb_bool},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_TINYINT,
    _stmt_param_adjust_tsdb_tinyint,
    _stmt_param_conv_sqlc_char_to_tsdb_tinyint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_UTINYINT,
    _stmt_param_adjust_tsdb_utinyint,
    _stmt_param_conv_sqlc_char_to_tsdb_utinyint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_SMALLINT,
    _stmt_param_adjust_tsdb_smallint,
    _stmt_param_conv_sqlc_char_to_tsdb_smallint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_USMALLINT,
    _stmt_param_adjust_tsdb_usmallint,
    _stmt_param_conv_sqlc_char_to_tsdb_usmallint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_INT,
    _stmt_param_adjust_tsdb_int,
    _stmt_param_conv_sqlc_char_to_tsdb_int},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_UINT,
    _stmt_param_adjust_tsdb_uint,
    _stmt_param_conv_sqlc_char_to_tsdb_uint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_BIGINT,
    _stmt_param_adjust_tsdb_bigint,
    _stmt_param_conv_sqlc_char_to_tsdb_bigint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_UBIGINT,
    _stmt_param_adjust_tsdb_ubigint,
    _stmt_param_conv_sqlc_char_to_tsdb_ubigint},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_FLOAT,
    _stmt_param_adjust_tsdb_float,
    _stmt_param_conv_sqlc_char_to_tsdb_float},
  {SQL_C_CHAR, SQL_VARCHAR, TSDB_DATA_TYPE_DOUBLE,
    _stmt_param_adjust_tsdb_double,
    _stmt_param_conv_sqlc_char_to_tsdb_double},

  {SQL_C_WCHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_VARCHAR,
    _stmt_param_adjust_tsdb_varchar,
    _stmt_param_conv_sqlc_wchar_to_tsdb_varchar},
  {SQL_C_WCHAR, SQL_VARCHAR, TSDB_DATA_TYPE_VARCHAR,
    _stmt_param_adjust_tsdb_varchar,
    _stmt_param_conv_sqlc_wchar_to_tsdb_varchar},
  {SQL_C_WCHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_NCHAR,
    _stmt_param_adjust_tsdb_nchar,
    _stmt_param_conv_sqlc_wchar_to_tsdb_nchar},
  {SQL_C_WCHAR, SQL_WVARCHAR, TSDB_DATA_TYPE_JSON,
    _stmt_param_adjust_tsdb_json,
    _stmt_param_conv_sqlc_wchar_to_tsdb_json},


  {SQL_C_SLONG, SQL_INTEGER, TSDB_DATA_TYPE_INT,
    _stmt_param_adjust_reuse_sqlc_long,
    _stmt_param_conv_dummy},
  {SQL_C_LONG, SQL_INTEGER, TSDB_DATA_TYPE_INT,
    _stmt_param_adjust_reuse_sqlc_long,
    _stmt_param_conv_dummy},
  {SQL_C_SLONG, SQL_VARCHAR, TSDB_DATA_TYPE_INT,
    _stmt_param_adjust_reuse_sqlc_long,
    _stmt_param_conv_dummy},

  {SQL_C_SHORT, SQL_SMALLINT, TSDB_DATA_TYPE_SMALLINT,
    _stmt_param_adjust_reuse_sqlc_short,
    _stmt_param_conv_dummy},

  {SQL_C_SHORT, SQL_BIT, TSDB_DATA_TYPE_BOOL,
    _stmt_param_adjust_reuse_sqlc_short,
    _stmt_param_conv_dummy},  

  {SQL_C_STINYINT, SQL_TINYINT, TSDB_DATA_TYPE_TINYINT,
    _stmt_param_adjust_reuse_sqlc_tinyint,
    _stmt_param_conv_dummy},
  {SQL_C_STINYINT, SQL_VARCHAR, TSDB_DATA_TYPE_SMALLINT,
    _stmt_param_adjust_reuse_sqlc_tinyint,
    _stmt_param_conv_dummy},

  {SQL_C_BINARY, SQL_VARBINARY, TSDB_DATA_TYPE_VARBINARY,
    _stmt_param_adjust_tsdb_varbinary,
    _stmt_param_conv_sqlc_binary_to_tsdb_varbinary},

  {SQL_C_BINARY, SQL_VARBINARY, TSDB_DATA_TYPE_GEOMETRY,
    _stmt_param_adjust_tsdb_geometry,
    _stmt_param_conv_sqlc_binary_to_tsdb_geometry},
};

static SQLRETURN _stmt_param_tsdb_init(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;

  int            i_param    = param_state->i_param;
  desc_record_t *APD_record = param_state->APD_record;
  desc_record_t *IPD_record = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field = param_state->tsdb_field;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int8_t      tsdb_type     = tsdb_field->type;

  for (size_t i=0; i<sizeof(_param_bind_map)/sizeof(_param_bind_map[0]); ++i) {
    const param_bind_map_t *map = _param_bind_map + i;
    if (map->tsdb_type != tsdb_type) continue;
    if (map->ValueType != ValueType) continue;
    if (map->ParameterType != ParameterType) continue;

    if (map->adjust == NULL) break;
    if (map->conv   == NULL) break;

    r = _params_bind_meta_set_adjust_conv(&stmt->params_bind_meta, i_param, map->adjust, map->conv);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    return SQL_SUCCESS;
  }

  stmt_append_err_format(stmt, "HY000", 0,
      "General error:#%d Parameter `%s[0x%x/%d]:%s[0x%x/%d]:%s[0x%x/%d]` not implemented yet",
      i_param + 1,
      sqlc_data_type(ValueType), ValueType, ValueType,
      sql_data_type(ParameterType), ParameterType, ParameterType,
      taos_data_type(tsdb_type), tsdb_type, tsdb_type);
  return SQL_ERROR;
}

static SQLRETURN _stmt_param_check(stmt_t *stmt, param_state_t *param_state)
{
  param_bind_meta_t *meta = _params_bind_meta_get(&stmt->params_bind_meta, param_state->i_param);
  if (!meta || !meta->check) {
    int            i_row      = param_state->i_row;
    int            i_param    = param_state->i_param;
    desc_record_t *APD_record = param_state->APD_record;
    desc_record_t *IPD_record = param_state->IPD_record;

    SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
    SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

    stmt_append_err_format(stmt, "HY000", 0,
        "General error:no param checker found for parameter(#%d,#%d) [%s/%s]",
        i_row + 1, i_param + 1,
        sqlc_data_type(ValueType), sql_data_type(ParameterType));
    return SQL_ERROR;
  }
  if (param_state->sqlc_data.is_null) return SQL_SUCCESS;
  if (meta->check == _stmt_param_check_dummy) return SQL_SUCCESS;
  return meta->check(stmt, param_state);
}

static SQLRETURN _stmt_param_conv(stmt_t *stmt, param_state_t *param_state)
{
  param_bind_meta_t *meta = _params_bind_meta_get(&stmt->params_bind_meta, param_state->i_param);
  if (!meta || !meta->conv) {
    int            i_row      = param_state->i_row;
    int            i_param    = param_state->i_param;
    desc_record_t *APD_record = param_state->APD_record;
    desc_record_t *IPD_record = param_state->IPD_record;
    TAOS_FIELD_E  *tsdb_field = param_state->tsdb_field;

    SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
    SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

    stmt_append_err_format(stmt, "HY000", 0,
        "General error:no param converter found for parameter(#%d,#%d) [%s/%s/%s]",
        i_row + 1, i_param + 1,
        sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
    return SQL_ERROR;
  }

  if (meta->conv == _stmt_param_conv_dummy) return SQL_SUCCESS;

  return meta->conv(stmt, param_state);
}

static SQLRETURN _stmt_conv_param_data(stmt_t *stmt, param_state_t *param_state)
{
  int                        i_row                      = param_state->i_row;
  int                        i_param                    = param_state->i_param;
  desc_record_t             *APD_record                 = param_state->APD_record;

  TAOS_MULTI_BIND           *tsdb_bind                  = param_state->tsdb_bind;

  if (param_state->sqlc_data.is_null) {
    if (!tsdb_bind->is_null) {
      stmt_append_err_format(stmt, "22002", 0,
          "Indicator variable required but not supplied:Parameter(#%d,#%d)[%s] is null",
          i_row + 1, i_param + 1, sqlc_data_type(APD_record->DESC_CONCISE_TYPE));
      return SQL_ERROR;
    }
    tsdb_bind->is_null[i_row - param_state->i_batch_offset] = 1;
    return SQL_SUCCESS;
  }

  if (tsdb_bind->is_null) {
    tsdb_bind->is_null[i_row - param_state->i_batch_offset] = 0;
  }

  return _stmt_param_conv(stmt, param_state);
}

static SQLRETURN _stmt_param_check_and_conv(stmt_t *stmt, param_state_t *param_state)
{
  int with_info = 0;
  SQLRETURN sr = SQL_SUCCESS;
  sr = _stmt_param_check(stmt, param_state);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return sr;

  if (param_state->is_subtbl) return sr;

  if (sr == SQL_SUCCESS_WITH_INFO) with_info = 1;

  sr = _stmt_conv_param_data(stmt, param_state);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return sr;
  if (with_info) return SQL_SUCCESS_WITH_INFO;
  return sr;
}

static SQLRETURN _stmt_execute_rebind_subtbl(stmt_t *stmt, const char *subtbl, size_t subtbl_len)
{
  tsdb_params_t *tsdb_params = &stmt->tsdb_stmt.params;

  TOD_SAFE_FREE(tsdb_params->subtbl);
  tsdb_params->subtbl = strndup(subtbl, subtbl_len);
  if (!tsdb_params->subtbl) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  return tsdb_stmt_rebind_subtbl(&stmt->tsdb_stmt);
}

static SQLRETURN _stmt_init_param_state_cnvs(stmt_t *stmt, param_state_t *param_state)
{
  const char *fromcode = NULL;
  const char *tocode   = NULL;
  charset_conv_t *cnv  = NULL;

  fromcode = conn_get_sqlc_charset(stmt->conn);
  tocode   = "UTF-16LE";
  if (1) {
    // FIXME:
    fromcode = conn_get_sqlc_charset_for_param_bind(stmt->conn);
  }
  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }
  param_state->charset_convs.cnv_from_sqlc_charset_for_param_bind_to_wchar = cnv;

  fromcode = "UTF-16LE";
  tocode   = "UTF-16LE";
  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }
  param_state->charset_convs.cnv_from_wchar_to_wchar = cnv;

  fromcode = "UTF-16LE";
  tocode   = conn_get_sqlc_charset(stmt->conn);
  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }
  param_state->charset_convs.cnv_from_wchar_to_sqlc = cnv;

  fromcode = conn_get_sqlc_charset(stmt->conn);
  tocode   = conn_get_tsdb_charset(stmt->conn);
  if (1) {
    // FIXME:
    fromcode = conn_get_sqlc_charset_for_param_bind(stmt->conn);
  }
  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }
  param_state->charset_convs.cnv_from_sqlc_charset_for_param_bind_to_tsdb = cnv;

  fromcode = "UTF-16LE";
  tocode   = conn_get_tsdb_charset(stmt->conn);
  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }
  param_state->charset_convs.cnv_from_wchar_to_tsdb = cnv;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_col_subtbl(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf_subtbl[192 * 6]; // FIXME: hard-coded
  const char *fromcode = NULL;
  const char *tocode   = conn_get_sqlc_charset(stmt->conn);
  charset_conv_t *cnv = NULL;

  const char *subtbl = NULL;
  size_t subtbl_len  = 0;

  desc_record_t *APD_record = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_WCHAR:
      fromcode = "UTF-16LE";
      break;
    case SQL_C_CHAR:
      fromcode = tocode;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:subtbl is required as `SQL_C_CHAR|SQL_C_WCHAR` type, but got ==[%s]==", sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  for (size_t i_row_offset = 0; i_row_offset < (size_t)param_state->nr_batch_size; ++i_row_offset) {
    size_t i_row = param_state->i_batch_offset + i_row_offset;
    param_state->i_row      = (int)i_row;

    sr = _stmt_param_get(stmt, param_state);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    if (param_state->sqlc_data.is_null) {
      stmt_append_err(stmt, "HY000", 0, "General error:subtbl is required, but got ==null==");
      return SQL_ERROR;
    }

    sr = _stmt_param_check(stmt, param_state);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    char buf[sizeof(buf_subtbl)];
    const char *base = NULL;
    size_t len = 0;

    size_t           inbytes = 0;
    char            *inbuf   = NULL;

    if (ValueType == SQL_C_WCHAR) {
      inbuf               = (char*)param_state->sqlc_data.wstr.wstr;
      inbytes             = param_state->sqlc_data.wstr.wlen * 2;
    } else {
      inbuf               = (char*)param_state->sqlc_data.str.str;
      inbytes             = param_state->sqlc_data.str.len;
    }

    size_t           inbytesleft         = inbytes;
    char            *outbuf              = buf;
    const size_t     outbytes            = sizeof(buf);
    size_t           outbytesleft        = sizeof(buf);

    size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    int e = errno;
    iconv(cnv->cnv, NULL, NULL, NULL, NULL);
    if (n == (size_t)-1) {
      OA_NIY(0);
      if (e != E2BIG) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
            cnv->from, cnv->to, e, strerror(e));
        return SQL_ERROR;
      }
    }

    // FIXME: check outbytesleft;
    *outbuf = '\0';
    base = buf;
    len  = outbytes - outbytesleft;

    if (subtbl == NULL) {
      strncpy(buf_subtbl, base, len);
      buf_subtbl[len] = '\0';
      subtbl      = buf_subtbl;
      subtbl_len  = len;

      sr = _stmt_execute_rebind_subtbl(stmt, subtbl, subtbl_len);
      if (sr != SQL_SUCCESS) return SQL_ERROR;

      continue;
    }

    if (subtbl_len == len && strncmp(subtbl, base, len) == 0) continue;

    param_state->nr_batch_size    = (int)(i_row - param_state->i_batch_offset);
    return SQL_SUCCESS;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_col(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (param_state->i_param == 0 && stmt->tsdb_stmt.is_insert_stmt && stmt->tsdb_stmt.params.subtbl_required) {
    param_state->is_subtbl  = 1;
    return _stmt_prepare_col_subtbl(stmt, param_state);
  }

  param_state->is_subtbl  = 0;
  param_state->tsdb_field = &stmt->tsdb_paramset.params[param_state->i_param].tsdb_field;

  if (param_state->i_batch_offset == 0) {
    if (!stmt->tsdb_stmt.is_insert_stmt) {
      sr = _stmt_param_guess(stmt, param_state);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
    if (stmt->conn->cfg.url) {
      // NOTE: does taosws-rs support select-with-params?
      // sr = _stmt_param_guess(stmt, param_state);
      // if (sr != SQL_SUCCESS) return SQL_ERROR;
      sr = _stmt_param_tsdb_init(stmt, param_state);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
    } else {
#endif                       /* ] */
      sr = _stmt_param_tsdb_init(stmt, param_state);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */
  }

  param_state->param_column = stmt->tsdb_paramset.params + param_state->i_param;
  param_state->tsdb_bind = stmt->tsdb_binds.mbs + param_state->i_param;

  sr = _stmt_param_tsdb_array_adjust(stmt, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

static void _stmt_prepare_col_data(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (param_state->i_param == 0 && stmt->tsdb_stmt.is_insert_stmt && stmt->tsdb_stmt.params.subtbl_required) {
    param_state->is_subtbl  = 1;
    return;
  }

  param_state->is_subtbl  = 0;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  SQLUSMALLINT *param_status_ptr = IPD_header->DESC_ARRAY_STATUS_PTR;

  param_state->tsdb_bind = stmt->tsdb_binds.mbs + param_state->i_param;

  size_t i_row_offset = 0;
  size_t i_row = 0;
  for (; i_row_offset < (size_t)param_state->nr_batch_size; ++i_row_offset) {
    i_row = param_state->i_batch_offset + i_row_offset;
    param_state->i_row      = (int)i_row;

    if (param_status_ptr) {
      param_status_ptr[i_row] = SQL_PARAM_UNUSED;
    }

    sr = _stmt_param_get(stmt, param_state);
    if (sr != SQL_SUCCESS) {
      sr = SQL_ERROR;
    } else {
      sr = _stmt_param_check_and_conv(stmt, param_state);
    }
    switch (sr) {
      case SQL_SUCCESS:
        break;
      case SQL_SUCCESS_WITH_INFO:
        if (stmt->strict) {
          sr = SQL_ERROR;
        } else {
          param_state->row_with_info = 1;
        }
        break;
      case SQL_ERROR:
        break;
      default:
        stmt_append_err(stmt, "HY000", 0, "General error:internal logic error when processing paramset");
        sr = SQL_ERROR;
        break;
    }

    if (sr == SQL_ERROR) {
      if (param_status_ptr) {
        param_status_ptr[i_row] = SQL_PARAM_ERROR;
      }
      param_state->row_err = 1;
      break;
    }

    if (param_status_ptr) {
      param_status_ptr[i_row] = param_state->row_with_info ? SQL_PARAM_SUCCESS_WITH_INFO : SQL_PARAM_SUCCESS;
    }
  }

  param_state->nr_batch_size = (int)i_row_offset;
}

static SQLRETURN _stmt_execute_with_param_state(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  size_t nr_paramset_size = APD_header->DESC_ARRAY_SIZE;
  SQLULEN *params_processed_ptr = IPD_header->DESC_ROWS_PROCESSED_PTR;
  if (params_processed_ptr) *params_processed_ptr = 0;
  SQLULEN nr_params_processed = 0;

  if (APD_header->DESC_BIND_TYPE != SQL_PARAM_BIND_BY_COLUMN) {
      stmt_append_err_format(stmt, "HY000", 0, "General error:invalid bind type, expected SQL_PARAM_BIND_BY_COLUMN, but got ==%u==", (uint32_t)APD_header->DESC_BIND_TYPE);
    return SQL_ERROR;
  }

  if (nr_paramset_size != 1) {
    if (stmt->tsdb_stmt.prepared && !stmt->tsdb_stmt.is_insert_stmt) {
      stmt_append_err(stmt, "HY000", 0, "General error:taosc currently does not support batch execution for non-insert-statement");
      return SQL_ERROR;
    }
  }

  for (size_t i_row = 0; i_row < nr_paramset_size; i_row += param_state->nr_batch_size) {
    param_state->i_batch_offset = i_row;
    param_state->nr_batch_size = (int)(nr_paramset_size - i_row);
    if (param_state->nr_batch_size > INT16_MAX) param_state->nr_batch_size = INT16_MAX;

    param_state->row_with_info = 0;
    param_state->row_err = 0;

    for (size_t i_col = 0; i_col < (size_t)param_state->nr_tsdb_fields; ++i_col) {
      param_state->i_param    = (int)i_col;
      param_state->APD_record = APD->records + i_col;
      param_state->IPD_record = IPD->records + i_col;

      sr = _stmt_prepare_col(stmt, param_state);
      if (sr != SQL_SUCCESS) return sr;

      _stmt_prepare_col_data(stmt, param_state);

      if (param_state->nr_batch_size == 0) {
        if (i_row == 0) return SQL_ERROR;
        return SQL_SUCCESS;
      }
    }

    nr_params_processed += param_state->nr_batch_size;
    if (params_processed_ptr) *params_processed_ptr = nr_params_processed;

    for (size_t i=0; i<(size_t)param_state->nr_tsdb_fields; ++i) {
      TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + i;
      mbs->num = (int)param_state->nr_batch_size;
    }

    tsdb_params_t *tsdb_params = &stmt->tsdb_stmt.params;
    if (stmt->tsdb_stmt.is_insert_stmt) {
      if (tsdb_params->nr_tag_fields) {
        TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + (!!stmt->tsdb_stmt.params.subtbl_required);
#ifdef HAVE_TAOSWS           /* [ */
        if (stmt->conn->cfg.url) {
          r = CALL_ws_stmt_set_tags((WS_STMT*)stmt->tsdb_stmt.stmt, (WS_MULTI_BIND*)mbs, tsdb_params->nr_tag_fields);
          if (r) {
            stmt_append_err_format(stmt, "HY000", r, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->tsdb_stmt.stmt));
            return SQL_ERROR;
          }
        } else {
#endif                       /* ] */
          r = CALL_taos_stmt_set_tags(stmt->tsdb_stmt.stmt, mbs);
          if (r) {
            stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->tsdb_stmt.stmt));
            return SQL_ERROR;
          }
#ifdef HAVE_TAOSWS           /* [ */
        }
#endif                       /* ] */
      }
    }

    TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + (!!stmt->tsdb_stmt.params.subtbl_required) + stmt->tsdb_stmt.params.nr_tag_fields;
#ifdef HAVE_TAOSWS           /* [ */
    if (stmt->conn->cfg.url) {
      // r = CALL_ws_stmt_bind_param_batch((WS_STMT*)stmt->tsdb_stmt.stmt, (WS_MULTI_BIND*)mb, tsdb_params->nr_tag_fields);
      r = CALL_ws_stmt_bind_param_batch((WS_STMT*)stmt->tsdb_stmt.stmt, (WS_MULTI_BIND*)mbs, tsdb_params->nr_col_fields);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->tsdb_stmt.stmt));
        return SQL_ERROR;
      }
    } else {
#endif                       /* ] */
      r = CALL_taos_stmt_bind_param_batch(stmt->tsdb_stmt.stmt, mbs);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->tsdb_stmt.stmt));
        return SQL_ERROR;
      }
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */

#ifdef HAVE_TAOSWS           /* [ */
    if (stmt->conn->cfg.url) {
      r = CALL_ws_stmt_add_batch((WS_STMT*)stmt->tsdb_stmt.stmt);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->tsdb_stmt.stmt));
        return SQL_ERROR;
      }
    } else {
#endif                       /* ] */
      r = CALL_taos_stmt_add_batch(stmt->tsdb_stmt.stmt);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->tsdb_stmt.stmt));
        return SQL_ERROR;
      }
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */

    sr = stmt->base->execute(stmt->base);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
#ifdef USE_TICK_TO_DEBUG                 /* { */
    _stmt_reset_ticks(stmt);
#endif                                   /* } */

    if (param_state->row_err) return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_execute_with_params(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  SQLSMALLINT n = 0;
  sr = _stmt_get_num_params(stmt, &n);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (n < 0) {
    stmt_niy(stmt);
    return SQL_ERROR;
  }

  if (APD_header->DESC_COUNT < n) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:The number of parameters[%d] specified in SQLBindParameter was less than the number of parameters[%d] in the SQL statement",
        APD_header->DESC_COUNT, n);
    return SQL_ERROR;
  }

  if (APD_header->DESC_COUNT > n) {
    // NOTE: just ignore them
    // stmt_niy(stmt);
    // return SQL_ERROR;
  }

  sqls_t *sqls = &stmt->sqls;
  if (sqls->nr > 1) {
    // ref: https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/batches-of-sql-statements?view=sql-server-ver16
    stmt_append_err_format(stmt, "HY000", 0, "General error:batch statements with arrays of parameters not supported yet");
    return SQL_ERROR;
  }

  param_state_t *param_state = &stmt->param_state;
  _param_state_reset(param_state);
  param_state->nr_tsdb_fields            = n;
  param_state->i_batch_offset            = 0;

  sr = _stmt_init_param_state_cnvs(stmt, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return _stmt_execute_with_param_state(stmt, param_state);
}

static SQLRETURN _stmt_execute(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }

  sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;
  if (sqlc_tsdb->qms > 0) {
    return _stmt_execute_with_params(stmt);
  }

  SQLRETURN sr = stmt->base->execute(stmt->base);
#ifdef USE_TICK_TO_DEBUG                 /* { */
  _stmt_reset_ticks(stmt);
#endif                                   /* } */
  return sr;
}

static SQLRETURN _stmt_exec_direct(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->ds_conn.taos);

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }

  sr = _stmt_prepare(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _stmt_execute(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  return _stmt_fill_IRD(stmt);
}

static SQLRETURN _stmt_exec_direct_sql(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);

  int prev = atomic_fetch_add(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  sr = _stmt_exec_direct(stmt);

  prev = atomic_fetch_sub(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  return sr;
}

static SQLRETURN _stmt_exec_direct_with_simple_sql(stmt_t *stmt)
{
  const sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;

  const char *start = sqlc_tsdb->tsdb;
  const char *end   = sqlc_tsdb->tsdb + sqlc_tsdb->tsdb_bytes;
  if (end > start && start[0] == '!') {
    stmt_niy(stmt);
    return SQL_ERROR;
  }

  return _stmt_exec_direct_sql(stmt);
}

static SQLRETURN _stmt_prepare_ext(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;
  sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;

  const char *start = sqlc_tsdb->tsdb;
  const char *end   = sqlc_tsdb->tsdb + sqlc_tsdb->tsdb_bytes;

  if (sqlc_tsdb->qms) {
    stmt_append_err(stmt, "HY000", 0, "General error:parameterized-topic-consumer-statement not supported yet");
    return SQL_ERROR;
  }
  ext_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  r = ext_parser_parse(start, end-start, &param);
  if (r) {
    parser_loc_t *loc = &param.ctx.bad_token;
    stmt_append_err_format(stmt, "HY000", 0, "General error:parsing:%.*s", (int)(end-start), start);
    stmt_append_err_format(stmt, "HY000", 0, "General error:location:(%d,%d)->(%d,%d)",
        loc->first_line, loc->first_column, loc->last_line, loc->last_column);
    stmt_append_err_format(stmt, "HY000", 0, "General error:failed:%.*s", (int)strlen(param.ctx.err_msg), param.ctx.err_msg);
    stmt_append_err(stmt, "HY000", 0, "General error:taos_odbc_extended syntax for `topic`: !topic [name]+ [{[key[=val];]*}]?");
    stmt_append_err(stmt, "HY000", 0, "General error:taos_odbc_extended syntax for `insert`: !insert into ...");

    ext_parser_param_release(&param);
    return SQL_ERROR;
  }

  if (param.is_topic) {
    sr = topic_open(&stmt->topic, sqlc_tsdb, &param.topic_cfg);
    ext_parser_param_release(&param);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    stmt->base = &stmt->topic.base;

    return SQL_SUCCESS;
  }

  stmt_append_err_format(stmt, "HY000", 0, "General error:parsing:%.*s", (int)(end-start), start);
  stmt_append_err(stmt, "HY000", 0, "General error:taos_odbc_extended syntax for `insert`: !insert into ...");

  ext_parser_param_release(&param);

  return SQL_ERROR;
}

static int _stmt_sql_parse(stmt_t *stmt, const char *sql, size_t len)
{
  SQLRETURN sr = SQL_SUCCESS;

  sqls_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  param.sql_found = _stmt_sql_found;
  param.arg       = stmt;

  sr = _stmt_cache_and_parse(stmt, &param, sql, len);
  sqls_parser_param_release(&param);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sqls_t *sqls = &stmt->sqls;
  if (sqls->nr > 1) {
    for (size_t i=0; i<sqls->nr; ++i) {
      sqls_parser_nterm_t *nterms = sqls->sqls + i;
      if (nterms->qms) {
        stmt_append_err(stmt, "HY000", 0, "General error:parameterized-statement in batch-statements not supported yet");
        return SQL_ERROR;
      }
    }
  }

  sr = _stmt_get_next_sql(stmt);
  if (sr == SQL_NO_DATA) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:empty sql statement");
    return SQL_ERROR;
  }
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

SQLRETURN stmt_prepare(stmt_t *stmt,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_unprepare(stmt);

  const char *sql = (const char*)StatementText;

  size_t len = TextLength;
  if (TextLength == SQL_NTS) len = strlen(sql);
  else                       len = strnlen(sql, TextLength);

  sr = _stmt_sql_parse(stmt, sql, len);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (stmt->tsdb_stmt.is_ext) {
    return _stmt_prepare_ext(stmt);
  }

  return _stmt_prepare(stmt);
}

SQLRETURN stmt_exec_direct(stmt_t *stmt, SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  SQLRETURN sr = SQL_SUCCESS;

  // column-binds remain valid among executes
  _stmt_close_result(stmt);

  const char *sql = (const char*)StatementText;

  size_t len = TextLength;
  if (TextLength == SQL_NTS) len = strlen(sql);
  else                       len = strnlen(sql, TextLength);

  sr = _stmt_sql_parse(stmt, sql, len);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (stmt->tsdb_stmt.is_ext) {
    sr = _stmt_prepare_ext(stmt);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    sr = _stmt_execute(stmt);
    if (sr == SQL_ERROR) return SQL_ERROR;
    return _stmt_fill_IRD(stmt);
  }

  return _stmt_exec_direct_with_simple_sql(stmt);
}

static SQLRETURN _stmt_set_row_array_size(stmt_t *stmt, SQLULEN row_array_size)
{
  if (row_array_size == 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`0` for `SQL_ATTR_ROW_ARRAY_SIZE` is substituted by current value");
    return SQL_SUCCESS_WITH_INFO;
  }

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_ARRAY_SIZE = row_array_size;

  return SQL_SUCCESS;
}

SQLRETURN stmt_execute(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->ds_conn.taos);

  // NOTE: no need to check whether it's prepared or not, DM would have already checked

  sr = _stmt_execute(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;
  return _stmt_fill_IRD(stmt);
}

static SQLRETURN _stmt_set_cursor_type(stmt_t *stmt, SQLULEN cursor_type)
{
  switch (cursor_type) {
    case SQL_CURSOR_FORWARD_ONLY:
    case SQL_CURSOR_STATIC:
      stmt->cursor_type = cursor_type;
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s` for `SQL_ATTR_CURSOR_TYPE` not supported yet", sql_cursor_type(cursor_type));
      return SQL_ERROR;
  }
}

SQLRETURN stmt_set_attr(stmt_t *stmt, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_APP_PARAM_DESC:
      return _stmt_set_param_desc(stmt, ValuePtr);
    case SQL_ATTR_APP_ROW_DESC:
      return _stmt_set_row_desc(stmt, ValuePtr);
    case SQL_ATTR_ASYNC_ENABLE:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_ASYNC_ENABLE_OFF) return SQL_SUCCESS;
      break;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_ASYNC_STMT_EVENT:
      break;
    case SQL_ATTR_ASYNC_STMT_PCALLBACK:
      break;
    case SQL_ATTR_ASYNC_STMT_PCONTEXT:
      break;
#endif                       /* } */
    case SQL_ATTR_CONCURRENCY:
      if (stmt->conn->cfg.customproduct == CUSTP_ADO) {
        if ((SQLULEN)(uintptr_t)ValuePtr == SQL_CONCUR_LOCK || (SQLULEN)(uintptr_t)ValuePtr == SQL_CONCUR_READ_ONLY) 
        {
          stmt->concurrency_attr = (SQLULEN)ValuePtr;
          return SQL_SUCCESS;
        }
      }
      break;
    case SQL_ATTR_CURSOR_SCROLLABLE:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_NONSCROLLABLE) return SQL_SUCCESS;
      break;
    case SQL_ATTR_CURSOR_SENSITIVITY:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_UNSPECIFIED) return SQL_SUCCESS;
      break;
    case SQL_ATTR_CURSOR_TYPE:
      return _stmt_set_cursor_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ENABLE_AUTO_IPD:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_FALSE) return SQL_SUCCESS;
      break;
    case SQL_ATTR_FETCH_BOOKMARK_PTR:
      break;
    case SQL_ATTR_IMP_PARAM_DESC:
      break;
    case SQL_ATTR_IMP_ROW_DESC:
      break;
    case SQL_ATTR_KEYSET_SIZE:
      break;
    case SQL_ATTR_MAX_LENGTH:
      return _stmt_set_max_length(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_MAX_ROWS:
      if ((SQLULEN)(uintptr_t)ValuePtr == 0) return SQL_SUCCESS;
      break;
    case SQL_ATTR_METADATA_ID:
      // FIXME:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_FALSE) return SQL_SUCCESS;
      break;
    case SQL_ATTR_NOSCAN:
      // FIXME:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_NOSCAN_ON) return SQL_SUCCESS;
      break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      break;
    case SQL_ATTR_PARAM_BIND_TYPE:
      return _stmt_set_param_bind_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_PARAM_OPERATION_PTR:
      break;
    case SQL_ATTR_PARAM_STATUS_PTR:
      return _stmt_set_param_status_ptr(stmt, (SQLUSMALLINT*)ValuePtr);
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      return _stmt_set_params_processed_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_PARAMSET_SIZE:
      return _stmt_set_paramset_size(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_QUERY_TIMEOUT:
      if ((SQLULEN)(uintptr_t)ValuePtr == 0) return SQL_SUCCESS;
      stmt_append_err_format(stmt, "01S02", 0, "Option value changed:`%zd` for `SQL_ATTR_QUERY_TIMEOUT` is substituted by `0`", (SQLULEN)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_RETRIEVE_DATA:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_RD_ON) return SQL_SUCCESS;
      stmt_append_err_format(stmt, "01S02", 0, "Option value changed:`%zd` for `SQL_ATTR_RETRIEVE_DATA` is substituted by `SQL_RD_ON`", (SQLULEN)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_ROW_ARRAY_SIZE:
      return _stmt_set_row_array_size(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      return _stmt_set_row_bind_offset_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_ROW_BIND_TYPE:
      return _stmt_set_row_bind_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_NUMBER:
      break;
    case SQL_ATTR_ROW_OPERATION_PTR:
      break;
    case SQL_ATTR_ROW_STATUS_PTR:
      return _stmt_set_row_status_ptr(stmt, (SQLUSMALLINT*)ValuePtr);
    case SQL_ATTR_ROWS_FETCHED_PTR:
      return _stmt_set_rows_fetched_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_SIMULATE_CURSOR:
      break;
    case SQL_ATTR_USE_BOOKMARKS:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_UB_OFF) return SQL_SUCCESS;
      break;
    case SQL_ROWSET_SIZE:
      return _stmt_set_row_array_size(stmt, (SQLULEN)ValuePtr);
      break;
    default:
      break;
  }

  stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` for `%p` not supported yet",
      sql_stmt_attr(Attribute), Attribute, Attribute, ValuePtr);
  return SQL_ERROR;
}

#if (ODBCVER >= 0x0300)          /* { */
SQLRETURN stmt_get_attr(stmt_t *stmt,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  (void)BufferLength;
  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_APP_PARAM_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(stmt->current_APD);
      return SQL_SUCCESS;
    case SQL_ATTR_APP_ROW_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(stmt->current_ARD);
      return SQL_SUCCESS;
    case SQL_ATTR_ASYNC_ENABLE:
      break;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ATTR_ASYNC_STMT_EVENT:
      break;
    case SQL_ATTR_ASYNC_STMT_PCALLBACK:
      break;
    case SQL_ATTR_ASYNC_STMT_PCONTEXT:
      break;
#endif                       /* } */
    case SQL_ATTR_CONCURRENCY:
      *(SQLULEN*)Value = stmt->concurrency_attr;
      return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_SCROLLABLE:
      break;
    case SQL_ATTR_CURSOR_SENSITIVITY:
      break;
    case SQL_ATTR_CURSOR_TYPE:
      *(SQLULEN*)Value = stmt->cursor_type;
      return SQL_SUCCESS;
    case SQL_ATTR_ENABLE_AUTO_IPD:
      break;
    case SQL_ATTR_FETCH_BOOKMARK_PTR:
      break;
    case SQL_ATTR_IMP_PARAM_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(&stmt->IPD);
      return SQL_SUCCESS;
    case SQL_ATTR_IMP_ROW_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(&stmt->IRD);
      return SQL_SUCCESS;
    case SQL_ATTR_KEYSET_SIZE:
      break;
    case SQL_ATTR_MAX_LENGTH:
      break;
    case SQL_ATTR_MAX_ROWS:
      break;
    case SQL_ATTR_METADATA_ID:
      break;
    case SQL_ATTR_NOSCAN:
      break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      break;
    case SQL_ATTR_PARAM_BIND_TYPE:
      break;
    case SQL_ATTR_PARAM_OPERATION_PTR:
      break;
    case SQL_ATTR_PARAM_STATUS_PTR:
      break;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      break;
    case SQL_ATTR_PARAMSET_SIZE:
      break;
    case SQL_ATTR_QUERY_TIMEOUT:
      *(SQLULEN*)Value = 0;
      return SQL_SUCCESS;
    case SQL_ATTR_RETRIEVE_DATA:
      break;
    case SQL_ATTR_ROW_ARRAY_SIZE:
      *(SQLULEN*)Value = (SQLULEN)_stmt_get_row_array_size(stmt);
      return SQL_SUCCESS;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      break;
    case SQL_ATTR_ROW_BIND_TYPE:
      break;
    case SQL_ATTR_ROW_NUMBER:
      break;
    case SQL_ATTR_ROW_OPERATION_PTR:
      break;
    case SQL_ATTR_ROW_STATUS_PTR:
      break;
    case SQL_ATTR_ROWS_FETCHED_PTR:
      break;
    case SQL_ATTR_SIMULATE_CURSOR:
      break;
    case SQL_ATTR_USE_BOOKMARKS:
      break;
    case SQL_ROWSET_SIZE:
      *(SQLULEN*)Value = (SQLULEN)_stmt_get_row_array_size(stmt);
      return SQL_SUCCESS;
    default:
      break;
  }
  stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
  return SQL_ERROR;
}
#endif                           /* } */

SQLRETURN stmt_free_stmt(stmt_t *stmt, SQLUSMALLINT Option)
{
  switch (Option) {
    case SQL_CLOSE:
      // TODO:
      // stmt_append_err_format(stmt, "01000", 0, "General warning:`%s[0x%x/%d]` not supported yet", sql_free_statement_option(Option), Option, Option);
      // return SQL_SUCCESS_WITH_INFO;
      _stmt_close_result(stmt);
      return SQL_SUCCESS;
    case SQL_UNBIND:
      _stmt_unbind_cols(stmt);
      return SQL_SUCCESS;
    case SQL_RESET_PARAMS:
      _stmt_reset_params(stmt);
      return SQL_SUCCESS;
    case SQL_DROP:
      // NOTE: don't know why windows odbc-driver-manager does not map SQL_DROP to SQLFreeHandle
      return stmt_free(stmt);
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_free_statement_option(Option), Option, Option);
      return SQL_ERROR;
  }
}

SQLRETURN stmt_close_cursor(stmt_t *stmt)
{
  // TODO:
  // stmt_append_err(stmt, "24000", 0, "Invalid cursor state:no cursor is open");
  // return SQL_SUCCESS_WITH_INFO;
  _stmt_close_result(stmt);
  return SQL_SUCCESS;
}

void stmt_clr_errs(stmt_t *stmt)
{
  errs_clr(&stmt->errs);
}

SQLRETURN stmt_tables(stmt_t *stmt,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *TableType,
    SQLSMALLINT    NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);
  _stmt_reset_params(stmt);
  mem_reset(&stmt->raw);
  mem_reset(&stmt->tsdb_sql);

  sr = tables_open(&stmt->tables, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, TableType, NameLength4);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_tables(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->tables.base;

  return _stmt_fill_IRD(stmt);
}

static SQLRETURN _stmt_get_diag_cursor_row_number(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)DiagIdentifier;
  (void)BufferLength;
  (void)StringLengthPtr;
  
  if (RecNumber == 0) {
    *(SQLLEN*)DiagInfoPtr = SQL_ROW_NUMBER_UNKNOWN;
  } else {
    *(SQLINTEGER*)DiagInfoPtr = (SQLINTEGER)stmt->errs.count;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_diag_field_row_number(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)DiagIdentifier;
  (void)BufferLength;
  (void)StringLengthPtr;

  if (RecNumber!=1) OA_NIY(0);
  if (1) {
    *(SQLLEN*)DiagInfoPtr = SQL_ROW_NUMBER_UNKNOWN;
    return SQL_SUCCESS;
  }

  // FIXME: get or put?
  tsdb_res_t           *res          = &stmt->tsdb_stmt.res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

  *(SQLLEN*)DiagInfoPtr = (SQLLEN)rows_block->pos;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_diag_number(
    stmt_t         *stmt,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)DiagIdentifier;
  (void)BufferLength;
  (void)StringLengthPtr;

  if (1) {
    *(SQLINTEGER*)DiagInfoPtr = (SQLINTEGER)stmt->errs.count;
    return SQL_SUCCESS;
  }

  // FIXME: get or put?
  tsdb_res_t           *res          = &stmt->tsdb_stmt.res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

  *(SQLLEN*)DiagInfoPtr = (SQLLEN)rows_block->pos;

  return SQL_SUCCESS;
}

SQLRETURN stmt_get_diag_field(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (DiagIdentifier) {
    case SQL_DIAG_SQLSTATE:
      return errs_get_diag_field_sqlstate(&stmt->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_ROW_NUMBER:
      return _stmt_get_diag_field_row_number(stmt, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_CURSOR_ROW_COUNT:
      return _stmt_get_diag_cursor_row_number(stmt, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_NUMBER:
      return _stmt_get_diag_number(stmt, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    default:
      OE("RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}

SQLRETURN stmt_col_attribute(
    stmt_t         *stmt,
    SQLUSMALLINT    ColumnNumber,
    SQLUSMALLINT    FieldIdentifier,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr,
    SQLLEN         *NumericAttributePtr)
{
  if (ColumnNumber == 0) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:ColumnNumber #%d not supported yet", ColumnNumber);
    return SQL_ERROR;
  }

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_record_t *IRD_record = IRD->records + ColumnNumber - 1;

  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
  switch(FieldIdentifier) {
    case SQL_DESC_AUTO_UNIQUE_VALUE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_AUTO_UNIQUE_VALUE;
      return SQL_SUCCESS;
    case SQL_DESC_BASE_COLUMN_NAME:
    case SQL_DESC_BASE_TABLE_NAME:
      if (_stmt_col_set_empty_string(stmt, CharacterAttributePtr, BufferLength, StringLengthPtr) != SQL_SUCCESS) break;
      return SQL_SUCCESS;
    case SQL_DESC_CASE_SENSITIVE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_CASE_SENSITIVE;
      return SQL_SUCCESS;
    case SQL_DESC_CATALOG_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_CATALOG_NAME, sizeof(IRD_record->DESC_CATALOG_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_CONCISE_TYPE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_CONCISE_TYPE;
      return SQL_SUCCESS;
    case SQL_DESC_COUNT:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_COUNT;
      return SQL_SUCCESS;
    case SQL_DESC_DISPLAY_SIZE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_DISPLAY_SIZE;
      return SQL_SUCCESS;
    case SQL_DESC_FIXED_PREC_SCALE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_FIXED_PREC_SCALE;
      return SQL_SUCCESS;
    case SQL_DESC_LABEL: // FIXME: share the same result?
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LABEL, sizeof(IRD_record->DESC_LABEL), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_LENGTH:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_LENGTH;
      return SQL_SUCCESS;
    case SQL_DESC_LITERAL_PREFIX:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LITERAL_PREFIX, sizeof(IRD_record->DESC_LITERAL_PREFIX), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_LITERAL_SUFFIX:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LITERAL_SUFFIX, sizeof(IRD_record->DESC_LITERAL_SUFFIX), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_LOCAL_TYPE_NAME: // FIXME: share the same result?
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LOCAL_TYPE_NAME, sizeof(IRD_record->DESC_LOCAL_TYPE_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_NAME, sizeof(IRD_record->DESC_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_NULLABLE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_NULLABLE;
      return SQL_SUCCESS;
    case SQL_DESC_NUM_PREC_RADIX:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_NUM_PREC_RADIX;
      return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_OCTET_LENGTH;
      return SQL_SUCCESS;
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
    case SQL_DESC_PRECISION:
    case SQL_COLUMN_PRECISION:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_PRECISION;
      return SQL_SUCCESS;
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
    case SQL_DESC_SCALE:
    case SQL_COLUMN_SCALE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_SCALE;
      return SQL_SUCCESS;
    case SQL_DESC_SCHEMA_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_SCHEMA_NAME, sizeof(IRD_record->DESC_SCHEMA_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_SEARCHABLE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_SEARCHABLE;
      return SQL_SUCCESS;
    case SQL_DESC_TABLE_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_TABLE_NAME, sizeof(IRD_record->DESC_TABLE_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_TYPE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_TYPE;
      return SQL_SUCCESS;
    case SQL_DESC_TYPE_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_TYPE_NAME, sizeof(IRD_record->DESC_TYPE_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_UNNAMED:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_UNNAMED;
      return SQL_SUCCESS;
    case SQL_DESC_UNSIGNED:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_UNSIGNED;
      return SQL_SUCCESS;
    case SQL_DESC_UPDATABLE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_UPDATABLE;
      return SQL_SUCCESS;

    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[%d/0x%x]` not supported yet", sql_col_attribute(FieldIdentifier), FieldIdentifier, FieldIdentifier);
      return SQL_ERROR;
  }

  stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[%d/0x%x]` for `%s` not supported yet",
      sql_col_attribute(FieldIdentifier), FieldIdentifier, FieldIdentifier,
      taos_data_type(IRD_record->tsdb_type));
  return SQL_ERROR;
}

SQLRETURN stmt_more_results(
    stmt_t         *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (stmt->base == &stmt->topic.base) {
    sr = stmt->base->more_results(stmt->base);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    return _stmt_fill_IRD(stmt);
  }

  if (stmt->base == &stmt->tsdb_stmt.base) {
#ifdef USE_TICK_TO_DEBUG                 /* { */
    _stmt_report_ticks(stmt);
#endif                                   /* } */
    sr = _stmt_get_next_sql(stmt);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    return _stmt_exec_direct_with_simple_sql(stmt);
  }

  return SQL_NO_DATA;
}

SQLRETURN stmt_columns(
    stmt_t         *stmt,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLCHAR *ColumnName, SQLSMALLINT NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);
  _stmt_reset_params(stmt);
  mem_reset(&stmt->raw);
  mem_reset(&stmt->tsdb_sql);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);
  if (ColumnName && NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen((const char*)ColumnName);

  sr = columns_open(&stmt->columns, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_columns(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->columns.base;

  return _stmt_fill_IRD(stmt);
}

#if (ODBCVER >= 0x0300)       /* { */
SQLRETURN stmt_bulk_operations(
    stmt_t             *stmt,
    SQLSMALLINT         Operation)
{
  stmt_append_err_format(stmt, "HY000", 0, "General error:Operation `%s[%d/0x%x]` not supported yet", sql_bulk_operation(Operation), Operation, Operation);
  return SQL_ERROR;
}

#endif                        /* } */

SQLRETURN stmt_column_privileges(
    stmt_t       *stmt,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *TableName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;
  const char *column  = (const char *)ColumnName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";
  if (column  == NULL) column  = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);
  if (NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen(column);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_extended_fetch(
    stmt_t          *stmt,
    SQLUSMALLINT     FetchOrientation,
    SQLLEN           FetchOffset,
    SQLULEN         *RowCountPtr,
    SQLUSMALLINT    *RowStatusArray)
{
  (void)RowCountPtr;
  (void)RowStatusArray;
  stmt_append_err_format(stmt, "HY000", 0, "General error:FetchOrientation[%d/0x%x],FetchOffset[%zd/0x%zx] not supported yet",
      FetchOrientation, FetchOrientation, FetchOffset, FetchOffset);
  return SQL_ERROR;
}

SQLRETURN stmt_foreign_keys(
    stmt_t        *stmt,
    SQLCHAR       *PKCatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *PKSchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *PKTableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *FKCatalogName,
    SQLSMALLINT    NameLength4,
    SQLCHAR       *FKSchemaName,
    SQLSMALLINT    NameLength5,
    SQLCHAR       *FKTableName,
    SQLSMALLINT    NameLength6)
{
  (void)stmt;

  const char *pkcatalog            = (const char*)PKCatalogName;
  const char *pkschema             = (const char*)PKSchemaName;
  const char *pktable              = (const char*)PKTableName;
  const char *fkcatalog            = (const char*)FKCatalogName;
  const char *fkschema             = (const char*)FKSchemaName;
  const char *fktable              = (const char*)FKTableName;

  if (!pkcatalog)             pkcatalog = "";
  if (!pkschema)              pkschema  = "";
  if (!pktable)               pktable   = "";
  if (!fkcatalog)             fkcatalog = "";
  if (!fkschema)              fkschema  = "";
  if (!fktable)               fktable   = "";

  if (NameLength1 == SQL_NTS)       NameLength1 = (SQLSMALLINT)strlen(pkcatalog);
  if (NameLength2 == SQL_NTS)       NameLength2 = (SQLSMALLINT)strlen(pkschema);
  if (NameLength3 == SQL_NTS)       NameLength3 = (SQLSMALLINT)strlen(pktable);
  if (NameLength4 == SQL_NTS)       NameLength4 = (SQLSMALLINT)strlen(fkcatalog);
  if (NameLength5 == SQL_NTS)       NameLength5 = (SQLSMALLINT)strlen(fkschema);
  if (NameLength6 == SQL_NTS)       NameLength6 = (SQLSMALLINT)strlen(fktable);

  return SQL_NO_DATA;
}

SQLRETURN stmt_get_cursor_name(
    stmt_t       *stmt,
    SQLCHAR      *CursorName,
    SQLSMALLINT   BufferLength,
    SQLSMALLINT  *NameLengthPtr)
{
  (void)CursorName;
  (void)BufferLength;
  (void)NameLengthPtr;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_get_type_info(
    stmt_t       *stmt,
    SQLSMALLINT   DataType)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = typesinfo_open(&stmt->typesinfo, DataType);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_typesinfo(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->typesinfo.base;

  return _stmt_fill_IRD(stmt);
}

SQLRETURN stmt_param_data(
    stmt_t       *stmt,
    SQLPOINTER   *Value)
{
  (void)Value;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_primary_keys(
    stmt_t        *stmt,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);
  _stmt_reset_params(stmt);
  mem_reset(&stmt->raw);
  mem_reset(&stmt->tsdb_sql);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);

  sr = primarykeys_open(&stmt->primarykeys, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_primarykeys(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->primarykeys.base;

  return _stmt_fill_IRD(stmt);
}

SQLRETURN stmt_procedure_columns(
    stmt_t       *stmt,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *ProcName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *proc    = (const char *)ProcName;
  const char *column  = (const char *)ColumnName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (proc    == NULL) proc    = "";
  if (column  == NULL) column  = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(proc);
  if (NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen(column);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_procedures(
    stmt_t         *stmt,
    SQLCHAR        *CatalogName,
    SQLSMALLINT     NameLength1,
    SQLCHAR        *SchemaName,
    SQLSMALLINT     NameLength2,
    SQLCHAR        *ProcName,
    SQLSMALLINT     NameLength3)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *proc    = (const char *)ProcName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (proc    == NULL) proc    = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(proc);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_put_data(
    stmt_t         *stmt,
    SQLPOINTER      Data,
    SQLLEN          StrLen_or_Ind)
{
  (void)Data;
  (void)StrLen_or_Ind;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_set_cursor_name(
    stmt_t         *stmt,
    SQLCHAR        *CursorName,
    SQLSMALLINT     NameLength)
{
  const char *cursor         = (const char*)CursorName;
  if (!cursor) cursor = "";
  if (NameLength == SQL_NTS) NameLength = (SQLSMALLINT)strlen(cursor);
  stmt_append_err_format(stmt, "HY000", 0, "General error:CursorName[%.*s] not supported yet", NameLength, cursor);
  return SQL_ERROR;
}

SQLRETURN stmt_set_pos(
    stmt_t         *stmt,
    SQLSETPOSIROW   RowNumber,
    SQLUSMALLINT    Operation,
    SQLUSMALLINT    LockType)
{
  stmt_append_err_format(stmt, "HY000", 0, "General error:RowNumber[%zd],Operation `%s[%d/0x%x]`,LockType `%s[%d/0x%x]`",
      RowNumber, sql_pos_operation(Operation), Operation, Operation, sql_pos_locktype(LockType), LockType, LockType);
  return SQL_ERROR;
}

SQLRETURN stmt_special_columns(
    stmt_t         *stmt,
    SQLUSMALLINT    IdentifierType,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope, SQLUSMALLINT Nullable)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);

  stmt_append_err_format(stmt, "HY000", 0, "General error:Identifier `%s[%d/0x%x]`,Scope `%s[%d/0x%x]`,Nullable `%s[%d/0x%x]` not supported yet",
      sql_special_columns_identifier(IdentifierType), IdentifierType, IdentifierType,
      sql_scope(Scope), Scope, Scope,
      sql_nullable(Nullable), Nullable, Nullable);
  return SQL_ERROR;
}

SQLRETURN stmt_statistics(
    stmt_t  *stmt,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);

  stmt_append_err_format(stmt, "HY000", 0, "General error:Unique `%s[%d/0x%x]`,Reserved `%s[%d/0x%x]` not supported yet",
      sql_index(Unique), Unique, Unique,
      sql_statistics_reserved(Reserved), Reserved, Reserved);
  return SQL_ERROR;
}

SQLRETURN stmt_table_privileges(
    stmt_t  *stmt,
    SQLCHAR *CatalogName,
    SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName,
    SQLSMALLINT NameLength2,
    SQLCHAR *TableName,
    SQLSMALLINT NameLength3)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_complete_async(
    stmt_t      *stmt,
    RETCODE     *AsyncRetCodePtr)
{
  (void)AsyncRetCodePtr;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

