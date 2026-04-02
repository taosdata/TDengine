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

#include "tsdb.h"

#include "desc.h"
#include "errs.h"
#include "log.h"
#include "stmt.h"
#include "taos_helpers.h"
#ifdef HAVE_TAOSWS           /* { */
#include "taosws_helpers.h"
#endif                       /* } */

#include <errno.h>

static int _tsdb_timestamp_to_tm_local(int64_t val, int time_precision, struct tm *tm, int32_t *fraction, int *w)
{
  time_t  tt;
  int32_t xfraction = 0;
  int xw;
  switch (time_precision) {
    case 2:
      tt = (time_t)(val / 1000000000);
      xfraction = val % 1000000000;
      xw = 9;
      break;
    case 1:
      tt = (time_t)(val / 1000000);
      xfraction = val % 1000000;
      xw = 6;
      break;
    case 0:
      tt = (time_t)(val / 1000);
      xfraction = val % 1000;
      xw = 3;
      break;
    default:
      OA_ILE(0);
      break;
  }

  if (tt <= 0 && xfraction < 0) {
    OA_NIY(0);
  }

  struct tm *p = localtime_r(&tt, tm);
  if (p != tm) return -1;
  if (fraction) *fraction = xfraction;
  if (w)  *w  = xw;
  return 0;
}

int tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(int64_t val, int time_precision, SQL_TIMESTAMP_STRUCT *ts)
{
  int32_t fraction = 0;
  int w;
  struct tm ptm = {0};
  int r = _tsdb_timestamp_to_tm_local(val, time_precision, &ptm, &fraction, &w);
  if (r) return -1;
  ts->year       = ptm.tm_year + 1900;
  ts->month      = ptm.tm_mon + 1;
  ts->day        = ptm.tm_mday;
  ts->hour       = ptm.tm_hour;
  ts->minute     = ptm.tm_min;
  ts->second     = ptm.tm_sec;
  switch (time_precision) {
    case 0:
      ts->fraction = fraction * 1000000;
      break;
    case 1:
      ts->fraction = fraction * 1000;
      break;
    default:
      ts->fraction = fraction;
      break;
  }

  return 0;
}

int tsdb_timestamp_to_string(int64_t val, int time_precision, char *buf, size_t len)
{
  int n;
  int32_t fraction = 0;
  int w;
  struct tm ptm = {0};
  int r = _tsdb_timestamp_to_tm_local(val, time_precision, &ptm, &fraction, &w);
  if (r) return -1;

  n = snprintf(buf, len,
      "%04d-%02d-%02d %02d:%02d:%02d.%0*d",
      ptm.tm_year + 1900, ptm.tm_mon + 1, ptm.tm_mday,
      ptm.tm_hour, ptm.tm_min, ptm.tm_sec,
      w, fraction);

  OA_ILE(n > 0);

  return n;
}

static void _tsdb_param_column_release(tsdb_param_column_t *pa)
{
  mem_release(&pa->mem);
  mem_release(&pa->mem_is_null);
  mem_release(&pa->mem_length);
}

void tsdb_paramset_reset(tsdb_paramset_t *paramset)
{
  paramset->nr = 0;
}

void tsdb_paramset_release(tsdb_paramset_t *paramset)
{
  for (int i=0; i<paramset->cap; ++i) {
    tsdb_param_column_t *pa = paramset->params + i;
    _tsdb_param_column_release(pa);
  }
  paramset->cap = 0;
  paramset->nr  = 0;
  TOD_SAFE_FREE(paramset->params);
}

static int _tsdb_paramset_calloc(tsdb_paramset_t *paramset, int nr)
{
  tsdb_paramset_reset(paramset);
  if (nr > paramset->cap) {
    int cap = (nr + 15) / 16 * 16;
    tsdb_param_column_t *pas= (tsdb_param_column_t*)realloc(paramset->params, sizeof(*pas) * cap);
    if (!pas) return -1;

    memset(pas + paramset->cap, 0, sizeof(*pas) * (cap - paramset->cap));

    paramset->cap    = cap;
    paramset->nr     = nr;
    paramset->params = pas;
  }
  paramset->nr = nr;
  return 0;
}

static void _tsdb_params_reset_tag_fields(tsdb_params_t *params)
{
  if (params->tag_fields) {
#ifdef HAVE_TAOSWS           /* [ */
    if (params->owner->owner->conn->cfg.url) {
      // OA_NIY(params->tag_fields == NULL);
      CALL_ws_stmt_reclaim_fields(params->owner->stmt, (struct StmtField**)&params->tag_fields, params->nr_tag_fields);
    } else {
#endif                       /* ] */
      CALL_taos_stmt_reclaim_fields(params->owner->stmt, params->tag_fields);
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */
    params->tag_fields = NULL;
  }
  params->nr_tag_fields = 0;
}

static void _tsdb_params_reset_params(tsdb_params_t *params)
{
  params->nr_params = 0;
}

static const TAOS_FIELD_E        _default_param_field = {
  .name        = "",
  .type        = TSDB_DATA_TYPE_VARCHAR,
  .precision   = 0,
  .scale       = 0,
  .bytes       = 16384,
};

static void _tsdb_params_reset_col_fields(tsdb_params_t *params)
{
  if (params->col_fields == &_default_param_field) params->col_fields = NULL;

  if (params->col_fields) {
#ifdef HAVE_TAOSWS           /* [ */
    if (params->owner->owner->conn->cfg.url) {
      CALL_ws_stmt_reclaim_fields(params->owner->stmt, (struct StmtField**)&params->col_fields, params->nr_col_fields);
    } else {
#endif                       /* ] */
      CALL_taos_stmt_reclaim_fields(params->owner->stmt, params->col_fields);
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */
    params->col_fields = NULL;
  }

  params->nr_col_fields = 0;
}

static void _tsdb_params_reset(tsdb_params_t *params)
{
  _tsdb_params_reset_tag_fields(params);
  _tsdb_params_reset_col_fields(params);
  _tsdb_params_reset_params(params);

  TOD_SAFE_FREE(params->subtbl);
  params->subtbl_required = 0;

  params->qms = 0;
}

static void _tsdb_params_release(tsdb_params_t *params)
{
  _tsdb_params_reset(params);

  params->owner = NULL;
}

static void _tsdb_binds_reset(tsdb_binds_t *tsdb_binds)
{
  tsdb_binds->nr = 0;
}

void tsdb_binds_release(tsdb_binds_t *tsdb_binds)
{
  _tsdb_binds_reset(tsdb_binds);

  TOD_SAFE_FREE(tsdb_binds->mbs);
  tsdb_binds->cap = 0;
}

static void _tsdb_fields_reset(tsdb_fields_t *fields)
{
  if (!fields) return;
  fields->fields         = NULL;
  fields->nr             = 0;
}

static void _tsdb_fields_release(tsdb_fields_t *fields)
{
  if (!fields) return;
  _tsdb_fields_reset(fields);
}

static void _tsdb_rows_block_reset(tsdb_rows_block_t *rows_block)
{
  if (!rows_block) return;
  rows_block->rows                 = NULL;
  rows_block->ws_ptr               = NULL;
  rows_block->nr                   = 0;
  rows_block->pos                  = 0;
}

static void _tsdb_rows_block_release(tsdb_rows_block_t *rows_block)
{
  if (!rows_block) return;
  _tsdb_rows_block_reset(rows_block);
}

void tsdb_res_reset(tsdb_res_t *res)
{
  if (!res) return;
  _tsdb_rows_block_reset(&res->rows_block);
  _tsdb_fields_reset(&res->fields);
  if (res->res) {
    if (res->res_is_from_taos_query) {
#ifdef HAVE_TAOSWS           /* [ */
      if (res->owner->owner->conn->cfg.url) {
        CALL_ws_free_result((WS_RES*)res->res);
      } else {
#endif                       /* ] */
        CALL_taos_free_result(res->res);
#ifdef HAVE_TAOSWS           /* [ */
      }
#endif                       /* ] */
      res->res_is_from_taos_query = 0;
    }
    res->res = NULL;
  }
  res->affected_row_count = 0;
  res->time_precision     = 0;
  res->eof                = 0;
}

void tsdb_res_release(tsdb_res_t *res)
{
  if (!res) return;
  tsdb_res_reset(res);

  _tsdb_rows_block_release(&res->rows_block);
  _tsdb_fields_release(&res->fields);
}

static int _tsdb_binds_keep(tsdb_binds_t *tsdb_binds, int nr_params)
{
  _tsdb_binds_reset(tsdb_binds);
  if (nr_params > tsdb_binds->cap) {
    int cap = (nr_params + 15) / 16 * 16;
    TAOS_MULTI_BIND *mbs = (TAOS_MULTI_BIND*)realloc(tsdb_binds->mbs, sizeof(*mbs) * cap);
    if (!mbs) return -1;
    memset(mbs + tsdb_binds->cap, 0, sizeof(*mbs) * (cap - tsdb_binds->cap));
    tsdb_binds->mbs = mbs;
    tsdb_binds->cap = cap;
  }
  tsdb_binds->nr = nr_params;
  return 0;
}

static SQLRETURN _stmt_post_query(tsdb_stmt_t *stmt)
{
  tsdb_res_t          *res         = &stmt->res;
  tsdb_fields_t       *fields      = &res->fields;

#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    if (res->res) {
      res->time_precision = CALL_ws_result_precision((WS_RES*)res->res);
      if (res->time_precision < 0 || res->time_precision > 2) {
        stmt_append_err_format(stmt->owner, "HY000", 0, "General error:time_precision [%d] out of range", res->time_precision);
        return SQL_ERROR;
      }
      res->affected_row_count = CALL_ws_affected_rows((WS_RES*)res->res);
      fields->nr = CALL_ws_field_count((WS_RES*)res->res);
      if (fields->nr > 0) {
        fields->fields = (TAOS_FIELD*)ws_fetch_fields((WS_RES*)res->res);
      }
    } else {
      res->affected_row_count = CALL_ws_stmt_affected_rows((WS_RES*)stmt->stmt);
    }
  } else {
#endif                       /* ] */
    if (res->res) {
      res->time_precision = CALL_taos_result_precision(res->res);
      if (res->time_precision < 0 || res->time_precision > 2) {
        stmt_append_err_format(stmt->owner, "HY000", 0, "General error:time_precision [%d] out of range", res->time_precision);
        return SQL_ERROR;
      }
      res->affected_row_count = CALL_taos_affected_rows(res->res);
      fields->nr = CALL_taos_field_count(res->res);
      if (fields->nr > 0) {
        fields->fields = CALL_taos_fetch_fields(res->res);
      }
    } else {
      res->affected_row_count = CALL_taos_stmt_affected_rows_once(stmt->stmt);
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */
  return SQL_SUCCESS;
}

static SQLRETURN _query(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t          *res         = &stmt->res;
  tsdb_res_reset(res);
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    res->res = CALL_ws_query((WS_TAOS*)stmt->owner->conn->ds_conn.taos, sqlc_tsdb->tsdb);
  } else {
#endif                       /* ] */
    res->res = CALL_taos_query(stmt->owner->conn->ds_conn.taos, sqlc_tsdb->tsdb);
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */
  res->res_is_from_taos_query = res->res ? 1 : 0;

#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    int e = ws_errno((WS_RES*)res->res);
    if (e) {
      const char *estr = ws_errstr((WS_RES*)res->res);
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosws]%s, executing:%.*s", estr, (int)sqlc_tsdb->sqlc_bytes, sqlc_tsdb->sqlc);
      return SQL_ERROR;
    }
  } else {
#endif                       /* ] */
    int e = CALL_taos_errno(res->res);
    if (e) {
      const char *estr = CALL_taos_errstr(res->res);
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosc]%s, executing:%.*s", estr, (int)sqlc_tsdb->sqlc_bytes, sqlc_tsdb->sqlc);
      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  return _stmt_post_query(stmt);
}

static TAOS_FIELD_E* _tsdb_stmt_get_tsdb_field_by_tsdb_params(tsdb_stmt_t *stmt, int i_param)
{
  tsdb_params_t *params = &stmt->params;

  if (!stmt->is_insert_stmt) return (TAOS_FIELD_E*)&_default_param_field;

#ifdef HAVE_TAOSWS           /* [ */
  // NOTE: does taosws-rs support select-with-params?
  // if (stmt->owner->conn->cfg.url) {
  //   return (TAOS_FIELD_E*)&_default_param_field;
  // }
#endif                       /* ] */

  if (i_param == 0 && params->subtbl_required) {
    return (TAOS_FIELD_E*)&_default_param_field;
  }
  i_param -= !!params->subtbl_required;
  if (i_param < params->nr_tag_fields)
    return params->tag_fields + i_param;
  i_param -= params->nr_tag_fields;

  OA_NIY(i_param < params->nr_col_fields);
  return params->col_fields + i_param;
}

static SQLRETURN _tsdb_stmt_post_check(tsdb_stmt_t *stmt)
{
  int r = 0;

  if (stmt->is_insert_stmt) {
    SQLSMALLINT n = 0;

    n = !!stmt->params.subtbl_required;
    n += stmt->params.nr_tag_fields;
    n += stmt->params.nr_col_fields;

    stmt->params.nr_params = n;
  }

  SQLSMALLINT n = stmt->params.nr_params;
  if (n <= 0) {
    stmt_append_err(stmt->owner, "HY000", 0, "General error:statement-without-parameter-placemarker not allowed to be prepared");
    return SQL_ERROR;
  }
  if (n != stmt->params.qms) {
    stmt_append_err_format(stmt->owner, "HY000", 0,
        "General error:statement parsed by taos_odbc has #%d parameter-placemarkers, but [taosc] reports #%d parameter-placemarkers",
        stmt->params.qms, n);

    return SQL_ERROR;
  }
  if (!stmt->is_insert_stmt && n != stmt->params.nr_params) {
    stmt_append_err_format(stmt->owner, "HY000", 0,
        "General error:statement parsed by taos_odbc has #%d parameter-placemarkers, but [taosc] reports #%d parameter-placemarkers",
        stmt->params.nr_params, n);

    return SQL_ERROR;
  }

  r = _tsdb_binds_keep(&stmt->owner->tsdb_binds, n);
  if (r) {
    stmt_oom(stmt->owner);
    return SQL_ERROR;
  }

  r = _tsdb_paramset_calloc(&stmt->owner->tsdb_paramset, n);
  if (r) {
    stmt_oom(stmt->owner);
    return SQL_ERROR;
  }

  for (int i=0; i<n; ++i) {
    stmt->owner->tsdb_paramset.params[i].tsdb_field = *_tsdb_stmt_get_tsdb_field_by_tsdb_params(stmt, i);
  }

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_generate_default_param_fields(tsdb_stmt_t *stmt, size_t nr_params)
{
  tsdb_params_t *params = &stmt->params;

  params->nr_params = (int)nr_params;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_get_taos_params_for_non_insert(tsdb_stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  int nr_params = 0;
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    // stmt_append_err(stmt->owner, "HY000", r, "General error:not implemented yet");
    // tsdb_stmt_reset(stmt);
    // return SQL_ERROR;
    int nr_params = stmt->params.qms;

    sr = _tsdb_stmt_generate_default_param_fields(stmt, nr_params);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_num_params(stmt->stmt, &nr_params);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      tsdb_stmt_reset(stmt);
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;

      return SQL_ERROR;
    }

    if (nr_params != stmt->params.qms) {
      stmt_append_err_format(stmt->owner, "HY000", 0,
          "General error:statement parsed by taos_odbc has #%d parameter-placemarkers, but [taosc] reports #%d parameter-placemarkers",
          stmt->params.qms, nr_params);

      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  sr = _tsdb_stmt_generate_default_param_fields(stmt, nr_params);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  stmt->prepared = 1;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_describe_tags(tsdb_stmt_t *stmt)
{
  int r = 0;

  int tagNum = 0;
  TAOS_FIELD_E *tags = NULL;
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    // stmt_append_err(stmt->owner, "HY000", r, "General error:not implemented yet");
    // return SQL_ERROR;
    r = CALL_ws_stmt_get_tag_fields(stmt->stmt, &tagNum, (struct StmtField**)&tags);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_ws_errstr(NULL));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;

      return SQL_ERROR;
    }
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tags);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;

      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */
  stmt->params.nr_tag_fields = tagNum;
  stmt->params.tag_fields    = tags;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_describe_cols(tsdb_stmt_t *stmt)
{
  int r = 0;

  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    // stmt_append_err(stmt->owner, "HY000", r, "General error:not implemented yet");
    // return SQL_ERROR;
    r = CALL_ws_stmt_get_col_fields(stmt->stmt, &colNum, (struct StmtField**)&cols);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_ws_errstr(NULL));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;
      return SQL_ERROR;
    }
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_get_col_fields(stmt->stmt, &colNum, &cols);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;
      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */
  stmt->params.nr_col_fields = colNum;
  stmt->params.col_fields    = cols;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_get_taos_tags_cols_for_subtbled_insert(tsdb_stmt_t *stmt, int e)
{
  // fake subtbl name to get tags/cols params-info
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  const char *subtbl = stmt->params.subtbl;
  if (!subtbl) subtbl = "__hard_coded_fake_name__";
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    r = CALL_ws_stmt_set_tbname((WS_STMT*)stmt->stmt, subtbl);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->stmt));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;
      return SQL_ERROR;
    }
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_set_tbname(stmt->stmt, subtbl);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;
      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  sr = _tsdb_stmt_describe_tags(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _tsdb_stmt_describe_cols(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  // insert into ? ... will result in TSDB_CODE_TSC_STMT_TBNAME_ERROR
  stmt->params.subtbl_required = 1;
  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_get_taos_tags_cols_for_normal_insert(tsdb_stmt_t *stmt, int e)
{
  // insert into t ... and t is normal tablename, will result in TSDB_CODE_TSC_STMT_API_ERROR
  SQLRETURN sr = SQL_SUCCESS;

  stmt->params.subtbl_required = 0;
  stmt_append_err(stmt->owner, "HY000", e, "General error:this is believed an non-subtbl insert statement");
  sr = _tsdb_stmt_describe_cols(stmt);

  return sr;
}

static SQLRETURN _tsdb_stmt_get_taos_tags_cols_for_insert(tsdb_stmt_t *stmt)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;
  int tagNum = 0;
  TAOS_FIELD_E *tag_fields = NULL;
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    // // stmt_append_err(stmt->owner, "HY000", r, "General error:not implemented yet");
    // // sr = SQL_ERROR;

    // // TODO: tags not supported yet
    // stmt->params.subtbl_required = 0;
    // stmt->params.nr_tag_fields = 0;

    // stmt->params.nr_col_fields = stmt->params.qms;
    // int nr_params = stmt->params.qms;

    // sr = _tsdb_stmt_generate_default_param_fields(stmt, nr_params);
    // if (sr != SQL_SUCCESS) return SQL_ERROR;
    r = CALL_ws_stmt_get_tag_fields(stmt->stmt, &tagNum, (struct StmtField**)&tag_fields);
    if (r) {
      int e = CALL_ws_errno(NULL);
      if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
        sr = _tsdb_stmt_get_taos_tags_cols_for_subtbled_insert(stmt, r);
      } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
        sr = _tsdb_stmt_get_taos_tags_cols_for_normal_insert(stmt, r);
      } else {
        stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_ws_stmt_errstr(stmt->stmt));
        sr = SQL_ERROR;
      }
      if (tag_fields) {
        // CALL_taos_stmt_reclaim_fields(stmt->stmt, tag_fields);
        tag_fields = NULL;
      }
    } else {
      // OA_NIY(tagNum == 0);
      // OA_NIY(tag_fields == NULL);
      OA_NIY(stmt->params.tag_fields == NULL);
      OA_NIY(stmt->params.nr_tag_fields == 0);
      stmt->params.tag_fields = tag_fields;
      stmt->params.nr_tag_fields = tagNum;
      // TODO: temp modify
      // if (tagNum == 0) stmt->params.tag_fields = NULL;
      sr = _tsdb_stmt_describe_cols(stmt);
    }
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tag_fields);
    if (r) {
      int e = CALL_taos_errno(NULL);
      if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
        sr = _tsdb_stmt_get_taos_tags_cols_for_subtbled_insert(stmt, r);
      } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
        sr = _tsdb_stmt_get_taos_tags_cols_for_normal_insert(stmt, r);
      } else {
        stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
        sr = SQL_ERROR;
      }
      if (tag_fields) {
        CALL_taos_stmt_reclaim_fields(stmt->stmt, tag_fields);
        tag_fields = NULL;
      }
    } else {
      // OA_NIY(tagNum == 0);
      // OA_NIY(tag_fields == NULL);
      OA_NIY(stmt->params.tag_fields == NULL);
      OA_NIY(stmt->params.nr_tag_fields == 0);
      stmt->params.tag_fields = tag_fields;
      stmt->params.nr_tag_fields = tagNum;
      sr = _tsdb_stmt_describe_cols(stmt);
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  if (sr == SQL_SUCCESS) stmt->prepared = 1;
  return sr;
}

static SQLRETURN _tsdb_stmt_prepare(tsdb_stmt_t *stmt, const sqlc_tsdb_t *sqlc_tsdb)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  int32_t isInsert = 0;

#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    stmt->stmt = CALL_ws_stmt_init((WS_TAOS*)stmt->owner->conn->ds_conn.taos);
    if (!stmt->stmt) {
      stmt_append_err_format(stmt->owner, "HY000", CALL_taos_errno(NULL), "General error:[taosws]%s", ws_errstr(NULL));
      return SQL_ERROR;
    }

    r = CALL_ws_stmt_prepare((WS_STMT*)stmt->stmt, sqlc_tsdb->tsdb, (unsigned long)sqlc_tsdb->tsdb_bytes);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosws]%s", ws_errstr(NULL));
      return SQL_ERROR;
    }

    r = CALL_ws_stmt_is_insert((WS_STMT*)stmt->stmt, &isInsert);
    isInsert = !!isInsert;

    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->stmt));
      tsdb_stmt_reset(stmt);

      return SQL_ERROR;
    }

    if (!isInsert) {
      stmt_append_err(stmt->owner, "HY000", r, "General error:not implemented yet");
      tsdb_stmt_reset(stmt);

      return SQL_ERROR;
    }
  } else {
#endif                       /* ] */
    stmt->stmt = CALL_taos_stmt_init(stmt->owner->conn->ds_conn.taos);
    if (!stmt->stmt) {
      stmt_append_err_format(stmt->owner, "HY000", CALL_taos_errno(NULL), "General error:[taosc]%s", CALL_taos_errstr(NULL));
      return SQL_ERROR;
    }

    r = CALL_taos_stmt_prepare(stmt->stmt, sqlc_tsdb->tsdb, (unsigned long)sqlc_tsdb->tsdb_bytes);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
      return SQL_ERROR;
    }

    r = CALL_taos_stmt_is_insert(stmt->stmt, &isInsert);
    isInsert = !!isInsert;

    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      tsdb_stmt_reset(stmt);

      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */
  stmt->is_insert_stmt = isInsert;

  if (!stmt->is_insert_stmt) {
    sr = _tsdb_stmt_get_taos_params_for_non_insert(stmt);
  } else {
    sr = _tsdb_stmt_get_taos_tags_cols_for_insert(stmt);
  }
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return _tsdb_stmt_post_check(stmt);
}

static SQLRETURN _prepare(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_stmt_unprepare(stmt);
  tsdb_stmt_reset(stmt);

  stmt->current_sql = sqlc_tsdb;

  if (sqlc_tsdb->qms == 0) return SQL_SUCCESS;

  stmt->params.qms = sqlc_tsdb->qms;

  return _tsdb_stmt_prepare(stmt, sqlc_tsdb);
}

static SQLRETURN _execute(stmt_base_t *base)
{
  int r = 0;

  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t          *res         = &stmt->res;
  tsdb_res_reset(res);

  descriptor_t *APD = stmt_APD(stmt->owner);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt->owner);
  desc_header_t *IPD_header = &IPD->header;

  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err_format(stmt->owner, "HY000", 0,
        "COUNT field of APD [%d] differs with COUNT field of IPD [%d]",
        APD_header->DESC_COUNT, IPD_header->DESC_COUNT);
    return SQL_ERROR;
  }

  const SQLSMALLINT n = stmt->params.nr_params;
  if (APD_header->DESC_COUNT < n) {
    stmt_append_err_format(stmt->owner, "07002", 0,
        "COUNT field incorrect:%d parameter markers required, but only %d parameters bound",
        n, APD_header->DESC_COUNT);
    return SQL_ERROR;
  }

  if (APD_header->DESC_COUNT > n) {
    OW("bind more parameters (#%d) than required (#%d) by sql-statement", APD_header->DESC_COUNT, n);
  }

  if (stmt->params.qms == 0) {
    if (APD_header->DESC_COUNT > 0) {
      stmt_append_err(stmt->owner, "HY000", 0,
        "General error:[taos-odbc]non-parameterized-statement with param_bound is ambiguous, thus not supported yet");
      return SQL_ERROR;
    }
    return _query(base, stmt->current_sql);
  }

#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    int32_t affected_rows = 0;
    r = CALL_ws_stmt_execute((WS_STMT*)stmt->stmt, &affected_rows);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->stmt));
      return SQL_ERROR;
    }

    OA_NIY(res->res == NULL);

    int e = ws_errno((WS_RES*)res->res);
    if (e) {
      const char *estr = ws_errstr((WS_RES*)res->res);
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosws]%s", estr);
      return SQL_ERROR;
    }
    res->affected_row_count = affected_rows;
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_execute(stmt->stmt);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      return SQL_ERROR;
    }

    res->res = CALL_taos_stmt_use_result(stmt->stmt);
    res->res_is_from_taos_query = 0;

    int e = CALL_taos_errno(res->res);
    if (e) {
      const char *estr = CALL_taos_errstr(res->res);
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosc]%s", estr);
      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  return _stmt_post_query(stmt);
}

static SQLRETURN _get_col_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t           *res          = &stmt->res;

  *fields = res->fields.fields;
  *nr     = res->fields.nr;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_fetch_rows_block(tsdb_stmt_t *stmt)
{
  tsdb_res_t           *res          = &stmt->res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

  _tsdb_rows_block_reset(rows_block);

#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    tsdb_fields_t       *fields      = &res->fields;
    if (fields->nr == 0) return SQL_NO_DATA;
    const void *ptr = NULL;
    int32_t nr_rows = 0;
    int32_t r = CALL_ws_fetch_raw_block((WS_RES*)res->res, &ptr, &nr_rows);
    if (r != 0) return SQL_ERROR;
    
    if (nr_rows == 0) return SQL_NO_DATA;

    rows_block->ws_ptr = ptr;
    rows_block->nr     = nr_rows;
    rows_block->pos    = 0;
  } else {
#endif                       /* ] */
#ifdef USE_TICK_TO_DEBUG                 /* { */
    int r = stmt_enter_fetch_block(stmt->owner);
    OA_ILE(r == 0);
#endif                                   /* } */
    TAOS_ROW rows = NULL;
    int nr_rows = CALL_taos_fetch_block(res->res, &rows);
#ifdef USE_TICK_TO_DEBUG                 /* { */
    stmt_leave_fetch_block(stmt->owner);
#endif                                   /* } */
    if (nr_rows == 0) return SQL_NO_DATA;
    rows_block->rows   = rows;
    rows_block->nr     = nr_rows;
    rows_block->pos    = 0;
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t           *res          = &stmt->res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

  if (res->eof) return SQL_NO_DATA;

again:
  // TODO: before and after
  if (rows_block->pos >= rows_block->nr) {
    sr = _tsdb_stmt_fetch_rows_block(stmt);
    if (sr == SQL_NO_DATA) {
      res->eof = 1;
      return SQL_NO_DATA;
    }
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    goto again;
  }
  ++rows_block->pos;
  return SQL_SUCCESS;
}

static SQLRETURN _more_results(stmt_base_t *base)
{
  (void)base;
  return SQL_NO_DATA;
}

static SQLRETURN _tsdb_stmt_describe_param_by_field(
    tsdb_stmt_t    *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr,
    TAOS_FIELD_E   *field)
{
  int type = field->type;
  int bytes = field->bytes;

  switch (type) {
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_INTEGER;
      if (ParameterSizePtr) *ParameterSizePtr = 10;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_SMALLINT;
      if (ParameterSizePtr) *ParameterSizePtr = 5;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_TINYINT;
      if (ParameterSizePtr) *ParameterSizePtr = 3;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_BOOL:
      if (DataTypePtr)      *DataTypePtr      = SQL_TINYINT; // FIXME: SQL_BIT
      if (ParameterSizePtr) *ParameterSizePtr = 3;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_BIGINT;
      if (ParameterSizePtr) *ParameterSizePtr = 19;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (DataTypePtr)      *DataTypePtr      = SQL_REAL;
      if (ParameterSizePtr) *ParameterSizePtr = 7;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (DataTypePtr)      *DataTypePtr      = SQL_DOUBLE;
      if (ParameterSizePtr) *ParameterSizePtr = 15;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      if (DataTypePtr)      *DataTypePtr      = SQL_VARCHAR;
      if (ParameterSizePtr) *ParameterSizePtr = bytes - 2;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (DataTypePtr)      *DataTypePtr      = SQL_TYPE_TIMESTAMP;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = (field->precision + 1) * 3;
      if (ParameterSizePtr) *ParameterSizePtr = 20 + *DecimalDigitsPtr;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (DataTypePtr)      *DataTypePtr      = SQL_WVARCHAR;
      // /* taos internal storage: sizeof(int16_t) + payload */
      if (ParameterSizePtr) *ParameterSizePtr = (bytes - 2) / 4;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    default:
      stmt_append_err_format(stmt->owner, "HY000", 0,
          "General error:#%d param:`%s[0x%x/%d]` not implemented yet", ParameterNumber, taos_data_type(type), type, type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _describe_param(stmt_base_t *base,
      SQLUSMALLINT    ParameterNumber,
      SQLSMALLINT    *DataTypePtr,
      SQLULEN        *ParameterSizePtr,
      SQLSMALLINT    *DecimalDigitsPtr,
      SQLSMALLINT    *NullablePtr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  TAOS_FIELD_E *tsdb_field = _tsdb_stmt_get_tsdb_field_by_tsdb_params(stmt, ParameterNumber - 1);
  return _tsdb_stmt_describe_param_by_field(stmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr, tsdb_field);
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  SQLSMALLINT n = stmt->params.nr_params;
  // NOTE: always return qms
  OA_NIY(stmt->params.qms == stmt->owner->current_sql.qms);
  n = stmt->params.qms;
  if (ParameterCountPtr) *ParameterCountPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  TAOS_FIELD_E *p = _tsdb_stmt_get_tsdb_field_by_tsdb_params(stmt, i_param);
  if (p == NULL) {
    stmt_append_err_format(stmt->owner, "HY000", 0,
        "General error:Parameter[%d] out of range",
        i_param + 1);
    return SQL_ERROR;
  }
  *field = p;
  return SQL_SUCCESS;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t           *res          = &stmt->res;

  if (row_count_ptr) *row_count_ptr = res->affected_row_count;
  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t        *res        = &stmt->res;
  tsdb_fields_t     *fields     = &res->fields;

  *ColumnCountPtr = (SQLSMALLINT)fields->nr;

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  int r = 0;

  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t          *res         = &stmt->res;
  tsdb_fields_t       *fields      = &res->fields;
  tsdb_rows_block_t   *rows_block  = &res->rows_block;

  int          i_row      = (int)rows_block->pos - 1;
  int          i_col      = Col_or_Param_Num - 1;
  TAOS_ROW     rows       = rows_block->rows;

  char buf[4096];
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    WS_RES   *ws_res        = (WS_RES*)res->res;
    WS_FIELD *ws_fields     = (WS_FIELD*)fields->fields;
    int result_precision    = res->time_precision;

    uint8_t     col_type = 0;
    const void *col_data = NULL;
    uint32_t    col_len  = 0;

    col_data = CALL_ws_get_value_in_block(ws_res, i_row, i_col, &col_type, &col_len);

    const WS_FIELD *ws_field = ws_fields + i_col;

    r = helper_get_tsdb_ws(result_precision, ws_field->name, col_type, col_data, col_len, i_row, i_col, tsdb, buf, sizeof(buf));
  } else {
#endif                       /* ] */
    r = helper_get_tsdb(res->res, 1, fields->fields, res->time_precision, rows, i_row, i_col, tsdb, buf, sizeof(buf));
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", 0, "General error:%.*s", (int)strlen(buf), buf);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void tsdb_stmt_init(tsdb_stmt_t *stmt, stmt_t *owner)
{
  stmt_base_t *base = &stmt->base;

  base->prepare                 = _prepare;
  base->execute                 = _execute;
  base->get_col_fields          = _get_col_fields;
  base->fetch_row               = _fetch_row;
  base->more_results            = _more_results;
  base->describe_param          = _describe_param;
  base->get_num_params          = _get_num_params;
  base->tsdb_field_by_param     = _tsdb_field_by_param;
  base->row_count               = _row_count;
  base->get_num_cols            = _get_num_cols;
  base->get_data                = _get_data;

  stmt->owner = owner;
  stmt->params.owner = stmt;
  stmt->res.owner = stmt;
}

void tsdb_stmt_unprepare(tsdb_stmt_t *stmt)
{
  stmt->current_sql = NULL;
  _tsdb_params_reset(&stmt->params);
  stmt->prepared = 0;
  stmt->is_ext   = 0;
  stmt->is_insert_stmt = 0;
  _tsdb_binds_reset(&stmt->owner->tsdb_binds);
}

static void _tsdb_stmt_close_result(tsdb_stmt_t *stmt)
{
  tsdb_res_reset(&stmt->res);
}

void tsdb_stmt_reset(tsdb_stmt_t *stmt)
{
  if (!stmt) return;
  _tsdb_stmt_close_result(stmt);
  if (stmt->stmt) {
    int r = 0;
#ifdef HAVE_TAOSWS           /* [ */
    if (stmt->owner->conn->cfg.url) {
      CALL_ws_stmt_close((WS_STMT*)stmt->stmt);
    } else {
#endif                       /* ] */
      r = CALL_taos_stmt_close(stmt->stmt);
#ifdef HAVE_TAOSWS           /* [ */
    }
#endif                       /* ] */
    OA_NIY(r == 0);
    stmt->stmt = NULL;
  }
}

void tsdb_stmt_release(tsdb_stmt_t *stmt)
{
  if (!stmt) return;

  tsdb_stmt_reset(stmt);

  tsdb_res_release(&stmt->res);

  _tsdb_params_release(&stmt->params);

  stmt->owner = NULL;
}


SQLRETURN tsdb_stmt_query(tsdb_stmt_t *stmt, const sqlc_tsdb_t *sqlc_tsdb)
{
  SQLRETURN sr = _prepare(&stmt->base, sqlc_tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  return _execute(&stmt->base);
}

SQLRETURN tsdb_stmt_rebind_subtbl(tsdb_stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (!stmt->params.subtbl_required) {
    stmt_niy(stmt->owner);
    return SQL_ERROR;
  }

  _tsdb_params_reset_tag_fields(&stmt->params);
  _tsdb_params_reset_col_fields(&stmt->params);
  _tsdb_params_reset_params(&stmt->params);

  int e = 0;
  const char *subtbl = stmt->params.subtbl;
  int r = 0;
#ifdef HAVE_TAOSWS           /* [ */
  if (stmt->owner->conn->cfg.url) {
    r = CALL_ws_stmt_set_tbname((WS_STMT*)stmt->stmt, subtbl);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosws]%s", ws_stmt_errstr((WS_STMT*)stmt->stmt));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;
      return SQL_ERROR;
    }
  } else {
#endif                       /* ] */
    r = CALL_taos_stmt_set_tbname(stmt->stmt, subtbl);
    if (r) {
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      if (r != TSDB_CODE_APP_ERROR) return SQL_ERROR;
      return SQL_ERROR;
    }
#ifdef HAVE_TAOSWS           /* [ */
  }
#endif                       /* ] */

  sr = _tsdb_stmt_describe_tags(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _tsdb_stmt_describe_cols(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  // insert into ? ... will result in TSDB_CODE_TSC_STMT_TBNAME_ERROR
  stmt->params.subtbl_required = 1;

  return _tsdb_stmt_post_check(stmt);
}

