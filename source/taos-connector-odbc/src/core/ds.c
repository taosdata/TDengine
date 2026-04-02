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

#include "ds.h"
#include "log.h"

static void _ds_stmt_setup(ds_stmt_t *ds_stmt);
static void _ds_res_setup(ds_res_t *ds_res);
static void _ds_block_setup(ds_block_t *ds_block);
static void _ds_fields_setup(ds_fields_t *ds_fields);

#ifdef HAVE_TAOSWS           /* { */
static int _ds_ws_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);

  WS_TAOS *ws_taos = (WS_TAOS*)ds_conn->taos;
  WS_RES  *res     = CALL_ws_query(ws_taos, sql);

  ds_res->res = res;
  if (ws_errno(res)) return -1;
  if (!res) return 0;
  ds_res->result_precision = CALL_ws_result_precision(res);
  if (ws_errno(res)) return -1;
  ds_res->fields.nr_fields = CALL_ws_field_count(res);
  if (ws_errno(res)) return -1;
  ds_res->fields.fields = CALL_ws_fetch_fields(res);

  return 0;
}

static const char* _ds_ws_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return CALL_ws_get_server_info((WS_TAOS*)ds_conn->taos);
}

static const char* _ds_ws_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return CALL_ws_get_client_info();
}

static int _ds_ws_get_current_db_with_ds_res(ds_conn_t *ds_conn, char *db, size_t len, ds_res_t *ds_res, ds_err_t *ds_err)
{
  int r = 0;

  const char *sql = "select database()";
  r = ds_conn_query(ds_conn, sql, ds_res);
  if (r) {
    ds_err->err        = ds_res_errno(ds_res);
    snprintf(ds_err->str, sizeof(ds_err->str), "[taosws]:%s", ds_res_errstr(ds_res));
    return -1;
  }

  r = ds_res_fetch_block(ds_res);
  if (r) {
    ds_err->err        = ds_res_errno(ds_res);
    snprintf(ds_err->str, sizeof(ds_err->str), "[taosws]:%s", ds_res_errstr(ds_res));
    return -1;
  }

  tsdb_data_t tsdb_data = {0};
  r = ds_block_get_into_tsdb(&ds_res->block, 0, 0, &tsdb_data, ds_err);
  if (r) return -1;

  if (tsdb_data.is_null) {
    db[0] = '\0';
    return 0;
  }

  if (tsdb_data.type != TSDB_DATA_TYPE_VARCHAR) {
    ds_err->err        = 0;
    snprintf(ds_err->str, sizeof(ds_err->str), "expecting TSDB_DATA_TYPE_VARCHAR, but got ==%s==", taos_data_type(tsdb_data.type));
    return -1;
  }

  snprintf(db, len, "%.*s", (int)tsdb_data.str.len, tsdb_data.str.str);
  return 0;
}

static int _ds_ws_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, ds_err_t *ds_err)
{
  int r = 0;

  OA_NIY(ds_conn->conn);

  ds_res_t ds_res = {0};
  r = _ds_ws_get_current_db_with_ds_res(ds_conn, db, len, &ds_res, ds_err);
  ds_res_close(&ds_res);

  return r ? -1 : 0;
}

static void _ds_ws_close(ds_conn_t *ds_conn)
{
  if (!ds_conn->conn || !ds_conn->taos) return;

  CALL_ws_close((WS_RES*)ds_conn->taos);
  ds_conn->taos = NULL;
}

static int _ds_ws_stmt_init(ds_conn_t *ds_conn, ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_conn->conn);

  WS_TAOS *ws_taos = (WS_TAOS*)ds_conn->taos;
  WS_STMT *ws_stmt = CALL_ws_stmt_init(ws_taos);
  if (!ws_stmt) return -1;

  ds_stmt->stmt = ws_stmt;
  return 0;
}

static void _ds_stmt_ws_close(ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_stmt->ds_conn);

  WS_STMT *ws_stmt = (WS_STMT*)ds_stmt->stmt;
  if (!ws_stmt) return;

  CALL_ws_stmt_close(ws_stmt);
  ds_stmt->stmt = NULL;
}

static int _ds_stmt_ws_prepare(ds_stmt_t *ds_stmt, const char *sql)
{
  OA_NIY(ds_stmt->ds_conn);
  OA_NIY(ds_stmt->stmt);

  WS_STMT *ws_stmt = (WS_STMT*)ds_stmt->stmt;

  int r = CALL_ws_stmt_prepare(ws_stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  return 0;
}
#endif                       /* } */

static int _ds_tsdb_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);

  TAOS *taos    = (TAOS*)ds_conn->taos;
  TAOS_RES *res = CALL_taos_query(taos, sql);

  ds_res->res = res;
  if (taos_errno(res)) return -1;
  if (!res) return -1;
  ds_res->result_precision = CALL_taos_result_precision(res);
  if (taos_errno(res)) return -1;
  ds_res->fields.nr_fields = CALL_taos_field_count(res);
  if (taos_errno(res)) return -1;
  ds_res->fields.fields = CALL_taos_fetch_fields(res);

  return 0;
}

static const char* _ds_tsdb_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return CALL_taos_get_server_info((TAOS*)ds_conn->taos);
}

static const char* _ds_tsdb_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return CALL_taos_get_client_info();
}

static int _ds_tsdb_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, ds_err_t *ds_err)
{
  int r = 0;

  OA_NIY(ds_conn->conn);

  int required = 0;
  r = CALL_taos_get_current_db((TAOS*)ds_conn->taos, db, (int)len, &required);
  if (r) {
    ds_err->err = taos_errno(NULL);
    snprintf(ds_err->str, sizeof(ds_err->str), "%s", taos_errstr(NULL));
    return -1;
  }

  return 0;
}

static void _ds_tsdb_close(ds_conn_t *ds_conn)
{
  if (!ds_conn->conn || !ds_conn->taos) return;
  CALL_taos_close((TAOS*)ds_conn->taos);
  ds_conn->taos = NULL;
}

static int _ds_tsdb_stmt_init(ds_conn_t *ds_conn, ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_conn->conn);

  TAOS      *taos = (TAOS*)ds_conn->taos;
  TAOS_STMT *stmt = taos_stmt_init(taos);
  if (!stmt) return -1;

  ds_stmt->stmt = stmt;
  return 0;
}

static void _ds_stmt_tsdb_close(ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_stmt->ds_conn);

  TAOS_STMT *stmt = (TAOS_STMT*)ds_stmt->stmt;
  if (!stmt) return;

  taos_stmt_close(stmt);
  ds_stmt->stmt = NULL;
}

static int _ds_stmt_tsdb_prepare(ds_stmt_t *ds_stmt, const char *sql)
{
  OA_NIY(ds_stmt->ds_conn);
  OA_NIY(ds_stmt->stmt);

  TAOS_STMT *stmt = (TAOS_STMT*)ds_stmt->stmt;

  int r = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  return 0;
}

void ds_conn_setup(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_conn->conn->cfg.url) {
    ds_conn->query               = _ds_ws_query;
    ds_conn->get_server_info     = _ds_ws_get_server_info;
    ds_conn->get_client_info     = _ds_ws_get_client_info;
    ds_conn->get_current_db      = _ds_ws_get_current_db;
    ds_conn->close               = _ds_ws_close;

    ds_conn->stmt_init           = _ds_ws_stmt_init;
    return;
  }
#endif                       /* } */

  ds_conn->query               = _ds_tsdb_query;
  ds_conn->get_server_info     = _ds_tsdb_get_server_info;
  ds_conn->get_client_info     = _ds_tsdb_get_client_info;
  ds_conn->get_current_db      = _ds_tsdb_get_current_db;
  ds_conn->close               = _ds_tsdb_close;

  ds_conn->stmt_init           = _ds_tsdb_stmt_init;
  return;
}

int ds_conn_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);
  OA_NIY(ds_conn->query);

  ds_res->ds_conn = ds_conn;
  _ds_res_setup(ds_res);

  return ds_conn->query(ds_conn, sql, ds_res);
}

const char* ds_conn_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);
  OA_NIY(ds_conn->get_server_info);

  return ds_conn->get_server_info(ds_conn);
}

const char* ds_conn_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);
  OA_NIY(ds_conn->get_client_info);

  return ds_conn->get_client_info(ds_conn);
}

int ds_conn_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, ds_err_t *ds_err)
{
  OA_NIY(ds_conn->conn);
  OA_NIY(ds_conn->get_current_db);

  return ds_conn->get_current_db(ds_conn, db, len, ds_err);
}

void ds_conn_close(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->close);

  ds_conn->close(ds_conn);
}

int ds_conn_stmt_init(ds_conn_t *ds_conn, ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_stmt->ds_conn == ds_conn);
  OA_NIY(ds_conn->stmt_init);

  _ds_stmt_setup(ds_stmt);

  return ds_conn->stmt_init(ds_conn, ds_stmt);
}

static void _ds_stmt_setup(ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_stmt->ds_conn);
  OA_NIY(ds_stmt->ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  conn_t *conn = ds_stmt->ds_conn->conn;

  if (conn->cfg.url) {
    ds_stmt->close             = _ds_stmt_ws_close;
    ds_stmt->prepare           = _ds_stmt_ws_prepare;
    return;
  }
#endif                       /* } */

  ds_stmt->close             = _ds_stmt_tsdb_close;
  ds_stmt->prepare           = _ds_stmt_tsdb_prepare;
  return;
}

void ds_stmt_close(ds_stmt_t *ds_stmt)
{
  OA_NIY(ds_stmt->close);

  ds_stmt->close(ds_stmt);
}

int ds_stmt_prepare(ds_stmt_t *ds_stmt, const char *sql)
{
  OA_NIY(ds_stmt->prepare);

  return ds_stmt->prepare(ds_stmt, sql);
}

#ifdef HAVE_TAOSWS           /* { */
static void _ds_res_ws_close(ds_res_t *ds_res)
{
  WS_RES *res = (WS_RES*)ds_res->res;

  CALL_ws_free_result(res);
  ds_res->res = NULL;
}

static int _ds_res_ws_errno(ds_res_t *ds_res)
{
  WS_RES *res = (WS_RES*)ds_res->res;
  return ws_errno(res);
}

static const char* _ds_res_ws_errstr(ds_res_t *ds_res)
{
  WS_RES *res = (WS_RES*)ds_res->res;
  return ws_errstr(res);
}

static int _ds_res_ws_fetch_block(ds_res_t *ds_res)
{
  int r = 0;

  WS_RES *res = (WS_RES*)ds_res->res;

  const void *block = NULL;
  int32_t rows_in_block = 0;
  r = CALL_ws_fetch_raw_block(res, &block, &rows_in_block);
  if (r == 0) {
    ds_block_t *ds_block = &ds_res->block;
    ds_block->nr_rows_in_block = rows_in_block;
    ds_block->block            = block;
  }

  return r;
}

static int8_t _ds_fields_ws_field_type(ds_fields_t *ds_fields, int i_col)
{
  const WS_FIELD *fields = (const WS_FIELD*)ds_fields->fields;
  return (int8_t)fields[i_col].type;
}

static int _ds_block_ws_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, ds_err_t *ds_err)
{
  ds_res_t *ds_res = ds_block->ds_res;

  WS_RES   *res        = (WS_RES*)ds_res->res;
  WS_FIELD *fields     = (WS_FIELD*)ds_res->fields.fields;
  int result_precision = ds_res->result_precision;

  uint8_t     col_type = 0;
  const void *col_data = NULL;
  uint32_t    col_len  = 0;

  col_data = CALL_ws_get_value_in_block(res, i_row, i_col, &col_type, &col_len);

  const WS_FIELD *field = fields + i_col;

  return helper_get_tsdb_ws(result_precision, field->name, col_type, col_data, col_len, i_row, i_col, tsdb, ds_err->str, sizeof(ds_err->str));
}

#endif                       /* } */

static void _ds_res_tsdb_close(ds_res_t *ds_res)
{
  TAOS_RES *res = (TAOS_RES*)ds_res->res;
  CALL_taos_free_result(res);
  ds_res->res = NULL;
}

static int _ds_res_tsdb_errno(ds_res_t *ds_res)
{
  TAOS_RES *res = (TAOS_RES*)ds_res->res;
  return taos_errno(res);
}

static const char* _ds_res_tsdb_errstr(ds_res_t *ds_res)
{
  TAOS_RES *res = (TAOS_RES*)ds_res->res;
  return taos_errstr(res);
}

static int _ds_res_tsdb_fetch_block(ds_res_t *ds_res)
{
  int r = 0;

  TAOS_RES *res = (TAOS_RES*)ds_res->res;

  int rows_in_block = 0;
  TAOS_ROW rows = NULL;
  r = CALL_taos_fetch_block_s(res, &rows_in_block, &rows);
  if (r == 0) {
    ds_block_t *ds_block = &ds_res->block;
    ds_block->nr_rows_in_block = rows_in_block;
    ds_block->block            = rows;
  }

  return r;
}

static int8_t _ds_fields_tsdb_field_type(ds_fields_t *ds_fields, int i_col)
{
  const TAOS_FIELD *fields = (const TAOS_FIELD*)ds_fields->fields;
  return fields[i_col].type;
}

static int _ds_block_tsdb_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, ds_err_t *ds_err)
{
  ds_res_t *ds_res = ds_block->ds_res;

  int block_mode = 1;
  TAOS_RES     *res    = (TAOS_RES*)ds_res->res;
  TAOS_FIELD   *fields = (TAOS_FIELD*)ds_res->fields.fields;
  TAOS_ROW      block  = (TAOS_ROW)ds_block->block;
  int result_precision = ds_res->result_precision;

  return helper_get_tsdb(res, block_mode, fields, result_precision, block, i_row, i_col, tsdb, ds_err->str, sizeof(ds_err->str));
}

static void _ds_res_setup(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

  ds_res->fields.ds_res  = ds_res;
  _ds_fields_setup(&ds_res->fields);

  ds_res->block.ds_res = ds_res;
  _ds_block_setup(&ds_res->block);

#ifdef HAVE_TAOSWS           /* { */
  conn_t *conn = ds_res->ds_conn->conn;

  if (conn->cfg.url) {
    ds_res->close             = _ds_res_ws_close;
    ds_res->xerrno            = _ds_res_ws_errno;
    ds_res->errstr            = _ds_res_ws_errstr;
    ds_res->fetch_block       = _ds_res_ws_fetch_block;
    return;
  }
#endif                       /* } */

  ds_res->close             = _ds_res_tsdb_close;
  ds_res->xerrno            = _ds_res_tsdb_errno;
  ds_res->errstr            = _ds_res_tsdb_errstr;
  ds_res->fetch_block       = _ds_res_tsdb_fetch_block;
  return;
}

void ds_res_close(ds_res_t *ds_res)
{
  if (!ds_res->res) return;
  OA_NIY(ds_res->close);

  ds_res->close(ds_res);
}

int ds_res_errno(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->xerrno);

  return ds_res->xerrno(ds_res);
}

const char* ds_res_errstr(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->errstr);

  return ds_res->errstr(ds_res);
}

int ds_res_fetch_block(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->block.ds_res == ds_res);
  OA_NIY(ds_res->fetch_block);

  return ds_res->fetch_block(ds_res);
}

static void _ds_fields_setup(ds_fields_t *ds_fields)
{
  OA_NIY(ds_fields->ds_res);

  ds_res_t *ds_res = ds_fields->ds_res;
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->ds_conn->conn->cfg.url) {
    ds_fields->field_type = _ds_fields_ws_field_type;
    return;
  }
#endif                       /* } */

  ds_fields->field_type = _ds_fields_tsdb_field_type;
  return;
}

int8_t ds_fields_field_type(ds_fields_t *ds_fields, int i_col)
{
  OA_NIY(ds_fields->ds_res);
  OA_NIY(ds_fields->field_type);

  return ds_fields->field_type(ds_fields, i_col);
}

static void _ds_block_setup(ds_block_t *ds_block)
{
  OA_NIY(ds_block->ds_res);

  ds_res_t *ds_res = ds_block->ds_res;
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->ds_conn->conn->cfg.url) {
    ds_block->get_into_tsdb   = _ds_block_ws_get_into_tsdb;
    return;
  }
#endif                       /* } */

  ds_block->get_into_tsdb   = _ds_block_tsdb_get_into_tsdb;
  return;
}

int ds_block_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, ds_err_t *ds_err)
{
  OA_NIY(ds_block->get_into_tsdb);
  return ds_block->get_into_tsdb(ds_block, i_row, i_col, tsdb, ds_err);
}

