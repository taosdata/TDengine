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
  void *raw_block = NULL;
  r = CALL_taos_fetch_raw_block(res, &rows_in_block, &raw_block);
  if (r == 0) {
    ds_block_t *ds_block = &ds_res->block;
    ds_block->nr_rows_in_block = rows_in_block;
    ds_block->block            = raw_block;
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
  const void   *block  = ds_block->block;
  int result_precision = ds_res->result_precision;

  (void)res;
  (void)block_mode;
  return helper_get_tsdb_from_raw_block(block, ds_block->nr_rows_in_block, fields, result_precision, i_row, i_col, tsdb, ds_err->str, sizeof(ds_err->str));
}

static void _ds_res_setup(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

  ds_res->fields.ds_res  = ds_res;
  _ds_fields_setup(&ds_res->fields);

  ds_res->block.ds_res = ds_res;
  _ds_block_setup(&ds_res->block);

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

  ds_block->get_into_tsdb   = _ds_block_tsdb_get_into_tsdb;
  return;
}

int ds_block_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, ds_err_t *ds_err)
{
  OA_NIY(ds_block->get_into_tsdb);
  return ds_block->get_into_tsdb(ds_block, i_row, i_col, tsdb, ds_err);
}

