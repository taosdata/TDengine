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

#include "columns.h"

#include "conn.h"
#include "errs.h"
#include "log.h"
#include "tables.h"
#include "taos_helpers.h"
#include "tls.h"
#include "tsdb.h"

#include <errno.h>

void columns_args_reset(columns_args_t *args)
{
  if (!args) return;

  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
  WILD_SAFE_FREE(args->column_pattern);
}

void columns_args_release(columns_args_t *args)
{
  if (!args) return;
  columns_args_reset(args);
}

static TAOS_FIELD _fields[] = {
  {
    /* 1 */
    /* name                            */ "TABLE_CAT",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 2 */
    /* name                            */ "TABLE_SCHEM",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 3 */
    /* name                            */ "TABLE_NAME",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 4 */
    /* name                            */ "COLUMN_NAME",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 5 */
    /* name                            */ "DATA_TYPE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 6 */
    /* name                            */ "TYPE_NAME",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 7 */
    /* name                            */ "COLUMN_SIZE",
    /* type                            */ TSDB_DATA_TYPE_INT,
    /* bytes                           */ 4,
  },{
    /* 8 */
    /* name                            */ "BUFFER_LENGTH",
    /* type                            */ TSDB_DATA_TYPE_INT,
    /* bytes                           */ 4,
  },{
    /* 9 */
    /* name                            */ "DECIMAL_DIGITS",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 10 */
    /* name                            */ "NUM_PREC_RADIX",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 11 */
    /* name                            */ "NULLABLE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 12 */
    /* name                            */ "REMARKS",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 13 */
    /* name                            */ "COLUMN_DEF",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 14 */
    /* name                            */ "SQL_DATA_TYPE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 15 */
    /* name                            */ "SQL_DATETIME_SUB",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 16 */
    /* name                            */ "CHAR_OCTET_LENGTH",
    /* type                            */ TSDB_DATA_TYPE_INT,
    /* bytes                           */ 4,
  },{
    /* 17 */
    /* name                            */ "ORDINAL_POSITION",
    /* type                            */ TSDB_DATA_TYPE_INT,
    /* bytes                           */ 4,
  },{
    /* 18 */
    /* name                            */ "IS_NULLABLE",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },
};

void columns_reset(columns_t *columns)
{
  if (!columns) return;

  tsdb_stmt_reset(&columns->desc);
  mem_reset(&columns->tsdb_desc);
  tsdb_stmt_reset(&columns->query);
  mem_reset(&columns->tsdb_query);
  tables_reset(&columns->tables);

  mem_reset(&columns->column_cache);

  columns_args_reset(&columns->columns_args);

  columns->ordinal_order = 0;
}

void columns_release(columns_t *columns)
{
  if (!columns) return;
  columns_reset(columns);

  tsdb_stmt_release(&columns->desc);
  mem_release(&columns->tsdb_desc);
  tsdb_stmt_release(&columns->query);
  mem_release(&columns->tsdb_query);
  tables_release(&columns->tables);

  mem_release(&columns->column_cache);

  columns_args_release(&columns->columns_args);

  columns->owner = NULL;
}

static SQLRETURN _prepare(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  (void)sqlc_tsdb;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _get_col_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  (void)fields;
  (void)nr;
  columns_t *columns = (columns_t*)base;
  (void)columns;
  *fields = _fields;
  *nr = sizeof(_fields)/sizeof(_fields[0]);
  return SQL_SUCCESS;
}

static SQLRETURN _fetch_and_desc_next_table(columns_t *columns)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  sr = columns->tables.base.fetch_row(&columns->tables.base);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = columns->tables.base.get_data(&columns->tables.base, 1, &columns->current_catalog);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = columns->tables.base.get_data(&columns->tables.base, 2, &columns->current_schema);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = columns->tables.base.get_data(&columns->tables.base, 3, &columns->current_table);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = columns->tables.base.get_data(&columns->tables.base, 4, &columns->current_table_type);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  char sql[4096];
  int n;
  n = snprintf(sql, sizeof(sql), "desc `%.*s`.`%.*s`",
      (int)columns->current_catalog.str.len, columns->current_catalog.str.str,
      (int)columns->current_table.str.len, columns->current_table.str.str);
  if (n < 0 || (size_t)n >= sizeof(sql)) {
    stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  tsdb_stmt_reset(&columns->desc);
  tsdb_stmt_init(&columns->desc, columns->owner);
  tsdb_stmt_reset(&columns->query);
  tsdb_stmt_init(&columns->query, columns->owner);

  sqlc_tsdb_t sqlc_tsdb = {
    .sqlc          = sql,
    .sqlc_bytes    = n,
  };

  stmt_t *stmt = columns->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  mem_reset(&columns->tsdb_desc);
  r = mem_conv(&columns->tsdb_desc, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  sqlc_tsdb.tsdb        = (const char*)columns->tsdb_desc.base;
  sqlc_tsdb.tsdb_bytes  = columns->tsdb_desc.nr;

  sr = tsdb_stmt_query(&columns->desc, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) {
    stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  n = snprintf(sql, sizeof(sql), "select * from `%.*s`.`%.*s` where 1=2",
      (int)columns->current_catalog.str.len, columns->current_catalog.str.str,
      (int)columns->current_table.str.len, columns->current_table.str.str);
  if (n < 0 || (size_t)n >= sizeof(sql)) {
    stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  sqlc_tsdb.sqlc        = sql;
  sqlc_tsdb.sqlc_bytes  = n;
  sqlc_tsdb.tsdb        = NULL;
  sqlc_tsdb.tsdb_bytes  = 0;

  mem_reset(&columns->tsdb_query);
  r = mem_conv(&columns->tsdb_query, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  sqlc_tsdb.tsdb        = (const char*)columns->tsdb_query.base;
  sqlc_tsdb.tsdb_bytes  = columns->tsdb_query.nr;

  sr = tsdb_stmt_query(&columns->query, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) {
    stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row_with_tsdb(stmt_base_t *base, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  columns_t *columns = (columns_t*)base;
  stmt_t *stmt = columns->owner;

  const char *fromcode = conn_get_tsdb_charset(stmt->conn);

again:

  sr = columns->desc.base.fetch_row(&columns->desc.base);
  if (sr == SQL_NO_DATA) {
    sr = _fetch_and_desc_next_table(columns);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    goto again;
  }
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (columns->columns_args.column_pattern) {
    sr = columns->desc.base.get_data(&columns->desc.base, 1, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    string_t str = {
      .charset              = fromcode,
      .str                  = tsdb->str.str,
      .bytes                = tsdb->str.len,
    };
    int matched = 0;
    r = wildexec(columns->columns_args.column_pattern, &str, &matched);
    if (r) {
      stmt_append_err(columns->owner, "HY000", 0, "General error:wild matching failed");
      return SQL_ERROR;
    }
    if (!matched) goto again;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  columns_t *columns = (columns_t*)base;

  tsdb_data_t tsdb = {0};

  sr = _fetch_row_with_tsdb(base, &tsdb);
  if (sr == SQL_SUCCESS) {
    ++columns->ordinal_order;
  }

  return sr;
}

static SQLRETURN _more_results(stmt_base_t *base)
{
  (void)base;
  return SQL_NO_DATA;
}

static SQLRETURN _describe_param(stmt_base_t *base,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  (void)ParameterNumber;
  (void)DataTypePtr;
  (void)ParameterSizePtr;
  (void)DecimalDigitsPtr;
  (void)NullablePtr;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  columns_t *columns = (columns_t*)base;
  (void)columns;

  if (row_count_ptr) *row_count_ptr = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)base;

  size_t nr = (sizeof(_fields) / sizeof(_fields[0]));
  *ColumnCountPtr = (SQLSMALLINT)nr;

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;

  columns_t *columns = (columns_t*)base;
  tsdb_data_t *col_name        = &columns->current_col_name;
  tsdb_data_t *col_type        = &columns->current_col_type;
  tsdb_data_t *col_length      = &columns->current_col_length;
  tsdb_data_t *col_note        = &columns->current_col_note;

  sr = columns->desc.base.get_data(&columns->desc.base, 1, col_name);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_name->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(columns->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_name->type));
    return SQL_ERROR;
  }
  sr = columns->desc.base.get_data(&columns->desc.base, 2, col_type);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_type->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(columns->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_type->type));
    return SQL_ERROR;
  }
  sr = columns->desc.base.get_data(&columns->desc.base, 3, col_length);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_length->type != TSDB_DATA_TYPE_INT) {
    stmt_append_err_format(columns->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_length->type));
    return SQL_ERROR;
  }
  sr = columns->desc.base.get_data(&columns->desc.base, 4, col_note);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_note->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(columns->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_note->type));
    return SQL_ERROR;
  }

  TAOS_FIELD fake = {0};
  int n = snprintf(fake.name, sizeof(fake.name), "%.*s", (int)col_name->str.len, col_name->str.str);
  if (n < 0 || (size_t)n >= sizeof(fake.name)) {
    stmt_append_err_format(columns->owner, "HY000", 0, "General error:buffer too small to hold `%.*s`", (int)col_name->str.len, col_name->str.str);
    return SQL_ERROR;
  }

  static const struct {
    const char *s;
    int         tsdb_type;
    int         bytes;
  } supported[] = {
    {"TIMESTAMP",                 TSDB_DATA_TYPE_TIMESTAMP,           8},
    {"VARCHAR",                   TSDB_DATA_TYPE_VARCHAR,            -1},
    {"NCHAR",                     TSDB_DATA_TYPE_NCHAR,              -1},
    {"JSON",                      TSDB_DATA_TYPE_JSON,               -1},
    {"BOOL",                      TSDB_DATA_TYPE_BOOL,                1},
    {"TINYINT",                   TSDB_DATA_TYPE_TINYINT,             1},
    {"SMALLINT",                  TSDB_DATA_TYPE_SMALLINT,            2},
    {"INT",                       TSDB_DATA_TYPE_INT,                 4},
    {"BIGINT",                    TSDB_DATA_TYPE_BIGINT,              8},
    {"FLOAT",                     TSDB_DATA_TYPE_FLOAT,               4},
    {"DOUBLE",                    TSDB_DATA_TYPE_DOUBLE,              8},
    {"TINYINT UNSIGNED",          TSDB_DATA_TYPE_UTINYINT,            1},
    {"SMALLINT UNSIGNED",         TSDB_DATA_TYPE_USMALLINT,           2},
    {"INT UNSIGNED",              TSDB_DATA_TYPE_UINT,                4},
    {"BIGINT UNSIGNED",           TSDB_DATA_TYPE_UBIGINT,             8},
    {"VARBINARY",                 TSDB_DATA_TYPE_VARBINARY,          -1},
    {"GEOMETRY",                  TSDB_DATA_TYPE_GEOMETRY,           -1},
  };

  for (size_t i=0; i<=sizeof(supported)/sizeof(supported[0]); ++i) {
    if (i == sizeof(supported)/sizeof(supported[0])) {
      stmt_append_err_format(columns->owner, "HY000", 0, "General error:not implemented yet for column[%d] `%.*s`",
          Col_or_Param_Num, (int)col_type->str.len, col_type->str.str);
      return SQL_ERROR;
    }
    const char *s = supported[i].s;
    size_t len = strlen(s);
    int tsdb_type = supported[i].tsdb_type;
    int bytes = supported[i].bytes;
    if (col_type->str.len != len) continue;
    if (strncmp(col_type->str.str, s, len)) continue;
    fake.type = tsdb_type;
    fake.bytes = col_length->i32;
    if (bytes != -1 && fake.bytes != bytes) {
      stmt_append_err_format(columns->owner, "HY000", 0, "General error:internal logic error for Column[%d] `%.*s`, ColumnSize is expected %d, but got ==%d==",
          Col_or_Param_Num, (int)col_type->str.len, col_type->str.str, bytes, fake.bytes);
      return SQL_ERROR;
    }
    break;
  }

  tsdb->is_null = 0;

  switch (Col_or_Param_Num) {
    case 1: // TABLE_CAT
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = columns->current_catalog.str;
      break;
    case 2: // TABLE_SCHEM
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = columns->current_schema.str;
      break;
    case 3: // TABLE_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = columns->current_table.str;
      break;
    case 4: // COLUMN_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_name->str;
      break;
    case 5: // DATA_TYPE
      {
        static const struct {
          int               tsdb_type;
          int16_t           sql_type;
        } supported[] = {
          {TSDB_DATA_TYPE_TIMESTAMP,          SQL_TYPE_TIMESTAMP},
          {TSDB_DATA_TYPE_VARCHAR,            SQL_VARCHAR},
          {TSDB_DATA_TYPE_NCHAR,              SQL_WVARCHAR},
          {TSDB_DATA_TYPE_JSON,               SQL_WVARCHAR},
          {TSDB_DATA_TYPE_BOOL,               SQL_BIT},
          {TSDB_DATA_TYPE_TINYINT,            SQL_TINYINT},
          {TSDB_DATA_TYPE_SMALLINT,           SQL_SMALLINT},
          {TSDB_DATA_TYPE_INT,                SQL_INTEGER},
          {TSDB_DATA_TYPE_BIGINT,             SQL_BIGINT},
          {TSDB_DATA_TYPE_FLOAT,              SQL_REAL},
          {TSDB_DATA_TYPE_DOUBLE,             SQL_DOUBLE},
          {TSDB_DATA_TYPE_UTINYINT,           SQL_TINYINT},
          {TSDB_DATA_TYPE_USMALLINT,          SQL_SMALLINT},
          {TSDB_DATA_TYPE_UINT,               SQL_INTEGER},
          {TSDB_DATA_TYPE_UBIGINT,            SQL_BIGINT},
          {TSDB_DATA_TYPE_VARBINARY,          SQL_VARBINARY},
          {TSDB_DATA_TYPE_GEOMETRY,           SQL_VARBINARY},
        };

        for (size_t i=0; i<=sizeof(supported)/sizeof(supported[0]); ++i) {
          if (i == sizeof(supported)/sizeof(supported[0])) {
            stmt_append_err_format(columns->owner, "HY000", 0, "General error:not implemented yet for column[%d] `%.*s`",
                Col_or_Param_Num, (int)col_type->str.len, col_type->str.str);
            return SQL_ERROR;
          }
          int tsdb_type = supported[i].tsdb_type;
          int16_t sql_type = supported[i].sql_type;
          if (fake.type != tsdb_type) continue;
          if (fake.type == TSDB_DATA_TYPE_TIMESTAMP) {
            if (!columns->owner->conn->cfg.timestamp_as_is) {
              tsdb->type = TSDB_DATA_TYPE_SMALLINT;
              tsdb->i16 = SQL_WVARCHAR;
              break;
            }
          }
          tsdb->type = TSDB_DATA_TYPE_SMALLINT;
          tsdb->i16 = sql_type;
          break;
        }
        break;
      }
    case 6: // TYPE_NAME
      // better approach?
      if (fake.type== TSDB_DATA_TYPE_TIMESTAMP) {
        if (!columns->owner->conn->cfg.timestamp_as_is) {
          tsdb->type = TSDB_DATA_TYPE_VARCHAR;
          tsdb->str.str = "NCHAR";
          tsdb->str.len = 5;
          break;
        }
      }
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_type->str;
      break;
    case 7: // COLUMN_SIZE
      // better approach?
      if (fake.type== TSDB_DATA_TYPE_TIMESTAMP) {
        if (!columns->owner->conn->cfg.timestamp_as_is) {
          int time_precision = columns->query.res.time_precision;
          int precision = 20 + (time_precision + 1) * 3;
          tsdb->type = TSDB_DATA_TYPE_INT;
          tsdb->i32  = precision;
          return SQL_SUCCESS;
        }
      }
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = col_length->i32;
      break;
    case 8: // BUFFER_LENGTH
      {
        static const struct {
          int               tsdb_type;
          int               len;
        } supported[] = {
          {TSDB_DATA_TYPE_TIMESTAMP,          8},
          {TSDB_DATA_TYPE_VARCHAR,           -1},
          {TSDB_DATA_TYPE_NCHAR,             -1},
          {TSDB_DATA_TYPE_JSON,              -1},
          {TSDB_DATA_TYPE_BOOL,               1},
          {TSDB_DATA_TYPE_TINYINT,            1},
          {TSDB_DATA_TYPE_SMALLINT,           2},
          {TSDB_DATA_TYPE_INT,                4},
          {TSDB_DATA_TYPE_BIGINT,             8},
          {TSDB_DATA_TYPE_FLOAT,              4},
          {TSDB_DATA_TYPE_DOUBLE,             8},
          {TSDB_DATA_TYPE_UTINYINT,           1},
          {TSDB_DATA_TYPE_USMALLINT,          2},
          {TSDB_DATA_TYPE_UINT,               4},
          {TSDB_DATA_TYPE_UBIGINT,            8},
          {TSDB_DATA_TYPE_VARBINARY,         -1},
          {TSDB_DATA_TYPE_GEOMETRY,          -1},
        };

        for (size_t i=0; i<=sizeof(supported)/sizeof(supported[0]); ++i) {
          if (i == sizeof(supported)/sizeof(supported[0])) {
            stmt_append_err_format(columns->owner, "HY000", 0, "General error:not implemented yet for column[%d] `%.*s`",
                Col_or_Param_Num, (int)col_type->str.len, col_type->str.str);
            return SQL_ERROR;
          }
          int tsdb_type = supported[i].tsdb_type;
          int len = supported[i].len;
          if (fake.type != tsdb_type) continue;
          tsdb->type = TSDB_DATA_TYPE_INT;
          tsdb->i32 = len == -1 ? fake.bytes : len;
          break;
        }
        break;
      }
    case 9: // DECIMAL_DIGITS
      if (fake.type == TSDB_DATA_TYPE_TIMESTAMP) {
        tsdb->type = TSDB_DATA_TYPE_INT;
        tsdb->i32  = 3; // FIXME:
        break;
      }
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = 0;
      break;
    case 10: // NUM_PREC_RADIX
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = 10;
      break;
    case 11: // NULLABLE
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = SQL_NULLABLE_UNKNOWN;
      break;
    case 12: // REMARKS
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_note->str;
      tsdb->is_null = col_note->is_null;
      break;
    case 13: // COLUMN_DEF
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str.len = 0;
      tsdb->str.str = "";
      tsdb->is_null = 1;
      break;
    case 14: // SQL_DATA_TYPE
      {
        static const struct {
          int               tsdb_type;
          int               sql_type; // non-concise-type
        } supported[] = {
          {TSDB_DATA_TYPE_TIMESTAMP,          SQL_TIMESTAMP},
          {TSDB_DATA_TYPE_VARCHAR,            SQL_VARCHAR},
          {TSDB_DATA_TYPE_NCHAR,              SQL_WVARCHAR},
          {TSDB_DATA_TYPE_JSON,               SQL_WVARCHAR},
          {TSDB_DATA_TYPE_BOOL,               SQL_BIT},
          {TSDB_DATA_TYPE_TINYINT,            SQL_TINYINT},
          {TSDB_DATA_TYPE_SMALLINT,           SQL_SMALLINT},
          {TSDB_DATA_TYPE_INT,                SQL_INTEGER},
          {TSDB_DATA_TYPE_BIGINT,             SQL_BIGINT},
          {TSDB_DATA_TYPE_FLOAT,              SQL_REAL},
          {TSDB_DATA_TYPE_DOUBLE,             SQL_DOUBLE},
          {TSDB_DATA_TYPE_UTINYINT,           SQL_TINYINT},
          {TSDB_DATA_TYPE_USMALLINT,          SQL_SMALLINT},
          {TSDB_DATA_TYPE_UINT,               SQL_INTEGER},
          {TSDB_DATA_TYPE_UBIGINT,            SQL_BIGINT},
          {TSDB_DATA_TYPE_VARBINARY,          SQL_VARBINARY},
          {TSDB_DATA_TYPE_GEOMETRY,           SQL_VARBINARY},
        };

        for (size_t i=0; i<=sizeof(supported)/sizeof(supported[0]); ++i) {
          if (i == sizeof(supported)/sizeof(supported[0])) {
            stmt_append_err_format(columns->owner, "HY000", 0, "General error:not implemented yet for column[%d] `%.*s`",
                Col_or_Param_Num, (int)col_type->str.len, col_type->str.str);
            return SQL_ERROR;
          }
          int tsdb_type = supported[i].tsdb_type;
          int16_t sql_type = supported[i].sql_type;
          if (fake.type != tsdb_type) continue;
          tsdb->type = TSDB_DATA_TYPE_INT;
          tsdb->i32 = sql_type;
          break;
        }
        break;
      }
    case 15: // SQL_DATETIME_SUB
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = 0;
      tsdb->is_null = 1;
      break;
    case 16: // CHAR_OCTET_LENGTH
      if (fake.type == TSDB_DATA_TYPE_VARCHAR) {
        tsdb->type = TSDB_DATA_TYPE_INT;
        tsdb->i32  = fake.bytes;
      } else {
        tsdb->type = TSDB_DATA_TYPE_INT;
        tsdb->i32  = 0;
        tsdb->is_null = 1;
      }
      break;
    case 17: // ORDINAL_POSITION
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = columns->ordinal_order;
      break;
    case 18: // IS_NULLABLE
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str.str = "";
      tsdb->str.len = 0;
      break;
    default:
      stmt_append_err_format(columns->owner, "HY000", 0, "General error:not implemented yet for column[%d]", Col_or_Param_Num);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void columns_init(columns_t *columns, stmt_t *stmt)
{
  columns->owner = stmt;
  tables_init(&columns->tables, stmt);
  tsdb_stmt_init(&columns->desc, stmt);

  stmt_base_t *base = &columns->base;

  base->prepare                      = _prepare;
  base->execute                      = _execute;
  base->get_col_fields               = _get_col_fields;
  base->fetch_row                    = _fetch_row;
  base->more_results                 = _more_results;
  base->describe_param               = _describe_param;
  base->get_num_params               = _get_num_params;
  base->tsdb_field_by_param          = _tsdb_field_by_param;
  base->row_count                    = _row_count;
  base->get_num_cols                 = _get_num_cols;
  base->get_data                     = _get_data;
}

SQLRETURN columns_open(
    columns_t     *columns,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  columns_reset(columns);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS)  NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS)   NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);
  if (ColumnName && NameLength4 == SQL_NTS)  NameLength4 = (SQLSMALLINT)strlen((const char*)ColumnName);

  stmt_t *stmt = columns->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = "UCS-2LE";
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  if (ColumnName) {
    if (mem_conv(&columns->column_cache, cnv->cnv, (const char*)ColumnName, NameLength4)) {
      stmt_oom(columns->owner);
      return SQL_ERROR;
    }
    string_t str = {
      .charset              = cnv->from,
      .str                  = (const char*)ColumnName,
      .bytes                = NameLength4,
    };
    if (wildcomp(&columns->columns_args.column_pattern, &str)) {
      stmt_append_err_format(columns->owner, "HY000", 0,
          "General error:wild compile failed for ColumnName[%.*s]", (int)NameLength4, (const char*)ColumnName);
      return SQL_ERROR;
    }
  }

  sr = tables_open(&columns->tables, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, (SQLCHAR*)"TABLE,VIEW", SQL_NTS);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return _fetch_and_desc_next_table(columns);
}
