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

#include "primarykeys.h"
#include "tables.h"

#include "conn.h"
#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tls.h"
#include "tsdb.h"

#include <errno.h>

void primarykeys_args_reset(primarykeys_args_t *args)
{
  if (!args) return;

  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
}

void primarykeys_args_release(primarykeys_args_t *args)
{
  if (!args) return;
  primarykeys_args_reset(args);
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
    /* name                            */ "KEY_SEQ",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 6 */
    /* name                            */ "PK_NAME",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },
};

void primarykeys_reset(primarykeys_t *primarykeys)
{
  if (!primarykeys) return;

  tsdb_stmt_reset(&primarykeys->desc);
  mem_reset(&primarykeys->tsdb_desc);
  tables_reset(&primarykeys->tables);

  primarykeys_args_reset(&primarykeys->primarykeys_args);

  primarykeys->ordinal_order = 0;
}

void primarykeys_release(primarykeys_t *primarykeys)
{
  if (!primarykeys) return;
  primarykeys_reset(primarykeys);

  tsdb_stmt_release(&primarykeys->desc);
  mem_release(&primarykeys->tsdb_desc);
  tables_release(&primarykeys->tables);

  primarykeys_args_release(&primarykeys->primarykeys_args);

  primarykeys->owner = NULL;
}

static SQLRETURN _prepare(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  (void)sqlc_tsdb;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _get_col_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  (void)fields;
  (void)nr;
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  *fields = _fields;
  *nr = sizeof(_fields)/sizeof(_fields[0]);
  return SQL_SUCCESS;
}

static SQLRETURN _fetch_and_desc_next_table(primarykeys_t *primarykeys)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  sr = primarykeys->tables.base.fetch_row(&primarykeys->tables.base);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = primarykeys->tables.base.get_data(&primarykeys->tables.base, 1, &primarykeys->current_catalog);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = primarykeys->tables.base.get_data(&primarykeys->tables.base, 2, &primarykeys->current_schema);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = primarykeys->tables.base.get_data(&primarykeys->tables.base, 3, &primarykeys->current_table);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  char sql[4096];
  int n = snprintf(sql, sizeof(sql), "desc `%.*s`.`%.*s`",
      (int)primarykeys->current_catalog.str.len, primarykeys->current_catalog.str.str,
      (int)primarykeys->current_table.str.len, primarykeys->current_table.str.str);
  if (n < 0 || (size_t)n >= sizeof(sql)) {
    stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  sqlc_tsdb_t sqlc_tsdb = {
    .sqlc          = sql,
    .sqlc_bytes    = n,
  };

  stmt_t *stmt = primarykeys->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  mem_reset(&primarykeys->tsdb_desc);
  r = mem_conv(&primarykeys->tsdb_desc, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  tsdb_stmt_reset(&primarykeys->desc);
  tsdb_stmt_init(&primarykeys->desc, primarykeys->owner);

  sqlc_tsdb.tsdb          = (const char*)primarykeys->tsdb_desc.base;
  sqlc_tsdb.tsdb_bytes    = primarykeys->tsdb_desc.nr;

  sr = tsdb_stmt_query(&primarykeys->desc, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) {
    stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row_with_tsdb(stmt_base_t *base, tsdb_data_t *tsdb)
{
  (void)tsdb;

  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_t *primarykeys = (primarykeys_t*)base;

again:

  sr = primarykeys->desc.base.fetch_row(&primarykeys->desc.base);
  if (sr == SQL_NO_DATA) {
    sr = _fetch_and_desc_next_table(primarykeys);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    goto again;
  }

  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_t *primarykeys = (primarykeys_t*)base;

  tsdb_data_t tsdb = {0};

  if (primarykeys->ordinal_order == 1) return SQL_NO_DATA;

  sr = _fetch_row_with_tsdb(base, &tsdb);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tsdb_data_t *col_type        = &primarykeys->current_col_type;

  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 2, col_type);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_type->is_null) {
    stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }
  if (col_type->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_type->type));
    return SQL_ERROR;
  }

  ++primarykeys->ordinal_order;

  if (col_type->str.len != 9 || strncmp(col_type->str.str, "TIMESTAMP", 9)) return SQL_NO_DATA;

  return SQL_SUCCESS;
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

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;

  if (row_count_ptr) *row_count_ptr = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)base;

  size_t nr = sizeof(_fields) / sizeof(_fields[0]);

  *ColumnCountPtr = (SQLSMALLINT)nr;

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  tsdb_data_t *col_name        = &primarykeys->current_col_name;
  tsdb_data_t *col_type        = &primarykeys->current_col_type;
  tsdb_data_t *col_length      = &primarykeys->current_col_length;
  tsdb_data_t *col_note        = &primarykeys->current_col_note;

  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 1, col_name);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_name->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_name->type));
    return SQL_ERROR;
  }
  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 2, col_type);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_type->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_type->type));
    return SQL_ERROR;
  }
  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 3, col_length);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_length->type != TSDB_DATA_TYPE_INT) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_length->type));
    return SQL_ERROR;
  }
  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 4, col_note);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_note->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_note->type));
    return SQL_ERROR;
  }

  TAOS_FIELD fake = {0};
  int n = snprintf(fake.name, sizeof(fake.name), "%.*s", (int)col_name->str.len, col_name->str.str);
  if (n < 0 || (size_t)n >= sizeof(fake.name)) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:buffer too small to hold `%.*s`", (int)col_name->str.len, col_name->str.str);
    return SQL_ERROR;
  }

  if (col_type->str.len == 9 && strncmp(col_type->str.str, "TIMESTAMP", 9) == 0) {
    fake.type = TSDB_DATA_TYPE_TIMESTAMP;
    fake.bytes = col_length->i32;
    if (fake.bytes != 8) {
      stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
      return SQL_ERROR;
    }
  } else if (col_type->str.len == 3 && strncmp(col_type->str.str, "INT", 3) == 0) {
    fake.type = TSDB_DATA_TYPE_INT;
    fake.bytes = col_length->i32;
    if (fake.bytes != 4) {
      stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
      return SQL_ERROR;
    }
  } else if (col_type->str.len == 7 && strncmp(col_type->str.str, "VARCHAR", 7) == 0) {
    fake.type = TSDB_DATA_TYPE_VARCHAR;
    fake.bytes = col_length->i32;
  } else if (col_type->str.len == 5 && strncmp(col_type->str.str, "NCHAR", 5) == 0) {
    fake.type = TSDB_DATA_TYPE_VARCHAR;
    fake.bytes = col_length->i32;
  } else {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:not implemented yet for column[%d] `%.*s`",
        Col_or_Param_Num, (int)col_type->str.len, col_type->str.str);
    return SQL_ERROR;
  }

  tsdb->is_null = 0;

  switch (Col_or_Param_Num) {
    case 1: // TABLE_CAT
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = primarykeys->current_catalog.str;
      break;
    case 2: // TABLE_SCHEM
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = primarykeys->current_schema.str;
      break;
    case 3: // TABLE_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = primarykeys->current_table.str;
      break;
    case 4: // COLUMN_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_name->str;
      break;
    case 5: // KEY_SEQ
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = primarykeys->ordinal_order;
      break;
    case 6: // PK_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_name->str;
      break;
    default:
      stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:not implemented yet for column[%d]", Col_or_Param_Num);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void primarykeys_init(primarykeys_t *primarykeys, stmt_t *stmt)
{
  primarykeys->owner = stmt;
  tables_init(&primarykeys->tables, stmt);
  tsdb_stmt_init(&primarykeys->desc, stmt);

  stmt_base_t *base = &primarykeys->base;

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

SQLRETURN primarykeys_open(
    primarykeys_t      *primarykeys,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3)
{
  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_reset(primarykeys);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS)  NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS)   NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);

  sr = tables_open(&primarykeys->tables, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, (SQLCHAR*)"TABLE", SQL_NTS);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return _fetch_and_desc_next_table(primarykeys);
}
