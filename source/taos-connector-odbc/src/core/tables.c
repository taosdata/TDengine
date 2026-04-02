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

#include "tables.h"

#include "charset.h"
#include "conn.h"
#include "ds.h"
#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tls.h"
#include "tsdb.h"

#include <ctype.h>

void tables_args_reset(tables_args_t *args)
{
  if (!args) return;

  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
  WILD_SAFE_FREE(args->type_pattern);
}

void tables_args_release(tables_args_t *args)
{
  if (!args) return;
  tables_args_reset(args);
}

static void table_types_reset(tables_t *tables)
{
  mem_reset(&tables->table_types);
}

static void table_types_release(tables_t *tables)
{
  if (!tables) return;
  table_types_reset(tables);

  mem_release(&tables->table_types);
}

void tables_reset(tables_t *tables)
{
  if (!tables) return;

  tsdb_stmt_reset(&tables->stmt);
  mem_reset(&tables->tsdb_stmt);
  table_types_reset(tables);
  mem_reset(&tables->catalog_cache);
  mem_reset(&tables->schema_cache);
  mem_reset(&tables->table_cache);
  mem_reset(&tables->type_cache);

  tables_args_reset(&tables->tables_args);

  tables->catalog = NULL;
  tables->schema  = NULL;
  tables->table   = NULL;
  tables->type    = NULL;
}

void tables_release(tables_t *tables)
{
  if (!tables) return;
  tables_reset(tables);

  table_types_release(tables);

  tsdb_stmt_release(&tables->stmt);
  mem_release(&tables->tsdb_stmt);

  mem_release(&tables->catalog_cache);
  mem_release(&tables->schema_cache);
  mem_release(&tables->table_cache);
  mem_release(&tables->type_cache);

  tables_args_release(&tables->tables_args);

  tables->owner = NULL;
}

static SQLRETURN _prepare(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  (void)sqlc_tsdb;

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _get_col_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;
  return tables->stmt.base.get_col_fields(&tables->stmt.base, fields, nr);
}

static void _match(tables_t *tables, tsdb_data_t *tsdb, int *matched)
{
  const char *begin = (const char*)tables->table_types.base;
  const char *end   = begin + tables->table_types.nr;

  const char *p = begin;
  while (p < end) {
    // NOTE: non-unicode stored in table_types
    size_t n = strlen(p);
    if (tsdb->str.len == n && strncmp(tsdb->str.str, p, n) == 0) {
      *matched = 1;
      return;
    }
    p += n + 1;
    continue;
  }

  *matched = 0;

  return;
}

static SQLRETURN _fetch_row_with_tsdb(stmt_base_t *base, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  tables_t *tables = (tables_t*)base;
  stmt_t *stmt = tables->owner;

  const char *fromcode = conn_get_tsdb_charset(stmt->conn);

again:

  sr = tables->stmt.base.fetch_row(&tables->stmt.base);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (tables->tables_type != TABLES_FOR_GENERIC) return SQL_SUCCESS;

  sr = tables->stmt.base.get_data(&tables->stmt.base, 1, tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  string_t str = {
    .charset              = fromcode,
    .str                  = tsdb->str.str,
    .bytes                = tsdb->str.len,
  };

  if (tables->tables_args.catalog_pattern) {
    int matched = 0;
    r = wildexec(tables->tables_args.catalog_pattern, &str, &matched);
    if (r) {
      stmt_append_err(tables->owner, "HY000", 0, "General error:wild matching failed");
      return SQL_ERROR;
    }
    if (!matched) goto again;
  } else if (tables->tables_args.select_current_db) {
    const char *db = tables->tables_args.db;
    int matched = 0;
    if (!matched) matched = (str.bytes == strlen(db) && strncmp(str.str, db, str.bytes) == 0);
    if (!matched) goto again;
  }

  if (tables->tables_args.schema_pattern) {
    sr = tables->stmt.base.get_data(&tables->stmt.base, 2, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    string_t str = {
      .charset              = fromcode,
      .str                  = tsdb->str.str,
      .bytes                = tsdb->str.len,
    };
    int matched = 0;
    r = wildexec(tables->tables_args.schema_pattern, &str, &matched);
    if (r) {
      stmt_append_err(tables->owner, "HY000", 0, "General error:wild matching failed");
      return SQL_ERROR;
    }
    if (!matched) goto again;
  }

  if (tables->tables_args.table_pattern) {
    sr = tables->stmt.base.get_data(&tables->stmt.base, 3, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    string_t str = {
      .charset              = fromcode,
      .str                  = tsdb->str.str,
      .bytes                = tsdb->str.len,
    };
    int matched = 0;
    r = wildexec(tables->tables_args.table_pattern, &str, &matched);
    if (r) {
      stmt_append_err(tables->owner, "HY000", 0, "General error:wild matching failed");
      return SQL_ERROR;
    }
    if (!matched) goto again;
  }

  if (tables->table_types.nr > 0) {
    sr = tables->stmt.base.get_data(&tables->stmt.base, 4, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    int matched = 0;
    _match(tables, tsdb, &matched);

    if (!matched) goto again;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  tsdb_data_t tsdb = {0};

  sr = _fetch_row_with_tsdb(base, &tsdb);

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

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;

  return tables->stmt.base.row_count(&tables->stmt.base, row_count_ptr);
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  tables_t *tables = (tables_t*)base;
  return tables->stmt.base.get_num_cols(&tables->stmt.base, ColumnCountPtr);
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  tables_t *tables = (tables_t*)base;
  return tables->stmt.base.get_data(&tables->stmt.base, Col_or_Param_Num, tsdb);
}

void tables_init(tables_t *tables, stmt_t *stmt)
{
  tables->owner = stmt;
  tsdb_stmt_init(&tables->stmt, stmt);

  stmt_base_t *base = &tables->base;

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

static SQLRETURN _tables_open_catalogs(tables_t *tables)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  const char *sql =
    "select name `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, null `TABLE_TYPE`, null `REMARKS` from information_schema.ins_databases order by `TABLE_CAT`";
  // BI mode not show system databases
  if (tables->owner->conn->cfg.conn_mode == 1){
    sql = "select name `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, null `TABLE_TYPE`, null `REMARKS` from information_schema.ins_databases "
          "where name not in ('information_schema', 'performance_schema') order by `TABLE_CAT`";
  }

  sqlc_tsdb_t sqlc_tsdb = {
    .sqlc           = sql,
    .sqlc_bytes     = strlen(sql),
  };

  stmt_t *stmt = tables->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  mem_reset(&tables->tsdb_stmt);
  r = mem_conv(&tables->tsdb_stmt, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  sqlc_tsdb.tsdb       = (const char*)tables->tsdb_stmt.base;
  sqlc_tsdb.tsdb_bytes = tables->tsdb_stmt.nr;

  sr = tsdb_stmt_query(&tables->stmt, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_CATALOGS;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_open_schemas(tables_t *tables)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  const char *sql =
    "select null `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, null `TABLE_TYPE`, null `REMARKS` where 1=2";

  sqlc_tsdb_t sqlc_tsdb = {
    .sqlc           = sql,
    .sqlc_bytes     = strlen(sql),
  };

  stmt_t *stmt = tables->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  mem_reset(&tables->tsdb_stmt);
  r = mem_conv(&tables->tsdb_stmt, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  sqlc_tsdb.tsdb       = (const char*)tables->tsdb_stmt.base;
  sqlc_tsdb.tsdb_bytes = tables->tsdb_stmt.nr;

  sr = tsdb_stmt_query(&tables->stmt, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_SCHEMAS;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_open_tabletypes_with_buffer(tables_t *tables, buffer_t *buf)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  const char *table_types[] = {
    "CHILD TABLE",
    "SUPER TABLE",
    "SYSTEM TABLE",
    "TABLE",
    "VIEW",
  };

  for (size_t i=0; i<sizeof(table_types)/sizeof(table_types[0]); ++i) {
    const char *table_type = table_types[i];
    if (i) {
      r = buffer_concat_fmt(buf, " union ");
      if (r) {
        stmt_oom(tables->owner);
        return SQL_ERROR;
      }
    }
    r = buffer_concat_fmt(buf, "select null `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, '%s' `TABLE_TYPE`, null `REMARKS`", table_type);
    if (r) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
  }

  const char *sql = buf->base;

  sqlc_tsdb_t sqlc_tsdb = {
    .sqlc           = sql,
    .sqlc_bytes     = strlen(sql),
  };

  stmt_t *stmt = tables->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  mem_reset(&tables->tsdb_stmt);
  r = mem_conv(&tables->tsdb_stmt, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  sqlc_tsdb.tsdb       = (const char*)tables->tsdb_stmt.base;
  sqlc_tsdb.tsdb_bytes = tables->tsdb_stmt.nr;

  sr = tsdb_stmt_query(&tables->stmt, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_TABLETYPES;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_open_tabletypes(tables_t *tables)
{
  SQLRETURN sr = SQL_SUCCESS;

  buffer_t buf = {0};

  sr = _tables_open_tabletypes_with_buffer(tables, &buf);

  buffer_release(&buf);

  return sr;
}

static SQLRETURN _tables_add_table_type(tables_t *tables, SQLCHAR *TableType, SQLSMALLINT NameLength4, const char *begin, const char *p)
{
  stmt_t *stmt = tables->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  char *t = (char*)tables->table_types.base + tables->table_types.nr;
  char       *inbuf                 = (char*)begin;
  size_t      inbytesleft           = p-begin;
  char       *outbuf                = t;
  size_t      outbytesleft          = tables->table_types.cap - tables->table_types.nr;
  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  if (n != 0) {
    stmt_append_err_format(tables->owner, "HY000", 0, "convert [%.*s] from %s to %s failed or non-reversible characters found therein",
        (int)NameLength4, (const char*)TableType, cnv->from, cnv->to);
    return SQL_ERROR;
  }

  // NOTE: non-unicode for tsdb_varchar, thus '\0' is a string terminator
  memset(outbuf, 0, 1);

  tables->table_types.nr += outbuf - t + 1;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_parse_table_type(tables_t *tables, SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *begin = (const char*)TableType;
  const char *end   = begin + NameLength4;

  size_t cap = (NameLength4 + 1) * 6;
  int r = mem_keep(&tables->table_types, cap);
  if (r) {
    stmt_oom(tables->owner);
    return SQL_ERROR;
  }

  const char *p = begin;
  begin = NULL;

  while (p < end) {
    if (!begin) {
      if (isspace(*p)) {
        ++p;
        continue;
      }
      begin = p;
      continue;
    }

    if (*p != ',') {
      ++p;
      continue;
    }

    if (p > begin) {
      sr = _tables_add_table_type(tables, TableType, NameLength4, begin, p);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
    }
    begin = NULL;
    ++p;
  }

  sr = _tables_add_table_type(tables, TableType, NameLength4, begin, p);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

SQLRETURN tables_open(
    tables_t      *tables,
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
  int r = 0;

  tables_reset(tables);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS)  NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS)   NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);
  if (TableType && NameLength4 == SQL_NTS)   NameLength4 = (SQLSMALLINT)strlen((const char*)TableType);

  if (CatalogName && strncmp((const char*)CatalogName, SQL_ALL_CATALOGS, 1) == 0) {
    if ((!SchemaName || !*SchemaName) && (!TableName || !*TableName) && (!TableType || !*TableType)) {
      return _tables_open_catalogs(tables);
    }
  }
  if (SchemaName && strncmp((const char*)SchemaName, SQL_ALL_SCHEMAS, 1) == 0) {
    if ((!CatalogName || !*CatalogName) && (!TableName || !*TableName)&& (!TableType || !*TableType)) {
      return _tables_open_schemas(tables);
    }
  }
  if (TableType && strncmp((const char*)TableType, SQL_ALL_TABLE_TYPES, 1) == 0) {
    if ((!CatalogName || !*CatalogName) && (!SchemaName || !*SchemaName) && (!TableName || !*TableName)) {
      return _tables_open_tabletypes(tables);
    }
  }


  stmt_t *stmt = tables->owner;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = "UCS-2LE";
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  if (CatalogName) {
    if (mem_conv(&tables->catalog_cache, cnv->cnv, (const char*)CatalogName, NameLength1)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->catalog = tables->catalog_cache.base;
    string_t str = {
      .charset              = cnv->from,
      .str                  = (const char*)CatalogName,
      .bytes                = NameLength1,
    };
    if (wildcomp(&tables->tables_args.catalog_pattern, &str)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for CatalogName[%.*s]", (int)NameLength1, (const char*)CatalogName);
      return SQL_ERROR;
    }
    tables->tables_args.select_current_db = 0;
  } else {
    ds_conn_t *ds_conn = &tables->owner->conn->ds_conn;
    char      *db      = tables->tables_args.db;
    size_t     len     = sizeof(tables->tables_args.db);
    ds_err_t   ds_err;
    ds_err.err = 0; ds_err.str[0] = '\0';
    r = ds_conn_get_current_db(ds_conn, db, len, &ds_err);
    if (r) {
      stmt_append_err_format(tables->owner, "HY000", 0, "General error:failed getting current db:[%d]%s", ds_err.err, ds_err.str);
      return SQL_ERROR;
    }
    tables->tables_args.select_current_db = 1;
  }
  if (SchemaName) {
    if (mem_conv(&tables->schema_cache, cnv->cnv, (const char*)SchemaName, NameLength2)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->schema = tables->schema_cache.base;
    string_t str = {
      .charset              = cnv->from,
      .str                  = (const char*)SchemaName,
      .bytes                = NameLength2,
    };
    if (wildcomp(&tables->tables_args.schema_pattern, &str)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for SchemaName[%.*s]", (int)NameLength2, (const char*)SchemaName);
      return SQL_ERROR;
    }
  }
  if (TableName) {
    if (mem_conv(&tables->table_cache, cnv->cnv, (const char*)TableName, NameLength3)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->table = tables->table_cache.base;
    string_t str = {
      .charset              = cnv->from,
      .str                  = (const char*)TableName,
      .bytes                = NameLength3,
    };
    if (wildcomp(&tables->tables_args.table_pattern, &str)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for TableName[%.*s]", (int)NameLength3, (const char*)TableName);
      return SQL_ERROR;
    }
  }
  if (TableType) {
    if (mem_conv(&tables->type_cache, cnv->cnv, (const char*)TableType, NameLength4)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->type = tables->type_cache.base;
    string_t str = {
      .charset              = cnv->from,
      .str                  = (const char*)TableType,
      .bytes                = NameLength4,
    };
    if (wildcomp(&tables->tables_args.type_pattern, &str)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for TypeName[%.*s]", (int)NameLength4, (const char*)TableType);
      return SQL_ERROR;
    }
    if (*TableType) {
      sr = _tables_parse_table_type(tables, TableType, NameLength4);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
    }
  }

  const char *sql =
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, stable_name `TABLE_NAME`, 'TABLE' `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_stables"
    " "
    "union all"
    " "
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, table_name `TABLE_NAME`,"
    "  case when `type`='SYSTEM_TABLE' then 'TABLE'"
    "       when `type`='NORMAL_TABLE' then 'TABLE'"
    "       when `type`='CHILD_TABLE' then 'TABLE'"
    "       else 'UNKNOWN'"
    "  end `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_tables"
    " "
    "union all"
    " "
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, view_name `TABLE_NAME`, 'VIEW' `TABLE_TYPE`, NULL `REMARKS` from information_schema.ins_views"
    " "
    "order by `TABLE_TYPE`, `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`";
  // BI mode do not show system table and child table
  if (stmt->conn->cfg.conn_mode){
    sql =
        "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, stable_name `TABLE_NAME`, 'TABLE' `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_stables"
        " "
        "union all"
        " "
        "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, table_name `TABLE_NAME`, 'TABLE' `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_tables "
        "where type = 'NORMAL_TABLE' "
        " "
        "union all"
        " "
        "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, view_name `TABLE_NAME`, 'VIEW' `TABLE_TYPE`, NULL `REMARKS` from information_schema.ins_views"
        " "
        "order by `TABLE_TYPE`, `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`";
  }

  sqlc_tsdb_t sqlc_tsdb = {
    .sqlc           = sql,
    .sqlc_bytes     = strlen(sql),
  };

  fromcode = conn_get_sqlc_charset(stmt->conn);
  tocode   = conn_get_tsdb_charset(stmt->conn);
  cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  mem_reset(&tables->tsdb_stmt);
  r = mem_conv(&tables->tsdb_stmt, cnv->cnv, sqlc_tsdb.sqlc, sqlc_tsdb.sqlc_bytes);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  sqlc_tsdb.tsdb       = (const char*)tables->tsdb_stmt.base;
  sqlc_tsdb.tsdb_bytes = tables->tsdb_stmt.nr;

  sr = tsdb_stmt_query(&tables->stmt, &sqlc_tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_GENERIC;
  return SQL_SUCCESS;
}
