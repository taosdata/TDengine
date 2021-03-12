/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "stmt.h"

#include "conn.h"
#include "param.h"

static int static_app_row;    // SQL_ATTR_APP_ROW_DESC
static int static_app_param;  // SQL_ATTR_APP_PARAM_DESC
static int static_imp_row;    // SQL_ATTR_IMP_ROW_DESC
static int static_imp_param;  // SQL_ATTR_IMP_PARAM_DESC

static int stmt_init_descs(stmt_t *stmt) {
  OILE(stmt, "");
  descs_t *descs = &stmt->descs;

  descs->app_row   = &static_app_row;    // SQL_ATTR_APP_ROW_DESC
  descs->app_param = &static_app_param;  // SQL_ATTR_APP_PARAM_DESC
  descs->imp_row   = &static_imp_row;    // SQL_ATTR_IMP_ROW_DESC
  descs->imp_param = &static_imp_param;  // SQL_ATTR_IMP_PARAM_DESC

  return 0;
}

static int stmt_init_sql(stmt_t *stmt) {
  OILE(stmt, "");
  sql_t *sql = &stmt->sql;
  OILE(sql->cache==NULL, "");
  OILE(sql->txt.buf==NULL, "");
  OILE(sql->txt.bytes==0, "");
  OILE(sql->txt.total_bytes==0, "");

  sql->cache = todbc_buf_create();
  if (!sql->cache) return -1;

  return 0;
}

static void stmt_release_sql(stmt_t *stmt) {
  OILE(stmt, "");
  sql_t *sql = &stmt->sql;
  if (!sql->cache) return;
  todbc_buf_free(sql->cache);
  sql->cache = NULL;
  sql->txt.buf         = NULL;
  sql->txt.bytes       = 0;
  sql->txt.total_bytes = 0;
}

static int stmt_init(stmt_t *stmt, conn_t *conn) {
  OILE(stmt, "");
  OILE(conn, "");
  OILE(stmt->owner==NULL, "");

  stmt->typeinfo = SQL_UNKNOWN_TYPE;

  int r = errs_init(&stmt->errs);
  if (r) return -1;

  r = stmt_init_descs(stmt);
  if (r) return -1;

  r = stmt_init_sql(stmt);
  if (r) return -1;

  stmt->attr.bind_type = SQL_PARAM_BIND_BY_COLUMN;

  r = conn_add_stmt(conn, stmt);
  OILE(r==0, "");
  OILE(stmt->owner && stmt->owner->conn==conn, "");

  return 0;
}

static void stmt_release(stmt_t *stmt) {
  if (!stmt) return;
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;

  conn_del_stmt(conn, stmt);

  paramset_release(&stmt->paramset);
  fieldset_release(&stmt->fieldset);
  stmt_release_sql(stmt);
  errs_release(&stmt->errs);
}

static SQLRETURN do_process_param(stmt_t *stmt) {
  OILE(stmt->ext.proc_param, "");
  return stmt->ext.proc_param(stmt);
}

static SQLRETURN do_process_param_row(stmt_t *stmt) {
  paramset_t *paramset = &stmt->paramset;
  int         n_params = paramset->n_params;

  SQLRETURN r = SQL_SUCCESS;

  paramset->i_col = 0;
  for (; paramset->i_col<n_params; ++paramset->i_col) {
    r = do_process_param(stmt);
    if (r!=SQL_SUCCESS) return r;
  }

  return SQL_SUCCESS;
}

stmt_t* stmt_new(conn_t *conn) {
  stmt_t *stmt = (stmt_t*)calloc(1, sizeof(*stmt));
  if (!stmt) return NULL;

  if (stmt_init(stmt, conn)) {
    free(stmt);
    return NULL;
  }
  
  return stmt;
}

void stmt_free(stmt_t *stmt) {
  if (!stmt) return;

  // clean ext stuff
  if (stmt->ext.free_stmt) {
    stmt->ext.free_stmt(stmt);
    stmt->ext.free_stmt = NULL;
  }

  // clean stmt stuff
  stmt_release(stmt);

  free(stmt);
}

conn_t* stmt_get_conn(stmt_t *stmt) {
  if (!stmt) return NULL;
  if (!stmt->owner) return NULL;

  return stmt->owner->conn;
}

void stmt_reclaim_params(stmt_t *stmt) {
  if (!stmt) return;
  paramset_reclaim_params(&stmt->paramset);
}

void stmt_reclaim_param_bindings(stmt_t *stmt) {
  if (!stmt) return;
  paramset_reclaim_bindings(&stmt->paramset);
}

void stmt_reclaim_field_bindings(stmt_t *stmt) {
  if (!stmt) return;
  fieldset_reclaim_bindings(&stmt->fieldset);
}

void stmt_close_rs(stmt_t *stmt) {
  if (!stmt) return;

  OILE(stmt->ext.close_rs, "");
  stmt->ext.close_rs(stmt);

  stmt->typeinfo = SQL_UNKNOWN_TYPE;

  stmt->eof      = 0;

  fieldset_reclaim_fields(&stmt->fieldset);
  // for the performance
  // we don't reclaim field-binds here
  // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindcol-function?view=sql-server-ver15
  STMT_SET_NORM(stmt);
}

void stmt_reset_params(stmt_t *stmt) {
  if (!stmt) return;

  stmt->attr.paramset_size = 0;
  stmt_reclaim_param_bindings(stmt);
}

static SQLRETURN stmt_set_sql(stmt_t *stmt, todbc_string_t *txt) {
  OILE(stmt, "");
  OILE(txt, "");
  errs_t *errs = &stmt->errs;
  sql_t *sql   = &stmt->sql;

  OILE(txt!=&sql->txt, "");

  todbc_buf_t *cache = sql->cache;
  OILE(sql->cache, "");

  conn_t *conn = stmt_get_conn(stmt);
  OILE(conn, "");
  const char *enc = conn->enc_db;

  todbc_buf_reclaim(cache);

  sql->txt = todbc_string_conv_to(txt, enc, cache);
  if (!sql->txt.buf) {
    SET_OOM(errs, "");
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_stmt_prepare(stmt_t *stmt) {
  OILE(stmt->ext.prepare, "");
  return stmt->ext.prepare(stmt);
}

static void do_stmt_clear_param_vals(stmt_t *stmt) {
  OILE(stmt->ext.clear_param_vals, "");
  stmt->ext.clear_param_vals(stmt);
}

static void do_stmt_clear_params(stmt_t *stmt) {
  stmt->prepared = 0;
  paramset_t *paramset = &stmt->paramset;
  paramset_reclaim_params(paramset);
  do_stmt_clear_param_vals(stmt);
}

SQLRETURN stmt_exec_direct(stmt_t *stmt, todbc_string_t *txt) {
  OILE(stmt, "");
  OILE(txt, "");

  SQLRETURN r = stmt_set_sql(stmt, txt);
  if (r!=SQL_SUCCESS) return r;

  do_stmt_clear_params(stmt);
  
  if (stmt->ext.exec_direct) {
    r = stmt->ext.exec_direct(stmt);
  } else {
    r = do_stmt_prepare(stmt);
    if (r!=SQL_SUCCESS) return r;
    stmt->prepared = 1;
    OILE(0, "");
  }

  return r;
}

SQLRETURN stmt_prepare(stmt_t *stmt, todbc_string_t *txt) {
  OILE(stmt, "");
  OILE(txt, "");

  SQLRETURN r = stmt_set_sql(stmt, txt);
  if (r!=SQL_SUCCESS) return r;

  do_stmt_clear_params(stmt);

  r = do_stmt_prepare(stmt);
  if (r!=SQL_SUCCESS) return r;
  stmt->prepared = 1;

  return SQL_SUCCESS;
}

SQLRETURN stmt_bind_param(stmt_t *stmt, param_binding_t *arg) {
  OILE(stmt, "");
  OILE(arg, "");
  OILE(arg->ParameterNumber>0, "");
  int idx = arg->ParameterNumber - 1;

  errs_t     *errs     = &stmt->errs;
  paramset_t *paramset = &stmt->paramset;

  paramset_init_bindings_cache(paramset);
  if (!paramset->bindings_cache) {
    SET_OOM(errs, "failed to alloc cache for param binds");
    return SQL_ERROR;
  }

  todbc_buf_t  *cache = paramset->bindings_cache;
  OILE(cache, "");

  param_binding_t *bindings = paramset->bindings;
  if (idx>=paramset->n_bindings) {
    size_t num = (size_t)(idx + 1);
    // align
    const size_t block = 10;
    num = (num + block-1) / block * block;
    bindings = (param_binding_t*)todbc_buf_realloc(cache, bindings, num * sizeof(*bindings));
    if (!bindings) {
      SET_OOM(errs, "failed to realloc buf for param binds");
      return SQL_ERROR;
    }
    paramset->bindings     = bindings;
    paramset->n_bindings   = idx + 1;
  }
  OILE(paramset->bindings, "");
  OILE(idx<paramset->n_bindings, "");

  param_binding_t *binding = bindings + idx;
  *binding = *arg;

  if (paramset->n_bindings>paramset->n_params) return SQL_SUCCESS;

  OILE(stmt->ext.set_param_conv, "");
  return stmt->ext.set_param_conv(stmt, idx);
}

SQLRETURN stmt_execute(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");

  SQLRETURN r = SQL_SUCCESS;

  if (stmt->prepared==0) {
    do_stmt_clear_params(stmt);

    r = do_stmt_prepare(stmt);
    if (r!=SQL_SUCCESS) return r;
    stmt->prepared = 1;
  }
  OILE(stmt->prepared==1, "");


  errs_t *errs = &stmt->errs;

  paramset_t *paramset = &stmt->paramset;
  int         n_params = paramset->n_params;
  int         n_bindings = paramset->n_bindings;

  if (n_params>n_bindings) {
    SET_GENERAL(errs, "parameters need to be bound first");
    return SQL_ERROR;
  }

  if (n_params>0) {
    int paramset_size = (int)stmt->attr.paramset_size;
    if (paramset_size==0) paramset_size = 1;
    stmt->attr.paramset_size = (SQLULEN)paramset_size;

    paramset->i_row = 0;
    for (; paramset->i_row<paramset_size; ++paramset->i_row) {
      r = do_process_param_row(stmt);
      if (r) return r;

      OILE(stmt->ext.param_row_processed, "");
      r = stmt->ext.param_row_processed(stmt);
      if (r) return r;
    }
  }

  OILE(stmt->ext.execute, "");
  return stmt->ext.execute(stmt);
}

SQLRETURN stmt_fetch(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  if (stmt->eof) return SQL_NO_DATA;

  OILE(stmt->ext.fetch, "");
  SQLRETURN r = stmt->ext.fetch(stmt);
  if (r!=SQL_SUCCESS) return r;

  fieldset_t *fieldset = &stmt->fieldset;
  if (fieldset->n_bindings==0) return SQL_SUCCESS;
  OILE(fieldset->n_bindings>0, "");

  for (size_t i=0; i<fieldset->n_bindings; ++i) {
    OILE(fieldset->bindings, "");
    col_binding_t *binding = fieldset->bindings + i;
    if (binding->ColumnNumber!=i+1) {
      OILE(binding->ColumnNumber==0, "");
      continue;
    }
    OILE(stmt->ext.get_data, "");
    r = stmt->ext.get_data(stmt, binding);
    if (r!=SQL_SUCCESS) return r;
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_get_data(stmt_t *stmt, col_binding_t *binding) {
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  OILE(stmt->eof==0, "");

  OILE(stmt->ext.get_data, "");
  return stmt->ext.get_data(stmt, binding);
}

SQLRETURN stmt_bind_col(stmt_t *stmt, col_binding_t *binding) {
  OILE(stmt, "");
  // shall we check execute state?

  errs_t *errs = &stmt->errs;

  fieldset_t *fieldset = &stmt->fieldset;

  todbc_buf_t *cache = fieldset->bindings_cache;
  if (cache==NULL) {
    fieldset_init_bindings(fieldset);
    cache = fieldset->bindings_cache;
    if (!cache) {
      SET_OOM(errs, "");
      return SQL_ERROR;
    }
  }
  OILE(cache, "");

  col_binding_t *bindings = fieldset->bindings;

  OILE(binding->ColumnNumber>0, "");
  if (binding->ColumnNumber>=fieldset->n_bindings) {
    size_t num    = (size_t)binding->ColumnNumber;
    const size_t block = 10;
    size_t align  = (num+block-1)/block*block;
    size_t total = align * sizeof(*bindings);
    bindings = (col_binding_t*)todbc_buf_realloc(cache, bindings, total);
    if (!bindings) {
      SET_OOM(errs, "");
      return SQL_ERROR;
    }
    for (size_t i = (size_t)fieldset->n_bindings; i<num; ++i) {
      bindings[i].ColumnNumber = 0;  // not set yet
    }

    fieldset->bindings   = bindings;
    fieldset->n_bindings = (int)num;
  }
  OILE(bindings, "");
  OILE(binding->ColumnNumber<=fieldset->n_bindings, "");
  bindings[binding->ColumnNumber-1] = *binding;

  return SQL_SUCCESS;
}




// public
errs_t* stmt_get_errs(stmt_t *stmt) {
  OILE(stmt, "");

  return &stmt->errs;
}

void stmt_clr_errs(stmt_t *stmt) {
  if (!stmt) return;

  errs_reclaim(&stmt->errs);
}

