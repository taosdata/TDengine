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

#ifndef _stmt_h_
#define _stmt_h_

#include "../base.h"

#include "err.h"
#include "field.h"
#include "param.h"
#include "rs.h"

typedef struct descs_s                     descs_t;
typedef struct stmts_s                     stmts_t;
typedef struct stmt_ext_s                  stmt_ext_t;
typedef struct stmt_attr_s                 stmt_attr_t;
typedef struct sql_s                       sql_t;

struct descs_s {
  void                  *app_row;    // SQL_ATTR_APP_ROW_DESC
  void                  *app_param;  // SQL_ATTR_APP_PARAM_DESC
  void                  *imp_row;    // SQL_ATTR_IMP_ROW_DESC
  void                  *imp_param;  // SQL_ATTR_IMP_PARAM_DESC
};

struct stmt_ext_s {
  void                  *ext;
  void (*free_stmt)(stmt_t *stmt);

  void      (*clear_params)(stmt_t *stmt);
  void      (*clear_param_vals)(stmt_t *stmt);

  SQLRETURN (*get_param)(stmt_t *stmt, param_t *arg);
  SQLRETURN (*set_param_conv)(stmt_t *stmt, int idx);

  SQLRETURN (*exec_direct)(stmt_t *stmt);
  SQLRETURN (*prepare)(stmt_t *stmt);
  SQLRETURN (*proc_param)(stmt_t *stmt);
  SQLRETURN (*param_row_processed)(stmt_t *stmt);
  SQLRETURN (*execute)(stmt_t *stmt);
  SQLRETURN (*get_affected_rows)(stmt_t *stmt, SQLLEN *RowCount);
  SQLRETURN (*get_fields_count)(stmt_t *stmt, SQLSMALLINT *ColumnCount);
  SQLRETURN (*get_field)(stmt_t *stmt, field_arg_t *arg);
  SQLRETURN (*fetch)(stmt_t *stmt);
  SQLRETURN (*get_data)(stmt_t *stmt, col_binding_t *col);
  void      (*close_rs)(stmt_t *stmt);
};

struct stmt_attr_s {
  // SQL_ATTR_PARAM_BIND_TYPE: SQL_PARAM_BIND_BY_COLUMN or row size
  SQLULEN                bind_type;             // default: SQL_PARAM_BIND_BY_COLUMN
  // SQL_ATTR_PARAM_BIND_OFFSET_PTR
  SQLULEN               *bind_offset_ptr;       // default: NULL
  // SQL_ATTR_PARAMSET_SIZE
  SQLULEN                paramset_size;         // default: 0
};

struct sql_s {
  todbc_buf_t           *cache;
  todbc_string_t         txt;
};

struct stmt_s {
  stmts_t               *owner;
  stmt_t                *next;
  stmt_t                *prev;

  SQLSMALLINT            typeinfo;

  descs_t                descs;

  sql_t                  sql;

  stmt_attr_t            attr;

  paramset_t             paramset;
  fieldset_t             fieldset;

  int                    affected_rows;
  rs_t                   rs;

  errs_t                 errs;

  stmt_ext_t             ext;

  unsigned int           prepared:1;
  unsigned int           execute:2;
  unsigned int           eof:1;
};

struct stmts_s {
  conn_t                *conn;

  int                    count;
  stmt_t                *head;
  stmt_t                *tail;
};

#define STMT_TYPEINFO(stmt)  (stmt->typeinfo!=SQL_UNKNOWN_TYPE)

#define STMT_SET_EXECUTING(stmt)       (stmt->execute=0x01)
#define STMT_SET_EXECUTED(stmt)        (stmt->execute=0x02)
#define STMT_SET_NORM(stmt)            (stmt->execute=0x00)

#define STMT_IS_EXECUTING(stmt)        (stmt->execute==0x01)
#define STMT_IS_EXECUTED(stmt)         (stmt->execute==0x02)
#define STMT_IS_NORM(stmt)             (stmt->execute==0x00)

stmt_t*   stmt_new(conn_t *conn);
void      stmt_free(stmt_t *stmt);

conn_t*   stmt_get_conn(stmt_t *stmt);

void      stmt_reclaim_params(stmt_t *stmt);
void      stmt_reclaim_param_binds(stmt_t *stmt);
void      stmt_reclaim_field_binds(stmt_t *stmt);
void      stmt_close_rs(stmt_t *stmt);
void      stmt_reset_params(stmt_t *stmt);
SQLRETURN stmt_exec_direct(stmt_t *stmt, todbc_string_t *txt);
SQLRETURN stmt_prepare(stmt_t *stmt, todbc_string_t *txt);
SQLRETURN stmt_bind_param(stmt_t *stmt, param_binding_t *arg);
SQLRETURN stmt_execute(stmt_t *stmt);
SQLRETURN stmt_fetch(stmt_t *stmt);
SQLRETURN stmt_get_data(stmt_t *stmt, col_binding_t *binding);
SQLRETURN stmt_bind_col(stmt_t *stmt, col_binding_t *binding);

#endif // _stmt_h_



