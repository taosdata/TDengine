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

#include "null_conn.h"

#include "env.h"

// null_conn

static void null_conn_free(conn_t *conn) {
  OILE(conn, "");
  null_conn_t *null_conn = (null_conn_t*)conn->ext.ext;
  OILE(null_conn, "");

  conn->ext.ext       = NULL;
  conn->ext.free_conn = NULL;

  free(null_conn);
}

static SQLRETURN null_conn_connect(conn_t *conn) {
  OILE(conn, "");
  null_conn_t *null_conn = (null_conn_t*)conn->ext.ext;
  OILE(null_conn && null_conn->conn==conn, "");
  OILE(CONN_IS_NORM(conn), "");
  return SQL_SUCCESS;
}

static void null_conn_disconnect(conn_t *conn) {
  OILE(conn, "");
  OILE(CONN_IS_CONNECTED(conn), "");
}

static void null_conn_free_stmt(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
}

static SQLRETURN null_conn_exec_direct(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  OILE(!STMT_IS_EXECUTED(stmt), "");
  STMT_SET_EXECUTED(stmt);
  return SQL_SUCCESS;
}

static SQLRETURN null_conn_prepare(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  return SQL_SUCCESS;
}

static SQLRETURN null_conn_proc_param(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  return SQL_SUCCESS;
}

static SQLRETURN null_conn_execute(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->owner, "");
  OILE(!STMT_IS_EXECUTED(stmt), "");
  STMT_SET_EXECUTED(stmt);
  return SQL_SUCCESS;
}

static int null_conn_init_stmt(stmt_t *stmt) {
  OILE(stmt, "");
  OILE(stmt->ext.ext==NULL, "");
  OILE(stmt->ext.free_stmt==NULL, "");

  OILE(stmt->owner==NULL, "");

  stmt->ext.ext              = NULL;
  stmt->ext.free_stmt        = null_conn_free_stmt;
  stmt->ext.exec_direct      = null_conn_exec_direct;
  stmt->ext.prepare          = null_conn_prepare;
  stmt->ext.proc_param       = null_conn_proc_param;
  stmt->ext.execute          = null_conn_execute;

  return 0;
}

int conn_init_null_conn(conn_t *conn) {
  OILE(conn, "");
  OILE(conn->ext.ext==NULL, "");
  OILE(conn->ext.free_conn==NULL, "");

  null_conn_t *null_conn = (null_conn_t*)calloc(1, sizeof(*null_conn));
  if (!null_conn) return -1;

  null_conn->conn       = conn;
  conn->ext.ext         = null_conn;
  conn->ext.free_conn   = null_conn_free;
  conn->ext.connect     = null_conn_connect;
  conn->ext.disconnect  = null_conn_disconnect;
  conn->ext.init_stmt   = null_conn_init_stmt;

  return 0;
}



