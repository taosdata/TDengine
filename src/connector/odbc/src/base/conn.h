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

#ifndef _conn_h_
#define _conn_h_

#include "../base.h"

#include "stmt.h"


#include "../todbc_flex.h"

typedef struct conn_ext_s         conn_ext_t;
struct conn_ext_s {
  void                     *ext;
  void (*free_conn)(conn_t *conn);
  int  (*init_stmt)(stmt_t *stmt);

  SQLRETURN (*connect)(conn_t *conn);
  void      (*disconnect)(conn_t *conn);
};

struct conn_s {
  env_t                  *env;

  char                    enc_src[64];     // c source file encoding
  char                    enc_char[64];    // SQL_CHAR encoding
  char                    enc_wchar[64];   // SQL_WCHAR encoding
  char                    enc_db[64];      // taos client encoding
  // use this for system i/o, such as reading from stdin, writing to stdout/stderr
  char                    enc_locale[64];  // default: current localee

  size_t                  wchar_size;      // shall be fix-length

  conn_val_t              val;

  stmts_t                 stmts;

  errs_t                  errs;

  conn_ext_t              ext;

  unsigned int            connect:2;
};

#define CONN_SET_CONNECTING(conn)       (conn->connect=0x01)
#define CONN_SET_CONNECTED(conn)        (conn->connect=0x02)
#define CONN_SET_NORM(conn)             (conn->connect=0x00)

#define CONN_IS_CONNECTING(conn)        (conn->connect==0x01)
#define CONN_IS_CONNECTED(conn)         (conn->connect==0x02)
#define CONN_IS_NORM(conn)              (conn->connect==0x00)

conn_t* conn_new(env_t *env);
void    conn_free(conn_t *conn);

SQLRETURN conn_check_charset(conn_t *conn, errs_t *errs);

int  conn_add_stmt(conn_t *conn, stmt_t *stmt);
void conn_del_stmt(conn_t *conn, stmt_t *stmt);

SQLRETURN conn_connect(conn_t *conn);
void      conn_disconnect(conn_t *conn);


#endif // _conn_h_


