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

#ifndef _TODBC_FLEX_H_
#define _TODBC_FLEX_H_

#include <sql.h>
#include <sqlext.h>

// TSDB predefined field types
// TINYINT SMALLINT INT BIGINT FLOAT DOUBLE BOOL TIMESTAMP BINARY NCHAR

typedef struct map_tsdb_type_s           map_tsdb_type_t;
struct map_tsdb_type_s {
  SQLSMALLINT      tsdb_tinyint;
  SQLSMALLINT      tsdb_smallint;
  SQLSMALLINT      tsdb_int;
  SQLSMALLINT      tsdb_bigint;
  SQLULEN          tsdb_bigint_size;
  SQLSMALLINT      tsdb_float;
  SQLSMALLINT      tsdb_double;
  SQLSMALLINT      tsdb_bool;
  SQLSMALLINT      tsdb_timestamp;
  SQLSMALLINT      tsdb_binary;
  SQLSMALLINT      tsdb_nchar;
};

typedef struct conn_val_s              conn_val_t;
struct conn_val_s {
  char                *dsn;
  char                *uid;
  char                *pwd;
  char                *db;
  char                *server;
  char                *enc_local;
  char                *enc_char;
  char                *enc_wchar;
  char                *enc_db;

  map_tsdb_type_t      tsdb_map;
};


void conn_val_reset(conn_val_t *val);
int todbc_parse_conn_string(const char *conn, conn_val_t *val);

#endif // _TODBC_FLEX_H_

