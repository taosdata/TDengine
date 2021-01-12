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

#ifndef TDENGINE_ODBC_DRIVER_H
#define TDENGINE_ODBC_DRIVER_H

#include "odbc.h"

#include "taos.h"
#include "tsclient.h"
#include "tlog.h"
#include "taosdef.h"

#ifndef SQL_API
#define SQL_API
#endif


#define odbcFatal(...) { if (odbcDebugFlag & DEBUG_FATAL) { taosPrintLog("ODB FATAL ", tscEmbedded ? 255 : odbcDebugFlag, __VA_ARGS__); }}
#define odbcError(...) { if (odbcDebugFlag & DEBUG_ERROR) { taosPrintLog("ODB ERROR ", tscEmbedded ? 255 : odbcDebugFlag, __VA_ARGS__); }}
#define odbcWarn(...)  { if (odbcDebugFlag & DEBUG_WARN)  { taosPrintLog("ODB WARN ", tscEmbedded ? 255 : odbcDebugFlag, __VA_ARGS__); }}
#define odbcInfo(...)  { if (odbcDebugFlag & DEBUG_INFO)  { taosPrintLog("ODB ", tscEmbedded ? 255 : cDodbcDebugFlagebugFlag, __VA_ARGS__); }}
#define odbcDebug(...) { if (odbcDebugFlag & DEBUG_DEBUG) { taosPrintLog("ODB ", odbcDebugFlag, __VA_ARGS__); }}
#define odbcTrace(...) { if (odbcDebugFlag & DEBUG_TRACE) { taosPrintLog("ODB ", odbcDebugFlag, __VA_ARGS__); }}

#define MAX_BIND_COL TSDB_MAX_COLUMNS
#define MAX_ERROR_LEN 1024
#define DEAD_MAGIC 0xdeadbeef

/**
* @typedef ENV
* @struct env
* Driver internal structure for environment (HENV).
*/
typedef struct env {
  void *signature;
  pthread_mutex_t mutex;
} ENV;

/**
* @typedef DBC
* @struct dbc
* Driver internal structure for database connection (HDBC).
*/
typedef struct dbc {
  void *signature;
  pthread_mutex_t mutex;
  ENV *env;
  int32_t version;
  uint8_t version_maj;
  uint8_t version_min;
  uint8_t version_lev;
  uint8_t version_reserve;
  bool ov3;

  TAOS*con;
  char dsn[TSDB_USER_LEN];
  char server[TSDB_IPv4ADDR_LEN];
  char dbname[TSDB_DB_NAME_LEN];
  char user[TSDB_USER_LEN];
  char pwd[TSDB_KEY_LEN];
  char tbname[TSDB_TABLE_NAME_LEN];  //for sql columns
  
  int naterr;
  SQLCHAR logmsg[MAX_ERROR_LEN + 1];
  char sqlstate[6];
} DBC;

/**
* @typedef COL
* @struct COL
* Internal structure to describe a column in a result set.
*/
typedef struct {
  //bind info
  void *val;
  SQLLEN maxLen;
  SQLLEN* len;
  int type;
  //raw info
  char fieldName[TSDB_COL_NAME_LEN];
  int fieldSize;
  int fieldType;
  int fieldDisplaySize;
  int fieldScale;
} COL;

typedef enum  {
  STMT_NORMAL_SQL,
  STMT_DESCRIBE_COLUMNS_SQL,
  STMT_SHOW_DATABASE_SQL,
  STMT_SHOW_SCHEMA_SQL,
  STMT_SHOW_TABLES_TYPE_SQL,
  STMT_SHOW_TABLES_SQL,
  STMT_SHOW_STABLES_SQL,
  STMT_PRIMARY_KEY_SQL,
  STMT_FOERIGN_KEY_SQL
} STMT_TYPE;

/**
* @typedef STMT
* @struct stmt
* Driver internal structure for statment (HSTMT).
*/
typedef struct stmt {
  void *signature;
  DBC *dbc;
  char sql[TSDB_DEFAULT_PKT_SIZE];
  STMT_TYPE type;

  TAOS_RES *result;
  TAOS_FIELD *fields;
  TAOS_ROW row;
  int numFields;
  int rowsAffacted;
  SQLULEN rowsFetched;
  COL cols[MAX_BIND_COL];     //bind cols

  int fixedResultSetIndex;
  bool isPreparedStmt;
} STMT;

void odbc_init();

#define HENV_LOCK(henv)            \
{                                  \
  ENV *_e;                         \
  if ((henv) == SQL_NULL_HENV) {   \
    return SQL_INVALID_HANDLE;     \
  }                                \
  _e = (ENV *) (henv);             \
  if (_e->signature != _e) {       \
    return SQL_INVALID_HANDLE;     \
  }                                \
  pthread_mutex_lock(&_e->mutex);  \
}

#define HENV_UNLOCK(henv)          \
{                                  \
  ENV *_e;                         \
  if ((henv) == SQL_NULL_HENV) {   \
    return SQL_INVALID_HANDLE;     \
  }                                \
  _e = (ENV *) (henv);             \
  if (_e->signature != _e) {       \
    return SQL_INVALID_HANDLE;     \
  }                                \
  pthread_mutex_unlock(&_e->mutex);\
}

#define HDBC_LOCK(hdbc)            \
{                                  \
  DBC *_d;                         \
  if ((hdbc) == SQL_NULL_HDBC) {   \
    return SQL_INVALID_HANDLE;     \
  }                                \
  _d = (DBC *) (hdbc);             \
  if (_d->signature != _d) {       \
    return SQL_INVALID_HANDLE;     \
  }                                \
  pthread_mutex_lock(&_d->mutex);  \
}

#define HDBC_UNLOCK(hdbc)          \
{                                  \
  DBC *_d;                         \
  if ((hdbc) == SQL_NULL_HDBC) {   \
    return SQL_INVALID_HANDLE;     \
  }                                \
  _d = (DBC *) (hdbc);             \
  if (_d->signature != _d) {       \
    return SQL_INVALID_HANDLE;     \
  }                                \
  pthread_mutex_unlock(&_d->mutex);\
}

#define HSTMT_LOCK(hstmt)          \
{                                  \
  STMT *_s;                        \
  if ((hstmt) == SQL_NULL_HSTMT) { \
    return SQL_INVALID_HANDLE;     \
  }                                \
  _s = (STMT *)(hstmt);            \
  if (_s->signature != _s) {       \
    return SQL_INVALID_HANDLE;     \
  }                                \
  HDBC_LOCK(_s->dbc)               \
}

#define HSTMT_UNLOCK(hstmt)        \
{                                  \
  STMT *_s;                        \
  if ((hstmt) == SQL_NULL_HSTMT) { \
    return SQL_INVALID_HANDLE;     \
  }                                \
  _s = (STMT *)(hstmt);            \
  if (_s->signature != _s) {       \
    return SQL_INVALID_HANDLE;     \
  }                                \
  HDBC_UNLOCK(_s->dbc)             \
}

#define VERINFO(maj, min, lev) ((maj) << 16 | (min) << 8 | (lev))

#define array_size(x) (sizeof (x) / sizeof (x[0]))

#define strmak(dst, src, max, lenp) { \
    SQLSMALLINT len = (SQLSMALLINT)strlen(src); \
    SQLSMALLINT cnt = (SQLSMALLINT)(min(len + 1, max)); \
    strncpy(dst, src, (size_t)cnt); \
    *lenp = (cnt > len) ? len : cnt; \
}

#endif