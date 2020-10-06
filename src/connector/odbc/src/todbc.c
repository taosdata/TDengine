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

#include "taos.h"

#include "os.h"

#include <sql.h>
#include <sqlext.h>

#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define D(fmt, ...)                                              \
  fprintf(stderr,                                                \
          "%s[%d]:%s() " fmt "\n",                               \
          basename((char*)__FILE__), __LINE__, __func__,         \
          ##__VA_ARGS__)

#define DASSERT(statement)                                       \
do {                                                             \
  if (statement) break;                                          \
  D("Assertion failure: %s", #statement);                        \
  abort();                                                       \
} while (0)

#define GET_REF(obj) atomic_load_64(&obj->refcount)
#define INC_REF(obj) atomic_add_fetch_64(&obj->refcount, 1)
#define DEC_REF(obj) atomic_sub_fetch_64(&obj->refcount, 1)

#define LOCK(obj)   pthread_mutex_lock(&obj->lock);
#define UNLOCK(obj) pthread_mutex_unlock(&obj->lock);


typedef struct env_s             env_t;
typedef struct conn_s            conn_t;
typedef struct sql_s             sql_t;



struct env_s {
  unsigned int            destroying:1;
};

struct conn_s {
  env_t                  *env;

  TAOS                   *taos;
};

struct sql_s {
  conn_t                 *conn;

  TAOS_RES               *rs;
  TAOS_ROW                row;
};

static pthread_once_t          init_once         = PTHREAD_ONCE_INIT;
static void init_routine(void);

static int do_field_display_size(TAOS_FIELD *field);
static void do_convert(SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind, TAOS_FIELD *field, void *row);

SQLRETURN  SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle) {
  pthread_once(&init_once, init_routine);

  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return SQL_ERROR;

  *EnvironmentHandle = env;

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle) {
  env_t *env = (env_t*)EnvironmentHandle;
  if (!env) return SQL_ERROR;
  
  DASSERT(!env->destroying);

  env->destroying = 1;
  DASSERT(env->destroying == 1);

  free(env);

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle,
                                   SQLHDBC *ConnectionHandle) {
  env_t *env = (env_t*)EnvironmentHandle;
  if (!env) return SQL_ERROR;

  conn_t *conn = NULL;
  do {
    conn = (conn_t*)calloc(1, sizeof(*conn));
    if (!conn) break;

    conn->env = env;
    *ConnectionHandle = conn;
  } while (0);

  return conn ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN  SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle) {
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  do {
    if (conn->taos) {
      taos_close(conn->taos);
      conn->taos = NULL;
    }
    conn->env = NULL;
    free(conn);
  } while (0);

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLConnect(SQLHDBC ConnectionHandle,
                              SQLCHAR *ServerName, SQLSMALLINT NameLength1,
                              SQLCHAR *UserName, SQLSMALLINT NameLength2,
                                  SQLCHAR *Authentication, SQLSMALLINT NameLength3) {
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;
  
  if (conn->taos) return SQL_ERROR;

  conn->taos = taos_connect("localhost", (const char*)UserName, (const char*)Authentication, NULL, 0);
    
  return conn->taos ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN  SQL_API SQLDisconnect(SQLHDBC ConnectionHandle) {
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  if (conn->taos) {
    taos_close(conn->taos);
    conn->taos = NULL;
  }

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle,
                                SQLHSTMT *StatementHandle) {
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  sql_t *sql = (sql_t*)calloc(1, sizeof(*sql));
  if (!sql) return SQL_ERROR;

  sql->conn = conn;
  *StatementHandle = sql;

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFreeStmt(SQLHSTMT StatementHandle,
                               SQLUSMALLINT Option) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
  }
  sql->conn = NULL;
  free(sql);

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                 SQLCHAR *StatementText, SQLINTEGER TextLength) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
    sql->row = NULL;
  }
  sql->rs = taos_query(sql->conn->taos, (const char*)StatementText);
  if (sql->rs) {
    sql->row = taos_fetch_row(sql->rs);
  }

  return sql->row ? SQL_SUCCESS : SQL_NO_DATA;
}

SQLRETURN  SQL_API SQLNumResultCols(SQLHSTMT StatementHandle,
                                    SQLSMALLINT *ColumnCount) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->rs) return SQL_ERROR;

  int fields = taos_field_count(sql->rs);
  if (ColumnCount) {
    *ColumnCount = fields;
  }

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLRowCount(SQLHSTMT StatementHandle,
                               SQLLEN *RowCount) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->rs) return SQL_ERROR;
  int rows = taos_affected_rows(sql->rs);
  if (RowCount) {
    *RowCount = rows;
  }
  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLColAttribute(SQLHSTMT StatementHandle,
                                    SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
                                    SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength,
                                    SQLSMALLINT *StringLength, SQLLEN *NumericAttribute ) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->rs) return SQL_ERROR;
  int nfields = taos_field_count(sql->rs);
  TAOS_FIELD *fields = taos_fetch_fields(sql->rs);

  if (nfields==0 || fields==NULL) return SQL_ERROR;
  if (ColumnNumber>nfields) return SQL_ERROR;

  TAOS_FIELD *field = fields + ColumnNumber-1;

  switch (FieldIdentifier) {
    case SQL_COLUMN_DISPLAY_SIZE: {
      *NumericAttribute = do_field_display_size(field);
    } break;
    case SQL_COLUMN_LABEL: {
      strncpy(CharacterAttribute, field->name, field->bytes);
    } break;
    default: {
      return SQL_ERROR;
    } break;
  }
  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLGetData(SQLHSTMT StatementHandle,
                              SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                              SQLPOINTER TargetValue, SQLLEN BufferLength,
                              SQLLEN *StrLen_or_Ind) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->rs) return SQL_ERROR;
  if (!sql->row) return SQL_ERROR;
  int nfields = taos_field_count(sql->rs);
  TAOS_FIELD *fields = taos_fetch_fields(sql->rs);

  if (nfields==0 || fields==NULL) return SQL_ERROR;
  if (ColumnNumber>nfields) return SQL_ERROR;

  TAOS_FIELD *field = fields + ColumnNumber-1;

  switch (TargetType) {
    case SQL_CHAR: {
      do_convert(TargetValue, BufferLength, StrLen_or_Ind, field, sql->row[ColumnNumber-1]);
      *StrLen_or_Ind = SQL_NTS;
    } break;
    default: {
      return SQL_ERROR;
    } break;
  }
  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFetch(SQLHSTMT StatementHandle) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->rs) return SQL_ERROR;
  sql->row = taos_fetch_row(sql->rs);
  return sql->row ? SQL_SUCCESS : SQL_NO_DATA;
}

SQLRETURN  SQL_API SQLPrepare(SQLHSTMT StatementHandle,
                              SQLCHAR *StatementText, SQLINTEGER TextLength) {
  return SQL_ERROR;
}

SQLRETURN  SQL_API SQLExecute(SQLHSTMT StatementHandle) {
  return SQL_ERROR;
}

static void init_routine(void) {
  taos_init();
}

static int do_field_display_size(TAOS_FIELD *field) {
  switch (field->type) {
    case TSDB_DATA_TYPE_TINYINT:
      return 5;
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      return 7;
      break;

    case TSDB_DATA_TYPE_INT:
      return 12;
      break;

    case TSDB_DATA_TYPE_BIGINT:
      return 22;
      break;

    case TSDB_DATA_TYPE_FLOAT: {
      return 12;
    } break;

    case TSDB_DATA_TYPE_DOUBLE: {
      return 12;
    } break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR: {
      return 3*(field->bytes - VARSTR_HEADER_SIZE) + 2;
    } break;

    case TSDB_DATA_TYPE_TIMESTAMP:
      return 22;
      break;

    case TSDB_DATA_TYPE_BOOL:
      return 7;
    default:
      break;
  }

  return 10;
}

static void do_convert(SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind, TAOS_FIELD *field, void *row) {
  switch (field->type) {
    case TSDB_DATA_TYPE_TINYINT:
      snprintf((char*)TargetValue, BufferLength, "%d", *((int8_t *)row));
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      snprintf((char*)TargetValue, BufferLength, "%d", *((int16_t *)row));
      break;

    case TSDB_DATA_TYPE_INT:
      snprintf((char*)TargetValue, BufferLength, "%d", *((int32_t *)row));
      break;

    case TSDB_DATA_TYPE_BIGINT:
      snprintf((char*)TargetValue, BufferLength, "%" PRId64, *((int64_t *)row));
      break;

    case TSDB_DATA_TYPE_FLOAT: {
      float fv = 0;
      fv = GET_FLOAT_VAL(row);
      snprintf((char*)TargetValue, BufferLength, "%f", fv);
    } break;

    case TSDB_DATA_TYPE_DOUBLE: {
      double dv = 0;
      dv = GET_DOUBLE_VAL(row);
      snprintf((char*)TargetValue, BufferLength, "%lf", dv);
    } break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR: {
      size_t xlen = 0;
      char *p = (char*)TargetValue;
      size_t n = BufferLength;
      for (xlen = 0; xlen < field->bytes - VARSTR_HEADER_SIZE; xlen++) {
        char c = ((char *)row)[xlen];
        if (c == 0) break;
        int v = snprintf(p, n, "%c", c);
        p += v;
        n -= v;
        if (n<=0) break;
      }
      if (n>0) *p = '\0';
      ((char*)TargetValue)[BufferLength-1] = '\0';
    } break;

    case TSDB_DATA_TYPE_TIMESTAMP:
      snprintf((char*)TargetValue, BufferLength, "%" PRId64, *((int64_t *)row));
      break;

    case TSDB_DATA_TYPE_BOOL:
      snprintf((char*)TargetValue, BufferLength, "%d", *((int8_t *)row));
    default:
      break;
  }
}

