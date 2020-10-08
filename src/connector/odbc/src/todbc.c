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
#include "taoserror.h"

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

#define SET_ERROR(obj, sqlstate, eno, err_fmt, ...)                                                       \
do {                                                                                                      \
  obj->err.err_no = eno;                                                                                  \
  const char* estr = tstrerror(eno);                                                                      \
  if (!estr) estr = "Unknown error";                                                                      \
  int n = snprintf(NULL, 0, "%s: @[%d][TSDB:%x]" err_fmt "", estr, __LINE__, eno, ##__VA_ARGS__);         \
  if (n<0) break;                                                                                         \
  char *err_str = (char*)realloc(obj->err.err_str, n+1);                                                  \
  if (!err_str) break;                                                                                    \
  obj->err.err_str = err_str;                                                                             \
  snprintf(obj->err.err_str, n+1, "%s: @[%d][TSDB:%x]" err_fmt "", estr, __LINE__, eno, ##__VA_ARGS__);   \
  snprintf((char*)obj->err.sql_state, sizeof(obj->err.sql_state), "%s", sqlstate);                        \
} while (0)

#define CLR_ERROR(obj)                                                          \
do {                                                                            \
  obj->err.err_no = TSDB_CODE_SUCCESS;                                          \
  if (obj->err.err_str) obj->err.err_str[0] = '\0';                             \
  obj->err.sql_state[0] = '\0';                                                 \
} while (0)

#define FILL_ERROR(obj)                                                             \
do {                                                                                \
  size_t n = sizeof(obj->err.sql_state);                                            \
  if (Sqlstate) strncpy((char*)Sqlstate, (char*)obj->err.sql_state, n);             \
  if (NativeError) *NativeError = obj->err.err_no;                                  \
  snprintf((char*)MessageText, BufferLength, "%s", obj->err.err_str);               \
  if (TextLength && obj->err.err_str) *TextLength = strlen(obj->err.err_str);       \
} while (0)

#define FREE_ERROR(obj)                    \
do {                                       \
  obj->err.err_no = TSDB_CODE_SUCCESS;     \
  if (obj->err.err_str) {                  \
    free(obj->err.err_str);                \
    obj->err.err_str = NULL;               \
  }                                        \
  obj->err.sql_state[0] = '\0';            \
} while (0)

#define SDUP(s,n)      (s ? (s[n] ? (const char*)strndup((const char*)s,n) : (const char*)s) : strdup(""))
#define SFRE(x,s,n)               \
do {                              \
  if (x==(const char*)s) break;   \
  if (x) {                        \
    free((char*)x);               \
    x = NULL;                     \
  }                               \
} while (0)

#define CHK_RS(r_091c, sql_091c, fmt_091c, ...)               \
do {                                                          \
  r_091c = SQL_ERROR;                                         \
  int e = sql_091c->rs ? taos_errno(sql_091c->rs) : terrno;   \
  if (e != TSDB_CODE_SUCCESS) {                               \
    SET_ERROR(sql_091c, "HY000", e, fmt_091c, ##__VA_ARGS__); \
    break;                                                    \
  }                                                           \
  r_091c = SQL_SUCCESS;                                       \
} while (0)

typedef struct env_s             env_t;
typedef struct conn_s            conn_t;
typedef struct sql_s             sql_t;
typedef struct taos_error_s      taos_error_t;
typedef struct param_bind_s      param_bind_t;

struct param_bind_s {
  SQLUSMALLINT      ParameterNumber;
  SQLPOINTER        ParameterValue;
  SQLLEN           *StrLen_or_Ind;
  unsigned int      valid;
};

struct taos_error_s {
  char            *err_str;
  int              err_no;

  SQLCHAR          sql_state[6];
};

struct env_s {
  uint64_t                refcount;
  unsigned int            destroying:1;

  taos_error_t            err;
};

struct conn_s {
  uint64_t                refcount;
  env_t                  *env;

  TAOS                   *taos;

  taos_error_t            err;
};

struct sql_s {
  uint64_t                refcount;
  conn_t                 *conn;

  TAOS_STMT              *stmt;
  TAOS_BIND              *binds;
  param_bind_t           *params;
  int                     n_params;
  TAOS_RES               *rs;
  TAOS_ROW                row;

  taos_error_t            err;
};

static pthread_once_t          init_once         = PTHREAD_ONCE_INIT;
static void init_routine(void);

static int do_field_display_size(TAOS_FIELD *field);
static void do_convert(SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind, TAOS_FIELD *field, void *row);

SQLRETURN  SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle) {
  pthread_once(&init_once, init_routine);

  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return SQL_ERROR;

  DASSERT(INC_REF(env)>0);

  *EnvironmentHandle = env;

  CLR_ERROR(env);
  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle) {
  env_t *env = (env_t*)EnvironmentHandle;
  if (!env) return SQL_ERROR;

  DASSERT(GET_REF(env)==1);

  DASSERT(!env->destroying);

  env->destroying = 1;
  DASSERT(env->destroying == 1);

  DASSERT(DEC_REF(env)==0);

  FREE_ERROR(env);
  free(env);

  return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle,
                                   SQLHDBC *ConnectionHandle) {
  env_t *env = (env_t*)EnvironmentHandle;
  if (!env) return SQL_ERROR;

  DASSERT(INC_REF(env)>1);

  conn_t *conn = NULL;
  do {
    conn = (conn_t*)calloc(1, sizeof(*conn));
    if (!conn) {
      SET_ERROR(env, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to alloc connection handle");
      break;
    }

    conn->env = env;
    *ConnectionHandle = conn;

    DASSERT(INC_REF(conn)>0);

    return SQL_SUCCESS;
  } while (0);

  DASSERT(DEC_REF(env)>0);

  return SQL_ERROR;
}

SQLRETURN  SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle) {
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  DASSERT(GET_REF(conn)==1);

  DASSERT(conn->env);

  do {
    if (conn->taos) {
      taos_close(conn->taos);
      conn->taos = NULL;
    }

    DASSERT(DEC_REF(conn->env)>0);
    DASSERT(DEC_REF(conn)==0);

    conn->env = NULL;
    FREE_ERROR(conn);
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

  if (conn->taos) {
    SET_ERROR(conn, "HY000", TSDB_CODE_TSC_APP_ERROR, "connection still in use");
    return SQL_ERROR;
  }

  const char *serverName = SDUP(ServerName,     NameLength1);
  const char *userName   = SDUP(UserName,       NameLength2);
  const char *auth       = SDUP(Authentication, NameLength3);

  do {
    if (!serverName || !userName || !auth) {
      SET_ERROR(conn, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to connect to database");
      break;
    }
    // TODO: data-race
    // TODO: shall receive ip/port from odbc.ini
    conn->taos = taos_connect("localhost", userName, auth, NULL, 0);
    if (!conn->taos) {
      SET_ERROR(conn, "HY000", terrno, "failed to connect to database");
      break;
    }
  } while (0);

  SFRE(serverName, ServerName,     NameLength1);
  SFRE(userName,   UserName,       NameLength2);
  SFRE(auth,       Authentication, NameLength3);

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

  DASSERT(INC_REF(conn)>1);

  do {
    sql_t *sql = (sql_t*)calloc(1, sizeof(*sql));
    if (!sql) {
      SET_ERROR(conn, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to alloc statement handle");
      break;
    }

    sql->conn = conn;
    DASSERT(INC_REF(sql)>0);

    *StatementHandle = sql;

    return SQL_SUCCESS;
  } while (0);

  DASSERT(DEC_REF(conn)>0);

  return SQL_ERROR;
}

SQLRETURN  SQL_API SQLFreeStmt(SQLHSTMT StatementHandle,
                               SQLUSMALLINT Option) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  if (Option != SQL_DROP) {
    SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "failed to free statement");
    return SQL_ERROR;
  }

  DASSERT(GET_REF(sql)==1);

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
  }

  if (sql->stmt) {
    taos_stmt_close(sql->stmt);
    sql->stmt = NULL;
  }

  if (sql->binds) {
    free(sql->binds);
    sql->binds = NULL;
  }
  if (sql->params) {
    free(sql->params);
    sql->params = NULL;
  }
  sql->n_params = 0;


  DASSERT(DEC_REF(sql->conn)>0);
  DASSERT(DEC_REF(sql)==0);

  sql->conn = NULL;

  FREE_ERROR(sql);
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

  if (sql->stmt) {
    taos_stmt_close(sql->stmt);
    sql->stmt = NULL;
  }

  if (sql->binds) {
    free(sql->binds);
    sql->binds = NULL;
  }

  if (sql->params) {
    free(sql->params);
    sql->params = NULL;
  }
  sql->n_params = 0;

  const char *stxt = SDUP(StatementText, TextLength);

  SQLRETURN r = SQL_ERROR;
  do {
    if (!stxt) {
      SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to query");
      break;
    }
    sql->rs = taos_query(sql->conn->taos, stxt);
    CHK_RS(r, sql, "failed to query");
  } while (0);

  SFRE(stxt, StatementText, TextLength);

  return r;
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
      if (sql->row[ColumnNumber-1]) {
        do_convert(TargetValue, BufferLength, StrLen_or_Ind, field, sql->row[ColumnNumber-1]);
        *StrLen_or_Ind = SQL_NTS;
      } else {
        *StrLen_or_Ind = SQL_NULL_DATA;
      }
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
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
    sql->row = NULL;
  }

  if (sql->stmt) {
    taos_stmt_close(sql->stmt);
    sql->stmt = NULL;
  }

  if (sql->binds) {
    free(sql->binds);
    sql->binds = NULL;
  }
  if (sql->params) {
    free(sql->params);
    sql->params = NULL;
  }
  sql->n_params = 0;

  do {
    sql->stmt = taos_stmt_init(sql->conn->taos);
    if (!sql->stmt) {
      SET_ERROR(sql, "HY000", terrno, "failed to initialize statement internally");
      break;
    }

    int r = taos_stmt_prepare(sql->stmt, (const char *)StatementText, TextLength);
    if (r) {
      SET_ERROR(sql, "HY000", r, "failed to prepare a statement");
      taos_stmt_close(sql->stmt);
      sql->stmt = NULL;
      break;
    }
  } while (0);

  return sql->stmt ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN  SQL_API SQLExecute(SQLHSTMT StatementHandle) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->stmt) return SQL_ERROR;

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
    sql->row = NULL;
  }

  int r = 0;

  for (int i=0; i<sql->n_params; ++i) {
    param_bind_t *pb = sql->params + i;
    if (!pb->valid) {
      SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "default parameter [@%d] not supported yet", i+1);
      return SQL_ERROR;
    }
    TAOS_BIND *b = sql->binds + i;
    int yes = 1;
    int no  = 0;
    if (pb->StrLen_or_Ind && *pb->StrLen_or_Ind == SQL_NULL_DATA) {
      b->is_null = &yes;
    } else {
      b->is_null = &no;
      switch (b->buffer_type) {
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_DOUBLE:
        case TSDB_DATA_TYPE_TIMESTAMP: {
          b->length = &b->buffer_length;
          b->buffer = pb->ParameterValue;
        } break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
          if (!pb->StrLen_or_Ind) {
            SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "value [@%d] bad StrLen_or_Ind", i+1);
            return SQL_ERROR;
          }
          size_t n = *pb->StrLen_or_Ind;
          if (n == SQL_NTS) {
            n = strlen(pb->ParameterValue);
          } else if (n < 0 || n > b->buffer_length) {
            SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "value [@%d] bad StrLen_or_Ind", i+1);
            return SQL_ERROR;
          }

          b->buffer_length = n;
          b->length = &b->buffer_length;
          b->buffer = pb->ParameterValue;
        } break;
        default: {
          SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "value [@%d] not supported yet", i+1);
          return SQL_ERROR;
        } break;
      }
    }
  }

  if (sql->n_params > 0) {
    r = taos_stmt_bind_param(sql->stmt, sql->binds);
    if (r) {
      SET_ERROR(sql, "HY000", r, "failed to bind parameters");
      return SQL_ERROR;
    }

    r = taos_stmt_add_batch(sql->stmt);
    if (r) {
      SET_ERROR(sql, "HY000", r, "failed to add batch");
      return SQL_ERROR;
    }
  }

  r = taos_stmt_execute(sql->stmt);
  if (r) {
    SET_ERROR(sql, "HY000", r, "failed to execute statement");
    return SQL_ERROR;
  }

  SQLRETURN ret = SQL_ERROR;

  sql->rs = taos_stmt_use_result(sql->stmt);
  CHK_RS(ret, sql, "failed to use result");

  return ret;
}

SQLRETURN  SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                                   SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLength) {
  // if this function is not exported, isql will never call SQLGetDiagRec
  return SQL_ERROR;
}

SQLRETURN  SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                 SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                 SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                 SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
  if (RecNumber>1) return SQL_NO_DATA;
  switch (HandleType) {
    case SQL_HANDLE_ENV: {
      env_t *env = (env_t*)Handle;
      if (!env) break;
      FILL_ERROR(env);
      return SQL_SUCCESS;
    } break;
    case SQL_HANDLE_DBC: {
      conn_t *conn = (conn_t*)Handle;
      if (!conn) break;
      FILL_ERROR(conn);
      return SQL_SUCCESS;
    } break;
    case SQL_HANDLE_STMT: {
      sql_t *sql = (sql_t*)Handle;
      if (!sql) break;
      FILL_ERROR(sql);
      return SQL_SUCCESS;
    } break;
    default: {
    } break;
  }

  return SQL_ERROR;
}

SQLRETURN SQL_API SQLBindParameter(
    SQLHSTMT           StatementHandle,
    SQLUSMALLINT       ParameterNumber,
    SQLSMALLINT        fParamType,
    SQLSMALLINT        ValueType,
    SQLSMALLINT        ParameterType,
    SQLULEN            LengthPrecision,
    SQLSMALLINT        ParameterScale,
    SQLPOINTER         ParameterValue,
    SQLLEN             cbValueMax, // ignore for now, since only SQL_PARAM_INPUT is supported now
    SQLLEN 		      *StrLen_or_Ind) {
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;
  if (!sql->conn) return SQL_ERROR;
  if (!sql->conn->taos) return SQL_ERROR;
  if (!sql->stmt) return SQL_ERROR;

  if (fParamType != SQL_PARAM_INPUT) {
    SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "non-input parameter [@%d] not supported yet", ParameterNumber);
    return SQL_ERROR;
  }
  switch (ParameterType) {
    case SQL_BIT: { // TSDB_DATA_TYPE_BOOL
      if (ValueType!=SQL_C_BIT) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_BOOL;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_TINYINT: { // TSDB_DATA_TYPE_TINYINT
      if (ValueType!=SQL_C_TINYINT) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_TINYINT;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_SMALLINT: { // TSDB_DATA_TYPE_SMALLINT
      if (ValueType!=SQL_C_SHORT) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_SMALLINT;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_INTEGER: { // TSDB_DATA_TYPE_INT
      if (ValueType!=SQL_C_LONG) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_INT;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_BIGINT: { // TSDB_DATA_TYPE_BIGINT
      if (ValueType!=SQL_C_SBIGINT) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_BIGINT;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_FLOAT: { // TSDB_DATA_TYPE_FLOAT
      if (ValueType!=SQL_C_FLOAT) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_FLOAT;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_DOUBLE: { // TSDB_DATA_TYPE_DOUBLE
      if (ValueType!=SQL_C_DOUBLE) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_DOUBLE;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_TIMESTAMP: { // TSDB_DATA_TYPE_TIMESTAMP
      if (ValueType!=SQL_C_SBIGINT) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      // LengthPrecision ignored;
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_TIMESTAMP;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_VARBINARY: { // TSDB_DATA_TYPE_BINARY
      if (ValueType!=SQL_C_BINARY) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      if (LengthPrecision <=0) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching length precision", ParameterNumber);
        return SQL_ERROR;
      }
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_BINARY;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    case SQL_VARCHAR: { // TSDB_DATA_TYPE_NCHAR
      if (ValueType!=SQL_C_CHAR) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching value type", ParameterNumber);
        return SQL_ERROR;
      }
      if (LengthPrecision <=0) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "parameter [@%d] not matching length precision", ParameterNumber);
        return SQL_ERROR;
      }
      // ParameterScale ignored;
      param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
      TAOS_BIND *binds = (TAOS_BIND*)(sql->n_params>=ParameterNumber ? sql->binds : realloc(sql->binds, ParameterNumber * sizeof(*binds)));
      if (!ar || !binds) {
        SET_ERROR(sql, "HY000", TSDB_CODE_COM_OUT_OF_MEMORY, "failed to bind parameter [@%d]", ParameterNumber);
        if (ar) sql->params = ar;
        if (binds) sql->binds = binds;
        return SQL_ERROR;
      }
      sql->params = ar;
      sql->binds = binds;
      if (sql->n_params<ParameterNumber) {
        sql->n_params = ParameterNumber;
      }
      param_bind_t *pb = ar + ParameterNumber - 1;
      TAOS_BIND *b = binds + ParameterNumber - 1;
      b->buffer_type   = TSDB_DATA_TYPE_NCHAR;
      b->buffer_length = LengthPrecision;
      b->buffer        = NULL;
      b->length        = NULL;
      b->is_null       = NULL;
      b->is_unsigned   = 0;
      b->error         = NULL;
      pb->ParameterValue   = ParameterValue;
      pb->StrLen_or_Ind    = StrLen_or_Ind;
      pb->valid            = 1;
    } break;
    default: {
      SET_ERROR(sql, "HY000", TSDB_CODE_COM_OPS_NOT_SUPPORT, "xdoes not support parameter type[%x]", ParameterType);
      return SQL_ERROR;
		} break;
  }
  return SQL_SUCCESS;
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

