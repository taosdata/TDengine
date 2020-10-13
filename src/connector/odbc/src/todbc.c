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

// #define _BSD_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "taos.h"

#include "os.h"
#include "taoserror.h"
#include "todbc_util.h"

#include <sql.h>
#include <sqlext.h>

#include <time.h>


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
  if (TextLength && obj->err.err_str) *TextLength = utf8_chars(obj->err.err_str);   \
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

#define SET_UNSUPPORT_ERROR(obj, sqlstate, err_fmt, ...)                             \
do {                                                                                 \
  SET_ERROR(obj, sqlstate, TSDB_CODE_ODBC_NOT_SUPPORT, err_fmt, ##__VA_ARGS__);      \
} while (0)                                                                          \

#define SET_HANDLE_INVALID(obj, sqlstate, err_fmt, ...)                              \
do {                                                                                 \
  SET_ERROR(obj, sqlstate, TSDB_CODE_QRY_INVALID_QHANDLE, err_fmt, ##__VA_ARGS__);   \
} while (0);

#define SDUP(s,n)      (s ? (s[n] ? (const char*)strndup((const char*)s,n) : (const char*)s) : strdup(""))
#define SFRE(x,s,n)               \
do {                              \
  if (x==(const char*)s) break;   \
  if (x) {                        \
    free((char*)x);               \
    x = NULL;                     \
  }                               \
} while (0)

#define CHK_CONN(obj)                                                                                 \
do {                                                                                                  \
  if (!obj->conn) {                                                                                   \
    SET_ERROR(obj, "HY000", TSDB_CODE_ODBC_INVALID_HANDLE, "connection closed or not ready");         \
    return SQL_ERROR;                                                                                 \
  }                                                                                                   \
} while (0);

#define CHK_CONN_TAOS(obj)                                                                                     \
do {                                                                                                           \
  if (!obj->conn->taos) {                                                                                      \
    SET_ERROR(obj, "HY000", TSDB_CODE_ODBC_INVALID_HANDLE, "connection to data source closed or not ready");   \
    return SQL_ERROR;                                                                                          \
  }                                                                                                            \
} while (0);

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

#define PROFILING 0

#define PROFILE(statement)                            \
do {                                                  \
  if (!PROFILING) {                                   \
    statement;                                        \
    break;                                            \
  }                                                   \
  struct timeval tv0, tv1;                            \
  gettimeofday(&tv0, NULL);                           \
  statement;                                          \
  gettimeofday(&tv1, NULL);                           \
  double delta = difftime(tv1.tv_sec, tv0.tv_sec);    \
  delta *= 1000000;                                   \
  delta += (tv1.tv_usec-tv0.tv_usec);                 \
  delta /= 1000000;                                   \
  D("%s: elapsed: [%.6f]s", #statement, delta);       \
} while (0)

typedef struct env_s             env_t;
typedef struct conn_s            conn_t;
typedef struct sql_s             sql_t;
typedef struct taos_error_s      taos_error_t;
typedef struct param_bind_s      param_bind_t;

struct param_bind_s {
  SQLUSMALLINT      ParameterNumber;
  SQLSMALLINT       ValueType;
  SQLSMALLINT       ParameterType;
  SQLULEN           LengthPrecision;
  SQLSMALLINT       ParameterScale;
  SQLPOINTER        ParameterValue;
  SQLLEN 		       *StrLen_or_Ind;

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
  param_bind_t           *params;
  int                     n_params;
  TAOS_RES               *rs;
  TAOS_ROW                row;

  taos_error_t            err;
};

typedef struct c_target_s           c_target_t;
struct c_target_s {
  SQLUSMALLINT      col;
  SQLSMALLINT       ct;    // c type: SQL_C_XXX
  char             *ptr;
  SQLLEN            len;
  SQLLEN           *soi;
};

static pthread_once_t          init_once         = PTHREAD_ONCE_INIT;
static void init_routine(void);



static int do_field_display_size(TAOS_FIELD *field);

static SQLRETURN doSQLAllocEnv(SQLHENV *EnvironmentHandle)
{
  pthread_once(&init_once, init_routine);

  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return SQL_ERROR;

  DASSERT(INC_REF(env)>0);

  *EnvironmentHandle = env;

  CLR_ERROR(env);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle)
{
  SQLRETURN r;
  r = doSQLAllocEnv(EnvironmentHandle);
  return r;
}

static SQLRETURN doSQLFreeEnv(SQLHENV EnvironmentHandle)
{
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

SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle)
{
  SQLRETURN r;
  r = doSQLFreeEnv(EnvironmentHandle);
  return r;
}

static SQLRETURN doSQLAllocConnect(SQLHENV EnvironmentHandle,
                                   SQLHDBC *ConnectionHandle)
{
  env_t *env = (env_t*)EnvironmentHandle;
  if (!env) return SQL_ERROR;

  DASSERT(INC_REF(env)>1);

  conn_t *conn = NULL;
  do {
    conn = (conn_t*)calloc(1, sizeof(*conn));
    if (!conn) {
      SET_ERROR(env, "HY001", TSDB_CODE_ODBC_OOM, "");
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

SQLRETURN SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle,
                                  SQLHDBC *ConnectionHandle)
{
  SQLRETURN r;
  r = doSQLAllocConnect(EnvironmentHandle, ConnectionHandle);
  return r;
}

static SQLRETURN doSQLFreeConnect(SQLHDBC ConnectionHandle)
{
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

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle)
{
  SQLRETURN r;
  r = doSQLFreeConnect(ConnectionHandle);
  return r;
}

static SQLRETURN doSQLConnect(SQLHDBC ConnectionHandle,
                              SQLCHAR *ServerName, SQLSMALLINT NameLength1,
                              SQLCHAR *UserName, SQLSMALLINT NameLength2,
                              SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  if (conn->taos) {
    SET_ERROR(conn, "08002", TSDB_CODE_ODBC_CONNECTION_BUSY, "connection still in use");
    return SQL_ERROR;
  }

  const char *serverName = SDUP(ServerName,     NameLength1);
  const char *userName   = SDUP(UserName,       NameLength2);
  const char *auth       = SDUP(Authentication, NameLength3);

  do {
    if ((ServerName && !serverName) || (UserName && !userName) || (Authentication && !auth)) {
      SET_ERROR(conn, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }

    // TODO: data-race
    // TODO: shall receive ip/port from odbc.ini
    conn->taos = taos_connect("localhost", userName, auth, NULL, 0);
    if (!conn->taos) {
      SET_ERROR(conn, "08001", terrno, "failed to connect to data source");
      break;
    }
  } while (0);

  SFRE(serverName, ServerName,     NameLength1);
  SFRE(userName,   UserName,       NameLength2);
  SFRE(auth,       Authentication, NameLength3);

  return conn->taos ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLConnect(SQLHDBC ConnectionHandle,
                             SQLCHAR *ServerName, SQLSMALLINT NameLength1,
                             SQLCHAR *UserName, SQLSMALLINT NameLength2,
                             SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
  SQLRETURN r;
  r = doSQLConnect(ConnectionHandle, ServerName, NameLength1,
                   UserName, NameLength2,
                   Authentication, NameLength3);
  return r;
}

static SQLRETURN doSQLDisconnect(SQLHDBC ConnectionHandle)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  if (conn->taos) {
    taos_close(conn->taos);
    conn->taos = NULL;
  }

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle)
{
  SQLRETURN r;
  r = doSQLDisconnect(ConnectionHandle);
  return r;
}

static SQLRETURN doSQLAllocStmt(SQLHDBC ConnectionHandle,
                              SQLHSTMT *StatementHandle)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  DASSERT(INC_REF(conn)>1);

  do {
    sql_t *sql = (sql_t*)calloc(1, sizeof(*sql));
    if (!sql) {
      SET_ERROR(conn, "HY001", TSDB_CODE_ODBC_OOM, "");
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

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle,
                               SQLHSTMT *StatementHandle)
{
  SQLRETURN r;
  r = doSQLAllocStmt(ConnectionHandle, StatementHandle);
  return r;
}

static SQLRETURN doSQLFreeStmt(SQLHSTMT StatementHandle,
                              SQLUSMALLINT Option)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  if (Option == SQL_CLOSE) return SQL_SUCCESS;
  if (Option != SQL_DROP) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "free statement with Option[%x] not supported yet", Option);
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

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle,
                              SQLUSMALLINT Option)
{
  SQLRETURN r;
  r = doSQLFreeStmt(StatementHandle, Option);
  return r;
}

static SQLRETURN doSQLExecDirect(SQLHSTMT StatementHandle,
                                 SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
    sql->row = NULL;
  }

  if (sql->stmt) {
    taos_stmt_close(sql->stmt);
    sql->stmt = NULL;
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
      SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }
    sql->rs = taos_query(sql->conn->taos, stxt);
    CHK_RS(r, sql, "failed to execute");
  } while (0);

  SFRE(stxt, StatementText, TextLength);

  return r;
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  SQLRETURN r;
  r = doSQLExecDirect(StatementHandle, StatementText, TextLength);
  return r;
}

SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT hstmt, SQLWCHAR *szSqlStr, SQLINTEGER cbSqlStr)
{
  size_t bytes = 0;
  SQLCHAR *utf8 = wchars_to_chars(szSqlStr, cbSqlStr, &bytes);
  return SQLExecDirect(hstmt, utf8, bytes);
}

static SQLRETURN doSQLNumResultCols(SQLHSTMT StatementHandle,
                                    SQLSMALLINT *ColumnCount)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  int fields = taos_field_count(sql->rs);
  if (ColumnCount) {
    *ColumnCount = fields;
  }

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT StatementHandle,
                                   SQLSMALLINT *ColumnCount)
{
  SQLRETURN r;
  r = doSQLNumResultCols(StatementHandle, ColumnCount);
  return r;
}

static SQLRETURN doSQLRowCount(SQLHSTMT StatementHandle,
                               SQLLEN *RowCount)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  int rows = taos_affected_rows(sql->rs);
  if (RowCount) {
    *RowCount = rows;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT StatementHandle,
                              SQLLEN *RowCount)
{
  SQLRETURN r;
  r = doSQLRowCount(StatementHandle, RowCount);
  return r;
}

static SQLRETURN doSQLColAttribute(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
                                   SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLength, SQLLEN *NumericAttribute )
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  int nfields = taos_field_count(sql->rs);
  TAOS_FIELD *fields = taos_fetch_fields(sql->rs);

  if (nfields==0 || fields==NULL) {
    SET_ERROR(sql, "07005", TSDB_CODE_ODBC_NO_FIELDS, "");
    return SQL_ERROR;
  }

  if (ColumnNumber<=0 || ColumnNumber>nfields) {
    SET_ERROR(sql, "07009", TSDB_CODE_ODBC_OUT_OF_RANGE, "invalid column number [%d]", ColumnNumber);
    return SQL_ERROR;
  }

  TAOS_FIELD *field = fields + ColumnNumber-1;

  switch (FieldIdentifier) {
    case SQL_COLUMN_DISPLAY_SIZE: {
      *NumericAttribute = do_field_display_size(field);
    } break;
    case SQL_COLUMN_LABEL: {
      size_t n = sizeof(field->name);
      strncpy(CharacterAttribute, field->name, (n>BufferLength ? BufferLength : n));
    } break;
    case SQL_COLUMN_UNSIGNED: {
      *NumericAttribute = SQL_FALSE;
    } break;
    default: {
      SET_ERROR(sql, "HY091", TSDB_CODE_ODBC_OUT_OF_RANGE,
                "FieldIdentifier[%d/0x%x] for Column [%d] not supported yet",
                FieldIdentifier, FieldIdentifier, ColumnNumber);
      return SQL_ERROR;
    } break;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
                                  SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength,
                                  SQLSMALLINT *StringLength, SQLLEN *NumericAttribute )
{
  SQLRETURN r;
  r = doSQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier,
                        CharacterAttribute, BufferLength,
                        StringLength, NumericAttribute);
  return r;
}

static SQLRETURN conv_tsdb_bool_to_c_bit(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_tinyint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_short(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_bool_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b);
static SQLRETURN conv_tsdb_v1_to_c_tinyint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_short(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v1_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1);
static SQLRETURN conv_tsdb_v2_to_c_short(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v2_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v2_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v2_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v2_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v2_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v2_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2);
static SQLRETURN conv_tsdb_v4_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4);
static SQLRETURN conv_tsdb_v4_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4);
static SQLRETURN conv_tsdb_v4_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4);
static SQLRETURN conv_tsdb_v4_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4);
static SQLRETURN conv_tsdb_v4_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4);
static SQLRETURN conv_tsdb_v4_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4);
static SQLRETURN conv_tsdb_v8_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8);
static SQLRETURN conv_tsdb_v8_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8);
static SQLRETURN conv_tsdb_v8_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8);
static SQLRETURN conv_tsdb_v8_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8);
static SQLRETURN conv_tsdb_v8_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8);
static SQLRETURN conv_tsdb_f4_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4);
static SQLRETURN conv_tsdb_f4_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4);
static SQLRETURN conv_tsdb_f4_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4);
static SQLRETURN conv_tsdb_f4_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4);
static SQLRETURN conv_tsdb_f8_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, double f8);
static SQLRETURN conv_tsdb_f8_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, double f8);
static SQLRETURN conv_tsdb_f8_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, double f8);
static SQLRETURN conv_tsdb_ts_to_c_v8(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts);
static SQLRETURN conv_tsdb_ts_to_c_str(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts);
static SQLRETURN conv_tsdb_ts_to_c_bin(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts);
static SQLRETURN conv_tsdb_ts_to_c_ts(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts);
static SQLRETURN conv_tsdb_bin_to_c_str(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const unsigned char *bin);
static SQLRETURN conv_tsdb_bin_to_c_bin(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const unsigned char *bin);
static SQLRETURN conv_tsdb_str_to_c_bit(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_v1(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_v2(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_v4(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_v8(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_f4(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_f8(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_str(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);
static SQLRETURN conv_tsdb_str_to_c_bin(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str);

static SQLRETURN doSQLGetData(SQLHSTMT StatementHandle,
                              SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                              SQLPOINTER TargetValue, SQLLEN BufferLength,
                              SQLLEN *StrLen_or_Ind)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  if (!sql->row) {
    SET_ERROR(sql, "24000", TSDB_CODE_ODBC_INVALID_CURSOR, "");
    return SQL_ERROR;
  }

  DASSERT(TargetValue);

  int nfields = taos_field_count(sql->rs);
  TAOS_FIELD *fields = taos_fetch_fields(sql->rs);

  if (ColumnNumber<=0 || ColumnNumber>nfields) {
    SET_ERROR(sql, "07009", TSDB_CODE_ODBC_OUT_OF_RANGE, "invalid column number [%d]", ColumnNumber);
    return SQL_ERROR;
  }

  TAOS_FIELD *field = fields + ColumnNumber-1;
  void *row = sql->row[ColumnNumber-1];

  if (!row) {
    if (StrLen_or_Ind) {
      *StrLen_or_Ind = SQL_NULL_DATA;
    }
    return SQL_SUCCESS;
  }

  c_target_t target = {0};
  target.col       = ColumnNumber;
  target.ct        = TargetType;
  target.ptr       = TargetValue;
  target.len       = BufferLength;
  target.soi       = StrLen_or_Ind;

  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL: {
      int8_t v = *(int8_t*)row;
      if (v) v = 1;
      switch (target.ct) {
        case SQL_C_BIT:      return conv_tsdb_bool_to_c_bit(sql, &target, field, v);
        case SQL_C_TINYINT:  return conv_tsdb_bool_to_c_tinyint(sql, &target, field, v);
        case SQL_C_SHORT:    return conv_tsdb_bool_to_c_short(sql, &target, field, v);
        case SQL_C_LONG:     return conv_tsdb_bool_to_c_long(sql, &target, field, v);
        case SQL_C_SBIGINT:  return conv_tsdb_bool_to_c_sbigint(sql, &target, field, v);
        case SQL_C_FLOAT:    return conv_tsdb_bool_to_c_float(sql, &target, field, v);
        case SQL_C_DOUBLE:   return conv_tsdb_bool_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_bool_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_bool_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t v = *(int8_t*)row;
      switch (target.ct) {
        case SQL_C_TINYINT:  return conv_tsdb_v1_to_c_tinyint(sql, &target, field, v);
        case SQL_C_SHORT:    return conv_tsdb_v1_to_c_short(sql, &target, field, v);
        case SQL_C_LONG:     return conv_tsdb_v1_to_c_long(sql, &target, field, v);
        case SQL_C_SBIGINT:  return conv_tsdb_v1_to_c_sbigint(sql, &target, field, v);
        case SQL_C_FLOAT:    return conv_tsdb_v1_to_c_float(sql, &target, field, v);
        case SQL_C_DOUBLE:   return conv_tsdb_v1_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_v1_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_v1_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t*)row;
      switch (target.ct) {
        case SQL_C_SHORT:    return conv_tsdb_v2_to_c_short(sql, &target, field, v);
        case SQL_C_LONG:     return conv_tsdb_v2_to_c_long(sql, &target, field, v);
        case SQL_C_SBIGINT:  return conv_tsdb_v2_to_c_sbigint(sql, &target, field, v);
        case SQL_C_FLOAT:    return conv_tsdb_v2_to_c_float(sql, &target, field, v);
        case SQL_C_DOUBLE:   return conv_tsdb_v2_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_v2_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_v2_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t*)row;
      switch (target.ct) {
        case SQL_C_LONG:     return conv_tsdb_v4_to_c_long(sql, &target, field, v);
        case SQL_C_SBIGINT:  return conv_tsdb_v4_to_c_sbigint(sql, &target, field, v);
        case SQL_C_FLOAT:    return conv_tsdb_v4_to_c_float(sql, &target, field, v);
        case SQL_C_DOUBLE:   return conv_tsdb_v4_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_v4_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_v4_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)row;
      switch (target.ct) {
        case SQL_C_SBIGINT:  return conv_tsdb_v8_to_c_sbigint(sql, &target, field, v);
        case SQL_C_FLOAT:    return conv_tsdb_v8_to_c_float(sql, &target, field, v);
        case SQL_C_DOUBLE:   return conv_tsdb_v8_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_v8_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_v8_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float*)row;
      switch (target.ct) {
        case SQL_C_FLOAT:    return conv_tsdb_f4_to_c_float(sql, &target, field, v);
        case SQL_C_DOUBLE:   return conv_tsdb_f4_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_f4_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_f4_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)row;
      switch (target.ct) {
        case SQL_C_DOUBLE:   return conv_tsdb_f8_to_c_double(sql, &target, field, v);
        case SQL_C_CHAR:     return conv_tsdb_f8_to_c_char(sql, &target, field, v);
        case SQL_C_BINARY:   return conv_tsdb_f8_to_c_binary(sql, &target, field, v);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      TIMESTAMP_STRUCT ts = {0};
      int64_t v = *(int64_t*)row;
      time_t t = v/1000;
      struct tm tm = {0};
      localtime_r(&t, &tm);
      ts.year     = tm.tm_year + 1900;
      ts.month    = tm.tm_mon + 1;
      ts.day      = tm.tm_mday;
      ts.hour     = tm.tm_hour;
      ts.minute   = tm.tm_min;
      ts.second   = tm.tm_sec;
      ts.fraction = v%1000;
      switch (target.ct) {
        case SQL_C_SBIGINT:   return conv_tsdb_ts_to_c_v8(sql, &target, field, &ts);
        case SQL_C_CHAR:      return conv_tsdb_ts_to_c_str(sql, &target, field, &ts);
        case SQL_C_BINARY:    return conv_tsdb_ts_to_c_bin(sql, &target, field, &ts);
        case SQL_C_TIMESTAMP: return conv_tsdb_ts_to_c_ts(sql, &target, field, &ts);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_BINARY: {
      const unsigned char *bin = (const unsigned char *)row;
      switch (target.ct) {
        case SQL_C_CHAR:      return conv_tsdb_bin_to_c_str(sql, &target, field, bin);
        case SQL_C_BINARY:    return conv_tsdb_bin_to_c_bin(sql, &target, field, bin);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_NCHAR: {
      const char *str = (const char *)row;
      switch (target.ct) {
        case SQL_C_BIT:       return conv_tsdb_str_to_c_bit(sql, &target, field, str);
        case SQL_C_TINYINT:   return conv_tsdb_str_to_c_v1(sql, &target, field, str);
        case SQL_C_SHORT:     return conv_tsdb_str_to_c_v2(sql, &target, field, str);
        case SQL_C_LONG:      return conv_tsdb_str_to_c_v4(sql, &target, field, str);
        case SQL_C_SBIGINT:   return conv_tsdb_str_to_c_v8(sql, &target, field, str);
        case SQL_C_FLOAT:     return conv_tsdb_str_to_c_f4(sql, &target, field, str);
        case SQL_C_DOUBLE:    return conv_tsdb_str_to_c_f8(sql, &target, field, str);
        case SQL_C_CHAR:      return conv_tsdb_str_to_c_str(sql, &target, field, str);
        case SQL_C_BINARY:    return conv_tsdb_str_to_c_bin(sql, &target, field, str);
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
    } break;
    default: {
      SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for col [%d]",
                taos_data_type(field->type), field->type, field->type,
                sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
      return SQL_ERROR;
    } break;
  }
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT StatementHandle,
                             SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                             SQLPOINTER TargetValue, SQLLEN BufferLength,
                             SQLLEN *StrLen_or_Ind)
{
  SQLRETURN r;
  r = doSQLGetData(StatementHandle, ColumnNumber, TargetType,
                   TargetValue, BufferLength,
                   StrLen_or_Ind);
  return r;
}

static SQLRETURN doSQLFetch(SQLHSTMT StatementHandle)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  sql->row = taos_fetch_row(sql->rs);
  return sql->row ? SQL_SUCCESS : SQL_NO_DATA;
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle)
{
  SQLRETURN r;
  r = doSQLFetch(StatementHandle);
  return r;
}

static SQLRETURN doSQLPrepare(SQLHSTMT StatementHandle,
                              SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
    sql->row = NULL;
  }

  if (sql->stmt) {
    taos_stmt_close(sql->stmt);
    sql->stmt = NULL;
  }

  if (sql->params) {
    free(sql->params);
    sql->params = NULL;
  }
  sql->n_params = 0;

  do {
    sql->stmt = taos_stmt_init(sql->conn->taos);
    if (!sql->stmt) {
      SET_ERROR(sql, "HY001", terrno, "failed to initialize TAOS statement internally");
      break;
    }

    int r = taos_stmt_prepare(sql->stmt, (const char *)StatementText, TextLength);
    if (r) {
      SET_ERROR(sql, "HY000", r, "failed to prepare a TAOS statement");
      taos_stmt_close(sql->stmt);
      sql->stmt = NULL;
      break;
    }
  } while (0);

  return sql->stmt ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle,
                             SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  SQLRETURN r;
  r = doSQLPrepare(StatementHandle, StatementText, TextLength);
  return r;
}

static const int yes = 1;
static const int no  = 0;

static SQLRETURN do_bind_param_value(sql_t *sql, int idx, param_bind_t *param, TAOS_BIND *bind)
{
  if (!param->valid) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "parameter [@%d] not bound yet", idx+1);
    return SQL_ERROR;
  }
  if (param->StrLen_or_Ind && *param->StrLen_or_Ind == SQL_NULL_DATA) {
    bind->is_null = (int*)&yes;
    return SQL_SUCCESS;
  }
  bind->is_null = (int*)&no;
  int type = 0;
  int bytes = 0;
  int r = taos_stmt_get_param(sql->stmt, idx, &type, &bytes);
  if (r) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_OUT_OF_RANGE, "parameter [@%d] not valid", idx+1);
    return SQL_ERROR;
  }

  // ref: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/converting-data-from-c-to-sql-data-types?view=sql-server-ver15
  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.b);
      bind->buffer = &bind->u.b;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_LONG: {
          bind->u.b = *(int32_t*)param->ParameterValue;
        } break;
        case SQL_C_BIT: {
          bind->u.b = *(int8_t*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_TINYINT: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.v1);
      bind->buffer = &bind->u.v1;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_TINYINT: {
          bind->u.v1 = *(int8_t*)param->ParameterValue;
        } break;
        case SQL_C_SHORT: {
          bind->u.v1 = *(int16_t*)param->ParameterValue;
        } break;
        case SQL_C_LONG: {
          bind->u.v1 = *(int32_t*)param->ParameterValue;
        } break;
        case SQL_C_SBIGINT: {
          bind->u.v1 = *(int64_t*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_SMALLINT: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.v2);
      bind->buffer = &bind->u.v2;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_LONG: {
          bind->u.v2 = *(int32_t*)param->ParameterValue;
        } break;
        case SQL_C_SHORT: {
          bind->u.v2 = *(int16_t*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_INT: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.v4);
      bind->buffer = &bind->u.v4;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_LONG: {
          bind->u.v4 = *(int32_t*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_BIGINT: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.v8);
      bind->buffer = &bind->u.v8;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_SBIGINT: {
          bind->u.v8 = *(int64_t*)param->ParameterValue;
        } break;
        case SQL_C_LONG: {
          bind->u.v8 = *(int32_t*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_FLOAT: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.f4);
      bind->buffer = &bind->u.f4;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_DOUBLE: {
          bind->u.f4 = *(double*)param->ParameterValue;
        } break;
        case SQL_C_FLOAT: {
          bind->u.f4 = *(float*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_LONG:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_DOUBLE: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.f8);
      bind->buffer = &bind->u.f8;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_DOUBLE: {
          bind->u.f8 = *(double*)param->ParameterValue;
        } break;
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_LONG:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_BINARY: {
      bind->buffer_type = type;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_WCHAR: {
          DASSERT(param->StrLen_or_Ind);
          DASSERT(*param->StrLen_or_Ind != SQL_NTS);
          size_t bytes = 0;
          SQLCHAR *utf8 = wchars_to_chars(param->ParameterValue, *param->StrLen_or_Ind/2, &bytes);
          bind->allocated = 1;
          bind->u.bin = utf8;
          bind->buffer_length = bytes;
          bind->buffer = bind->u.bin;
        } break;
        case SQL_C_BINARY: {
          bind->u.bin = (unsigned char*)param->ParameterValue;
          if (*param->StrLen_or_Ind == SQL_NTS) {
            bind->buffer_length = strlen((const char*)param->ParameterValue);
          } else {
            bind->buffer_length = *param->StrLen_or_Ind;
          }
          bind->buffer = bind->u.bin;
        } break;
        case SQL_C_CHAR:
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_LONG:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.v8);
      bind->buffer = &bind->u.v8;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_WCHAR: {
          DASSERT(param->StrLen_or_Ind);
          DASSERT(*param->StrLen_or_Ind != SQL_NTS);
          size_t bytes = 0;
          SQLCHAR *utf8 = wchars_to_chars(param->ParameterValue, *param->StrLen_or_Ind/2, &bytes);
          struct tm tm = {0};
          strptime((const char*)utf8, "%Y-%m-%d %H:%M:%S", &tm);
          int64_t t = (int64_t)mktime(&tm);
          t *= 1000;
          bind->u.v8 = t;
          free(utf8);
        } break;
        case SQL_C_SBIGINT: {
          int64_t t = *(int64_t*)param->ParameterValue;
          bind->u.v8 = t;
        } break;
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_LONG:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_NCHAR: {
      bind->buffer_type = type;
      bind->length = &bind->buffer_length;
      switch (param->ValueType) {
        case SQL_C_WCHAR: {
          DASSERT(param->StrLen_or_Ind);
          DASSERT(*param->StrLen_or_Ind != SQL_NTS);
          size_t bytes = 0;
          SQLCHAR *utf8 = wchars_to_chars(param->ParameterValue, *param->StrLen_or_Ind/2, &bytes);
          bind->allocated = 1;
          bind->u.nchar = (char*)utf8;
          bind->buffer_length = bytes;
          bind->buffer = bind->u.nchar;
        } break;
        case SQL_C_CHAR: {
          bind->u.nchar = (char*)param->ParameterValue;
          if (*param->StrLen_or_Ind == SQL_NTS) {
            bind->buffer_length = strlen((const char*)param->ParameterValue);
          } else {
            bind->buffer_length = *param->StrLen_or_Ind;
          }
          bind->buffer = bind->u.nchar;
        } break;
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_LONG:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BINARY:
        case SQL_C_DATE:
        case SQL_C_TIME:
        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    default: {
      SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                sql_c_type(param->ValueType), param->ValueType, param->ValueType,
                taos_data_type(type), type, type, idx+1);
      return SQL_ERROR;
    } break;
  }
  return SQL_SUCCESS;
}

static SQLRETURN do_execute(sql_t *sql, TAOS_BIND *binds)
{
  SQLRETURN r = SQL_SUCCESS;
  for (int i=0; i<sql->n_params; ++i) {
    r = do_bind_param_value(sql, i, sql->params+i, binds+i);
    if (r==SQL_SUCCESS) continue;
    return r;
  }

  if (sql->n_params > 0) {
    PROFILE(r = taos_stmt_bind_param(sql->stmt, binds));
    if (r) {
      SET_ERROR(sql, "HY000", r, "failed to bind parameters[%d in total]", sql->n_params);
      return SQL_ERROR;
    }

    PROFILE(r = taos_stmt_add_batch(sql->stmt));
    if (r) {
      SET_ERROR(sql, "HY000", r, "failed to add batch");
      return SQL_ERROR;
    }
  }

  PROFILE(r = taos_stmt_execute(sql->stmt));
  if (r) {
    SET_ERROR(sql, "HY000", r, "failed to execute statement");
    return SQL_ERROR;
  }

  PROFILE(sql->rs = taos_stmt_use_result(sql->stmt));
  CHK_RS(r, sql, "failed to use result");

  return r;
}

static SQLRETURN doSQLExecute(SQLHSTMT StatementHandle)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->stmt) {
    SET_ERROR(sql, "HY010", TSDB_CODE_ODBC_STATEMENT_NOT_READY, "");
    return SQL_ERROR;
  }

  if (sql->rs) {
    taos_free_result(sql->rs);
    sql->rs = NULL;
    sql->row = NULL;
  }

  TAOS_BIND *binds       = NULL;
  if (sql->n_params>0) {
    binds = (TAOS_BIND*)calloc(sql->n_params, sizeof(*binds));
    if (!binds) {
      SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
      return SQL_ERROR;
    }
  }

  SQLRETURN r = do_execute(sql, binds);

  if (binds) {
    for (int i = 0; i<sql->n_params; ++i) {
      TAOS_BIND *bind = binds + i;
      if (bind->allocated) {
        free(bind->u.nchar);
        bind->u.nchar = NULL;
      }
    }
    free(binds);
  }

  return r;
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle)
{
  SQLRETURN r;
  PROFILE(r = doSQLExecute(StatementHandle));
  return r;
}

static SQLRETURN doSQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                                   SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLength)
{
  // if this function is not exported, isql will never call SQLGetDiagRec
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                  SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                                  SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
                                  SQLSMALLINT *StringLength)
{
  SQLRETURN r;
  r = doSQLGetDiagField(HandleType, Handle,
                        RecNumber, DiagIdentifier,
                        DiagInfo, BufferLength,
                        StringLength);
  return r;
}

static SQLRETURN doSQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                 SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                 SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                 SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)
{
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

  // how to return error?
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)
{
  SQLRETURN r;
  r = doSQLGetDiagRec(HandleType, Handle,
                      RecNumber, Sqlstate,
                      NativeError, MessageText,
                      BufferLength, TextLength);
  return r;
}

static SQLRETURN doSQLBindParameter(
    SQLHSTMT           StatementHandle,
    SQLUSMALLINT       ParameterNumber,
    SQLSMALLINT        fParamType,
    SQLSMALLINT        ValueType,
    SQLSMALLINT        ParameterType,
    SQLULEN            LengthPrecision,
    SQLSMALLINT        ParameterScale,
    SQLPOINTER         ParameterValue,
    SQLLEN             cbValueMax, // ignore for now, since only SQL_PARAM_INPUT is supported now
    SQLLEN 		      *StrLen_or_Ind)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->stmt) {
    SET_ERROR(sql, "HY010", TSDB_CODE_ODBC_STATEMENT_NOT_READY, "");
    return SQL_ERROR;
  }

  if (fParamType != SQL_PARAM_INPUT) {
    SET_ERROR(sql, "HY105", TSDB_CODE_ODBC_NOT_SUPPORT, "non-input parameter [@%d] not supported yet", ParameterNumber);
    return SQL_ERROR;
  }

  param_bind_t *ar = (param_bind_t*)(sql->n_params>=ParameterNumber ? sql->params : realloc(sql->params, ParameterNumber * sizeof(*ar)));
  if (!ar) {
    SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
    return SQL_ERROR;
  }
  sql->params = ar;
  if (sql->n_params<ParameterNumber) {
    sql->n_params = ParameterNumber;
  }

  param_bind_t *pb = ar + ParameterNumber - 1;

  pb->ParameterNumber      = ParameterNumber;
  pb->ValueType            = ValueType;
  pb->ParameterType        = ParameterType;
  pb->LengthPrecision      = LengthPrecision;
  pb->ParameterScale       = ParameterScale;
  pb->ParameterValue       = ParameterValue;
  pb->StrLen_or_Ind        = StrLen_or_Ind;

  pb->valid            = 1;
  return SQL_SUCCESS;
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
    SQLLEN 		      *StrLen_or_Ind)
{
  SQLRETURN r;
  r = doSQLBindParameter(StatementHandle, ParameterNumber, fParamType, ValueType, ParameterType,
                         LengthPrecision, ParameterScale, ParameterValue, cbValueMax, StrLen_or_Ind);
  return r;
}

static SQLRETURN doSQLDriverConnect(
    SQLHDBC            hdbc,
    SQLHWND            hwnd,
    SQLCHAR 		      *szConnStrIn,
    SQLSMALLINT        cbConnStrIn,
    SQLCHAR           *szConnStrOut,
    SQLSMALLINT        cbConnStrOutMax,
    SQLSMALLINT 	    *pcbConnStrOut,
    SQLUSMALLINT       fDriverCompletion)
{
  conn_t *conn = (conn_t*)hdbc;
  if (!conn) return SQL_ERROR;

  if (fDriverCompletion!=SQL_DRIVER_NOPROMPT) {
    SET_ERROR(conn, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT, "option[%d] other than SQL_DRIVER_NOPROMPT not supported yet", fDriverCompletion);
    return SQL_ERROR;
  }

  if (conn->taos) {
    SET_ERROR(conn, "08002", TSDB_CODE_ODBC_CONNECTION_BUSY, "connection still in use");
    return SQL_ERROR;
  }

  // DSN=<dsn>; UID=<uid>; PWD=<pwd>

  const char *connStr    = SDUP(szConnStrIn, cbConnStrIn);

  char *serverName = NULL;
  char *userName   = NULL;
  char *auth       = NULL;
  int bytes = 0;

  do {
    if (szConnStrIn && !connStr) {
      SET_ERROR(conn, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }

    int n = sscanf((const char*)connStr, "DSN=%m[^;]; UID=%m[^;]; PWD=%m[^;] %n", &serverName, &userName, &auth, &bytes);
    if (n<1) {
      SET_ERROR(conn, "HY000", TSDB_CODE_ODBC_BAD_CONNSTR, "unrecognized connection string: [%s]", (const char*)szConnStrIn);
      break;
    }

    // TODO: data-race
    // TODO: shall receive ip/port from odbc.ini
    conn->taos = taos_connect("localhost", userName, auth, NULL, 0);
    if (!conn->taos) {
      SET_ERROR(conn, "HY000", terrno, "failed to connect to data source");
      break;
    }

    if (szConnStrOut) {
      snprintf((char*)szConnStrOut, cbConnStrOutMax, "%s", connStr);
    }
    if (pcbConnStrOut) {
      *pcbConnStrOut = cbConnStrIn;
    }

  } while (0);

  if (serverName) free(serverName);
  if (userName)   free(userName);
  if (auth)       free(auth);

  SFRE(connStr, szConnStrIn,     cbConnStrIn);

  return conn->taos ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC            hdbc,
    SQLHWND            hwnd,
    SQLCHAR 		      *szConnStrIn,
    SQLSMALLINT        cbConnStrIn,
    SQLCHAR           *szConnStrOut,
    SQLSMALLINT        cbConnStrOutMax,
    SQLSMALLINT 	    *pcbConnStrOut,
    SQLUSMALLINT       fDriverCompletion)
{
    SQLRETURN r;
    r = doSQLDriverConnect(hdbc, hwnd, szConnStrIn, cbConnStrIn, szConnStrOut, cbConnStrOutMax, pcbConnStrOut, fDriverCompletion);
    return r;
}

static SQLRETURN doSQLSetConnectAttr(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute, SQLPOINTER Value,
                                     SQLINTEGER StringLength)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  if (Attribute != SQL_ATTR_AUTOCOMMIT) {
    SET_ERROR(conn, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT, "Attribute other than SQL_ATTR_AUTOCOMMIT not supported yet");
    return SQL_ERROR;
  }
  if (Value != (SQLPOINTER)SQL_AUTOCOMMIT_ON) {
    SET_ERROR(conn, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT, "Attribute Value other than SQL_AUTOCOMMIT_ON not supported yet[%p]", Value);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute, SQLPOINTER Value,
                                    SQLINTEGER StringLength)
{
  SQLRETURN r;
  r = doSQLSetConnectAttr(ConnectionHandle, Attribute, Value, StringLength);
  return r;
}

static SQLRETURN doSQLDescribeCol(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                                  SQLSMALLINT BufferLength, SQLSMALLINT *NameLength,
                                  SQLSMALLINT *DataType, SQLULEN *ColumnSize,
                                  SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  int nfields = taos_field_count(sql->rs);
  TAOS_FIELD *fields = taos_fetch_fields(sql->rs);

  if (ColumnNumber<=0 || ColumnNumber>nfields) {
    SET_ERROR(sql, "07009", TSDB_CODE_ODBC_OUT_OF_RANGE, "invalid column number [%d]", ColumnNumber);
    return SQL_ERROR;
  }

  TAOS_FIELD *field = fields + ColumnNumber - 1;
  if (ColumnName) {
    size_t n = sizeof(field->name);
    if (n>BufferLength) n = BufferLength;
    strncpy((char*)ColumnName, field->name, n);
  }
  if (NameLength) {
    *NameLength = strnlen(field->name, sizeof(field->name));
  }
  if (DataType) {
    switch (field->type) {
      case TSDB_DATA_TYPE_BOOL: {
        *DataType = SQL_C_TINYINT;
      }  break;

      case TSDB_DATA_TYPE_TINYINT: {
        *DataType = SQL_C_TINYINT;
      } break;

      case TSDB_DATA_TYPE_SMALLINT: {
        *DataType = SQL_C_SHORT;
      } break;

      case TSDB_DATA_TYPE_INT: {
        *DataType = SQL_C_LONG;
      } break;

      case TSDB_DATA_TYPE_BIGINT: {
        *DataType = SQL_BIGINT;
      } break;

      case TSDB_DATA_TYPE_FLOAT: {
        *DataType = SQL_C_FLOAT;
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        *DataType = SQL_C_DOUBLE;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP: {
        *DataType = SQL_C_TIMESTAMP;
      } break;

      case TSDB_DATA_TYPE_NCHAR: {
        *DataType = SQL_C_CHAR; // unicode ?
      } break;

      case TSDB_DATA_TYPE_BINARY: {
        *DataType = SQL_C_BINARY;
      } break;

      default:
        SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT,
                  "unknown [%s[%d/0x%x]]", taos_data_type(field->type), field->type, field->type);
        return SQL_ERROR;
        break;
    }
  }
  if (ColumnSize) {
    *ColumnSize = field->bytes;
  }
  if (DecimalDigits) {
    if (field->type == TSDB_DATA_TYPE_TIMESTAMP) {
      *DecimalDigits = 3;
    } else {
      *DecimalDigits = 0;
    }
  }
  if (Nullable) {
    *Nullable = SQL_NULLABLE_UNKNOWN;
  }

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT StatementHandle,
                                 SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                                 SQLSMALLINT BufferLength, SQLSMALLINT *NameLength,
                                 SQLSMALLINT *DataType, SQLULEN *ColumnSize,
                                 SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable)
{
  SQLRETURN r;
  r = doSQLDescribeCol(StatementHandle, ColumnNumber, ColumnName,
                       BufferLength, NameLength,
                       DataType, ColumnSize,
                       DecimalDigits, Nullable);
  return r;
}

static SQLRETURN doSQLNumParams(SQLHSTMT hstmt, SQLSMALLINT *pcpar)
{
  sql_t *sql = (sql_t*)hstmt;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->stmt) {
    SET_ERROR(sql, "HY010", TSDB_CODE_ODBC_STATEMENT_NOT_READY, "");
    return SQL_ERROR;
  }

  int params = 0;
  int r = taos_stmt_num_params(sql->stmt, &params);
  if (r) {
    SET_ERROR(sql, "HY000", terrno, "fetch num of statement params failed");
    return SQL_ERROR;
  }

  if (pcpar) *pcpar = params;

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumParams(SQLHSTMT hstmt, SQLSMALLINT *pcpar)
{
  SQLRETURN r;
  r = doSQLNumParams(hstmt, pcpar);
  return r;
}









static void init_routine(void) {
  if (0) {
    string_conv(NULL, NULL, NULL, 0, NULL, 0, NULL, NULL);
    utf8_to_ucs4le(NULL, NULL);
    ucs4le_to_utf8(NULL, 0, NULL);
  }
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
      return 20;
    } break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR: {
      return 3*(field->bytes - VARSTR_HEADER_SIZE) + 2;
    } break;

    case TSDB_DATA_TYPE_TIMESTAMP:
      return 26;
      break;

    case TSDB_DATA_TYPE_BOOL:
      return 7;
    default:
      break;
  }

  return 10;
}

// convertion from TSDB_DATA_TYPE_XXX to SQL_C_XXX
static SQLRETURN conv_tsdb_bool_to_c_bit(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  int8_t v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_tinyint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  int8_t v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_short(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  int16_t v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  int32_t v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  int64_t v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  float v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  double v = b;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  DASSERT(target->len>0);
  *target->soi = 1;
  if (target->ptr) {
    target->ptr[0] = '0' + b;
    if (target->len>1) {
      target->ptr[1] = '\0';
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bool_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t b)
{
  DASSERT(target->len>0);
  *target->soi = 1;
  target->ptr[0] = '0' + b;
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_tinyint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  int8_t v = v1;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_short(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  int16_t v = v1;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  int32_t v = v1;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  int64_t v = v1;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  float v = v1;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  double v = v1;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v1_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v1);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>=target->len ? target->len : n+1));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_TINYINT -> SQL_C_BIT");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_v1_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int8_t v1)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v1);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>target->len ? target->len : n));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_TINYINT -> SQL_C_BIT");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_v2_to_c_short(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  int16_t v = v2;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v2_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  int32_t v = v2;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v2_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  int64_t v = v2;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v2_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  float v = v2;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v2_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  double v = v2;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v2_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v2);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>=target->len ? target->len : n+1));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_SMALLINT -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_v2_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int16_t v2)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v2);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>target->len ? target->len : n));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_SMALLINT -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}


static SQLRETURN conv_tsdb_v4_to_c_long(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4)
{
  int32_t v = v4;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v4_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4)
{
  int64_t v = v4;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v4_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4)
{
  float v = v4;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v4_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4)
{
  double v = v4;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v4_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v4);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>=target->len ? target->len : n+1));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_INTEGER -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_v4_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int32_t v4)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v4);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>target->len ? target->len : n));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_INTEGER -> SQL_C_BINARY");
  return SQL_SUCCESS_WITH_INFO;
}


static SQLRETURN conv_tsdb_v8_to_c_sbigint(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8)
{
  int64_t v = v8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v8_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8)
{
  float v = v8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v8_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8)
{
  double v = v8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_v8_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%ld", v8);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>=target->len ? target->len : n+1));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_BIGINT -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_v8_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, int64_t v8)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%ld", v8);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>target->len ? target->len : n));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_BIGINT -> SQL_C_BINARY");
  return SQL_SUCCESS_WITH_INFO;
}


static SQLRETURN conv_tsdb_f4_to_c_float(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4)
{
  float v = f4;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_f4_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4)
{
  double v = f4;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_f4_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%g", f4);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>=target->len ? target->len : n+1));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_FLOAT -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_f4_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, float f4)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%g", f4);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>target->len ? target->len : n));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_FLOAT -> SQL_C_BINARY");
  return SQL_SUCCESS_WITH_INFO;
}


static SQLRETURN conv_tsdb_f8_to_c_double(sql_t *sql, c_target_t *target, TAOS_FIELD *field, double f8)
{
  double v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_f8_to_c_char(sql_t *sql, c_target_t *target, TAOS_FIELD *field, double f8)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%.6f", f8);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>=target->len ? target->len : n+1));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_DOUBLE -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_f8_to_c_binary(sql_t *sql, c_target_t *target, TAOS_FIELD *field, double f8)
{
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%g", f8);
  DASSERT(n<sizeof(buf));
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, buf, (n>target->len ? target->len : n));
  if (n<=target->len) return SQL_SUCCESS;
  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_DOUBLE -> SQL_C_BINARY");
  return SQL_SUCCESS_WITH_INFO;
}


static SQLRETURN conv_tsdb_ts_to_c_v8(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts)
{
  struct tm tm = {0};
  tm.tm_sec        = ts->second;
  tm.tm_min        = ts->minute;
  tm.tm_hour       = ts->hour;
  tm.tm_mday       = ts->day;
  tm.tm_mon        = ts->month - 1;
  tm.tm_year       = ts->year - 1900;
  time_t t = mktime(&tm);
  DASSERT(sizeof(t) == sizeof(int64_t));
  int64_t v = (int64_t)t;
  v *= 1000;
  v += ts->fraction % 1000;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_ts_to_c_str(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts)
{
  struct tm tm = {0};
  tm.tm_sec        = ts->second;
  tm.tm_min        = ts->minute;
  tm.tm_hour       = ts->hour;
  tm.tm_mday       = ts->day;
  tm.tm_mon        = ts->month - 1;
  tm.tm_year       = ts->year - 1900;

  char buf[64];
  int n = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
  DASSERT(n < sizeof(buf));

  *target->soi = n;

  if (target->ptr) {
    snprintf(target->ptr, target->len, "%s.%03d", buf, ts->fraction);
  }

  if (n <= target->len) {
    return SQL_SUCCESS;
  }

  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_TIMESTAMP -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_ts_to_c_bin(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts)
{
  struct tm tm = {0};
  tm.tm_sec        = ts->second;
  tm.tm_min        = ts->minute;
  tm.tm_hour       = ts->hour;
  tm.tm_mday       = ts->day;
  tm.tm_mon        = ts->month - 1;
  tm.tm_year       = ts->year - 1900;

  char buf[64];
  int n = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
  DASSERT(n < sizeof(buf));

  *target->soi = n;

  if (target->ptr) memcpy(target->ptr, buf, (n>target->len ? target->len : n));

  if (n <= target->len) {
    return SQL_SUCCESS;
  }

  SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_TIMESTAMP -> SQL_C_BINARY");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_ts_to_c_ts(sql_t *sql, c_target_t *target, TAOS_FIELD *field, TIMESTAMP_STRUCT *ts)
{
  if (target->ptr) memcpy(target->ptr, ts, sizeof(*ts));
  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_bin_to_c_str(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const unsigned char *bin)
{
  size_t n = field->bytes;
  n = strlen((const char*)bin);
  *target->soi = n;
  if (target->ptr) memcpy(target->ptr, bin, (n>target->len ? target->len : n));
  if (n <= target->len) return SQL_SUCCESS;

  SET_ERROR(sql, "01004", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_BINARY -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_bin_to_c_bin(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const unsigned char *bin)
{
  size_t n = field->bytes;
  n = strlen((const char*)bin);
  *target->soi = n;
  if (target->ptr) memcpy(target->ptr, bin, (n>target->len ? target->len : n));
  if (n <= target->len) return SQL_SUCCESS;

  SET_ERROR(sql, "01004", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_BINARY -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_bit(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  int8_t v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 1;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_BIT");
    return SQL_SUCCESS_WITH_INFO;
  }

  char buf[64];
  snprintf(buf, sizeof(buf), "%d", v);

  if (strcmp(buf, str)==0) {
    if (v==0 || v==1) return SQL_SUCCESS;
    SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_BIT");
    return SQL_SUCCESS_WITH_INFO;
  }

  if (f8>0 || f8<2) {
    SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_BIT");
    return SQL_SUCCESS_WITH_INFO;
  }

  if (f8<0 || f8>2) {
    SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_BIT");
    return SQL_SUCCESS_WITH_INFO;
  }

  SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_BIT");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_v1(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  int8_t v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 1;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_TINYINT");
    return SQL_SUCCESS_WITH_INFO;
  }

  char buf[64];
  snprintf(buf, sizeof(buf), "%d", v);

  if (strcmp(buf, str)==0) return SQL_SUCCESS;

  if (f8>INT8_MAX || f8<INT8_MIN) {
    SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_TINYINT");
    return SQL_SUCCESS_WITH_INFO;
  }

  SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_TINYINT");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_v2(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  int16_t v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 2;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_SHORT");
    return SQL_SUCCESS_WITH_INFO;
  }

  char buf[64];
  snprintf(buf, sizeof(buf), "%d", v);

  if (strcmp(buf, str)==0) return SQL_SUCCESS;

  if (f8>INT16_MAX || f8<INT16_MIN) {
    SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_SHORT");
    return SQL_SUCCESS_WITH_INFO;
  }

  SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_SHORT");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_v4(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  int32_t v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 4;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_LONG");
    return SQL_SUCCESS_WITH_INFO;
  }

  char buf[64];
  snprintf(buf, sizeof(buf), "%d", v);

  if (strcmp(buf, str)==0) return SQL_SUCCESS;

  if (f8>INT32_MAX || f8<INT32_MIN) {
    SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_LONG");
    return SQL_SUCCESS_WITH_INFO;
  }

  SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_LONG");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_v8(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  int64_t v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 8;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_SBIGINT");
    return SQL_SUCCESS_WITH_INFO;
  }

  char buf[64];
  snprintf(buf, sizeof(buf), "%ld", v);

  if (strcmp(buf, str)==0) return SQL_SUCCESS;

  if (f8>INT64_MAX || f8<INT64_MIN) {
    SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_SBIGINT");
    return SQL_SUCCESS_WITH_INFO;
  }

  SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_SBIGINT");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_f4(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  float v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 4;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_FLOAT");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_str_to_c_f8(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  int bytes = 0;
  double f8 = 0;
  int n = sscanf(str, "%lf%n", &f8, &bytes);

  float v = f8;
  if (target->ptr) memcpy(target->ptr, &v, sizeof(v));

  *target->soi = 8;

  if (n!=1 || bytes!=strlen(str)) {
    SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_UNDEF, "TSDB_DATA_TYPE_NCHAR -> SQL_C_DOUBLE");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN conv_tsdb_str_to_c_str(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  size_t n = strlen(str);
  *target->soi = n;
  if (target->ptr) strncpy(target->ptr, str, (n < target->len ? n+1 : target->len));

  if (n < target->len) return SQL_SUCCESS;

  SET_ERROR(sql, "01004", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_CHAR");
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN conv_tsdb_str_to_c_bin(sql_t *sql, c_target_t *target, TAOS_FIELD *field, const char *str)
{
  size_t n = strlen(str);
  *target->soi = n;
  if (target->ptr) memcpy(target->ptr, str, (n < target->len ? n : target->len));

  if (n <= target->len) return SQL_SUCCESS;

  SET_ERROR(sql, "01004", TSDB_CODE_ODBC_CONV_TRUNC, "TSDB_DATA_TYPE_NCHAR -> SQL_C_BINARY");
  return SQL_SUCCESS_WITH_INFO;
}

