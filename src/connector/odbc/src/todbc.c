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
#include "todbc_conv.h"

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
  int n = snprintf(NULL, 0, "[TSDB:%x]%s: @%s[%d]" err_fmt "",                                            \
                   eno, estr,                                                                             \
                   basename((char*)__FILE__), __LINE__,                                                   \
                   ##__VA_ARGS__);                                                                        \
  if (n<0) break;                                                                                         \
  char *err_str = (char*)realloc(obj->err.err_str, n+1);                                                  \
  if (!err_str) break;                                                                                    \
  obj->err.err_str = err_str;                                                                             \
  snprintf(obj->err.err_str, n+1, "[TSDB:%x]%s: @%s[%d]" err_fmt "",                                      \
           eno, estr,                                                                                     \
           basename((char*)__FILE__), __LINE__,                                                           \
           ##__VA_ARGS__);                                                                                \
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

#define CHK_CONV(todb, statement)                                       \
do {                                                                    \
  TSDB_CONV_CODE code = (statement);                                    \
  switch (code) {                                                       \
    case TSDB_CONV_OK: return SQL_SUCCESS;                              \
    case TSDB_CONV_OOM: {                                               \
      SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");                  \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_OOR: {                                               \
      SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_OOR, "");             \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_CHAR_NOT_NUM: {                                      \
      SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_CHAR_NOT_NUM, "");    \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_TRUNC_FRACTION: {                                    \
      SET_ERROR(sql, "01S07", TSDB_CODE_ODBC_CONV_TRUNC_FRAC, "");      \
      return todb ? SQL_ERROR : SQL_SUCCESS_WITH_INFO;                  \
    } break;                                                            \
    case TSDB_CONV_TRUNC: {                                             \
      SET_ERROR(sql, "22001", TSDB_CODE_ODBC_CONV_TRUNC, "");           \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    default: {                                                          \
      DASSERTX(0, "internal logic error");                              \
      return SQL_ERROR; /* never reached here */                        \
    } break;                                                            \
  }                                                                     \
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

  iconv_t                 w2c;      // wchar* -> char*
  iconv_t                 u2c;      // unicode -> char*
  iconv_t                 u2w;      // unicode -> wchar*

  TAOS_STMT              *stmt;
  param_bind_t           *params;
  int                     n_params;
  size_t                  rowlen;
  size_t                  n_rows;
  size_t                  ptr_offset;

  TAOS_RES               *rs;
  TAOS_ROW                row;

  taos_error_t            err;
  unsigned int            is_prepared:1;
  unsigned int            is_insert:1;
  unsigned int            is_executed:1;
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
static iconv_t sql_get_w2c(sql_t *sql) {
  if (sql->w2c == (iconv_t)-1) {
    sql->w2c = iconv_open("UTF-8", "UCS-2LE");
  }

  return sql->w2c;
}

// static iconv_t sql_get_u2c(sql_t *sql) {
//   if (sql->u2c == (iconv_t)-1) {
//     sql->u2c = iconv_open("UTF-8", "UCS-4LE");
//   }
//
//   return sql->u2c;
// }
//
// static iconv_t sql_get_u2w(sql_t *sql) {
//   if (sql->u2w == (iconv_t)-1) {
//     sql->u2w = iconv_open("UCS-2LE", "UCS-4LE");
//   }
//
//   return sql->u2w;
// }

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

  NameLength1 = (NameLength1==SQL_NTS) ? strlen((const char*)ServerName) : NameLength1;
  NameLength2 = (NameLength2==SQL_NTS) ? strlen((const char*)UserName) : NameLength2;
  NameLength3 = (NameLength3==SQL_NTS) ? strlen((const char*)Authentication) : NameLength3;

  if (NameLength1 < 0 || NameLength2 < 0 || NameLength3 < 0) {
    SET_ERROR(conn, "HY090", TSDB_CODE_ODBC_BAD_ARG, "");
    return SQL_ERROR;
  }
  if (NameLength1>SQL_MAX_DSN_LENGTH) {
    SET_ERROR(conn, "HY090", TSDB_CODE_ODBC_BAD_ARG, "");
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

    sql->w2c = (iconv_t)-1;
    sql->u2c = (iconv_t)-1;
    sql->u2w = (iconv_t)-1;

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

static SQLRETURN doSQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  switch (HandleType) {
    case SQL_HANDLE_ENV: {
      SQLHENV env = {0};
      SQLRETURN r = doSQLAllocEnv(&env);
      if (r==SQL_SUCCESS && OutputHandle) *OutputHandle = env;
      return r;
    } break;
    case SQL_HANDLE_DBC: {
      SQLHDBC dbc = {0};
      SQLRETURN r = doSQLAllocConnect(InputHandle, &dbc);
      if (r==SQL_SUCCESS && OutputHandle) *OutputHandle = dbc;
      return r;
    } break;
    case SQL_HANDLE_STMT: {
      SQLHSTMT stmt = {0};
      SQLRETURN r = doSQLAllocStmt(InputHandle, &stmt);
      if (r==SQL_SUCCESS && OutputHandle) *OutputHandle = stmt;
      return r;
    } break;
    default: {
      return SQL_ERROR;
    } break;
  }
}

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  SQLRETURN r;
  r = doSQLAllocHandle(HandleType, InputHandle, OutputHandle);
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

  if (sql->w2c!=(iconv_t)-1) {
    iconv_close(sql->w2c);
    sql->w2c = (iconv_t)-1;
  }
  if (sql->u2c!=(iconv_t)-1) {
    iconv_close(sql->u2c);
    sql->u2c = (iconv_t)-1;
  }
  if (sql->u2w!=(iconv_t)-1) {
    iconv_close(sql->u2w);
    sql->u2w = (iconv_t)-1;
  }

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

  if (sql->is_insert) {
    if (ColumnCount) {
      *ColumnCount = 0;
    }
    return SQL_SUCCESS;
  }

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

  if (sql->is_insert) {
    if (RowCount) *RowCount = 0;
    return SQL_SUCCESS;
  }

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

  if (TargetValue == NULL) {
    SET_ERROR(sql, "HY009", TSDB_CODE_ODBC_BAD_ARG, "NULL TargetValue not allowed for col [%d]", ColumnNumber);
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
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v;
      switch (field->type) {
        case TSDB_DATA_TYPE_BOOL:       v = *(int8_t*)row;  if (v) v = 1; break;
        case TSDB_DATA_TYPE_TINYINT:    v = *(int8_t*)row;  break;
        case TSDB_DATA_TYPE_SMALLINT:   v = *(int16_t*)row; break;
        case TSDB_DATA_TYPE_INT:        v = *(int32_t*)row; break;
        case TSDB_DATA_TYPE_BIGINT:     // fall through
        default:                        v = *(int64_t*)row; break;
      }
      switch (target.ct) {
        case SQL_C_BIT: {
          CHK_CONV(0, tsdb_int64_to_bit(v, TargetValue));
        } break;
        case SQL_C_TINYINT: {
          CHK_CONV(0, tsdb_int64_to_tinyint(v, TargetValue));
        } break;
        case SQL_C_SHORT: {
          CHK_CONV(0, tsdb_int64_to_smallint(v, TargetValue));
        } break;
        case SQL_C_LONG: {
          CHK_CONV(0, tsdb_int64_to_int(v, TargetValue));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(0, tsdb_int64_to_bigint(v, TargetValue));
        } break;
        case SQL_C_FLOAT: {
          CHK_CONV(0, tsdb_int64_to_float(v, TargetValue));
        } break;
        case SQL_C_DOUBLE: {
          CHK_CONV(0, tsdb_int64_to_double(v, TargetValue));
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(0, tsdb_int64_to_char(v, TargetValue, BufferLength));
        } break;
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
        case SQL_C_FLOAT: {
          *(float*)TargetValue = v;
          return SQL_SUCCESS;
        } break;
        case SQL_C_DOUBLE: {
          *(double*)TargetValue = v;
          return SQL_SUCCESS;
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(0, tsdb_double_to_char(v, TargetValue, BufferLength));
        } break;
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
        case SQL_C_DOUBLE: {
          *(double*)TargetValue = v;
          return SQL_SUCCESS;
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(0, tsdb_double_to_char(v, TargetValue, BufferLength));
        } break;
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      SQL_TIMESTAMP_STRUCT ts = {0};
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
      ts.fraction = v%1000 * 1000000;
      switch (target.ct) {
        case SQL_C_SBIGINT: {
          *(int64_t*)TargetValue = v;
          return SQL_SUCCESS;
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(0, tsdb_timestamp_to_char(ts, TargetValue, BufferLength));
        } break;
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_TIMESTAMP: {
          *(SQL_TIMESTAMP_STRUCT*)TargetValue = ts;
          return SQL_SUCCESS;
        } break;
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_BINARY: {
      size_t field_bytes = field->bytes;
      field_bytes -= VARSTR_HEADER_SIZE;
      switch (target.ct) {
        case SQL_C_CHAR: {
          CHK_CONV(0, tsdb_chars_to_char((const char*)row, field_bytes, TargetValue, BufferLength));
        } break;
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT,
                    "no convertion from [%s] to [%s[%d][0x%x]] for col [%d]",
                    taos_data_type(field->type), sql_c_type(target.ct), target.ct, target.ct, ColumnNumber);
          return SQL_ERROR;
        }
      }
		} break;
    case TSDB_DATA_TYPE_NCHAR: {
      size_t field_bytes = field->bytes;
      field_bytes -= VARSTR_HEADER_SIZE;
      switch (target.ct) {
        case SQL_C_CHAR: {
          CHK_CONV(0, tsdb_chars_to_char((const char*)row, field_bytes, TargetValue, BufferLength));
        } break;
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
  sql->is_insert = 0;

  do {
    sql->stmt = taos_stmt_init(sql->conn->taos);
    if (!sql->stmt) {
      SET_ERROR(sql, "HY001", terrno, "failed to initialize TAOS statement internally");
      break;
    }

    int ok = 0;
    do {
      int r = taos_stmt_prepare(sql->stmt, (const char *)StatementText, TextLength);
      if (r) {
        SET_ERROR(sql, "HY000", r, "failed to prepare a TAOS statement");
        break;
      }
      sql->is_prepared = 1;

      int is_insert = 0;
      r = taos_stmt_is_insert(sql->stmt, &is_insert);
      if (r) {
        SET_ERROR(sql, "HY000", r, "failed to determine if a prepared-statement is of insert");
        break;
      }
      sql->is_insert = is_insert ? 1 : 0;

      int params = 0;
      r = taos_stmt_num_params(sql->stmt, &params);
      if (r) {
        SET_ERROR(sql, "HY000", terrno, "fetch num of statement params failed");
        break;
      }
      DASSERT(params>=0);

      if (params>0) {
        param_bind_t *ar = (param_bind_t*)calloc(1, params * sizeof(*ar));
        if (!ar) {
          SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
          break;
        }
        sql->params = ar;
      }

      sql->n_params = params;

      ok = 1;
    } while (0);

    if (!ok) {
      taos_stmt_close(sql->stmt);
      sql->stmt = NULL;
      sql->is_prepared = 0;
      sql->is_insert   = 0;
      sql->is_executed = 0;
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

static SQLRETURN do_bind_param_value(sql_t *sql, int idx_row, int idx, param_bind_t *param, TAOS_BIND *bind)
{
  if (!param->valid) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "parameter [@%d] not bound yet", idx+1);
    return SQL_ERROR;
  }

  SQLPOINTER    paramValue = param->ParameterValue;
  SQLSMALLINT   valueType  = param->ValueType;
  SQLLEN       *soi        = param->StrLen_or_Ind;

  size_t offset = idx_row * sql->rowlen + sql->ptr_offset;

  if (paramValue) paramValue += offset;
  if (soi)               soi = (SQLLEN*)((char*)soi + offset);


  if (soi && *soi == SQL_NULL_DATA) {
    bind->is_null = (int*)&yes;
    return SQL_SUCCESS;
  }
  bind->is_null = (int*)&no;
  int type = 0;
  int bytes = 0;
  if (sql->is_insert) {
    int r = taos_stmt_get_param(sql->stmt, idx, &type, &bytes);
    if (r) {
      SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_OUT_OF_RANGE, "parameter [@%d] not valid", idx+1);
      return SQL_ERROR;
    }
  } else {
    switch (valueType) {
      case SQL_C_LONG: {
        type = TSDB_DATA_TYPE_INT;
      } break;
      case SQL_C_WCHAR: {
        type = TSDB_DATA_TYPE_NCHAR;
        bytes = SQL_NTS;
      } break;
      case SQL_C_CHAR:
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
                  "no convertion from [%s[%d/0x%x]] for parameter [%d]",
                  sql_c_type(valueType), valueType, valueType,
                  idx+1);
        return SQL_ERROR;
      } break;
    }
  }

  // ref: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/converting-data-from-c-to-sql-data-types?view=sql-server-ver15
  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      bind->buffer_type = type;
      bind->buffer_length = sizeof(bind->u.b);
      bind->buffer = &bind->u.b;
      bind->length = &bind->buffer_length;
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_bit(*(int8_t*)paramValue, &bind->u.b));
        } break;
        case SQL_C_TINYINT:
        case SQL_C_STINYINT: {
          CHK_CONV(1, tsdb_int64_to_bit(*(int8_t*)paramValue, &bind->u.b));
        } break;
        case SQL_C_SHORT:
        case SQL_C_SSHORT: {
          CHK_CONV(1, tsdb_int64_to_bit(*(int16_t*)paramValue, &bind->u.b));
        } break;
        case SQL_C_LONG:
        case SQL_C_SLONG: {
          CHK_CONV(1, tsdb_int64_to_bit(*(int32_t*)paramValue, &bind->u.b));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_bit(*(int64_t*)paramValue, &bind->u.b));
        } break;
        case SQL_C_FLOAT: {
          CHK_CONV(1, tsdb_double_to_bit(*(float*)paramValue, &bind->u.b));
        } break;
        case SQL_C_DOUBLE: {
          CHK_CONV(1, tsdb_double_to_bit(*(double*)paramValue, &bind->u.b));
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_bit((const char *)paramValue, *soi, &bind->u.b));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_bit(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.b));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_tinyint(*(int8_t*)paramValue, &bind->u.v1));
        } break;
        case SQL_C_STINYINT:
        case SQL_C_TINYINT: {
          CHK_CONV(1, tsdb_int64_to_tinyint(*(int8_t*)paramValue, &bind->u.v1));
        } break;
        case SQL_C_SSHORT:
        case SQL_C_SHORT: {
          CHK_CONV(1, tsdb_int64_to_tinyint(*(int16_t*)paramValue, &bind->u.v1));
        } break;
        case SQL_C_SLONG:
        case SQL_C_LONG: {
          CHK_CONV(1, tsdb_int64_to_tinyint(*(int32_t*)paramValue, &bind->u.v1));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_tinyint(*(int64_t*)paramValue, &bind->u.v1));
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_tinyint((const char*)paramValue, *soi, &bind->u.v1));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_tinyint(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.v1));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_smallint(*(int8_t*)paramValue, &bind->u.v2));
        } break;
        case SQL_C_STINYINT:
        case SQL_C_TINYINT: {
          CHK_CONV(1, tsdb_int64_to_smallint(*(int8_t*)paramValue, &bind->u.v2));
        } break;
        case SQL_C_SSHORT:
        case SQL_C_SHORT: {
          CHK_CONV(1, tsdb_int64_to_smallint(*(int16_t*)paramValue, &bind->u.v2));
        } break;
        case SQL_C_SLONG:
        case SQL_C_LONG: {
          CHK_CONV(1, tsdb_int64_to_smallint(*(int32_t*)paramValue, &bind->u.v2));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_smallint(*(int64_t*)paramValue, &bind->u.v2));
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_smallint((const char*)paramValue, *soi, &bind->u.v2));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_smallint(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.v2));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_int(*(int8_t*)paramValue, &bind->u.v4));
        } break;
        case SQL_C_STINYINT:
        case SQL_C_TINYINT: {
          CHK_CONV(1, tsdb_int64_to_int(*(int8_t*)paramValue, &bind->u.v4));
        } break;
        case SQL_C_SSHORT:
        case SQL_C_SHORT: {
          CHK_CONV(1, tsdb_int64_to_int(*(int16_t*)paramValue, &bind->u.v4));
        } break;
        case SQL_C_SLONG:
        case SQL_C_LONG: {
          CHK_CONV(1, tsdb_int64_to_int(*(int32_t*)paramValue, &bind->u.v4));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_int(*(int64_t*)paramValue, &bind->u.v4));
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_int((const char*)paramValue, *soi, &bind->u.v4));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_int(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.v4));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_bigint(*(int8_t*)paramValue, &bind->u.v8));
        } break;
        case SQL_C_STINYINT:
        case SQL_C_TINYINT: {
          CHK_CONV(1, tsdb_int64_to_bigint(*(int8_t*)paramValue, &bind->u.v8));
        } break;
        case SQL_C_SSHORT:
        case SQL_C_SHORT: {
          CHK_CONV(1, tsdb_int64_to_bigint(*(int16_t*)paramValue, &bind->u.v8));
        } break;
        case SQL_C_SLONG:
        case SQL_C_LONG: {
          CHK_CONV(1, tsdb_int64_to_bigint(*(int32_t*)paramValue, &bind->u.v8));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_bigint(*(int64_t*)paramValue, &bind->u.v8));
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_bigint((const char*)paramValue, *soi, &bind->u.v8));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_bigint(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.v8));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_float(*(int8_t*)paramValue, &bind->u.f4));
        } break;
        case SQL_C_STINYINT:
        case SQL_C_TINYINT: {
          CHK_CONV(1, tsdb_int64_to_float(*(int8_t*)paramValue, &bind->u.f4));
        } break;
        case SQL_C_SSHORT:
        case SQL_C_SHORT: {
          CHK_CONV(1, tsdb_int64_to_float(*(int16_t*)paramValue, &bind->u.f4));
        } break;
        case SQL_C_SLONG:
        case SQL_C_LONG: {
          CHK_CONV(1, tsdb_int64_to_float(*(int32_t*)paramValue, &bind->u.f4));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_float(*(int64_t*)paramValue, &bind->u.f4));
        } break;
        case SQL_C_FLOAT: {
          bind->u.f4 = *(float*)paramValue;
        } break;
        case SQL_C_DOUBLE: {
          bind->u.f4 = (float)*(double*)paramValue;
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_float((const char*)paramValue, *soi, &bind->u.f4));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_float(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.f4));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_BIT: {
          CHK_CONV(1, tsdb_int64_to_double(*(int8_t*)paramValue, &bind->u.f8));
        } break;
        case SQL_C_STINYINT:
        case SQL_C_TINYINT: {
          CHK_CONV(1, tsdb_int64_to_double(*(int8_t*)paramValue, &bind->u.f8));
        } break;
        case SQL_C_SSHORT:
        case SQL_C_SHORT: {
          CHK_CONV(1, tsdb_int64_to_double(*(int16_t*)paramValue, &bind->u.f8));
        } break;
        case SQL_C_SLONG:
        case SQL_C_LONG: {
          CHK_CONV(1, tsdb_int64_to_double(*(int32_t*)paramValue, &bind->u.f8));
        } break;
        case SQL_C_SBIGINT: {
          CHK_CONV(1, tsdb_int64_to_double(*(int64_t*)paramValue, &bind->u.f8));
        } break;
        case SQL_C_FLOAT: {
          bind->u.f8 = *(float*)paramValue;
        } break;
        case SQL_C_DOUBLE: {
          bind->u.f8 = *(double*)paramValue;
        } break;
        case SQL_C_CHAR: {
          CHK_CONV(1, tsdb_chars_to_double((const char*)paramValue, *soi, &bind->u.f8));
        } break;
        case SQL_C_WCHAR: {
          CHK_CONV(1, tsdb_wchars_to_double(sql_get_w2c(sql), (const unsigned char*)paramValue, *soi, &bind->u.f8));
        } break;
        case SQL_C_USHORT:
        case SQL_C_ULONG:
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
                    sql_c_type(valueType), valueType, valueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_BINARY: {
      bind->buffer_type = type;
      bind->length = &bind->buffer_length;
      switch (valueType) {
        case SQL_C_WCHAR: {
          DASSERT(soi);
          DASSERT(*soi != SQL_NTS);
          size_t bytes = 0;
          SQLCHAR *utf8 = wchars_to_chars(paramValue, *soi/2, &bytes);
          bind->allocated = 1;
          bind->u.bin = utf8;
          bind->buffer_length = bytes;
          bind->buffer = bind->u.bin;
        } break;
        case SQL_C_BINARY: {
          bind->u.bin = (unsigned char*)paramValue;
          if (*soi == SQL_NTS) {
            bind->buffer_length = strlen((const char*)paramValue);
          } else {
            bind->buffer_length = *soi;
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
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_BIT:
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
                    sql_c_type(valueType), valueType, valueType,
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
      switch (valueType) {
        case SQL_C_WCHAR: {
          DASSERT(soi);
          DASSERT(*soi != SQL_NTS);
          size_t bytes = 0;
          int r = 0;
          int64_t t = 0;
          SQLCHAR *utf8 = wchars_to_chars(paramValue, *soi/2, &bytes);
          // why cast utf8 to 'char*' ?
          r = taosParseTime((char*)utf8, &t, strlen((const char*)utf8), TSDB_TIME_PRECISION_MILLI, 0);
          bind->u.v8 = t;
          free(utf8);
          if (r) {
            SET_ERROR(sql, "22007", TSDB_CODE_ODBC_OUT_OF_RANGE,
                      "convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d] failed",
                      sql_c_type(valueType), valueType, valueType,
                      taos_data_type(type), type, type, idx+1);
            return SQL_ERROR;
          }
        } break;
        case SQL_C_SBIGINT: {
          int64_t t = *(int64_t*)paramValue;
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
                    sql_c_type(valueType), valueType, valueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_NCHAR: {
      bind->buffer_type = type;
      bind->length = &bind->buffer_length;
      switch (valueType) {
        case SQL_C_WCHAR: {
          DASSERT(soi);
          DASSERT(*soi != SQL_NTS);
          size_t bytes = 0;
          SQLCHAR *utf8 = wchars_to_chars(paramValue, *soi/2, &bytes);
          bind->allocated = 1;
          bind->u.nchar = (char*)utf8;
          bind->buffer_length = bytes;
          bind->buffer = bind->u.nchar;
        } break;
        case SQL_C_CHAR: {
          bind->u.nchar = (char*)paramValue;
          if (*soi == SQL_NTS) {
            bind->buffer_length = strlen((const char*)paramValue);
          } else {
            bind->buffer_length = *soi;
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
                    sql_c_type(valueType), valueType, valueType,
                    taos_data_type(type), type, type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    default: {
      SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                sql_c_type(valueType), valueType, valueType,
                taos_data_type(type), type, type, idx+1);
      return SQL_ERROR;
    } break;
  }
  return SQL_SUCCESS;
}

static SQLRETURN do_bind_batch(sql_t *sql, int idx_row, TAOS_BIND *binds)
{
  for (int j=0; j<sql->n_params; ++j) {
    SQLRETURN r = do_bind_param_value(sql, idx_row, j, sql->params+j, binds+j);
    if (r==SQL_SUCCESS) continue;
    return r;
  }
  if (sql->n_params > 0) {
    int tr = 0;
    PROFILE(tr = taos_stmt_bind_param(sql->stmt, binds));
    if (tr) {
      SET_ERROR(sql, "HY000", tr, "failed to bind parameters[%d in total]", sql->n_params);
      return SQL_ERROR;
    }

    if (sql->is_insert) {
      int r = 0;
      PROFILE(r = taos_stmt_add_batch(sql->stmt));
      if (r) {
        SET_ERROR(sql, "HY000", r, "failed to add batch");
        return SQL_ERROR;
      }
    }
  }
  return SQL_SUCCESS;
}

static SQLRETURN do_execute(sql_t *sql)
{
  int tr = TSDB_CODE_SUCCESS;
  if (sql->n_rows==0) sql->n_rows = 1;
  for (int i=0; i<sql->n_rows; ++i) {
    TAOS_BIND *binds       = NULL;
    if (sql->n_params>0) {
      binds = (TAOS_BIND*)calloc(sql->n_params, sizeof(*binds));
      if (!binds) {
        SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
        return SQL_ERROR;
      }
    }

    SQLRETURN r = do_bind_batch(sql, i, binds);

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

    if (r) return r;
  }

  PROFILE(tr = taos_stmt_execute(sql->stmt));
  if (tr) {
    SET_ERROR(sql, "HY000", tr, "failed to execute statement");
    return SQL_ERROR;
  }

  sql->is_executed = 1;
  if (sql->is_insert) return SQL_SUCCESS;

  SQLRETURN r = SQL_SUCCESS;
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

  SQLRETURN r = do_execute(sql);

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

  if (ParameterNumber<=0 || ParameterNumber>sql->n_params) {
    SET_ERROR(sql, "07009", TSDB_CODE_ODBC_BAD_ARG,
              "parameter [@%d] invalid", ParameterNumber);
    return SQL_ERROR;
  }

  if (fParamType != SQL_PARAM_INPUT) {
    SET_ERROR(sql, "HY105", TSDB_CODE_ODBC_NOT_SUPPORT, "non-input parameter [@%d] not supported yet", ParameterNumber);
    return SQL_ERROR;
  }

  if (ValueType == SQL_C_DEFAULT) {
    SET_ERROR(sql, "HY003", TSDB_CODE_ODBC_NOT_SUPPORT, "default value for parameter [@%d] not supported yet", ParameterNumber);
    return SQL_ERROR;
  }

  if (!is_valid_sql_c_type(ValueType)) {
    SET_ERROR(sql, "HY003", TSDB_CODE_ODBC_NOT_SUPPORT,
              "SQL_C_TYPE [%s/%d/0x%x] for parameter [@%d] unknown",
              sql_c_type(ValueType), ValueType, ValueType, ParameterNumber);
    return SQL_ERROR;
  }

  if (!is_valid_sql_sql_type(ParameterType)) {
    SET_ERROR(sql, "HY004", TSDB_CODE_ODBC_NOT_SUPPORT,
              "SQL_TYPE [%s/%d/0x%x] for parameter [@%d] unknown",
              sql_c_type(ParameterType), ParameterType, ParameterType, ParameterNumber);
    return SQL_ERROR;
  }

  param_bind_t *pb = sql->params + ParameterNumber - 1;

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
  char *host       = NULL;
  char *ip         = NULL;
  int   port       = 0;

  do {
    if (szConnStrIn && !connStr) {
      SET_ERROR(conn, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }

    int n = todbc_parse_conn_string((const char *)connStr, &serverName, &userName, &auth, &host);
    if (n) {
      SET_ERROR(conn, "HY000", TSDB_CODE_ODBC_BAD_CONNSTR, "unrecognized connection string: [%s]", (const char*)szConnStrIn);
      break;
    }
    if (host) {
      char *p = strchr(host, ':');
      if (p) {
        ip = strndup(host, p-host);
        port = atoi(p+1);
      }
    }

    // TODO: data-race
    // TODO: shall receive ip/port from odbc.ini
    conn->taos = taos_connect(ip ? ip : "localhost", userName, auth, NULL, port);
    free(ip); ip = NULL;
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
  if (ColumnSize) {
    *ColumnSize = field->bytes;
  }
  if (DecimalDigits) *DecimalDigits = 0;

  if (DataType) {
    switch (field->type) {
      case TSDB_DATA_TYPE_BOOL: {
        *DataType = SQL_TINYINT;
      }  break;

      case TSDB_DATA_TYPE_TINYINT: {
        *DataType = SQL_TINYINT;
      } break;

      case TSDB_DATA_TYPE_SMALLINT: {
        *DataType = SQL_SMALLINT;
      } break;

      case TSDB_DATA_TYPE_INT: {
        *DataType = SQL_INTEGER;
      } break;

      case TSDB_DATA_TYPE_BIGINT: {
        *DataType = SQL_BIGINT;
      } break;

      case TSDB_DATA_TYPE_FLOAT: {
        *DataType = SQL_FLOAT;
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        *DataType = SQL_DOUBLE;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP: {
        // *DataType = SQL_TIMESTAMP;
        // *ColumnSize = 30;
        // *DecimalDigits = 3;
        *DataType = SQL_TIMESTAMP;
        *ColumnSize = sizeof(SQL_TIMESTAMP_STRUCT);
        *DecimalDigits = 0;
      } break;

      case TSDB_DATA_TYPE_NCHAR: {
        *DataType = SQL_CHAR; // unicode ?
        if (ColumnSize) *ColumnSize -= VARSTR_HEADER_SIZE;
      } break;

      case TSDB_DATA_TYPE_BINARY: {
        *DataType = SQL_CHAR;
        if (ColumnSize) *ColumnSize -= VARSTR_HEADER_SIZE;
      } break;

      default:
        SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT,
                  "unknown [%s[%d/0x%x]]", taos_data_type(field->type), field->type, field->type);
        return SQL_ERROR;
        break;
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

  int insert = 0;
  int r = taos_stmt_is_insert(sql->stmt, &insert);
  if (r) {
    SET_ERROR(sql, "HY000", terrno, "");
    return SQL_ERROR;
  }
  // if (!insert) {
  //   SET_ERROR(sql, "HY000", terrno, "taos does not provide count of parameters for statement other than insert");
  //   return SQL_ERROR;
  // }

  int params = 0;
  r = taos_stmt_num_params(sql->stmt, &params);
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

static SQLRETURN doSQLSetStmtAttr(SQLHSTMT StatementHandle,
                                  SQLINTEGER Attribute, SQLPOINTER Value,
                                  SQLINTEGER StringLength)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_ERROR;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  if (!sql->stmt) {
    SET_ERROR(sql, "HY010", TSDB_CODE_ODBC_STATEMENT_NOT_READY, "");
    return SQL_ERROR;
  }

  if (sql->is_executed) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "change attr after executing statement not supported yet");
    return SQL_ERROR;
  }

  switch (Attribute) {
    case SQL_ATTR_PARAM_BIND_TYPE: {
      SQLULEN val = (SQLULEN)Value;
      if (val==SQL_BIND_BY_COLUMN) {
        sql->rowlen = 0;
        SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "SQL_ATTR_PARAM_BIND_TYPE/SQL_BIND_BY_COLUMN");
        return SQL_ERROR;
      }
      sql->rowlen = val;
      return SQL_SUCCESS;
    } break;
    case SQL_ATTR_PARAMSET_SIZE: {
      SQLULEN val = (SQLULEN)Value;
      DASSERT(val>0);
      sql->n_rows = val;
      return SQL_SUCCESS;
    } break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
      if (Value) {
        SQLULEN val = *(SQLULEN*)Value;
        sql->ptr_offset = val;
      } else {
        sql->ptr_offset = 0;
      }
      return SQL_SUCCESS;
    } break;
    default: {
      SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "Attribute:%d", Attribute);
    } break;
  }
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle,
                                 SQLINTEGER Attribute, SQLPOINTER Value,
                                 SQLINTEGER StringLength)
{
  SQLRETURN r;
  r = doSQLSetStmtAttr(StatementHandle, Attribute, Value, StringLength);
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



