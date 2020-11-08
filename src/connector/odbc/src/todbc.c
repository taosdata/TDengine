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

#include "todbc_log.h"
#include "todbc_flex.h"

#include "taos.h"

#include "tglobal.h"
#include "taoserror.h"
#include "todbc_util.h"
#include "todbc_conv.h"

#include "os.h"

#include <odbcinst.h>
#include <sqlext.h>

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

#define UTF8_ENC     "UTF-8"
#define UTF16_ENC    "UCS-2LE"
#define UNICODE_ENC  "UCS-4LE"
#define GB18030_ENC  "GB18030"

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
  char *err_str = (char*)realloc(obj->err.err_str, (size_t)n+1);                                          \
  if (!err_str) break;                                                                                    \
  obj->err.err_str = err_str;                                                                             \
  snprintf(obj->err.err_str, (size_t)n+1, "[TSDB:%x]%s: @%s[%d]" err_fmt "",                              \
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

#define FILL_ERROR(obj)                                                                          \
do {                                                                                             \
  size_t n = sizeof(obj->err.sql_state);                                                         \
  if (Sqlstate) strncpy((char*)Sqlstate, (char*)obj->err.sql_state, n);                          \
  if (NativeError) *NativeError = obj->err.err_no;                                               \
  snprintf((char*)MessageText, (size_t)BufferLength, "%s", obj->err.err_str);                    \
  if (TextLength && obj->err.err_str) *TextLength = (SQLSMALLINT)utf8_chars(obj->err.err_str);   \
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

#define SDUP(s,n)      (s ? (s[(size_t)n] ? (const char*)strndup((const char*)s,(size_t)n) : (const char*)s) : strdup(""))
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

#define NORM_STR_LENGTH(obj, ptr, len)                                               \
do {                                                                                 \
  if ((len) < 0 && (len)!=SQL_NTS) {                                                 \
    SET_ERROR((obj), "HY090", TSDB_CODE_ODBC_BAD_ARG, "");                           \
    return SQL_ERROR;                                                                \
  }                                                                                  \
  if (len==SQL_NTS) len = (ptr) ? (SQLSMALLINT)strlen((const char*)(ptr)) : 0;       \
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
  delta += (double)(tv1.tv_usec-tv0.tv_usec);         \
  delta /= 1000000;                                   \
  D("%s: elapsed: [%.6f]s", #statement, delta);       \
} while (0)

#define CHK_CONV(todb, statement)                                       \
do {                                                                    \
  TSDB_CONV_CODE code_0c80 = (statement);                               \
  switch (code_0c80) {                                                  \
    case TSDB_CONV_OK: return SQL_SUCCESS;                              \
    case TSDB_CONV_OOM:                                                 \
    case TSDB_CONV_NOT_AVAIL: {                                         \
      SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");                  \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_OOR: {                                               \
      SET_ERROR(sql, "22003", TSDB_CODE_ODBC_CONV_OOR, "");             \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_CHAR_NOT_NUM:                                        \
    case TSDB_CONV_CHAR_NOT_TS: {                                       \
      SET_ERROR(sql, "22018", TSDB_CODE_ODBC_CONV_CHAR_NOT_NUM, "");    \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_NOT_VALID_TS: {                                      \
      SET_ERROR(sql, "22007", TSDB_CODE_ODBC_CONV_NOT_VALID_TS, "");    \
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
    case TSDB_CONV_SRC_TOO_LARGE: {                                     \
      SET_ERROR(sql, "22001", TSDB_CODE_ODBC_CONV_SRC_TOO_LARGE, "");   \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_SRC_BAD_SEQ: {                                       \
      SET_ERROR(sql, "22001", TSDB_CODE_ODBC_CONV_SRC_BAD_SEQ, "");     \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_SRC_INCOMPLETE: {                                    \
      SET_ERROR(sql, "22001", TSDB_CODE_ODBC_CONV_SRC_INCOMPLETE, "");  \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_SRC_GENERAL: {                                       \
      SET_ERROR(sql, "22001", TSDB_CODE_ODBC_CONV_SRC_GENERAL, "");     \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    case TSDB_CONV_BAD_CHAR: {                                          \
      SET_ERROR(sql, "22001", TSDB_CODE_ODBC_CONV_TRUNC, "");           \
      return SQL_ERROR;                                                 \
    } break;                                                            \
    default: {                                                          \
      DASSERTX(0, "internal logic error: %d", code_0c80);                              \
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

  char                    env_locale[64];
  char                    env_charset[64];

  taos_error_t            err;
};

struct conn_s {
  uint64_t                refcount;
  env_t                  *env;

  char                    client_enc[64];  // ODBC client that communicates with this driver
  char                    server_enc[64];  // taos dynamic library that's loaded by this driver

  tsdb_conv_t            *client_to_server;
  tsdb_conv_t            *server_to_client;
  tsdb_conv_t            *utf8_to_client;
  tsdb_conv_t            *utf16_to_utf8;
  tsdb_conv_t            *utf16_to_server;
  tsdb_conv_t            *client_to_utf8;

  TAOS                   *taos;

  taos_error_t            err;
};

struct sql_s {
  uint64_t                refcount;
  conn_t                 *conn;

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

static size_t do_field_display_size(TAOS_FIELD *field);

static tsdb_conv_t* tsdb_conn_client_to_server(conn_t *conn) {
  if (!conn->client_to_server) {
    conn->client_to_server = tsdb_conv_open(conn->client_enc, conn->server_enc);
  }
  return conn->client_to_server;
}

static tsdb_conv_t* tsdb_conn_server_to_client(conn_t *conn) {
  if (!conn->server_to_client) {
    conn->server_to_client = tsdb_conv_open(conn->server_enc, conn->client_enc);
  }
  return conn->server_to_client;
}

static tsdb_conv_t* tsdb_conn_utf8_to_client(conn_t *conn) {
  if (!conn->utf8_to_client) {
    conn->utf8_to_client = tsdb_conv_open(UTF8_ENC, conn->client_enc);
  }
  return conn->utf8_to_client;
}

static tsdb_conv_t* tsdb_conn_utf16_to_utf8(conn_t *conn) {
  if (!conn->utf16_to_utf8) {
    conn->utf16_to_utf8 = tsdb_conv_open(UTF16_ENC, UTF8_ENC);
  }
  return conn->utf16_to_utf8;
}

static tsdb_conv_t* tsdb_conn_utf16_to_server(conn_t *conn) {
  if (!conn->utf16_to_server) {
    conn->utf16_to_server = tsdb_conv_open(UTF16_ENC, conn->server_enc);
  }
  return conn->utf16_to_server;
}

static tsdb_conv_t* tsdb_conn_client_to_utf8(conn_t *conn) {
  if (!conn->client_to_utf8) {
    conn->client_to_utf8 = tsdb_conv_open(conn->client_enc, UTF8_ENC);
  }
  return conn->client_to_utf8;
}

static void tsdb_conn_close_convs(conn_t *conn) {
  if (conn->client_to_server) {
    tsdb_conv_close(conn->client_to_server);
    conn->client_to_server = NULL;
  }
  if (conn->server_to_client) {
    tsdb_conv_close(conn->server_to_client);
    conn->server_to_client = NULL;
  }
  if (conn->utf8_to_client) {
    tsdb_conv_close(conn->utf8_to_client);
    conn->utf8_to_client = NULL;
  }
  if (conn->utf16_to_utf8) {
    tsdb_conv_close(conn->utf16_to_utf8);
    conn->utf16_to_utf8 = NULL;
  }
  if (conn->utf16_to_server) {
    tsdb_conv_close(conn->utf16_to_server);
    conn->utf16_to_server = NULL;
  }
  if (conn->client_to_utf8) {
    tsdb_conv_close(conn->client_to_utf8);
    conn->client_to_utf8 = NULL;
  }
}

#define SFREE(buffer, v, src)                                                           \
do {                                                                                    \
  const char *v_096a = (const char*)(v);                                                \
  const char *src_6a = (const char*)(src);                                              \
  if (v_096a && v_096a!=src_6a && !is_owned_by_stack_buffer((buffer), v_096a)) {        \
    free((char*)v_096a);                                                                \
  }                                                                                     \
} while (0)

static SQLRETURN doSQLAllocEnv(SQLHENV *EnvironmentHandle)
{
  pthread_once(&init_once, init_routine);

  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return SQL_INVALID_HANDLE;

  DASSERT(INC_REF(env)>0);

  snprintf(env->env_locale,  sizeof(env->env_locale),  "%s", tsLocale);
  snprintf(env->env_charset, sizeof(env->env_charset), "%s", tsCharset);

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
  if (!env) return SQL_INVALID_HANDLE;

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
  if (!env) return SQL_INVALID_HANDLE;

  if (!ConnectionHandle) {
    SET_ERROR(env, "HY009", TSDB_CODE_ODBC_BAD_ARG, "ConnectionHandle [%p] not valid", ConnectionHandle);
    return SQL_ERROR;
  }

  DASSERT(INC_REF(env)>1);

  conn_t *conn = NULL;
  do {
    conn = (conn_t*)calloc(1, sizeof(*conn));
    if (!conn) {
      SET_ERROR(env, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }

    conn->env = env;

    snprintf(conn->client_enc, sizeof(conn->client_enc), "%s", conn->env->env_charset);
    snprintf(conn->server_enc, sizeof(conn->server_enc), "%s", conn->env->env_charset);

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
  if (!conn) return SQL_INVALID_HANDLE;

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
    tsdb_conn_close_convs(conn);
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
  stack_buffer_t buffer; buffer.next = 0;

  conn_t *conn = (conn_t*)ConnectionHandle;
  if (!conn) return SQL_ERROR;

  if (conn->taos) {
    SET_ERROR(conn, "08002", TSDB_CODE_ODBC_CONNECTION_BUSY, "connection still in use");
    return SQL_ERROR;
  }

  NORM_STR_LENGTH(conn, ServerName,     NameLength1);
  NORM_STR_LENGTH(conn, UserName,       NameLength2);
  NORM_STR_LENGTH(conn, Authentication, NameLength3);

  if (NameLength1>SQL_MAX_DSN_LENGTH) {
    SET_ERROR(conn, "HY090", TSDB_CODE_ODBC_BAD_ARG, "");
    return SQL_ERROR;
  }

  tsdb_conv_t *client_to_server = tsdb_conn_client_to_server(conn);
  const char *dsn = NULL;
  const char *uid = NULL;
  const char *pwd = NULL;
  const char *svr = NULL;
  char server[4096]; server[0] = '\0';

  do {
    tsdb_conv(client_to_server, &buffer, (const char*)ServerName,     (size_t)NameLength1, &dsn, NULL);
    tsdb_conv(client_to_server, &buffer, (const char*)UserName,       (size_t)NameLength2, &uid, NULL);
    tsdb_conv(client_to_server, &buffer, (const char*)Authentication, (size_t)NameLength3, &pwd, NULL);
    int n = SQLGetPrivateProfileString(dsn, "Server", "", server, sizeof(server)-1, "Odbc.ini");
    if (n<=0) {
      snprintf(server, sizeof(server), "localhost:6030"); // all 7-bit ascii
    }
    tsdb_conv(client_to_server, &buffer, (const char*)server, (size_t)strlen(server), &svr, NULL);

    if ((!dsn) || (!uid) || (!pwd) || (!svr)) {
      SET_ERROR(conn, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }

    char *ip         = NULL;
    int   port       = 0;
    char *p = strchr(svr, ':');
    if (p) {
      ip = strndup(svr, (size_t)(p-svr));
      port = atoi(p+1);
    }

    // TODO: data-race
    // TODO: shall receive ip/port from odbc.ini
    conn->taos = taos_connect(ip, uid, pwd, NULL, (uint16_t)port);
    if (!conn->taos) {
      SET_ERROR(conn, "08001", terrno, "failed to connect to data source for DSN[%s] @[%s:%d]", dsn, ip, port);
      break;
    }
  } while (0);

  tsdb_conv_free(client_to_server, dsn, &buffer, (const char*)ServerName);
  tsdb_conv_free(client_to_server, uid, &buffer, (const char*)UserName);
  tsdb_conv_free(client_to_server, pwd, &buffer, (const char*)Authentication);
  tsdb_conv_free(client_to_server, svr, &buffer, (const char*)server);

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
  if (!conn) return SQL_INVALID_HANDLE;

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
  if (!conn) return SQL_INVALID_HANDLE;

  if (!StatementHandle) {
    SET_ERROR(conn, "HY009", TSDB_CODE_ODBC_BAD_ARG, "StatementHandle [%p] not valid", StatementHandle);
    return SQL_ERROR;
  }

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

static SQLRETURN doSQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  switch (HandleType) {
    case SQL_HANDLE_ENV: {
      SQLHENV env = {0};
      if (!OutputHandle) return SQL_ERROR;
      SQLRETURN r = doSQLAllocEnv(&env);
      if (r==SQL_SUCCESS) *OutputHandle = env;
      return r;
    } break;
    case SQL_HANDLE_DBC: {
      SQLRETURN r = doSQLAllocConnect(InputHandle, OutputHandle);
      return r;
    } break;
    case SQL_HANDLE_STMT: {
      SQLRETURN r = doSQLAllocStmt(InputHandle, OutputHandle);
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
  if (!sql) return SQL_INVALID_HANDLE;

  switch (Option) {
    case SQL_CLOSE: return SQL_SUCCESS;
    case SQL_DROP:  break;
    case SQL_UNBIND:
    case SQL_RESET_PARAMS: {
      SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NOT_SUPPORT, "free statement with Option[%x] not supported yet", Option);
      return SQL_ERROR;
    } break;
    default: {
      SET_ERROR(sql, "HY092", TSDB_CODE_ODBC_OUT_OF_RANGE, "free statement with Option[%x] not supported yet", Option);
      return SQL_ERROR;
    } break;
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

static SQLRETURN do_exec_direct(sql_t *sql, TSDB_CONV_CODE code, const char *statement) {
  if (code) CHK_CONV(1, code);
  DASSERT(code==TSDB_CONV_OK);

  SQLRETURN r = SQL_ERROR;
  do {
    sql->rs = taos_query(sql->conn->taos, statement);
    CHK_RS(r, sql, "failed to execute");
  } while (0);

  return r;
}

static SQLRETURN doSQLExecDirect(SQLHSTMT StatementHandle,
                                 SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_INVALID_HANDLE;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  conn_t *conn = sql->conn;

  NORM_STR_LENGTH(sql, StatementText, TextLength);

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

  SQLRETURN r = SQL_SUCCESS;
  stack_buffer_t buffer; buffer.next = 0;
  tsdb_conv_t *client_to_server = tsdb_conn_client_to_server(conn);
  const char *stxt = NULL;
  do {
    TSDB_CONV_CODE code = tsdb_conv(client_to_server, &buffer, (const char*)StatementText, (size_t)TextLength, &stxt, NULL);
    r = do_exec_direct(sql, code, stxt);
  } while (0);
  tsdb_conv_free(client_to_server, stxt, &buffer, (const char*)StatementText);

  return r;
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  SQLRETURN r;
  r = doSQLExecDirect(StatementHandle, StatementText, TextLength);
  return r;
}

static SQLRETURN doSQLExecDirectW(SQLHSTMT hstmt, SQLWCHAR *szSqlStr, SQLINTEGER cbSqlStr)
{
  sql_t *sql = (sql_t*)hstmt;
  if (!sql) return SQL_INVALID_HANDLE;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  conn_t *conn = sql->conn;

  if (!szSqlStr) {
    SET_ERROR(sql, "HY009", TSDB_CODE_ODBC_BAD_ARG, "szSqlStr [%p] not allowed", szSqlStr);
    return SQL_ERROR;
  }
  if (cbSqlStr < 0) {
    SET_ERROR(sql, "HY090", TSDB_CODE_ODBC_BAD_ARG, "");
    return SQL_ERROR;
  }

  SQLRETURN r = SQL_SUCCESS;
  stack_buffer_t buffer; buffer.next = 0;
  tsdb_conv_t *utf16_to_server = tsdb_conn_utf16_to_server(conn);
  const char *stxt = NULL;
  do {
    size_t slen = (size_t)cbSqlStr * sizeof(*szSqlStr);
    TSDB_CONV_CODE code = tsdb_conv(utf16_to_server, &buffer, (const char*)szSqlStr, slen, &stxt, NULL);
    r = do_exec_direct(sql, code, stxt);
  } while (0);
  tsdb_conv_free(utf16_to_server, stxt, &buffer, (const char*)szSqlStr);

  return r;
}

SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT hstmt, SQLWCHAR *szSqlStr, SQLINTEGER cbSqlStr)
{
  SQLRETURN r = doSQLExecDirectW(hstmt, szSqlStr, cbSqlStr);
  return r;
}

static SQLRETURN doSQLNumResultCols(SQLHSTMT StatementHandle,
                                    SQLSMALLINT *ColumnCount)
{
  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_INVALID_HANDLE;

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
    *ColumnCount = (SQLSMALLINT)fields;
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

  // ref: https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlrowcount-function?view=sql-server-ver15
  // Summary
  // SQLRowCount returns the number of rows affected by an UPDATE, INSERT, or DELETE statement;
  // an SQL_ADD, SQL_UPDATE_BY_BOOKMARK, or SQL_DELETE_BY_BOOKMARK operation in SQLBulkOperations;
  // or an SQL_UPDATE or SQL_DELETE operation in SQLSetPos.

  // how to fetch affected rows from taos?
  // taos_affected_rows?

  if (1) {
    SET_ERROR(sql, "IM001", TSDB_CODE_ODBC_NOT_SUPPORT, "");
    // if (RowCount) *RowCount = 0;
    return SQL_SUCCESS_WITH_INFO;
  }

  if (!sql->is_insert) {
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
      *NumericAttribute = (SQLLEN)do_field_display_size(field);
    } break;
    case SQL_COLUMN_LABEL: {
      // todo: check BufferLength
      size_t n = sizeof(field->name);
      strncpy(CharacterAttribute, field->name, (n>BufferLength ? (size_t)BufferLength : n));
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
  if (!sql) return SQL_INVALID_HANDLE;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  conn_t *conn = sql->conn;

  if (!sql->rs) {
    SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_NO_RESULT, "");
    return SQL_ERROR;
  }

  if (!sql->row) {
    SET_ERROR(sql, "24000", TSDB_CODE_ODBC_INVALID_CURSOR, "");
    return SQL_ERROR;
  }

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
  if (BufferLength<0) {
    SET_ERROR(sql, "HY090", TSDB_CODE_ODBC_BAD_ARG, "");
    return SQL_ERROR;
  }

  TAOS_FIELD *field = fields + ColumnNumber-1;
  void *row = sql->row[ColumnNumber-1];

  if (!row) {
    if (!StrLen_or_Ind) {
      SET_ERROR(sql, "22002", TSDB_CODE_ODBC_BAD_ARG, "NULL StrLen_or_Ind not allowed for col [%d]", ColumnNumber);
      return SQL_ERROR;
    }
    *StrLen_or_Ind = SQL_NULL_DATA;
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
          tsdb_conv_t *utf8_to_client = tsdb_conn_utf8_to_client(conn);
          size_t len = (size_t)BufferLength;
          TSDB_CONV_CODE code = tsdb_conv_write_int64(utf8_to_client, v, (char*)TargetValue, &len);
          if (StrLen_or_Ind) *StrLen_or_Ind = (SQLLEN)len;
          CHK_CONV(0, code);
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
          tsdb_conv_t *utf8_to_client = tsdb_conn_utf8_to_client(conn);
          size_t len = (size_t)BufferLength;
          TSDB_CONV_CODE code = tsdb_conv_write_double(utf8_to_client, v, (char*)TargetValue, &len);
          if (StrLen_or_Ind) *StrLen_or_Ind = (SQLLEN)len;
          CHK_CONV(0, code);
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
          tsdb_conv_t *utf8_to_client = tsdb_conn_utf8_to_client(conn);
          size_t len = (size_t)BufferLength;
          TSDB_CONV_CODE code = tsdb_conv_write_double(utf8_to_client, v, (char*)TargetValue, &len);
          if (StrLen_or_Ind) *StrLen_or_Ind = (SQLLEN)len;
          CHK_CONV(0, code);
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
      struct tm vtm = {0};
      localtime_r(&t, &vtm);
      ts.year     = (SQLSMALLINT)(vtm.tm_year + 1900);
      ts.month    = (SQLUSMALLINT)(vtm.tm_mon + 1);
      ts.day      = (SQLUSMALLINT)(vtm.tm_mday);
      ts.hour     = (SQLUSMALLINT)(vtm.tm_hour);
      ts.minute   = (SQLUSMALLINT)(vtm.tm_min);
      ts.second   = (SQLUSMALLINT)(vtm.tm_sec);
      ts.fraction = (SQLUINTEGER)(v%1000 * 1000000);
      switch (target.ct) {
        case SQL_C_SBIGINT: {
          *(int64_t*)TargetValue = v;
          return SQL_SUCCESS;
        } break;
        case SQL_C_CHAR: {
          tsdb_conv_t *utf8_to_client = tsdb_conn_utf8_to_client(conn);
          size_t len = (size_t)BufferLength;
          TSDB_CONV_CODE code = tsdb_conv_write_timestamp(utf8_to_client, ts, (char*)TargetValue, &len);
          if (StrLen_or_Ind) *StrLen_or_Ind = (SQLLEN)len;
          CHK_CONV(0, code);
        } break;
        case SQL_C_TYPE_TIMESTAMP: {
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
      size_t field_bytes = (size_t)field->bytes;
      field_bytes -= VARSTR_HEADER_SIZE;
      switch (target.ct) {
        case SQL_C_CHAR: {
          // taos cares nothing about what would be stored in 'binary' as most sql implementations do
          // but the client requires to fetch it as a SQL_C_CHAR
          // thus, we first try to decode binary to client charset
          // if failed, we then do hex-serialization

          tsdb_conv_t *server_to_client = tsdb_conn_server_to_client(conn);
          size_t slen = strnlen((const char*)row, field_bytes);
          size_t len = (size_t)BufferLength;
          TSDB_CONV_CODE code = tsdb_conv_write(server_to_client,
                                    (const char*)row, &slen,
                                    (char*)TargetValue, &len);
          if (code==TSDB_CONV_OK) {
            if (StrLen_or_Ind) *StrLen_or_Ind = (SQLLEN)((size_t)BufferLength - len);
            CHK_CONV(0, code);
            // code never reached here
          }

          // todo: hex-serialization
          const char *bad = "<bad-charset>";
          int n = snprintf((char*)TargetValue, (size_t)BufferLength, "%s", bad);
          // what if n < 0 ?
          if (StrLen_or_Ind) *StrLen_or_Ind = n;
          CHK_CONV(0, n>=BufferLength ? TSDB_CONV_TRUNC : TSDB_CONV_OK);
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
      size_t field_bytes = (size_t)field->bytes;
      field_bytes -= VARSTR_HEADER_SIZE;
      switch (target.ct) {
        case SQL_C_CHAR: {
          tsdb_conv_t *server_to_client = tsdb_conn_server_to_client(conn);
          size_t slen = strnlen((const char*)row, field_bytes);
          size_t len = (size_t)BufferLength;
          TSDB_CONV_CODE code = tsdb_conv_write(server_to_client,
                                   (const char*)row, &slen,
                                   (char*)TargetValue, &len);
          if (StrLen_or_Ind) *StrLen_or_Ind = (SQLLEN)((size_t)BufferLength - len);
          CHK_CONV(0, code);
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
  if (!sql) return SQL_INVALID_HANDLE;

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
  stack_buffer_t buffer; buffer.next = 0;

  sql_t *sql = (sql_t*)StatementHandle;
  if (!sql) return SQL_INVALID_HANDLE;

  CHK_CONN(sql);
  CHK_CONN_TAOS(sql);

  conn_t *conn = sql->conn;

  NORM_STR_LENGTH(sql, StatementText, TextLength);

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

    tsdb_conv_t *client_to_server = tsdb_conn_client_to_server(conn);
    const char *stxt = NULL;
    int ok = 0;
    do {
      tsdb_conv(client_to_server, &buffer, (const char*)StatementText, (size_t)TextLength, &stxt, NULL);
      if ((!stxt)) {
        SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
        break;
      }

      int r = taos_stmt_prepare(sql->stmt, stxt, (unsigned long)strlen(stxt));
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
        param_bind_t *ar = (param_bind_t*)calloc(1, ((size_t)params) * sizeof(*ar));
        if (!ar) {
          SET_ERROR(sql, "HY001", TSDB_CODE_ODBC_OOM, "");
          break;
        }
        sql->params = ar;
      }

      sql->n_params = params;

      ok = 1;
    } while (0);

    tsdb_conv_free(client_to_server, stxt, &buffer, (const char*)StatementText);

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
  if (param->ParameterValue==NULL) {
    SET_ERROR(sql, "HY009", TSDB_CODE_ODBC_BAD_ARG, "ParameterValue [@%p] not allowed", param->ParameterValue);
    return SQL_ERROR;
  }
  if (param->StrLen_or_Ind==NULL) {
    SET_ERROR(sql, "HY009", TSDB_CODE_ODBC_BAD_ARG, "StrLen_or_Ind [@%p] not allowed", param->StrLen_or_Ind);
    return SQL_ERROR;
  }

  conn_t *conn = sql->conn;

  unsigned char *paramValue = param->ParameterValue;
  SQLSMALLINT    valueType  = param->ValueType;
  SQLLEN        *soi        = param->StrLen_or_Ind;

  size_t offset = ((size_t)idx_row) * sql->rowlen + sql->ptr_offset;

  paramValue += offset;
  soi = (SQLLEN*)((char*)soi + offset);


  if (*soi == SQL_NULL_DATA) {
    bind->is_null = (int*)&yes;
    return SQL_SUCCESS;
  }
  bind->is_null = (int*)&no;
  int tsdb_type = 0;   // taos internal data tsdb_type to be bound to
  int tsdb_bytes = 0;  // we don't rely much on 'tsdb_bytes' here, we delay until taos to check it internally
  if (sql->is_insert) {
    int r = taos_stmt_get_param(sql->stmt, idx, &tsdb_type, &tsdb_bytes);
    if (r) {
      SET_ERROR(sql, "HY000", TSDB_CODE_ODBC_OUT_OF_RANGE, "parameter [@%d] not valid", idx+1);
      return SQL_ERROR;
    }
  } else {
    // we don't have correspondent data type from taos api
    // we have to give a good guess here
    switch (valueType) {
      case SQL_C_BIT: {
        tsdb_type = TSDB_DATA_TYPE_BOOL;
      } break;
      case SQL_C_STINYINT:
      case SQL_C_TINYINT: {
        tsdb_type = TSDB_DATA_TYPE_TINYINT;
      } break;
      case SQL_C_SSHORT:
      case SQL_C_SHORT: {
        tsdb_type = TSDB_DATA_TYPE_SMALLINT;
      } break;
      case SQL_C_SLONG:
      case SQL_C_LONG: {
        tsdb_type = TSDB_DATA_TYPE_INT;
      } break;
      case SQL_C_SBIGINT: {
        tsdb_type = TSDB_DATA_TYPE_BIGINT;
      } break;
      case SQL_C_FLOAT: {
        tsdb_type = TSDB_DATA_TYPE_FLOAT;
      } break;
      case SQL_C_DOUBLE: {
        tsdb_type = TSDB_DATA_TYPE_DOUBLE;
      } break;
      case SQL_C_TIMESTAMP: {
        tsdb_type = TSDB_DATA_TYPE_TIMESTAMP;
      } break;
      case SQL_C_CHAR: {
        tsdb_type = TSDB_DATA_TYPE_BINARY;
        tsdb_bytes = SQL_NTS;
      } break;
      case SQL_C_WCHAR: {
        tsdb_type = TSDB_DATA_TYPE_NCHAR;
        tsdb_bytes = SQL_NTS;
      } break;
      case SQL_C_USHORT:
      case SQL_C_ULONG:
      case SQL_C_UTINYINT:
      case SQL_C_UBIGINT:
      case SQL_C_BINARY:
      case SQL_C_DATE:
      case SQL_C_TIME:
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
  switch (tsdb_type) {
    case TSDB_DATA_TYPE_BOOL: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_bit(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.b));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_bit(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.b));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_TINYINT: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_tinyint(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v1));
          // CHK_CONV(1, tsdb_chars_to_tinyint((const char *)paramValue, (size_t)*soi, &bind->u.v1));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_tinyint(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v1));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_SMALLINT: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_smallint(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v2));
          // CHK_CONV(1, tsdb_chars_to_smallint((const char*)paramValue, (size_t)*soi, &bind->u.v2));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_smallint(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v2));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_INT: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_int(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v4));
          // CHK_CONV(1, tsdb_chars_to_int((const char*)paramValue, (size_t)*soi, &bind->u.v4));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_int(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v4));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_BIGINT: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_bigint(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v8));
          // CHK_CONV(1, tsdb_chars_to_bigint((const char*)paramValue, (size_t)*soi, &bind->u.v8));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_bigint(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v8));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_FLOAT: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_float(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.f4));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_float(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.f4));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_DOUBLE: {
      bind->buffer_type = tsdb_type;
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
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          CHK_CONV(1, tsdb_conv_chars_to_double(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.f8));
          // CHK_CONV(1, tsdb_chars_to_double((const char*)paramValue, (size_t)*soi, &bind->u.f8));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_double(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.f8));
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
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      bind->buffer_type = tsdb_type;
      bind->buffer_length = sizeof(bind->u.v8);
      bind->buffer = &bind->u.v8;
      bind->length = &bind->buffer_length;
      switch (valueType) {
        case SQL_C_CHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *client_to_utf8 = tsdb_conn_client_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_timestamp_ts(client_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v8));
        } break;
        case SQL_C_WCHAR: {
          stack_buffer_t buffer; buffer.next = 0;
          tsdb_conv_t *utf16_to_utf8 = tsdb_conn_utf16_to_utf8(conn);
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          CHK_CONV(1, tsdb_conv_chars_to_timestamp_ts(utf16_to_utf8, &buffer, (const char *)paramValue, slen, &bind->u.v8));
        } break;
        case SQL_C_SBIGINT: {
          int64_t t = *(int64_t*)paramValue;
          bind->u.v8 = t;
        } break;
        case SQL_C_TYPE_TIMESTAMP: {
          SQL_TIMESTAMP_STRUCT ts = *(SQL_TIMESTAMP_STRUCT*)paramValue;
          struct tm vtm = {0};
          vtm.tm_year     = ts.year - 1900;
          vtm.tm_mon      = ts.month - 1;
          vtm.tm_mday     = ts.day;
          vtm.tm_hour     = ts.hour;
          vtm.tm_min      = ts.minute;
          vtm.tm_sec      = ts.second;
          int64_t t = (int64_t) mktime(&vtm);
          if (t==-1) {
            CHK_CONV(1, TSDB_CONV_NOT_VALID_TS);
            // code never reached here
          }
          bind->u.ts  = t * 1000 + ts.fraction / 1000000;
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
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(valueType), valueType, valueType,
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_BINARY: {
      bind->buffer_type = tsdb_type;
      bind->length = &bind->buffer_length;
      switch (valueType) {
        case SQL_C_WCHAR: {
          // taos cares nothing about what would be stored in 'binary' as most sql implementations do
          // thus, we just copy it as is
          // it's caller's responsibility to maintain data-consistency
          // if he/she is going to use 'binary' to store characters
          // taos might extend it's sql syntax to let user specify
          // what charset is to be used for specific 'binary' field when
          // table is to be created
          // in such way, 'binary' would be 'internationalized'
          // but actually speaking, normally, 'char' field is a better
          // one for this purpose
          size_t slen = (size_t)*soi;
          DASSERT(slen != SQL_NTS);
          bind->u.bin = (unsigned char*)malloc(slen + 1); // add null-terminator, just for case of use
          if (!bind->u.bin) {
            CHK_CONV(1, TSDB_CONV_OOM);
            // code never reached here
          }
          memcpy(bind->u.bin, paramValue, slen);
          bind->buffer_length = slen;
          bind->buffer = bind->u.bin;
          CHK_CONV(1, TSDB_CONV_OK);

          // tsdb_conv_t *utf16_to_server = tsdb_conn_utf16_to_server(conn);
          // size_t slen = (size_t)*soi;
          // DASSERT(slen != SQL_NTS);
          // const char *buf = NULL;
          // size_t blen = 0;
          // TSDB_CONV_CODE code = tsdb_conv(utf16_to_server, NULL, (const char *)paramValue, slen, &buf, &blen);
          // if (code==TSDB_CONV_OK) {
          //   if (buf!=(const char*)paramValue) {
          //     bind->allocated = 1;
          //   }
          //   bind->u.bin = (unsigned char*)buf;
          //   bind->buffer_length = blen;
          //   bind->buffer = bind->u.bin;
          // }
          // CHK_CONV(1, code);
        } break;
        case SQL_C_CHAR: {
          // taos cares nothing about what would be stored in 'binary' as most sql implementations do
          // thus, we just copy it as is
          // it's caller's responsibility to maintain data-consistency
          // if he/she is going to use 'binary' to store characters
          // taos might extend it's sql syntax to let user specify
          // what charset is to be used for specific 'binary' field when
          // table is to be created
          // in such way, 'binary' would be 'internationalized'
          // but actually speaking, normally, 'char' field is a better
          // one for this purpose
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          // we can not use strndup, because ODBC client might pass in a buffer without null-terminated
          bind->u.bin = (unsigned char*)malloc(slen + 1); // add null-terminator, just for case of use
          if (!bind->u.bin) {
            CHK_CONV(1, TSDB_CONV_OOM);
            // code never reached here
          }
          memcpy(bind->u.bin, paramValue, slen);
          bind->buffer_length = slen;
          bind->buffer = bind->u.bin;
          CHK_CONV(1, TSDB_CONV_OK);
          // code never reached here

          // tsdb_conv_t *client_to_server = tsdb_conn_client_to_server(conn);
          // size_t slen = (size_t)*soi;
          // if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          // const char *buf = NULL;
          // size_t blen = 0;
          // TSDB_CONV_CODE code = tsdb_conv(client_to_server, NULL, (const char *)paramValue, slen, &buf, &blen);
          // if (code==TSDB_CONV_OK) {
          //   if (buf!=(const char*)paramValue) {
          //     bind->allocated = 1;
          //   }
          //   bind->u.bin = (unsigned char*)buf;
          //   bind->buffer_length = blen;
          //   bind->buffer = bind->u.bin;
          // }
          // CHK_CONV(1, code);
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
        case SQL_C_TYPE_TIMESTAMP: // we don't provide auto-converstion
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(valueType), valueType, valueType,
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    case TSDB_DATA_TYPE_NCHAR: {
      bind->buffer_type = tsdb_type;
      bind->length = &bind->buffer_length;
      switch (valueType) {
        case SQL_C_WCHAR: {
          tsdb_conv_t *utf16_to_server = tsdb_conn_utf16_to_server(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          const char *buf = NULL;
          size_t blen = 0;
          TSDB_CONV_CODE code = tsdb_conv(utf16_to_server, NULL, (const char *)paramValue, slen, &buf, &blen);
          if (code==TSDB_CONV_OK) {
            if (buf!=(const char*)paramValue) {
              bind->allocated = 1;
            }
            bind->u.nchar = (char*)buf;
            bind->buffer_length = blen;
            bind->buffer = bind->u.nchar;
          }
          CHK_CONV(1, code);
        } break;
        case SQL_C_CHAR: {
          tsdb_conv_t *client_to_server = tsdb_conn_client_to_server(conn);
          size_t slen = (size_t)*soi;
          if (slen==SQL_NTS) slen = strlen((const char*)paramValue);
          const char *buf = NULL;
          size_t blen = 0;
          TSDB_CONV_CODE code = tsdb_conv(client_to_server, NULL, (const char *)paramValue, slen, &buf, &blen);
          if (code==TSDB_CONV_OK) {
            if (buf!=(const char*)paramValue) {
              bind->allocated = 1;
            }
            bind->u.bin = (unsigned char*)buf;
            bind->buffer_length = blen;
            bind->buffer = bind->u.bin;
          }
          CHK_CONV(1, code);
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
        case SQL_C_TYPE_TIMESTAMP: // we don't provide auto-converstion
        case SQL_C_NUMERIC:
        case SQL_C_GUID:
        default: {
          SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                    "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                    sql_c_type(valueType), valueType, valueType,
                    taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
          return SQL_ERROR;
        } break;
      }
		} break;
    default: {
      SET_ERROR(sql, "HYC00", TSDB_CODE_ODBC_OUT_OF_RANGE,
                "no convertion from [%s[%d/0x%x]] to [%s[%d/0x%x]] for parameter [%d]",
                sql_c_type(valueType), valueType, valueType,
                taos_data_type(tsdb_type), tsdb_type, tsdb_type, idx+1);
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
      binds = (TAOS_BIND*)calloc((size_t)sql->n_params, sizeof(*binds));
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
  // if (sql->is_insert) return SQL_SUCCESS;

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
  switch (DiagIdentifier) {
    case SQL_DIAG_CLASS_ORIGIN: {
      *StringLength = 0;
    } break;
  }
  return SQL_SUCCESS;
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
  if (!sql) return SQL_INVALID_HANDLE;

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

  if (ParameterValue==NULL) {
    SET_ERROR(sql, "HY009", TSDB_CODE_ODBC_BAD_ARG, "ParameterValue [@%p] not allowed", ParameterValue);
    return SQL_ERROR;
  }

  if (StrLen_or_Ind==NULL) {
    SET_ERROR(sql, "HY009", TSDB_CODE_ODBC_BAD_ARG, "StrLen_or_Ind [@%p] not allowed", StrLen_or_Ind);
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
  if (!conn) return SQL_INVALID_HANDLE;

  if (conn->taos) {
    SET_ERROR(conn, "08002", TSDB_CODE_ODBC_CONNECTION_BUSY, "connection still in use");
    return SQL_ERROR;
  }

  if (fDriverCompletion!=SQL_DRIVER_NOPROMPT) {
    SET_ERROR(conn, "HYC00", TSDB_CODE_ODBC_NOT_SUPPORT, "option[%d] other than SQL_DRIVER_NOPROMPT not supported yet", fDriverCompletion);
    return SQL_ERROR;
  }

  NORM_STR_LENGTH(conn, szConnStrIn, cbConnStrIn);

  // DSN=<dsn>; UID=<uid>; PWD=<pwd>

  const char *connStr    = SDUP(szConnStrIn, cbConnStrIn);

  conn_val_t val   = {0};

  do {
    if (szConnStrIn && !connStr) {
      SET_ERROR(conn, "HY001", TSDB_CODE_ODBC_OOM, "");
      break;
    }

    int n = todbc_parse_conn_string((const char *)connStr, &val);
    if (n) {
      SET_ERROR(conn, "HY000", TSDB_CODE_ODBC_BAD_CONNSTR, "unrecognized connection string: [%s]", (const char*)szConnStrIn);
      break;
    }
    char *ip         = NULL;
    int   port       = 0;
    if (val.server) {
      char *p = strchr(val.server, ':');
      if (p) {
        ip = strndup(val.server, (size_t)(p-val.server));
        port = atoi(p+1);
      }
    }

    if ((val.cli_enc && strcmp(val.cli_enc, conn->client_enc)) ||
        (val.svr_enc && strcmp(val.svr_enc, conn->server_enc)) )
    {
      tsdb_conn_close_convs(conn);
      if (val.cli_enc) {
        snprintf(conn->client_enc, sizeof(conn->client_enc), "%s", val.cli_enc);
      }
      if (val.svr_enc) {
        snprintf(conn->server_enc, sizeof(conn->server_enc), "%s", val.svr_enc);
      }
    }

    // TODO: data-race
    // TODO: shall receive ip/port from odbc.ini
    // shall we support non-ansi uid/pwd/db etc?
    conn->taos = taos_connect(ip ? ip : "localhost", val.uid, val.pwd, val.db, (uint16_t)port);
    free(ip); ip = NULL;
    if (!conn->taos) {
      SET_ERROR(conn, "HY000", terrno, "failed to connect to data source");
      break;
    }

    if (szConnStrOut) {
      snprintf((char*)szConnStrOut, (size_t)cbConnStrOutMax, "%s", connStr);
    }
    if (pcbConnStrOut) {
      *pcbConnStrOut = cbConnStrIn;
    }
  } while (0);

  conn_val_reset(&val);

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
  if (!conn) return SQL_INVALID_HANDLE;

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
  if (!sql) return SQL_INVALID_HANDLE;

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
  if (BufferLength<0) {
    SET_ERROR(sql, "HY090", TSDB_CODE_ODBC_BAD_ARG, "");
    return SQL_ERROR;
  }

  TAOS_FIELD *field = fields + ColumnNumber - 1;
  if (ColumnName) {
    size_t n = sizeof(field->name);
    if (n>BufferLength) n = (size_t)BufferLength;
    strncpy((char*)ColumnName, field->name, n);
  }
  if (NameLength) {
    *NameLength = (SQLSMALLINT)strnlen(field->name, sizeof(field->name));
  }
  if (ColumnSize) *ColumnSize = (SQLULEN)field->bytes;
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
        *DataType = SQL_TIMESTAMP;
        if (ColumnSize) *ColumnSize = sizeof(SQL_TIMESTAMP_STRUCT);
        if (DecimalDigits) *DecimalDigits = 0;
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
  if (!sql) return SQL_INVALID_HANDLE;

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

  if (pcpar) *pcpar = (SQLSMALLINT)params;

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
  if (!sql) return SQL_INVALID_HANDLE;

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

#ifdef _MSC_VER

#define POST_INSTALLER_ERROR(hwndParent, code, fmt, ...)             \
do {                                                                 \
  char buf[4096];                                                    \
  snprintf(buf, sizeof(buf), "%s[%d]%s():" fmt "",                   \
           basename((char*)__FILE__), __LINE__, __func__,            \
           ##__VA_ARGS__);                                           \
  SQLPostInstallerError(code, buf);                                  \
  if (hwndParent) {                                                  \
    MessageBox(hwndParent, buf, "Error", MB_OK|MB_ICONEXCLAMATION);  \
  }                                                                  \
} while (0)

typedef struct kv_s           kv_t;
struct kv_s {
  char    *line;
  size_t   val;
};

static BOOL get_driver_dll_path(HWND hwndParent, char *buf, size_t len)
{
  HMODULE hm = NULL;

  if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
          (LPCSTR) &ConfigDSN, &hm) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleHandle failed, error = %d\n", ret);
      return FALSE;
  }
  if (GetModuleFileName(hm, buf, (DWORD)len) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleFileName failed, error = %d\n", ret);
      return FALSE;
  }
  return TRUE;
}

static BOOL doDSNAdd(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = TRUE;

  kv_t *kvs = NULL;

  kv_t dsn = {0};
  char *line = NULL;

  do {
    char driver_dll[MAX_PATH + 1];
    r = get_driver_dll_path(hwndParent, driver_dll, sizeof(driver_dll));
    if (!r) break;

    dsn.line = strdup("DSN=TAOS_DEMO");
    if (!dsn.line) { r = FALSE; break; }

    const char *p = lpszAttributes;
    int ikvs = 0;
    while (p && *p) {
      line = strdup(p);
      if (!line) { r = FALSE; break; }
      char *v = strchr(line, '=');
      if (!v) { r = FALSE; break; }

      if (strstr(line, "DSN")==line) {
        if (dsn.line) {
          free(dsn.line);
          dsn.line = NULL;
          dsn.val  = 0;
        }
        dsn.line = line;
        line = NULL;
      } else {
        kv_t *t = (kv_t*)realloc(kvs, (ikvs+1)*sizeof(*t));
        if (!t) { r = FALSE; free(line); break; }
        t[ikvs].line = line;
        *v = '\0';
        if (v) t[ikvs].val = v - line + 1;
        line = NULL;

        kvs = t;
        ++ikvs;
      }

      p += strlen(p) + 1;
    }

    if (hwndParent) {
      MessageBox(hwndParent, "Please use odbcconf to add DSN for TAOS ODBC Driver", "Warning!", MB_OK|MB_ICONEXCLAMATION);
    }
    if (!r) break;

    char *v = NULL;
    v = strchr(dsn.line, '=');
    if (!v) { r = FALSE; break; }
    *v = '\0';
    dsn.val = v - dsn.line + 1;

    if ((!dsn.line)) {
      if (!r) POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "lack of either DSN or Driver");
    } else {
      if (r) r = SQLWritePrivateProfileString("ODBC Data Sources", dsn.line+dsn.val, lpszDriver, "Odbc.ini");
      if (r) r = SQLWritePrivateProfileString(dsn.line+dsn.val, "Driver", driver_dll, "Odbc.ini");
    }

    for (int i=0; r && i<ikvs; ++i) {
      const char *k = kvs[i].line;
      const char *v = NULL;
      if (kvs[i].val) v = kvs[i].line + kvs[i].val;
      r = SQLWritePrivateProfileString(dsn.line+dsn.val, k, v, "Odbc.ini");
    }
  } while (0);

  if (dsn.line) free(dsn.line);
  if (line) free(line);

  return r;
}

static BOOL doDSNConfig(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  const char *p = lpszAttributes;
  while (p && *p) {
    p += strlen(p) + 1;
  }
  return FALSE;
}

static BOOL doDSNRemove(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = TRUE;

  kv_t dsn = {0};
  char *line = NULL;

  do {
    const char *p = lpszAttributes;
    int ikvs = 0;
    while (p && *p) {
      line = strdup(p);
      if (!line) { r = FALSE; break; }
      char *v = strchr(line, '=');
      if (!v) { r = FALSE; break; }
      *v = '\0';

      if (strstr(line, "DSN")==line) {
        if (dsn.line) {
          free(dsn.line);
          dsn.line = NULL;
          dsn.val  = 0;
        }
        dsn.line = line;
        dsn.val = v - line + 1;
        line = NULL;
        break;
      } else {
        free(line);
        line = NULL;
      }

      p += strlen(p) + 1;
    }

    if (!r) break;

    if (!dsn.line) {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "lack of DSN");
      r = FALSE;
      break;
    }

    r = SQLWritePrivateProfileString("ODBC Data Sources", dsn.line+dsn.val, NULL, "Odbc.ini");
    if (!r) break;

    char buf[8192];
    r = SQLGetPrivateProfileString(dsn.line+dsn.val, NULL, "null", buf, sizeof(buf), "Odbc.ini");
    if (!r) break;

    int n = 0;
    char *s = buf;
    while (s && *s && n++<10) {
      SQLWritePrivateProfileString(dsn.line+dsn.val, s, NULL, "Odbc.ini");
      s += strlen(s) + 1;
    }
  } while (0);

  if (dsn.line) free(dsn.line);
  if (line) free(line);
  return r;
}

static BOOL doConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = FALSE;
  const char *sReq = NULL;
  switch(fRequest) {
    case ODBC_ADD_DSN:    sReq = "ODBC_ADD_DSN";      break;
    case ODBC_CONFIG_DSN: sReq = "ODBC_CONFIG_DSN";   break;
    case ODBC_REMOVE_DSN: sReq = "ODBC_REMOVE_DSN";   break;
    default:              sReq = "UNKNOWN";           break;
  }
  switch(fRequest) {
    case ODBC_ADD_DSN: {
      r = doDSNAdd(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_CONFIG_DSN: {
      r = doDSNConfig(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_REMOVE_DSN: {
      r = doDSNRemove(hwndParent, lpszDriver, lpszAttributes);
    } break;
    default: {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
      r = FALSE;
    } break;
  }
  return r;
}

BOOL INSTAPI ConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r;
  r = doConfigDSN(hwndParent, fRequest, lpszDriver, lpszAttributes);
  return r;
}

BOOL INSTAPI ConfigTranslator(HWND hwndParent, DWORD *pvOption)
{
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszArgs,
                          LPSTR lpszMsg, WORD cbMsgMax, WORD *pcbMsgOut)
{
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

#endif // _MSC_VER


static void init_routine(void) {
  taos_init();
}

static size_t do_field_display_size(TAOS_FIELD *field) {
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
      return 3*((size_t)field->bytes - VARSTR_HEADER_SIZE) + 2;
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



