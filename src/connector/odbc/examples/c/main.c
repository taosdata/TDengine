#include "../../src/todbc_log.h"

#ifdef _MSC_VER
#include <winsock2.h>
#include <windows.h>
#include "os.h"
#endif
#include <sql.h>
#include <sqlext.h>
#include <odbcinst.h>

#include <stdio.h>
#include <string.h>

#define CHK_TEST(statement)                       \
do {                                              \
  D("testing: %s", #statement);                   \
  int r = (statement);                            \
  if (r) {                                        \
    D("testing failed: %s", #statement);          \
    return 1;                                     \
  }                                               \
} while (0);

typedef struct {
  int         batch_size;
  int         batchs;
  int         keep_stmt_among_batchs;
} insert_arg_t;

typedef struct db_column_s          db_column_t;
struct db_column_s {
  SQLSMALLINT             nameLength;
  char                    name[4096]; // seems enough
  SQLSMALLINT             dataType;
  SQLULEN                 columnSize;
  SQLSMALLINT             decimalDigits;
  SQLSMALLINT             nullable;
};

static db_column_t       *columns = NULL;

typedef struct data_s            data_t;
struct data_s {
  int64_t                 ts;
  int8_t                  b;
  int8_t                  v1;
  int16_t                 v2;
  int32_t                 v4;
  int64_t                 v8;
  float                   f4;
  double                  f8;
  char                    bin[40+1];
  char                    blob[40+1]; // why 80?  ref: tests/examples/c/apitest.c
};

#define CHK_RESULT(r, ht, h, fmt, ...)                                          \
do {                                                                            \
  if (r==0) break;                                                              \
  SQLSMALLINT i_0381 = 1;                                                       \
  while (1) {                                                                   \
    SQLCHAR      ss[10];                                                        \
    SQLINTEGER   ne = 0;                                                        \
    SQLCHAR      es[4096];                                                      \
    SQLSMALLINT  n  = 0;                                                        \
    ss[0] = '\0';                                                               \
    es[0] = '\0';                                                               \
    SQLRETURN ret = SQLGetDiagRec(ht, h, i_0381, ss, &ne, es, sizeof(es), &n);  \
    if (ret) break;                                                             \
    D("[%s]%s: " fmt "", ss, es, ##__VA_ARGS__);                                \
    ++i_0381;                                                                   \
  }                                                                             \
} while (0)

static int open_connect(const char *dsn, const char *uid, const char *pwd, SQLHENV *pEnv, SQLHDBC *pConn) {
  SQLRETURN r;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  r = SQLAllocEnv(&env);
  if (r!=SQL_SUCCESS) return 1;
  do {
    r = SQLAllocConnect(env, &conn);
    CHK_RESULT(r, SQL_HANDLE_ENV, env, "");
    if (r!=SQL_SUCCESS) break;
    do {
      r = SQLConnect(conn, (SQLCHAR*)dsn, (SQLSMALLINT)(dsn ? strlen(dsn) : 0),
                           (SQLCHAR*)uid, (SQLSMALLINT)(uid ? strlen(uid) : 0),
                           (SQLCHAR*)pwd, (SQLSMALLINT)(pwd ? strlen(pwd) : 0));
      CHK_RESULT(r, SQL_HANDLE_DBC, conn, "");
      if (r==SQL_SUCCESS) {
        *pEnv  = env;
        *pConn = conn;
        return 0;
      }
    } while (0);
    SQLFreeConnect(conn);
  } while (0);
  SQLFreeEnv(env);

  return 1;
}

static int open_driver_connect(const char *connstr, SQLHENV *pEnv, SQLHDBC *pConn) {
  SQLRETURN r;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  r = SQLAllocEnv(&env);
  if (r!=SQL_SUCCESS) return 1;
  do {
    r = SQLAllocConnect(env, &conn);
    CHK_RESULT(r, SQL_HANDLE_ENV, env, "");
    if (r!=SQL_SUCCESS) break;
    do {
      SQLCHAR buf[4096];
      SQLSMALLINT blen = 0;
      SQLHDBC         ConnectionHandle      = conn;
      SQLHWND         WindowHandle          = NULL;
      SQLCHAR *       InConnectionString    = (SQLCHAR*)connstr;
      SQLSMALLINT     StringLength1         = (SQLSMALLINT)(connstr ? strlen(connstr) : 0);
      SQLCHAR *       OutConnectionString   = buf;
      SQLSMALLINT     BufferLength          = sizeof(buf);
      SQLSMALLINT *   StringLength2Ptr      = &blen;
      SQLUSMALLINT    DriverCompletion      = SQL_DRIVER_NOPROMPT;
      r = SQLDriverConnect(ConnectionHandle, WindowHandle, InConnectionString,
                           StringLength1, OutConnectionString, BufferLength,
                           StringLength2Ptr, DriverCompletion);
      CHK_RESULT(r, SQL_HANDLE_DBC, conn, "");
      if (r==SQL_SUCCESS) {
        *pEnv  = env;
        *pConn = conn;
        return 0;
      }
    } while (0);
    SQLFreeConnect(conn);
  } while (0);
  SQLFreeEnv(env);

  return 1;
}

static SQLRETURN traverse_cols(SQLHSTMT stmt, SQLSMALLINT cols) {
  SQLRETURN r = SQL_ERROR;
  for (SQLSMALLINT i=0; i<cols; ++i) {
    db_column_t column = {0};
    r = SQLDescribeCol(stmt, (SQLUSMALLINT)(i+1), (SQLCHAR*)column.name,
                       (SQLSMALLINT)sizeof(column.name), &column.nameLength,
                       &column.dataType, &column.columnSize,
                       &column.decimalDigits, &column.nullable);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    D("col%02d:[%s]%d,type:[%d],colSize:[%zu],decimalDigits:[%d],nullable:[%d]",
       i+1, column.name, column.nameLength, column.dataType, column.columnSize,
       column.decimalDigits, column.nullable);
    db_column_t *col = (db_column_t*)realloc(columns, (size_t)(i+1)*sizeof(*col));
    if (!col) {
      D("out of memory");
      return SQL_ERROR;
    }
    col[i] = column;
    columns = col;
  }
  return SQL_SUCCESS;
}

static int do_statement(SQLHSTMT stmt, const char *statement) {
  SQLRETURN r = 0;
  do {
    r = SQLExecDirect(stmt, (SQLCHAR*)statement, SQL_NTS);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: [%s]", statement);
    if (r) break;
    SQLSMALLINT cols = 0;
    r = SQLNumResultCols(stmt, &cols);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    if (cols <= 0) break;
    r = traverse_cols(stmt, cols);
    char buf[4096];
    while (1) {
      SQLRETURN r = SQLFetch(stmt);
      if (r==SQL_NO_DATA) break;
      CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
      for (size_t i=0; i<cols; ++i) {
        SQLLEN soi = 0;
        r = SQLGetData(stmt, (SQLUSMALLINT)(i+1), SQL_C_CHAR, buf, sizeof(buf), &soi);
        CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "i/cols:[%ld/%d]", i,cols);
        if (r) {
          if (r!=SQL_SUCCESS_WITH_INFO) {
            if (i>0) fprintf(stdout, "\n");
            return r;
          }
        }
        if (soi==SQL_NULL_DATA) {
          fprintf(stdout, "%snull", i==0?"":",");
        } else {
          fprintf(stdout, "%s\"%s\"", i==0?"":",", buf);
        }
      }
      fprintf(stdout, "\n");
    }
  } while (0);
  return r;
}

int test_statements(const char *dsn, const char *uid, const char *pwd, const char **statements) {
  SQLRETURN r = SQL_SUCCESS;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  int n = open_connect(dsn, uid, pwd, &env, &conn);
  if (n) return 1;
  do {
    SQLHSTMT stmt = {0};
    r = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (r!=SQL_SUCCESS) break;
    const char **p = statements;
    while (*p) {
      if (do_statement(stmt, *p)) {
        r = SQL_ERROR;
        break;
      }
      ++p;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  } while (0);
  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

int test_driver_connect(const char *connstr) {
  SQLRETURN r = SQL_SUCCESS;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  int n = open_driver_connect(connstr, &env, &conn);
  if (n) return 1;
  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

int create_statement(SQLHENV env, SQLHDBC conn, SQLHSTMT *pStmt) {
  SQLHSTMT stmt = {0};
  SQLRETURN r = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
  CHK_RESULT(r, SQL_HANDLE_DBC, conn, "");
  if (r==SQL_SUCCESS) {
    *pStmt = stmt;
    return 0;
  }
  if (r==SQL_SUCCESS_WITH_INFO) {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  }
  return 1;
}

int do_statements(SQLHSTMT stmt, const char **statements) {
  const char **p = statements;
  while (p && *p) {
    CHK_TEST(do_statement(stmt, *p));
    ++p;
  }
  return 0;
}

int tests_stmt(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt) {
  const char *statements[] = {
    "drop database if exists m",
    "create database m",
    "use m",
    // "create table t (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, blob binary(1), name nchar(1))",
    "create table t (ts timestamp, b bool)",
    "insert into t values('2020-10-10 00:00:00', 0)",
    "insert into t values('2020-10-10 00:00:00.001', 1)",
    NULL
  };
  CHK_TEST(do_statements(stmt, statements));
  return 0;
}

int tests(SQLHENV env, SQLHDBC conn) {
  SQLHSTMT stmt = {0};
  CHK_TEST(create_statement(env, conn, &stmt));
  int r = tests_stmt(env, conn, stmt);
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return r ? 1 : 0;
}

int test_env(void) {
  SQLRETURN r;
  SQLHENV env = {0};
  r = SQLAllocEnv(&env);
  if (r!=SQL_SUCCESS) return 1;
  SQLFreeEnv(env);
  return 0;
}

static int test_sqls_in_stmt(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, const char *sqls) {
  FILE *f = fopen(sqls, "rb");
  if (!f) {
    D("failed to open file [%s]", sqls);
    return -1;
  }

  int r = 0;
  while (!feof(f)) {
    char *line = NULL;
    size_t len = 0;

    ssize_t n = 0;
#ifdef _MSC_VER
    n = taosGetlineImp(&line, &len, f);
#else
    n = getline(&line, &len, f);
#endif
    if (n==-1) break;

    const char *p = NULL;
    do {
      if (line[0] == '#') break;
      if (n>0 && line[n-1] == '\n') line[n-1]='\0';
      if (n>0 && line[n-1] == '\r') line[n-1]='\0';
      if (n>1 && line[n-2] == '\r') line[n-2]='\0';
      p = line;
      while (isspace(*p)) ++p;

      if (*p==0) break;

      int positive = 1;
      if (strncmp(p, "N:", 2)==0) {
        // negative sample
        positive = 0;
        p += 2;
      } else if (strncmp(p, "P:", 2)==0) {
        // positive sample
        p += 2;
      }

      D("statement: [%s]", p);
      r = do_statement(stmt, p);

      if (positive && r==0) break;
      if (!positive && r) { r = 0; break; }
      if (positive) return r;
      D("expecting negative result, but got positive");
      return -1;
    } while (0);

    free(line);

    if (r) break;
  }

  fclose(f);
  return r ? 1 : 0;
}

static int test_sqls_in_conn(SQLHENV env, SQLHDBC conn, const char *sqls) {
  SQLHSTMT stmt = {0};
  CHK_TEST(create_statement(env, conn, &stmt));
  int r = test_sqls_in_stmt(env, conn, stmt, sqls);
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return r ? 1 : 0;
}

static int test_sqls(const char *dsn, const char *uid, const char *pwd, const char *connstr, const char *sqls) {
  int r = 0;
  SQLHENV env  = {0};
  SQLHDBC conn = {0};
  if (dsn) {
    CHK_TEST(open_connect(dsn, uid, pwd, &env, &conn));
  } else {
    CHK_TEST(open_driver_connect(connstr, &env, &conn));
  }

  if (sqls) {
    r = test_sqls_in_conn(env, conn, sqls);
  }
  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

typedef struct record_s          record_t;
struct record_s {
  int               dummy;
  char              ts[64];
  SQLLEN            ts_len;
  int32_t           v1;
  SQLLEN            v1_len;
  char              ts2[64];
  SQLLEN            ts2_len;
};

static int do_prepare_in_stmt(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt) {
  SQLRETURN r = SQL_SUCCESS;
  do {
    const char *sql = "insert into m.v (ts, v1, ts2) values (?, ?, ?)";
    r = SQLPrepare(stmt, (SQLCHAR*)sql, SQL_NTS);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");

    record_t records[] = {
      {0, "2020-01-03 11:22:33.345",  SQL_NTS,   1, sizeof(int32_t), "2020-01-02 11:22:33.455",   SQL_NTS},
      {0, "2020-01-03 11:22:34.346",  SQL_NTS,   2, sizeof(int32_t), "2020-01-02 11:22:34.445",   SQL_NTS},
      {0, "2020-01-04 11:22:34.345",  SQL_NTS,   2, sizeof(int32_t), "2020-01-02 11:22:34.445",   SQL_NTS},
      {0, "2020-01-05 11:22:34.345",  SQL_NTS,   2, sizeof(int32_t), "2020-01-02 11:22:34.445",   SQL_NTS},
    };

    record_t *base = (record_t*)0;

    r = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, sizeof(base->ts)-1, 0, base->ts, sizeof(base->ts), &(base->ts_len));
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    r = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &base->v1, 0, &(base->v1_len));
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    r = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, sizeof(base->ts2)-1, 0, base->ts2, sizeof(base->ts2), &(base->ts2_len));
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(*base), 0);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(sizeof(records)/sizeof(records[0])), 0);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    record_t *record = NULL;

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, &record, 0);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    record = records;

    r = SQLExecute(stmt);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
  } while (0);

  return r ? -1 : 0;
}

static int do_prepare_in_conn(SQLHENV env, SQLHDBC conn) {
  SQLHSTMT stmt = {0};
  CHK_TEST(create_statement(env, conn, &stmt));
  int r = do_prepare_in_stmt(env, conn, stmt);
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return r ? 1 : 0;
}

static int do_prepare(const char *dsn, const char *uid, const char *pwd, const char *connstr) {
  int r = 0;
  SQLHENV env  = {0};
  SQLHDBC conn = {0};
  if (dsn) {
    CHK_TEST(open_connect(dsn, uid, pwd, &env, &conn));
  } else {
    CHK_TEST(open_driver_connect(connstr, &env, &conn));
  }

  r = do_prepare_in_conn(env, conn);

  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

typedef struct {
  int                  dummy;
  int64_t              ts;
  SQLLEN               ts_len;
  int8_t               v1;
  SQLLEN               v1_len;
  int16_t              v2;
  SQLLEN               v2_len;
  int32_t              v4;
  SQLLEN               v4_len;
  int64_t              v8;
  SQLLEN               v8_len;
} test_v_t;

static int do_insert_in_stmt(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, int64_t *ts, insert_arg_t *arg) {
  SQLRETURN r = SQL_SUCCESS;
  int batch_size      = arg->batch_size;
  test_v_t *recs = NULL;
  do {
    const char *sql = "insert into test.v (ts, v1, v2, v4, v8) values (?, ?, ?, ?, ?)";
    r = SQLPrepare(stmt, (SQLCHAR*)sql, SQL_NTS);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");

    test_v_t *base = NULL;

    r = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &base->ts, 0, &base->ts_len);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    r = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, 0, 0, &base->v1, 0, &base->v1_len);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    r = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_SHORT, SQL_SMALLINT, 0, 0, &base->v2, 0, &base->v2_len);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    r = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &base->v4, 0, &base->v4_len);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;
    r = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &base->v8, 0, &base->v8_len);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(*base), 0);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    base = NULL;

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, &base, 0);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    size_t    n_recs = (size_t)batch_size;
    recs = (test_v_t*)calloc(n_recs, sizeof(*recs));
    OILE(recs, "");

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)n_recs, 0);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    if (r) break;

    base = recs;

    for (int batch=0; batch<arg->batchs; ++batch) {
      for (int i=0; i<n_recs; ++i) {
        test_v_t *rec = recs + i;
        rec->dummy = 0;
        rec->ts = *ts + i;
        rec->ts_len = sizeof(rec->ts);
        rec->v1 = (int8_t)rand();
        rec->v1_len = sizeof(rec->v1);
        rec->v2 = (int16_t)rand();
        rec->v2_len = sizeof(rec->v2);
        rec->v4 = rand();
        rec->v4_len = sizeof(rec->v4);
        rec->v8 = rand();
        rec->v8_len = sizeof(rec->v8);
      }

      *ts += (int64_t)n_recs;

      r = SQLExecute(stmt);
      CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
      if (r) break;
    }
  } while (0);

  free(recs);
  return r ? -1 : 0;
}

static int do_insert_in_conn(SQLHENV env, SQLHDBC conn, insert_arg_t *arg) {
  SQLHSTMT stmt = {0};
  int64_t ts = 1502535178128;
  int r = 0;
  CHK_TEST(create_statement(env, conn, &stmt));
  for (int i=0; i<1 && i<arg->batchs; ++i) {
    r = do_insert_in_stmt(env, conn, stmt, &ts, arg);
    if (r) break;
    if (!arg->keep_stmt_among_batchs) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      r = create_statement(env, conn, &stmt);
      if (r) break;
    }
  }
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return r ? 1 : 0;
}

static int do_insert_batch(const char *dsn, const char *uid, const char *pwd, const char *connstr, insert_arg_t *arg) {
  int r = 0;
  SQLHENV env  = {0};
  SQLHDBC conn = {0};
  if (dsn) {
    CHK_TEST(open_connect(dsn, uid, pwd, &env, &conn));
  } else {
    CHK_TEST(open_driver_connect(connstr, &env, &conn));
  }

  const char *sqls[] = {
    "drop database if exists test",
    "create database test",
    "create table test.v (ts timestamp, v1 tinyint, v2 smallint, v4 int, v8 bigint)",
    NULL
  };
  SQLHSTMT stmt = {0};
  CHK_TEST(create_statement(env, conn, &stmt));
  CHK_TEST(do_statements(stmt, sqls));
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  OD("................");
  r = do_insert_in_conn(env, conn, arg);
  OD("................");

  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

static int do_debug_col_name_max_len(const char *dsn, const char *uid, const char *pwd, const char *connstr) {
  SQLRETURN r;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  r = SQLAllocEnv(&env);
  if (r!=SQL_SUCCESS) {
    D("SQLAllocEnv failed");
    return 1;
  };
  do {
    r = SQLAllocConnect(env, &conn);
    CHK_RESULT(r, SQL_HANDLE_ENV, env, "");
    if (r!=SQL_SUCCESS) break;
    do {
      if (dsn) {
        r = SQLConnect(conn, (SQLCHAR*)dsn, (SQLSMALLINT)(dsn ? strlen(dsn) : 0),
                             (SQLCHAR*)uid, (SQLSMALLINT)(uid ? strlen(uid) : 0),
                             (SQLCHAR*)pwd, (SQLSMALLINT)(pwd ? strlen(pwd) : 0));
      } else {
        SQLCHAR buf[4096];
        SQLSMALLINT blen = 0;
        SQLHDBC         ConnectionHandle      = conn;
        SQLHWND         WindowHandle          = NULL;
        SQLCHAR *       InConnectionString    = (SQLCHAR*)connstr;
        SQLSMALLINT     StringLength1         = (SQLSMALLINT)(connstr ? strlen(connstr) : 0);
        SQLCHAR *       OutConnectionString   = buf;
        SQLSMALLINT     BufferLength          = sizeof(buf);
        SQLSMALLINT *   StringLength2Ptr      = &blen;
        SQLUSMALLINT    DriverCompletion      = SQL_DRIVER_NOPROMPT;
        r = SQLDriverConnect(ConnectionHandle, WindowHandle, InConnectionString,
                             StringLength1, OutConnectionString, BufferLength,
                             StringLength2Ptr, DriverCompletion);
      }
      CHK_RESULT(r, SQL_HANDLE_DBC, conn, "");
      if (r!=SQL_SUCCESS) break;
      D("connected");
      if (1) {
        SQLSMALLINT maxColumnNameLength = 0;
        SQLSMALLINT len = 0;
        r = SQLGetInfo(conn, SQL_MAX_COLUMN_NAME_LEN, &maxColumnNameLength, sizeof(SQLSMALLINT), &len);
        CHK_RESULT(r, SQL_HANDLE_DBC, conn, "");
        if (r!=SQL_SUCCESS) break;
        D("maxColumnNameLength: %d", maxColumnNameLength);
      }
    } while (0);
    SQLFreeConnect(conn);
    conn = NULL;
  } while (0);
  SQLFreeEnv(env);
  env = NULL;

  return (r==SQL_SUCCESS) ? 0 : 1;
}

void usage(const char *arg0) {
  fprintf(stdout, "%s usage:\n", arg0);
  fprintf(stdout, "%s [--dsn <dsn>] [--uid <uid>] [--pwd <pwd>] [-C <conn_str>] [--sts <sts>]\n", arg0);
  fprintf(stdout, "  --dsn <dsn>: DSN\n");
  fprintf(stdout, "  --uid <uid>: UID\n");
  fprintf(stdout, "  --pwd <pwd>: PWD\n");
  fprintf(stdout, "  -C <conn_str>: driver connection string\n");
  fprintf(stdout, "  --sts <sts>: file where statements store\n");
}

int main(int argc, char *argv[]) {
  srand((unsigned)time(0));
  const char *conn_str = NULL;
  const char *dsn = NULL;
  const char *uid = NULL;
  const char *pwd = NULL;
  const char *sts = NULL; // statements file
  int         debug_col_name_max_len = 0;
  int         prepare                = 0;
  int         insert                 = 0;
  insert_arg_t   insert_arg = {
    .batch_size      = 100,
    .batchs          = 100,
    .keep_stmt_among_batchs = 0
  };
  for (size_t i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h")==0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "-d")==0) {
      debug_col_name_max_len = 1;
      continue;
    }
    if (strcmp(arg, "--insert")==0) {
      insert = 1;
      continue;
    }
    if (strcmp(arg, "--batch_size")==0) {
      ++i;
      if (i>=argc) {
        D("<batch_size> expected but got nothing");
        return 1;
      }
      sscanf(argv[i], "%d", &insert_arg.batch_size);
      if (insert_arg.batch_size<=0) {
        D("<batch_size> invalid");
        return 1;
      }
      continue;
    }
    if (strcmp(arg, "--batchs")==0) {
      ++i;
      if (i>=argc) {
        D("<batchs> expected but got nothing");
        return 1;
      }
      sscanf(argv[i], "%d", &insert_arg.batchs);
      if (insert_arg.batchs<=0) {
        D("<batchs> invalid");
        return 1;
      }
      continue;
    }
    if (strcmp(arg, "--keep_stmt_among_batchs")==0) {
      insert_arg.keep_stmt_among_batchs = 1;
      continue;
    }
    if (strcmp(arg, "--dsn")==0) {
      ++i;
      if (i>=argc) {
        D("<dsn> expected but got nothing");
        return 1;
      }
      if (conn_str) {
        D("-C has already been specified");
        return 1;
      }
      dsn = argv[i];
      continue;
    }
    if (strcmp(arg, "--uid")==0) {
      ++i;
      if (i>=argc) {
        D("<uid> expected but got nothing");
        return 1;
      }
      uid = argv[i];
      continue;
    }
    if (strcmp(arg, "--pwd")==0) {
      ++i;
      if (i>=argc) {
        D("<pwd> expected but got nothing");
        return 1;
      }
      pwd = argv[i];
      continue;
    }
    if (strcmp(arg, "-C")==0) {
      ++i;
      if (i>=argc) {
        D("<connection string> expected but got nothing");
        return 1;
      }
      if (dsn || uid || pwd) {
        D("either of --dsn/--uid/--pwd has already been specified");
        return 1;
      }
      conn_str = argv[i];
      continue;
    }
    if (strcmp(arg, "--sts")==0) {
      ++i;
      if (i>=argc) {
        D("<sts> expected but got nothing");
        return 1;
      }
      sts = argv[i];
      continue;
    }
    if (strcmp(arg, "-p")==0) {
      prepare = 1;
      continue;
    }
  }
  if (debug_col_name_max_len) {
    int r = do_debug_col_name_max_len(dsn, uid, pwd, conn_str);
    if (r) return 1;
  }
  if (insert) {
    int r = do_insert_batch(dsn, uid, pwd, conn_str, &insert_arg);
    if (r) return 1;
  }
  if (sts) {
    int r = test_sqls(dsn, uid, pwd, conn_str, sts);
    if (r) return 1;
  }
  if (prepare) {
    int r = do_prepare(dsn, uid, pwd, conn_str);
    if (r) return 1;
  }
  D("Done!");
  return 0;
}

