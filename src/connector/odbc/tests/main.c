#include <libgen.h>
#include <sql.h>
#include <sqlext.h>

#include <stdio.h>
#include <string.h>

#include "os.h"
#include "../src/todbc_log.h"

// static const char *dsn = "TAOS_DSN";
// static const char *uid = "root";
// static const char *pwd = "taosdata";

#define CHK_TEST(statement)                       \
do {                                              \
  D("testing: %s", #statement);                   \
  int r = (statement);                            \
  if (r) return 1;                                \
} while (0);


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

static const char *pre_stmts[] = {
  "create database db",
  "use db",
  "create table t (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), blob nchar(10))"
};

static const char *pro_stmts[] = {
  // "insert into t values ('2019-07-15 00:00:00', 1)",
  // "insert into t values ('2019-07-15 01:00:00', 2)",
  "select * from t"
  // "drop database db"
};

#define CHK_RESULT(r, ht, h, fmt, ...)                                          \
do {                                                                            \
  if (r==0) break;                                                              \
  SQLCHAR      ss[10];                                                          \
  SQLINTEGER   ne = 0;                                                          \
  SQLCHAR      es[4096];                                                        \
  SQLSMALLINT  n  = 0;                                                          \
  ss[0] = '\0';                                                                 \
  es[0] = '\0';                                                                 \
  SQLRETURN ret = SQLGetDiagRec(ht, h, 1, ss, &ne, es, sizeof(es), &n);         \
  if (ret) break;                                                               \
  D("[%s]%s: " fmt "", ss, es, ##__VA_ARGS__);                                  \
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
      r = SQLConnect(conn, (SQLCHAR*)dsn, strlen(dsn),
                           (SQLCHAR*)uid, strlen(uid),
                           (SQLCHAR*)pwd, strlen(pwd));
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
      SQLSMALLINT     StringLength1         = strlen(connstr);
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
    char buf[4096];
    while (1) {
      SQLRETURN r = SQLFetch(stmt);
      if (r==SQL_NO_DATA) break;
      CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
      for (size_t i=0; i<cols; ++i) {
        SQLLEN soi = 0;
        r = SQLGetData(stmt, i+1, SQL_C_CHAR, buf, sizeof(buf), &soi);
        CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
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

    // r = SQLFetch(stmt);
    // if (r==SQL_NO_DATA) {
    //   D("..........");
    //   r = SQL_SUCCESS;
    //   break;
    // }
    // CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    // if (r) break;
    // r = SQLPrepare(stmt, (SQLCHAR*)statement, strlen(statement));
    // CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "");
    // if (r) break;
    // r = SQLExecute(stmt);
    // CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    // if (r) break;
  } while (0);
  return r;
}

static int do_insert(SQLHSTMT stmt, data_t data) {
  SQLRETURN r = 0;
  SQLLEN 		lbin;
  SQLLEN    lblob;

  const char *statement = "insert into t values (?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
  int ignored = 0;

  do {
    r = SQLPrepare(stmt, (SQLCHAR*)statement, strlen(statement));
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_TIMESTAMP, ignored, ignored, &data.ts, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, ignored, ignored, &data.b, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, ignored, ignored, &data.v1, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SHORT, SQL_SMALLINT, ignored, ignored, &data.v2, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, ignored, ignored, &data.v4, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, ignored, ignored, &data.v8, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 7, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, ignored, ignored, &data.f4, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLBindParameter(stmt, 8, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, ignored, ignored, &data.f8, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    lbin = SQL_NTS;
    r = SQLBindParameter(stmt, 9, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, sizeof(data.bin)-1, ignored, &data.bin, ignored, &lbin);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    lblob = SQL_NTS;
    r = SQLBindParameter(stmt, 10, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(data.blob)-1, ignored, &data.blob, ignored, &lblob);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    r = SQLExecute(stmt);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt, "statement: %s", statement);
    if (r) break;

    // ts += 1;
    // v  = 2;
    // r = SQLExecute(stmt);
    // if (r) break;
  } while (0);
  return r;
}

static int test1(const char *dsn, const char *uid, const char *pwd) {
  SQLRETURN r = SQL_SUCCESS;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  int n = open_connect(dsn, uid, pwd, &env, &conn);
  if (n) return 1;

  do {
    SQLHSTMT stmt = {0};
    r = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (r!=SQL_SUCCESS) break;
    do {
      if (do_statement(stmt, "drop database if exists db")) {
        r = SQL_ERROR;
        break;
      }
      for (size_t i=0; i<sizeof(pre_stmts)/sizeof(pre_stmts[0]); ++i) {
        r = do_statement(stmt, pre_stmts[i]);
        if (r!=SQL_SUCCESS) break;
      }
      do {
        data_t       data = {0};
        data.ts      = 1591060628001;
        data.b       = 1;
        data.v1      = 127;
        data.v2      = 32767;
        data.v4      = 2147483647;
        data.v8      = 9223372036854775807;
        data.f4      = 123.456;
        data.f8      = 9999999.999999;
        memset(data.bin, 0, sizeof(data.bin));
        memset(data.blob, 0, sizeof(data.blob));
        snprintf(data.bin, sizeof(data.bin), "hel我lo");
        snprintf(data.blob, sizeof(data.blob), "world");
        snprintf(data.blob, sizeof(data.blob), "wo人rld");
        SQLHSTMT stmt = {0};
        r = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
        if (r!=SQL_SUCCESS) break;
        do {
          r = do_insert(stmt, data);
          if (r!=SQL_SUCCESS) break;
        } while (0);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // r = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
        // if (r!=SQL_SUCCESS) break;
        // do {
        //   r = do_insert(stmt, ts++, v++);
        //   if (r!=SQL_SUCCESS) break;
        // } while (0);
        // SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      } while (0);
      if (r!=SQL_SUCCESS) break;
      for (size_t i=0; i<sizeof(pro_stmts)/sizeof(pro_stmts[0]); ++i) {
        r = do_statement(stmt, pro_stmts[i]);
        if (r!=SQL_SUCCESS) break;
      }
    } while (0);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  } while (0);
  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);

  return r ? 1 : 0;
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

int test_sqls_in_stmt(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, const char *sqls) {
  FILE *f = fopen(sqls, "rb");
  if (!f) {
    D("failed to open file [%s]", sqls);
    return -1;
  }

  int r = 0;
  while (!feof(f)) {
    char *line = NULL;
    size_t len = 0;

    ssize_t n = getline(&line, &len, f);
    if (n==-1) break;

    const char *p = NULL;
    do {
      if (line[0] == '#') break;
      if (line[n-1] == '\n') line[n-1]='\0';
      p = line;
      while (isspace(*p)) ++p;

      if (*p==0) break;

      int positive = 1;
      if (strncmp(p, "N:", 2)==0) {
        positive = 0;
        p += 2;
      } else if (strncmp(p, "P:", 2)==0) {
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

int test_sqls_in_conn(SQLHENV env, SQLHDBC conn, const char *sqls) {
  SQLHSTMT stmt = {0};
  CHK_TEST(create_statement(env, conn, &stmt));
  int r = test_sqls_in_stmt(env, conn, stmt, sqls);
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return r ? 1 : 0;
}

int test_sqls(const char *dsn, const char *uid, const char *pwd, const char *connstr, const char *sqls) {
  int r = 0;
  SQLHENV env  = {0};
  SQLHDBC conn = {0};
  if (dsn) {
    CHK_TEST(open_connect(dsn, uid, pwd, &env, &conn));
  } else {
    CHK_TEST(open_driver_connect(connstr, &env, &conn));
  }
  r = test_sqls_in_conn(env, conn, sqls);
  SQLDisconnect(conn);
  SQLFreeConnect(conn);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

int main(int argc, char *argv[]) {
  if (argc==1) {
    CHK_TEST(test_env());
    return 0;
  }

  const char *dsn = (argc>1) ? argv[1] : NULL;
  const char *uid = (argc>2) ? argv[2] : NULL;
  const char *pwd = (argc>3) ? argv[3] : NULL;
  const char *connstr = (argc>4) ? argv[4] : NULL;
  const char *sqls = (argc>5) ? argv[5] : NULL;

  if (0) {
    CHK_TEST(test_env());

    CHK_TEST(test1(dsn, uid, pwd));

    const char *statements[] = {
      "drop database if exists m",
      "create database m",
      "use m",
      "drop database m",
      NULL
    };
    CHK_TEST(test_statements(dsn, uid, pwd, statements));

    if (connstr)
      CHK_TEST(test_driver_connect(connstr));

    if (connstr) {
      SQLHENV env  = {0};
      SQLHDBC conn = {0};
      CHK_TEST(open_driver_connect(connstr, &env, &conn));
      int r = tests(env, conn);
      SQLDisconnect(conn);
      SQLFreeConnect(conn);
      SQLFreeEnv(env);
      if (r) return 1;
    }
  }

  if ((dsn || connstr) && 1) {
    CHK_TEST(test_sqls(dsn, uid, pwd, connstr, sqls));
  }

  return 0;
}

