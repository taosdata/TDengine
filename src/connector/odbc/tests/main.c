#include <sql.h>
#include <sqlext.h>

#include <stdio.h>
#include <string.h>

#include "os.h"

// static const char *dsn = "TAOS_DSN";
// static const char *uid = "root";
// static const char *pwd = "taosdata";

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

#define CHK_RESULT(r, ht, h)                                                    \
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
  fprintf(stderr, "%s%s\n", ss, es);                                            \
} while (0)

static int do_statement(SQLHSTMT stmt, const char *statement) {
  SQLRETURN r = 0;
  do {
    fprintf(stderr, "prepare [%s]\n", statement);
    r = SQLPrepare(stmt, (SQLCHAR*)statement, strlen(statement));
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;
    fprintf(stderr, "execute [%s]\n", statement);
    r = SQLExecute(stmt);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;
    fprintf(stderr, "done\n");
  } while (0);
  fprintf(stderr, "r: [%x][%d]\n", r, r);
  return r;
}

static int do_insert(SQLHSTMT stmt, data_t data) {
  SQLRETURN r = 0;
  SQLLEN 		lbin;
  SQLLEN    lblob;

  const char *statement = "insert into t values (?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
  int ignored = 0;

  do {
    fprintf(stderr, "prepare [%s]\n", statement);
    r = SQLPrepare(stmt, (SQLCHAR*)statement, strlen(statement));
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 1 [%s]\n", statement);
    r = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_TIMESTAMP, ignored, ignored, &data.ts, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 2 [%s]\n", statement);
    r = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, ignored, ignored, &data.b, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 3 [%s]\n", statement);
    r = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, ignored, ignored, &data.v1, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 4 [%s]\n", statement);
    r = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SHORT, SQL_SMALLINT, ignored, ignored, &data.v2, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 5 [%s]\n", statement);
    r = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, ignored, ignored, &data.v4, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 6 [%s]\n", statement);
    r = SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, ignored, ignored, &data.v8, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 7 [%s]\n", statement);
    r = SQLBindParameter(stmt, 7, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, ignored, ignored, &data.f4, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 8 [%s]\n", statement);
    r = SQLBindParameter(stmt, 8, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, ignored, ignored, &data.f8, ignored, NULL);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 9 [%s]\n", statement);
    lbin = SQL_NTS;
    r = SQLBindParameter(stmt, 9, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, sizeof(data.bin)-1, ignored, &data.bin, ignored, &lbin);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "bind 10 [%s]\n", statement);
    lblob = SQL_NTS;
    r = SQLBindParameter(stmt, 10, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(data.blob)-1, ignored, &data.blob, ignored, &lblob);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    fprintf(stderr, "execute [%s]\n", statement);
    r = SQLExecute(stmt);
    CHK_RESULT(r, SQL_HANDLE_STMT, stmt);
    if (r) break;

    // ts += 1;
    // v  = 2;
    // fprintf(stderr, "execute [%s]\n", statement);
    // r = SQLExecute(stmt);
    // if (r) break;

    fprintf(stderr, "done\n");
  } while (0);
  fprintf(stderr, "r: [%x][%d]\n", r, r);
  return r;
}

int main(int argc, char *argv[]) {
  if (argc < 4) return 1;
  const char *dsn = argv[1];
  const char *uid = argv[2];
  const char *pwd = argv[3];
  SQLRETURN r;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  r = SQLAllocEnv(&env);
  if (r!=SQL_SUCCESS) return 1;
  do {
    r = SQLAllocConnect(env, &conn);
    CHK_RESULT(r, SQL_HANDLE_ENV, env);
    if (r!=SQL_SUCCESS) break;
    do {
      r = SQLConnect(conn, (SQLCHAR*)dsn, strlen(dsn),
                           (SQLCHAR*)uid, strlen(uid),
                           (SQLCHAR*)pwd, strlen(pwd));
      CHK_RESULT(r, SQL_HANDLE_DBC, conn);
      if (r!=SQL_SUCCESS) break;
      do {
        SQLHSTMT stmt = {0};
        r = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
        if (r!=SQL_SUCCESS) break;
        do {
          do_statement(stmt, "drop database db");
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
    } while (0);
    SQLFreeConnect(conn);
  } while (0);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

