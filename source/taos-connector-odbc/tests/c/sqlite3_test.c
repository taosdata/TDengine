/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "odbc_helpers.h"

#include <stdlib.h>
#include <time.h>

static int create_connection(SQLHANDLE *penv, SQLHANDLE *pdbc, const char *conn_str, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr;
  SQLHANDLE henv, hdbc;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (henv == SQL_NULL_HANDLE) goto fail_henv;

  sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (FAILED(sr)) goto fail_odbc_version;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  if (hdbc == SQL_NULL_HANDLE) goto fail_hdbc;

  if (!conn_str) {
    sr = CALL_SQLConnect(hdbc, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
    if (FAILED(sr)) goto fail_connect;
  } else {
    SQLSMALLINT StringLength2 = 0;
    char buf[1024];
    buf[0] = '\0';
    sr = CALL_SQLDriverConnect(hdbc, 0, (SQLCHAR*)conn_str, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength2, SQL_DRIVER_NOPROMPT);
    D("driver completed connection string: [%s]", buf);
    if (FAILED(sr)) goto fail_connect;
  }

  *penv = henv;
  *pdbc = hdbc;

  return 0;

fail_connect:
  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);

fail_hdbc:
fail_odbc_version:
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

fail_henv:
  return -1;
}

static int create_statement(SQLHANDLE *pstmt, SQLHANDLE hdbc)
{
  SQLRETURN sr;
  SQLHANDLE hstmt;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  if (FAILED(sr)) goto fail_hstmt;

  *pstmt = hstmt;
  return 0;

fail_hstmt:
  return -1;
}

static int test_connect(const char *conn_str, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr;
  SQLHANDLE henv, hdbc;

  int r = create_connection(&henv, &hdbc, conn_str, dsn, uid, pwd);
  if (r) return -1;

  sr = CALL_SQLDisconnect(hdbc);
  if (FAILED(sr)) r = -1;

  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

static int test_direct_exec(SQLHANDLE hstmt, const char *sql)
{
  SQLRETURN sr;

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  if (rand() % 2) {
    sr = CALL_SQLFreeStmt(hstmt, SQL_CLOSE);
    if (FAILED(sr)) return -1;
  }

  return 0;
}

static int test_prepare_test_data(SQLHANDLE hstmt)
{
  CHECK(test_direct_exec(hstmt, "drop database if exists foo"));
  CHECK(test_direct_exec(hstmt, "create database if not exists foo"));
  CHECK(test_direct_exec(hstmt, "use foo"));
  CHECK(test_direct_exec(hstmt, "drop table if exists t"));
  CHECK(test_direct_exec(hstmt, "create table if not exists t (ts timestamp, name varchar(10))"));
  CHECK(test_direct_exec(hstmt, "drop table if exists t"));
  CHECK(test_direct_exec(hstmt, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"));

  return 0;
}

static int test_direct_executes(SQLHANDLE hstmt)
{
  CHECK(test_direct_exec(hstmt, "show databases"));
  CHECK(test_direct_exec(hstmt, "use foo"));
  CHECK(test_direct_exec(hstmt, "insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')"));
  CHECK(test_direct_exec(hstmt, "insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')"));
  CHECK(test_direct_exec(hstmt, "insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)"));
  CHECK(test_direct_exec(hstmt, "select * from t"));

  return 0;
}

static int test_queries(SQLHANDLE hdbc)
{
  SQLHANDLE hstmt;

  int r = create_statement(&hstmt, hdbc);
  if (r) return -1;

  r = test_prepare_test_data(hstmt);
  if (r) goto end;

  do {
    r = test_direct_executes(hstmt);
    if (r) break;
  } while (0);

end:
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r;
}

static int test_large_dataset(SQLHANDLE hdbc)
{
  SQLRETURN sr = SQL_SUCCESS;
  SQLHANDLE hstmt;

  int r = create_statement(&hstmt, hdbc);
  if (r) return -1;

  do {
    r = test_direct_exec(hstmt, "drop table if exists t");
    if (r) break;

    r = test_direct_exec(hstmt, "create table t (ts timestamp, bi bigint)");
    if (r) break;

#define ARRAY_SIZE   (64)

    size_t         batches = 32;

    SQLBIGINT      TsArray[ARRAY_SIZE];
    SQLLEN         TsIndArray[ARRAY_SIZE];

    SQLBIGINT      BiArray[ARRAY_SIZE];
    SQLLEN         BiIndArray[ARRAY_SIZE];

    SQLUSMALLINT   ParamStatusArray[ARRAY_SIZE];
    SQLULEN        ParamsProcessed;

    memset(TsIndArray, 0, sizeof(TsIndArray));
    memset(BiIndArray, 0, sizeof(BiIndArray));

    // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
    // column-wise binding.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
    if (FAILED(sr)) break;

    // Specify the number of elements in each parameter array.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)ARRAY_SIZE, 0);
    if (FAILED(sr)) break;

    // Specify an array in which to return the status of each set of
    // parameters.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0);
    if (FAILED(sr)) break;

    // Specify an SQLUINTEGER value in which to return the number of sets of
    // parameters processed.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);
    if (FAILED(sr)) break;

    // Bind the parameters in column-wise fashion.
    sr = CALL_SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_TYPE_TIMESTAMP, 20+3, 3, TsArray, 0, TsIndArray);
    if (FAILED(sr)) break;

    sr = CALL_SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, BiArray, 0, BiIndArray);
    if (FAILED(sr)) break;

    const char *sql = "insert into t (ts,bi) values (?,?)";
    sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    for (size_t j = 0; j < ARRAY_SIZE; j++) {
      TsIndArray[j] = 0;
      BiIndArray[j] = 0;
    }

    SQLBIGINT ts = 1662861448752;
    SQLBIGINT bi = 1663861448752;
    for (size_t i = 0; i<batches; ++i) {
      // Set ts, name
      for (size_t j = 0; j < ARRAY_SIZE; j++) {
        TsArray[j] = ts++;
        BiArray[j] = bi++;
        TsIndArray[j] = 0;
        BiIndArray[j] = 0;
      }

      sr = CALL_SQLExecute(hstmt);
      if (FAILED(sr)) break;
    }

    if (0) {
      CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

      r = create_statement(&hstmt, hdbc);
      if (r) return -1;

      sql = "select * from t";
      sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (FAILED(sr)) break;

      size_t nr = 0;
      while (1) {
        sr = CALL_SQLFetch(hstmt);
        if (sr == SQL_ERROR) break;
        if (sr == SQL_NO_DATA) {
          sr = SQL_SUCCESS;
          break;
        }
        ++nr;
      }

      if (nr != batches * ARRAY_SIZE) {
        D("internal logic error");
        r = -1;
      }
    } else {
      sr = CALL_SQLFreeStmt(hstmt, SQL_UNBIND);
      if (FAILED(sr)) break;

      // Specify the number of elements in each parameter array.
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, 0);
      if (FAILED(sr)) break;

      sr = CALL_SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, BiArray, 0, BiIndArray);
      if (FAILED(sr)) break;

      sql = "select * from t where bi > ?";
      sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (FAILED(sr)) break;

      BiArray[0] = 0;
      BiIndArray[0] = 0;

      sr = CALL_SQLExecute(hstmt);
      if (FAILED(sr)) break;

      size_t nr = 0;
      while (1) {
        sr = CALL_SQLFetch(hstmt);
        if (sr == SQL_ERROR) break;
        if (sr == SQL_NO_DATA) {
          sr = SQL_SUCCESS;
          break;
        }
        ++nr;
      }

      if (nr != batches * ARRAY_SIZE) {
        D("internal logic error");
        r = -1;
      }
    }

#undef ARRAY_SIZE
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  A(r == 0, "");
  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_bind_array_of_params(SQLHANDLE hdbc)
{
  SQLRETURN sr = SQL_SUCCESS;
  SQLHANDLE hstmt;

  int r = create_statement(&hstmt, hdbc);
  if (r) return -1;

  do {
    r = test_direct_exec(hstmt, "create database if not exists foo");
    if (r) break;
    r = test_direct_exec(hstmt, "use foo");
    if (r) break;
    r = test_direct_exec(hstmt, "drop table if exists t");
    if (r) break;
    r = test_direct_exec(hstmt, "create table t (ts timestamp, name varchar(10))");
    if (r) break;

    // https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/binding-arrays-of-parameters?view=sql-server-ver16

#define DESC_LEN     (10+1)
#define ARRAY_SIZE   (2)

    SQLBIGINT      TsArray[ARRAY_SIZE];
    SQLLEN         TsIndArray[ARRAY_SIZE];

    SQLCHAR        NameArray[ARRAY_SIZE][DESC_LEN];
    SQLLEN         NameLenOrIndArray[ARRAY_SIZE];

    SQLUSMALLINT   ParamStatusArray[ARRAY_SIZE];
    SQLULEN        ParamsProcessed;

    memset(TsIndArray, 0, sizeof(TsIndArray));
    memset(NameLenOrIndArray, 0, sizeof(NameLenOrIndArray));

    // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
    // column-wise binding.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
    if (FAILED(sr)) break;

    // Specify the number of elements in each parameter array.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)ARRAY_SIZE, 0);
    if (FAILED(sr)) break;

    // Specify an array in which to return the status of each set of
    // parameters.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0);
    if (FAILED(sr)) break;

    // Specify an SQLUINTEGER value in which to return the number of sets of
    // parameters processed.
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);
    if (FAILED(sr)) break;

    // Bind the parameters in column-wise fashion.
    sr = CALL_SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_TYPE_TIMESTAMP, 20+3, 3, TsArray, 0, TsIndArray);
    if (FAILED(sr)) break;

    const char *sql = "insert into t (ts,name) values (?,?)";
    if (1) {
      // NOTE: intentionally prepare before all parameters have been bound
      sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (FAILED(sr)) break;

      // NOTE: parameter bind after statement has been prepared
      sr = CALL_SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, DESC_LEN - 1, 0, NameArray, DESC_LEN, NameLenOrIndArray);
      if (FAILED(sr)) break;

      // Set ts, name
      for (size_t i = 0; i < ARRAY_SIZE; i++) {
        TsArray[i] = 1662861448752 + i;
        snprintf((char*)(NameArray[i]), sizeof(NameArray[i]), "name%ld", i);
        NameLenOrIndArray[i] = SQL_NTS;
        TsIndArray[i] = 0;
      }

      sr = CALL_SQLExecute(hstmt);
      if (FAILED(sr)) break;
    } else {
      sr = CALL_SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, DESC_LEN - 1, 0, NameArray, DESC_LEN, NameLenOrIndArray);
      if (FAILED(sr)) break;

      // Set ts, name
      for (size_t i = 0; i < ARRAY_SIZE; i++) {
        TsArray[i] = 1662861448752 + i;
        snprintf((char*)(NameArray[i]), sizeof(NameArray[i]), "name%ld", i);
        NameLenOrIndArray[i] = SQL_NTS;
        TsIndArray[i] = 0;
      }

      // Execute the statement.
      // NOTE: still looking for a way to make taosc compatible with both parameterised and non-parameterised statement
      sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (FAILED(sr)) break;
    }

    r = 0;

    // Check to see which sets of parameters were processed successfully.
    for (size_t i = 0; i < ParamsProcessed; i++) {
      printf("Parameter Set  Status\n");
      printf("-------------  -------------\n");
      switch (ParamStatusArray[i]) {
        case SQL_PARAM_SUCCESS:
        case SQL_PARAM_SUCCESS_WITH_INFO:
          printf("%13ld  Success\n", i);
          break;

        case SQL_PARAM_ERROR:
          printf("%13ld  Error\n", i);
          r = -1;
          break;

        case SQL_PARAM_UNUSED:
          printf("%13ld  Not processed\n", i);
          r = -1;
          break;

        case SQL_PARAM_DIAG_UNAVAILABLE:
          printf("%13ld  Unknown\n", i);
          r = -1;
          break;
      }
    }

    // NOTE: another batch of parameter values
    if (1) {
      // NOTE: no need to prepare nor parameter bind

      // NOTE: just Set ts, name
      for (size_t i = 0; i < ARRAY_SIZE; i++) {
        TsArray[i] = 1662861458752 + i;
        snprintf((char*)(NameArray[i]), sizeof(NameArray[i]), "memo%ld", i);
        NameLenOrIndArray[i] = SQL_NTS;
        TsIndArray[i] = 0;
      }

      sr = CALL_SQLExecute(hstmt);
      if (FAILED(sr)) break;
    } else {
      sr = CALL_SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, DESC_LEN - 1, 0, NameArray, DESC_LEN, NameLenOrIndArray);
      if (FAILED(sr)) break;

      // Set ts, name
      for (size_t i = 0; i < ARRAY_SIZE; i++) {
        TsArray[i] = 1662861448752 + i;
        snprintf((char*)(NameArray[i]), sizeof(NameArray[i]), "name%ld", i);
        NameLenOrIndArray[i] = SQL_NTS;
        TsIndArray[i] = 0;
      }

      // Execute the statement.
      // NOTE: still looking for a way to make taosc compatible with both parameterised and non-parameterised statement
      sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (FAILED(sr)) break;
    }

    // Check to see which sets of parameters were processed successfully.
    for (size_t i = 0; i < ParamsProcessed; i++) {
      printf("Parameter Set  Status\n");
      printf("-------------  -------------\n");
      switch (ParamStatusArray[i]) {
        case SQL_PARAM_SUCCESS:
        case SQL_PARAM_SUCCESS_WITH_INFO:
          printf("%13ld  Success\n", i);
          break;

        case SQL_PARAM_ERROR:
          printf("%13ld  Error\n", i);
          r = -1;
          break;

        case SQL_PARAM_UNUSED:
          printf("%13ld  Not processed\n", i);
          r = -1;
          break;

        case SQL_PARAM_DIAG_UNAVAILABLE:
          printf("%13ld  Unknown\n", i);
          r = -1;
          break;
      }
    }

#undef DESC_LEN
#undef ARRAY_SIZE
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test(void)
{
  CHECK(!!test_connect("Driver={SQLite3};Database=/tmp/bar.sqlite3", NULL, NULL, NULL));

  SQLRETURN sr;
  SQLHANDLE henv, hdbc;
  int r = 0;

  const char *conn_str = "Driver={SQLite3};Database=/tmp/bar.sqlite3";
  const char *dsn = NULL;
  const char *uid = NULL;
  const char *pwd = NULL;
  r = create_connection(&henv, &hdbc, conn_str, dsn, uid, pwd);
  if (r) return 1;

  do {
    if (0) {
      r = test_queries(hdbc);
      if (r) break;

      r = test_bind_array_of_params(hdbc);
      if (r) break;
    }

    r = test_large_dataset(hdbc);
  } while (0);

  sr = CALL_SQLDisconnect(hdbc);
  if (FAILED(sr)) r = 1;

  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

int main(int argc, char *argv[])
{
  (void)argc;
  (void)argv;
  srand(time(0));

  int r = test();

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}
