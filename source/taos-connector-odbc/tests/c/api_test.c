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
#include "os_port.h"

#include "enums.h"
#include "test_config.h"

#include <assert.h>
#ifndef _WIN32
#include <dlfcn.h>
#endif
#include <errno.h>
#include <sql.h>
#include <sqlext.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

#define TEST_CASE_BEG(_func)                                \
  D("test case: %s...", _func)

#define TEST_CASE_END(_func, _exp)                          \
  if (MATCH(r, _exp)) {                                     \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_green(),                          \
                    "succeeded",                            \
                    color_reset());                         \
  } else if (r) {                                           \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failed",                               \
                    color_reset());                         \
    return -1;                                              \
  } else {                                                  \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failure expected but succeeded",       \
                    color_reset());                         \
    return -1;                                              \
  }

#define CHK0(_func, _exp)                                                                          \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_exp ")");                                                           \
    r = _func();                                                                                   \
    TEST_CASE_END(#_func "(" #_exp ")", _exp);                                                     \
  }

#define CHK1(_func, _arg1, _exp)                                                                   \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_arg1 "," #_exp ")");                                                \
    r = _func(_arg1);                                                                              \
    TEST_CASE_END(#_func "(" #_arg1 "," #_exp ")", _exp);                                          \
  }

#define CHK2(_func, _v1, _v2, _exp)                                                                \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_exp ")");                                         \
    r = _func(_v1, _v2);                                                                           \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_exp ")", _exp);                                   \
  }

#define CHK3(_func, _v1, _v2, _v3, _exp)                                                           \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")");                                \
    r = _func(_v1, _v2, _v3);                                                                      \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")", _exp);                          \
  }

#define CHK4(_func, _v1, _v2, _v3, _v4, _exp)                                                      \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")");                       \
    r = _func(_v1, _v2, _v3, _v4);                                                                 \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")", _exp);                 \
  }

#define MATCH(a, b)  (!!(a) == !!(b))

__attribute__((unused))
static int test_ok(void)
{
  return 0;
}

__attribute__((unused))
static int test_failure(void)
{
  return -1;
}

__attribute__((unused))
static int test_so(const char *so)
{
  void *h1 = dlopen(so, RTLD_NOW);
  if (h1 == NULL) {
    D("%s", dlerror());
    return -1;
  }

  int (*get_nr_load)(void);

  int r = -1;

  get_nr_load = dlsym(h1, "get_nr_load");
  if (!get_nr_load) {
    D("`get_nr_load` not exported in [%s]", so);
    goto fail_dlsym;
  }

  int nr_load = get_nr_load();
  if (nr_load != 1) {
    D("internal logic error in [%s]", so);
    goto fail_get_nr_load;
  }

  r = 0;

fail_get_nr_load:
fail_dlsym:
  dlclose(h1);

  return r ? -1 : 0;
}

__attribute__((unused))
static int _sql_stmt_get_long_data_by_col(SQLHANDLE stmth, SQLSMALLINT ColumnNumber)
{
  char buf[1024];
  buf[0] = '\0';

  char *p = buf;

  SQLUSMALLINT   Col_or_Param_Num    = ColumnNumber;
  SQLSMALLINT    TargetType          = SQL_C_CHAR;
  SQLPOINTER     TargetValuePtr      = (SQLPOINTER)p;
  // TODO: remove this restriction
  SQLLEN         BufferLength        = 4;
  SQLLEN         StrLen_or_Ind;

  while (1) {
    SQLRETURN r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (r == SQL_NO_DATA) break;
    if (SUCCEEDED(r)) {
      if (StrLen_or_Ind == SQL_NULL_DATA) {
        D("Column[#%d]: [[null]]", ColumnNumber);
        return 0;
      }
      int n = (int)strlen(p);
      if (n == 0) {
        BufferLength += 2;
        A((size_t)BufferLength <= sizeof(buf), "");
        continue;
      }
      p += strlen(p);
      TargetValuePtr = p;
      continue;
    }
    A(0, "");
    return -1;
  }

  D("Column[#%d]: [%s]", ColumnNumber, buf);

  return 0;
}

__attribute__((unused))
static int _sql_stmt_get_long_data(SQLHANDLE stmth, SQLSMALLINT ColumnCount)
{
  int i = 1;
  for (i=1; i<=ColumnCount; ++i) {
    if (_sql_stmt_get_long_data_by_col(stmth, i)) break;
  }

  if (i <= ColumnCount) return -1;

  return 0;
}

__attribute__((unused))
static int _sql_stmt_get_data(SQLHANDLE stmth, SQLSMALLINT ColumnCount)
{
  char buf[1024];
  int i = 1;
  for (i=1; i<=ColumnCount; ++i) {
    SQLUSMALLINT   Col_or_Param_Num    = i;
    SQLSMALLINT    TargetType          = SQL_C_CHAR;
    SQLPOINTER     TargetValuePtr      = (SQLPOINTER)buf;
    SQLLEN         BufferLength        = sizeof(buf);
    SQLLEN         StrLen_or_Ind;

    buf[0] = '\0';
    SQLRETURN r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (r == SQL_NO_DATA) continue;
    if (r != SQL_SUCCESS) break;
    if (StrLen_or_Ind == SQL_NULL_DATA) {
      D("Column[#%d]: [[null]]", i);
      continue;
    }

    D("Column[#%d]: [%s]", i, buf);

    r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (FAILED(r) && r != SQL_NO_DATA) break;

    r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (FAILED(r) && r != SQL_NO_DATA) break;
  }

  if (i <= ColumnCount) return -1;

  return 0;
}

__attribute__((unused))
static int test_sql_stmt_execute_direct(SQLHANDLE stmth, const char *statement)
{
  SQLRETURN r = SQL_SUCCESS;
  SQLSMALLINT ColumnCount = 0;
  int rr = 0;

  r = CALL_SQLExecDirect(stmth, (SQLCHAR*)statement, (SQLINTEGER)strlen(statement));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_SQLNumResultCols(stmth, &ColumnCount);
  if (FAILED(r)) return -1;

  r = CALL_SQLFetch(stmth);
  if (r == SQL_NO_DATA) return 0;

  rr = _sql_stmt_get_data(stmth, ColumnCount);
  if (rr == 0) {
    rr = _sql_stmt_get_data(stmth, ColumnCount);
  }

  CALL_SQLCloseCursor(stmth);

  if (rr) return -1;

  r = CALL_SQLExecDirect(stmth, (SQLCHAR*)statement, (SQLINTEGER)strlen(statement));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_SQLNumResultCols(stmth, &ColumnCount);
  if (FAILED(r)) return -1;

  r = CALL_SQLFetch(stmth);
  if (r == SQL_NO_DATA) return 0;

  rr = _sql_stmt_get_long_data(stmth, ColumnCount);
  if (rr == 0) {
    if (ColumnCount > 1) {
      rr = _sql_stmt_get_long_data(stmth, ColumnCount);
    } else {
      rr = _sql_stmt_get_long_data(stmth, ColumnCount);
    }
  }

  CALL_SQLCloseCursor(stmth);

  return rr ? -1 : 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "xshow databases", -1);
  CHK2(test_sql_stmt_execute_direct, stmth, "show databases", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop database if exists foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create database foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "use foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "show tables", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table t (ts timestamp, v int)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select count(*) from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, v) values (now, 123)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select count(*) from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop stable if exists s", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create stable s (ts timestamp, v int) tags (id int, name nchar(10))", 0);

  return 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct_prepare(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "create database if not exists foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "use foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, v int)", 0);

  // SQLRETURN r;
  // SQL_TIMESTAMP_STRUCT ts = {};
  // SQLLEN cbOrderDate;

  // r = CALL_SQLBindParameter(stmth, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, sizeof(ts), 0, &ts, 0, &cbOrderDate);
  // D("input: ipar: %d", 1);
  // D("input: fParamType: %d", SQL_PARAM_INPUT);
  // D("input: fCType: %d", SQL_C_TYPE_TIMESTAMP);
  // D("input: fSqlType: %d", SQL_TYPE_TIMESTAMP);
  // D("input: cbColDef: %ld", sizeof(ts));
  // D("input: ibScale: %d", 0);
  // D("input: rgbValue: %p", &ts);
  // D("input: cbValueMax: %d", 0);
  // D("input: pcbValue: %p", &cbOrderDate);
  // if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  // if (r != SQL_SUCCESS) return -1;

  return 0;
}

__attribute__((unused))
static int do_sql_stmt(SQLHANDLE connh)
{
  SQLRETURN r;
  SQLHANDLE stmth;
  SQLHANDLE desch;

  r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, connh, &stmth);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_SQLAllocHandle(SQL_HANDLE_DESC, connh, &desch);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = SQLSetStmtAttr(stmth, SQL_ATTR_APP_PARAM_DESC, desch, 0);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  do {
    r = do_sql_stmt_execute_direct(stmth);
    if (r) break;

    r = do_sql_stmt_execute_direct_prepare(stmth);
  } while (0);

  SQLFreeHandle(SQL_HANDLE_STMT, stmth);

  SQLFreeHandle(SQL_HANDLE_DESC, desch);

  return r ? -1 : 0;
}

__attribute__((unused))
static int do_conn_get_info(SQLHANDLE connh)
{
  SQLRETURN  r;
  SQLSMALLINT     StringLength;

  char buf[1024];

  r = CALL_SQLGetInfo(connh, SQL_DRIVER_NAME, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DRIVER_NAME: %s", buf);

  r = CALL_SQLGetInfo(connh, SQL_DBMS_NAME, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DBMS_NAME: %s", buf);

  r = CALL_SQLGetInfo(connh, SQL_DM_VER, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DM_VER: %s", buf);

  // Test SQL_CATALOG_NAME_SEPARATOR with buf as non-NULL
  r = CALL_SQLGetInfo(connh, SQL_CATALOG_NAME_SEPARATOR, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_CATALOG_NAME_SEPARATOR (non-NULL): %s", buf);

  // Test SQL_CATALOG_NAME_SEPARATOR with buf as NULL
  r = CALL_SQLGetInfo(connh, SQL_CATALOG_NAME_SEPARATOR, NULL, 0, &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_CATALOG_NAME_SEPARATOR (NULL): StringLength=%d", StringLength);

  return 0;
}

__attribute__((unused))
static int test_sql_driver_conn(SQLHANDLE connh, const char *conn_str)
{
  SQLRETURN r;
  SQLHWND WindowHandle = NULL;
  SQLCHAR *InConnectionString = (SQLCHAR*)conn_str;
  SQLSMALLINT StringLength1 = (SQLSMALLINT)strlen(conn_str);
  SQLCHAR OutConnectionString[1024];
  SQLSMALLINT BufferLength = sizeof(OutConnectionString);
  SQLSMALLINT StringLength2 = 0;
  SQLUSMALLINT DriverCompletion = SQL_DRIVER_NOPROMPT;

  OutConnectionString[0] = '\0';

  r = CALL_SQLDriverConnect(connh, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, &StringLength2, DriverCompletion);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  do {
// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_conn_get_info(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */

    if (r) break;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_sql_stmt(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */
  } while (0);

  SQLDisconnect(connh);

  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_conn(SQLHANDLE connh, const char *dsn, const char *uid, const char *pwd)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(connh, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  do {
// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_conn_get_info(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */

    if (r) break;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_sql_stmt(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */
  } while (0);

  CALL_SQLDisconnect(connh);

  return r ? -1 : 0;
}

__attribute__((unused))
static int test_new_user_connect(const char *dsn, const char *uid, const char *pwd)
{
  SQLHENV   env   = SQL_NULL_HENV;
  SQLHDBC   dbc   = SQL_NULL_HDBC;
  SQLHSTMT  stmt  = SQL_NULL_HSTMT;
  SQLRETURN sr    = SQL_SUCCESS;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = CALL_SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = CALL_SQLConnect(dbc, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)uid, SQL_NTS, (SQLCHAR *)pwd, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = CALL_SQLExecDirect(stmt, (SQLCHAR *)"show databases", SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

end:
  if (stmt != SQL_NULL_HSTMT) {
    CALL_SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    stmt = SQL_NULL_HSTMT;
  }

  if (dbc != SQL_NULL_HDBC) {
    CALL_SQLDisconnect(dbc);
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    dbc = SQL_NULL_HDBC;
  }

  if (env != SQL_NULL_HENV) {
    CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
    env = SQL_NULL_HENV;
  }

  return (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) ? 0 : -1;
}

__attribute__((unused))
static int test_sql_conn_special_char(SQLHANDLE connh, const char *dsn, const char *uid, const char *pwd)
{
  int r = -1;
  SQLRETURN sr = SQL_SUCCESS;
  SQLHANDLE stmth = SQL_NULL_HSTMT;
  char sql[1024];


  sr = CALL_SQLConnect(connh, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)NULL, SQL_NTS, (SQLCHAR *)NULL, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, connh, &stmth);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    goto end;

  (void)snprintf(sql, sizeof(sql), "create user %s pass '%s' sysinfo 1 createdb 1", uid, pwd);
  r = CALL_SQLExecDirect(stmth, (SQLCHAR *)sql, SQL_NTS);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    goto end;

  r = test_new_user_connect(dsn, uid, pwd);
  if (r != 0) {
    D("test_new_user_connect failed, uid: %s, pwd: %s", uid, pwd);
    goto end;
  }

  (void)snprintf(sql, sizeof(sql), "drop user %s", uid);
  r = CALL_SQLExecDirect(stmth, (SQLCHAR *)sql, SQL_NTS);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    goto end;

  // Success
  r = 0;

end:
  if (stmth != SQL_NULL_HSTMT) {
    CALL_SQLFreeHandle(SQL_HANDLE_STMT, stmth);
    stmth = SQL_NULL_HSTMT;
  }

  CALL_SQLDisconnect(connh);
  return r;
}

__attribute__((unused)) static int do_sql_driver_conns(SQLHANDLE connh)
{
#ifndef FAKE_TAOS
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", "", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", "taosdata", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", "", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", NULL, -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, "", -1);
  CHK2(test_sql_driver_conn, connh, "bad", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=NOT_EXIST", -1);
#ifndef _WIN32                     /* { */
  // NOTE: since TDengine 3.1.x.x, taosd is not included in so-called TDengine OSS package on Windows Platform
  //       no free lunch, haha
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};Server=" SERVER_FOR_TEST "", 0);
#endif                             /* } */
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN", 0);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};DB=what", -1);
#ifndef _WIN32                     /* { */
  // NOTE: since TDengine 3.1.x.x, taosd is not included in so-called TDengine OSS package on Windows Platform
  //       no free lunch, haha
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN;Server=" SERVER_FOR_TEST "", 0);
#endif                             /* } */
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN;Server=127.0.0.1:6666", -1);
#endif

#ifdef HAVE_TAOSWS                /* { */
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", NULL, NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", NULL, "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", "wrong-password", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", "", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "", "taosdata", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "", "", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "", NULL, -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", NULL, "", -1);
  CHK2(test_sql_driver_conn, connh, "bad", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=NOT_EXIST", -1);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};URL={http://www.examples.com};Server=" WS_FOR_TEST "", 0);
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_WS_DSN", 0);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};URL={http://" WS_FOR_TEST "};DB=what", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_WS_DSN;URL={http://www.examples.com};Server=" WS_FOR_TEST "", 0);

  // Note that the ws_connect interface is blocking
  // CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_WS_DSN;URL={http://www.examples.com};Server=127.0.0.1:6666", -1);

  CHK4(test_sql_conn_special_char, connh, "TAOS_ODBC_WS_DSN", "user_for_api_test", "aD5!@#$%!^&*()!-_+=[]{}:;><?|~,.", 0);

#endif                            /* } */

  return 0;
}

__attribute__((unused))
static int do_sql_alloc_conn(SQLHANDLE envh)
{
  SQLRETURN r;
  SQLHANDLE connh;

  r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, envh, &connh);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
  r = do_sql_driver_conns(connh);
#ifdef LOOPING        /* { */
  if (r == 0) goto again;
#endif                /* } */

  SQLFreeHandle(SQL_HANDLE_DBC, connh);
  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_alloc_env(void)
{
  SQLRETURN r;
  SQLHANDLE envh;

  int ret = tod_setenv("TAOS_ODBC_DEBUG_BISON", "1", 1);
  if (ret) {
    int e = errno;
    D("set env TAOS_ODBC_DEBUG_BISON failed, error:[%d]%s", e, strerror(e));
    return -1;
  }

  r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envh);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
    D("failure in SQLAllocHandle(SQL_HANDLE_ENV)");
    return -1;
  }

  r = CALL_SQLSetEnvAttr(envh, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
  r = do_sql_alloc_conn(envh);
#ifdef LOOPING        /* { */
  if (r==0) goto again;
#endif                /* } */

  SQLFreeHandle(SQL_HANDLE_ENV, envh);

  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_end_tran() {
  SQLHENV envh;
  SQLHDBC connh;
  SQLRETURN sr;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLSetEnvAttr(envh, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  // End transaction (commit)
  sr = CALL_SQLEndTran(SQL_HANDLE_ENV, envh, SQL_COMMIT);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  // End transaction (rollback)
  sr = CALL_SQLEndTran(SQL_HANDLE_ENV, envh, SQL_ROLLBACK);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, envh, &connh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLConnect(connh, (SQLCHAR*)"TAOS_ODBC_DSN", SQL_NTS, (SQLCHAR*)NULL, SQL_NTS, (SQLCHAR*)NULL, SQL_NTS);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  // End transaction (commit)
  sr = CALL_SQLEndTran(SQL_HANDLE_DBC, connh, SQL_COMMIT);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  // End transaction (rollback)
  sr = CALL_SQLEndTran(SQL_HANDLE_DBC, connh, SQL_ROLLBACK);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLDisconnect(connh);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DBC, connh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_ENV, envh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  D("test_sql_end_tran passed.");
  return 0;
}

__attribute__((unused))
static int test_sql_diag_rec() {
  SQLHENV envh;
  SQLHDBC connh;
  SQLHDBC desch;
  SQLRETURN sr;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envh);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLSetEnvAttr(envh, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, envh, &connh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLConnect(connh, (SQLCHAR*)"TAOS_ODBC_DSN", SQL_NTS, (SQLCHAR*)NULL, SQL_NTS, (SQLCHAR*)NULL, SQL_NTS);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  // Intentionally cause an error by setting an invalid attribute
  sr = CALL_SQLSetEnvAttr(envh, -1, NULL, 0);
  assert(sr == SQL_ERROR);

  const char* file = __FILE__;
  const char* func = __func__;
  int line = __LINE__;
  diag(sr, SQL_HANDLE_ENV, envh);


  sr = CALL_SQLAllocHandle(SQL_HANDLE_DESC, connh, &desch);
  assert(sr == SQL_SUCCESS);

  // Intentionally cause an error by setting an invalid descriptor attribute
  sr = SQLSetDescField(desch, -1, SQL_DESC_COUNT, (SQLPOINTER)0, 0);
  assert(sr == SQL_ERROR);

  line = __LINE__;
  diag(sr, SQL_HANDLE_DESC, desch);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DESC, desch);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLDisconnect(connh);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DBC, connh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_ENV, envh);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  D("test_sql_diag_rec passed.\n");
  return 0;
}

__attribute__((unused))
static void test_sql_diag_field_common(SQLSMALLINT handleType, SQLHANDLE handle) {
  SQLRETURN sr;
  SQLCHAR sqlState[6];
  SQLLEN rowNumber;
  SQLINTEGER diagNumber = 0;

  // Retrieve SQL state using SQLGetDiagField
  sr = CALL_SQLGetDiagField(handleType, handle, 1, SQL_DIAG_SQLSTATE, sqlState, sizeof(sqlState), NULL);
  assert(sr == SQL_SUCCESS);
  D("SQLGetDiagField (%d - SQLState): %s", handleType, sqlState);

  // Retrieve row number using SQLGetDiagField
  sr = CALL_SQLGetDiagField(handleType, handle, 1, SQL_DIAG_ROW_NUMBER, &rowNumber, sizeof(rowNumber), NULL);
  assert(sr == SQL_SUCCESS);
  D("SQLGetDiagField (%d - RowNumber): %lld", handleType, (long long)rowNumber);

  // Retrieve diagnostic number using SQLGetDiagField
  sr = CALL_SQLGetDiagField(handleType, handle, 1, SQL_DIAG_NUMBER, &diagNumber, sizeof(diagNumber), NULL);
  // assert(sr == SQL_SUCCESS);
  D("SQLGetDiagField (%d - DiagNumber): %d", handleType, diagNumber);
}

__attribute__((unused))
static void test_sql_diag_field_env() {
  SQLHENV env;
  SQLRETURN sr;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  assert(sr == SQL_SUCCESS);

  // Intentionally cause an error by setting an invalid attribute
  sr = CALL_SQLSetEnvAttr(env, -1, NULL, 0);
  assert(sr == SQL_ERROR);

  // Test SQLGetDiagField for environment handle
  test_sql_diag_field_common(SQL_HANDLE_ENV, env);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
  assert(sr == SQL_SUCCESS);

  D("test_sql_diag_field_env passed.");
}

__attribute__((unused))
static void test_sql_diag_field_dbc() {
  SQLHENV env;
  SQLHDBC dbc;
  SQLRETURN sr;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  assert(sr == SQL_SUCCESS);

  // Intentionally cause an error by connecting with invalid DSN
  sr = SQLConnect(dbc, (SQLCHAR*)"InvalidDSN", SQL_NTS, NULL, 0, NULL, 0);
  assert(sr == SQL_ERROR);

  // Test SQLGetDiagField for connection handle
  test_sql_diag_field_common(SQL_HANDLE_DBC, dbc);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
  assert(sr == SQL_SUCCESS);

  D("test_sqldiagfield_dbc passed.");
}

__attribute__((unused))
static void test_sql_diag_field_stmt() {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN sr;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLConnect(dbc, (SQLCHAR*)"TAOS_ODBC_DSN", SQL_NTS, (SQLCHAR*)NULL, SQL_NTS, (SQLCHAR*)NULL, SQL_NTS);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  assert(sr == SQL_SUCCESS);

  // Intentionally cause an error by executing an invalid SQL statement
  sr = CALL_SQLExecDirect(stmt, (SQLCHAR*)"INVALID SQL", SQL_NTS);
  assert(sr == SQL_ERROR);

  // Test SQLGetDiagField for statement handle
  test_sql_diag_field_common(SQL_HANDLE_STMT, stmt);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLDisconnect(dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
  assert(sr == SQL_SUCCESS);

  D("test_sql_diag_field_stmt passed.");
}

__attribute__((unused))
static void test_sql_diag_field_desc() {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHDESC desc;
  SQLRETURN sr;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLConnect(dbc, (SQLCHAR*)"TAOS_ODBC_DSN", SQL_NTS, (SQLCHAR*)NULL, SQL_NTS, (SQLCHAR*)NULL, SQL_NTS);
  assert(sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO);

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DESC, dbc, &desc);
  assert(sr == SQL_SUCCESS);

  // Intentionally cause an error by setting an invalid descriptor field
  sr = SQLSetDescField(desc, -1, SQL_DESC_COUNT, (SQLPOINTER)0, 0);
  assert(sr == SQL_ERROR);

  // Test SQLGetDiagField for descriptor handle
  test_sql_diag_field_common(SQL_HANDLE_DESC, desc);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DESC, desc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLDisconnect(dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  assert(sr == SQL_SUCCESS);

  sr = CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
  assert(sr == SQL_SUCCESS);

  D("test_sql_diag_field_desc passed.");
}

__attribute__((unused))
static int test_sql_diag_field() {
  // test_sql_diag_field_env();
  // test_sql_diag_field_dbc();
  test_sql_diag_field_stmt();
  // test_sql_diag_field_desc();
  D("test_sql_diag_field passed.");
  return 0;
}

static int do_cases(void)
{
  CHK0(test_ok, 0);
  CHK0(test_failure, -1);
#ifdef __APPLE__
  CHK1(test_so, "/tmp/not_exists.dylib", -1);
  CHK1(test_so, "libtaos_odbc.dylib", 0);
#elif defined(_WIN32)
  CHK1(test_so, "taos_odbc.dll", -1);

  char dll_fullname[256] = {0};
#ifdef TODBC_X86
  char *dll_fullpath = "C:/TDengine/taos_odbc/x86/bin";
#else
  char *dll_fullpath = "C:/TDengine/taos_odbc/x64/bin";
#endif
  (void)snprintf(dll_fullname, sizeof(dll_fullname), "%s/%s", dll_fullpath, "taos_odbc.dll");

  if (!SetDllDirectory(dll_fullpath)) {  
    E("set dll directory failed.");
    return -1;
  }
  CHK1(test_so, dll_fullname, 0);

  CHK1(test_so, "taos_odbc.dll", 0);
#else
  CHK1(test_so, "/tmp/not_exists.so", -1);
  CHK1(test_so, "libtaos_odbc.so", 0);
#endif
  CHK0(test_sql_alloc_env, 0);
  CHK0(test_sql_end_tran, 0);
  CHK0(test_sql_diag_rec, 0);
  CHK0(test_sql_diag_field, 0);

  return 0;
}

#define RUN_E(fmt, ...) fprintf(stderr, "" fmt "\n", ##__VA_ARGS__)

typedef struct options_s          options_t;
struct options_s {
  const char *dsn_or_conn;
  const char *uid;
  const char *pwd;

  uint8_t     dsn:1;
};

int open_conn(SQLHANDLE *henv, SQLHANDLE *hconn, SQLHANDLE *hstmt, const options_t *options)
{
  int r = -1;

  SQLRETURN sr;

  sr = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, henv);
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLAllocHandle failure");
    goto end1;
  }

  sr = SQLSetEnvAttr(*henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLSetEnvAttr failure");
    goto end2;
  }

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, *henv, hconn);
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLAllocHandle failure");
    goto end3;
  }

  if (options->dsn) {
    sr = CALL_SQLConnect(*hconn, (SQLCHAR*)options->dsn_or_conn, SQL_NTS, (SQLCHAR*)options->uid, SQL_NTS, (SQLCHAR*)options->pwd, SQL_NTS);
    if (!SQL_SUCCEEDED(sr)) {
      RUN_E("SQLConnect failure");
      goto end4;
    }
  } else {
    sr = CALL_SQLDriverConnect(*hconn, NULL, (SQLCHAR*)options->dsn_or_conn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(sr)) {
      RUN_E("SQLDriverConnect failure");
      goto end4;
    }
  }

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, *hconn, hstmt);
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLAllocHandle failure");
    goto end5;
  }

  return 0;

end5:
  SQLDisconnect(*hconn);

end4:

end3:
  SQLFreeHandle(SQL_HANDLE_DBC, *hconn);
  *hconn = SQL_NULL_HANDLE;

end2:
  SQLFreeHandle(SQL_HANDLE_ENV, *henv);
  *henv = SQL_NULL_HANDLE;

end1:

  return r;
}

static int exec_direct(SQLHANDLE hstmt, const char *sql)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLExecDirect failure");
    return -1;
  }

  return 0;
}

static int run_SQLGetData2(SQLHANDLE hstmt, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType, SQLLEN BufferLength, size_t fix_length)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  sr = SQLFetch(hstmt);
  if (sr == SQL_NO_DATA) return 0;
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLExecDirect failure");
    return -1;
  }

  size_t bytes = BufferLength > 0 ? (size_t)BufferLength : 0;
  if (bytes < fix_length) bytes = fix_length;

  SQLCHAR *p = (SQLCHAR*)calloc(1, bytes);
  if (!p) {
    RUN_E("calloc failure");
    return -1;
  }

  r = -1;

  SQLLEN StrLen_or_Ind = 0;
  SQLLEN nr = 0;

again:

  if (BufferLength>0) memset(p, 'x', BufferLength);
  StrLen_or_Ind = 0;
  sr = CALL_SQLGetData(hstmt, Col_or_Param_Num, TargetType, p, BufferLength, &StrLen_or_Ind);
  fprintf(stdout, "sr:[%d]%s\n", sr, sql_return_type(sr));
  fprintf(stdout, "StrLen_or_Ind:%" PRId64 "\n", (int64_t)StrLen_or_Ind);
  if (sr == SQL_NO_DATA) goto end;

  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLGetData failure");
    goto end;
  }

  r = 0;

  if (StrLen_or_Ind < 0) {
    if (StrLen_or_Ind == SQL_NULL_DATA) goto end;
    if (StrLen_or_Ind != SQL_NO_TOTAL) {
      RUN_E("unexpected StrLen_or_Ind value");
      r = -1;
      goto end;
    }
  }

  nr = StrLen_or_Ind;
  if (sr == SQL_SUCCESS_WITH_INFO) {
    if (StrLen_or_Ind == SQL_NO_TOTAL) {
      nr = BufferLength;
    } else if (nr >= BufferLength) {
      nr = BufferLength;
    }
    if (TargetType == SQL_C_CHAR) nr -= 1;
    if (TargetType == SQL_C_WCHAR) nr = (nr / 2 - 1) * 2;
  } else {
    nr = StrLen_or_Ind;
  }

  fprintf(stdout, "data:[0x");
  for (SQLLEN i=0; i<nr; ++i) {
    unsigned char c = p[i];
    fprintf(stdout, "%02x", c);
  }
  fprintf(stdout, "]\n");

  nr = bytes;
  fprintf(stdout, "buff:[0x");
  for (SQLLEN i=0; i<nr; ++i) {
    unsigned char c = p[i];
    fprintf(stdout, "%02x", c);
  }
  fprintf(stdout, "]\n");

  if (SQL_SUCCEEDED(sr) && fix_length) goto end;

  goto again;

end:
  free(p);
  return r;
}

static int run_SQLGetData1(int argc, char *argv[], int *i, SQLHANDLE hstmt)
{
  int r = 0;

// {
#define R(x, y) {#x, x, y}
  static struct {
    const char          *name;
    SQLSMALLINT          TargetType;
    size_t               fix_length;
  } _target_types[] = {
    R(SQL_C_CHAR,           0),
    R(SQL_C_BINARY,         0),
    R(SQL_C_WCHAR,          0),
    R(SQL_C_TYPE_TIMESTAMP, sizeof(TIMESTAMP_STRUCT)),
    R(SQL_C_BIT,            sizeof(uint8_t)),
    R(SQL_C_STINYINT,       sizeof(int8_t)),
    R(SQL_C_UTINYINT,       sizeof(uint8_t)),
    R(SQL_C_SSHORT,         sizeof(int16_t)),
    R(SQL_C_USHORT,         sizeof(uint16_t)),
    R(SQL_C_SLONG,          sizeof(int32_t)),
    R(SQL_C_ULONG,          sizeof(uint32_t)),
    R(SQL_C_SBIGINT,        sizeof(int64_t)),
    R(SQL_C_UBIGINT,        sizeof(uint64_t)),
    R(SQL_C_FLOAT,          sizeof(float)),
    R(SQL_C_DOUBLE,         sizeof(double)),
    R(SQL_C_NUMERIC,        sizeof(SQL_NUMERIC_STRUCT)),
  };
#undef R
// }

  SQLSMALLINT       TargetType       = SQL_C_CHAR;
  SQLLEN            BufferLength     = 1024;
  SQLUSMALLINT      Col_or_Param_Num = 1;
  size_t            fix_length       = 0;

  for (++*i; *i<argc; ++*i) {
    const char *arg = argv[*i];
    if (strcmp(arg, "-b") == 0) {
      if (++*i>=argc) {
        RUN_E("-b requires an argument for <TargetType>");
        return -1;
      }
      r = -1;
      for (size_t j=0; j<sizeof(_target_types)/sizeof(_target_types[0]); ++j) {
        if (strcmp(argv[*i], _target_types[j].name)) continue;
        TargetType      = _target_types[j].TargetType;
        fix_length      = _target_types[j].fix_length;
        r = 0;
        break;
      }
      if (r) {
        RUN_E("unknown TargetType:-b %s", argv[*i]);
        return -1;
      }
      continue;
    }
    if (strcmp(arg, "-n") == 0) {
      if (++*i>=argc) {
        RUN_E("-n requires an argument for <BufferLength>");
        return -1;
      }
      char *end = 0;
      errno = 0;
      long v = strtol(argv[*i], &end, 0);
      if (*end) {
        RUN_E("invalid BufferLength:-n %s", argv[*i]);
        return -1;
      }
      if (errno == ERANGE && (v == LONG_MIN || v == LONG_MAX)) {
        RUN_E("invalid BufferLength:-n %s", argv[*i]);
        return -1;
      }

      BufferLength = v;

      continue;
    }
    if (strcmp(arg, "-c") == 0) {
      if (++*i>=argc) {
        RUN_E("-c requires an argument for <Col_or_Param_Num>");
        return -1;
      }
      char *end = 0;
      errno = 0;
      long v = strtol(argv[*i], &end, 0);
      if (*end) {
        RUN_E("invalid Col_or_Param_Num:-c %s", argv[*i]);
        return -1;
      }
      if (errno == ERANGE && (v == LONG_MIN || v == LONG_MAX)) {
        RUN_E("invalid Col_or_Param_Num:-c %s", argv[*i]);
        return -1;
      }

      Col_or_Param_Num = (SQLUSMALLINT)v; // NOTE: yes, we know it might underflow 

      continue;
    }

    r = exec_direct(hstmt, arg);
    if (r) return -1;

    r = run_SQLGetData2(hstmt, Col_or_Param_Num, TargetType, BufferLength, fix_length);
    if (r) return -1;

    ++*i;

    break;
  }

  return 0;
}

static int run_SQLGetData(int argc, char *argv[], int *i, const options_t *options)
{
  int r = 0;

  SQLHANDLE henv     = SQL_NULL_HANDLE;
  SQLHANDLE hconn    = SQL_NULL_HANDLE;
  SQLHANDLE hstmt    = SQL_NULL_HANDLE;

  r = open_conn(&henv, &hconn, &hstmt, options);
  if (r) goto end;

  r = run_SQLGetData1(argc, argv, i, hstmt);

  SQLFreeStmt(hstmt, SQL_UNBIND);
  SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt); hstmt = SQL_NULL_HANDLE;
  SQLDisconnect(hconn);
  SQLFreeHandle(SQL_HANDLE_DBC,  hconn); hconn = SQL_NULL_HANDLE;
  SQLFreeHandle(SQL_HANDLE_ENV,  henv);  henv  = SQL_NULL_HANDLE;

end:
  return r;
}

static int run_SQLDescribeCol2(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT ColumnCount = 0;

  sr = SQLNumResultCols(hstmt, &ColumnCount);
  if (!SQL_SUCCEEDED(sr)) {
    RUN_E("SQLNumResultCols failure");
    return -1;
  }

  for (SQLSMALLINT i=1; i<=ColumnCount; ++i) {
    SQLCHAR     ColumnName[4096]; *ColumnName = '\0';
    SQLSMALLINT BufferLength  = (SQLSMALLINT)sizeof(ColumnName);
    SQLSMALLINT NameLength    = 0;
    SQLSMALLINT DataType      = 0;
    SQLULEN     ColumnSize    = 0;
    SQLSMALLINT DecimalDigits = 0;
    SQLSMALLINT Nullable      = 0;
    sr = CALL_SQLDescribeCol(hstmt, i, ColumnName, BufferLength, &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (!SQL_SUCCEEDED(sr)) {
      RUN_E("SQLDescribeCol failure");
      return -1;
    }
    if (i>1) fprintf(stdout, "\n");
    fprintf(stdout, "   ColumnName:[%d][%.*s]\n", NameLength, NameLength, (const char*)ColumnName);
    fprintf(stdout, "     DataType:[%d]%s\n", DataType, sql_data_type(DataType));
    fprintf(stdout, "   ColumnSize:[%" PRIu64 "]\n", (uint64_t)ColumnSize);
    fprintf(stdout, "DecimalDigits:[%d]\n", DecimalDigits);
    fprintf(stdout, "     Nullable:[%d]%s\n", Nullable, sql_nullable(Nullable));
  }

  return 0;
}

static int run_SQLDescribeCol1(int argc, char *argv[], int *i, SQLHANDLE hstmt)
{
  int r = 0;

  for (++*i; *i<argc; ++*i) {
    const char *arg = argv[*i];
    r = exec_direct(hstmt, arg);
    if (r) return -1;
    r = run_SQLDescribeCol2(hstmt);
    if (r) return -1;

    ++*i;

    break;
  }

  return 0;
}

static int run_SQLDescribeCol(int argc, char *argv[], int *i, const options_t *options)
{
  int r = 0;

  SQLHANDLE henv     = SQL_NULL_HANDLE;
  SQLHANDLE hconn    = SQL_NULL_HANDLE;
  SQLHANDLE hstmt    = SQL_NULL_HANDLE;

  r = open_conn(&henv, &hconn, &hstmt, options);
  if (r) goto end;

  r = run_SQLDescribeCol1(argc, argv, i, hstmt);

  SQLFreeStmt(hstmt, SQL_UNBIND);
  SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt); hstmt = SQL_NULL_HANDLE;
  SQLDisconnect(hconn);
  SQLFreeHandle(SQL_HANDLE_DBC,  hconn); hconn = SQL_NULL_HANDLE;
  SQLFreeHandle(SQL_HANDLE_ENV,  henv);  henv  = SQL_NULL_HANDLE;

end:
  return r;
}

static int run(int argc, char *argv[])
{
  int r = 0;

  options_t      options = {NULL};

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (tod_strcasecmp(arg, "-d") == 0) {
      if (++i>=argc) {
        RUN_E("-d requires an argument for <DSN>");
        return -1;
      }
      options.dsn_or_conn = argv[i];
      options.dsn         = 1;
      continue;
    }
    if (tod_strcasecmp(arg, "-c") == 0) {
      if (++i>=argc) {
        RUN_E("-c requires an argument for <connection string>");
        return -1;
      }
      options.dsn_or_conn = argv[i];
      options.dsn         = 0;
      continue;
    }
    if (tod_strcasecmp(arg, "-u") == 0) {
      if (++i>=argc) {
        RUN_E("-u requires an argument for <UID>");
        return -1;
      }
      options.uid         = argv[i];
      continue;
    }
    if (tod_strcasecmp(arg, "-p") == 0) {
      if (++i>=argc) {
        RUN_E("-p requires an argument for <PWD>");
        return -1;
      }
      options.pwd         = argv[i];
      continue;
    }

    if (strcmp(arg, "SQLGetData") == 0) {
      if (options.dsn_or_conn == NULL) {
        RUN_E("missing -d <DSN> or -c <connectiong string");
        return -1;
      }
      r = run_SQLGetData(argc, argv, &i, &options);
      if (r) return -1;
      --i;
      continue;
    }

    if (strcmp(arg, "SQLDescribeCol") == 0) {
      if (options.dsn_or_conn == NULL) {
        RUN_E("missing -d <DSN> or -c <connectiong string");
        return -1;
      }
      r = run_SQLDescribeCol(argc, argv, &i, &options);
      if (r) return -1;
      --i;
      continue;
    }

    RUN_E("unknown argument:[%d]%s", i, arg);
    return -1;
  }

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

  if (argc > 1) {
    return run(argc, argv);
  }

// #define CHK_LEAK
#ifdef CHK_LEAK                /* { */
again:
#endif                          /* } */
  r = do_cases();
#ifdef CHK_LEAK                /* { */
  if (r == 0) goto again;
#endif                          /* } */

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

