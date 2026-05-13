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

#define DUMP(fmt, ...) fprintf(stderr, fmt "\n", ##__VA_ARGS__)

static int run_with_stmt(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;

  r = odbc_case->run(odbc_case, ODBC_STMT, handles);
  DUMP("");

  return r ? -1 : 0;
}

static int run_with_connected_conn(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024]; buf[0] = '\0';

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DRIVER_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DRIVER_NAME:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DRIVER_VER, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DRIVER_VER:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DRIVER_ODBC_VER, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DRIVER_ODBC_VER:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DBMS_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DBMS_NAME:%s", buf);
    handles->taos_backend = (strcmp(buf, "tdengine") == 0) ? 1 : 0;
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DBMS_VER, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DBMS_VER:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_SERVER_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("SERVER_NAME:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DATA_SOURCE_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DATA_SOURCE_NAME:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_CATALOG_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("CATALOG_NAME:%s", buf);
    if (buf[0] == 'Y') handles->support_catalog = 1;
  }

  r = odbc_case->run(odbc_case, ODBC_DBC, handles);
  DUMP("");
  if (r) return -1;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hconn, &handles->hstmt);
  if (FAILED(sr)) return -1;

  r = run_with_stmt(odbc_case, handles);
  DUMP("");

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
  handles->hstmt = SQL_NULL_HANDLE;

  return r ? -1 : 0;
}

static int _connect(SQLHANDLE hconn, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (FAILED(sr)) {
    E("connect [dsn:%s,uid:%s,pwd:%s] failed", dsn, uid, pwd);
    return -1;
  }

  return 0;
}

static int _driver_connect(SQLHANDLE hconn, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024];
  buf[0] = '\0';
  SQLSMALLINT StringLength = 0;
  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength, SQL_DRIVER_COMPLETE);
  if (FAILED(sr)) {
    E("driver_connect [%s] failed", connstr);
    return -1;
  }

  DUMP("connection str:%s", buf);
  return 0;
}

static int _connect_by_arg(const odbc_conn_arg_t *conn_arg, odbc_handles_t *handles)
{
  if (conn_arg->connstr) {
    return _driver_connect(handles->hconn, conn_arg->connstr);
  } else {
    return _connect(handles->hconn, conn_arg->dsn, conn_arg->uid, conn_arg->pwd);
  }
}

static int run_with_conn(odbc_case_t *odbc_case, const odbc_conn_arg_t *conn_arg, odbc_handles_t *handles)
{
  int r = 0;

  r = _connect_by_arg(conn_arg, handles);
  if (r) return -1;

  r = run_with_connected_conn(odbc_case, handles);

  CALL_SQLDisconnect(handles->hconn);

  return r ? -1 : 0;
}

static int run_with_env(odbc_case_t *odbc_case, const odbc_conn_arg_t *conn_arg, odbc_handles_t *handles)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, handles->henv, &handles->hconn);
  if (FAILED(sr)) return -1;

  do {
    r = odbc_case->run(odbc_case, ODBC_ENV, handles);
    DUMP("");
    if (r) break;

    r = run_with_conn(odbc_case, conn_arg, handles);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, handles->hconn);
  handles->hconn = SQL_NULL_HANDLE;

  return r ? -1 : 0;
}

static int run_odbc_case(odbc_case_t *odbc_case, const odbc_conn_arg_t *conn_arg, odbc_handles_t *handles)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = odbc_case->run(odbc_case, ODBC_INITED, handles);
  DUMP("");
  if (r) return -1;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->henv);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = run_with_env(odbc_case, conn_arg, handles);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, handles->henv);
  handles->henv = SQL_NULL_HANDLE;

  return r ? -1 : 0;
}

int run_odbc_cases(const char *name, const odbc_conn_arg_t *conn_arg, odbc_case_t *cases, size_t cases_nr)
{
  int r = 0;

  for (size_t i=0; i<cases_nr; ++i) {
    odbc_case_t *odbc_case = cases + i;
    if (name && strcmp(name, odbc_case->name)) continue;
    odbc_handles_t handles = {
      .henv          = SQL_NULL_HANDLE,
      .hconn         = SQL_NULL_HANDLE,
      .hstmt         = SQL_NULL_HANDLE,
    };
    r = run_odbc_case(odbc_case, conn_arg, &handles);
    if (r) return -1;
  }

  return 0;
}

