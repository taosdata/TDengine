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

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)


int main (int argc, char *argv[])
{
  (void)argc;
  (void)argv;

  SQLHENV   henv  = SQL_NULL_HENV;
  SQLHDBC   hdbc  = SQL_NULL_HDBC;
  SQLHSTMT  hstmt = SQL_NULL_HSTMT;
  SQLRETURN sr;

  char ts[1024]; ts[0] = '\0';
  char name[1024]; name[0] = '\0';
  int age = 0;
  SQLLEN len_ts = 0, len_name = 0, len_age = 0;

  // create table foo.t (ts timestamp, name varchar(20), age int)
  const char *dsn = "TAOS_ODBC_DSN";
  const char *sql = "select ts, name, age from foo.t";

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  sr = CALL_SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
  sr = CALL_SQLConnect(hdbc, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*) NULL, 0, NULL, 0);
  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  sr = CALL_SQLBindCol(hstmt, 1, SQL_C_CHAR,   &ts,   sizeof(ts),   &len_ts);
  sr = CALL_SQLBindCol(hstmt, 2, SQL_C_CHAR,   &name, sizeof(name), &len_name);
  sr = CALL_SQLBindCol(hstmt, 3, SQL_C_SLONG,  &age,  sizeof(age),  &len_age);

  for (int i=0; ; i++) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) break;
    if (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) {
      DUMP("row[%d]: ts[%s], name[%s], age[%d]", i+1, ts, name, age);
      continue;
    }
    break;
  }

  if (hstmt != SQL_NULL_HSTMT) CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  if (hdbc != SQL_NULL_HDBC) {
    CALL_SQLDisconnect(hdbc);
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  }

  if (henv != SQL_NULL_HENV) CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  DUMP("==done==");

  return 0;
}

