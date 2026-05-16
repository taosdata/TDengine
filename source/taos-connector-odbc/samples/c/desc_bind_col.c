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

#define MAX_COLS             20

int main (int argc, char *argv[])
{
  (void)argc;
  (void)argv;

  int r = 0;

  SQLHENV   henv  = SQL_NULL_HENV;
  SQLHDBC   hdbc  = SQL_NULL_HDBC;
  SQLHSTMT  hstmt = SQL_NULL_HSTMT;
  SQLRETURN sr = SQL_SUCCESS;

  char           col_name[MAX_COLS][64];
  SQLSMALLINT    col_nameLen[MAX_COLS];
  SQLSMALLINT    col_data_type[MAX_COLS];
  SQLULEN        col_data_size[MAX_COLS];
  SQLSMALLINT    col_data_digits[MAX_COLS];
  SQLSMALLINT    col_data_nullable[MAX_COLS];
  char           col_data[MAX_COLS][64];
  SQLLEN         col_data_len[MAX_COLS];

  const char *dsn = "TAOS_ODBC_DSN";
  const char *sql = "select ts, name, age from foo.t";

  SQLSMALLINT nr_cols;

  for (int i=0;i<MAX_COLS;i++) {
    col_name[i][0]='\0';
    col_data[i][0]='\0';
  }

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  sr = CALL_SQLConnect(hdbc, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*) NULL, 0, NULL, 0);
  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  sr = CALL_SQLExecute(hstmt);

  sr = CALL_SQLNumResultCols(hstmt, &nr_cols);
  DUMP("Number of Result Columns %d", nr_cols);

  for (int i=0;i<nr_cols;i++) {
    sr = CALL_SQLDescribeCol(hstmt, i+1,
        (SQLCHAR*)col_name[i], sizeof(col_name[i]), &col_nameLen[i],
        &col_data_type[i], &col_data_size[i], &col_data_digits[i], &col_data_nullable[i]);
    if (sr != SQL_SUCCESS) break;

    DUMP("col %d: Name[%s]; Len[%d]; Type[%s]; Size[%d]; Digits[%d]; Nullable[%s]",
        i+1, col_name[i], (int)col_nameLen[i], sql_data_type(col_data_type[i]),
        (int)col_data_size[i], (int)col_data_digits[i], sql_nullable(col_data_nullable[i]));

    sr = CALL_SQLBindCol(hstmt, i+1, SQL_C_CHAR, col_data[i], sizeof(col_data[i]), &col_data_len[i]);
  }

  for (int i=0; ; i++) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) break;
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
      r = -1;
      break;
    }

    for (int j=0;j<nr_cols;j++) {
      DUMP("data[%d,%d]:[%s]", i+1, j+1, col_data[j]);
    }
  }

  if (hstmt != SQL_NULL_HSTMT) CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  if (hdbc != SQL_NULL_HDBC) {
    CALL_SQLDisconnect(hdbc);
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  }

  if (henv != SQL_NULL_HENV) CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  DUMP("==%s==", r ? "failure" : "success");

  return 0;
}
