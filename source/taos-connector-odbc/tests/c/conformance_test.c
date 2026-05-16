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

#include "../test_helper.h"

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>


#define TAOS_ODBC        0x01
#define MYSQL_ODBC       0x02
#define SQLITE3_ODBC     0x04
#define SQLSERVER_ODBC   0x08

static int _under_taos_mysql_sqlite3 = 0;  // 0: taos; 1: mysql; 2: sqlite3
static int _test_with_enterprise     = 0;

typedef struct conn_arg_s             conn_arg_t;
struct conn_arg_s {
  const char      *dsn;
  const char      *uid;
  const char      *pwd;
  const char      *connstr;
  unsigned int     odbc_type;
  unsigned int     ws;
};

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

  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) {
    E("driver_connect [connstr:%s] failed", connstr);
    return -1;
  }

  return 0;
}

static int _exec_direct(SQLHANDLE hconn, const char *sql)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _exec_and_check_count(SQLHANDLE hconn, const char *sql, size_t *count)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    *count = 0;
    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_NO_DATA) {
        sr = SQL_SUCCESS;
        break;
      }
      *count += 1;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _check_varchar(const int irow, const int icol, SQLLEN ind, const char *varchar, const char *exp)
{
  if (ind == SQL_NULL_DATA) {
    if (exp) {
      E("(r#%d, c#%d) [%s] expected, but got ==%s==", irow+1, icol+1, exp, varchar);
      return -1;
    }
    return 0;
  }
  if(ind < 0) {
    E("not implemented yet");
    return -1;
  }

  if (!exp) {
    E("(r#%d, c#%d) `null` expected, but got ==%s==", irow+1, icol+1, varchar);
    return -1;
  }

  if (strcmp(varchar, exp)) {
    E("(r#%d, c#%d) [%s] expected, but got ==%s==", irow+1, icol+1, exp, varchar);
    return -1;
  }

  return 0;
}

static int _exec_and_check_with_stmt_v(SQLHANDLE hstmt, char *buf, size_t sz, const int cols, const int rows, va_list ap)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT ColumnCount = 0;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  if (cols != ColumnCount) {
    E("#%d cols expected, but got ==%d==", cols, ColumnCount);
    return -1;
  }

  for (int i=0; i<rows; ++i) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) {
      E("#%d rows expected, but got ==%d==", rows, i);
      return -1;
    }
    if (FAILED(sr)) {
      E("fetching #%d row failed", i+1);
      return -1;
    }

    for (int j=0; j<cols; ++j) {
      if (sz > 0) buf[0] = '\0';

      SQLLEN ind = 0;
      sr = CALL_SQLGetData(hstmt, j+1, SQL_C_CHAR, (SQLPOINTER)buf, sz, &ind);
      if (FAILED(sr)) {
        E("getting (r#%d, c#%d) failed", i+1, j+1);
        return -1;
      }

      if (sr == SQL_SUCCESS) {
        const char *exp = va_arg(ap, const char*);
        r = _check_varchar(i, j, ind, buf, exp);
        if (r) return -1;
      } else if (sr == SQL_SUCCESS_WITH_INFO) {
        E("not implemented yet");
        return -1;
      } else {
        E("internal logic error");
        return -1;
      }
    }
  }

  return 0;
}

static int _exec_and_check(SQLHANDLE hconn, char *buf, size_t sz, const char *sql, const int cols, const int rows, ...)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    if (buf == (char*)-1) break;

    va_list ap;
    va_start(ap, rows);
    r = _exec_and_check_with_stmt_v(hstmt, buf, sz, cols, rows, ap);
    va_end(ap);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _exec_and_bind_check_with_stmt_v(SQLHANDLE hstmt, char *buf, size_t sz, const int cols, const int rows, va_list ap)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT ColumnCount = 0;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  if (cols != ColumnCount) {
    E("#%d cols expected, but got ==%d==", cols, ColumnCount);
    return -1;
  }

  const size_t sz_num_rows_fetched = sizeof(SQLULEN);
  const size_t sz_row_status_array = sizeof(SQLUSMALLINT) * rows;
  const size_t sz_ind_array        = sizeof(SQLLEN) * rows;
  const size_t sz_ind_arrays       = sz_ind_array * cols;
  if (sz < sz_num_rows_fetched + sz_row_status_array + sz_ind_arrays) {
    W("buffer too small");
    r = -1;
    return -1;
  }
  const size_t sz_data_arrays = sz - sz_num_rows_fetched - sz_row_status_array - sz_ind_arrays;
  const size_t sz_data_array  = sz_data_arrays / cols;
  const size_t sz_data        = sz_data_array / rows;

  const char *p = buf;
  const char *num_rows_fetched = p;           p += sz_num_rows_fetched;
  const char *row_status_array = p;           p += sz_row_status_array;
  const char *ind_arrays       = p;           p += sz_ind_arrays;
  const char *data_arrays      = p;

  int count_rows = 0;
  while (1) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) {
      if (count_rows != rows) {
        E("#%d rows expected, but got ==%d==", rows, count_rows);
        return -1;
      }
      break;
    }
    if (FAILED(sr)) {
      E("fetching #%d row failed", count_rows);
      return -1;
    }

    const SQLULEN rows_fetched = *(const SQLULEN*)num_rows_fetched;
    for (size_t i=0; i<rows_fetched; ++i) {
      const SQLUSMALLINT row_status = ((const SQLUSMALLINT*)row_status_array)[i];
      if (row_status == SQL_SUCCESS) {
        for (int j=0; j<cols; ++j) {
          const char *ind_array  = ind_arrays + sz_ind_array * j;
          const char *data_array = data_arrays + sz_data_array * j;

          const SQLLEN *ind   = (const SQLLEN*)(ind_array + sizeof(SQLLEN) * i);
          const char *data  = data_array + sz_data * i;

          if (sr == SQL_SUCCESS) {
            const char *exp = va_arg(ap, const char*);
            r = _check_varchar((int)i, j, *ind, data, exp);
            if (r) return -1;
          } else if (sr == SQL_SUCCESS_WITH_INFO) {
            E("not implemented yet");
            return -1;
          } else {
            E("internal logic error");
            return -1;
          }
        }
        count_rows += 1;
      } else {
        E("internal logic error");
        return -1;
      }
    }
  }

  return 0;
}

static int _exec_and_bind_check(SQLHANDLE hconn, char *buf, size_t sz, const char *sql, const int cols, const int rows, ...)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    if (buf == (char*)-1) break;

    if (cols && rows) {
      size_t sz_num_rows_fetched = sizeof(SQLULEN);
      size_t sz_row_status_array = sizeof(SQLUSMALLINT) * rows;
      size_t sz_ind_array        = sizeof(SQLLEN) * rows;
      size_t sz_ind_arrays       = sz_ind_array * cols;
      if (sz < sz_num_rows_fetched + sz_row_status_array + sz_ind_arrays) {
        W("buffer too small");
        r = -1;
        break;
      }
      size_t sz_data_arrays = sz - sz_num_rows_fetched - sz_row_status_array - sz_ind_arrays;
      size_t sz_data_array  = sz_data_arrays / cols;
      size_t sz_data        = sz_data_array / rows;

      char *p = buf;
      char *num_rows_fetched = p;           p += sz_num_rows_fetched;
      char *row_status_array = p;           p += sz_row_status_array;
      char *ind_arrays       = p;           p += sz_ind_arrays;
      char *data_arrays      = p;

      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
      if (FAILED(sr)) { r = -1; break; }
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(uintptr_t)rows, 0);
      if (FAILED(sr)) { r = -1; break; }
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, row_status_array, 0);
      if (FAILED(sr)) { r = -1; break; }
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR, num_rows_fetched, 0);
      if (FAILED(sr)) { r = -1; break; }

      char *ind_array  = ind_arrays;
      char *data_array = data_arrays;
      for (int i=0; i<cols; ++i) {
        sr = CALL_SQLBindCol(hstmt, i+1, SQL_C_CHAR, data_array, sz_data, (SQLLEN*)ind_array);
        if (FAILED(sr)) {
          r = -1;
          break;
        }
        ind_array  += sz_ind_array;
        data_array += sz_data_array;
      }
    }

    va_list ap;
    va_start(ap, rows);
    r = _exec_and_bind_check_with_stmt_v(hstmt, buf, sz, cols, rows, ap);
    va_end(ap);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _prepare_dataset_conn(SQLHANDLE hconn, const char **sqls, size_t nr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  for (size_t i=0; i<nr; ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) {
      r = -1;
      break;
    }
  }

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r;
}

static int test_case1(SQLHANDLE hconn)
{
  int r = 0;

  char buf[1024];
  r = _exec_and_check(hconn, buf, sizeof(buf), "drop table if exists t", 0, 0);
  if (r) return -1;

  r = _exec_and_check(hconn, buf, sizeof(buf), "create table t (ts timestamp, bi bigint)", 0, 0);
  if (r) return -1;

  r = _exec_and_check(hconn, buf, sizeof(buf), "insert into t (ts, bi) values ('2022-10-12 13:14:15', 34)", 0, 0);
  if (r) return -1;

  r = _exec_and_check(hconn, buf, sizeof(buf), "select bi from t where ts = '2022-10-12 13:14:15'", 1, 1, "34");
  if (r) return -1;

  return 0;
}

static int test_case2(SQLHANDLE hconn)
{
  int r = 0;

  const char *sqls[] = {
    "drop table if exists t",
    "create table t (ts timestamp, bi bigint)",
  };

  r = _prepare_dataset_conn(hconn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

#define COUNT 128

  for (size_t i=0; i<COUNT; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = _exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  size_t count;
  r = _exec_and_check_count(hconn, "select * from t where ts = '2022-10-12 13:14:15'", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  r = _exec_and_check_count(hconn, "select * from t", &count);
  if (r) return -1;
  if (count != COUNT) {
    E("%d expected, but got ==%zd==", COUNT, count);
    return -1;
  }

#undef COUNT

  return 0;
}

static int select_count_with_col_bind(SQLHANDLE hconn, const char *sql, size_t *count)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  SQLCHAR ts[128];
  int64_t bi;
  do {
    sr = CALL_SQLBindCol(hstmt, 1, SQL_C_CHAR, &ts, sizeof(ts), NULL);
    if (FAILED(sr)) break;

    sr = CALL_SQLBindCol(hstmt, 2, SQL_C_SBIGINT, &bi, 0, NULL);
    if (FAILED(sr)) break;

    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    *count = 0;
    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_NO_DATA) {
        sr = SQL_SUCCESS;
        break;
      }
      *count += 1;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case3(SQLHANDLE hconn)
{
  int r = 0;

  const char *sqls[] = {
    "drop table if exists t",
    "create table t (ts timestamp, bi bigint)",
  };

  r = _prepare_dataset_conn(hconn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

#define COUNT 3

  for (size_t i=0; i<COUNT; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = _exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  char buf[1024]; buf[0] = '\0';
  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select bi from t where ts = '2022-10-12 13:14:15'",
      1, 1,
      "34");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select bi from t",
      1, 3,
      "34",
      "35",
      "36");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select bi a, bi+3 b from t",
      2, 3,
      "34", "37",
      "35", "38",
      "36", "39");
  if (r) return -1;

  size_t count;
  r = select_count_with_col_bind(hconn, "select * from t where ts = '2022-10-12 13:14:15'", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  r = select_count_with_col_bind(hconn, "select * from t", &count);
  if (r) return -1;
  if (count != COUNT) {
    E("%d expected, but got ==%zd==", COUNT, count);
    return -1;
  }

#undef COUNT

  return 0;
}

static int select_count_with_col_bind_array(SQLHANDLE hconn, const char *sql, size_t array_size, size_t *count, size_t *batches)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

#define ARRAY_SIZE 4096
  SQLCHAR ts[ARRAY_SIZE][128];
  SQLLEN ts_ind[ARRAY_SIZE];
  int64_t bi[ARRAY_SIZE];
  SQLLEN bi_ind[ARRAY_SIZE];
  SQLUSMALLINT status[ARRAY_SIZE];
  SQLULEN nr_rows;
  do {
    if (array_size > ARRAY_SIZE) {
      E("array_size[%zd] too large [%d]", array_size, ARRAY_SIZE);
      r = -1;
      break;
    }
    sr = CALL_SQLBindCol(hstmt, 1, SQL_C_CHAR, &ts[0][0], sizeof(ts[0]), ts_ind);
    if (FAILED(sr)) break;

    sr = CALL_SQLBindCol(hstmt, 2, SQL_C_SBIGINT, &bi, 0, bi_ind);
    if (FAILED(sr)) break;

    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)array_size, 0);
    if (FAILED(sr)) break;
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, status, 0);
    if (FAILED(sr)) break;
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR, &nr_rows, 0);
    if (FAILED(sr)) break;

    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    *batches = 0;
    *count = 0;
    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_NO_DATA) {
        sr = SQL_SUCCESS;
        break;
      }
      // D("nr_rows: %d, array_size: %ld", nr_rows, array_size);
      *count += nr_rows;
      ++*batches;
    }
  } while (0);

#undef ARRAY_SIZE

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case4(SQLHANDLE hconn, int non_taos, const size_t dataset, const size_t array_size)
{
  int r = 0;

  r = _exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = _exec_direct(hconn, "create table t (ts timestamp, bi bigint)");
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

  for (size_t i=0; i<dataset; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = _exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  size_t count;
  size_t batches;
  r = select_count_with_col_bind_array(hconn, "select * from t", array_size, &count, &batches);
  if (r) return -1;
  if (count != dataset) {
    E("%zd in total expected, but got ==%zd==", dataset, count);
    return -1;
  }
  if (batches != (dataset + array_size - 1) / array_size) {
    // TODO: SQLFetch for taos-odbc is still not fully implemented yet
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetch-function?view=sql-server-ver16#positioning-the-cursor
    if (non_taos) {
      E("%zd in total, batches[%zd] expected, but got ==%zd==", count, (dataset + array_size - 1) / array_size, batches);
      return -1;
    }
  }

  return 0;
}

static int _dump_rs_to_sql_c_char(SQLHANDLE hstmt, SQLSMALLINT ColumnCount)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  for (SQLSMALLINT i=0; i<ColumnCount; ++i) {
    char buf[1024];
    SQLSMALLINT NameLength;
    SQLSMALLINT DataType;
    SQLULEN ColumnSize;
    SQLSMALLINT DecimalDigits;
    SQLSMALLINT Nullable;
    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)buf, sizeof(buf), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) break;
  }
  if (FAILED(sr)) return -1;

  while (1) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) break;
    if (sr == SQL_NO_DATA) {
      sr = SQL_SUCCESS;
      break;
    }
    for (SQLSMALLINT i=0; i<ColumnCount; ++i) {
      char buf[1024];
      SQLLEN ind;
      sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
      if (FAILED(sr)) break;
      D("Column[%d]: ==%s==", i+1, ind == SQL_NULL_DATA ? "(null)" : buf);
    }
  }

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case5_with_stmt_1(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  const char *CatalogName;
  const char *SchemaName;
  const char *TableName;
  const char *TableType;

  SQLSMALLINT ColumnCount;

  CatalogName = "%";
  SchemaName = "";
  TableName = "";
  TableType = "";

  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, SQL_NTS/*strlen(CatalogName)*/,
    (SQLCHAR*)SchemaName,  SQL_NTS/*strlen(SchemaName)*/,
    (SQLCHAR*)TableName,   SQL_NTS/*strlen(TableName)*/,
    (SQLCHAR*)TableType,   SQL_NTS/*strlen(TableType)*/);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  if (ColumnCount != 5) {
    E("5 result columns expected, but got ==%d==", ColumnCount);
    return -1;
  }

  r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
  if (r) return -1;

  CatalogName = NULL;
  SchemaName = NULL;
  TableName = NULL;
  TableType = NULL;

  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, SQL_NTS/*strlen(CatalogName)*/,
    (SQLCHAR*)SchemaName,  SQL_NTS/*strlen(SchemaName)*/,
    (SQLCHAR*)TableName,   SQL_NTS/*strlen(TableName)*/,
    (SQLCHAR*)TableType,   SQL_NTS/*strlen(TableType)*/);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  if (ColumnCount != 5) {
    E("5 result columns expected, but got ==%d==", ColumnCount);
    return -1;
  }

  r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
  if (r) return -1;

  // CatalogName = "";
  // SchemaName = "%";
  // TableName = "";
  // TableType = "";
  // sr = CALL_SQLTables(hstmt,
  //   (SQLCHAR*)CatalogName, strlen(CatalogName),
  //   (SQLCHAR*)SchemaName,  strlen(SchemaName),
  //   (SQLCHAR*)TableName,   strlen(TableName),
  //   (SQLCHAR*)TableType,   strlen(TableType));
  // if (FAILED(sr)) break;

  // r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
  // if (r) break;

  CatalogName = "";
  SchemaName = "";
  TableName = "";
  TableType = "%";
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
  if (r) return -1;

  CatalogName = "";
  SchemaName = "";
  TableName = "";
  TableType = "'TABLE'";
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
  if (r) return -1;

  struct {
    const char *sql;
  } sqls[] = {
    {"drop database if exists foo"},
    {"create database if not exists foo"},
  };

  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i].sql;
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS) return -1;
  }

  CatalogName = "foo";
  SchemaName = "";
  TableName = "bar";
  TableType = "";
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_NO_DATA) {
    E("SQL_NO_DATA expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }


  // test view
  if (_test_with_enterprise) {
    struct {
      const char *sql;
    } view_sqls[] = {
      {"drop database if exists foo"},
      {"create database if not exists foo"},
      {"create table foo.t (ts timestamp, name varchar(20), mark nchar(20))"},
      {"create view foo.view_test as select * from foo.t"},
    };

    for (size_t i=0; i<sizeof(view_sqls)/sizeof(view_sqls[0]); ++i) {
      const char *sql = view_sqls[i].sql;
      sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (sr != SQL_SUCCESS) return -1;
    }

    CatalogName = "foo";
    SchemaName = NULL;
    TableName = NULL;
    TableType = "VIEW";
    sr = CALL_SQLTables(hstmt,
      (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
      (SQLCHAR*)SchemaName,  (SQLSMALLINT)0,
      (SQLCHAR*)TableName,   (SQLSMALLINT)0,
      (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
    if (FAILED(sr)) return -1;

    sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
    if (FAILED(sr)) return -1;

    if (ColumnCount != 5) {
      E("5 result columns expected, but got ==%d==", ColumnCount);
      return -1;
    }

    sr = CALL_SQLFetch(hstmt);
    if (sr != SQL_SUCCESS) {
      E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
      return -1;
    }

    SQLUSMALLINT col_index = 3;
    char buf[1024];
    SQLLEN ind;
    sr = CALL_SQLGetData(hstmt, col_index, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (FAILED(sr)) return -1;
    D("Column[%d]: ==%s==", col_index, ind == SQL_NULL_DATA ? "(null)" : buf);
    if (strcmp(buf, "view_test")) {
      E("view test failed, expected view_test, but got ==%s==", buf);
      return -1;
    }

    sr = CALL_SQLFetch(hstmt);
    if (sr != SQL_NO_DATA) {
      E("SQL_NO_DATA expected, but got ==%s==", sql_return_type(sr));
      return -1;
    }

    const char *CatalogName;
    const char *SchemaName;
    const char *TableName;
    const char *ColumnName;

    CatalogName = "foo";
    SchemaName = "";
    TableName = "view_test";
    ColumnName = NULL;

    sr = CALL_SQLColumns(hstmt,
      (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
      (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
      (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
      (SQLCHAR*)ColumnName,   (SQLSMALLINT)0);
    if (sr != SQL_SUCCESS) {
      E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
      return -1;
    }

  }

  return 0;
}

static int test_case5_with_stmt_2(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[4096];
  SQLLEN StrLen_or_Ind;

  const char *CatalogName;
  const char *SchemaName;
  const char *TableName;
  const char *TableType;
  const char *ColumnName;

  struct {
    const char *sql;
  } sqls[] = {
    {"drop database if exists foo"},
    {"create database if not exists foo"},
    {"use foo"},
    {"create table bar(ts timestamp, name varchar(20))"},
  };

  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i].sql;
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS) return -1;
  }

  CatalogName = "foo";
  SchemaName = "";
  TableName = "bar";
  TableType = "";
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_SUCCESS) {
    E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }

  CatalogName = "foo";
  SchemaName = "";
  TableName = "%b_r";
  TableType = "";
  CALL_SQLCloseCursor(hstmt);
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_SUCCESS) {
    E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }

  CatalogName = "f_o%";
  SchemaName = "";
  TableName = "%b_r";
  TableType = "";
  CALL_SQLCloseCursor(hstmt);
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_SUCCESS) {
    E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }

  CatalogName = "f_o%";
  SchemaName = "";
  TableName = "%b_r";
  TableType = "";
  CALL_SQLCloseCursor(hstmt);
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_SUCCESS) {
    E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }

  CALL_SQLCloseCursor(hstmt);

  sr = CALL_SQLBindCol(hstmt, 3, SQL_C_CHAR, buf, sizeof(buf), &StrLen_or_Ind);

  CatalogName = "foo";
  SchemaName = "";
  TableName = "bar";
  TableType = "";
  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_SUCCESS) {
    E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }

  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("SQL_NULL_DATA not expected");
    return -1;
  }

  if (StrLen_or_Ind < 0) {
    E("StrLen_or_Ind is negative");
    return -1;
  }

  if (strcmp(buf, "bar")) {
    E("`bar` expected, but got ==%s==", buf);
    return -1;
  }

  CALL_SQLCloseCursor(hstmt);
  CALL_SQLFreeStmt(hstmt, SQL_UNBIND);

  sr = CALL_SQLBindCol(hstmt, 4, SQL_C_CHAR, buf, sizeof(buf), &StrLen_or_Ind);

  CatalogName = "foo";
  SchemaName = "";
  TableName = "bar";
  ColumnName = "ts";
  sr = CALL_SQLColumns(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)ColumnName,   (SQLSMALLINT)strlen(ColumnName));
  if (FAILED(sr)) return -1;

  sr = CALL_SQLFetch(hstmt);
  if (sr != SQL_SUCCESS) {
    E("SQL_SUCCESS expected, but got ==%s==", sql_return_type(sr));
    return -1;
  }

  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("SQL_NULL_DATA not expected");
    return -1;
  }

  if (StrLen_or_Ind < 0) {
    E("StrLen_or_Ind is negative");
    return -1;
  }

  if (strcmp(buf, "ts")) {
    E("`ts` expected, but got ==%s==", buf);
    return -1;
  }

  return 0;
}

static int test_case5(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  if (r == 0) r = test_case5_with_stmt_1(hstmt);
  if (r == 0) r = test_case5_with_stmt_2(hstmt);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r ? -1 : 0;
}

static int test_case6(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = _exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = _exec_direct(hconn, "create table t (ts timestamp, name varchar(1024))");
  if (r) return -1;

  const char *name = "helloworldfoobargreatwall";
  char sql[1024];
  snprintf(sql, sizeof(sql), "insert into t (ts, name) values ('2022-10-12 13:14:15', '%s')", name);

  r = _exec_direct(hconn, sql);
  if (r) return -1;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)"select name from t", SQL_NTS);
    if (FAILED(sr)) break;

    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) break;
    if (sr == SQL_NO_DATA) {
      sr = SQL_SUCCESS;
      break;
    }

    char buf[2048];
    buf[0] = '\0';
    char *p = buf;
    size_t count = 0;
    (void)count;
    // TODO: remove this restriction
    SQLLEN BufferLength = 4;
    SQLLEN StrLen_or_Ind;

    while (1) {
      StrLen_or_Ind = 0;
      sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, p, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_SUCCESS_WITH_INFO || sr == SQL_SUCCESS) {
        size_t n;
        if (StrLen_or_Ind == SQL_NO_TOTAL) {
          n = strnlen(p, BufferLength);
        } else {
          n = strlen(p);
        }
        if (n == (size_t)BufferLength) {
          E("internal logic error");
          r = -1;
          break;
        }
        p += n;
        count += n;
        continue;
      }
      if (sr == SQL_NO_DATA) {
        if (strncmp(buf, name, strlen(name) + 1)) {
          E("internal logic error: %.*s <> %s", (int)sizeof(buf), buf, name);
          r = -1;
        }
        sr = SQL_SUCCESS;
        break;
      }
      E("internal logic error");
      r = -1;
      break;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case7(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = _exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = _exec_direct(hconn, "create table t (ts timestamp, name varchar(1024))");
  if (r) return -1;

  const char *ts   = "2022-10-12 13:14:15";
  const char *name = "foo";
  char sql[1024];
  snprintf(sql, sizeof(sql), "insert into t (ts, name) values ('%s', '%s')", ts, name);

  r = _exec_direct(hconn, sql);
  if (r) return -1;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)"select ts from t", SQL_NTS);
    if (FAILED(sr)) break;

    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) break;
    if (sr == SQL_NO_DATA) {
      sr = SQL_SUCCESS;
      break;
    }

    char buf[2048];
    buf[0] = '\0';
    char *p = buf;
    size_t count = 0;
    (void)count;
    // TODO: remove this restriction
    SQLLEN BufferLength = 4;
    SQLLEN StrLen_or_Ind;

    while (1) {
      StrLen_or_Ind = 0;
      sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, p, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_SUCCESS_WITH_INFO || sr == SQL_SUCCESS) {
        size_t n;
        if (StrLen_or_Ind == SQL_NO_TOTAL) {
          n = strnlen(p, BufferLength);
        } else {
          n = strlen(p);
        }
        if (n == (size_t)BufferLength) {
          E("internal logic error");
          r = -1;
          break;
        }
        p += n;
        count += n;
        continue;
      }
      if (sr == SQL_NO_DATA) {
        if (strncmp(buf, ts, strlen(ts))) {
          E("internal logic error: %.*s <> %s", (int)sizeof(buf), buf, ts);
          r = -1;
        }
        sr = SQL_SUCCESS;
        break;
      }
      E("internal logic error");
      r = -1;
      break;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case8(SQLHANDLE hconn)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024]; buf[0] = '\0';
  SQLINTEGER len;
  sr = CALL_SQLGetConnectAttr(hconn, SQL_CURRENT_QUALIFIER, (SQLPOINTER)buf, sizeof(buf), &len);
  if (sr == SQL_ERROR) return -1;
  if (sr == SQL_SUCCESS) {
    D("SQL_CURRENT_QUALIFIER:%.*s", len, buf);
    return 0;
  }
  E("sr:%s", sql_return_type(sr));
  return -1;
}

static int test_case9_with_stmt(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (1) {
    const char *sqls[] = {
      "drop database if exists foo",
      "create database foo",
      "use foo",
      "create table bar (ts timestamp, name varchar(20))",
    };

    for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
      const char *sql = sqls[i];
      sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
      if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
    }

    const char *CatalogName = NULL;
    const char *SchemaName = NULL;
    const char *TableName = NULL;
    const char *TableType = "TABLE,SYSTEM TABLE";

    SQLSMALLINT ColumnCount = 0;

    sr = CALL_SQLTables(hstmt,
      (SQLCHAR*)CatalogName, SQL_NTS/*strlen(CatalogName)*/,
      (SQLCHAR*)SchemaName,  SQL_NTS/*strlen(SchemaName)*/,
      (SQLCHAR*)TableName,   SQL_NTS/*strlen(TableName)*/,
      (SQLCHAR*)TableType,   SQL_NTS/*strlen(TableType)*/);
    if (FAILED(sr)) return -1;

    sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
    if (FAILED(sr)) return -1;

    if (ColumnCount != 5) {
      E("5 result columns expected, but got ==%d==", ColumnCount);
      return -1;
    }

    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) return -1;
      if (sr == SQL_NO_DATA) break;
      char catalog[1024], name[1024];
      SQLLEN ind;
      sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, (SQLPOINTER)catalog, sizeof(catalog), &ind);
      if (FAILED(sr)) return -1;
      sr = CALL_SQLGetData(hstmt, 3, SQL_C_CHAR, (SQLPOINTER)name, sizeof(name), &ind);
      if (FAILED(sr)) return -1;
      D("==%s.%s==", catalog, name);
    }

    CALL_SQLCloseCursor(hstmt);

    CatalogName = (const char*)"foo";
    SchemaName = (const char*)"";
    TableName = (const char*)"bar";
    const char *ColumnName = NULL;

    sr = CALL_SQLColumns(hstmt,
      (SQLCHAR*)CatalogName, SQL_NTS/*strlen(CatalogName)*/,
      (SQLCHAR*)SchemaName,  SQL_NTS/*strlen(SchemaName)*/,
      (SQLCHAR*)TableName,   SQL_NTS/*strlen(TableName)*/,
      (SQLCHAR*)ColumnName,  SQL_NTS/*strlen(ColumnName)*/);
    if (FAILED(sr)) return -1;

    sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
    if (FAILED(sr)) return -1;

    if (ColumnCount != 18) {
      E("5 result columns expected, but got ==%d==", ColumnCount);
      return -1;
    }

    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) return -1;
      if (sr == SQL_NO_DATA) break;
      for (SQLSMALLINT i=0; i<ColumnCount; ++i) {
        W("col %d...", i+1);
        char buf[4096];
        SQLLEN ind;
        sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
        if (FAILED(sr)) return -1;
        if (ind == SQL_NULL_DATA) {
          W("==col[%d]:null==", i+1);
        } else {
          W("==col[%d]:%s==", i+1, buf);
        }
      }
    }

    D("ok");
  }

  const char *sqls[] = {
    "drop database if exists foo",
    "create database foo",
    "use foo",
    "create table bar(ts timestamp, name varchar(20))",
  };

  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
  }

  const char *sql;

  char buf[4096];
  SQLSMALLINT len;
  SQLLEN attr;

  SQLUSMALLINT    ColumnNumber                = 1;
  SQLUSMALLINT    FieldIdentifier             = SQL_DESC_OCTET_LENGTH;
  SQLPOINTER      CharacterAttributePtr       = buf;
  SQLSMALLINT     BufferLength                = sizeof(buf);
  SQLSMALLINT    *StringLengthPtr             = &len;
  SQLLEN         *NumericAttributePtr         = &attr;

  sql = "select name from bar";
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  ColumnNumber                = 1;
  FieldIdentifier             = SQL_DESC_OCTET_LENGTH;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  StringLengthPtr             = &len;
  NumericAttributePtr         = &attr;

  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
  if (sr != SQL_SUCCESS) return -1;

  if (attr != 20) {
    D("expected `20`, but got ==%zd==", (size_t)attr);
    return -1;
  }

  CALL_SQLCloseCursor(hstmt);

  sql = "select ts from bar";
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  ColumnNumber                = 1;
  FieldIdentifier             = SQL_DESC_OCTET_LENGTH;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  StringLengthPtr             = &len;
  NumericAttributePtr         = &attr;

  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
  if (sr != SQL_SUCCESS) return -1;

  if (attr != 8 && attr != 46) {
    E("expected `8` or `46`, but got ==%zd==", (size_t)attr);
    return -1;
  }

  return 0;
}

static int test_case9(SQLHANDLE hconn)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  SQLHANDLE hstmt;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  r = test_case9_with_stmt(hstmt);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r ? -1 : 0;
}

static int _vexec_(SQLHANDLE hstmt, const char *fmt, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
  if (sr == SQL_ERROR) return -1;

  char buf[1024];
  char *sql = buf;

  va_list aq;
  va_copy(aq, ap);
  int n = vsnprintf(buf, sizeof(buf), fmt, aq);
  if (n < 0) {
    int e = errno;
    E("vsnprintf failed:[%d]%s", e, strerror(e));
    return -1;
  }
  if ((size_t)n >= sizeof(buf)) {
    sql = (char*)malloc(n + 1);
    if (!sql) {
      W("out of memory");
      return -1;
    }
    int m = vsnprintf(sql, n+1, fmt, ap);
    A(m == n, "");
  }
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sql != buf) free(sql);
  va_end(aq);

  if (sr == SQL_ERROR) return -1;
  return 0;
}

static int _exec_impl(SQLHANDLE hstmt, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int r = _vexec_(hstmt, fmt, ap);
  va_end(ap);

  return r ? -1 : 0;
}


#define _exec_(hstmt, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : _exec_impl(hstmt, fmt, ##__VA_ARGS__))

typedef struct simple_str_s                 simple_str_t;
struct simple_str_s {
  size_t                       cap;
  size_t                       nr;
  char                        *base;
};

static int _simple_str_fmt_impl(simple_str_t *str, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  char *base = str->base ? str->base + str->nr : NULL;
  size_t sz = str->cap - str->nr;
  int n = vsnprintf(base, sz, fmt, ap);
  va_end(ap);

  if (n < 0) {
    int e = errno;
    E("vsnprintf failed:[%d]%s", e, strerror(e));
    return -1;
  }

  if (str->base) {
    if ((size_t)n >= sz) {
      W("buffer too small");
      return -1;
    }
    str->nr += n;
  }

  return 0;
}

#define _simple_str_fmt(str, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : _simple_str_fmt_impl(str, fmt, ##__VA_ARGS__))

typedef struct field_s            field_t;
struct field_s {
  const char       *name;
  const char       *field;
};

static int _gen_table_create_sql(simple_str_t *str, const char *table, const field_t *fields, size_t nr_fields)
{
  int r = 0;
  r = _simple_str_fmt(str, "create table %s", table);
  if (r) return -1;

  for (size_t i=0; i<nr_fields; ++i) {
    const field_t *field = fields + i;
    if (i == 0) {
      r = _simple_str_fmt(str, " (");
    } else {
      r = _simple_str_fmt(str, ",");
    }
    if (r) return -1;
    r = _simple_str_fmt(str, "%s %s", field->name, field->field);
  }
  if (nr_fields > 0) {
    r = _simple_str_fmt(str, ")");
    if (r) return -1;
  }

  return 0;
}

static int _gen_table_param_insert(simple_str_t *str, const char *table, const field_t *fields, size_t nr_fields)
{
  int r = 0;
  r = _simple_str_fmt(str, "insert into %s", table);
  if (r) return -1;

  for (size_t i=0; i<nr_fields; ++i) {
    const field_t *field = fields + i;
    if (i == 0) {
      r = _simple_str_fmt(str, " (");
    } else {
      r = _simple_str_fmt(str, ",");
    }
    if (r) return -1;
    r = _simple_str_fmt(str, "%s", field->name);

    if (i+1 < nr_fields) continue;
    r = _simple_str_fmt(str, ")");
    if (r) return -1;
  }

  for (size_t i=0; i<nr_fields; ++i) {
    if (i == 0) {
      r = _simple_str_fmt(str, " values (");
    } else {
      r = _simple_str_fmt(str, ",");
    }
    if (r) return -1;
    r = _simple_str_fmt(str, "?");

    if (i+1 < nr_fields) continue;
    r = _simple_str_fmt(str, ")");
    if (r) return -1;
  }

  return 0;
}

typedef struct param_s                   param_t;
struct param_s {
  SQLSMALLINT     InputOutputType;
  SQLSMALLINT     ValueType;
  SQLSMALLINT     ParameterType;
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLPOINTER      ParameterValuePtr;
  SQLLEN          BufferLength;
  SQLLEN         *StrLen_or_IndPtr;
};

static int test_bind_params_with_stmt(SQLHANDLE hconn, SQLHANDLE hstmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  char buf[1024];
  simple_str_t str = {
    .base           = buf,
    .cap            = sizeof(buf),
    .nr             = 0,
  };

  const char *database = "foo";

  if (_under_taos_mysql_sqlite3 != 2) {
    r = _exec_(hstmt, "drop database if exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "create database if not exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "use %s", database);
    if (r) return -1;
  }

  const char *table = "t";
  field_t fields[] = {
    {"ts", "timestamp"},
    {"vname", "varchar(10)"},
    {"wname", "nchar(10)"},
    {"bi", "bigint"},
  };
  if (_under_taos_mysql_sqlite3) {
    fields[0].field = "bigint";
  }

  r = _exec_(hstmt, "drop table if exists %s", table);
  if (r) return -1;

  r = _gen_table_create_sql(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

#define ARRAY_SIZE 10

  SQLUSMALLINT param_status_arr[ARRAY_SIZE] = {0};
  SQLULEN nr_params_processed = 0;
  int64_t ts_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  ts_ind[ARRAY_SIZE] = {0};
  char    varchar_arr[ARRAY_SIZE][100];
  SQLLEN  varchar_ind[ARRAY_SIZE] = {0};
  char    nchar_arr[ARRAY_SIZE][100];
  SQLLEN  nchar_ind[ARRAY_SIZE] = {0};
  int64_t i64_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  i64_ind[ARRAY_SIZE] = {0};

  const param_t params[] = {
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_TYPE_TIMESTAMP,  23,       3,          ts_arr,           0,   ts_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
  };

  SQLULEN nr_paramset_size = 4;

  for (int i=0; i<ARRAY_SIZE; ++i) {
    ts_arr[i] = 1662861448751 + i;
    snprintf(varchar_arr[i], 100, "a人%d", i);
    varchar_ind[i] = SQL_NTS;
    snprintf(nchar_arr[i], 100, "b民%d", i);
    nchar_ind[i] = SQL_NTS;
    i64_arr[i] = 54321 + i;
  }

  str.nr = 0;
  r = _gen_table_param_insert(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_paramset_size, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_status_arr, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &nr_params_processed, 0);

  // Bind the parameters in column-wise fashion.
  for (size_t i=0; i<sizeof(params)/sizeof(params[0]); ++i) {
    const param_t *param = params + i;
    sr = CALL_SQLBindParameter(hstmt, (SQLUSMALLINT)i+1,
        param->InputOutputType,
        param->ValueType,
        param->ParameterType,
        param->ColumnSize,
        param->DecimalDigits,
        param->ParameterValuePtr,
        param->BufferLength,
        param->StrLen_or_IndPtr);
    if (sr == SQL_ERROR) return -1;
  }


  // Set part ID, description, and price.

  // Execute the statement.
  sr = CALL_SQLExecute(hstmt);
  if (sr == SQL_ERROR) return -1;

  if (nr_params_processed != nr_paramset_size) {
    W("%zu rows of params to be processed, but only %zu", nr_paramset_size, nr_params_processed);
    return -1;
  }

  // Check to see which sets of parameters were processed successfully.
  for (size_t i = 0; i < nr_params_processed; i++) {
    switch (param_status_arr[i]) {
      case SQL_PARAM_SUCCESS:
        X("param row #%13zd  Success", i + 1);
        break;
      case SQL_PARAM_SUCCESS_WITH_INFO:
        X("param row #%13zd  Success with info", i + 1);
        break;

      case SQL_PARAM_ERROR:
        E("param row #%13zd  Error", i + 1);
        r = -1;
        break;

      case SQL_PARAM_UNUSED:
        E("param row #%13zd  Not processed", i + 1);
        r = -1;
        break;

      case SQL_PARAM_DIAG_UNAVAILABLE:
        E("param row #%13zd  Diag unavailable", i + 1);
        r = -1;
        break;

      default:
        E("internal logic error");
        r = -1;
        break;
    }
  }

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select wname from t",
      1, 4,
      "b民0", "b民1", "b民2", "b民3");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select vname from t",
      1, 4,
      "a人0", "a人1", "a人2", "a人3");
  if (r) return -1;

  return r ? -1 : 0;
}

static int test_bind_params_with_stmt_status(SQLHANDLE hconn, SQLHANDLE hstmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  char buf[1024];
  simple_str_t str = {
    .base           = buf,
    .cap            = sizeof(buf),
    .nr             = 0,
  };

  const char *database = "foo";

  if (_under_taos_mysql_sqlite3 != 2) {
    r = _exec_(hstmt, "drop database if exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "create database if not exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "use %s", database);
    if (r) return -1;
  } else {
    W("not fully test under sqlite3");
    return 0;
  }

  const char *table = "t";
  field_t fields[] = {
    {"ts", "timestamp"},
    {"vname", "varchar(5)"},
    {"wname", "nchar(10)"},
    {"bi", "bigint"},
  };
  if (_under_taos_mysql_sqlite3) {
    fields[0].field = "bigint";
  }

  r = _exec_(hstmt, "drop table if exists %s", table);
  if (r) return -1;

  r = _gen_table_create_sql(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

#define ARRAY_SIZE 10

  SQLUSMALLINT param_status_arr[ARRAY_SIZE] = {0};
  SQLULEN nr_params_processed = 0;
  int64_t ts_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  ts_ind[ARRAY_SIZE] = {0};
  char    varchar_arr[ARRAY_SIZE][100];
  SQLLEN  varchar_ind[ARRAY_SIZE] = {0};
  char    nchar_arr[ARRAY_SIZE][100];
  SQLLEN  nchar_ind[ARRAY_SIZE] = {0};
  int64_t i64_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  i64_ind[ARRAY_SIZE] = {0};

  const param_t params[] = {
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_TYPE_TIMESTAMP,  23,       3,          ts_arr,           0,   ts_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
  };

  SQLULEN nr_paramset_size = 4;

  for (int i=0; i<ARRAY_SIZE; ++i) {
    ts_arr[i] = 1662861448751 + i;
    snprintf(varchar_arr[i], 100, "abcd%d", i+9);
    varchar_ind[i] = SQL_NTS;
    snprintf(nchar_arr[i], 100, "b民%d", i);
    nchar_ind[i] = SQL_NTS;
    i64_arr[i] = 54321 + i;
  }

  str.nr = 0;
  r = _gen_table_param_insert(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_paramset_size, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_status_arr, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &nr_params_processed, 0);

  // Bind the parameters in column-wise fashion.
  for (size_t i=0; i<sizeof(params)/sizeof(params[0]); ++i) {
    const param_t *param = params + i;
    sr = CALL_SQLBindParameter(hstmt, (SQLUSMALLINT)i+1,
        param->InputOutputType,
        param->ValueType,
        param->ParameterType,
        param->ColumnSize,
        param->DecimalDigits,
        param->ParameterValuePtr,
        param->BufferLength,
        param->StrLen_or_IndPtr);
    if (sr == SQL_ERROR) return -1;
  }


  // Set part ID, description, and price.

  // Execute the statement.
  sr = CALL_SQLExecute(hstmt);
  if (sr == SQL_ERROR) return -1;

  if (nr_params_processed != nr_paramset_size) {
    W("%zu rows of params to be processed, but only %zu", nr_paramset_size, nr_params_processed);
  }

  // Check to see which sets of parameters were processed successfully.
  for (size_t i = 0; i < nr_params_processed; i++) {
    switch (param_status_arr[i]) {
      case SQL_PARAM_SUCCESS:
        X("param row #%13zd  Success", i + 1);
        break;
      case SQL_PARAM_SUCCESS_WITH_INFO:
        X("param row #%13zd  Success with info", i + 1);
        break;

      case SQL_PARAM_ERROR:
        E("param row #%13zd  Error", i + 1);
        r = -1;
        break;

      case SQL_PARAM_UNUSED:
        E("param row #%13zd  Not processed", i + 1);
        r = -1;
        break;

      case SQL_PARAM_DIAG_UNAVAILABLE:
        E("param row #%13zd  Diag unavailable", i + 1);
        r = -1;
        break;

      default:
        E("internal logic error");
        r = -1;
        break;
    }
  }

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select wname from t",
      1, 1,
      "b民0");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select vname from t",
      1, 1,
      "abcd9");
  if (r) return -1;

  return r ? -1 : 0;
}

static int test_bind_params(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    r = test_bind_params_with_stmt(hconn, hstmt);
    if (r) break;

    r = test_bind_params_with_stmt_status(hconn, hstmt);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

typedef struct prepare_checker_s              prepare_checker_t;
struct prepare_checker_s {
  const char                     *sql;
  SQLSMALLINT                     paramCount;
};

static int test_prepare_with_stmt(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  const char *sqls[] = {
    "show databases",
    "drop database if exists foo",
    "create database if not exists foo",
    "use foo",
    "create table t (ts timestamp, v int)",
    "create stable s (ts timestamp, v int) tags (id int)",
  };

  for (size_t i=0; i<sizeof(sqls) / sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }

  prepare_checker_t checkers[] = {
    {"insert into ? using s tags (?) values (?, ?)", 4},
    {"insert into suzhou using s tags (?) values (?, ?)", 3},
    // {"insert into suzhou using s tags (3) values (now(), ?)", 3},
  };

  for (size_t i=0; i<sizeof(checkers)/sizeof(checkers[0]); ++i) {
    prepare_checker_t *checker = checkers + i;
    SQLSMALLINT paramCount = 0;

    sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)checker->sql, SQL_NTS);
    if (sr == SQL_ERROR) return SQL_ERROR;
    sr = CALL_SQLNumParams(hstmt, &paramCount);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (paramCount != checker->paramCount) {
      E("`%s`:expected %d params, but got ==%d==", checker->sql, checker->paramCount, paramCount);
      return -1;
    }
  }

  return 0;
}

static int test_prepare(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    r = test_prepare_with_stmt(hstmt);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_bind_exec_direct_with_stmt(SQLHANDLE hconn, SQLHANDLE hstmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  char buf[1024];
  simple_str_t str = {
    .base           = buf,
    .cap            = sizeof(buf),
    .nr             = 0,
  };

  const char *database = "foo";

  if (_under_taos_mysql_sqlite3 != 2) {
    r = _exec_(hstmt, "drop database if exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "create database if not exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "use %s", database);
    if (r) return -1;
  }

  const char *table = "t";
  field_t fields[] = {
    {"ts", "timestamp"},
    {"vname", "varchar(10)"},
    {"wname", "nchar(10)"},
    {"bi", "bigint"},
  };
  if (_under_taos_mysql_sqlite3) {
    fields[0].field = "bigint";
  }

  r = _exec_(hstmt, "drop table if exists %s", table);
  if (r) return -1;

  r = _gen_table_create_sql(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

#define ARRAY_SIZE 10

  SQLUSMALLINT param_status_arr[ARRAY_SIZE] = {0};
  SQLULEN nr_params_processed = 0;
  int64_t ts_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  ts_ind[ARRAY_SIZE] = {0};
  char    varchar_arr[ARRAY_SIZE][100];
  SQLLEN  varchar_ind[ARRAY_SIZE] = {0};
  char    nchar_arr[ARRAY_SIZE][100];
  SQLLEN  nchar_ind[ARRAY_SIZE] = {0};
  int64_t i64_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  i64_ind[ARRAY_SIZE] = {0};

  const param_t params[] = {
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_TYPE_TIMESTAMP,  23,       3,          ts_arr,           0,   ts_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         20,       0,          varchar_arr,      100, varchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        20,       0,          nchar_arr,        100, nchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          8,        0,          i64_arr,          0,   i64_ind},
  };

  SQLULEN nr_paramset_size = 4;

  for (int i=0; i<ARRAY_SIZE; ++i) {
    ts_arr[i] = 1662861448751 + i;
    snprintf(varchar_arr[i], 100, "a人%d", i);
    varchar_ind[i] = SQL_NTS;
    snprintf(nchar_arr[i], 100, "b民%d", i);
    nchar_ind[i] = SQL_NTS;
    i64_arr[i] = 54321 + i;
  }

  str.nr = 0;
  r = _gen_table_param_insert(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;

  sr = CALL_SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
  if (sr == SQL_ERROR) return -1;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_paramset_size, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_status_arr, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &nr_params_processed, 0);

  // Bind the parameters in column-wise fashion.
  for (size_t i=0; i<sizeof(params)/sizeof(params[0]); ++i) {
    const param_t *param = params + i;
    sr = CALL_SQLBindParameter(hstmt, (SQLUSMALLINT)i+1,
        param->InputOutputType,
        param->ValueType,
        param->ParameterType,
        param->ColumnSize,
        param->DecimalDigits,
        param->ParameterValuePtr,
        param->BufferLength,
        param->StrLen_or_IndPtr);
    if (sr == SQL_ERROR) return -1;
  }


  // Set part ID, description, and price.

  // Execute the statement.
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

  if (nr_params_processed != nr_paramset_size) {
    W("%zu rows of params to be processed, but only %zu", nr_paramset_size, nr_params_processed);
    return -1;
  }

  // Check to see which sets of parameters were processed successfully.
  for (size_t i = 0; i < nr_params_processed; i++) {
    switch (param_status_arr[i]) {
      case SQL_PARAM_SUCCESS:
        X("param row #%13zd  Success", i + 1);
        break;
      case SQL_PARAM_SUCCESS_WITH_INFO:
        X("param row #%13zd  Success with info", i + 1);
        break;

      case SQL_PARAM_ERROR:
        E("param row #%13zd  Error", i + 1);
        r = -1;
        break;

      case SQL_PARAM_UNUSED:
        E("param row #%13zd  Not processed", i + 1);
        r = -1;
        break;

      case SQL_PARAM_DIAG_UNAVAILABLE:
        E("param row #%13zd  Diag unavailable", i + 1);
        r = -1;
        break;

      default:
        E("internal logic error");
        r = -1;
        break;
    }
  }

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select wname from t",
      1, 4,
      "b民0", "b民1", "b民2", "b民3");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select vname from t",
      1, 4,
      "a人0", "a人1", "a人2", "a人3");
  if (r) return -1;

  return r ? -1 : 0;
}

static int test_bind_exec_direct(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    r = test_bind_exec_direct_with_stmt(hconn, hstmt);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_cases(SQLHANDLE hconn, conn_arg_t *conn_arg)
{
  int r = 0;

  _exec_direct(hconn, "create database if not exists foo");
  _exec_direct(hconn, "use foo");

  if (!conn_arg->ws) {
    r = test_bind_exec_direct(hconn);
    if (r) return r;

    if (r) r = test_prepare(hconn);
    if (r) return r;

    if (r) r = test_bind_params(hconn);
    if (r) return r;
  }

  r = test_case1(hconn);
  if (r) return r;

  r = test_case2(hconn);
  if (r) return r;

  r = test_case3(hconn);
  if (r) return r;

  int non_taos = 0;

  r = test_case4(hconn, non_taos, 128, 113);
  if (r) return r;

  if (0 && !non_taos) {
    r = test_case4(hconn, non_taos, 5000, 4000);
    if (r) return r;
  }

  r = test_case5(hconn);
  if (r) return r;

  r = test_case6(hconn);
  if (r) return r;

  r = test_case7(hconn);
  if (r) return r;

  r = test_case8(hconn);
  if (r) return r;

  r = test_case9(hconn);
  if (r) return r;

  return r;
}

static void check_driver(SQLHANDLE hconn)
{
  char buf[1024]; buf[0] = '\0';
  SQLSMALLINT StringLength = 0;
  SQLRETURN sr = CALL_SQLGetInfo(hconn, SQL_DRIVER_NAME, (SQLCHAR*)buf, sizeof(buf), &StringLength);
  if (FAILED(sr)) {
    W("fall back to taos-odbc");
    _under_taos_mysql_sqlite3 = 0;
    return;
  }
  D("driver_name:%s", buf);
  if (strstr(buf, "taos")) {
    _under_taos_mysql_sqlite3 = 0;
    return;
  }
  if (strstr(buf, "sqlite3")) {
    _under_taos_mysql_sqlite3 = 2;
    return;
  }
  if (strstr(buf, "my")) {
    _under_taos_mysql_sqlite3 = 1;
    return;
  }
  W("fall back to taos-odbc");
  _under_taos_mysql_sqlite3 = 0;
}

static int test_connected_conn(SQLHANDLE hconn, conn_arg_t *conn_arg)
{
  int r = 0;

  check_driver(hconn);

  if (_under_taos_mysql_sqlite3 == 0) {
    r = _exec_and_check(hconn, (char*)-1, 0, "show apps", 0, 0);
    if (r) return -1;
  }

  if (conn_arg->odbc_type & TAOS_ODBC) {
    r = test_cases(hconn, conn_arg);
    if (r) return -1;
  }

  return 0;
}

static int test_SQLColumns(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *sqls[] = {
    "drop database if exists foo",
    "create database foo",
    "use foo",
    "create table bar (ts timestamp, name varchar(20), f float)",
  };

  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    CALL_SQLCloseCursor(hstmt);
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
  }

  const char *CatalogName;
  const char *SchemaName;
  const char *TableName;
  const char *ColumnName;
  SQLSMALLINT ColumnCount;

  CatalogName = "foo";
  SchemaName = "";
  TableName = "bar";
  ColumnName = "ts";

  CALL_SQLCloseCursor(hstmt);
  sr = CALL_SQLColumns(hstmt,
    (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
    (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
    (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
    (SQLCHAR*)ColumnName,   (SQLSMALLINT)strlen(ColumnName));
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

  if (ColumnCount < 18) {
    E("at least 18 result columns expected, but got ==%d==", ColumnCount);
    return -1;
  }

  char buf[4096];

  SQLUSMALLINT    ColumnNumber;
  SQLUSMALLINT    FieldIdentifier;
  SQLPOINTER      CharacterAttributePtr = buf;
  SQLSMALLINT     BufferLength;
  SQLSMALLINT     StringLength, *StringLengthPtr;
  SQLLEN          NumericAttribute, *NumericAttributePtr;

#define DUMP(fmt, ...) printf(fmt "\n", ##__VA_ARGS__)           /* { */
  for (SQLSMALLINT i=0; 0 && i<ColumnCount; ++i) {
    ColumnNumber                = i + 1;

    FieldIdentifier             = SQL_DESC_CONCISE_TYPE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_CONCISE_TYPE`,[%d]%s", i+1, (int)NumericAttribute, sql_data_type((SQLSMALLINT)NumericAttribute));

    FieldIdentifier             = SQL_DESC_OCTET_LENGTH;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_OCTET_LENGTH`,[%d]", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_PRECISION;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_PRECISION`,[%d]", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_SCALE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_SCALE`,[%d]", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_AUTO_UNIQUE_VALUE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_AUTO_UNIQUE_VALUE`,[%s]", i+1, NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");

    FieldIdentifier             = SQL_DESC_UPDATABLE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_UPDATABLE`,[%s]", i+1, sql_updatable(NumericAttribute));

    FieldIdentifier             = SQL_DESC_NULLABLE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_NULLABLE`,[%s]", i+1, sql_nullable((SQLSMALLINT)NumericAttribute));

    FieldIdentifier             = SQL_DESC_NAME;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_NAME`,%d,[%.*s]", i+1, (int)StringLength, (int)StringLength, buf);

    FieldIdentifier             = SQL_COLUMN_TYPE_NAME;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_COLUMN_TYPE_NAME`,%d,[%.*s]", i+1, (int)StringLength, (int)StringLength, buf);

    FieldIdentifier             = SQL_DESC_LENGTH;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_LENGTH`,%d", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_NUM_PREC_RADIX;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_NUM_PREC_RADIX`,%d", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_UNSIGNED;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d:`SQL_DESC_UNSIGNED`,[%s]", i+1, NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");
  }

  while (1) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) return -1;
    if (sr == SQL_NO_DATA) break;

    for (SQLSMALLINT i=0; i<ColumnCount; ++i) {
      DUMP("col %d...", i+1);
      char buf[4096];
      SQLLEN ind;
      sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
      if (FAILED(sr)) return -1;
      if (ind == SQL_NULL_DATA) {
        DUMP("==col[%d]:null==", i+1);
      } else {
        DUMP("==col[%d]:%s==", i+1, buf);
      }
    }
    break;
  }
#undef DUMP                                                      /* } */

  return 0;
}

static int test_SQLGetTypeInfo(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT DataType;
  SQLSMALLINT ColumnCount;

  CALL_SQLCloseCursor(hstmt);
  DataType = SQL_ALL_TYPES;
  sr = CALL_SQLGetTypeInfo(hstmt, DataType);
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

  if (ColumnCount < 19) {
    E("at least 19 result columns expected, but got ==%d==", ColumnCount);
    return -1;
  }

  char buf[4096];

  SQLUSMALLINT    ColumnNumber;
  SQLUSMALLINT    FieldIdentifier;
  SQLPOINTER      CharacterAttributePtr = buf;
  SQLSMALLINT     BufferLength;
  SQLSMALLINT     StringLength, *StringLengthPtr;
  SQLLEN          NumericAttribute, *NumericAttributePtr;

  for (SQLSMALLINT i=0; 0 && i<ColumnCount; ++i) {
    ColumnNumber                = i + 1;

    FieldIdentifier             = SQL_DESC_CONCISE_TYPE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_CONCISE_TYPE`,[%d]%s", i+1, (int)NumericAttribute, sql_data_type((SQLSMALLINT)NumericAttribute));

    FieldIdentifier             = SQL_DESC_OCTET_LENGTH;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_OCTET_LENGTH`,[%d]", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_PRECISION;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_PRECISION`,[%d]", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_SCALE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_SCALE`,[%d]", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_AUTO_UNIQUE_VALUE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_AUTO_UNIQUE_VALUE`,[%s]", i+1, NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");

    FieldIdentifier             = SQL_DESC_UPDATABLE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_UPDATABLE`,[%s]", i+1, sql_updatable(NumericAttribute));

    FieldIdentifier             = SQL_DESC_NULLABLE;
    CharacterAttributePtr       = NULL;
    BufferLength                = 0;
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_NULLABLE`,[%s]", i+1, sql_nullable((SQLSMALLINT)NumericAttribute));

    FieldIdentifier             = SQL_DESC_NAME;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_NAME`,%d,[%.*s]", i+1, (int)StringLength, (int)StringLength, buf);

    FieldIdentifier             = SQL_COLUMN_TYPE_NAME;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_COLUMN_TYPE_NAME`,%d,[%.*s]", i+1, (int)StringLength, (int)StringLength, buf);

    FieldIdentifier             = SQL_DESC_LENGTH;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_LENGTH`,%d", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_NUM_PREC_RADIX;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_NUM_PREC_RADIX`,%d", i+1, (int)NumericAttribute);

    FieldIdentifier             = SQL_DESC_UNSIGNED;
    CharacterAttributePtr       = buf;
    BufferLength                = sizeof(buf);
    StringLengthPtr             = &StringLength;
    NumericAttributePtr         = &NumericAttribute;

    sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
    if (sr != SQL_SUCCESS) return -1;
    W("Column%d:`SQL_DESC_UNSIGNED`,[%s]", i+1, NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");
  }

#define DUMP(fmt, ...) printf(fmt "\n", ##__VA_ARGS__)           /* { */
  while (1) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) return -1;
    if (sr == SQL_NO_DATA) break;

    DUMP("                          ");
    char buf[4096];
    SQLLEN ind;
    SQLSMALLINT i16;

    sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("TYPE_NAME:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 2, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("DATA_TYPE:[%d]%s", ind==SQL_NULL_DATA ? 0 : i16, ind==SQL_NULL_DATA ? "[null]" : sql_data_type(i16));

    sr = CALL_SQLGetData(hstmt, 3, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("COLUMN_SIZE:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 4, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("LITERAL_PREFIX:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 5, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("LITERAL_SUFFIX:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 6, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("CREATE_PARAMS:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 7, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("NULLABLE:%s", ind==SQL_NULL_DATA ? "[null]" : sql_nullable(i16));

    sr = CALL_SQLGetData(hstmt, 8, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("CASE_SENSITIVE:%s", ind==SQL_NULL_DATA ? "[null]" : !!i16 ? "SQL_TRUE" : "SQL_FALSE");

    sr = CALL_SQLGetData(hstmt, 9, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("SEARCHABLE:%s", ind==SQL_NULL_DATA ? "[null]" : sql_searchable(i16));

    sr = CALL_SQLGetData(hstmt, 10, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("UNSIGNED_ATTRIBUTE:%s", ind==SQL_NULL_DATA ? "[null]" : !!i16 ? "SQL_TRUE" : "SQL_FALSE");

    sr = CALL_SQLGetData(hstmt, 11, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("FIXED_PREC_SCALE:%s", ind==SQL_NULL_DATA ? "[null]" : !!i16 ? "SQL_TRUE" : "SQL_FALSE");

    sr = CALL_SQLGetData(hstmt, 12, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("AUTO_UNIQUE_VALUE:%s", ind==SQL_NULL_DATA ? "[null]" : !!i16 ? "SQL_TRUE" : "SQL_FALSE");

    sr = CALL_SQLGetData(hstmt, 13, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("LOCAL_TYPE_NAME:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 14, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("MINIMUM_SCALE:%s", ind==SQL_NULL_DATA ? "[null]" : !!i16 ? "SQL_TRUE" : "SQL_FALSE");

    sr = CALL_SQLGetData(hstmt, 15, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("MAXIMUM_SCALE:%s", ind==SQL_NULL_DATA ? "[null]" : !!i16 ? "SQL_TRUE" : "SQL_FALSE");

    sr = CALL_SQLGetData(hstmt, 16, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("SQL_DATA_TYPE:%s", ind==SQL_NULL_DATA ? "[null]" : sql_data_type(i16));

    sr = CALL_SQLGetData(hstmt, 17, SQL_C_SSHORT, (SQLPOINTER)&i16, sizeof(i16), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("SQL_DATETIME_SUB:%s", ind==SQL_NULL_DATA ? "[null]" : sql_data_type(i16)); // FIXME:

    sr = CALL_SQLGetData(hstmt, 18, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("NUM_PREC_RADIX:%s", ind==SQL_NULL_DATA ? "[null]" : buf);

    sr = CALL_SQLGetData(hstmt, 19, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("INTERVAL_PRECISION:%s", ind==SQL_NULL_DATA ? "[null]" : buf);
  }
#undef DUMP                                                      /* } */

  return 0;
}

static int test_conn_SQL_catalog_functions(SQLHANDLE hconn)
{
#ifndef _WIN32         /* { */
  if (1) return 0;
#endif                 /* } */
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  // const char *dsn = "SQLSERVER_ODBC_DSN";
  // const char *uid = NULL;
  // const char *pwd = NULL;

  // sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  // if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  do {
    SQLHANDLE hstmt;

    sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
    if (FAILED(sr)) {
      r = -1;
      break;
    }

    do {
      if (1 && r == 0) r = test_SQLColumns(hstmt);
      if (0 && r == 0) r = test_SQLGetTypeInfo(hstmt);
      if (r) return -1;
    } while (0);

    CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  } while (0);

  // CALL_SQLDisconnect(hconn);

  return r ? -1 : 0;
}

static int test_conn(int argc, char *argv[], SQLHANDLE hconn)
{
  conn_arg_t conn_arg = {0};

  conn_arg.odbc_type = TAOS_ODBC;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "--dsn") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.dsn = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--connstr") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.connstr = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--sqlite3") == 0) {
      conn_arg.odbc_type= SQLITE3_ODBC;
      continue;
    }
    if (strcmp(argv[i], "--mysql") == 0) {
      conn_arg.odbc_type = MYSQL_ODBC;
      continue;
    }
    if (strcmp(argv[i], "--sqlserver") == 0) {
      conn_arg.odbc_type = SQLSERVER_ODBC;
      continue;
    }
    if (strcmp(argv[i], "--ws") == 0) {
      conn_arg.ws = 1;
    }
  }

  int r = 0;

  if (conn_arg.connstr) {
    r = _driver_connect(hconn, conn_arg.connstr);
  } else if (conn_arg.dsn) {
    r = _connect(hconn, conn_arg.dsn, conn_arg.uid, conn_arg.pwd);
  } else {
    r = _connect(hconn, "TAOS_ODBC_DSN", NULL, NULL);
  }

  if (r == 0) {
    r = test_conn_SQL_catalog_functions(hconn);

    if (r == 0) {
      r = test_connected_conn(hconn, &conn_arg);
    }
  }

  CALL_SQLDisconnect(hconn);

  return r ? -1 : 0;
}

static int test_env(int argc, char *argv[], SQLHANDLE henv)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = test_conn(argc, argv, hconn);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

static int test(int argc, char *argv[])
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = test_env(argc, argv, henv);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

int main(int argc, char *argv[])
{
  int r = 0;

  const char *env_val = getenv("TEST_WITH_ENTERPRISE");
  // eg.:
  // linux:         export TEST_WITH_ENTERPRISE=ON
  // or
  // windows:       set TEST_WITH_ENTERPRISE=ON

  if (!env_val || tod_strcasecmp(env_val, "ON")) {
    I("set environment `TEST_WITH_ENTERPRISE` to `ON` to allow test-cases that require features provided by TAOS-enterprise");
  } else {
    _test_with_enterprise = 1;
  }

  r = test(argc, argv);
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}

