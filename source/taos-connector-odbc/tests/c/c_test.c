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

#include "iconv_wrapper.h"

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#ifdef _WIN32       /* { */
#include <process.h>
#else               /* }{ */
#include <unistd.h>
#endif              /* } */
#include <wchar.h>


#define DUMP(fmt, ...)             fprintf(stderr, fmt "\n", ##__VA_ARGS__)
#define DCASE(fmt, ...)            fprintf(stderr, "@%d:%s():@%d:%s():" fmt "\n", __LINE__, __func__, line, func, ##__VA_ARGS__)
#define DCASE_W(fmt, ...)          do { \
    wchar_t wfunc1[512]; \
    wchar_t wfunc2[512]; \
    size_t convertedChars1 = mbstowcs(wfunc1, __func__, sizeof(wfunc1)); \
    size_t convertedChars2 = mbstowcs(wfunc2, func, sizeof(wfunc2)); \
    if (convertedChars1 != (size_t)-1 && convertedChars2 != (size_t)-1) { \
        wfunc1[convertedChars1] = L'\0'; \
        wfunc2[convertedChars2] = L'\0'; \
        wprintf(L"@%d:%ls():@%d:%ls():" fmt L"\n", __LINE__, wfunc1, line, wfunc2, ##__VA_ARGS__); \
    } else { \
        wprintf(L"@%d: Conversion failed for function name\n", __LINE__); \
    } \
} while (0)


#ifdef _WIN32             /* { */
#define WCHAR_ENCODER_NAME          "UTF-16LE"
#else                     /* }{ */
#define WCHAR_ENCODER_NAME          "UTF-32LE"
#endif                    /* } */

static const char          *SQL_C_WCHAR_encoder_name = "UTF-16LE";
static const char          *wchar_encoder_name       = WCHAR_ENCODER_NAME;
static iconv_t              widechar_conv            = (iconv_t)-1;

typedef struct handles_s                 handles_t;
struct handles_s {
  SQLHANDLE henv;
  SQLHANDLE hdbc;
  SQLHANDLE hstmt;

  uint8_t              connected:1;
};

static void handles_free_stmt(handles_t *handles)
{
  if (!handles) return;
  if (handles->hstmt == SQL_NULL_HANDLE) return;

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
  handles->hstmt = SQL_NULL_HANDLE;
}

static void handles_disconnect(handles_t *handles)
{
  if (!handles) return;

  if (handles->hdbc == SQL_NULL_HANDLE) return;

  if (!handles->connected) return;

  handles_free_stmt(handles);

  CALL_SQLDisconnect(handles->hdbc);
  handles->connected = 0;
}

static void handles_free_conn(handles_t *handles)
{
  if (!handles) return;
  if (handles->hdbc == SQL_NULL_HANDLE) return;

  handles_disconnect(handles);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, handles->hdbc);
  handles->hdbc = SQL_NULL_HANDLE;
}

static void handles_release(handles_t *handles)
{
  if (!handles) return;

  handles_free_conn(handles);

  if (handles->henv != SQL_NULL_HANDLE) {
    CALL_SQLFreeHandle(SQL_HANDLE_ENV, handles->henv);
    handles->henv = SQL_NULL_HANDLE;
  }
}

static int _driver_connect(SQLHANDLE hdbc, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024];
  buf[0] = '\0';
  SQLSMALLINT StringLength = 0;
  sr = CALL_SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connstr, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) return -1;

  return 0;
}

static int handles_init(handles_t *handles, const char *connstr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  do {
    if (handles->henv == SQL_NULL_HANDLE) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->henv);
      if (FAILED(sr)) break;

      sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
      if (FAILED(sr)) break;

      if (0) sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
      if (FAILED(sr)) break;
    }

    if (handles->hdbc == SQL_NULL_HANDLE) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, handles->henv, &handles->hdbc);
      if (FAILED(sr)) break;
    }

    if (!handles->connected) {
      r = _driver_connect(handles->hdbc, connstr);
      if (r) break;

      handles->connected = 1;
    }

    if (handles->hstmt == SQL_NULL_HANDLE) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
      if (FAILED(sr)) break;
    }

    return 0;
  } while (0);

  handles_release(handles);

  return -1;
}

static SQLSMALLINT sqltype_2_ctype(SQLLEN v, SQLLEN is_unsigned)
{
  SQLSMALLINT TargetType = 0;
  switch (v) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
      TargetType = SQL_C_CHAR;
      break;
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
      TargetType = SQL_C_WCHAR;
      break;
    case SQL_DECIMAL:
    case SQL_NUMERIC:
      TargetType = SQL_C_NUMERIC;
      break;
    case SQL_SMALLINT:
      TargetType = is_unsigned ? SQL_C_USHORT : SQL_C_SSHORT;
      break;
    case SQL_INTEGER:
      TargetType = is_unsigned ? SQL_C_ULONG : SQL_C_SLONG;
      break;
    case SQL_REAL:
    case SQL_FLOAT:
      TargetType = SQL_C_FLOAT;
      break;
    case SQL_DOUBLE:
      TargetType = SQL_C_DOUBLE;
      break;
    case SQL_BIT:
      TargetType = SQL_C_BIT;
      break;
    case SQL_TINYINT:
      TargetType = is_unsigned ? SQL_C_UTINYINT : SQL_C_STINYINT;
      break;
    case SQL_BIGINT:
      TargetType = is_unsigned ? SQL_C_UBIGINT : SQL_C_SBIGINT;
      break;
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
      TargetType = SQL_C_BINARY;
      break;
    case SQL_DATETIME:
      TargetType = SQL_C_TYPE_DATE;
      break;
    case SQL_TYPE_DATE:
      TargetType = SQL_C_DATE;
      break;
    case SQL_TYPE_TIME:
      TargetType = SQL_C_TIME;
      break;
    case SQL_TYPE_TIMESTAMP:
      TargetType = SQL_C_TIMESTAMP;
      break;
    case SQL_GUID:
      TargetType = SQL_C_GUID;
      break;
    case SQL_ALL_TYPES:
      TargetType = SQL_C_DEFAULT;
      break;
    default:
      TargetType = SQL_C_CHAR;
  }
  return TargetType;
}

static int _dump_result_sets(handles_t *handles)
{
  SQLRETURN sr = SQL_SUCCESS;
  int i = 0;
  do {
    DUMP("MoreResults[%d] ...", ++i);
    SQLLEN RowCount = 0;
    sr = CALL_SQLRowCount(handles->hstmt, &RowCount);
    if (FAILED(sr)) return -1;
    DUMP("Rows affected:%" PRId64 "", (int64_t)RowCount);

    SQLSMALLINT ColumnCount = 0;
    sr = CALL_SQLNumResultCols(handles->hstmt, &ColumnCount);
    if (FAILED(sr)) return -1;
    DUMP("ColumnCount:%d", ColumnCount);

    size_t j = 0;
    do {
      if (ColumnCount == 0) break;
      sr = CALL_SQLFetch(handles->hstmt);
      if (sr==SQL_NO_DATA) break;
      if (FAILED(sr)) return -1;
      ++j;
      for (int i=0; i<ColumnCount; ++i) {
        char colName[1024]; colName[0] = '\0';
        SQLSMALLINT NameLength      = 0;
        SQLSMALLINT DataType        = 0;
        SQLULEN     ColumnSize      = 0;
        SQLSMALLINT DecimalDigits   = 0;
        SQLSMALLINT Nullable        = 0;
        sr = CALL_SQLDescribeCol(handles->hstmt, (SQLUSMALLINT)i+1, (SQLCHAR*)colName, sizeof(colName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
        if (FAILED(sr)) return -1;

        char buf[4096]; buf[0] = '\0';
        SQLLEN len = 0;
        sr = CALL_SQLGetData(handles->hstmt, (SQLUSMALLINT)i+1, SQL_C_CHAR, buf, sizeof(buf), &len);
        if (FAILED(sr)) return -1;
        if (len == SQL_NULL_DATA) {
          DUMP("RowCol[%zd,%d]:%s=null", j, i+1, colName);
          continue;
        }
        DUMP("RowCol[%zd,%d]:%s=[%s]", j, i+1, colName, buf);
      }
    } while (1);

    sr = CALL_SQLMoreResults(handles->hstmt);
    if (sr == SQL_NO_DATA) break;
    if (FAILED(sr)) return -1;
  } while (1);

  return 0;
}

static int _execute_batches_of_statements(handles_t *handles, const char *sqls)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLUINTEGER bs_support = 0;

  sr = CALL_SQLGetInfo(handles->hdbc, SQL_BATCH_SUPPORT, &bs_support, sizeof(bs_support), NULL);
  if (FAILED(sr)) return -1;
  if (sr != SQL_SUCCESS) return -1;
  DUMP("bs_support:0x%x", bs_support);

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sqls, SQL_NTS);
  if (FAILED(sr)) return -1;

  return _dump_result_sets(handles);
}

static int execute_batches_of_statements(const char *connstr, const char *sqls)
{
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, connstr);
  if (r) return -1;

  if (sqls) r = _execute_batches_of_statements(&handles, sqls);

  handles_disconnect(&handles);

  return r ? -1 : 0;
}

static int _execute_with_params(handles_t *handles, const char *sql, size_t nr_params, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr_params; ++i) {
    const char *val = va_arg(ap, const char*);
    size_t len = strlen(val);
    sr = CALL_SQLBindParameter(handles->hstmt, (SQLUSMALLINT)i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, len, 0, (SQLPOINTER)val, (SQLLEN)len, NULL);
    if (FAILED(sr)) return -1;
  }

  sr = CALL_SQLPrepare(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLExecute(handles->hstmt);
  if (FAILED(sr)) return -1;

  return _dump_result_sets(handles);
}

static int execute_with_params(const char *connstr, const char *sql, size_t nr_params, ...)
{
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, connstr);
  if (r) return -1;

  va_list ap;
  va_start(ap, nr_params);
  r = _execute_with_params(&handles, sql, nr_params, ap);
  va_end(ap);

  handles_disconnect(&handles);

  return r ? -1 : 0;
}

static int _execute_with_int(handles_t *handles, const char *sql, int v, size_t len)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLBindParameter(handles->hstmt, (SQLUSMALLINT)1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_VARCHAR, len, 0, (SQLPOINTER)&v, (SQLLEN)sizeof(v), NULL);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLPrepare(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLExecute(handles->hstmt);
  if (FAILED(sr)) return -1;

  return _dump_result_sets(handles);
}

static int execute_with_int(const char *connstr, const char *sql, int v, size_t len)
{
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, connstr);
  if (r) return -1;

  r = _execute_with_int(&handles, sql, v, len);

  handles_disconnect(&handles);

  return r ? -1 : 0;
}

static int test_case0(handles_t *handles, const char *conn_str, int ws)
{
  (void)handles;
  (void)ws;

  int r = 0;
  const char *connstr = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;
  char tmbuf[128]; tmbuf[0] = '\0';

#ifdef _WIN32_SQLSERVER     /* { */
  connstr = "DSN=SQLSERVER_ODBC_DSN";
  sqls = "drop table if exists t;"
         "create table t(name varchar(7), mark nchar(20), i16 smallint);"
         "insert into t (name, mark) values ('测试', '人物');"
         "insert into t (name, mark) values ('测试', '人物a');"
         "insert into t (name, mark) values ('测试', '人物x');"
         "select * from t;";
  r = execute_batches_of_statements(connstr, sqls);
  if (r) return -1;
  if (!ws) {
    sql = "insert into t (name, mark) values (?, ?)";
    r = execute_with_params(connstr, sql, 2, "测试x", "人物y");
    if (r) return -1;
    sqls = "select * from t;";
    r = execute_batches_of_statements(connstr, sqls);
    if (r) return -1;
    sql = "select * from t where name like ?";
    r = execute_with_params(connstr, sql, 1, "测试");
    if (r) return -1;
    sql = "insert into t (i16) values (?)";
    r = execute_with_int(connstr, sql, 32767, 5);
    if (r) return -1;
    sql = "select * from t where i16 = ?";
    r = execute_with_params(connstr, sql, 1, "32767");
    if (r) return -1;
  }
#endif                     /* } */
  connstr = conn_str;
  sqls = "drop database if exists foo;"
         "create database if not exists foo;"
         "use foo;"
         "drop table if exists t;"
         "create table t(ts timestamp, name varchar(20), mark nchar(20), i32 int);"
         "insert into t (ts, name, mark) values (now(), '测试', '人物');"
         "insert into t (ts, name, mark) values (now(), '测试', '人物m');"
         "insert into t (ts, name, mark) values (now(), '测试', '人物n');"
         "select * from t;";
  r = execute_batches_of_statements(connstr, sqls);
  if (r) return -1;
  if (!ws) {
    sql = "insert into foo.t (ts, name, mark) values (?, ?, ?)";
    r = execute_with_params(connstr, sql, 3, "2023-05-07 10:11:44.215", "测试", "人物y");
    if (r) return -1;
    tod_get_format_current_local_timestamp_ms(tmbuf, sizeof(tmbuf));
    DUMP("current:%s", tmbuf);
    r = execute_with_params(connstr, sql, 3, tmbuf, "测试", "人物z");
    if (r) return -1;
    sqls = "select * from foo.t;";
    r = execute_batches_of_statements(connstr, sqls);
    if (r) return -1;
    sql = "select * from foo.t where name like ?";
    r = execute_with_params(connstr, sql, 1, "测试");
    if (r) return -1;
    sql = "insert into foo.t (ts, i32) values (?, ?)";
    r = execute_with_params(connstr, sql, 2, "2023-05-07 10:11:45.216", "32767");
    if (r) return -1;
    sql = "select * from foo.t where i32 = ?";
    r = execute_with_int(connstr, sql, 32767, 5);
    if (r) return -1;
  }

  return 0;
}

static int _check_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  CALL_SQLCloseCursor(handles->hstmt);
  CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
  CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  SQLSMALLINT ColumnCount = 0;
  sr = CALL_SQLNumResultCols(handles->hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;
  if ((size_t)ColumnCount != nr_cols) {
    DCASE("expected %zd columns, but got ==%d==", nr_cols, ColumnCount);
    return -1;
  }

  for (size_t i_row = 0; i_row < nr_rows; ++i_row) {
    sr = CALL_SQLFetch(handles->hstmt);
    if (sr==SQL_NO_DATA) {
      DCASE("expected %zd rows, but got ==%zd==", nr_rows, i_row);
      return -1;
    }
    if (FAILED(sr)) return -1;
    for (size_t i_col=0; i_col<(size_t)ColumnCount; ++i_col) {
      char colName[1024]; colName[0] = '\0';
      SQLSMALLINT NameLength      = 0;
      SQLSMALLINT DataType        = 0;
      SQLULEN     ColumnSize      = 0;
      SQLSMALLINT DecimalDigits   = 0;
      SQLSMALLINT Nullable        = 0;
      sr = CALL_SQLDescribeCol(handles->hstmt, (SQLUSMALLINT)i_col+1, (SQLCHAR*)colName, sizeof(colName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
      if (FAILED(sr)) return -1;

      SQLLEN is_unsigned = SQL_FALSE;

      sr = CALL_SQLColAttribute(handles->hstmt, (SQLUSMALLINT)i_col+1, SQL_DESC_UNSIGNED, NULL, 0, NULL, &is_unsigned);
      if (FAILED(sr)) return -1;

      SQLSMALLINT c_type = sqltype_2_ctype(DataType, is_unsigned);
      SQLSMALLINT target_type = c_type;
      if (c_type != SQL_C_BINARY)
        target_type = SQL_C_CHAR;

      char buf[4096]; buf[0] = '\0';
      SQLLEN len = 0;
      sr = CALL_SQLGetData(handles->hstmt, (SQLUSMALLINT)i_col+1, target_type, buf, sizeof(buf), &len);
      if (FAILED(sr)) return -1;
      switch (target_type) {
        case SQL_C_CHAR:
        {
          const char *v = va_arg(ap, const char*);
          if (len == SQL_NULL_DATA) {
            if (v) {
              DCASE("[%zd,%zd]:expected [%s], but got ==null==", i_row+1, i_col+1, v);
              return -1;
            }
            continue;
          }
          if (len == SQL_NTS) len = (SQLLEN)strlen(buf);
          if (strlen(v) != (size_t)len || strncmp(buf, v, (size_t)len)) {
            DCASE("[%zd,%zd]:expected [%s], but got ==%.*s==", i_row+1, i_col+1, v, (int)len, buf);
            return -1;
          }
          break;
        }
        case SQL_C_BINARY:
        {
          char* expected = va_arg(ap, char*);
          if (memcmp(buf, expected, len)) {
            char hex_expected[1024] = {0};
            char hex_actual[1024] = {0};
            (void)tod_hexify(hex_expected, sizeof(hex_expected), (const unsigned char *)expected, len);
            (void)tod_hexify(hex_actual, sizeof(hex_actual), (const unsigned char *)buf, len);
            DCASE("[%zd,%zd]:expected [%s], but got ==%s==", i_row+1, i_col+1, hex_expected, hex_actual);
            return -1;
          }
          break;
        }
        default:
          DCASE("[%zd,%zd]:invalid target type, sql_type[%d][%s], c_type:[%d][%s]", i_row+1, i_col+1, DataType, sql_data_type(DataType), c_type, sqlc_data_type(c_type));
          return -1;
      }
    }
  }

  sr = CALL_SQLFetch(handles->hstmt);
  if (sr == SQL_NO_DATA) return 0;
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    return -1;
  }

  DCASE("expected %zd rows, but got ==more rows==", nr_rows);
  return -1;
}

static int _check_col_bind_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  CALL_SQLCloseCursor(handles->hstmt);
  CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
  CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);

  char bufs[30][1024];
  SQLLEN lens[30];
  SQLSMALLINT sql_types[30] = {0};
  SQLSMALLINT c_types[30] = {0};

  size_t nr_bufs = sizeof(bufs)/sizeof(bufs[0]);
  if (nr_bufs < nr_cols) {
    DCASE("columns %zd more than %zd:not supported yet", nr_cols, nr_bufs);
    return -1;
  }

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  SQLSMALLINT ColumnCount = 0;
  sr = CALL_SQLNumResultCols(handles->hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;
  if ((size_t)ColumnCount != nr_cols) {
    DCASE("expected %zd columns, but got ==%d==", nr_cols, ColumnCount);
    return -1;
  }

  for (size_t i_col=0; i_col<(size_t)ColumnCount; ++i_col) {
    char colName[1024]; colName[0] = '\0';
    SQLSMALLINT  NameLength     = 0;
    SQLSMALLINT *DataType       = &sql_types[i_col];
    SQLULEN      ColumnSize     = 0;
    SQLSMALLINT  DecimalDigits  = 0;
    SQLSMALLINT  Nullable       = 0;
    sr = CALL_SQLDescribeCol(handles->hstmt, (SQLUSMALLINT)i_col+1, (SQLCHAR*)colName, sizeof(colName), &NameLength, DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;

    SQLLEN is_unsigned = SQL_FALSE;

    sr = CALL_SQLColAttribute(handles->hstmt, (SQLUSMALLINT)i_col+1, SQL_DESC_UNSIGNED, NULL, 0, NULL, &is_unsigned);
    if (FAILED(sr)) return -1;

    c_types[i_col] = sqltype_2_ctype(*DataType, is_unsigned);
    sr = CALL_SQLBindCol(handles->hstmt, (SQLUSMALLINT)i_col+1, c_types[i_col], bufs[i_col], sizeof(bufs[i_col]), lens + i_col);
    if (sr != SQL_SUCCESS) return -1;
  }

  for (size_t i_row = 0; i_row < nr_rows; ++i_row) {
    memset(bufs, 0, sizeof(bufs));
    memset(lens, 0, sizeof(lens));

    sr = CALL_SQLFetch(handles->hstmt);
    if (sr==SQL_NO_DATA) {
      DCASE("expected %zd rows, but got ==%zd==", nr_rows, i_row);
      return -1;
    }
    if (FAILED(sr)) return -1;
    for (size_t i_col=0; i_col<(size_t)ColumnCount; ++i_col) {
      switch (c_types[i_col]) {
        case SQL_C_CHAR:
        {
          const char *v = va_arg(ap, const char *);
          if (lens[i_col] == SQL_NULL_DATA) {
            if (v) {
              DCASE("[%zd,%zd]:expected [%s], but got ==null==", i_row+1, i_col+1, v);
              return -1;
            }
            continue;
          }
          if (lens[i_col] == SQL_NTS) lens[i_col] = (SQLLEN)strlen(bufs[i_col]);
          if (strlen(v) != (size_t)lens[i_col] || strncmp(bufs[i_col], v, (size_t)lens[i_col])) {
            DCASE("[%zd,%zd]:expected [%s], but got ==%.*s==", i_row+1, i_col+1, v, (int)lens[i_col], bufs[i_col]);
            return -1;
          }
          break;
        }
        case SQL_C_WCHAR:
        {
          const unsigned char *utf16le = (const unsigned char*)bufs[i_col];
          const wchar_t *expected = va_arg(ap, const wchar_t *);
          if (lens[i_col] == SQL_NULL_DATA) {
            if (expected) {
              DCASE_W(L"[%zd,%zd]:expected [%ls], but got ==null==", i_row+1, i_col+1, expected);
              return -1;
            }
            continue;
          }

          wchar_t widechar[4096]; *widechar = L'\0';
          char          *inbuf           = (char*)utf16le;
          size_t         inbytesleft     = (size_t)lens[i_col];
          char          *outbuf          = (char*)widechar;
          size_t         outbytesleft    = sizeof(widechar);

          size_t n = iconv(widechar_conv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
          iconv(widechar_conv, NULL, NULL, NULL, NULL);
          if (n) {
            DCASE("failed to convert from %s -> %s", SQL_C_WCHAR_encoder_name, wchar_encoder_name);
            return -1;
          }
          size_t widechar_bytes = sizeof(widechar) - outbytesleft;
          size_t WCHAR_bytes = wcslen(expected) * sizeof(wchar_t);

          if (widechar_bytes != WCHAR_bytes) {
            DCASE("bytes expected [%zd], but got ====%zd====", WCHAR_bytes, widechar_bytes);
            return -1;
          }
          if (memcmp(widechar, expected, widechar_bytes)) {
            DCASE_W(L"[%zd,%zd]:expected [%ls], but got ==%ls==", i_row+1, i_col+1, expected, widechar);
            return -1;
          }
          break;
        }
        case SQL_C_BIT:
        case SQL_C_TINYINT:
        case SQL_C_STINYINT:
        {
          int64_t expected = va_arg(ap, int64_t);
          int64_t actual = *(int8_t*)bufs[i_col];
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRId64 "], but got ==%" PRId64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_UTINYINT:
        {
          uint64_t expected = va_arg(ap, uint64_t);
          uint64_t actual = *(uint8_t*)bufs[i_col];
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRIu64 "], but got ==%" PRIu64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_SHORT:
        case SQL_C_SSHORT:
        {
          int64_t expected = va_arg(ap, int64_t);
          int64_t actual = *(int16_t *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRId64 "], but got ==%" PRId64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_USHORT:
        {
          uint64_t expected = va_arg(ap, uint64_t);
          uint64_t actual = *(uint16_t *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRIu64 "], but got ==%" PRIu64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_LONG:
        case SQL_C_SLONG:
        {
          int64_t expected = va_arg(ap, int64_t);
          int64_t actual = *(int32_t *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRId64 "], but got ==%" PRId64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_ULONG:
        {
          uint64_t expected = va_arg(ap, uint64_t);
          uint64_t actual = *(uint32_t *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRIu64 "], but got ==%" PRIu64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_SBIGINT:
        {
          int64_t expected = va_arg(ap, int64_t);
          int64_t actual = *(int64_t*)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRId64 "], but got ==%" PRId64 "==", i_row+1, i_col+1, (int64_t)expected, (int64_t)actual);
            return -1;
          }
          break;
        }
        case SQL_C_UBIGINT:
        {
          uint64_t expected = va_arg(ap, uint64_t);
          uint64_t actual = *(uint64_t *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%" PRIu64 "], but got ==%" PRIu64 "==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_FLOAT:
        {
          double expected = va_arg(ap, double);
          double actual = *(float *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%f], but got ==%f==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_DOUBLE:
        {
          double expected = va_arg(ap, double);
          double actual = *(double *)(bufs[i_col]);
          if (actual != expected) {
            DCASE("[%zd,%zd]:expected [%lf], but got ==%lf==", i_row+1, i_col+1, expected, actual);
            return -1;
          }
          break;
        }
        case SQL_C_BINARY:
        {
          char* expected = va_arg(ap, char*);
          if (memcmp(bufs[i_col], expected, lens[i_col])) {
            char hex_expected[1024] = {0};
            char hex_actual[1024] = {0};
            (void)tod_hexify(hex_expected, sizeof(hex_expected), (const unsigned char *)expected, lens[i_col]);
            (void)tod_hexify(hex_actual, sizeof(hex_actual), (const unsigned char *)(bufs[i_col]), lens[i_col]);
            DCASE("[%zd,%zd]:expected [%s], but got ==%s==", i_row+1, i_col+1, hex_expected, hex_actual);
            return -1;
          }
          break;
        }
        default:
          DCASE("[%zd,%zd]:invalid target type, sql_type[%d][%s], c_type:[%d][%s]", i_row+1, i_col+1, sql_types[i_col], sql_data_type(sql_types[i_col]), c_types[i_col], sqlc_data_type(c_types[i_col]));
          return -1;
      }
    }
  }

  sr = CALL_SQLFetch(handles->hstmt);
  if (sr == SQL_NO_DATA) return 0;
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    return -1;
  }

  DCASE("expected %zd rows, but got ==more rows==", nr_rows);
  return -1;
}

static int _check_with_values(int line, const char *func, handles_t *handles, int with_col_bind, const char *sql, size_t nr_rows, size_t nr_cols, ...)
{
  int r = 0;

  va_list ap;
  va_start(ap, nr_cols);
  if (!with_col_bind) {
    r = _check_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  } else {
    r = _check_col_bind_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  }
  va_end(ap);

  return r ? -1 : 0;
}

#define CHECK_WITH_VALUES(...)       _check_with_values(__LINE__, __func__, ##__VA_ARGS__)
#define CHECK_BIND_RESULT(ret)                                              \
  do {                                                                      \
    if ((ret) != SQL_SUCCESS && (ret) != SQL_SUCCESS_WITH_INFO) {           \
      goto end;                                                             \
    }                                                                       \
  } while(0)

static int list_table_columns(handles_t* handles, const char* catalog, const char* schema, const char* table)
{
  RETCODE ret;

  ret = CALL_SQLColumns(handles->hstmt,
                  (SQLCHAR*)catalog, SQL_NTS,
                  (SQLCHAR*)schema,  SQL_NTS,
                  (SQLCHAR*)table,   SQL_NTS,
                  (SQLCHAR*)NULL,    SQL_NTS);

  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    return -1;
  }

  SQLCHAR table_cat[256], table_schem[256], table_name[256], column_name[256];
  SWORD data_type;
  SQLCHAR type_name[256];
  SQLINTEGER column_size;
  SQLINTEGER buffer_length;
  SQLSMALLINT decimal_digits, num_prec_radix, nullable;
  SQLCHAR remarks[256], column_def[256];
  SWORD sql_data_type, sql_datetime_sub;
  SQLINTEGER char_octet_length;
  SQLINTEGER ordinal_position;
  SQLCHAR is_nullable[256];

  SQLLEN table_cat_ind, table_schem_ind, table_name_ind, column_name_ind;
  SQLLEN type_name_ind, remarks_ind, column_def_ind, sql_datetime_sub_ind, char_octet_length_ind, is_nullable_ind;

  ret = CALL_SQLBindCol(handles->hstmt, 1, SQL_C_CHAR, table_cat, sizeof(table_cat), &table_cat_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 2, SQL_C_CHAR, table_schem, sizeof(table_schem), &table_schem_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 3, SQL_C_CHAR, table_name, sizeof(table_name), &table_name_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 4, SQL_C_CHAR, column_name, sizeof(column_name), &column_name_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 5, SQL_C_SHORT, &data_type, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 6, SQL_C_CHAR, type_name, sizeof(type_name), &type_name_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 7, SQL_C_SLONG, &column_size, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 8, SQL_C_SLONG, &buffer_length, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 9, SQL_C_SHORT, &decimal_digits, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 10, SQL_C_SHORT, &num_prec_radix, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 11, SQL_C_SHORT, &nullable, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 12, SQL_C_CHAR, remarks, sizeof(remarks), &remarks_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 13, SQL_C_CHAR, column_def, sizeof(column_def), &column_def_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 14, SQL_C_SHORT, &sql_data_type, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 15, SQL_C_SHORT, &sql_datetime_sub, 0, &sql_datetime_sub_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 16, SQL_C_SLONG, &char_octet_length, 0, &char_octet_length_ind);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 17, SQL_C_SLONG, &ordinal_position, 0, NULL);
  CHECK_BIND_RESULT(ret);
  ret = CALL_SQLBindCol(handles->hstmt, 18, SQL_C_CHAR, is_nullable, sizeof(is_nullable), &is_nullable_ind);
  CHECK_BIND_RESULT(ret);

  do {
    ret = CALL_SQLFetch(handles->hstmt);
    if (ret == SQL_NO_DATA) {
      ret = SQL_SUCCESS;
      break;
    } else if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      goto end;
    }

    D("Column: %s", (column_name_ind == SQL_NULL_DATA) ? "NULL" : (char*)column_name);
    D("Data Type: %d", data_type);
    D("Type Name: %s", (type_name_ind == SQL_NULL_DATA) ? "NULL" : (char*)type_name);
    D("Column Size: %d", column_size);
    D("Buffer Length: %d", buffer_length);
    D("Decimal Digits: %d", decimal_digits);
    D("Num Prec Radix: %d", num_prec_radix);
    D("Nullable: %d", nullable);
    D("Remarks: %s", (remarks_ind == SQL_NULL_DATA) ? "NULL" : (char*)remarks);
    D("Column Def: %s", (column_def_ind == SQL_NULL_DATA) ? "NULL" : (char*)column_def);
    D("SQL Data Type: %d", sql_data_type);
    D("SQL DateTime Sub: %s:%d", (sql_datetime_sub_ind == SQL_NULL_DATA) ? "NULL" : "", (sql_datetime_sub_ind == SQL_NULL_DATA) ? 0 : sql_datetime_sub);
    D("Char Octet Length: %s:%d", (char_octet_length_ind == SQL_NULL_DATA) ? "NULL" : "", (char_octet_length_ind == SQL_NULL_DATA) ? 0 : char_octet_length);
    D("Ordinal Position: %d", ordinal_position);
    D("Is Nullable: %s", (is_nullable_ind == SQL_NULL_DATA) ? "NULL" : (char*)is_nullable);
    D("----------------------------------------");
  } while(1);

end:
  return ret;
}

static int test_all_types_with_col_bind(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create stable if not exists foo.test_types (ts timestamp, i32 int, u32 int unsigned, i64 bigint, u64 bigint unsigned, flt float, dbl double, bin binary(64), i16 smallint, u16 smallint unsigned, i8 tinyint, u8 tinyint unsigned, b bool, mark nchar(10), name varchar(64), vb varbinary(20), geo geometry(256)) tags(tag1 int);"
    "create table foo.d0 using foo.test_types (tag1) tags (1);"
    "insert into foo.d0 values('2024-08-25 10:20:45.678', -1, 2, -3, 4, 5.5, 6.6666666666, 'test', -32768, 65535, -128, 255, true, '中文2', 'test123', '\\x4eba', 'POINT(1 2)');";

  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  sql = "select ts from foo.test_types";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, L"2024-08-25 10:20:45.678");
  if (r) return -1;

  sql = "select i32 from foo.test_types";
  int64_t i32 = -1;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, i32);
  if (r) return -1;

  sql = "select u32 from foo.test_types";
  uint64_t u32 = 2;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, u32);
  if (r) return -1;

  sql = "select i64 from foo.test_types";
  int64_t i64 = -3;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, i64);
  if (r) return -1;

  sql = "select u64 from foo.test_types";
  uint64_t u64 = 4;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, u64);
  if (r) return -1;

  // TODO: there is a problem with passing the float parameter in variable arguments
  sql = "select flt from foo.test_types";
  double flt = 5.5;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, flt);
  if (r) return -1;

  sql = "select dbl from foo.test_types";
  double dbl = 6.6666666666;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, dbl);
  if (r) return -1;

  sql = "select i16 from foo.test_types";
  int64_t i16 = -32768;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, i16);
  if (r) return -1;

  sql = "select u16 from foo.test_types";
  uint64_t u16 = 65535;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, u16);
  if (r) return -1;

  sql = "select i8 from foo.test_types";
  int64_t i8 = -128;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, i8);
  if (r) return -1;

  sql = "select u8 from foo.test_types";
  uint64_t u8 = 255;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, u8);
  if (r) return -1;

  sql = "select b from foo.test_types";
  uint64_t b = 1;
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, b);
  if (r) return -1;

  sql = "select mark from foo.test_types";
  const wchar_t mark[] = L"中文2";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, mark);
  if (r) return -1;

  sql = "select name from foo.test_types";
  const char name[] = "test123";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, name);
  if (r) return -1;

  sql = "select vb from foo.test_types";
  const char vb[] = { 0x4e, 0xba };
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, vb);
  if (r) return -1;

  sql = "select geo from foo.test_types";
  const char geo[] = { 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 };
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, geo);
  if (r) return -1;

  r = list_table_columns(handles, "foo", NULL, "test_types");
  if (r < 0) return -1;

  return 0;
}

static int test_charsets(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));"
    "insert into foo.t (ts, name, mark) values (now(), 'name', 'mark');"
    "insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验');";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  sql = "select name from foo.t where name='name'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "name");
  if (r) return -1;

  sql = "select mark from foo.t where mark='mark'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "mark");
  if (r) return -1;

  sql = "select name from foo.t where name='测试'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "测试");
  if (r) return -1;

  sql = "select mark from foo.t where mark='检验'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "检验");
  if (r) return -1;

  return 0;
}

static int test_charsets_with_col_bind(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));"
    "insert into foo.t (ts, name, mark) values (now(), 'name', 'mark');"
    "insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验');";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  sql = "select name from foo.t where name='name'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, "name");
  if (r) return -1;

  sql = "select mark from foo.t where mark='mark'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, L"mark");
  if (r) return -1;

  sql = "select name from foo.t where name='测试'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, "测试");
  if (r) return -1;

  sql = "select mark from foo.t where mark='检验'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, L"检验");
  if (r) return -1;

  return 0;
}

static int _insert_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

#define COLS         20
#define ROWS         20
  char values[COLS][ROWS][1024] = {0};
  SQLLEN lens[COLS][ROWS] = {0};
  SQLULEN ColumnSizes[COLS] = {0};
  SQLUSMALLINT ParamStatusArray[ROWS] = {0};
  SQLULEN ParamsProcessed = 0;
#undef ROWS
#undef COLS

  size_t cols = sizeof(values) / sizeof(values[0]);
  size_t rows = sizeof(values[0]) / sizeof(values[0][0]);
  if (nr_rows > rows) {
    DCASE("rows %zd more than %zd:not supported yet", nr_rows, rows);
    return -1;
  }
  if (nr_cols > cols) {
    DCASE("columns %zd more than %zd:not supported yet", nr_cols, cols);
    return -1;
  }

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify the number of elements in each parameter array.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)nr_rows, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an array in which to return the status of each set of
  // parameters.
  if (0) sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  if (0) sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);
  if (sr != SQL_SUCCESS) return -1;

  for (size_t j=0; j<nr_cols; ++j) {
    for (size_t i=0; i<nr_rows; ++i) {
      const char *v = va_arg(ap, const char*);
      int n = snprintf(values[j][i], sizeof(values[j][i]), "%s", v);
      if (n < 0 || (size_t)n >= sizeof(values[j][i])) {
        DCASE("internal logic error:value[%zd,%zd]:`%s` too big", i+1, j+1, v);
        return -1;
      }
      lens[j][i] = n;
      if ((SQLULEN)n > ColumnSizes[j]) ColumnSizes[j] = n;
    }
    sr = CALL_SQLBindParameter(handles->hstmt, (SQLUSMALLINT)j+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
      ColumnSizes[j], 0, values[j], sizeof(values[j][0])/sizeof(values[j][0][0]), lens[j]);
    if (sr != SQL_SUCCESS) return -1;
  }

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  return 0;
}

static int _insert_with_values(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, ...)
{
  int r = 0;

  va_list ap;
  va_start(ap, nr_cols);
  r = _insert_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  va_end(ap);

  return r ? -1 : 0;
}

#define INSERT_WITH_VALUES(...)       _insert_with_values(__LINE__, __func__, ##__VA_ARGS__)

static int test_charsets_with_param_bind(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  if (!ws) {
    sql = "insert into foo.t (ts, name, mark) values (?, ?, ?)";
    r = INSERT_WITH_VALUES(handles, sql, 2, 3, "2023-05-14 12:13:14.567", "2023-05-14 12:13:15.678", "name", "测试", "mark", "检验");
    if (r) return -1;

    sql = "select name from foo.t where name='name'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "name");
    if (r) return -1;

    sql = "select mark from foo.t where mark='mark'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "mark");
    if (r) return -1;

    sql = "select name from foo.t where name='测试'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "测试");
    if (r) return -1;

    sql = "select mark from foo.t where mark='检验'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "检验");
    if (r) return -1;
  }

  return 0;
}

static int _remove_topics(handles_t *handles, const char *db)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *sql = "select topic_name from information_schema.ins_topics where db_name = ?";
  char topic_name_buf[1024]; topic_name_buf[0] = '\0';
  char drop_buf[2048]; drop_buf[0] = '\0';

again:

  if (0) sr = CALL_SQLPrepare(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLBindParameter(handles->hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(db), 0, (SQLPOINTER)db, (SQLLEN)strlen(db), NULL);
  if (sr != SQL_SUCCESS) return -1;

  SQLLEN topic_name_len = 0;

  sr = CALL_SQLBindCol(handles->hstmt, 1, SQL_C_CHAR, topic_name_buf, sizeof(topic_name_buf), &topic_name_len);
  if (sr != SQL_SUCCESS) return -1;

  if (0) sr = CALL_SQLExecute(handles->hstmt);
  else   sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLFetch(handles->hstmt);
  if (sr == SQL_NO_DATA) {
    CALL_SQLCloseCursor(handles->hstmt);
    CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
    CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);
    return 0;
  }
  if (sr != SQL_SUCCESS) return -1;
  // FIXME: vulnerability
  snprintf(drop_buf, sizeof(drop_buf), "drop topic `%s`", topic_name_buf);
  CALL_SQLCloseCursor(handles->hstmt);
  CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
  CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);
  
  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)drop_buf, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  goto again;
}


typedef struct test_topic_s      test_topic_t;
struct test_topic_s {
  const char *conn_str;
  const char *(*_cases)[2];
  size_t _nr_cases;
  const char *(*_expects)[2];
  volatile int8_t            flag; // 0: wait; 1: start; 2: stop
};

static void _test_topic_routine_impl(test_topic_t *ds)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, ds->conn_str);
  if (r) return;

  while (ds->flag == 0) {
#ifdef _WIN32            /* { */
    Sleep(0);
#else                    /* }{ */
    sleep(0);
#endif                   /* } */
  }

  for (size_t i=0; i<ds->_nr_cases; ++i) {
    char sql[4096]; sql[0] = '\0';
    snprintf(sql, sizeof(sql), "insert into foobar.demo (ts, name, mark) values (now()+%zds, '%s', '%s')", i, ds->_cases[i][0], ds->_cases[i][1]);
    sr = CALL_SQLExecDirect(handles.hstmt, (SQLCHAR*)sql, SQL_NTS);
    CALL_SQLCloseCursor(handles.hstmt);
    if (sr != SQL_SUCCESS) {
      r = -1;
      break;
    }
  }

  handles_disconnect(&handles);
}

#ifdef _WIN32                   /* { */
static unsigned __stdcall _test_topic_routine(void *arg)
{
  test_topic_t *ds = (test_topic_t*)arg;
  _test_topic_routine_impl(ds);
  return 0;
}
#else                           /* }{ */
static void* _test_topic_routine(void *arg)
{
  test_topic_t *ds = (test_topic_t*)arg;
  _test_topic_routine_impl(ds);
  return NULL;
}
#endif                          /* } */

static int _test_topic_monitor(handles_t *handles, test_topic_t *ds)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = handles->hstmt;

  size_t demo_limit = 0;
  SQLSMALLINT ColumnCount;

  char name[4096], value[4096];
  char sql[1024]; sql[0] = '\0';
  char row_buf[4096]; row_buf[0] = '\0';
  char *p = NULL;
  const char *end = NULL;
  SQLSMALLINT    NameLength;
  SQLSMALLINT    DataType;
  SQLULEN        ColumnSize;
  SQLSMALLINT    DecimalDigits;
  SQLSMALLINT    Nullable;

  // syntax: !topic [name]+ [{[key[=val];]*}]?");
  // ref: https://github.com/taosdata/TDengine/blob/main/docs/en/07-develop/07-tmq.mdx#create-a-consumer
  // NOTE: although both 'enable.auto.commit' and 'auto.commit.interval.ms' are still valid,
  //       taos_odbc chooses it's owner way to call `tmq_commit_sync` under the hood.
  snprintf(sql, sizeof(sql), "!topic demo {group.id=cgrpName; taos_odbc.limit.seconds=30; taos_odbc.limit.records=%zd}", ds->_nr_cases);

  DUMP("starting topic consumer ...");
  DUMP("will stop either 30 seconds have passed or %zd records have been fetched", ds->_nr_cases);
  DUMP("press Ctrl-C to abort");
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

describe:

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

fetch:

  if (demo_limit == ds->_nr_cases) {
    DUMP("demo_limit of #%zd has been reached", ds->_nr_cases);
    return 0;
  }

  sr = CALL_SQLFetch(hstmt);
  if (sr == SQL_NO_DATA) {
    sr = CALL_SQLMoreResults(hstmt);
    if (sr == SQL_NO_DATA) return 0;
    if (sr == SQL_SUCCESS) goto describe;
  }
  if (sr != SQL_SUCCESS) return -1;

  demo_limit++;

  row_buf[0] = '\0';
  p = row_buf;
  end = row_buf + sizeof(row_buf);
  for (int i=0; i<ColumnCount; ++i) {
    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)name, sizeof(name), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN Len_or_Ind;
    sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, value, sizeof(value), &Len_or_Ind);
    if (sr != SQL_SUCCESS) return -1;

    int n = snprintf(p, end - p, "%s%s:[%s]",
        i ? ";" : "",
        name,
        Len_or_Ind == SQL_NULL_DATA ? "null" : value);
    if (n < 0 || n >= end - p) {
      E("buffer too small");
      return -1;
    }
    p += n;

    if (i<3) continue;

    const char *exp = ds->_expects[demo_limit-1][i-3];
    if (Len_or_Ind == SQL_NULL_DATA) {
      if (exp) {
        E("[%zd,%d] expected `%s`, but got ==null==", demo_limit, i+1, exp);
        return -1;
      }
    } else {
      if (Len_or_Ind == SQL_NTS) Len_or_Ind = strlen(value);
      if (!exp) {
        E("[%zd,%d] expected null, but got ==`%.*s`==", demo_limit, i+1, (int)Len_or_Ind, value);
        return -1;
      }
      if ((size_t)Len_or_Ind != strlen(exp) || strncmp(value, exp, (size_t)Len_or_Ind)) {
        E("[%zd,%d] expected `%s`, but got ==`%.*s`==", demo_limit, i+1, exp, (int)Len_or_Ind, value);
        return -1;
      }
    }
  }

  DUMP("new data:%s", row_buf);

  goto fetch;
}

static int test_topic(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  if (ws) return 0;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  r = _remove_topics(handles, "foobar");
  if (r) return -1;

  sqls =
    "drop database if exists foobar;"
    "create database if not exists foobar WAL_RETENTION_PERIOD 2592000;"
    "create table foobar.demo (ts timestamp, name varchar(20), mark nchar(20));"
    "create topic demo as select name, mark from foobar.demo;";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  const char *rows[][2] = {
    {"name", "mark"},
    {"测试", "检验"},
  };

  const char *expects[][2] = {
    {"name", "mark"},
    {"测试", "检验"},
  };

  test_topic_t ds = {
    conn_str,
    rows,
    sizeof(rows)/sizeof(rows[0]),
    expects,
    0,
  };

#ifdef _WIN32                   /* { */
  uintptr_t worker;
  worker = _beginthreadex(NULL, 0, _test_topic_routine, &ds, 0, NULL);
#else                           /* }{ */
  pthread_t worker;
  r = pthread_create(&worker, NULL, _test_topic_routine, &ds);
#endif                          /* } */
  if (r) {
    fprintf(stderr, "@%d:%s():pthread_create failed:[%d]%s\n", __LINE__, __func__, r, strerror(r));
    return -1;
  }
  ds.flag = 1;

  r = _test_topic_monitor(handles, &ds);
  ds.flag = 2;

#ifdef _WIN32                   /* { */
  WaitForSingleObject((HANDLE)worker, INFINITE);
#else                           /* }{ */
  pthread_join(worker, NULL);
#endif                          /* } */

  return r ? -1 : 0;
}

static int test_params_with_all_chars(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, b bool, "
                        "i8 tinyint, u8 tinyint unsigned, i16 smallint, u16 smallint unsigned, i32 int, u32 int unsigned, i64 bigint, u64 bigint unsigned, "
                        "flt float, dbl double, name varchar(20), mark nchar(20));";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  if (!ws) {
    sql = "insert into foo.t (ts, b, i8, u8, i16, u16, i32, u32, i64, u64, flt, dbl, name, mark) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    r = INSERT_WITH_VALUES(handles, sql, 1, 14, "2023-05-14 12:13:14.567", "1", "127", "255", "32767", "65535",
        "2147483647", "4294967295", "9223372036854775807", "18446744073709551615", "1.23", "2.34", "测试", "检验");
    if (r) return -1;

    sql = "select * from foo.t where name='测试'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 14, "2023-05-14 12:13:14.567", "true", "127", "255", "32767", "65535",
        "2147483647", "4294967295", "9223372036854775807", "18446744073709551615", "1.23", "2.34", "测试", "检验");
    if (r) return -1;
  }

  return 0;
}

static int test_json_tag(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create stable foo.s1 (ts timestamp, v1 int) tags (info json);";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  if (!ws) {
    sql = "insert into foo.s1_1 using foo.s1 tags (?) values (?, ?)";
    r = INSERT_WITH_VALUES(handles, sql, 2, 3,
      "{\"k1\":\"值1\"}", "{\"k1\":\"值1\"}",
      "2023-05-14 12:13:14.567", "2023-05-14 12:13:14.568",
      "1", "2");
    if (r) return -1;

    sql = "select * from foo.s1";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 3,
      "2023-05-14 12:13:14.567", "1", "{\"k1\":\"值1\"}",
      "2023-05-14 12:13:14.568", "2", "{\"k1\":\"值1\"}");
    if (r) return -1;

    sql = "select info->'k1' from foo.s1";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 1,
      "\"值1\"",
      "\"值1\"");
    if (r) return -1;

    sql = "select v1 from foo.s1 where info contains 'k1'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 1,
      "1",
      "2");
    if (r) return -1;

    sql = "select v1 from foo.s1 where info->'k1' match '值1'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 1,
      "1",
      "2");
    if (r) return -1;
  }

  return 0;
}

static int test_pool_stmt(handles_t *handles)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)"select * from information_schema.ins_tables", SQL_NTS);
  if (FAILED(sr)) return -1;

  CALL_SQLCloseCursor(handles->hstmt);
  return 0;
}

#ifdef HAVE_TAOSWS               /* { */
static int test_taosws_conn(handles_t *handles, const char *conn_str, int ws)
{
  (void)handles;
  (void)ws;

  int r = 0;
  const char *connstr = conn_str; // "DSN=TAOS_ODBC_DSN;URL={http://127.0.0.1:6041}";
  const char *sqls = NULL;

  sqls = NULL;
  r = execute_batches_of_statements(connstr, sqls);
  if (r) return -1;

  return 0;
}
#endif                           /* }*/

static int test_pool(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  const char *conn_str = NULL;

  if (0) sr = CALL_SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
  if (FAILED(sr)) return -1;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);
  if (r) return -1;

  r = test_pool_stmt(handles);
  if (r) return -1;

  handles_disconnect(handles);

  r = handles_init(handles, conn_str);
  if (r) return -1;

  r = test_pool_stmt(handles);
  if (r) return -1;

  handles_disconnect(handles);

  return 0;
}

typedef struct case_s              case_t;
struct case_s {
  const char               *name;
  int (*routine)(handles_t *handles, const char *conn_str, int ws);
};

static case_t* find_case(case_t *cases, size_t nr_cases, const char *name)
{
  for (size_t i=0; i<nr_cases; ++i) {
    case_t *p = cases + i;
    if (strcmp(p->name, name)==0) return p;
  }
  return NULL;
}

static int running_case(handles_t *handles, case_t *_case)
{
  int r = 0;
#ifndef FAKE_TAOS
  r = _case->routine(handles, "DSN=TAOS_ODBC_DSN", 0);
  handles_disconnect(handles);
  if (r) return -1;
#endif

#ifdef HAVE_TAOSWS                /* { */
  r = _case->routine(handles, "DSN=TAOS_ODBC_WS_DSN", 1);
  handles_disconnect(handles);
  if (r) return -1;
#endif                            /* } */
  return r;
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s [--pooling] [name]...\n"
                  "  running test case `name`\n"
                  "%s -l\n"
                  "  list all test cases\n",
                  arg0, arg0, arg0);
}

static void list_cases(case_t *cases, size_t nr_cases)
{
  fprintf(stderr, "supported test cases:\n");
  for (size_t i=0; i<nr_cases; ++i) {
    fprintf(stderr, "  %s\n", cases[i].name);
  }
}

static int running_with_args(int argc, char *argv[], handles_t *handles, case_t *_cases, size_t _nr_cases)
{
  int r = 0;

  size_t nr_cases = 0;

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h")==0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "-l")==0) {
      list_cases(_cases, _nr_cases);
      return 0;
    }
    if (strcmp(arg, "--pooling")==0) {
      // ref: https://www.unixodbc.org/doc/conn_pool.html
      SQLRETURN sr = SQL_SUCCESS;
      sr = CALL_SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
      if (FAILED(sr)) return -1;
      continue;
    }
    case_t *p = find_case(_cases, _nr_cases, arg);
    if (!p) {
      DUMP("test case `%s`:not found", arg);
      return -1;
    }
    r = running_case(handles, p);
    if (r) return -1;
    ++nr_cases;
  }

  if (nr_cases == 0) {
    for (size_t i=0; i<_nr_cases; ++i) {
      r = running_case(handles, _cases + i);
      if (r) return -1;
    }
  }

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

#define RECORD(x) {#x, x}
  case_t _cases[] = {
    RECORD(test_all_types_with_col_bind),
    RECORD(test_case0),
    RECORD(test_charsets),
    RECORD(test_charsets_with_col_bind),
    RECORD(test_charsets_with_param_bind),
    RECORD(test_topic),
    RECORD(test_params_with_all_chars),
    RECORD(test_json_tag),
#ifdef HAVE_TAOSWS               /* { */
    RECORD(test_taosws_conn),
#endif                           /* } */
    // ref: https://www.unixodbc.org/doc/conn_pool.html
    RECORD(test_pool),  // NOTE: for the test purpose, this must keep in the last!!!
  };
#undef RECORD
  size_t _nr_cases = sizeof(_cases)/sizeof(_cases[0]);

  SQLRETURN sr = SQL_SUCCESS;
  if (0) sr = CALL_SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
  if (FAILED(sr)) return -1;

  widechar_conv = iconv_open(wchar_encoder_name, SQL_C_WCHAR_encoder_name);
  if (widechar_conv == (iconv_t)-1) {
    E("failed to open convertion from %s -> %s", SQL_C_WCHAR_encoder_name, wchar_encoder_name);
    r = -1;
  } else {
    handles_t handles = {0};
    r = running_with_args(argc, argv, &handles, _cases, _nr_cases);
    handles_release(&handles);
  }
  if (widechar_conv != (iconv_t)-1) {
    iconv_close(widechar_conv);
  }

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}

