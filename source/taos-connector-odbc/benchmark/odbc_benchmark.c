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

#ifdef _WIN32           /* { */
#include <windows.h>
#endif                  /* } */

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sql.h>
#include <sqlext.h>

#ifdef _WIN32           /* { */
static int gettimeofday(struct timeval *tp, void *tzp);
#define sscanf sscanf_s
#else                   /* }{ */
#include <sys/time.h>
#endif                  /* } */


#define E(fmt, ...) fprintf(stderr, "@%d:%s():" fmt "\n", __LINE__, __func__, ##__VA_ARGS__)
#define SFREE(x) if (x) { free(x); x = NULL; }

#define PROFILE(statement) do {                                                                  \
  struct timeval tv0 = {0};                                                                      \
  struct timeval tv1 = {0};                                                                      \
  gettimeofday(&tv0, NULL);                                                                      \
  statement;                                                                                     \
  gettimeofday(&tv1, NULL);                                                                      \
                                                                                                 \
  double diff = difftime(tv1.tv_sec, tv0.tv_sec);                                                \
  diff += ((double)(tv1.tv_usec - tv0.tv_usec)) / 1000000;                                       \
                                                                                                 \
  E("%s", #statement);                                                                           \
  E("run_with_params(%s), with %zd rows / %zd cols:", cfg->insert, cfg->rows, cfg->cols);        \
  E("elapsed: %lf secs", diff);                                                                  \
  E("throughput: %lf rows/secs", cfg->rows / diff);                                              \
} while (0)

#ifdef _WIN32           /* { */
static int gettimeofday(struct timeval *tp, void *tzp)
{
  static const uint64_t EPOCH = ((uint64_t) 116444736000000000ULL);
  LARGE_INTEGER li;
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);

  li.QuadPart   = ft.dwHighDateTime;
  li.QuadPart <<= 32;
  li.QuadPart  += ft.dwLowDateTime;
  li.QuadPart  -= EPOCH;

  tp->tv_sec    = (long)(li.QuadPart / 10000000);
  tp->tv_usec   = (li.QuadPart % 10000000) / 10;

  return 0;
}
#endif                  /* } */

static int _timestamp_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int64_t *p64 = (int64_t*)calloc(rows, sizeof(*p64));
  if (!p64) {
    E("oom");
    return -1;
  }
  data[i_col] = p64;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  time_t t0; time(&t0);
  int64_t v = t0 * 1000;

  for (size_t i=0; i<rows; ++i) {
    p64[i] = v + i;
    p[i] = sizeof(p64[i]);
    // E("timestamp:[%zd]", p64[i]);
  }

  return 0;
}

static int _bigint_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int64_t *p64 = (int64_t*)calloc(rows, sizeof(*p64));
  if (!p64) {
    E("oom");
    return -1;
  }
  data[i_col] = p64;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    p64[i] = rand();
    p[i] = sizeof(p64[i]);
    // E("bigint:[%zd]", p64[i]);
  }

  return 0;
}

static int _int_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int32_t *p32 = (int32_t*)calloc(rows, sizeof(*p32));
  if (!p32) {
    E("oom");
    return -1;
  }
  data[i_col] = p32;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    p32[i] = rand();
    p[i] = sizeof(p32[i]);
    // E("int:[%zd]", p32[i]);
  }

  return 0;
}

static int _short_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int16_t *p16 = (int16_t*)calloc(rows, sizeof(*p16));
  if (!p16) {
    E("oom");
    return -1;
  }
  data[i_col] = p16;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    p16[i] = rand();
    p[i] = sizeof(p16[i]);
    // E("smallint:[%zd]", p16[i]);
  }

  return 0;
}

static int _tinyint_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int8_t *p8 = (int8_t*)calloc(rows, sizeof(*p8));
  if (!p8) {
    E("oom");
    return -1;
  }
  data[i_col] = p8;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    p8[i] = rand();
    p[i] = sizeof(p8[i]);
    // E("tinyint:[%zd]", p8[i]);
  }

  return 0;
}

static int _double_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  double *dbl = (double*)calloc(rows, sizeof(*dbl));
  if (!dbl) {
    E("oom");
    return -1;
  }
  data[i_col] = dbl;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    dbl[i] = rand();
    dbl[i] /= (double)rand();
    p[i] = sizeof(dbl[i]);
    // E("double:[%lg]", dbl[i]);
  }

  return 0;
}

static int _float_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  float *flt = (float*)calloc(rows, sizeof(*flt));
  if (!flt) {
    E("oom");
    return -1;
  }
  data[i_col] = flt;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    flt[i] = (float)rand();
    flt[i] /= (float)rand();
    p[i] = sizeof(flt[i]);
    // E("float:[%lg]", flt[i]);
  }

  return 0;
}

static int _char_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col, size_t len)
{
  char *ps = (char*)calloc(rows, len + 1);
  if (!ps) {
    E("oom");
    return -1;
  }
  data[i_col] = ps;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    int v = rand();
    snprintf(ps + i * (len + 1), len + 1, "%0*d", (int)len, v);
    p[i] = len;
    // E("char:[%s]", ps + i * (len + 1));
  }

  return 0;
}

typedef struct odbc_conn_cfg_s               odbc_conn_cfg_t;
struct odbc_conn_cfg_s {
  const char                    *conn;
  size_t                         rows;
  size_t                         cols;

  const char                    *sqlc_names[64];
  int                            sqlc_types[64];
  int                            sqlc_col_sizes[64];


  const char                    *drop;
  char                           create[4096];
  char                           insert[4096];
};

static int _prepare_data_v(SQLHANDLE hstmt, odbc_conn_cfg_t *cfg,
    SQLUSMALLINT **ParamStatusArray, SQLLEN ***StrLen_or_IndPtr, SQLULEN *ParamsProcessed, void ***data)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  int len = 0;

  void **p = (void**)calloc(cfg->cols, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  *data = p;

  SQLUSMALLINT *pa = (SQLUSMALLINT*)calloc(cfg->rows, sizeof(*pa));
  if (!pa) {
    E("oom");
    return -1;
  }
  *ParamStatusArray = pa;

  SQLLEN **pn = (SQLLEN**)calloc(cfg->cols, sizeof(*pn));
  if (!pn) {
    E("oom");
    return -1;
  }
  *StrLen_or_IndPtr = pn;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)(SQLULEN)cfg->rows, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, pa, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, ParamsProcessed, 0);

  for (size_t i=0; i<cfg->cols; ++i) {
    SQLSMALLINT     sqlc_type       = cfg->sqlc_types[i];
    SQLSMALLINT     sql_type        = 0;
    SQLUSMALLINT    ColumnSize      = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLLEN          BufferLength    = 0;
    switch (sqlc_type) {
      case SQL_C_SBIGINT:
        if (i == 0) r = _timestamp_prepare(p, pn, cfg->rows, i);
        else        r = _bigint_prepare(p, pn, cfg->rows, i);
        if (r) return -1;
        sql_type = SQL_BIGINT;
        break;
      case SQL_C_SLONG:
        r = _int_prepare(p, pn, cfg->rows, i);
        if (r) return -1;
        sql_type = SQL_INTEGER;
        break;
      case SQL_C_SHORT:
        r = _short_prepare(p, pn, cfg->rows, i);
        if (r) return -1;
        sql_type = SQL_SMALLINT;
        break;
      case SQL_C_STINYINT:
        r = _tinyint_prepare(p, pn, cfg->rows, i);
        if (r) return -1;
        sql_type = SQL_TINYINT;
        break;
      case SQL_C_DOUBLE:
        r = _double_prepare(p, pn, cfg->rows, i);
        if (r) return -1;
        sql_type = SQL_DOUBLE;
        break;
      case SQL_C_FLOAT:
        r = _float_prepare(p, pn, cfg->rows, i);
        if (r) return -1;
        sql_type = SQL_REAL;
        break;
      case SQL_C_CHAR:
        len = cfg->sqlc_col_sizes[i];
        r = _char_prepare(p, pn, cfg->rows, i, len);
        if (r) return -1;
        sql_type = SQL_VARCHAR;
        ColumnSize = len;
        BufferLength = len + 1;
        break;
      default:
        E("SQL_C_xxx[%d/0x%x] not implemented yet", sqlc_type, sqlc_type);
        return -1;
    }

    sr = SQLBindParameter(hstmt, (SQLUSMALLINT)(i+1), SQL_PARAM_INPUT, sqlc_type, sql_type, ColumnSize, DecimalDigits, p[i], BufferLength, pn[i]);
    if (sr != SQL_SUCCESS) return -1;
  }

  return 0;
}

static int _prepare_and_run(SQLHANDLE hstmt, odbc_conn_cfg_t *cfg)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = SQLPrepare(hstmt, (SQLCHAR*)cfg->insert, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = SQLExecute(hstmt);
  if (sr != SQL_SUCCESS) return -1;

  return 0;
}

static int _run_prepare(SQLHANDLE hstmt, odbc_conn_cfg_t *cfg, void ***data)
{
  int r = 0;

  SQLUSMALLINT *ParamStatusArray = NULL;
  SQLULEN ParamsProcessed = 0;
  SQLLEN **StrLen_or_IndPtr = NULL;

  r = _prepare_data_v(hstmt, cfg, &ParamStatusArray, &StrLen_or_IndPtr, &ParamsProcessed, data);

  if (r) return -1;

  r = _prepare_and_run(hstmt, cfg);

  SFREE(ParamStatusArray);
  for (size_t i=0; i<cfg->cols; ++i) {
    SFREE(StrLen_or_IndPtr[i]);
  }
  SFREE(StrLen_or_IndPtr);

  if (r) return -1;

  return 0;
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s --conn <conn> --rows <rows> [field_desc]...\n"
                  "  running benchmark\n",
                  arg0, arg0);
}

static int _parse_sqlc_type(const char *sqlc, int *sqlc_type, int *col_size)
{
  if (strcmp(sqlc, "timestamp") == 0) {
    *sqlc_type = SQL_C_SBIGINT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(sqlc, "bigint") == 0) {
    *sqlc_type = SQL_C_SBIGINT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(sqlc, "int") == 0) {
    *sqlc_type = SQL_C_SLONG;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(sqlc, "smallint") == 0) {
    *sqlc_type = SQL_C_SHORT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(sqlc, "tinyint") == 0) {
    *sqlc_type = SQL_C_STINYINT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(sqlc, "double") == 0) {
    *sqlc_type = SQL_C_DOUBLE;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(sqlc, "float") == 0) {
    *sqlc_type = SQL_C_FLOAT;
    *col_size  = 0;
    return 0;
  }

  int n = 0;

  n = sscanf(sqlc, "varchar(%d)", col_size);
  if (n == 1) {
    *sqlc_type = SQL_C_CHAR;
    return 0;
  }

  n = sscanf(sqlc, "nchar(%d)", col_size);
  if (n == 1) {
    // NOTE: we check map SQL_C_CHAR/SQL_VARCHAR/TSDB_DATA_TYPE_NCHAR
    *sqlc_type = SQL_C_CHAR;
    return 0;
  }

  E("unknown type `%s`", sqlc);
  return -1;
}

static int _gen_sqls(odbc_conn_cfg_t *cfg)
{
  int n = 0;
  char *p = NULL;
  char *end = NULL;

  cfg->drop = "drop table if exists benchmark_case0",

  // create
  p = cfg->create;
  end = cfg->create + sizeof(cfg->create);

  n = snprintf(p, end - p, "create table benchmark_case0 (");
  if (n < 0 || n >= end - p) {
    E("buffer too small");
    return -1;
  }
  p += n;

  for (size_t i=0; i<cfg->cols; ++i) {
    int sqlc_type = cfg->sqlc_types[i];
    const char *s = cfg->sqlc_names[i];
    if (!s) {
      E("unknown sqlc_type [%d]", sqlc_type);
      return -1;
    }
    if (i) {
      n = snprintf(p, end - p, ", t%zd %s", i, s);
    } else {
      n = snprintf(p, end - p, "t%zd %s", i, s);
    }
    if (n < 0 || n >= end - p) {
      E("buffer too small");
      return -1;
    }
    p += n;
  }

  n = snprintf(p, end - p, ")");
  if (n < 0 || n >= end - p) {
    E("buffer too small");
    return -1;
  }
  p += n;

  // insert
  p = cfg->insert;
  end = cfg->insert + sizeof(cfg->insert);

  n = snprintf(p, end - p, "insert into benchmark_case0 (");
  if (n < 0 || n >= end - p) {
    E("buffer too small");
    return -1;
  }
  p += n;

  for (size_t i=0; i<cfg->cols; ++i) {
    if (i) {
      n = snprintf(p, end - p, ", t%zd", i);
    } else {
      n = snprintf(p, end - p, "t%zd", i);
    }
    if (n < 0 || n >= end - p) {
      E("buffer too small");
      return -1;
    }
    p += n;
  }

  n = snprintf(p, end - p, ") values (");
  if (n < 0 || n >= end - p) {
    E("buffer too small");
    return -1;
  }
  p += n;

  for (size_t i=0; i<cfg->cols; ++i) {
    if (i) {
      n = snprintf(p, end - p, ", ?");
    } else {
      n = snprintf(p, end - p, "?");
    }
    if (n < 0 || n >= end - p) {
      E("buffer too small");
      return -1;
    }
    p += n;
  }

  n = snprintf(p, end - p, ")");
  if (n < 0 || n >= end - p) {
    E("buffer too small");
    return -1;
  }
  p += n;

  return 0;
}

static int _do_run(SQLHANDLE hstmt, odbc_conn_cfg_t *cfg)
{
  void **data = NULL;

  int r = 0;

  r = _run_prepare(hstmt, cfg, &data);

  if (data) {
    for (size_t i=0; i<cfg->cols; ++i) {
      SFREE(data[i]);
    }
    SFREE(data);
  }

  return r ? -1 : 0;
}

static int _run(odbc_conn_cfg_t *cfg, SQLHANDLE *penv, SQLHANDLE *pdbc, SQLHANDLE *pstmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, penv);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE henv = *penv;

  sr = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (sr != SQL_SUCCESS) return -1;

  sr = SQLAllocHandle(SQL_HANDLE_DBC, henv, pdbc);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE hdbc = *pdbc;

  char buf[1024];
  buf[0] = '\0';
  SQLSMALLINT StringLength = 0;
  sr = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)cfg->conn, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength, SQL_DRIVER_COMPLETE);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  sr = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, pstmt);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE hstmt = *pstmt;

  sr = SQLExecDirect(hstmt, (SQLCHAR*)cfg->drop, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = SQLExecDirect(hstmt, (SQLCHAR*)cfg->create, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  int r = 0;

  PROFILE(r = _do_run(hstmt, cfg));

  return r ? -1 : 0;
}

static int _add_sqlc_type(odbc_conn_cfg_t *cfg, const char *sqlc)
{
  int r = 0;

  int sqlc_type = 0;
  int col_size = 0;
  r = _parse_sqlc_type(sqlc, &sqlc_type, &col_size);
  if (r) return -1;

  const size_t nr_cols = sizeof(cfg->sqlc_types) / sizeof(cfg->sqlc_types[0]);
  if (cfg->cols >= nr_cols) {
    E("%s specified, but # of cols overflow:%zd >= %zd", sqlc, cfg->cols, nr_cols);
    return -1;
  }

  cfg->sqlc_names[cfg->cols] = sqlc;
  cfg->sqlc_types[cfg->cols] = sqlc_type;
  cfg->sqlc_col_sizes[cfg->cols] = col_size;
  cfg->cols += 1;

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

  odbc_conn_cfg_t cfg = {0};
  cfg.conn = "DSN=TAOS_ODBC_DSN;DATABASE=foo";
  cfg.rows = 32767; // INT16_MAX

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "--conn") == 0) {
      ++i;
      if (i>=argc) {
        E("<conn> is expected after `--conn`, but got ==null==");
        return -1;
      }
      cfg.conn = argv[i];
      continue;
    }
    if (strcmp(arg, "--rows") == 0) {
      ++i;
      if (i>=argc) {
        E("<rows> is expected after `--rows`, but got ==null==");
        return -1;
      }
      char *end = NULL;
      errno = 0;
      long long rows = strtoll(argv[i], &end, 0);
      if (end && *end) {
        E("<rows> is expected after `--rows`, but got ==%s==", argv[i]);
        return -1;
      }
      if (rows == LLONG_MIN && errno == ERANGE) {
        E("<rows> is expected after `--rows`, but got ==%s==", argv[i]);
        return -1;
      }
      if (rows == LLONG_MAX && errno == ERANGE) {
        E("<rows> is expected after `--rows`, but got ==%s==", argv[i]);
        return -1;
      }
      if (rows == 0 && errno == EINVAL) {
        E("<rows> is expected after `--rows`, but got ==%s==", argv[i]);
        return -1;
      }
      cfg.rows = (size_t)rows;
      continue;
    }

    r = _add_sqlc_type(&cfg, argv[i]);
    if (r) return -1;
  }

  if (cfg.cols == 0) {
    r = _add_sqlc_type(&cfg, "timestamp");
    if (r) return -1;
    r = _add_sqlc_type(&cfg, "varchar(20)");
    if (r) return -1;
  }

  r = _gen_sqls(&cfg);
  if (r) return -1;

#ifdef _WIN32                    /* { */
  srand((unsigned int)time(NULL));
#else                            /* }{ */
  srand(time(NULL));
#endif                           /* } */


  SQLHANDLE henv = SQL_NULL_HANDLE, hdbc = SQL_NULL_HANDLE, hstmt = SQL_NULL_HANDLE;

  r = _run(&cfg, &henv, &hdbc, &hstmt);

  if (hstmt != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    hstmt = SQL_NULL_HANDLE;
  }

  if (hdbc != SQL_NULL_HANDLE) {
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    hdbc = SQL_NULL_HANDLE;
  }

  if (henv != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    henv = SQL_NULL_HANDLE;
  }

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return r ? 1 : 0;
}

