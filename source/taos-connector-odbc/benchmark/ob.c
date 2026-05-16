#include "odbc_helpers.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>


#define DUMP(fmt, ...) fprintf(stderr, "[%d]:%s():" fmt "\n", __LINE__, __func__, ##__VA_ARGS__)

#define safe_free(x) do {      \
  if (x == NULL) break;        \
  free(x);                     \
  x = NULL;                    \
} while (0)

#define LOG_SQL_CALL
#ifdef LOG_SQL_CALL                   /* { */
#define MAKE_CALL(x) CALL_##x
#else                                 /* }{ }*/
#define MAKE_CALL(x) x
#endif                                /* } */

#define CALLX_SQLColAttribute                 MAKE_CALL(SQLColAttribute)
#define CALLX_SQLBindCol                      MAKE_CALL(SQLBindCol)
#define CALLX_SQLFetch                        MAKE_CALL(SQLFetch)
#define CALLX_SQLGetData                      MAKE_CALL(SQLGetData)
#define CALLX_SQLAllocHandle                  MAKE_CALL(SQLAllocHandle)
#define CALLX_SQLExecDirect                   MAKE_CALL(SQLExecDirect)
#define CALLX_SQLNumResultCols                MAKE_CALL(SQLNumResultCols)
#define CALLX_SQLRowCount                     MAKE_CALL(SQLRowCount)
#define CALLX_SQLSetStmtAttr                  MAKE_CALL(SQLSetStmtAttr)
#define CALLX_SQLPrepare                      MAKE_CALL(SQLPrepare)
#define CALLX_SQLNumParams                    MAKE_CALL(SQLNumParams)
#define CALLX_SQLDescribeParam                MAKE_CALL(SQLDescribeParam)
#define CALLX_SQLBindParameter                MAKE_CALL(SQLBindParameter)
#define CALLX_SQLExecute                      MAKE_CALL(SQLExecute)
#define CALLX_SQLFreeHandle                   MAKE_CALL(SQLFreeHandle)
#define CALLX_SQLDisconnect                   MAKE_CALL(SQLDisconnect)
#define CALLX_SQLSetEnvAttr                   MAKE_CALL(SQLSetEnvAttr)
#define CALLX_SQLSetConnectAttr               MAKE_CALL(SQLSetConnectAttr)
#define CALLX_SQLConnect                      MAKE_CALL(SQLConnect)
#define CALLX_SQLDriverConnect                MAKE_CALL(SQLDriverConnect)


#ifdef _WIN32        /* { */
#define strcasecmp       _stricmp
#endif               /* } */

static void usage(const char *app)
{
  DUMP("%s [options] <actiion> <action-options>", app);
  DUMP("  options:");
  DUMP("    --dsn <dsn>");
  DUMP("    --conn_str <conn_str>");
  DUMP("    --user <user>");
  DUMP("    --pass <pass>");
  DUMP("    --");
  DUMP("  actions:");
  DUMP("    conn");
  DUMP("    query");
}

typedef struct handles_s                 handles_t;
struct handles_s {
  SQLHENV            henv;
  SQLHDBC            hdbc;
  SQLHSTMT           hstmt;
};

typedef struct col_bind_s                col_bind_t;
struct col_bind_s {
  size_t                  idx;
  SQLLEN                  sql_type;

  SQLCHAR                 name[1024];
  SQLSMALLINT             len;
  SQLLEN                  attr;
  SQLCHAR                 value[1024];
  SQLLEN                  value_ind_len;

  SQLSMALLINT             TargetType;
  SQLPOINTER             *TargetValuePtr;
  SQLLEN                  BufferLength;
  SQLLEN                  StrLen_or_Ind;
};

typedef struct col_binds_s               col_binds_t;
struct col_binds_s {
  col_bind_t           *col_binds;
  size_t                sz;
  size_t                nr;
};

static void col_bind_release(col_bind_t *col_bind)
{
  (void)col_bind;
}

static void col_binds_release(col_binds_t *col_binds)
{
  for (size_t i=0; i<col_binds->sz; ++i) {
    col_bind_t *col_bind = col_binds->col_binds + i;
    col_bind_release(col_bind);
  }
  safe_free(col_binds->col_binds);
  col_binds->nr = 0;
  col_binds->sz = 0;
}

static int col_binds_keep(col_binds_t *col_binds, size_t sz)
{
  if (sz <= col_binds->sz) return 0;
  sz = (sz + 15) / 16 * 16;
  col_bind_t *p = (col_bind_t*)realloc(col_binds->col_binds, sz * sizeof(*p));
  if (!p) return -1;
  memset(p + col_binds->nr, 0, (sz - col_binds->nr) * sizeof(*p));
  col_binds->col_binds = p;
  col_binds->sz        = sz;
  return 0;
}

static int col_binds_add_sql_integer(col_binds_t *col_binds)
{
  int r = 0;
  r = col_binds_keep(col_binds, col_binds->nr + 1);
  if (r) return -1;
  col_bind_t *col_bind           = col_binds->col_binds + col_binds->nr;
  col_bind->idx                  = col_binds->nr++;
  col_bind->sql_type             = SQL_INTEGER;
  col_bind->TargetType           = SQL_C_SLONG;
  col_bind->TargetValuePtr       = (SQLPOINTER)col_bind->value;
  col_bind->BufferLength         = sizeof(int32_t);
  col_bind->StrLen_or_Ind        = 0;
  return 0;
}

static int col_binds_add_sql_varchar(col_binds_t *col_binds)
{
  int r = 0;
  r = col_binds_keep(col_binds, col_binds->nr + 1);
  if (r) return -1;
  col_bind_t *col_bind           = col_binds->col_binds + col_binds->nr;
  col_bind->idx                  = col_binds->nr++;
  col_bind->sql_type             = SQL_VARCHAR;
  col_bind->TargetType           = SQL_C_CHAR;
  col_bind->TargetValuePtr       = (SQLPOINTER)col_bind->value;
  col_bind->BufferLength         = sizeof(col_bind->value);
  col_bind->StrLen_or_Ind        = 0;
  return 0;
}

static int col_binds_add_sql_wvarchar(col_binds_t *col_binds)
{
  int r = 0;
  r = col_binds_keep(col_binds, col_binds->nr + 1);
  if (r) return -1;
  col_bind_t *col_bind           = col_binds->col_binds + col_binds->nr;
  col_bind->idx                  = col_binds->nr++;
  col_bind->sql_type             = SQL_WVARCHAR;
  col_bind->TargetType           = SQL_C_CHAR;
  col_bind->TargetValuePtr       = (SQLPOINTER)col_bind->value;
  col_bind->BufferLength         = sizeof(col_bind->value);
  col_bind->StrLen_or_Ind        = 0;
  return 0;
}

typedef enum param_type_e                param_type_t;
enum param_type_e {
  param_type_ms,
  param_type_ms_str,
  param_type_str,
  param_type_i32,
  param_type_flt,
  param_type_dbl,
};

typedef struct param_bind_s              param_bind_t;
struct param_bind_s {
  size_t          idx;
  param_type_t    param_type;
  SQLSMALLINT     ValueType;
  SQLSMALLINT     ParameterType;
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLPOINTER      ParameterValuePtr;
  SQLLEN          BufferLength;
  SQLLEN         *StrLen_or_IndPtr;
  char           *buf;
  size_t          sz;
  size_t          nr;

  int64_t         ms;
};

typedef struct param_binds_s             param_binds_t;
struct param_binds_s {
  param_bind_t        *param_binds;
  size_t               sz;
  size_t               nr;
  SQLUSMALLINT        *ParamStatusPtr;
  SQLULEN              ParamsProcessed;
};

static void param_bind_release(param_bind_t *param_bind)
{
  if (param_bind->buf) {
    safe_free(param_bind->buf);
    param_bind->sz  = 0;
    param_bind->nr  = 0;
    param_bind->ParameterValuePtr = NULL;
  }
  if (param_bind->StrLen_or_IndPtr) {
    safe_free(param_bind->StrLen_or_IndPtr);
  }
}

static int bind_param_ms(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = sizeof(int64_t);

  param_bind->ValueType              = SQL_C_SBIGINT;
  param_bind->ParameterType          = SQL_BIGINT;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int bind_param_i32(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = sizeof(int32_t);

  param_bind->ValueType              = SQL_C_LONG;
  param_bind->ParameterType          = SQL_INTEGER;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int bind_param_flt(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = sizeof(float);

  param_bind->ValueType              = SQL_C_FLOAT;
  param_bind->ParameterType          = SQL_REAL;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int bind_param_dbl(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = sizeof(double);

  param_bind->ValueType              = SQL_C_DOUBLE;
  param_bind->ParameterType          = SQL_DOUBLE;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int bind_param_str(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = 64; // TODO: dynamic and configurable

  param_bind->ValueType              = SQL_C_CHAR;
  param_bind->ParameterType          = SQL_VARCHAR;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int prepare_param_ms(param_bind_t *param_bind, size_t nr_rows)
{
  int64_t *p = (int64_t*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    *p = param_bind->ms++;
    *pInd = 0; // NOTE: null
    ++p;
    ++pInd;
  }

  return 0;
}

static int prepare_param_i32(param_bind_t *param_bind, size_t nr_rows)
{
  int32_t i32 = rand();

  int32_t *p = (int32_t*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    *p = i32;
    *pInd = 0; // NOTE: null

    i32 = rand();
    ++p;
    ++pInd;
  }

  return 0;
}

static int prepare_param_flt(param_bind_t *param_bind, size_t nr_rows)
{
  int32_t v = rand();
  if (v == 0) v = 1;

  float flt = (float)rand();

  float *p = (float*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    *p = flt / v;
    *pInd = 0; // NOTE: null

    flt = (float)rand();
    ++p;
    ++pInd;
  }

  return 0;
}

static int prepare_param_dbl(param_bind_t *param_bind, size_t nr_rows)
{
  int32_t v = rand();
  if (v == 0) v = 1;

  double dbl = (double)rand();

  double *p = (double*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    *p = dbl / v;
    *pInd = 0; // NOTE: null

    dbl = (double)rand();
    ++p;
    ++pInd;
  }

  return 0;
}

static int prepare_param_str(param_bind_t *param_bind, size_t nr_rows)
{
  int32_t i32 = rand();

  char *p = (char*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    snprintf(p, param_bind->ColumnSize, "%d", i32);
    *pInd = strlen(p);

    i32 = rand();
    p += param_bind->BufferLength;
    ++pInd;
  }

  return 0;
}

static int bind_param(param_bind_t *param_bind, size_t nr_rows)
{
  switch (param_bind->param_type) {
    case param_type_ms:
      return bind_param_ms(param_bind, nr_rows);
    case param_type_i32:
      return bind_param_i32(param_bind, nr_rows);
    case param_type_str:
      return bind_param_str(param_bind, nr_rows);
    case param_type_flt:
      return bind_param_flt(param_bind, nr_rows);
    case param_type_dbl:
      return bind_param_dbl(param_bind, nr_rows);
    default:
      DUMP("unknown param_type:[0x%x]", param_bind->param_type);
      return -1;
  }
}

static int prepare_param(param_bind_t *param_bind, size_t nr_rows)
{
  switch (param_bind->param_type) {
    case param_type_ms:
      return prepare_param_ms(param_bind, nr_rows);
    case param_type_i32:
      return prepare_param_i32(param_bind, nr_rows);
    case param_type_str:
      return prepare_param_str(param_bind, nr_rows);
    case param_type_flt:
      return prepare_param_flt(param_bind, nr_rows);
    case param_type_dbl:
      return prepare_param_dbl(param_bind, nr_rows);
    default:
      DUMP("unknown param_type:[0x%x]", param_bind->param_type);
      return -1;
  }
}

static void param_binds_release(param_binds_t *param_binds)
{
  if (!param_binds) return;
  for (size_t i=0; i<param_binds->nr; ++i) {
    param_bind_t *param_bind = param_binds->param_binds + i;
    param_bind_release(param_bind);
  }
  if (param_binds->param_binds) {
    safe_free(param_binds->param_binds);
  }
  param_binds->sz = 0;
  param_binds->nr = 0;
  if (param_binds->ParamStatusPtr) {
    safe_free(param_binds->ParamStatusPtr);
  }
}

static int param_binds_keep(param_binds_t *param_binds, size_t sz)
{
  if (sz <= param_binds->sz) return 0;
  sz = (sz + 15) / 16 * 16;
  param_bind_t *p = (param_bind_t*)realloc(param_binds->param_binds, sizeof(*p) * sz);
  if (!p) return -1;
  memset(p + param_binds->nr, 0, (sz - param_binds->nr) * sizeof(*p));
  param_binds->param_binds = p;
  param_binds->sz          = sz;
  return 0;
}

static int param_binds_add_ms(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_ms;
  param_bind->ms                     = ((int64_t)time(0)) * 1000;
  return 0;
}

static int param_binds_add_ms_str(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_ms_str;
  param_bind->ValueType              = SQL_C_CHAR;
  param_bind->ParameterType          = SQL_BIGINT;
  param_bind->ColumnSize             = 64; // TODO: dynamic or configable
  param_bind->DecimalDigits          = 0;
  return 0;
}

static int param_binds_add_str(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_str;
  return 0;
}

static int param_binds_add_i32(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_i32;
  return 0;
}

static int param_binds_add_flt(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_flt;
  return 0;
}

static int param_binds_add_dbl(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_dbl;
  return 0;
}

static int run_query_with_cols(SQLHANDLE hstmt, col_binds_t *col_binds, SQLSMALLINT nr_cols, int display, int get_data)
{
  SQLRETURN          sr;
  int r = 0;

  for (int i=0; i<nr_cols; ++i) {
    SQLLEN sql_type = 0;
    sr = CALLX_SQLColAttribute(hstmt, i+1, SQL_DESC_TYPE, NULL, 0, NULL, &sql_type);
    if (sr != SQL_SUCCESS) return -1;
    switch (sql_type) {
      case SQL_INTEGER:
        r = col_binds_add_sql_integer(col_binds);
        if (r) return -1;
        break;
      case SQL_VARCHAR:
        r = col_binds_add_sql_varchar(col_binds);
        if (r) return -1;
        break;
      case SQL_WVARCHAR:
        r = col_binds_add_sql_wvarchar(col_binds);
        if (r) return -1;
        break;
      default:
        DUMP("unknown col type:[%" PRId64 "]", (int64_t)sql_type);
        return -1;
    }

    col_bind_t *col_bind = col_binds->col_binds + i;
    sr = CALLX_SQLColAttribute(hstmt, i+1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &col_bind->attr);
    if (sr != SQL_SUCCESS) return -1;
    sr = CALLX_SQLColAttribute(hstmt, i+1, SQL_DESC_NAME, col_bind->name, sizeof(col_bind->name),  &col_bind->len, NULL);
    if (sr != SQL_SUCCESS) return -1;
    // DUMP("col_bind[%d]:%s:len[%d]:attr[%" PRId64 "]", i+1, col_bind->name, col_bind->len, col_bind->attr);
    if (!get_data) {
      sr = CALLX_SQLBindCol(hstmt, i+1, col_bind->TargetType, col_bind->TargetValuePtr, col_bind->BufferLength, &col_bind->StrLen_or_Ind);
      if (sr != SQL_SUCCESS) return -1;
    }
  }

  size_t nr_rows = 0;
  while (sr == SQL_SUCCESS) {
    sr = CALLX_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) {
      DUMP("%zd rows fetched", nr_rows);
      return 0;
    }
    if (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) {
      if (get_data) {
        for (int i=0; i<nr_cols; ++i) {
          col_bind_t *col_bind = col_binds->col_binds + i;
          sr = CALLX_SQLGetData(hstmt, i+1, col_bind->TargetType, col_bind->TargetValuePtr, col_bind->BufferLength, &col_bind->StrLen_or_Ind);
          if (sr != SQL_SUCCESS) return -1;
        }
      }
      if (display) fprintf(stderr, "row[%zd]:", nr_rows+1);
      for (int i=0; i<nr_cols; ++i) {
        col_bind_t *col_bind = col_binds->col_binds + i;
        if (display) {
          if (i) fprintf(stderr, ",");
          if (col_bind->StrLen_or_Ind == SQL_NULL_DATA) {
            fprintf(stderr, "%s[null]", col_bind->name);
          } else {
            switch (col_bind->TargetType) {
              case SQL_C_SLONG:
                fprintf(stderr, "%s[%d]", col_bind->name, *(int32_t*)col_bind->value);
                break;
              case SQL_C_CHAR:
                fprintf(stderr, "%s[%s]", col_bind->name, col_bind->value);
                break;
              default:
                fprintf(stderr, "\n");
                DUMP("unknown TargetType:[%d]", col_bind->TargetType);
                return -1;
            }
          }
        }
      }
      if (display) fprintf(stderr, "\n");
      sr = SQL_SUCCESS;
      ++nr_rows;
      continue;
    }
  }
  DUMP("%zd rows returned", nr_rows);

  return sr == SQL_SUCCESS ? 0 : -1;
}

static int run_query_with_sql(handles_t *handles, col_binds_t *col_binds, const char *query, int display, int get_data)
{
  SQLRETURN          sr;
  int r = 0;

  if (handles->hstmt == SQL_NULL_HSTMT) {
    sr = CALLX_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
    if (sr != SQL_SUCCESS) return -1;
  }

  SQLHANDLE hstmt = handles->hstmt;

  SQLSMALLINT        nr_cols = 0;

  sr = CALLX_SQLExecDirect(hstmt, (SQLCHAR*)query, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  sr = CALLX_SQLNumResultCols(hstmt, &nr_cols);
  if (sr != SQL_SUCCESS) return -1;
  if (nr_cols == 0) {
    SQLLEN nr_rows_affected = 0;
    sr = CALLX_SQLRowCount(hstmt, &nr_rows_affected);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("%zd row(s) affected", (size_t)nr_rows_affected);
    return 0;
  }

  r = col_binds_keep(col_binds, nr_cols);
  if (r) {
    DUMP("out of memory");
    return -1;
  }

  return run_query_with_cols(hstmt, col_binds, nr_cols, display, get_data);
}

static int run_query_with_col_binds(handles_t *handles, col_binds_t *col_binds, int i, int argc, char *argv[])
{
  int r = 0;
  int display = 0;
  int get_data = 0;

  for (; i<argc; ++i) {
    const char *arg = argv[i];
    DUMP("arg:%s", arg);
    if (0 == strcasecmp(arg, "--")) break;
    if (0 == strcasecmp(arg, "--display")) {
      display = 1;
      continue;
    }
    if (0 == strcasecmp(arg, "--get_data")) {
      get_data = 1;
      continue;
    }
    r = run_query_with_sql(handles, col_binds, argv[i], display, get_data);
    if (r) return -1;
  }
  return i;
}

static int run_query(handles_t *handles, int i, int argc, char *argv[])
{
  col_binds_t       col_binds = {0};
  int r = run_query_with_col_binds(handles, &col_binds, i, argc, argv);
  col_binds_release(&col_binds);
  return r;
}

static int run_insert_with_options(handles_t *handles, const char *sql, size_t nr_rows, size_t nr_batch, param_binds_t *param_binds)
{
  SQLRETURN          sr;
  int r = 0;

  param_binds->ParamStatusPtr = (SQLUSMALLINT*)calloc(nr_batch, sizeof(*param_binds->ParamStatusPtr));
  if (!param_binds->ParamStatusPtr) {
    DUMP("out of memory");
    return -1;
  }
  param_binds->ParamsProcessed = 0;

  for (size_t i=0; i<param_binds->nr; ++i) {
    param_bind_t *param_bind = param_binds->param_binds + i;
    r = bind_param(param_bind, nr_batch);
    if (r) return -1;
  }

  if (handles->hstmt == SQL_NULL_HSTMT) {
    sr = CALLX_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
    if (sr != SQL_SUCCESS) return -1;
  }

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  sr = CALLX_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an array in which to return the status of each set of
  // parameters.
  sr = CALLX_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_binds->ParamStatusPtr, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  sr = CALLX_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &param_binds->ParamsProcessed, 0);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE hstmt = handles->hstmt;
  sr = CALLX_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT nr_params = 0;
  sr = CALLX_SQLNumParams(hstmt, &nr_params);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i=0; i<nr_params; ++i) {
    SQLSMALLINT     DataType;
    SQLULEN         ParameterSize;
    SQLSMALLINT     DecimalDigits;
    SQLSMALLINT     Nullable;
    sr = CALLX_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("DataType:[0x%x]%s;ParameterSize:%" PRIu64 ";DecimalDigits:%d;Nullable:%d",
        DataType, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, Nullable);
  }

  for (size_t i=0; i<param_binds->nr; ++i) {
    param_bind_t *param_bind = param_binds->param_binds + i;
    sr = CALLX_SQLBindParameter(
        handles->hstmt,
        (SQLUSMALLINT)param_bind->idx+1,
        SQL_PARAM_INPUT,
        param_bind->ValueType,
        param_bind->ParameterType,
        param_bind->ColumnSize,
        param_bind->DecimalDigits,
        param_bind->ParameterValuePtr,
        param_bind->BufferLength,
        param_bind->StrLen_or_IndPtr);
    if (sr != SQL_SUCCESS) return -1;
  }

  size_t sz_total = 0;

  for (size_t j=0; j<nr_rows; j+=nr_batch) {
    size_t nr = nr_rows - j;
    if (nr > nr_batch) nr = nr_batch;

    for (size_t i=0; i<param_binds->nr; ++i) {
      param_bind_t *param_bind = param_binds->param_binds + i;
      r = prepare_param(param_bind, nr);
      if (r) return -1;
    }

    // Specify the number of elements in each parameter array.
    sr = CALLX_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr, 0);
    if (sr != SQL_SUCCESS) return -1;

    sr = CALLX_SQLExecute(handles->hstmt);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

    sz_total += param_binds->ParamsProcessed;
  }

  DUMP("processed:%" PRId64 "", (int64_t)sz_total);

  return 0;
}

static int run_insert_with_param_binds(handles_t *handles, param_binds_t *param_binds, int i, int argc, char *argv[])
{
  int r = 0;

  size_t          nr_rows   = 0;
  size_t          nr_batch  = 0;
  const char     *sql       = NULL;
  char           *endptr    = NULL;
  long long       num       = 0;

  for (; i<argc; ++i) {
    const char *arg = argv[i];
    DUMP("arg:%s", arg);
    if (0 == strcasecmp(arg, "--")) break;
    if (0 == strcasecmp(arg, "--rows")) {
      if (++i >= argc) {
        DUMP("<rows> is expected after --rows");
        return -1;
      }

      errno = 0;
      num = strtoll(argv[i], &endptr, 0);
      if (endptr == argv[i] || *endptr != '\0' || errno != 0) {
        DUMP("error: invalid rows number '%s'", argv[i]);
        return -1;
      }

      nr_rows = (size_t)num;
      continue;
    }
    if (0 == strcasecmp(arg, "--batch")) {
      if (++i >= argc) {
        DUMP("<batch> is expected after --batch");
        return -1;
      }

      errno = 0;
      num = strtoll(argv[i], &endptr, 0);
      if (endptr == argv[i] || *endptr != '\0' || errno != 0) {
        DUMP("error: invalid batch number '%s'", argv[i]);
        return -1;
      }

      nr_batch = (size_t)num;
      continue;
    }
    if (0 == strcasecmp(arg, "--sql")) {
      if (++i >= argc) {
        DUMP("<sql> is expected after --sql");
        return -1;
      }
      sql = argv[i];
      continue;
    }
    if (0 == strcasecmp(arg, "--params")) {
      for (++i; i<argc; ++i) {
        arg = argv[i];
        if (0 == strcasecmp(arg, "--")) {
          --i;
          break;
        }
        if (0 == strcasecmp(arg, "ms")) {
          r = param_binds_add_ms(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "ms_str")) {
          r = param_binds_add_ms_str(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "str")) {
          r = param_binds_add_str(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "i32")) {
          r = param_binds_add_i32(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "flt")) {
          r = param_binds_add_flt(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "dbl")) {
          r = param_binds_add_dbl(param_binds);
          if (r) return -1;
          continue;
        }
        DUMP("unknown argument:%s", arg);
        return -1;
      }
      continue;
    }
    DUMP("unknown argument:%s", arg);
    return -1;
  }

  if (nr_batch == 0) nr_batch = 1024000;

  r = run_insert_with_options(handles, sql, nr_rows, nr_batch, param_binds);
  if (r) return -1;

  return i;
}

static int run_insert(handles_t *handles, int i, int argc, char *argv[])
{
  param_binds_t   param_binds = {0};
  i = run_insert_with_param_binds(handles, &param_binds, i, argc, argv);
  param_binds_release(&param_binds);

  return i;
}

static int run_cmd(handles_t *handles, const char *cmd)
{
  SQLRETURN          sr;
  int r = 0;

  if (handles->hstmt == SQL_NULL_HSTMT) {
    sr = CALLX_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
    if (sr != SQL_SUCCESS) return -1;
  }

  SQLHANDLE hstmt = handles->hstmt;

  sr = CALLX_SQLExecDirect(hstmt, (SQLCHAR*)cmd, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  return r;
}

static void handles_release(handles_t *handles)
{
  if (handles->hstmt != SQL_NULL_HSTMT) {
    CALLX_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
    handles->hstmt = SQL_NULL_HSTMT;
  }

  if (handles->hdbc != SQL_NULL_HDBC) {
    CALLX_SQLDisconnect(handles->hdbc);
    CALLX_SQLFreeHandle(SQL_HANDLE_DBC, handles->hdbc);
    handles->hdbc = SQL_NULL_HDBC;
  }

  if (handles->henv != SQL_NULL_HENV) {
    CALLX_SQLFreeHandle(SQL_HANDLE_ENV, handles->henv);
    handles->henv = SQL_NULL_HENV;
  }
}

static void handles_disconnect(handles_t *handles)
{
  if (handles->hstmt != SQL_NULL_HSTMT) {
    CALLX_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
    handles->hstmt = SQL_NULL_HSTMT;
  }

  if (handles->hdbc != SQL_NULL_HDBC) {
    CALLX_SQLDisconnect(handles->hdbc);
  }
}

static int handles_connect(handles_t *handles, const char *dsn, const char *user, const char *pass, const char *conn_str)
{
  SQLRETURN          sr;

  if (!dsn && !conn_str) {
    DUMP("either --dsn or --conn_str is required");
    return -1;
  }

  handles_disconnect(handles);
  if (handles->henv == SQL_NULL_HANDLE) {
    sr = CALLX_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->henv);
    if (sr != SQL_SUCCESS) return -1;
    sr = CALLX_SQLSetEnvAttr(handles->henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);
    if (sr != SQL_SUCCESS) return -1;
  }

  if (handles->hdbc == SQL_NULL_HANDLE) {
    sr = CALLX_SQLAllocHandle(SQL_HANDLE_DBC, handles->henv, &handles->hdbc);
    if (sr != SQL_SUCCESS) return -1;
  }

  sr = CALLX_SQLSetConnectAttr(handles->hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)0, 0);
  if (sr != SQL_SUCCESS) return -1;

  if (dsn) sr = CALLX_SQLConnect(handles->hdbc, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)user, SQL_NTS, (SQLCHAR*)pass, SQL_NTS);
  else     sr = CALLX_SQLDriverConnect(handles->hdbc, NULL, (SQLCHAR*)conn_str, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  return 0;
}

static int process(handles_t *handles, int argc, char *argv[])
{
  int r = 0;
  const char *dsn = NULL;
  const char *conn_str = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *app   = argv[0];
  int conn_changed = 1;
  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    DUMP("arg:%s", arg);
    if (0 == strcasecmp(arg, "-h")) {
      usage(app);
      return 0;
    }
    if (0 == strcasecmp(arg, "--")) continue;
    if (0 == strcasecmp(arg, "--dsn")) {
      if (++i >= argc) {
        DUMP("<dsn> is expected after --dsn");
        return -1;
      }
      conn_changed = 1;
      dsn = argv[i];
      conn_str = NULL;
      continue;
    }
    if (0 == strcasecmp(arg, "--conn_str")) {
      if (++i >= argc) {
        DUMP("<conn_str> is expected after --conn_str");
        return -1;
      }
      conn_changed = 1;
      conn_str = argv[i];
      dsn = NULL;
      user = NULL;
      pass = NULL;
      continue;
    }
    if (0 == strcasecmp(arg, "--user")) {
      if (++i >= argc) {
        DUMP("<user> is expected after --user");
        return -1;
      }
      conn_changed = 1;
      user = argv[i];
      continue;
    }
    if (0 == strcasecmp(arg, "--pass")) {
      if (++i >= argc) {
        DUMP("<pass> is expected after --pass");
        return -1;
      }
      conn_changed = 1;
      pass = argv[i];
      continue;
    }
    if (0 == strcasecmp(arg, "conn")) {
      r = handles_connect(handles, dsn, user, pass, conn_str);
      if (r) return -1;
      conn_changed = 0;
      continue;
    }
    if (0 == strcasecmp(arg, "cmd")) {
      if (++i >= argc) {
        DUMP("<query> is expected after query");
        return -1;
      }
      if (conn_changed) {
        r = handles_connect(handles, dsn, user, pass, conn_str);
        if (r) return -1;
        conn_changed = 0;
      }
      for (; i<argc; ++i) {
        if (0 == strcasecmp(argv[i], "--")) break;
        r = run_cmd(handles, argv[i]);
        if (r) return -1;
      }
      continue;
    }
    if (0 == strcasecmp(arg, "query")) {
      if (++i >= argc) {
        DUMP("<query> is expected after query");
        return -1;
      }
      if (conn_changed) {
        r = handles_connect(handles, dsn, user, pass, conn_str);
        if (r) return -1;
        conn_changed = 0;
      }
      i = run_query(handles, i, argc, argv);
      if (i < 0) return -1;
      continue;
    }
    if (0 == strcasecmp(arg, "insert")) {
      if (conn_changed) {
        r = handles_connect(handles, dsn, user, pass, conn_str);
        if (r) return -1;
        conn_changed = 0;
      }
      i = run_insert(handles, i+1, argc, argv);
      if (i < 0) return -1;
      continue;
    }
    DUMP("unknown argument:%s", arg);
    return -1;
  }

  return 0;
}

int main(int argc, char *argv[])
{
  handles_t handles = {
    .henv    = SQL_NULL_HENV,
    .hdbc    = SQL_NULL_HDBC,
    .hstmt   = SQL_NULL_HSTMT,
  };

  srand((unsigned int)time(0));

  int r = process(&handles, argc, argv);

  handles_release(&handles);

  if (r == 0) DUMP("-=Done=-");

  return !!r;
}

