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

#include <taos.h>

#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

static void _binds_release(TAOS_MULTI_BIND *binds, size_t nr)
{
  if (!binds) return;
  for (size_t i=0; i<nr; ++i) {
    TAOS_MULTI_BIND *p = binds + i;
    SFREE(p->buffer);
    SFREE(p->length);
    SFREE(p->is_null);
  }
}

static void _binds_free(TAOS_MULTI_BIND *binds, size_t nr)
{
  if (!binds) return;
  _binds_release(binds, nr);
  free(binds);
}

static int _bind_timestamp_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_TIMESTAMP;
  uintptr_t buffer_length        = sizeof(int64_t);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  time_t t0; time(&t0);
  int64_t v = t0 * 1000;
  int64_t *base = (int64_t*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = v + i;
    // E("timestamp:[%zd]", base[i]);
  }

  return 0;
}

static int _bind_bigint_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_BIGINT;
  uintptr_t buffer_length        = sizeof(int64_t);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  int64_t *base = (int64_t*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = rand();
    // E("bigint:[%zd]", base[i]);
  }

  return 0;
}

static int _bind_int_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_INT;
  uintptr_t buffer_length        = sizeof(int32_t);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  int32_t *base = (int32_t*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = rand();
    // E("int:[%zd]", base[i]);
  }

  return 0;
}

static int _bind_smallint_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_SMALLINT;
  uintptr_t buffer_length        = sizeof(int16_t);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  int16_t *base = (int16_t*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = rand();
    // E("smallint:[%zd]", base[i]);
  }

  return 0;
}

static int _bind_tinyint_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_TINYINT;
  uintptr_t buffer_length        = sizeof(int8_t);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  int8_t *base = (int8_t*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = rand();
    // E("tinyint:[%zd]", base[i]);
  }

  return 0;
}

static int _bind_double_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_DOUBLE;
  uintptr_t buffer_length        = sizeof(double);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  double *base = (double*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = rand();
    base[i] /= (double)rand();
    // E("double:[%lg]", base[i]);
  }

  return 0;
}

static int _bind_float_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_FLOAT;
  uintptr_t buffer_length        = sizeof(float);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;


  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  float *base = (float*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = (float)rand();
    base[i] /= (float)rand();
    // E("float:[%lg]", base[i]);
  }

  return 0;
}

static int _bind_varchar_prepare(TAOS_MULTI_BIND *bind, size_t rows, size_t len)
{
  int       buffer_type          = TSDB_DATA_TYPE_VARCHAR;
  uintptr_t buffer_length        = len + 1;
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = calloc(rows, sizeof(*length));
  char     *is_null              = NULL;
  int       num                  = (int)rows;

  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer || !bind->length) {
    E("oom");
    return -1;
  }

  char *base = (char*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    int v = rand();
    snprintf(base + i * bind->buffer_length, bind->buffer_length, "%0*d", (int)(bind->buffer_length - 1), v);
    // E("varchar:[%s]", base + i * bind->buffer_length);
    bind->length[i] = (int32_t)(bind->buffer_length - 1);
  }

  return 0;
}

static int _bind_nchar_prepare(TAOS_MULTI_BIND *bind, size_t rows, size_t len)
{
  int       buffer_type          = TSDB_DATA_TYPE_NCHAR;
  uintptr_t buffer_length        = len + 1;
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = calloc(rows, sizeof(*length));
  char     *is_null              = NULL;
  int       num                  = (int)rows;

  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer || !bind->length) {
    E("oom");
    return -1;
  }

  char *base = (char*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    int v = rand();
    snprintf(base + i * bind->buffer_length, bind->buffer_length, "%0*d", (int)(bind->buffer_length - 1), v);
    // E("varchar:[%s]", base + i * bind->buffer_length);
    bind->length[i] = (int32_t)(bind->buffer_length - 1);
  }

  return 0;
}

typedef struct taos_conn_cfg_s               taos_conn_cfg_t;
struct taos_conn_cfg_s {
  const char                    *ip;
  const char                    *usr;
  const char                    *pwd;
  const char                    *db;
  uint16_t                       port;
  size_t                         rows;
  size_t                         cols;

  const char                    *tsdb_names[64];
  int                            tsdb_types[64];
  int                            tsdb_col_sizes[64];


  const char                    *drop;
  char                           create[4096];
  char                           insert[4096];
};

static int _binds_prepare_v(TAOS_MULTI_BIND *binds, taos_conn_cfg_t *cfg)
{
  int r = 0;
  for (size_t i=0; i<cfg->cols; ++i) {
    int tsdb_type = cfg->tsdb_types[i];
    switch (tsdb_type) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        r = _bind_timestamp_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_VARCHAR:
        r = _bind_varchar_prepare(binds + i, cfg->rows, cfg->tsdb_col_sizes[i]);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_NCHAR:
        r = _bind_nchar_prepare(binds + i, cfg->rows, cfg->tsdb_col_sizes[i]);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_BIGINT:
        r = _bind_bigint_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_INT:
        r = _bind_int_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        r = _bind_smallint_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_TINYINT:
        r = _bind_tinyint_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        r = _bind_double_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_FLOAT:
        r = _bind_float_prepare(binds + i, cfg->rows);
        if (r) return -1;
        break;
      default:
        E("TSDB_DATA_TYPE_xxx[%d/0x%x] not implemented yet", tsdb_type, tsdb_type);
        return -1;
    }
  }

  return 0;
}

static int _bind_and_run(TAOS_STMT *stmt, taos_conn_cfg_t *cfg, TAOS_MULTI_BIND *binds)
{
  int r = 0;

  const char *sql = cfg->insert;

  r = taos_stmt_bind_param_batch(stmt, binds);
  if (r) {
    E("taos_stmt_bind_param_batch failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  r = taos_stmt_add_batch(stmt);
  if (r) {
    E("taos_stmt_add_batch failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  r = taos_stmt_execute(stmt);
  if (r) {
    E("taos_stmt_execute failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  return 0;
}

static int _prepare_and_run(TAOS_STMT *stmt, taos_conn_cfg_t *cfg, TAOS_MULTI_BIND *binds)
{
  int r = 0;
  const char *sql = cfg->insert;

  r = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) {
    E("taos_stmt_prepare failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  TAOS_MULTI_BIND this_binds[64];
  size_t nr_this_binds = sizeof(this_binds) / sizeof(this_binds[0]);
  if (cfg->cols > nr_this_binds) {
    E("too many cols %zd, no greater than %zd cols is permitted", cfg->cols, nr_this_binds);
    return -1;
  }
  for (size_t i=0; i<cfg->rows; i += INT16_MAX) {
    size_t num = cfg->rows - i;
    if (num > INT16_MAX) num = INT16_MAX;
    for (size_t j=0; j<cfg->cols; ++j) {
      this_binds[j] = binds[j];
      this_binds[j].num = (int)num;
      this_binds[j].buffer_type    = binds[j].buffer_type;
      this_binds[j].buffer_length  = binds[j].buffer_length;
      this_binds[j].buffer         = binds[j].buffer ? (((char*)binds[j].buffer) + binds[j].buffer_length * i) : NULL;
      this_binds[j].length         = binds[j].length ? (binds[j].length + i) : NULL;
      this_binds[j].is_null        = binds[j].is_null ? (binds[j].is_null + i) : NULL;
    }
    r = _bind_and_run(stmt, cfg, this_binds);
    if (r) return -1;
  }

  return 0;
}

static int _run_prepare(TAOS_STMT *stmt, taos_conn_cfg_t *cfg, TAOS_MULTI_BIND **binds)
{
  int r = 0;

  TAOS_MULTI_BIND *p = (TAOS_MULTI_BIND*)calloc(cfg->cols, sizeof(*p));
  if (!p) return -1;
  *binds = p;

  r = _binds_prepare_v(*binds, cfg);

  if (r) return -1;

  r = _prepare_and_run(stmt, cfg, *binds);

  if (r) return -1;

  return 0;
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s --ip <ip> --usr <usr> --pwd <pwd> --db <db> --port <port> --rows <rows> [field_desc]...\n"
                  "  running benchmark\n",
                  arg0, arg0);
}

static int _execute(TAOS *taos, const char *sql)
{
  TAOS_RES *res = NULL;
  int e = 0;

  res = taos_query(taos, sql);
  e = taos_errno(res);
  if (e) {
    E("taos_query failed for `%s`:[%d/0x%x]%s", sql, e, e, taos_errstr(res));
  }
  if (res) {
    taos_free_result(res);
    res = NULL;
  }
  if (e) return -1;

  return 0;
}

static int do_benchmark_case0(TAOS_STMT *stmt, taos_conn_cfg_t *cfg)
{
  int r = 0;

  TAOS_MULTI_BIND *binds = NULL;
  r = _run_prepare(stmt, cfg, &binds);

  if (binds) {
    _binds_free(binds, cfg->cols);
    binds = NULL;
  }

  return r ? -1 : 0;
}

static int benchmark_case0(TAOS *taos, TAOS_STMT *stmt, taos_conn_cfg_t *cfg)
{
  int r = 0;

  r = _execute(taos, cfg->drop);
  if (r) return -1;
  r = _execute(taos, cfg->create);
  if (r) return -1;

  PROFILE(r = do_benchmark_case0(stmt, cfg));

  return r ? -1 : 0;
}

static int _parse_tsdb_type(const char *tsdb, int *tsdb_type, int *col_size)
{
  if (strcmp(tsdb, "timestamp") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_TIMESTAMP;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(tsdb, "bigint") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_BIGINT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(tsdb, "int") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_INT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(tsdb, "smallint") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_SMALLINT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(tsdb, "tinyint") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_TINYINT;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(tsdb, "double") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_DOUBLE;
    *col_size  = 0;
    return 0;
  }

  if (strcmp(tsdb, "float") == 0) {
    *tsdb_type = TSDB_DATA_TYPE_FLOAT;
    *col_size  = 0;
    return 0;
  }

  int n = 0;

  n = sscanf(tsdb, "varchar(%d)", col_size);
  if (n == 1) {
    *tsdb_type = TSDB_DATA_TYPE_VARCHAR;
    return 0;
  }

  n = sscanf(tsdb, "nchar(%d)", col_size);
  if (n == 1) {
    *tsdb_type = TSDB_DATA_TYPE_NCHAR;
    return 0;
  }

  E("unknown type `%s`", tsdb);
  return -1;
}

static int _gen_sqls(taos_conn_cfg_t *cfg)
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
    int tsdb_type = cfg->tsdb_types[i];
    const char *s = cfg->tsdb_names[i];
    if (!s) {
      E("unknown tsdb_type [%d]", tsdb_type);
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

static int _add_tsdb_type(taos_conn_cfg_t *cfg, const char *tsdb)
{
  int r = 0;

  int tsdb_type = 0;
  int col_size = 0;
  r = _parse_tsdb_type(tsdb, &tsdb_type, &col_size);
  if (r) return -1;

  const size_t nr_cols = sizeof(cfg->tsdb_types) / sizeof(cfg->tsdb_types[0]);
  if (cfg->cols >= nr_cols) {
    E("%s specified, but # of cols overflow:%zd >= %zd", tsdb, cfg->cols, nr_cols);
    return -1;
  }

  cfg->tsdb_names[cfg->cols] = tsdb;
  cfg->tsdb_types[cfg->cols] = tsdb_type;
  cfg->tsdb_col_sizes[cfg->cols] = col_size;
  cfg->cols += 1;

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

  taos_conn_cfg_t cfg = {0};
  cfg.db = "foo";
  cfg.rows = 32767; // INT16_MAX

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "--ip") == 0) {
      ++i;
      if (i>=argc) {
        E("<ip> is expected after `--ip`, but got ==null==");
        return -1;
      }
      cfg.ip = argv[i];
      continue;
    }
    if (strcmp(arg, "--usr") == 0) {
      ++i;
      if (i>=argc) {
        E("<usr> is expected after `--usr`, but got ==null==");
        return -1;
      }
      cfg.usr = argv[i];
      continue;
    }
    if (strcmp(arg, "--pwd") == 0) {
      ++i;
      if (i>=argc) {
        E("<pwd> is expected after `--pwd`, but got ==null==");
        return -1;
      }
      cfg.pwd = argv[i];
      continue;
    }
    if (strcmp(arg, "--db") == 0) {
      ++i;
      if (i>=argc) {
        E("<db> is expected after `--db`, but got ==null==");
        return -1;
      }
      cfg.db = argv[i];
      continue;
    }
    if (strcmp(arg, "--port") == 0) {
      ++i;
      if (i>=argc) {
        E("<port> is expected after `--port`, but got ==null==");
        return -1;
      }
      char *end = NULL;
      errno = 0;
      long long port = strtoll(argv[i], &end, 0);
      if (end && *end) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port == LLONG_MIN && errno == ERANGE) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port == LLONG_MAX && errno == ERANGE) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port == 0 && errno == EINVAL) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port < 0 || port > UINT16_MAX) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      cfg.port = (uint16_t)port;
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

    r = _add_tsdb_type(&cfg, argv[i]);
    if (r) return -1;
  }

  if (cfg.cols == 0) {
    r = _add_tsdb_type(&cfg, "timestamp");
    if (r) return -1;
    r = _add_tsdb_type(&cfg, "varchar(20)");
    if (r) return -1;
  }

  r = _gen_sqls(&cfg);
  if (r) return -1;

#ifdef _WIN32                    /* { */
  srand((unsigned int)time(NULL));
#else                            /* }{ */
  srand(time(NULL));
#endif                           /* } */

  TAOS *taos = taos_connect(cfg.ip, cfg.usr, cfg.pwd, cfg.db, cfg.port);
  if (!taos) {
    E("taos_connect failed:[%d/0x%x]%s", taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  TAOS_STMT *stmt = taos_stmt_init(taos);
  do {
    if (!stmt) {
      E("taos_stmt_init failed:[%d/0x%x]%s", taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
      r = -1;
      break;
    }
    r = benchmark_case0(taos, stmt, &cfg);
  } while (0);

  if (stmt) {
    taos_stmt_close(stmt);
    stmt = NULL;
  }

  taos_close(taos);
  taos = NULL;

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return r ? 1 : 0;
}

