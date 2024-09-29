/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


#include "taos.h"   // NOTE: this is intentional, "taos.h" rather than <taos.h>

#include <inttypes.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LOGE(fmt, ...) fprintf(stderr, "" fmt "\n", ##__VA_ARGS__)
#define DBGE(fmt, ...) if (0) fprintf(stderr, "bm.c[%d]:%s():" fmt "\n", __LINE__, __FILE__, ##__VA_ARGS__)

#define CHK_TAOS(taos, fmt, ...) do {                                  \
  if (taos) break;                                                     \
  int _e = taos_errno(NULL);                                           \
  if (!_e) break;                                                      \
  fprintf(stderr, "bm.c[%d]:%s():[0x%08x]%s:" fmt "\n",                \
      __LINE__, __func__,                                              \
      _e, taos_errstr(NULL),                                           \
      ##__VA_ARGS__);                                                  \
} while (0)

#define CHK_RES(res, e, fmt, ...) do {                                 \
  if (!e) break;                                                       \
  fprintf(stderr, "bm.c[%d]:%s():[0x%08x]%s:" fmt "\n",                \
      __LINE__, __func__,                                              \
      e, taos_errstr(res),                                             \
      ##__VA_ARGS__);                                                  \
} while (0)

#define CHK_STMT(stmt, e, fmt, ...) do {                               \
  if (!e) break;                                                       \
  fprintf(stderr, "bm.c[%d]:%s():[0x%08x]%s:" fmt "\n",                \
      __LINE__, __func__,                                              \
      e, stmt ? taos_stmt_errstr(stmt) : taos_errstr(NULL),            \
      ##__VA_ARGS__);                                                  \
} while (0)

static int run_sql(TAOS *conn, const char *sql)
{
  TAOS_RES *res = taos_query(conn, sql);
  int e = taos_errno(res);
  CHK_RES(res, e, "taos_query(%s) failed", sql);
  if (!res) return -1;
  taos_free_result(res);
  if (e) return -1;
  return 0;
}

static int run_sqls(TAOS *conn, const char **sqls, size_t nr_sqls)
{
  int r = 0;

  for (size_t i=0; i<nr_sqls; ++i) {
    const char *sql = sqls[i];
    r = run_sql(conn, sql);
    if (r) return -1;
  }

  return 0;
}

#define EXEC_COL_END          ((const char*)-1)
#define SAFE_FREE(x) if (x) { free(x); x = NULL; }

static int64_t get_base_ts(void)
{
  static int init = 0;
  static int64_t base_ts = 0;
  if (init == 0) {
    base_ts = time(NULL);
    base_ts *= 1000;
    init = 1;
  }

  return base_ts;
}

static int64_t get_next_ms(void)
{
  static int64_t tick = 0;

  return get_base_ts() + tick++;
}

typedef struct param_meta_s             param_meta_t;
struct param_meta_s {
  int        type;
  int        len;
};

static void release_binds(TAOS_MULTI_BIND *binds, size_t nr_binds)
{
  if (!binds) return;
  for (size_t i=0; i<nr_binds; ++i) {
    TAOS_MULTI_BIND *bind = binds + i;
    SAFE_FREE(bind->buffer);
    SAFE_FREE(bind->length);
    SAFE_FREE(bind->is_null);
  }
}

static int gen_col(TAOS_MULTI_BIND *bind, int i_row)
{
  char    *data    = ((char*)(bind->buffer)) + bind->buffer_length * i_row;
  int32_t *length  = bind->length + i_row;
  char    *is_null = bind->is_null + i_row;

  *is_null = 0;

  switch (bind->buffer_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      *(int64_t*)data = get_next_ms();
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      snprintf(data, bind->buffer_length, "n%d", i_row);
      *length = strlen(data);
      break;
    case TSDB_DATA_TYPE_INT:
      *(int32_t*)data = i_row;
      break;
    default:
      LOGE("[%d]%s:not implemented yet", bind->buffer_type, taos_data_type(bind->buffer_type));
      return -1;
  }

  return 0;
}

static int gen_tbname_tags(TAOS_MULTI_BIND *bind, int i_tb, int i_row, int tb_rows)
{
  int r = 0;

  char    *data    = ((char*)(bind->buffer)) + bind->buffer_length * i_row;
  int32_t *length  = bind->length + i_row;
  char    *is_null = bind->is_null + i_row;

  *is_null = 0;

  switch (bind->buffer_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      *(int64_t*)data = get_base_ts() + i_tb;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      snprintf(data, bind->buffer_length, "n%d", i_tb);
      *length = strlen(data);
      break;
    case TSDB_DATA_TYPE_INT:
      *(int32_t*)data = i_tb;
      break;
    default:
      LOGE("[%d]%s:not implemented yet", bind->buffer_type, taos_data_type(bind->buffer_type));
      return -1;
  }

  for (int i=1; i<tb_rows; ++i) {
    int idx = i_row + i;

    char    *dst_data    = ((char*)(bind->buffer)) + bind->buffer_length * idx;
    int32_t *dst_length  = bind->length + idx;
    char    *dst_is_null = bind->is_null + idx;

    memcpy(dst_data, data, bind->buffer_length);
    *dst_length  = *length;
    *dst_is_null = *is_null;
  }

  return 0;
}

typedef struct binds_s        binds_t;
struct binds_s {
  TAOS_MULTI_BIND      *binds;
  int                   nr_binds;
  int                   cap_binds;
  int                  *cap_buffer;
  int                  *cap_length;
  int                  *cap_is_null;
};

static void binds_release(binds_t *binds)
{
  if (!binds) return;
  for (int i=0; i<binds->cap_binds; ++i) {
    TAOS_MULTI_BIND *bind = binds->binds + i;
    SAFE_FREE(bind->buffer);
    SAFE_FREE(bind->length);
    SAFE_FREE(bind->is_null);
  }
  SAFE_FREE(binds->cap_buffer);
  SAFE_FREE(binds->cap_length);
  SAFE_FREE(binds->cap_is_null);
  SAFE_FREE(binds->binds);
}

static int binds_realloc(binds_t *binds, int nr_binds)
{
  if (nr_binds > binds->cap_binds) {
    int cap_binds        = (nr_binds + 15) / 16 * 16;

    TAOS_MULTI_BIND *buffer       = (TAOS_MULTI_BIND*)realloc(binds->binds, cap_binds * sizeof(*buffer));
    if (!buffer) {
      LOGE("out of memory");
      return -1;
    }
    memset(buffer + binds->cap_binds, 0, (cap_binds - binds->cap_binds) * sizeof(*buffer));
    binds->binds = buffer;

    int *cap_buffer = (int*)realloc(binds->cap_buffer, cap_binds * sizeof(*cap_buffer));
    if (!cap_buffer) {
      LOGE("out of memory");
      return -1;
    }
    memset(cap_buffer + binds->cap_binds, 0, (cap_binds - binds->cap_binds) * sizeof(*cap_buffer));
    binds->cap_buffer = cap_buffer;

    int *cap_length = (int*)realloc(binds->cap_length, cap_binds * sizeof(*cap_length));
    if (!cap_length) {
      LOGE("out of memory");
      return -1;
    }
    memset(cap_length + binds->cap_binds, 0, (cap_binds - binds->cap_binds) * sizeof(*cap_length));
    binds->cap_length = cap_length;

    int *cap_is_null = (int*)realloc(binds->cap_is_null, cap_binds * sizeof(*cap_is_null));
    if (!cap_is_null) {
      LOGE("out of memory");
      return -1;
    }
    memset(cap_is_null + binds->cap_binds, 0, (cap_binds - binds->cap_binds) * sizeof(*cap_is_null));
    binds->cap_is_null = cap_is_null;

    binds->nr_binds  = nr_binds;
    binds->cap_binds = cap_binds;
  }

  return 0;
}

static int binds_realloc_by_rows(binds_t *binds, int col)
{
  int r = 0;

  TAOS_MULTI_BIND *p      = binds->binds + col;

  int bytes = 0;

  bytes = p->num * p->buffer_length;
  if (bytes > binds->cap_buffer[col]) {
    char *buffer = (char*)realloc(p->buffer, bytes);
    if (!buffer) {
      LOGE("out of memory");
      return -1;
    }
    p->buffer = buffer;
    binds->cap_buffer[col] = bytes;
  }

  bytes = p->num * sizeof(*p->length);
  if (bytes > binds->cap_length[col]) {
    char *length = (char*)realloc(p->length, bytes);
    if (!length) {
      LOGE("out of memory");
      return -1;
    }
    p->length = (int32_t*)length;
    binds->cap_length[col] = bytes;
  }

  bytes = p->num * sizeof(*p->is_null);
  if (bytes > binds->cap_is_null[col]) {
    char *is_null = (char*)realloc(p->is_null, bytes);
    if (!is_null) {
      LOGE("out of memory");
      return -1;
    }
    p->is_null = is_null;
    binds->cap_is_null[col] = bytes;
  }

  return 0;
}

static int gen_binds(binds_t *binds, int *rows, size_t *total_rows, param_meta_t *meta, size_t nr_meta, int tbname, int nr_tags, int nr_tbls, int nr_records)
{
  int r = 0;

  TAOS_MULTI_BIND *tags  = NULL;
  TAOS_MULTI_BIND *cols  = NULL;

  int nr_cols = nr_meta - !!tbname - nr_tags;
  if (nr_cols <= 0) {
    LOGE("invalid arg:nr_meta[%zd]-tbname[%d]-nr_tags[%d] == nr_cols[%d]", nr_meta, !!tbname, nr_tags, nr_cols);
    return -1;
  }

  if ((!!tbname) ^ (!!nr_tags)) {
    LOGE("not implemented yet");
    return -1;
  }

  if (tbname && meta->type != TSDB_DATA_TYPE_VARCHAR) {
    LOGE("not implemented yet");
    return -1;
  }

  size_t nr_rows = 0;
  for (int i=0; i<nr_tbls; ++i) {
    if (nr_tbls > 1) {
      rows[i] = (rand() % nr_records) + 1;
    } else {
      rows[i] = nr_records;
    }
    nr_rows += rows[i];
  }

  for (int i=0; i<nr_meta; ++i) {
    TAOS_MULTI_BIND *bind = binds->binds + i;
    param_meta_t    *pm   = meta + i;
    bind->buffer_type = pm->type;
    bind->num         = nr_rows;
    switch (bind->buffer_type) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        bind->buffer_length       = sizeof(int64_t);
        break;
      case TSDB_DATA_TYPE_VARCHAR:
        if (pm->len < 1) {
          LOGE("param[#%d] buffer_length missing or less than 1", i+1);
          return -1;
        }
        bind->buffer_length = pm->len + 1;
        break;
      case TSDB_DATA_TYPE_INT:
        bind->buffer_length       = sizeof(int32_t);
        break;
      default:
        LOGE("[%d]%s:not implemented yet", pm->type, taos_data_type(pm->type));
        return -1;
    }
    r = binds_realloc_by_rows(binds, i);
    if (r) {
      LOGE("out of memory");
      return -1;
    }
  }

  for (size_t i=0; i<nr_meta; ++i) {
    TAOS_MULTI_BIND *bind = binds->binds + i;
    if (i<!!tbname + nr_tags) {
      int i_row = 0;
      for (int j=0; j<nr_tbls; ++j) {
        int tb_rows = rows[j];
        r = gen_tbname_tags(bind, j, i_row, tb_rows);
        if (r) return -1;
        i_row += tb_rows;
      }
      continue;
    }
    for (int j=0; j<nr_rows; ++j) {
      r = gen_col(bind, j);
      if (r) return -1;
    }
  }

  if (r == 0) *total_rows = nr_rows;
  return r;
}

typedef struct buf_s              buf_t;
struct buf_s {
  char              *buf;
  size_t             nr;
  size_t             cap;
};

static void buf_reset(buf_t *buf)
{
  if (!buf) return;
  buf->nr = 0;
}

static void buf_release(buf_t *buf)
{
  if (!buf) return;
  buf_reset(buf);
  SAFE_FREE(buf->buf);
  buf->cap = 0;
}

static int buf_vprintf(buf_t *buf, const char *fmt, va_list ap)
{
  int n = 0;

  size_t  nr = 0;
  va_list aq;

again:

  nr = buf->cap - buf->nr;
  va_copy(aq, ap);
  n = vsnprintf(buf->buf + buf->nr, nr, fmt, aq);
  va_end(aq);
  if (n == -1) return -1;
  if (n >= nr) {
    size_t align = 1024 * 16;
    size_t cap = (buf->cap + n + align - 1) / align * align;
    char *p = (char*)realloc(buf->buf, cap * sizeof(*p));
    if (!p) return -1;
    buf->buf       = p;
    buf->cap       = cap;
    goto again;
  }

  buf->nr += n;

  return 0;
}

__attribute__((format(printf, 2, 3)))
static int buf_printf(buf_t *buf, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int r = buf_vprintf(buf, fmt, ap);
  va_end(ap);

  return r;
}

static int buf_append_bind(buf_t *buf, TAOS_MULTI_BIND *bind, int row)
{
  int r = 0;

  char    *data    = ((char*)bind->buffer) + bind->buffer_length * row;
  int32_t *length  = bind->length + row;
  char    *is_null = bind->is_null + row;
  if (is_null && *is_null) {
    r = buf_printf(buf, "null");
    if (r) goto end;
  }

  int32_t i = 0;

  switch (bind->buffer_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      r = buf_printf(buf, "%" PRId64 "", *(int64_t*)data);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      r = buf_printf(buf, "'");
      if (r) goto end;

      while (i < *length) {
        char *p = strchr(data + i, '\'');
        if (!p) {
          r = buf_printf(buf, "%.*s", (*length - i), data + i);
          i = *length;
        } else {
          r = buf_printf(buf, "%.*s''", (int)(p - data - i), data + i);
          i = p - data;
        }
        if (r) goto end;
      }
      r = buf_printf(buf, "'");
      break;
    case TSDB_DATA_TYPE_INT:
      r = buf_printf(buf, "%d", *(int32_t*)data);
      break;
    default:
      LOGE("[%d]%s:not implemented yet", bind->buffer_type, taos_data_type(bind->buffer_type));
      return -1;
  }

end:
  return r;
}

static int run_literal(TAOS *conn, int *rows_inserted, buf_t *buf, const char *st, binds_t *binds, size_t nr_binds, int tbname, int nr_tags)
{
  int r = 0;

  const size_t nr_max = 1024 * 1024;

  int row = 0;
  TAOS_MULTI_BIND *tbl   = !!tbname ? binds->binds : NULL;
  TAOS_MULTI_BIND *tags  = binds->binds + !!tbname;
  TAOS_MULTI_BIND *cols  = tags + nr_tags;
  int nr_cols = nr_binds - !!tbname - nr_tags;
  int rows_in_tbl = 0;

  int row0 = 0;

  *rows_inserted = 0;

  if (nr_cols < 0) {
    LOGE("internal logic error");
    return -1;
  }

  r = buf_printf(buf, "insert into");
  if (r) goto end;

  while (row < binds->binds->num) {
    size_t nr       = buf->nr;

    if (tbname) {
      const char *s      = ((const char*)tbl->buffer) + row * tbl->buffer_length;
      size_t      n      = strlen(s);
      int i = row + 1;
      for (; i<binds->binds->num; ++i) {
        const char *s1      = ((const char*)tbl->buffer) + i * tbl->buffer_length;
        size_t      n1      = strlen(s);
        if (n != n1 || strcasecmp(s, s1)) break;
      }

      rows_in_tbl = i - row;
      r = buf_printf(buf, " %s using %s", s, st);
      if (r) goto end;

      if (nr_tags) {
        r = buf_printf(buf, " tags (");
        if (r) goto end;
        for (int j=0; j<nr_tags; ++j) {
          if (j) {
            r = buf_printf(buf, ",");
            if (r) goto end;
          }
          TAOS_MULTI_BIND *tag = tags + j;
          r = buf_append_bind(buf, tag, row);
          if (r) goto end;
        }
        r = buf_printf(buf, ")");
        if (r) goto end;
      }
    }

    int i = 0;
    if (nr_cols) {
      r = buf_printf(buf, " values");
      if (r) goto end;
      for (i=0; i<rows_in_tbl; ++i) {
        r = buf_printf(buf, " (");
        if (r) goto end;
        for (int j=0; j<nr_cols; ++j) {
          if (j) {
            r = buf_printf(buf, ",");
            if (r) goto end;
          }
          TAOS_MULTI_BIND *col = cols + j;
          r = buf_append_bind(buf, col, row + i);
          if (r) goto end;
        }
        r = buf_printf(buf, ")");
        if (r) goto end;

        if (buf->nr >= nr_max) break;
        nr = buf->nr;
      }
    }

    if (i < rows_in_tbl) {
      buf->nr = nr;
      buf->buf[nr] = '\0';
      // LOGE("%s", buf->buf);
      r = run_sql(conn, buf->buf);
      buf_reset(buf);
      if (r) return -1;

      row0 = row + i;
      *rows_inserted = row0;

      r = buf_printf(buf, "insert into");
      if (r) return -1;
    }

    row += i;
  }

  if (row > row0) {
    // LOGE("%s", buf->buf);
    r = run_sql(conn, buf->buf);
    buf_reset(buf);
    if (r) return -1;

    row0 = row;
    *rows_inserted = row0;

    r = buf_printf(buf, "insert into");
    if (r) return -1;
  }

end:

  return r;
}

static int run_origin(TAOS_STMT *stmt, binds_t *binds, size_t nr_binds, int tbname, int nr_tags)
{
  int r = 0;

  TAOS_MULTI_BIND *batch = NULL;
  size_t           cap   = 0;

  int row = 0;
  TAOS_MULTI_BIND *tbl   = !!tbname ? binds->binds : NULL;
  TAOS_MULTI_BIND *tags  = binds->binds + !!tbname;
  TAOS_MULTI_BIND *cols  = tags + nr_tags;
  int nr_cols = nr_binds - !!tbname - nr_tags;

  if (nr_cols < 0) {
    LOGE("internal logic error");
    return -1;
  }

  cap = !!tbname;
  if (cap < nr_tags) cap = nr_tags;
  if (cap < nr_cols) cap = nr_cols;

  batch = (TAOS_MULTI_BIND*)malloc(cap * sizeof(*batch));
  if (!batch) {
    LOGE("out of memory");
    return -1;
  }

  int rows = 0;

  while (row < binds->binds->num) {
    rows = binds->binds->num - row;

    if (tbname) {
      const char *s      = ((const char*)tbl->buffer) + row * tbl->buffer_length;
      size_t      n      = strlen(s);
      int i = row + 1;
      for (; i<binds->binds->num; ++i) {
        const char *s1      = ((const char*)tbl->buffer) + i * tbl->buffer_length;
        size_t      n1      = strlen(s);
        if (n != n1 || strcasecmp(s, s1)) break;
      }

      rows = i - row;
      r = taos_stmt_set_tbname(stmt, s);
      CHK_STMT(stmt, r, "taos_stmt_set_tbname(%s) failed", s);
      if (r) break;

      if (nr_tags) {
        memcpy(batch, tags, nr_tags * sizeof(*batch));
        for (int j=0; j<nr_tags; ++j) {
          TAOS_MULTI_BIND *dst = batch + j;
          TAOS_MULTI_BIND *src = tags + j;
          dst->buffer  = ((char*)src->buffer) + src->buffer_length * row;
          dst->length  = src->length + row;
          dst->is_null = src->is_null + row;
          dst->num     = 1;
        }
        r = taos_stmt_set_tags(stmt, batch);
        CHK_STMT(stmt, r, "taos_stmt_set_tags() failed");
        if (r) break;
      }
    }

    if (nr_cols) {
      memcpy(batch, cols, nr_cols * sizeof(*batch));
      for (int j=0; j<nr_cols; ++j) {
        TAOS_MULTI_BIND *dst = batch + j;
        TAOS_MULTI_BIND *src = cols + j;
        dst->buffer  = ((char*)src->buffer) + src->buffer_length * row;
        dst->length  = src->length + row;
        dst->is_null = src->is_null + row;
        dst->num     = rows;
      }
      r = taos_stmt_bind_param_batch(stmt, batch);
      CHK_STMT(stmt, r, "taos_stmt_bind_params_batch() failed");
      if (r) break;
      r = taos_stmt_add_batch(stmt);
      CHK_STMT(stmt, r, "taos_stmt_add_batch() failed");
      if (r) break;
    }

    row += rows;
  }

  r = taos_stmt_execute(stmt);
  CHK_STMT(stmt, r, "taos_stmt_execute() failed");

end:

  SAFE_FREE(batch);

  return r;
}

static int run_api2(TAOS_STMT *stmt, binds_t *binds, size_t nr_binds, int tbname, int nr_tags)
{
  int r = 0;

  r = taos_stmt_bind_params2(stmt, binds->binds, nr_binds);
  CHK_STMT(stmt, r, "taos_stmt_bind_params2() failed");
  if (r) return -1;

  r = taos_stmt_execute(stmt);
  CHK_STMT(stmt, r, "taos_stmt_execute() failed");
  if (r) return -1;

  return 0;
}

static int run_with_stmt(TAOS *conn, TAOS_STMT *stmt, int mode, int loops, int tables, int records)
{
  int r = 0;

  const char *sqls[] = {
    "drop database if exists foo",
    "create database if not exists foo",
    "use foo",
    "create stable st (ts timestamp, i32 int) tags (tv int)",
  };
  r = run_sqls(conn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  const char *sql = "insert into ? using st (tv) tags (?) (ts, i32) values (?, ?)";
  param_meta_t ar_params[] = {
    {TSDB_DATA_TYPE_VARCHAR, 192},
    {TSDB_DATA_TYPE_INT},
    {TSDB_DATA_TYPE_TIMESTAMP},
    {TSDB_DATA_TYPE_INT},
  };

  const size_t nr_params = sizeof(ar_params) / sizeof(*ar_params);
  const int    tbname    = 1;
  const int    nr_tags   = 1;

  binds_t          binds = {0};
  int             *rows  = NULL;

  r = binds_realloc(&binds, nr_params);
  if (r) return -1;

  rows  = (int*)calloc(tables, sizeof(*rows));
  if (!rows) {
    LOGE("out of memory");
    binds_release(&binds);
    return -1;
  }

  struct timespec t0 = {0};
  struct timespec t1 = {0};
  buf_t buf = {0};

  size_t    total_records = 0;

  if (mode == 0) {
  } else if (mode == 1) {
    r = taos_stmt_prepare(stmt, sql, strlen(sql));
    CHK_STMT(stmt, r, "taos_stmt_prepare2(%s) failed", sql);
  } else {
    r = taos_stmt_prepare2(stmt, sql, strlen(sql));
    CHK_STMT(stmt, r, "taos_stmt_prepare2(%s) failed", sql);
  }

  clock_gettime(CLOCK_MONOTONIC, &t0);

  for (int i=0; i<loops; ++i) {
    if (r) break;
    size_t total_rows = 0;
    r = gen_binds(&binds, rows, &total_rows, ar_params, nr_params, !!tbname, nr_tags, tables, records);
    if (r) break;

    if (mode == 0) {
      buf_reset(&buf);
      int rows_inserted = 0;
      r = run_literal(conn, &rows_inserted, &buf, "st", &binds, nr_params, !!tbname, nr_tags);
      total_rows = rows_inserted;
    } else if (mode == 1) {
      r = run_origin(stmt, &binds, nr_params, !!tbname, nr_tags);
    } else {
      r = run_api2(stmt, &binds, nr_params, !!tbname, nr_tags);
    }

    total_records += total_rows;
  }

  clock_gettime(CLOCK_MONOTONIC, &t1);

  if (r == 0) {
    int64_t delta = t1.tv_sec - t0.tv_sec;
    delta *= 1000000000;
    delta += t1.tv_nsec - t0.tv_nsec;

    LOGE("total-records: %" PRId64 "", total_records);
    LOGE("        delta: %" PRId64 "ns; %lfs", delta, delta/(double)1000000000);
    LOGE("   throughput: %lf/s", total_records / (double)delta * 1000000000);
  }

  buf_release(&buf);
  binds_release(&binds);
  SAFE_FREE(rows);

  return !!r;
}

static void usage(FILE *f, const char *app)
{
  fprintf(f, "insert benchmark for taos\n");
  fprintf(f, "%s [options]...\n", app);
  fprintf(f, "  -t <tables>:             # of tables to generate, default 2 tables\n");
  fprintf(f, "  -r <max-rows-per-table>  # of rows at the most to insert per table, default 1 row\n");
  fprintf(f, "  -l <loops>               # of loops to run, default 1 loop\n");
  fprintf(f, "  --options <options>      # singleStbInsert | singleTableBindOnce\n");
  fprintf(f, "  -1                       use taos_stmt_prepare\n");
  fprintf(f, "  -2                       use taos_stmt_prepare2\n");
  fprintf(f, "  note: if -1/-2 is not specified, use taos_query instead\n");
}

static int run(int argc, char *argv[])
{
  int r = 0;
  int e = 0;

  const char *ip           = NULL;
  const char *user         = NULL;
  const char *pass         = NULL;
  const char *db           = NULL;
  uint16_t    port         = 0;

  int mode    = 0;
  int loops   = 1;
  int tables  = 2;
  int records = 1;
  TAOS_STMT_OPTIONS opt = {0};


  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(stdout, argv[0]);
      return 0;
    }
    if (strcmp(arg, "-1") == 0) {
      // origin mode
      mode = 1;
      continue;
    }
    if (strcmp(arg, "-2") == 0) {
      // stmtAPI2 mode
      mode = 2;
      continue;
    }
    if (strcmp(arg, "-l") == 0) {
      // how many loops to go
      if (++i >= argc) {
        fprintf(stderr, "-l <loops>; missing <loops>\n");
        return -1;
      }
      loops = atoi(argv[i]);
      continue;
    }
    if (strcmp(arg, "-t") == 0) {
      // how many tables to create
      if (++i >= argc) {
        fprintf(stderr, "-t <tables>; missing <tables>\n");
        return -1;
      }
      tables = atoi(argv[i]);
      continue;
    }
    if (strcmp(arg, "-r") == 0) {
      // how many records at most to create for each table
      if (++i >= argc) {
        fprintf(stderr, "-r <records>; missing <records>\n");
        return -1;
      }
      records = atoi(argv[i]);
      continue;
    }
    if (strcmp(arg, "--options") == 0) {
      if (++i >= argc) {
        fprintf(stderr, "--options <options>; missing <options>\n");
        return -1;
      }
      if (strstr(argv[i], "singleStbInsert")) {
        opt.singleStbInsert  = true;
      }
      if (strstr(argv[i], "singleTableBindOnce")) {
        opt.singleTableBindOnce  = true;
      }
      continue;
    }
    fprintf(stderr, "unknow argument:%s\n", arg);
    return -1;
  }

  TAOS *conn = taos_connect(ip, user, pass, db, port);
  CHK_TAOS(conn, "taos_connect failed");
  if (!conn) return -1;

  TAOS_STMT *stmt = taos_stmt_init_with_options(conn, &opt);
  e = taos_errno(NULL);
  CHK_STMT(stmt, e, "taos_stmt_init failed");
  if (!stmt) {
    taos_close(conn);
    return -1;
  }

  r = run_with_stmt(conn, stmt, mode, loops, tables, records);
  taos_stmt_close(stmt);

  return r;
}

int main(int argc, char *argv[])
{
  int r = 0;

  srand(time(0));

  r = run(argc, argv);

  // LOGE("%s", r ? "failure" : "success");

  return !!r;
}

