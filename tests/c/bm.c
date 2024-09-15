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
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LOGE(fmt, ...) fprintf(stderr, "" fmt "\n", ##__VA_ARGS__)
#define DBGE(fmt, ...) if (0) fprintf(stderr, "api2_test.c[%d]:%s():" fmt "\n", __LINE__, __FILE__, ##__VA_ARGS__)

#define CHK_TAOS(taos, fmt, ...) do {                                  \
  if (taos) break;                                                     \
  int _e = taos_errno(NULL);                                           \
  if (!_e) break;                                                      \
  fprintf(stderr, "api2_test.c[%d]:%s():[0x%08x]%s:" fmt "\n",         \
      __LINE__, __func__,                                              \
      _e, taos_errstr(NULL),                                           \
      ##__VA_ARGS__);                                                  \
} while (0)

#define CHK_RES(res, e, fmt, ...) do {                                 \
  if (!e) break;                                                       \
  fprintf(stderr, "api2_test.c[%d]:%s():[0x%08x]%s:" fmt "\n",         \
      __LINE__, __func__,                                              \
      e, taos_errstr(res),                                             \
      ##__VA_ARGS__);                                                  \
} while (0)

#define CHK_STMT(stmt, e, fmt, ...) do {                               \
  if (!e) break;                                                       \
  fprintf(stderr, "api2_test.c[%d]:%s():[0x%08x]%s:" fmt "\n",         \
      __LINE__, __func__,                                              \
      e, stmt ? taos_stmt_errstr(stmt) : taos_errstr(NULL),            \
      ##__VA_ARGS__);                                                  \
} while (0)

static const char* tsdb_data_type_name(int type)
{
  switch (type) {
    case TSDB_DATA_TYPE_NULL:        return "TSDB_DATA_TYPE_NULL";
    case TSDB_DATA_TYPE_BOOL:        return "TSDB_DATA_TYPE_BOOL";
    case TSDB_DATA_TYPE_TINYINT:     return "TSDB_DATA_TYPE_TINYINT";
    case TSDB_DATA_TYPE_SMALLINT:    return "TSDB_DATA_TYPE_SMALLINT";
    case TSDB_DATA_TYPE_INT:         return "TSDB_DATA_TYPE_INT";
    case TSDB_DATA_TYPE_BIGINT:      return "TSDB_DATA_TYPE_BIGINT";
    case TSDB_DATA_TYPE_FLOAT:       return "TSDB_DATA_TYPE_FLOAT";
    case TSDB_DATA_TYPE_DOUBLE:      return "TSDB_DATA_TYPE_DOUBLE";
    case TSDB_DATA_TYPE_VARCHAR:     return "TSDB_DATA_TYPE_VARCHAR";
    case TSDB_DATA_TYPE_TIMESTAMP:   return "TSDB_DATA_TYPE_TIMESTAMP";
    case TSDB_DATA_TYPE_NCHAR:       return "TSDB_DATA_TYPE_NCHAR";
    case TSDB_DATA_TYPE_UTINYINT:    return "TSDB_DATA_TYPE_UTINYINT";
    case TSDB_DATA_TYPE_USMALLINT:   return "TSDB_DATA_TYPE_USMALLINT";
    case TSDB_DATA_TYPE_UINT:        return "TSDB_DATA_TYPE_UINT";
    case TSDB_DATA_TYPE_UBIGINT:     return "TSDB_DATA_TYPE_UBIGINT";
    case TSDB_DATA_TYPE_JSON:        return "TSDB_DATA_TYPE_JSON";
    case TSDB_DATA_TYPE_VARBINARY:   return "TSDB_DATA_TYPE_VARBINARY";
    case TSDB_DATA_TYPE_DECIMAL:     return "TSDB_DATA_TYPE_DECIMAL";
    case TSDB_DATA_TYPE_BLOB:        return "TSDB_DATA_TYPE_BLOB";
    case TSDB_DATA_TYPE_MEDIUMBLOB:  return "TSDB_DATA_TYPE_MEDIUMBLOB";
    // case TSDB_DATA_TYPE_BINARY:      return "TSDB_DATA_TYPE_BINARY";
    case TSDB_DATA_TYPE_GEOMETRY:    return "TSDB_DATA_TYPE_GEOMETRY";
    case TSDB_DATA_TYPE_MAX:         return "TSDB_DATA_TYPE_MAX";
    default:                         return "TSDB_DATA_TYPE_UNKNOWN";
  }
}

static int run_sql(TAOS *conn, const char *sql)
{
  TAOS_RES *res = taos_query(conn, sql);
  int e = taos_errno(res);                                            \
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

static TAOS_MULTI_BIND* create_binds(param_meta_t *meta, size_t nr_meta, int tbname, int nr_tags, int nr_tbls, int nr_records)
{
  int r = 0;

  TAOS_MULTI_BIND *binds = NULL;
  int             *rows  = NULL;

  TAOS_MULTI_BIND *tags  = NULL;
  TAOS_MULTI_BIND *cols  = NULL;

  int nr_cols = nr_meta - !!tbname - nr_tags;
  if (nr_cols <= 0) {
    LOGE("invalid arg:nr_meta[%zd]-tbname[%d]-nr_tags[%d] == nr_cols[%d]", nr_meta, !!tbname, nr_tags, nr_cols);
    goto failure;
  }

  if ((!!tbname) ^ (!!nr_tags)) {
    LOGE("not implemented yet");
    goto failure;
  }

  if (tbname && meta->type != TSDB_DATA_TYPE_VARCHAR) {
    LOGE("not implemented yet");
    goto failure;
  }

  binds = (TAOS_MULTI_BIND*)malloc(nr_meta * sizeof(*binds));
  rows  = (int*)malloc(nr_tbls * sizeof(*rows));
  if (!binds || !rows) {
    LOGE("out of memory");
    goto failure;
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
    TAOS_MULTI_BIND *bind = binds + i;
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
          goto failure;
        }
        bind->buffer_length = pm->len + 1;
        break;
      case TSDB_DATA_TYPE_INT:
        bind->buffer_length       = sizeof(int32_t);
        break;
      default:
        LOGE("[%d]%s:not implemented yet", pm->type, taos_data_type(pm->type));
        goto failure;
    }
    bind->buffer  = (void*)malloc(bind->buffer_length * bind->num);
    bind->length  = (int32_t*)malloc(sizeof(*bind->length) * bind->num);
    bind->is_null = (char*)malloc(sizeof(*bind->is_null) * bind->num);
    if (!bind->buffer || !bind->length || !bind->is_null) {
      LOGE("out of memory");
      goto failure;
    }
  }

  for (size_t i=0; i<nr_meta; ++i) {
    TAOS_MULTI_BIND *bind = binds + i;
    if (i<!!tbname + nr_tags) {
      int i_row = 0;
      for (int j=0; j<nr_tbls; ++j) {
        int tb_rows = rows[j];
        r = gen_tbname_tags(bind, j, i_row, tb_rows);
        if (r) goto failure;
        i_row += tb_rows;
      }
      continue;
    }
    for (int j=0; j<nr_rows; ++j) {
      r = gen_col(bind, j);
      if (r) goto failure;
    }
  }

  goto end;

failure:
  release_binds(binds, nr_meta);
  SAFE_FREE(binds);

end:
  SAFE_FREE(rows);

  return binds;
}

static int run_origin(TAOS_STMT *stmt, const char *sql, TAOS_MULTI_BIND *binds, size_t nr_binds, int tbname, int nr_tags)
{
  int r = 0;

  TAOS_MULTI_BIND *batch = NULL;
  size_t           cap   = 0;

  int row = 0;
  TAOS_MULTI_BIND *tbl   = !!tbname ? binds : NULL;
  TAOS_MULTI_BIND *tags  = binds + !!tbname;
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

  r = taos_stmt_prepare(stmt, sql, strlen(sql));
  CHK_STMT(stmt, r, "taos_stmt_prepare2(%s) failed", sql);
  if (r) goto end;

  int rows = 0;

  while (row < binds->num) {
    rows = binds->num - row;

    if (tbname) {
      const char *s      = ((const char*)tbl->buffer) + row * tbl->buffer_length;
      size_t      n      = strlen(s);
      int i = row + 1;
      for (; i<binds->num; ++i) {
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

    r = taos_stmt_execute(stmt);
    CHK_STMT(stmt, r, "taos_stmt_execute() failed");
    if (r) break;

    row += rows;
  }

end:

  SAFE_FREE(batch);

  return r;
}

static int run_api2(TAOS_STMT *stmt, const char *sql, TAOS_MULTI_BIND *binds, size_t nr_binds, int tbname, int nr_tags)
{
  int r = 0;

  r = taos_stmt_prepare2(stmt, sql, strlen(sql));
  CHK_STMT(stmt, r, "taos_stmt_prepare2(%s) failed", sql);
  if (r) return -1;

  r = taos_stmt_bind_params2(stmt, binds, nr_binds);
  CHK_STMT(stmt, r, "taos_stmt_bind_params2() failed");
  if (r) return -1;

  r = taos_stmt_execute(stmt);
  CHK_STMT(stmt, r, "taos_stmt_execute() failed");
  if (r) return -1;

  return 0;
}

static int run_with_stmt(TAOS *conn, TAOS_STMT *stmt, int mode, int tables, int records)
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

  size_t nr_params = sizeof(ar_params) / sizeof(*ar_params);
  int    tbname    = 1;
  int    nr_tags   = 1;

  TAOS_MULTI_BIND *binds = create_binds(ar_params, nr_params, !!tbname, nr_tags, tables, records);
  if (!binds) return -1;

  struct timespec t0 = {0};
  clock_gettime(CLOCK_MONOTONIC, &t0);

  if (mode == 1) {
    r = run_origin(stmt, sql, binds, nr_params, !!tbname, nr_tags);
  } else {
    r = run_api2(stmt, sql, binds, nr_params, !!tbname, nr_tags);
  }

  struct timespec t1 = {0};
  clock_gettime(CLOCK_MONOTONIC, &t1);

  int64_t delta = t1.tv_sec - t0.tv_sec;
  delta *= 1000000000;
  delta += t1.tv_nsec - t0.tv_nsec;

  LOGE("delta: %" PRId64 ".%" PRId64 "s", delta/1000000000, delta % 1000000000);
  LOGE("throughput: %lf/s", ((double)binds->num) / delta * 1000000000);

  release_binds(binds, nr_params);
  SAFE_FREE(binds);

  return !!r;
}

static int run(TAOS *conn, int argc, char *argv[])
{
  int r = 0;
  int e = 0;

  int mode    = 1;
  int tables  = 2;
  int records = 1;

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
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
    fprintf(stderr, "unknow argument:%s\n", arg);
    return -1;
  }

  TAOS_STMT *stmt = taos_stmt_init(conn);
  e = taos_errno(NULL);
  CHK_STMT(stmt, e, "taos_stmt_init failed");
  if (!stmt) return -1;

  r = run_with_stmt(conn, stmt, mode, tables, records);
  taos_stmt_close(stmt);

  return r;
}

int main(int argc, char *argv[])
{
  int r = 0;

  srand(time(0));

  const char *ip           = NULL;
  const char *user         = NULL;
  const char *pass         = NULL;
  const char *db           = NULL;
  uint16_t    port         = 0;

  TAOS *conn = taos_connect(ip, user, pass, db, port);
  CHK_TAOS(conn, "taos_connect failed");
  if (!conn) r = -1;
  if (r == 0) r = run(conn, argc, argv);
  if (conn) taos_close(conn);

  return !!r;
}

