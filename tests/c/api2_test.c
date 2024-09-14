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

typedef struct describe_params_case_s describe_params_case_t;
struct describe_params_case_s {
  int                line;
  const char        *create_sql;
  const char        *sql;
  int                is_insert;
  TAOS_FIELD_E       params[60];
};

static int cmp_TAOS_FIELD_E(const TAOS_FIELD_E *ll, const TAOS_FIELD_E *rr)
{
  int r = 0;

  size_t nl = strnlen(ll->name, sizeof(ll->name));
  size_t nr = strnlen(rr->name, sizeof(rr->name));
  if (nl != nr) return (int)(nl - nr);

  r = strncmp(ll->name, rr->name, nl);
  if (r) return r;

  r = ll->type - rr->type;
  if (r) return r;

  r = ll->precision - rr->precision;
  if (r) return r;

  r = ll->scale - rr->scale;
  if (r) return r;

  return (ll->bytes - rr->bytes);
}

static int describe_params(TAOS *conn, const describe_params_case_t *_case)
{
  int r = 0;
  int           line             = _case->line;
  const char   *create_sql       = _case->create_sql;
  const char   *sql              = _case->sql;

  int           is_insert  = 0;
  TAOS_FIELD_E  params[60] = {0};
  int           nr_params  = 0;
  int           nr_params_expect = 0;

  if (line == 0) return 0;

  for (size_t i=0; i<sizeof(_case->params)/sizeof(_case->params[0]); ++i) {
    if (_case->params[i].type == -1) break;
    ++nr_params_expect;
  }

  if (create_sql) {
    r = run_sql(conn, create_sql);
    if (r) return -1;
  }

  TAOS_STMT *stmt = taos_stmt_init(conn);
  int e = taos_errno(NULL);
  if (!stmt) {
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_init failed", line);
    r = -1;
    goto end;
  }

  r = taos_stmt_prepare2(stmt, sql, strlen(sql));
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_prepare2(%s) failed", line, sql);
    r = -1;
    goto end;
  }

  r = taos_stmt_is_insert(stmt, &is_insert);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "taos_stmt_is_insert failed");
    r = -1;
    goto end;
  }

  if (is_insert != _case->is_insert) {
    DBGE("@[%d]: in [%s], expecting %s, but got ==%s==", line, sql, _case->is_insert ? "insert" : "non-insert", is_insert ? "insert" : "non-insert");
    r = -1;
    goto end;
  }

  r = taos_stmt_get_params2(stmt, NULL, 0, &nr_params);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: in [%s], taos_stmt_get_params2 failed", line, sql);
    r = -1;
    goto end;
  }
  if (nr_params != nr_params_expect) {
    DBGE("@[%d]: in [%s], expecting #%d params, but got ==%d==", line, sql, nr_params_expect, nr_params);
    r = -1;
    goto end;
  }

  r = taos_stmt_get_params2(stmt, params, sizeof(params)/sizeof(params[0]), &nr_params);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "taos_stmt_get_params2 failed");
    r = -1;
    goto end;
  }
  if (nr_params > sizeof(params) / sizeof(params[0])) {
    DBGE("@[%d]: bad test case", line);
    r = -1;
    goto end;
  }

  // NOTE: check consistency with taos_stmt_num_params
  nr_params = 0;
  r = taos_stmt_num_params(stmt, &nr_params);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "taos_stmt_num_params failed");
    r = -1;
    goto end;
  }
  if (nr_params > sizeof(params) / sizeof(params[0])) {
    DBGE("@[%d]: bad test case", line);
    r = -1;
    goto end;
  }

  for (size_t i=0; i<nr_params; ++i) {
    TAOS_FIELD_E *actual = params + i;
    const TAOS_FIELD_E *expect = _case->params + i;
    // NOTE: `name` field might differ after \0, thus adjust logic accordingly
    if (0==cmp_TAOS_FIELD_E(actual, expect)) {
      int type  = 0;
      int bytes = 0;
      r = taos_stmt_get_param(stmt, i, &type, &bytes);
      if (r) {
        e = taos_errno(NULL);
        CHK_STMT(stmt, e, "taos_stmt_get_param failed");
        r = -1;
        goto end;
      }
      if (type != expect->type) {
        DBGE("@[%d]: in [%s], #%zd param does not match", line, sql, i+1);
        fprintf(stderr, "expect type of [%d], but got ==[%d]==\n", expect->type, type);
        r = -1;
        goto end;
      }
      if (bytes != expect->bytes) {
        DBGE("@[%d]: in [%s], #%zd param does not match", line, sql, i+1);
        fprintf(stderr, "expect bytes of [%d], but got ==[%d]==\n", expect->bytes, bytes);
        r = -1;
        goto end;
      }
      continue;
    }

    DBGE("@[%d]: in [%s], #%zd param does not match", line, sql, i+1);
    fprintf(stderr, "expecting:(%.*s, %d, %d, %d, %d)\n",
        (int)(sizeof(expect->name)), expect->name, expect->type, expect->precision, expect->scale, expect->bytes);
    fprintf(stderr, "but =got=:(%.*s, %d, %d, %d, %d)\n",
        (int)(sizeof(actual->name)), actual->name, actual->type, actual->precision, actual->scale, actual->bytes);
    r = -1;
    break;
  }

end:
  taos_stmt_close(stmt);
  return r;
}

static int run_params_describe(TAOS *conn)
{
  int r = 0;

  static const describe_params_case_t _cases[] = {
    {
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into ? values (?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tbname", TSDB_DATA_TYPE_VARCHAR, 0, 0, 192+2},
        { "?",      TSDB_DATA_TYPE_NULL,    0, 0, 1024+2},
        { "?",      TSDB_DATA_TYPE_NULL,    0, 0, 1024+2},
        { "",       -1,                     0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = "create table t (ts timestamp, nm varchar(20), u8 tinyint unsigned)",
      .sql        = "select * from t",
      .is_insert  = 0,
      .params     = {
        { "",       -1,                     0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "select * from t where ts = ?",
      .is_insert  = 0,
      .params     = {
        { "?", TSDB_DATA_TYPE_VARCHAR, 0, 0, 1024+2},
        { "",  -1,                     0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into t (ts, nm, u8) values (?, ?, ?)",
      .is_insert  = 1,
      .params     = {
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into t (ts, u8, nm) values (?, ?, ?)",
      .is_insert  = 1,
      .params     = {
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = "create stable st (ts timestamp, nm varchar(20), u8 tinyint unsigned) tags (tn varchar(30), tt timestamp)",
      .sql        = "insert into xyz using st tags (?, ?) values (?, ?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tn", TSDB_DATA_TYPE_VARCHAR,   0, 0, 30+2},
        { "tt", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into ? using st tags (?, ?) values (?, ?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tbname", TSDB_DATA_TYPE_VARCHAR,   0, 0, 192+2},
        { "tn", TSDB_DATA_TYPE_VARCHAR,   0, 0, 30+2},
        { "tt", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into ? using st (tn, tt) tags (?, ?) (ts, nm, u8) values (?, ?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tbname", TSDB_DATA_TYPE_VARCHAR,   0, 0, 192+2},
        { "tn", TSDB_DATA_TYPE_VARCHAR,   0, 0, 30+2},
        { "tt", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into ? using st (tt, tn) tags (?, ?) (ts, u8, nm) values (?, ?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tbname", TSDB_DATA_TYPE_VARCHAR,   0, 0, 192+2},
        { "tt", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "tn", TSDB_DATA_TYPE_VARCHAR,   0, 0, 30+2},
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into ? using st (tn) tags (?) (ts, u8) values (?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tbname", TSDB_DATA_TYPE_VARCHAR,   0, 0, 192+2},
        { "tn", TSDB_DATA_TYPE_VARCHAR,   0, 0, 30+2},
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
        { "",   -1,                       0, 0, 0},
      },
    },{
      .line       = __LINE__,
      .create_sql = NULL,
      .sql        = "insert into ? using st (tt) tags (?) (ts, nm) values (?, ?)",
      .is_insert  = 1,
      .params     = {
        { "tbname", TSDB_DATA_TYPE_VARCHAR,   0, 0, 192+2},
        { "tt", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
        { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
        { "",   -1,                       0, 0, 0},
      },
    },{
      // .line       = __LINE__,
      // .create_sql = NULL,
      // .sql        = "insert into ? using st (ts, nm, u8) values (?, ?, ?)",
      // // NOTE: [0x80002602]Invalid column name
      // .is_insert  = 1,
      // .params     = {
      //   { "tbname", TSDB_DATA_TYPE_VARCHAR,   0, 0, 192+2},
      //   { "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8},
      //   { "nm", TSDB_DATA_TYPE_VARCHAR,   0, 0, 20+2},
      //   { "u8", TSDB_DATA_TYPE_UTINYINT,  0, 0, 1},
      //   { "",   -1,                       0, 0, 0},
      // },
    }
  };

  for (size_t i=0; i<sizeof(_cases)/sizeof(_cases[0]); ++i) {
    r = describe_params(conn, _cases+i);
    if (r) return -1;
  }

  return 0;
}

#define EXEC_COL_END          ((const char*)-1)

typedef struct exec_case_s       exec_case_t;
typedef struct exec_case_row_s   exec_case_row_t;

struct exec_case_row_s {
  int               line;
  const char       *cols[60];
};

struct exec_case_s {
  int                line;
  const char        *sql;

  int                is_insert;

  exec_case_row_t    params[60];
  exec_case_row_t    cells[60];
};

static void exec_case_row_get_size(const exec_case_row_t *rows, const exec_case_row_t *end, int *nr_rows, int *nr_cols)
{
  *nr_rows = 0;
  *nr_cols = 0;

  const size_t n = end - rows;

  size_t i = 0;
  for (i=0; i<n; ++i) {
    const exec_case_row_t *row = rows + i;
    if (row->cols[0] == EXEC_COL_END) break;

    size_t j = 0;
    for (j=1; j<sizeof(row->cols)/sizeof(row->cols[0]); ++j) {
      if (row->cols[j] == EXEC_COL_END) break;
    }
    if (*nr_cols < j) *nr_cols = j;
  }
  *nr_rows = i;
}

#define SAFE_FREE(x) if (x) { free(x); x = NULL; }

static void release_TAOS_MULTI_BIND(TAOS_MULTI_BIND *p)
{
  p->buffer_type                    = 0;
  SAFE_FREE(p->buffer);
  p->buffer_length                  = 0;
  SAFE_FREE(p->length);
  SAFE_FREE(p->is_null);
  p->num                            = 0;
}

static void release_TAOS_MULTI_BINDs(TAOS_MULTI_BIND *mbs, size_t nr_mbs)
{
  for (size_t i=0; i<nr_mbs; ++i) {
    TAOS_MULTI_BIND *p = mbs + i;
    release_TAOS_MULTI_BIND(p);
  }
}

#define PREPARE_SET_BIND(x, y) do {                                         \
  bind->buffer_type    = x;                                                 \
  bind->buffer         = calloc(nr_params_rows, sizeof(y));                 \
  bind->buffer_length  = sizeof(y);                                         \
  bind->length         = NULL;                                              \
  bind->is_null        = calloc(nr_params_rows, sizeof(*bind->is_null));    \
} while (0)

static int prepare_by_null_type(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  char *p = (char*)bind->buffer;
  size_t len = 0;
  for (int j=0; j<nr_params_rows; ++j, p+=meta->bytes) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) continue;
    if (s) {
      size_t n = strlen(s);
      if (n > len) len = n;
    }
  }

  bind->buffer_type                = TSDB_DATA_TYPE_VARCHAR;
  bind->buffer                     = calloc(nr_params_rows, len + 1);
  bind->buffer_length              = len + 1;
  bind->length                     = calloc(nr_params_rows, sizeof(*bind->length));
  bind->is_null                    = calloc(nr_params_rows, sizeof(*bind->is_null));
  if (!bind->buffer || !bind->length || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  p = (char*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, p+=bind->buffer_length) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      snprintf(p, bind->buffer_length, "%s", s);
      bind->length[j]    = strlen(p);
      bind->is_null[j]   = 0;
    } else {
      bind->length[j]    = 0;
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_varchar(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  bind->buffer_type                = TSDB_DATA_TYPE_VARCHAR;
  bind->buffer                     = calloc(nr_params_rows, meta->bytes);
  bind->buffer_length              = meta->bytes;
  bind->length                     = calloc(nr_params_rows, sizeof(*bind->length));
  bind->is_null                    = calloc(nr_params_rows, sizeof(*bind->is_null));
  if (!bind->buffer || !bind->length || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  char *p = (char*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, p+=meta->bytes) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      snprintf(p, meta->bytes, "%s", s);
      bind->length[j]    = strlen(p);
      bind->is_null[j]   = 0;
    } else {
      bind->length[j]    = 0;
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_nchar(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  // NOTE: we don't convert, and fall back to varchar, and let taosc to handle
  return prepare_by_varchar(stmt, _case, idx, meta, bind, nr_params_rows);
}

static int prepare_by_timestamp(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_TIMESTAMP, int64_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  int64_t *p = (int64_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      int64_t v = strtoll(s, NULL, 0); // TODO: check if strtoll fail or not
      *p = v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_bit(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_BOOL, int8_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  int8_t *p = (int8_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      int64_t v = strtoll(s, NULL, 0); // TODO: check if strtoll fail or not
      *p = !!v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_i8(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_TINYINT, int8_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  int8_t *p = (int8_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      int64_t v = strtoll(s, NULL, 0); // TODO: check if strtoll fail or not
      *p = (int8_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_u8(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_UTINYINT, uint8_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  uint8_t *p = (uint8_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      uint64_t v = strtoull(s, NULL, 0); // TODO: check if strtoull fail or not
      *p = (uint8_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_i16(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_SMALLINT, int16_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  int16_t *p = (int16_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      int64_t v = strtoll(s, NULL, 0); // TODO: check if strtoll fail or not
      *p = (int16_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_u16(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_USMALLINT, uint16_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  uint16_t *p = (uint16_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      uint64_t v = strtoull(s, NULL, 0); // TODO: check if strtoull fail or not
      *p = (uint16_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_i32(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_INT, int32_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  int32_t *p = (int32_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      int64_t v = strtoll(s, NULL, 0); // TODO: check if strtoll fail or not
      *p = (int32_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_u32(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_UINT, uint32_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  uint32_t *p = (uint32_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      uint64_t v = strtoull(s, NULL, 0); // TODO: check if strtoull fail or not
      *p = (uint32_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_i64(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_BIGINT, int64_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  int64_t *p = (int64_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      int64_t v = strtoll(s, NULL, 0); // TODO: check if strtoll fail or not
      *p = (int64_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_u64(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_UBIGINT, uint64_t);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  uint64_t *p = (uint64_t*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      uint64_t v = strtoull(s, NULL, 0); // TODO: check if strtoull fail or not
      *p = (uint64_t)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_flt(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_FLOAT, float);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  float *p = (float*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      float v = strtof(s, NULL); // TODO: check if strtof fail or not
      *p = (float)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_by_dbl(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  const exec_case_row_t *params    = _case->params;

  PREPARE_SET_BIND(TSDB_DATA_TYPE_DOUBLE, double);
  if (!bind->buffer || !bind->is_null) {
    DBGE("out of memory");
    return -1;
  }

  double *p = (double*)bind->buffer;
  for (int j=0; j<nr_params_rows; ++j, ++p) {
    const exec_case_row_t *row = params + j;
    const char *s = row->cols[idx];
    if (s == EXEC_COL_END) s = NULL;
    if (s) {
      double v = strtod(s, NULL);  // TODO: check if strtod fail or not
      *p = (double)v;
      bind->is_null[j]   = 0;
    } else {
      bind->is_null[j]   = 1;
    }
  }

  return 0;
}

static int prepare_data(TAOS_STMT *stmt, const exec_case_t *_case, size_t idx, TAOS_FIELD_E *meta, TAOS_MULTI_BIND *bind, int nr_params_rows)
{
  switch (meta->type) {
    case TSDB_DATA_TYPE_NULL:
      return prepare_by_null_type(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_VARCHAR:
      return prepare_by_varchar(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_NCHAR:
      return prepare_by_nchar(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_TIMESTAMP:
      return prepare_by_timestamp(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_BOOL:
      return prepare_by_bit(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_TINYINT:
      return prepare_by_i8(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_UTINYINT:
      return prepare_by_u8(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_SMALLINT:
      return prepare_by_i16(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_USMALLINT:
      return prepare_by_u16(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_INT:
      return prepare_by_i32(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_UINT:
      return prepare_by_u32(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_BIGINT:
      return prepare_by_i64(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_UBIGINT:
      return prepare_by_u64(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_FLOAT:
      return prepare_by_flt(stmt, _case, idx, meta, bind, nr_params_rows);
    case TSDB_DATA_TYPE_DOUBLE:
      return prepare_by_dbl(stmt, _case, idx, meta, bind, nr_params_rows);
    default:
      DBGE("[%d]%s: not implemented yet", meta->type, tsdb_data_type_name(meta->type));
      return -1;
  }
}

static int run_exec_by_case(TAOS_STMT *stmt, const exec_case_t *_case, TAOS_MULTI_BIND *mbs, size_t nr_mbs)
{
  int r = 0;
  int e = 0;

  int                    line      = _case->line;
  const char            *sql       = _case->sql;
  const exec_case_row_t *params    = _case->params;
  const exec_case_row_t *cells     = _case->cells;
  int nr_params_rows, nr_params_cols;
  int nr_cells_rows,  nr_cells_cols;
  TAOS_FIELD_E        params_meta[60] = {0};

  exec_case_row_get_size(params, params + sizeof(_case->params) / sizeof(_case->params[0]), &nr_params_rows, &nr_params_cols);
  exec_case_row_get_size(cells, cells + sizeof(_case->cells) / sizeof(_case->cells[0]), &nr_cells_rows, &nr_cells_cols);
  DBGE("params:%d/%d", nr_params_rows, nr_params_cols);

  if (nr_params_cols > nr_mbs) {
    DBGE("@[%d]: bad test case", line);
    return -1;
  }

  int is_insert = 0;
  int nr_params = 0;

  r = taos_stmt_prepare2(stmt, sql, strlen(sql));
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_prepare2(%s) failed", line, sql);
    return -1;
  }

  r = taos_stmt_is_insert(stmt, &is_insert);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_is_insert failed", line);
    return -1;
  }
  if (is_insert != _case->is_insert) {
    DBGE("@[%d]: in [%s], expecting %s, but got ==%s==", line, sql, _case->is_insert ? "insert" : "non-insert", is_insert ? "insert" : "non-insert");
    return -1;
  }

  r = taos_stmt_num_params(stmt, &nr_params);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_is_insert failed", line);
    return -1;
  }
  if (nr_params != nr_params_cols) {
    DBGE("@[%d]: in [%s], expecting %d params, but got ==%d==", line, sql, nr_params_cols, nr_params);
    return -1;
  }

  r = taos_stmt_get_params2(stmt, params_meta, sizeof(params_meta) / sizeof(params_meta[0]), &nr_params);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_is_insert failed", line);
    return -1;
  }
  if (nr_params != nr_params_cols) {
    DBGE("@[%d]: in [%s], expecting %d params, but got ==%d==", line, sql, nr_params_cols, nr_params);
    return -1;
  }
  if (nr_params > sizeof(params_meta) / sizeof(params_meta[0])) {
    DBGE("@[%d]: bad test case", line);
    return -1;
  }

  for (size_t i=0; i<nr_params; ++i) {
    TAOS_FIELD_E    *meta = params_meta + i;
    TAOS_MULTI_BIND *bind = mbs + i;
    r = prepare_data(stmt, _case, i, meta, bind, nr_params_rows);
    if (r) return -1;
    bind->num = nr_params_rows;
  }

  for (size_t i=0; 0 && i<nr_params_cols; ++i) {
    TAOS_FIELD_E    *meta = params_meta + i;
    DBGE("     name:[%.*s]", (int)sizeof(meta->name), meta->name);
    DBGE("     type:[%d]%s", meta->type, tsdb_data_type_name(meta->type));
    DBGE("precision:[%d]", meta->precision);
    DBGE("    scale:[%d]", meta->scale);
    DBGE("    bytes:[%d]", meta->bytes);
    TAOS_MULTI_BIND *bind = mbs + i;
    DBGE("  buffer_type:[%d]%s", bind->buffer_type, tsdb_data_type_name(bind->buffer_type));
    DBGE(" bind->buffer:%p", bind->buffer);
    DBGE("buffer_length:%zd", (size_t)bind->buffer_length);
    DBGE("       length:%p", bind->length);
    DBGE("      is_null:%p", bind->is_null);
  }

  r = taos_stmt_bind_params2(stmt, mbs, nr_params_cols);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_bind_params2 failed", line);
    return -1;
  }

  DBGE("@[%d]: in [%s]...", line, sql);
  r = taos_stmt_execute(stmt);
  if (r) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_execute failed", line);
    return -1;
  }

  TAOS_RES *res = taos_stmt_use_result(stmt);
  e = taos_errno(res);
  if (e) {
    e = taos_errno(NULL);
    CHK_STMT(stmt, e, "@[%d]: taos_stmt_use_result failed", line);
    return -1;
  }
  if (((!!is_insert)^(!res))) {
    DBGE("@[%d]: seems internal logic error, is_insert/res:%d/%p", line, is_insert, res);
    return -1;
  }

  if (res && !is_insert) {
    TAOS_ROW     row;
    int          rows       = 0;
    int          num_fields = taos_field_count(res);
    TAOS_FIELD *fields      = taos_fetch_fields(res);

    LOGE("@[%d]: %s", line, sql);
    LOGE("num_fields = %d", num_fields);
    LOGE("results:");
    // fetch the records row by row
    while ((row = taos_fetch_row(res))) {
      char temp[4096] = {0};
      rows++;
      taos_print_row(temp, row, fields, num_fields);
      LOGE("%s", temp);
    }
  }

  return 0;
}

static int run_exec(TAOS *conn)
{
  int r = 0;

  const char *sqls[] = {
    "drop database if exists foo",
    "create database if not exists foo",
    "use foo",
    "create table t (ts timestamp, nm varchar(20), i32 int)",
    "create stable st (ts timestamp, nm varchar(20), i32 int) tags (tn varchar(20), t32 int)",
    "insert into aef using st (tn, t32) tags ('aef-1', 32) (ts, nm, i32) values (1726146779123, '99', 23)",
    "create table tall(ts timestamp, b bool, i8 tinyint, u8 tinyint unsigned, i16 smallint, u16 smallint unsigned, i32 int, u32 int unsigned, i64 bigint, u64 bigint unsigned, flt float, dbl double, nm varchar(20), mm nchar(20), bb varbinary(20))",
    "create stable stall(ts timestamp, b bool, i8 tinyint, u8 tinyint unsigned, i16 smallint, u16 smallint unsigned, i32 int, u32 int unsigned, i64 bigint, u64 bigint unsigned, flt float, dbl double, nm varchar(20), mm nchar(20), bb varbinary(20)) tags (tn varchar(20))",
    "insert into tall1 using stall (tn) tags ('tall-1') (ts, b, i8, u8, i16, u16, i32, u32, i64, u64, flt, dbl, nm) values "
    "(1726146779100, 1, 127, 127, 32767, 32767, 2147483647, 2147483647, 9223372036854775807, 9223372036854775807, 2.34, 5.67, 'hello')",
  };

  r = run_sqls(conn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) goto end;

  const exec_case_t _cases[] = {
    {
      .line      = __LINE__,
      .sql       = "select nm from t",
      .is_insert = 0,
      .params    = {
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {"hello", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into ? (ts, b, i8, u8, i16, u16, i32, u32, i64, u64, flt, dbl, nm) "
                   "          values (?,  ?, ?,  ?,  ?,   ?,   ?,   ?,   ?,   ?,   ?,   ?,   ?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"tall1", "1726146779200", "1", "-127", "255", "-32768", "65535", "-2147483647", "4294967295",
                    "-9223372036854775808", "18446744073709551615", "1.23", "4.56", "helloworld",
                    EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "select * from tall1",
      .is_insert = 0,
      .params    = {
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into tall (ts, b, i8, u8, i16, u16, i32, u32, i64, u64, flt, dbl, nm) "
                   "          values (?,  ?, ?,  ?,  ?,   ?,   ?,   ?,   ?,   ?,   ?,   ?,   ?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"1726146779000", "1", "-128", "255", "-32768", "65535", "-2147483647", "4294967295",
                    "-9223372036854775808", "18446744073709551615", "1.23", "4.56", "hello",
                    EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "select * from tall",
      .is_insert = 0,
      .params    = {
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into t (ts, nm, i32) values(?, ?, ?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"1726146779000", "yes", "987", EXEC_COL_END}},
        {__LINE__, {"1726146779001", NULL,  NULL,  EXEC_COL_END}},
        {__LINE__, {"1726146779002", "123", "789", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {"hello", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "select * from t",
      .is_insert = 0,
      .params    = {
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into t (ts, nm) values(now()+1s, 'world')",
      .is_insert = 1,
      .params    = {
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {"hello", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "select nm from t where nm = ?",
      .is_insert = 0,
      .params    = {
        {__LINE__, {"yes", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {"hello", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into abc using st (tn, t32) tags ('hello', 5) (ts, nm, i32) values (1726146779000, 'hello-1', 7)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into abc (ts, nm, i32) values (?,?,?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"1726146779001", "world-2", "7", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into abc using st (tn, t32) tags (?, ?) (ts, nm, i32) values (?,?,?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"hello", "5", "1726146779002", "foo", "3", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into ? using st (tn, t32) tags (?, ?) (ts, nm, i32) values (?,?,?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"abc", "world", "5", "1726146779003", "bar", "4", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      .line      = __LINE__,
      .sql       = "insert into ? using st (tn, t32) tags (?, ?) (ts, nm, i32) values (?,?,?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"def", "world", "5", "1726146779004", "def1", "1", EXEC_COL_END}},
        {__LINE__, {"def", "world", "5", "1726146779005", "def2", "2", EXEC_COL_END}},
        {__LINE__, {"lmn", "foolm", "5", "1726146779014", "lmn1", "5", EXEC_COL_END}},
        {__LINE__, {"lmn", "foolm", "5", "1726146779015", "lmn2", "6", EXEC_COL_END}},
        {__LINE__, {"lmn", "foolm", "5", "1726146779016", "lmn3", "7", EXEC_COL_END}},
        {__LINE__, {"lmn", "foolm", "5", "1726146779017", "lmn4", "8", EXEC_COL_END}},
        {__LINE__, {"def", "world", "5", "1726146779006", "def3", "3", EXEC_COL_END}},
        {__LINE__, {"def", "world", "5", "1726146779007", "def4", "4", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      // TODO: do convertion
      .line      = __LINE__,
      .sql       = "insert into ?                         (ts, nm, i32) values(?, ?, ?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"t", "1726146978100", "yes", "987", EXEC_COL_END}},
        {__LINE__, {"t", "1726146978101", NULL,  NULL,  EXEC_COL_END}},
        {__LINE__, {"t", "1726146978102", "123", "789", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {"hello", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
    },{
      // TODO: do convertion
      .line      = __LINE__,
      .sql       = "insert into ? (ts, nm, i32) values (?,?,?)",
      .is_insert = 1,
      .params    = {
        {__LINE__, {"def", "1726146779014", "def11", "11", EXEC_COL_END}},
        {__LINE__, {"def", "1726146779015", "def12", "12", EXEC_COL_END}},
        {__LINE__, {"def", "1726146779016", "def13", "13", EXEC_COL_END}},
        {__LINE__, {"aef", "1726146779124", "aef11", "91", EXEC_COL_END}},
        {__LINE__, {"aef", "1726146779125", "aef12", "92", EXEC_COL_END}},
        {__LINE__, {"def", "1726146779017", "def14", "14", EXEC_COL_END}},
        {__LINE__, {"aef", "1726146779126", "aef13", "93", EXEC_COL_END}},
        {__LINE__, {"aef", "1726146779127", "aef14", "94", EXEC_COL_END}},
        {__LINE__, {EXEC_COL_END}},
      },
      .cells     = {
        {__LINE__, {EXEC_COL_END}},
      },
    }
  };

  TAOS_STMT *stmt = taos_stmt_init(conn);
  int e = taos_errno(NULL);
  if (!stmt) {
    CHK_STMT(stmt, e, "taos_stmt_init failed");
    r = -1;
    goto end;
  }

  for (size_t i=0; i<sizeof(_cases)/sizeof(_cases[0]); ++i) {
    const exec_case_t *_case = _cases + i;
    if (_case->line == 0) continue;

    TAOS_MULTI_BIND mbs[60] = {0};
    size_t          nr_mbs  = sizeof(mbs)/sizeof(mbs[0]);
    r = run_exec_by_case(stmt, _case, mbs, nr_mbs);
    release_TAOS_MULTI_BINDs(mbs, nr_mbs);
    if (r) break;
  }
  taos_stmt_close(stmt);

end:
  return r;
}

static int run(TAOS *conn)
{
  int r = 0;

  const char *sqls[] = {
    "drop database if exists foo",
    "create database if not exists foo",
    "use foo",
    "create table bar (ts timestamp, name varchar(20))",
    "insert into bar (ts, name) values (now(), 'hello')",
  };
  r = run_sqls(conn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  if (1) r = run_params_describe(conn);
  if (r) return -1;

  r = run_exec(conn);
  if (r) return -1;

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

  const char *ip           = NULL;
  const char *user         = NULL;
  const char *pass         = NULL;
  const char *db           = NULL;
  uint16_t    port         = 0;

  TAOS *conn = taos_connect(ip, user, pass, db, port);
  CHK_TAOS(conn, "taos_connect failed");
  if (conn) r = run(conn);
  if (conn) taos_close(conn);
  return !!r;
}

