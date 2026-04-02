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

#include "taos_helpers.h"

#include "../test_helper.h"
#include "iconv_wrapper.h"

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef enum stage_s          stage_t;
enum stage_s {
  STAGE_INITED,
  STAGE_CONNECTED,
  STAGE_STATEMENT,
};

typedef struct arg_s                arg_t;

typedef int (*regress_f)(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt);

struct arg_s {
  const char           *ip;
  const char           *uid;
  const char           *pwd;
  const char           *db;
  uint16_t              port;

  const char           *name;

  const char           *regname;
  regress_f             regress;
};

typedef struct sql_s                     sql_t;
struct sql_s {
  const char            *sql;
  int                    __line__;
};

static int _executes(TAOS *taos, const sql_t *sqls, size_t nr)
{
  int r = 0;
  for (size_t i=0; i<nr; ++i) {
    const sql_t *sql = sqls + i;

    TAOS_RES *res = CALL_taos_query(taos, sql->sql);
    int e = taos_errno(res);
    if (e) {
      E("executing sql @[%dL]:%s\n"
        "failed:[%d]%s",
        sql->__line__, sql->sql,
        e, taos_errstr(res));
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

  return 0;
}

static int _dummy(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stage;
  (void)taos;
  (void)stmt;
  return 0;
}

static int _query(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage != STAGE_CONNECTED) return 0;

#define RECORD(x,y) {x, __LINE__, y}
  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    RECORD("drop database if exists foo", 1),
    RECORD("create database if not exists foo", 1),
    RECORD("insert into foo.t (ts, name) values (now(), 'a')", 0),
    RECORD("create table foo.t (ts timestamp, name varchar(20))", 1),
    RECORD("insert into foo.t (ts, name) values (now(), 'a')", 1),
    RECORD("insert into foo.t (ts, name) values (now(), ?)", 0),
  };
#undef RECORD

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

  return 0;
}

static int _prepare_on_connected(TAOS *taos)
{
  const sql_t sqls[] = {
    {"drop database if exists foo", __LINE__},
    {"create database if not exists foo", __LINE__},
    {"use foo", __LINE__},
    {"create table t (ts timestamp, name varchar(20), age int, sex int)", __LINE__},
    {"create table st1 (ts timestamp, age int) tags (name varchar(20))", __LINE__},
    {"insert into suzhou using st1 tags ('suzhou') values (now(), 23)", __LINE__},
    {"insert into suzhou (ts, age) values (now(), 24)", __LINE__},
    {"insert into suzhou using st1 tags ('suzhou') (ts) values (now())", __LINE__},
  };

  const size_t nr = sizeof(sqls) / sizeof(sqls[0]);
  return _executes(taos, sqls, nr);
}

typedef struct prepare_case_s                  prepare_case_t;
struct prepare_case_s {
    int                        __line__;
    const char                *sql;

    int                        errCode;

    int                        nr_fields;
    int                        nr_tags;

    uint8_t                    req_tbname:1;
    uint8_t                    is_insert:1;
};

static int _prepare_describe_insert(TAOS_STMT *stmt, const prepare_case_t *prepare_case)
{
  int r = 0;

  const char     *_sql               = prepare_case->sql;
  int             __line__           = prepare_case->__line__;
  int             _errCode           = prepare_case->errCode;
  uint8_t         _req_tbname        = prepare_case->req_tbname;
  int             _nr_tags           = prepare_case->nr_tags;
  int             _nr_fields         = prepare_case->nr_fields;

  int fieldNum = 0;
  TAOS_FIELD_E *fields = NULL;

  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;

  r = CALL_taos_stmt_get_tag_fields(stmt, &fieldNum, &fields);
  if (r) {
    if (taos_errno(NULL) == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      if (_errCode == taos_errno(NULL)) return 0;
      E("sql @[%dL]::%s", __line__, _sql);
      E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
      return -1;
    }
    if (taos_errno(NULL) == TSDB_CODE_TSC_STMT_API_ERROR) {
      r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
      if (r) {
        if (_errCode == taos_errno(NULL)) return 0;
        E("sql @[%dL]::%s", __line__, _sql);
        E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
        return -1;
      }
      CALL_taos_stmt_reclaim_fields(stmt, cols); cols = NULL;

      if (_req_tbname) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("expected tblname-parameter, but got ==[none]==");
        return -1;
      }

      if (_nr_tags != fieldNum) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("expected %d tags, but got ==%d==", _nr_tags, fieldNum);
        return -1;
      }

      if (_nr_fields != colNum) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("expected %d cols, but got ==%d==", _nr_fields, colNum);
        return -1;
      }

      if (_errCode) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("errCode expected to be [0x%x/%d], but got ==TSDB_CODE_SUCCESS==", _errCode, _errCode);
        return -1;
      }
      return 0;
    }
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }
  CALL_taos_stmt_reclaim_fields(stmt, fields); fields = NULL;

  r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }
  CALL_taos_stmt_reclaim_fields(stmt, cols); cols = NULL;

  if (_req_tbname) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expected tblname-parameter, but got ==[none]==");
    return -1;
  }

  if (_nr_tags != fieldNum) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expected %d tags, but got ==%d==", _nr_tags, fieldNum);
    return -1;
  }

  if (_nr_fields != colNum) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expected %d cols, but got ==%d==", _nr_fields, colNum);
    return -1;
  }

  if (_errCode) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("errCode expected to be [0x%x/%d], but got ==TSDB_CODE_SUCCESS==", _errCode, _errCode);
    return -1;
  }
  return 0;
}

static int _prepare_describe_non_insert(TAOS_STMT *stmt, const prepare_case_t *prepare_case)
{
  int r = 0;

  const char     *_sql               = prepare_case->sql;
  int             __line__           = prepare_case->__line__;
  int             _errCode           = prepare_case->errCode;
  int             _nr_fields         = prepare_case->nr_fields;

  int num_params = 0;

  r = CALL_taos_stmt_num_params(stmt, &num_params);
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }

  if (num_params != _nr_fields) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expecting %d params, but got ==%d==", _nr_fields, num_params);
    return -1;
  }

  if (_errCode) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("errCode expected to be [0x%x/%d], but got ==TSDB_CODE_SUCCESS==", _errCode, _errCode);
    return -1;
  }

  return 0;
}

static int _prepare_describe_sql(TAOS_STMT *stmt, const prepare_case_t *prepare_case)
{
  int r = 0;

  const char        *_sql               = prepare_case->sql;
  int                __line__           = prepare_case->__line__;
  int                _errCode           = prepare_case->errCode;
  uint8_t            _is_insert         = prepare_case->is_insert;

  r = CALL_taos_stmt_prepare(stmt, _sql, (unsigned long)strlen(_sql));
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }

  int is_insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &is_insert);
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }

  if ((!!is_insert) ^ (!!_is_insert)) {
    E("sql @[%dL]::%s", __line__, _sql);
    if (is_insert) {
      E("expected insert statement, but failed");
    } else {
      E("expected non-insert statement, but failed");
    }
    return -1;
  }

  if (is_insert) {
    return _prepare_describe_insert(stmt, prepare_case);
  }

  return _prepare_describe_non_insert(stmt, prepare_case);
}

static int _prepare_on_statement(TAOS *taos, TAOS_STMT *stmt)
{
  (void)taos;

  int r = 0;

  const prepare_case_t _cases[] = {
    {__LINE__, "insert into ? (ts, age) values (?, ?)", TSDB_CODE_PAR_TABLE_NOT_EXIST, 0, 0, 0, 1},
    {__LINE__, "insert into ? (ts, sex) values (?, ?)", TSDB_CODE_PAR_TABLE_NOT_EXIST, 0, 0, 0, 1},
    {__LINE__, "insert into suzhou using ? tags (?) values (?, ?)", TSDB_CODE_PAR_TABLE_NOT_EXIST, 0, 0, 0, 1}, // NOTE: this is totally not recoverable
    {__LINE__, "insert into ? using st1 tags (?) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1}, // NOTE: lazy-meta-loading

    {__LINE__, "select * from t where ts > ? and name = ?", TSDB_CODE_SUCCESS, 2, 0, 0, 0},
    {__LINE__, "insert into ? values (?, ?)", TSDB_CODE_PAR_TABLE_NOT_EXIST, 2, 0, 1, 1},
    {__LINE__, "insert into suzhou using st1 tags (?) values (?, ?)", TSDB_CODE_SUCCESS, 2, 1, 0, 1},

    // fixed: since TDengine:02736bad02beadc60d7a93490365f00b69e3c64d
    // {__LINE__, "insert into suzhou using st1 tags ('suzhou') values (now(), 3)", TSDB_CODE_TSC_INVALID_OPERATION, 2, 1, 0, 1}, // FLAW: no parameter-place-marker, but succeed

    {__LINE__, "insert into ? using st1 tags (?) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},

    {__LINE__, "insert into ? using st1 tags (?) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},
    {__LINE__, "insert into t (ts, name) values (?, ?)", TSDB_CODE_SUCCESS, 2, 0, 0, 1},

    // since TDengine:02736bad02beadc60d7a93490365f00b69e3c64d
    {__LINE__, "insert into t (ts, name) values (?, 'a')", TSDB_CODE_SUCCESS, 2, 0, 0, 1},

    {__LINE__, "insert into ? (ts, name) values (?, ?)", TSDB_CODE_PAR_TABLE_NOT_EXIST, 0, 0, 0, 1},

    {__LINE__, "insert into t (ts, name) values (now(), 'a')", TSDB_CODE_SUCCESS, 2, 0, 0, 1},

    {__LINE__, "insert into suzhou values (?, ?)", TSDB_CODE_SUCCESS, 2, 0, 0, 1},
    {__LINE__, "select * from t where ts > ? and name = ? foo = ?", TSDB_CODE_PAR_SYNTAX_ERROR, 0, 0, 0, 0},    // syntax error will corrupt stmt environment, so put it last
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i < nr; ++i) {
    const prepare_case_t *prepare_case = _cases + i;

    r = _prepare_describe_sql(stmt, prepare_case);
    if (r) return -1;
  }

  return 0;
}

static int _prepare(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  switch (stage) {
    case STAGE_INITED:    return 0;
    case STAGE_CONNECTED: return _prepare_on_connected(taos);
    case STAGE_STATEMENT: return _prepare_on_statement(taos, stmt);
    default: return 0;
  }

  return 0;
}

#define NBIT                     (3u)
#define BitPos(_n)               ((_n) & ((1 << NBIT) - 1))
#define BMCharPos(bm_, r_)       ((bm_)[(r_) >> NBIT])
#define colDataIsNull_f(bm_, r_) ((BMCharPos(bm_, r_) & (1u << (7u - BitPos(r_)))) == (1u << (7u - BitPos(r_))))
#define BitmapLen(_n) (((_n) + ((1 << NBIT) - 1)) >> NBIT)

typedef struct block_layout_s {
  int32_t                 magic;
  int32_t                 blockSize;
  int32_t                 rows;
  int32_t                 cols;
  int32_t                 unknowns[3];
} block_layout_t;
#pragma pack(push, 1)
typedef struct field_layout_s {
  int8_t                  type;
  int32_t                 bytes;
} field_layout_t;
typedef struct col_bytes_s {
  int32_t                 bytes;
} col_bytes_t;
#pragma pack(pop)

typedef struct raw_block_s {
  char           *base;
  int             nr_rows;

  block_layout_t       *block;
  field_layout_t       *fields;
  col_bytes_t          *col_bytes;
  char                 *data;

  char                 *col_ptr[20];
} raw_block_t;

static int raw_block_init(raw_block_t *raw_block, int nr_rows, void *pData)
{
  raw_block->base            = (char*)pData;
  raw_block->nr_rows         = nr_rows;

  raw_block->block           = (block_layout_t*)pData;
  if (raw_block->block->rows != raw_block->nr_rows) {
    E("# of rows differ: %d <> %d", raw_block->block->rows, raw_block->nr_rows);
    return -1;
  }
  raw_block->fields          = (field_layout_t*)&raw_block->block[1];
  raw_block->col_bytes       = (col_bytes_t*)&raw_block->fields[raw_block->block->cols];
  raw_block->data            = (char*)&raw_block->col_bytes[raw_block->block->cols];

  return 0;
}

static int raw_block_visit(raw_block_t *raw_block, void *user, int (*cb)(int row, int col, int8_t type, const void *data, size_t len, void *user))
{
  block_layout_t *block     = raw_block->block;
  field_layout_t *fields    = raw_block->fields;
  col_bytes_t    *col_bytes = raw_block->col_bytes;
  char           *data      = raw_block->data;
  char           *data_ptr  = data;

  int nr_rows = block->rows;
  int nr_cols = block->cols;

  for (int i=0; i<nr_cols; ++i) {
    int8_t field_type = fields[i].type;
    int is_fix = 0;
    size_t nr_fix = 0;
    switch (field_type) {
      case TSDB_DATA_TYPE_BOOL: {
        is_fix = 1;
        nr_fix = sizeof(int8_t);
      } break;
      case TSDB_DATA_TYPE_TINYINT: {
        nr_fix = sizeof(int8_t);
        is_fix = 1;
      } break;
      case TSDB_DATA_TYPE_SMALLINT: {
        is_fix = 1;
        nr_fix = sizeof(int16_t);
      } break;
      case TSDB_DATA_TYPE_INT: {
        is_fix = 1;
        nr_fix = sizeof(int32_t);
      } break;
      case TSDB_DATA_TYPE_BIGINT: {
        is_fix = 1;
        nr_fix = sizeof(int64_t);
      } break;
      case TSDB_DATA_TYPE_FLOAT: {
        is_fix = 1;
        nr_fix = sizeof(float);
      } break;
      case TSDB_DATA_TYPE_DOUBLE: {
        is_fix = 1;
        nr_fix = sizeof(double);
      } break;
      case TSDB_DATA_TYPE_VARCHAR: {
      } break;
      case TSDB_DATA_TYPE_TIMESTAMP: {
        is_fix = 1;
        nr_fix = sizeof(int64_t);
      } break;
      case TSDB_DATA_TYPE_NCHAR: {
      } break;
      case TSDB_DATA_TYPE_UTINYINT: {
        is_fix = 1;
        nr_fix = sizeof(int8_t);
      } break;
      case TSDB_DATA_TYPE_USMALLINT: {
        is_fix = 1;
        nr_fix = sizeof(int16_t);
      } break;
      case TSDB_DATA_TYPE_UINT: {
        is_fix = 1;
        nr_fix = sizeof(int32_t);
      } break;
      case TSDB_DATA_TYPE_UBIGINT: {
        is_fix = 1;
        nr_fix = sizeof(int64_t);
      } break;
      case TSDB_DATA_TYPE_JSON: {
      } break;
      // case TSDB_DATA_TYPE_GEOMETRY: {
      // } break;
      // case TSDB_DATA_TYPE_VARBINARY: {
      // } break;
      // case TSDB_DATA_TYPE_DECIMAL: {
      // } break;
      // case TSDB_DATA_TYPE_BLOB: {
      // } break;
      // case TSDB_DATA_TYPE_MEDIUMBLOB: {
      // } break;
      default:
        E("`%s` not supported yet", taos_data_type(field_type));
        return -1;
    }

    size_t nr_head;
    if (is_fix) {
      nr_head = BitmapLen(nr_rows);
      char *data = data_ptr + nr_head;
      for (int j=0; j<block->rows; ++j) {
        if (colDataIsNull_f(data_ptr, j)) {
          if (cb(j, i, field_type, NULL, 0, user)) return -1;
        } else {
          if (cb(j, i, field_type, data + nr_fix*j, nr_fix, user)) return -1;
        }
      }
    } else {
      uint32_t *offsets = (uint32_t*)data_ptr;
      nr_head = sizeof(*offsets) * nr_rows;
      char *data = data_ptr + nr_head;
      size_t len = 0;
      for (int j=0; j<block->rows; ++j) {
        if (offsets[j] == (uint32_t)-1) {
          if (cb(j, i, field_type, NULL, 0, user)) return -1;
        } else {
          len = offsets[j];
          if (cb(j, i, field_type, data + len + 2,  *(uint16_t*)(data + len), user)) return -1;
        }
      }
    }

    data_ptr += nr_head;
    data_ptr += col_bytes[i].bytes;
  }

  return 0;
}

typedef struct fetch_exp_s                  fetch_exp_t;
typedef struct exp_s                        exp_t;
struct exp_s {
  const char            *s;
  size_t                 line;
};

struct fetch_exp_s {
  const exp_t           *exps;
  size_t                 nr_exps;

  size_t                 pos;

  iconv_t                cnv;
  char                  *buf;
  size_t                 cap_buf;
};

static int print_data(int row, int col, int8_t type, const void *data, size_t len, void *user)
{
  char buf[4096]; buf[0] = '\0';
  fetch_exp_t *exp = (fetch_exp_t*)user;

  if (exp->pos >= exp->nr_exps) {
    E("expecting %zd cells at most, but got ==%zd== at least", exp->nr_exps, exp->pos + 1);
    return -1;
  }

  const exp_t *s_exp = exp->exps + exp->pos;

  if (!data && len == 0) {
    snprintf(buf, sizeof(buf), "null");
    fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
    if (strcmp(s_exp->s, buf)) {
      E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
      return -1;
    }
    exp->pos += 1;
    return 0;
  }

  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      A(len == sizeof(int8_t), "internal logic error");
      int8_t v = *(int8_t*)data;
      snprintf(buf, sizeof(buf), "%s", v ? "true" : "false");
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
      A(len == sizeof(int8_t), "internal logic error");
      int8_t v = *(int8_t*)data;
      snprintf(buf, sizeof(buf), "%d", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      A(len == sizeof(int16_t), "internal logic error");
      int16_t v = *(int16_t*)data;
      snprintf(buf, sizeof(buf), "%d", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_INT: {
      A(len == sizeof(int32_t), "internal logic error");
      int32_t v = *(int32_t*)data;
      snprintf(buf, sizeof(buf), "%d", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      A(len == sizeof(int64_t), "internal logic error");
      int64_t v = *(int64_t*)data;
      snprintf(buf, sizeof(buf), "%" PRId64 "", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      A(len == sizeof(float), "internal logic error");
      float v = *(float*)data;
      snprintf(buf, sizeof(buf), "%f", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      A(len == sizeof(double), "internal logic error");
      double v = *(double*)data;
      snprintf(buf, sizeof(buf), "%f", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      const char *v = (const char*)data;
      snprintf(buf, sizeof(buf), "\"%.*s\"", (int)len, v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      A(len == sizeof(uint64_t), "internal logic error");
      uint64_t v = *(uint64_t*)data;
      snprintf(buf, sizeof(buf), "%" PRIu64 "", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      char    *inbuf         = (char*)data;
      size_t   inbytesleft   = len;
      char    *outbuf        = exp->buf;
      size_t   outbytesleft  = exp->cap_buf;
      *outbuf++ = '"';
      --outbytesleft;

      size_t n = iconv(exp->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
      iconv(exp->cnv, NULL, NULL, NULL, NULL);
      if (n == (size_t)-1) {
        fprintf(stderr, "failed to convert charset:0x");
        for (size_t i=0; i<len; ++i) {
          fprintf(stderr, "%02x", ((const unsigned char*)data)[i]);
        }
        fprintf(stderr, "\n");
        return -1;
      }
      if (inbytesleft) {
        fprintf(stderr, "failed to convert all:0x");
        for (size_t i=0; i<len; ++i) {
          fprintf(stderr, "%02x", ((const unsigned char*)data)[i]);
        }
        fprintf(stderr, "\n");
        return -1;
      }
      if (outbytesleft <= 1) {
        fprintf(stderr, "output buf too small:0x");
        for (size_t i=0; i<len; ++i) {
          fprintf(stderr, "%02x", ((const unsigned char*)data)[i]);
        }
        fprintf(stderr, "\n");
        return -1;
      }
      *outbuf++ = '"';
      *outbuf++ = '\0';
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, exp->buf);
      if (strcmp(s_exp->s, exp->buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, exp->buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      A(len == sizeof(uint8_t), "internal logic error");
      uint8_t v = *(uint8_t*)data;
      snprintf(buf, sizeof(buf), "%u", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      A(len == sizeof(uint16_t), "internal logic error");
      uint16_t v = *(uint16_t*)data;
      snprintf(buf, sizeof(buf), "%u", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_UINT: {
      A(len == sizeof(uint32_t), "internal logic error");
      uint32_t v = *(uint32_t*)data;
      snprintf(buf, sizeof(buf), "%u", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      A(len == sizeof(uint64_t), "internal logic error");
      uint64_t v = *(uint64_t*)data;
      snprintf(buf, sizeof(buf), "%" PRIu64 "", v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    case TSDB_DATA_TYPE_JSON: {
      const char *v = (const char*)data;
      snprintf(buf, sizeof(buf), "\"%.*s\"", (int)len, v);
      fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
      if (strcmp(s_exp->s, buf)) {
        E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
        return -1;
      }
    } break;
    // case TSDB_DATA_TYPE_GEOMETRY: {
    //   const char *v = (const char*)data;
    //   snprintf(buf, sizeof(buf), "\"%.*s\"", (int)len, v);
    //   fprintf(stderr, "[%d,%d]:%s\n", row+1, col+1, buf);
    //   if (strcmp(s_exp->s, buf)) {
    //     E("expecting %s at [%zd], but got ==%s== at [%d,%d]", s_exp->s, s_exp->line, buf, row+1, col+1);
    //     return -1;
    //   }
    // } break;
    // case TSDB_DATA_TYPE_VARBINARY: {
    // } break;
    // case TSDB_DATA_TYPE_DECIMAL: {
    // } break;
    // case TSDB_DATA_TYPE_BLOB: {
    // } break;
    // case TSDB_DATA_TYPE_MEDIUMBLOB: {
    // } break;
    default:
      E("`%s` not supported yet", taos_data_type(type));
      return -1;
  }

  exp->pos += 1;
  return 0;
}

static int _fetch_on_connected_with_exp(TAOS *taos, fetch_exp_t *exp)
{
  int r = 0;

  const char *tocode   = "UTF-8";
  const char *fromcode = "UCS-4LE";
#ifdef _WIN32                /* { */
  tocode = "GB18030"; // FIXME: better check according to code page
#endif                       /* } */
  exp->cnv = iconv_open(tocode, fromcode);
  if (exp->cnv == (iconv_t)-1) {
    E("no conversion from %s to %s", fromcode, tocode);
    return -1;
  }

  exp->cap_buf = 1024 * 1024;
  exp->buf = (char*)malloc(exp->cap_buf);
  if (exp->buf == NULL) {
    exp->cap_buf = 0;
    E("out of memory");
    return -1;
  }

  const sql_t sqls[] = {
    {"drop database if exists foo", __LINE__},
    {"create database if not exists foo", __LINE__},
    {"use foo", __LINE__},
    {"create table t (ts timestamp, name varchar(20), age int, sex int, mark nchar(20))", __LINE__},
    {"insert into t (ts, name, age, sex, mark) values (1688693481425, 'hello', 2, 305419896, '人')", __LINE__},
    {"insert into t (ts, name, age, sex, mark) values (1688693481429, 'worl', 3, 2, null)", __LINE__},
    {"insert into t (ts, name, age, sex, mark) values (1688693481433, null, 305419896, 8, '们')", __LINE__},
    {"insert into t (ts, name, age, sex, mark) values (1688693481438, 'foo', null, 8, '测试')", __LINE__},
    {"insert into t (ts, name, age, sex, mark) values (1688693481443, 'bar', 9, null, null)", __LINE__},
  };

  const size_t nr = sizeof(sqls) / sizeof(sqls[0]);
  r = _executes(taos, sqls, nr);
  if (r) return -1;

#define RECORD(x) {x, __LINE__}
  const exp_t s_exps[] = {
    RECORD("1688693481425"),
    RECORD("1688693481429"),
    RECORD("1688693481433"),
    RECORD("1688693481438"),
    RECORD("1688693481443"),
    RECORD("\"hello\""),
    RECORD("\"worl\""),
    RECORD("null"),
    RECORD("\"foo\""),
    RECORD("\"bar\""),
    RECORD("2"),
    RECORD("3"),
    RECORD("305419896"),
    RECORD("null"),
    RECORD("9"),
    RECORD("305419896"),
    RECORD("2"),
    RECORD("8"),
    RECORD("8"),
    RECORD("null"),
    RECORD("\"人\""),
    RECORD("null"),
    RECORD("\"们\""),
    RECORD("\"测试\""),
    RECORD("null"),
  };
#undef RECORD

  exp->exps = s_exps;
  exp->nr_exps = sizeof(s_exps) / sizeof(s_exps[0]);
  exp->pos = 0;

  const char *sql = "select ts, name, age, sex, mark from foo.t";
  // NOTE: flaw in case union all different resultset, eg.: select name from ta union all select ts from tb
  TAOS_RES *res = CALL_taos_query(taos, sql);
  int e = taos_errno(res);
  if (e) {
    E("executing sql:%s\n"
        "failed:[%d]%s",
        sql,
        e, taos_errstr(res));
    r = -1;
  }
  if (r == 0 && res) {
    const char *tmp_file;
#ifdef _WIN32                /* { */
    tmp_file = "C:\\Windows\\Temp\\block.data";
#else                        /* }{ */
    tmp_file = "/tmp/block.data";
#endif                       /* } */
    FILE *fout = fopen(tmp_file, "w");
    while (r == 0) {
      int numOfRows = 0;
      void *pData = NULL;
      r = CALL_taos_fetch_raw_block(res, &numOfRows, &pData);
      if (r) break;
      if (numOfRows == 0) break;
      raw_block_t raw_block = {0};
      if (raw_block_init(&raw_block, numOfRows, pData)) return -1;
      if (fout) {
        fwrite(pData, 1, raw_block.block->blockSize, fout);
      }
      r = raw_block_visit(&raw_block, exp, print_data);
      if (r) break;
    }
    if (fout) {
      fclose(fout);
      fout = NULL;
    }
  }
  if (res) CALL_taos_free_result(res);
  if (r) return -1;

  return 0;
}

static int _fetch_on_connected(TAOS *taos)
{
  fetch_exp_t exp = {0};

  int r = 0;

  r = _fetch_on_connected_with_exp(taos, &exp);

  if ((size_t)exp.cnv != (size_t)-1) {
    iconv_close(exp.cnv);
    exp.cnv = (iconv_t)-1;
  }
  if (exp.buf) {
    free(exp.buf);
    exp.buf = NULL;
  }

  if (r) return -1;

  return 0;
}

static int _fetch(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

#ifdef _WIN32                /* { */
  if (1) return 0;
#endif                       /* } */

  switch (stage) {
    case STAGE_INITED:    return 0;
    case STAGE_CONNECTED: return _fetch_on_connected(taos);
    case STAGE_STATEMENT: return 0;
    default: return 0;
  }

  return 0;
}

static int _prepare_insert(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into foo.t (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  int64_t               ts            = 1665025866843L;
  const int32_t         ts_len        = (int32_t)sizeof(ts);
  const char            ts_is_null    = (char)0;

  int                   v             = 223;
  const int32_t         v_len         = (int32_t)sizeof(v);
  const char            v_is_null     = (char)0;

  const char           *name          = "中国";
  const int32_t         name_len      = (int32_t)strlen(name);
  const char            name_is_null  = (char)0;

  const char           *mark          = "苏州";
  const int32_t         mark_len      = (int32_t)strlen(mark);
  const char            mark_is_null  = (char)0;

  TAOS_MULTI_BIND mbs[4] = {0};
  mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
  mbs[0].buffer             = (void*)&ts;
  mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
  mbs[0].length             = (int32_t*)&ts_len;
  mbs[0].is_null            = (char*)&ts_is_null;
  mbs[0].num = 1;

  mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
  mbs[1].buffer             = (void*)&v;
  mbs[1].buffer_length      = (uintptr_t)sizeof(v);
  mbs[1].length             = (int32_t*)&v_len;
  mbs[1].is_null            = (char*)&v_is_null;
  mbs[1].num = 1;

  mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
  mbs[2].buffer             = (void*)name;
  mbs[2].buffer_length      = (uintptr_t)name_len;
  mbs[2].length             = (int32_t*)&name_len;
  mbs[2].is_null            = (char*)&name_is_null;
  mbs[2].num = 1;

  mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
  mbs[3].buffer             = (void*)mark;
  mbs[3].buffer_length      = (uintptr_t)mark_len;
  mbs[3].length             = (int32_t*)&mark_len;
  mbs[3].is_null            = (char*)&mark_is_null;
  mbs[3].num = 1;

  r = CALL_taos_stmt_bind_param_batch(stmt, &mbs[0]);
  if (r) return -1;

  r = CALL_taos_stmt_add_batch(stmt);
  if (r) return -1;

  r = CALL_taos_stmt_execute(stmt);
  if (r) return -1;

  return 0;
}

static int _prepare_insert_many(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(21), mark nchar(20))", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into foo.t (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  int64_t               ts[2]                 = {1665025866843L, 1665025866844L};
  const int32_t         ts_len[2]             = {(int32_t)sizeof(ts[0]), (int32_t)sizeof(ts[1])};
  uintptr_t             ts_buf_len            = (uintptr_t)sizeof(ts[0]);
  A(ts_buf_len == 8, "");
  const char            ts_is_null[2]         = {(char)0, (char)0};

  int                   v[2]                  = {223, 224};
  const int32_t         v_len[2]              = {(int32_t)sizeof(v[0]), (int32_t)sizeof(v[1])};
  uintptr_t             v_buf_len             = (uintptr_t)sizeof(v[0]);
  const char            v_is_null[2]          = {(char)0, (char)0};

  const char            name[2][40]           = {"中国","中华人民共和国"};
  const int32_t         name_len[2]           = {(int32_t)strlen(name[0]), (int32_t)strlen(name[1])};
  uintptr_t             name_buf_len          = (int32_t)(sizeof(name[0])/sizeof(name[0][0]));
  A(name_buf_len == 40, "");
  const char            name_is_null[2]       = {(char)0, (char)0};

  const char            mark[2][40]           = {"苏州","无锡"};
  const int32_t         mark_len[2]           = {(int32_t)strlen(mark[0]), (int32_t)strlen(mark[1])};
  uintptr_t             mark_buf_len          = (int32_t)(sizeof(mark[0])/sizeof(mark[0][0]));
  A(mark_buf_len == 40, "");
  const char            mark_is_null[2]       = {(char)0, (char)0};

  TAOS_MULTI_BIND mbs[4] = {0};
  mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
  mbs[0].buffer             = (void*)&ts[0];
  mbs[0].buffer_length      = ts_buf_len;
  mbs[0].length             = (int32_t*)&ts_len[0];
  mbs[0].is_null            = (char*)&ts_is_null[0];
  mbs[0].num = 2;

  mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
  mbs[1].buffer             = (void*)&v[0];
  mbs[1].buffer_length      = v_buf_len;
  mbs[1].length             = (int32_t*)&v_len[0];
  mbs[1].is_null            = (char*)&v_is_null[0];
  mbs[1].num = 2;

  mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
  mbs[2].buffer             = (void*)&name[0];
  A((void*)&name[0] == (void*)&name[0][0], "");
  mbs[2].buffer_length      = name_buf_len;
  mbs[2].length             = (int32_t*)&name_len[0];
  mbs[2].is_null            = (char*)&name_is_null[0];
  mbs[2].num = 2;

  mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
  mbs[3].buffer             = (void*)&mark[0];
  mbs[3].buffer_length      = mark_buf_len;
  mbs[3].length             = (int32_t*)&mark_len[0];
  mbs[3].is_null            = (char*)&mark_is_null[0];
  mbs[3].num = 2;

  r = CALL_taos_stmt_bind_param_batch(stmt, &mbs[0]);
  if (r) return -1;

  r = CALL_taos_stmt_add_batch(stmt);
  if (r) return -1;

  r = CALL_taos_stmt_execute(stmt);
  if (r) return -1;

  return 0;
}

static int _prepare_insert_normal_dynamic(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into ? (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  int64_t               ts            = 1665025866843L;
  const int32_t         ts_len        = (int32_t)sizeof(ts);
  const char            ts_is_null    = (char)0;

  int                   v             = 223;
  const int32_t         v_len         = (int32_t)sizeof(v);
  const char            v_is_null     = (char)0;

  const char           *name          = "中国";
  const int32_t         name_len      = (int32_t)strlen(name);
  const char            name_is_null  = (char)0;

  const char           *mark          = "苏州";
  const int32_t         mark_len      = (int32_t)strlen(mark);
  const char            mark_is_null  = (char)0;

  TAOS_MULTI_BIND mbs[4] = {0};
  mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
  mbs[0].buffer             = (void*)&ts;
  mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
  mbs[0].length             = (int32_t*)&ts_len;
  mbs[0].is_null            = (char*)&ts_is_null;
  mbs[0].num = 1;

  mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
  mbs[1].buffer             = (void*)&v;
  mbs[1].buffer_length      = (uintptr_t)sizeof(v);
  mbs[1].length             = (int32_t*)&v_len;
  mbs[1].is_null            = (char*)&v_is_null;
  mbs[1].num = 1;

  mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
  mbs[2].buffer             = (void*)name;
  mbs[2].buffer_length      = (uintptr_t)name_len;
  mbs[2].length             = (int32_t*)&name_len;
  mbs[2].is_null            = (char*)&name_is_null;
  mbs[2].num = 1;

  mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
  mbs[3].buffer             = (void*)mark;
  mbs[3].buffer_length      = (uintptr_t)mark_len;
  mbs[3].length             = (int32_t*)&mark_len;
  mbs[3].is_null            = (char*)&mark_is_null;
  mbs[3].num = 1;

  const char *tbl = "t";
  r = CALL_taos_stmt_set_tbname(stmt, tbl);
  if (r) return -1;

  r = CALL_taos_stmt_bind_param_batch(stmt, &mbs[0]);
  if (r) return -1;

  r = CALL_taos_stmt_add_batch(stmt);
  if (r) return -1;

  r = CALL_taos_stmt_execute(stmt);
  if (r) return -1;

  return 0;
}

static int _prepare_insert_normals_dynamic(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"create table foo.m (ts timestamp, v int, name varchar(20), mark nchar(20))", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

  const char *sql = "insert into ? (ts, v, name, mark) values (?, ?, ?, ?)";
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &insert);
  if (r) return -1;
  A(insert == 1, "");

  {
    int64_t               ts            = 1665025866843L;
    const int32_t         ts_len        = (int32_t)sizeof(ts);
    const char            ts_is_null    = (char)0;

    int                   v             = 223;
    const int32_t         v_len         = (int32_t)sizeof(v);
    const char            v_is_null     = (char)0;

    char                  name[]        = "中国";
    const int32_t         name_len      = (int32_t)strlen(name);
    const char            name_is_null  = (char)0;

    char                  mark[]        = "苏州";
    const int32_t         mark_len      = (int32_t)strlen(mark);
    const char            mark_is_null  = (char)0;

    TAOS_MULTI_BIND mbs[4] = {0};
    mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
    mbs[0].buffer             = (void*)&ts;
    mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
    mbs[0].length             = (int32_t*)&ts_len;
    mbs[0].is_null            = (char*)&ts_is_null;
    mbs[0].num = 1;

    mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
    mbs[1].buffer             = (void*)&v;
    mbs[1].buffer_length      = (uintptr_t)sizeof(v);
    mbs[1].length             = (int32_t*)&v_len;
    mbs[1].is_null            = (char*)&v_is_null;
    mbs[1].num = 1;

    mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
    mbs[2].buffer             = (void*)name;
    mbs[2].buffer_length      = (uintptr_t)name_len;
    mbs[2].length             = (int32_t*)&name_len;
    mbs[2].is_null            = (char*)&name_is_null;
    mbs[2].num = 1;

    mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
    mbs[3].buffer             = (void*)mark;
    mbs[3].buffer_length      = (uintptr_t)mark_len;
    mbs[3].length             = (int32_t*)&mark_len;
    mbs[3].is_null            = (char*)&mark_is_null;
    mbs[3].num = 1;

    const char *tbl = "t";
    r = CALL_taos_stmt_set_tbname(stmt, tbl);
    if (r) return -1;

    r = CALL_taos_stmt_bind_param_batch(stmt, &mbs[0]);
    if (r) return -1;

    r = CALL_taos_stmt_add_batch(stmt);
    if (r) return -1;

    memset(mbs, 0, sizeof(mbs));
    memset(name, 0, sizeof(name));
    memset(mark, 0, sizeof(mark));
  }
  {
    int64_t               ts            = 1665025866844L;
    const int32_t         ts_len        = (int32_t)sizeof(ts);
    const char            ts_is_null    = (char)0;

    int                   v             = 224;
    const int32_t         v_len         = (int32_t)sizeof(v);
    const char            v_is_null     = (char)0;

    const char           *name          = "中国x";
    const int32_t         name_len      = (int32_t)strlen(name);
    const char            name_is_null  = (char)0;

    const char           *mark          = "苏州x";
    const int32_t         mark_len      = (int32_t)strlen(mark);
    const char            mark_is_null  = (char)0;

    TAOS_MULTI_BIND mbs[4] = {0};
    mbs[0].buffer_type        = TSDB_DATA_TYPE_TIMESTAMP;
    mbs[0].buffer             = (void*)&ts;
    mbs[0].buffer_length      = (uintptr_t)sizeof(ts);
    mbs[0].length             = (int32_t*)&ts_len;
    mbs[0].is_null            = (char*)&ts_is_null;
    mbs[0].num = 1;

    mbs[1].buffer_type        = TSDB_DATA_TYPE_INT;
    mbs[1].buffer             = (void*)&v;
    mbs[1].buffer_length      = (uintptr_t)sizeof(v);
    mbs[1].length             = (int32_t*)&v_len;
    mbs[1].is_null            = (char*)&v_is_null;
    mbs[1].num = 1;

    mbs[2].buffer_type        = TSDB_DATA_TYPE_VARCHAR;
    mbs[2].buffer             = (void*)name;
    mbs[2].buffer_length      = (uintptr_t)name_len;
    mbs[2].length             = (int32_t*)&name_len;
    mbs[2].is_null            = (char*)&name_is_null;
    mbs[2].num = 1;

    mbs[3].buffer_type        = TSDB_DATA_TYPE_NCHAR;
    mbs[3].buffer             = (void*)mark;
    mbs[3].buffer_length      = (uintptr_t)mark_len;
    mbs[3].length             = (int32_t*)&mark_len;
    mbs[3].is_null            = (char*)&mark_is_null;
    mbs[3].num = 1;

    const char *tbl = "m";
    r = CALL_taos_stmt_set_tbname(stmt, tbl);
    if (r) return -1;

    r = CALL_taos_stmt_bind_param_batch(stmt, &mbs[0]);
    if (r) return -1;

    r = CALL_taos_stmt_add_batch(stmt);
    if (r) return -1;
  }

  r = CALL_taos_stmt_execute(stmt);
  if (r) return -1;

  return 0;
}

static int _prepare_normal_get_col_fields(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.t (ts timestamp, v int, name varchar(21), mark nchar(20))", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

#define RECORD(...)       {__LINE__, ##__VA_ARGS__}
  struct {
    int                line;
    const char        *sql;
    int                nr_cols;
  } _inserts[] = {
    RECORD("insert into t (ts, v, name, mark) values (?, ?, ?, ?)", 4),
    RECORD("insert into t values (?, ?, ?, ?)", 4),
    RECORD("insert into t (ts, v, mark) values (?, ?, ?)", 3),
  };
  size_t _nr_inserts = sizeof(_inserts)/sizeof(_inserts[0]);
#undef RECORD

  for (size_t i=0; i<_nr_inserts; ++i) {
    int          line             = _inserts[i].line;
    const char  *sql              = _inserts[i].sql;
    int          nr_cols          = _inserts[i].nr_cols;

    r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
    if (r) return -1;

    int insert = 0;
    r = CALL_taos_stmt_is_insert(stmt, &insert);
    if (r) return -1;
    if (!insert) return -1;

    int colNum = 0;
    TAOS_FIELD_E *cols = NULL;
    r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
    if (cols) {
      CALL_taos_stmt_reclaim_fields(stmt, cols); cols = NULL;
    }
    if (r) return -1;
    if (colNum != nr_cols) {
      E("@%dL:cols expected %d, but got ==%d==", line, nr_cols, colNum);
      return -1;
    }
  }

  return 0;
}

static int _prepare_get_tag_col_fields(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) return 0;
  if (stage != STAGE_STATEMENT) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"create table foo.st (ts timestamp, v int, name varchar(21), mark nchar(20)) tags (val int, feat varchar(20))", __LINE__, 1},
    {"insert into foo.t using foo.st (val, feat) tags (3, 'f3') (ts, v, name, mark) values (1665025866843, 4, 'n4', 'm4')", __LINE__, 1},
    {"use foo", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

#ifdef _WIN32         /* { */
  E("TODO: for the sake of source-code-charset, need to implement later");
  if (1) return 0;
#endif                /* } */

#define RECORD(...)       {__LINE__, ##__VA_ARGS__}
  struct {
    int                line;
    const char        *sql;
    int                err_code;
    int                nr_tags;
    int                nr_cols;
  } _inserts[] = {
    RECORD("insert into t (ts, v, name, mark) values (?, ?, ?, ?)", TSDB_CODE_SUCCESS, 0, 4),
    RECORD("insert into t values (?, ?, ?, ?)", TSDB_CODE_SUCCESS, 0, 4),
    RECORD("insert into t (ts, v, mark) values (?, ?, ?)", TSDB_CODE_SUCCESS, 0, 3),
    RECORD("insert into t (ts) values (?)", TSDB_CODE_SUCCESS, 0, 1),
    RECORD("insert into t (ts) values (1665025866843)", TSDB_CODE_SUCCESS, 0, 1),
    RECORD("insert into t (ts, v) values (now(), 3)", TSDB_CODE_SUCCESS, 0, 2),
    RECORD("insert into t (ts, v) values (1665025866843, 3)", TSDB_CODE_SUCCESS, 0, 2),
    RECORD("insert into t (ts, v) values (now(), ?)", TSDB_CODE_SUCCESS, 0, 2),
    RECORD("insert into t (ts, v) values (1665025866843, ?)", TSDB_CODE_SUCCESS, 0, 2),
    RECORD("insert into t (ts, v, mark) values (1665025866843, ?, ?)", TSDB_CODE_SUCCESS, 0, 3),
    RECORD("insert into t values (1665025866843, ?, ?, ?)", TSDB_CODE_SUCCESS, 0, 4),
    RECORD("insert into t using st (val, feat) tags (?, ?) (ts, v, mark) values (?, ?, ?)", TSDB_CODE_SUCCESS, 2, 3),
    RECORD("insert into t using st (val) tags (?) (ts, v, mark) values (?, ?, ?)", TSDB_CODE_SUCCESS, 1, 3),
    RECORD("insert into t using st (val) tags (4) (ts, v, mark) values (?, ?, ?)", TSDB_CODE_SUCCESS, 1, 3),  // NOTE: weird!!!, literal tag value, but get_tag_fields still reports!!!
  };
  size_t _nr_inserts = sizeof(_inserts)/sizeof(_inserts[0]);
#undef RECORD

  for (size_t i=0; i<_nr_inserts; ++i) {
    int          line             = _inserts[i].line;
    const char  *sql              = _inserts[i].sql;
    int          err_code         = _inserts[i].err_code;
    int          nr_tags          = _inserts[i].nr_tags;
    int          nr_cols          = _inserts[i].nr_cols;

    r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
    if (r) return -1;

    int insert = 0;
    r = CALL_taos_stmt_is_insert(stmt, &insert);
    if (r) return -1;
    if (!insert) return -1;

    int tagNum = 0;
    TAOS_FIELD_E *tags = NULL;
    r = CALL_taos_stmt_get_tag_fields(stmt, &tagNum, &tags);
    if (tags) {
      CALL_taos_stmt_reclaim_fields(stmt, tags); tags = NULL;
    }
    if (r) {
      if (err_code != taos_errno(NULL)) return -1;
    }
    if (tagNum != nr_tags) {
      E("@%dL:tags expected %d, but got ==%d==", line, nr_tags, tagNum);
      return -1;
    }

    int colNum = 0;
    TAOS_FIELD_E *cols = NULL;
    r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
    if (cols) {
      CALL_taos_stmt_reclaim_fields(stmt, cols); cols = NULL;
    }
    if (r) {
      if (err_code != taos_errno(NULL)) return -1;
    }
    if (colNum != nr_cols) {
      E("@%dL:cols expected %d, but got ==%d==", line, nr_cols, colNum);
      return -1;
    }
  }

  return 0;
}

#define RECORD(x) {x, #x}

static struct {
  regress_f             func;
  const char           *name;
} _regressions[] = {
  RECORD(_dummy),
  RECORD(_query),
  RECORD(_prepare),
  RECORD(_fetch),
  RECORD(_prepare_insert),
  RECORD(_prepare_insert_many),
  RECORD(_prepare_insert_normal_dynamic),
  RECORD(_prepare_insert_normals_dynamic),
  RECORD(_prepare_normal_get_col_fields),
  RECORD(_prepare_get_tag_col_fields),
};

#undef RECORD

static int on_statement(const arg_t *arg, TAOS *taos, TAOS_STMT *stmt)
{
  int r = 0;

  r = arg->regress(arg, STAGE_STATEMENT, taos, stmt);
  if (r) return -1;

  return 0;
}

static int on_connected(const arg_t *arg, TAOS *taos)
{
  int r = 0;

  r = arg->regress(arg, STAGE_CONNECTED, taos, NULL);
  if (r) return -1;

  TAOS_STMT  *stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  r = on_statement(arg, taos, stmt);

  CALL_taos_stmt_close(stmt);

  return r ? -1 : 0;
}

static int on_init(const arg_t *arg)
{
  int r = 0;

  r = arg->regress(arg, STAGE_INITED, NULL, NULL);
  if (r) return -1;

  TAOS *taos = CALL_taos_connect(arg->ip, arg->uid, arg->pwd, arg->db, arg->port);
  if (!taos) return -1;

  r = on_connected(arg, taos);

  CALL_taos_close(taos);

  return r ? -1 : 0;
}

static int run_regress(const arg_t *arg)
{
  int r = 0;

  r = on_init(arg);

  return r ? -1 : 0;
}

static int start_by_arg(arg_t *arg)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);
  for (size_t i=0; i<nr; ++i) {
    arg->regname = _regressions[i].name;
    arg->regress = _regressions[i].func;
    if ((!arg->name) || tod_strcasecmp(arg->name, arg->regname) == 0) {
      r = run_regress(arg);
      if (r) return -1;
    }
  }

  return 0;
}

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s [--ip <IP>] [--uid <UID>] [--pwd <PWD>] [--db <DB>] [--port <PORT>] [name]", arg0);
  DUMP("");
  DUMP("supported regression names:");
  for (size_t i=0; i<sizeof(_regressions)/sizeof(_regressions[0]); ++i) {
    DUMP("  %s", _regressions[i].name);
  }
}

static int start(int argc, char *argv[])
{
  int r = 0;

  arg_t arg = {0};

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--ip") == 0) {
      ++i;
      if (i>=argc) break;
      if (argv[i][0]) arg.ip= argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      if (argv[i][0]) arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--db") == 0) {
      ++i;
      if (i>=argc) break;
      arg.db = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--port") == 0) {
      ++i;
      if (i>=argc) break;
      arg.port = atoi(argv[i]);
      continue;
    }

    arg.name = argv[i];
    r = start_by_arg(&arg);
    if (r) return -1;
  }

  if (arg.name) return 0;

  return start_by_arg(&arg);
}

int main(int argc, char *argv[])
{
  int r;

  r = CALL_taos_init();
  if (r) goto end;

  r = start(argc, argv);

  CALL_taos_cleanup();

end:

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

