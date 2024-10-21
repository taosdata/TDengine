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

#include "tsdb_value.h"
#include "osMemory.h"
#include "os.h"
#include "tcommon.h"

void dump_SName_x(const char *file, int line, const char *func, SName *name) {
  if (!name) return;
  char buf[4096+sizeof(*name)]; *buf = '\0';
  char   *p   = buf;
  size_t  len = sizeof(buf);

  int n = snprintf(p, len, "%s[%d]:%s():====", file, line, func);
  p      += n;
  len    -= n;
  if (len > 0) {
    int n = snprintf(p, len, "acct:[%d];", name->acctId);
    p      += n;
    len    -= n;
  }
  if (len > 0) {
    int n = snprintf(p, len, "db:[%.*s];", (int)sizeof(name->dbname), name->dbname);
    p      += n;
    len    -= n;
  }
  if (len > 0) {
    int n = snprintf(p, len, "table:[%.*s];", (int)sizeof(name->tname), name->tname);
    p      += n;
    len    -= n;
  }

  fprintf(stderr, "%.*s===\n", (int)sizeof(buf), buf);
}

void tsdb_value_reset(tsdb_value_t *value) {
  if (!value) return;
  value->type      = TSDB_DATA_TYPE_NULL;
  value->nr        = 0;
}

void tsdb_value_release(tsdb_value_t *value) {
  if (!value) return;
  tsdb_value_reset(value);
  taosMemoryFreeClear(value->buf);
  value->cap = 0;
}

void tsdb_value_swap(tsdb_value_t *l, tsdb_value_t *r) {
  tsdb_value_t tmp = *l;
  *l = *r;
  *r = tmp;
}

static int to_number(tsdb_value_t *t, tsdb_value_t *f) {
  switch (f->type) {
    case TSDB_DATA_TYPE_BOOL: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = !!f->b;
      return 0;
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->i8;
      return 0;
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->i16;
      return 0;
    } break;
    case TSDB_DATA_TYPE_INT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->i32;
      return 0;
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->i64;
      return 0;
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      t->type = TSDB_DATA_TYPE_DOUBLE;
      t->dbl  = f->flt;
      return 0;
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      t->type = TSDB_DATA_TYPE_DOUBLE;
      t->dbl  = f->dbl;
      return 0;
    } break;
    case TSDB_DATA_TYPE_VARCHAR: {
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->u8;
      return 0;
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->u16;
      return 0;
    } break;
    case TSDB_DATA_TYPE_UINT: {
      t->type = TSDB_DATA_TYPE_BIGINT;
      t->i64  = f->u32;
      return 0;
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      if (f->u64 > INT64_MAX) {
        t->type = TSDB_DATA_TYPE_UBIGINT;
        t->u64  = f->u64;
      } else {
        t->type = TSDB_DATA_TYPE_BIGINT;
        t->i64  = (int64_t)f->u64;
      }
      return 0;
    } break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
    default:
      break;
  }
  TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%d]%s to number: not supported yet", f->type, taosDataTypeName(f->type));
  return -1;
}

static int ts_add(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r) {
  v->type = l->type;
  v->ts   = l->ts;
  tsdb_value_t f = {0};
  if (to_number(&f, r)) return -1;
  switch (f.type) {
    case TSDB_DATA_TYPE_BOOL: {
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
      v->ts.epoch_nano += f.i8;
      return 0;
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      v->ts.epoch_nano += f.i16;
      return 0;
    } break;
    case TSDB_DATA_TYPE_INT: {
      v->ts.epoch_nano += f.i32;
      return 0;
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      v->ts.epoch_nano += f.i64;
      return 0;
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
    } break;
    case TSDB_DATA_TYPE_VARCHAR: {
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      v->ts.epoch_nano += f.u8;
      return 0;
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      v->ts.epoch_nano += f.u16;
      return 0;
    } break;
    case TSDB_DATA_TYPE_UINT: {
      v->ts.epoch_nano += f.u32;
      return 0;
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      v->ts.epoch_nano += f.u64;
      return 0;
    } break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
    default:
      break;
  }
  TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%d]%s to number: not supported yet", f.type, taosDataTypeName(f.type));
  return -1;
}

static int number_to_double(double *t, tsdb_value_t *f) {
  switch (f->type) {
    case TSDB_DATA_TYPE_BIGINT: {
      *t = f->i64;
      return 0;
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      *t = f->dbl;
      return 0;
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      *t = f->u64;
      return 0;
    } break;
    default:
      break;
  }
  TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "internal logic error");
  return -1;
}

static int number_to_u64(uint64_t *t, tsdb_value_t *f) {
  switch (f->type) {
    case TSDB_DATA_TYPE_BIGINT: {
      if (f->i64 < 0) {
        TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%" PRId64 "] to uint64_t underflow", f->i64);
        return -1;
      }
      *t = (uint64_t)f->i64;
      return 0;
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%lf] to uint64_t failed", f->dbl);
      return -1;
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      *t = f->u64;
      return 0;
    } break;
    default:
      break;
  }
  TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "internal logic error");
  return -1;
}

static int number_to_i64(int64_t *t, tsdb_value_t *f) {
  switch (f->type) {
    case TSDB_DATA_TYPE_BIGINT: {
      *t = f->i64;
      return 0;
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%lf] to uint64_t failed", f->dbl);
      return -1;
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      if (f->u64 > INT64_MAX) {
        TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%" PRIu64 "] to int64_t overflow", f->u64);
        return -1;
      }
      *t = (int64_t)f->u64;
      return 0;
    } break;
    default:
      break;
  }
  TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "internal logic error");
  return -1;
}

int tsdb_value_neg(tsdb_value_t *t, tsdb_value_t *f) {
  if (f->type == TSDB_DATA_TYPE_NULL) {
    t->type = TSDB_DATA_TYPE_NULL;
    return 0;
  }

  if (f->type == TSDB_DATA_TYPE_TIMESTAMP) {
    if (!f->ts.is_interval) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "neg([%d]%s) not defined", f->type, taosDataTypeName(f->type));
      return -1;
    }
    t->type              = f->type;
    t->ts                = f->ts;
    t->ts.is_interval    = f->ts.is_interval;
    t->ts.interval.yr    = -f->ts.interval.yr;
    t->ts.interval.mon   = -f->ts.interval.mon;
    t->ts.interval.day   = -f->ts.interval.day;
    t->ts.interval.hr    = -f->ts.interval.hr;
    t->ts.interval.min   = -f->ts.interval.min;
    t->ts.interval.sec   = -f->ts.interval.sec;
    t->ts.interval.nano  = -f->ts.interval.nano;
    return 0;
  }

  tsdb_value_t a = {0};
  if (to_number(&a, f)) return -1;

  if (a.type == TSDB_DATA_TYPE_DOUBLE) {
    double x;
    if (number_to_double(&x, &a)) return -1;
    t->type = TSDB_DATA_TYPE_DOUBLE;
    t->dbl  = -x;
    return 0;
  }
  if (a.type == TSDB_DATA_TYPE_UBIGINT) {
    uint64_t x;
    if (number_to_u64(&x, &a)) return -1;
    if (x > INT64_MAX) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "neg(%" PRIu64 "): overflow", x);
      return -1;
    }
    int64_t z = -(int64_t)x;
    t->type = TSDB_DATA_TYPE_BIGINT;
    t->i64  = z;
  }

  int64_t x;
  if (number_to_i64(&x, &a)) return -1;
  if (x == INT64_MIN) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "neg(%" PRIu64 "): overflow", x);
    return -1;
  }
  int64_t z = -x;
  t->type = TSDB_DATA_TYPE_BIGINT;
  t->i64  = z;

  return 0;
}

int tsdb_value_add(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r) {
  if (l->type == TSDB_DATA_TYPE_NULL || r->type == TSDB_DATA_TYPE_NULL) {
    v->type = TSDB_DATA_TYPE_NULL;
    return 0;
  }

  if (l->type == TSDB_DATA_TYPE_TIMESTAMP) {
    return ts_add(v, l, r);
  } else if (r->type == TSDB_DATA_TYPE_TIMESTAMP) {
    return ts_add(v, r, l);
  }

  tsdb_value_t a = {0};
  tsdb_value_t b = {0};
  if (to_number(&a, l)) return -1;
  if (to_number(&b, r)) return -1;

  if (a.type == TSDB_DATA_TYPE_DOUBLE || b.type == TSDB_DATA_TYPE_DOUBLE) {
    double x, y;
    if (number_to_double(&x, &a)) return -1;
    if (number_to_double(&y, &b)) return -1;
    v->type = TSDB_DATA_TYPE_DOUBLE;
    v->dbl  = x + y;
    return 0;
  }
  if (a.type == TSDB_DATA_TYPE_UBIGINT || b.type == TSDB_DATA_TYPE_UBIGINT) {
    uint64_t x, y;
    if (number_to_u64(&x, &a)) return -1;
    if (number_to_u64(&y, &b)) return -1;
    uint64_t z = x + y;
    if (z < x || z < y) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "%" PRIu64 " + %" PRIu64 ": overflow", x, y);
      return -1;
    }
    v->type = TSDB_DATA_TYPE_UBIGINT;
    v->u64  = z;
  }

  int64_t x, y;
  if (number_to_i64(&x, &a)) return -1;
  if (number_to_i64(&y, &b)) return -1;
  int64_t z = x + y;
  if (x>0 && y>0 && (z < x || z < y)) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "%" PRId64 " + %" PRId64 ": overflow", x, y);
    return -1;
  }
  if (x<0 && y<0 && (z > x || z > y)) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "%" PRId64 " + %" PRId64 ": underflow", x, y);
    return -1;
  }
  v->type = TSDB_DATA_TYPE_BIGINT;
  v->i64  = z;

  return 0;
}

int tsdb_value_sub(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r) {
  if (l->type == TSDB_DATA_TYPE_NULL || r->type == TSDB_DATA_TYPE_NULL) {
    v->type = TSDB_DATA_TYPE_NULL;
    return 0;
  }

  if (l->type == TSDB_DATA_TYPE_TIMESTAMP) {
    tsdb_value_t a = {0};
    if (tsdb_value_neg(&a, r)) return -1;
    return ts_add(v, l, &a);
  } else if (r->type == TSDB_DATA_TYPE_TIMESTAMP) {
    tsdb_value_t a = {0};
    if (tsdb_value_neg(&a, l)) return -1;
    return ts_add(v, r, &a);
  }

  tsdb_value_t a = {0};
  tsdb_value_t b = {0};
  if (to_number(&a, l)) return -1;
  if (to_number(&b, r)) return -1;

  if (a.type == TSDB_DATA_TYPE_DOUBLE || b.type == TSDB_DATA_TYPE_DOUBLE) {
    double x, y;
    if (number_to_double(&x, &a)) return -1;
    if (number_to_double(&y, &b)) return -1;
    v->type = TSDB_DATA_TYPE_DOUBLE;
    v->dbl  = x + y;
    return 0;
  }
  if (a.type == TSDB_DATA_TYPE_UBIGINT || b.type == TSDB_DATA_TYPE_UBIGINT) {
    uint64_t x, y;
    if (number_to_u64(&x, &a)) return -1;
    if (number_to_u64(&y, &b)) return -1;
    uint64_t z = x + y;
    if (z < x || z < y) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "%" PRIu64 " + %" PRIu64 ": overflow", x, y);
      return -1;
    }
    v->type = TSDB_DATA_TYPE_UBIGINT;
    v->u64  = z;
  }

  int64_t x, y;
  if (number_to_i64(&x, &a)) return -1;
  if (number_to_i64(&y, &b)) return -1;
  int64_t z = x + y;
  if (x>0 && y>0 && (z < x || z < y)) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "%" PRId64 " + %" PRId64 ": overflow", x, y);
    return -1;
  }
  if (x<0 && y<0 && (z > x || z > y)) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "%" PRId64 " + %" PRId64 ": underflow", x, y);
    return -1;
  }
  v->type = TSDB_DATA_TYPE_BIGINT;
  v->i64  = z;

  return 0;
}

int tsdb_value_mul(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r) {
  if (l->type == TSDB_DATA_TYPE_NULL || r->type == TSDB_DATA_TYPE_NULL) {
    v->type = TSDB_DATA_TYPE_NULL;
    return 0;
  }

  if (l->type == TSDB_DATA_TYPE_TIMESTAMP || r->type == TSDB_DATA_TYPE_TIMESTAMP) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "multiplication of [%d]%s not defined",
        TSDB_DATA_TYPE_TIMESTAMP, taosDataTypeName(TSDB_DATA_TYPE_TIMESTAMP));
    return -1;
  }

  tsdb_value_t a = {0};
  tsdb_value_t b = {0};
  if (to_number(&a, l)) return -1;
  if (to_number(&b, r)) return -1;

  if (a.type == TSDB_DATA_TYPE_DOUBLE || b.type == TSDB_DATA_TYPE_DOUBLE) {
    double x, y;
    if (number_to_double(&x, &a)) return -1;
    if (number_to_double(&y, &b)) return -1;
    v->type = TSDB_DATA_TYPE_DOUBLE;
    v->dbl  = x * y;
    return 0;
  }

  if (a.type == TSDB_DATA_TYPE_UBIGINT || b.type == TSDB_DATA_TYPE_UBIGINT) {
    uint64_t x, y;
    if (number_to_u64(&x, &a)) return -1;
    if (number_to_u64(&y, &b)) return -1;
    v->type = TSDB_DATA_TYPE_UBIGINT;
    v->u64  = x * y;
  }

  int64_t x, y;
  if (number_to_i64(&x, &a)) return -1;
  if (number_to_i64(&y, &b)) return -1;
  v->type = TSDB_DATA_TYPE_BIGINT;
  v->i64  = x * y;

  return 0;
}

int tsdb_value_div(tsdb_value_t *v, tsdb_value_t *l, tsdb_value_t *r) {
  if (l->type == TSDB_DATA_TYPE_NULL || r->type == TSDB_DATA_TYPE_NULL) {
    v->type = TSDB_DATA_TYPE_NULL;
    return 0;
  }

  if (l->type == TSDB_DATA_TYPE_TIMESTAMP || r->type == TSDB_DATA_TYPE_TIMESTAMP) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "division of [%d]%s not defined",
        TSDB_DATA_TYPE_TIMESTAMP, taosDataTypeName(TSDB_DATA_TYPE_TIMESTAMP));
    return -1;
  }

  tsdb_value_t a = {0};
  tsdb_value_t b = {0};
  if (to_number(&a, l)) return -1;
  if (to_number(&b, r)) return -1;

  if (a.type == TSDB_DATA_TYPE_DOUBLE || b.type == TSDB_DATA_TYPE_DOUBLE) {
    double x, y;
    if (number_to_double(&x, &a)) return -1;
    if (number_to_double(&y, &b)) return -1;
    v->type = TSDB_DATA_TYPE_DOUBLE;
    v->dbl  = x / y;
    return 0;
  }

  if (a.type == TSDB_DATA_TYPE_UBIGINT || b.type == TSDB_DATA_TYPE_UBIGINT) {
    uint64_t x, y;
    if (number_to_u64(&x, &a)) return -1;
    if (number_to_u64(&y, &b)) return -1;
    if (y == 0) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "divided by zero not defined");
      return -1;
    }
    v->type = TSDB_DATA_TYPE_UBIGINT;
    v->u64  = x / y;
  }

  int64_t x, y;
  if (number_to_i64(&x, &a)) return -1;
  if (number_to_i64(&y, &b)) return -1;
  if (y == 0) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "divided by zero not defined");
    return -1;
  }
  v->type = TSDB_DATA_TYPE_BIGINT;
  v->i64  = x / y;

  return 0;
}

int tsdb_value_as_bool(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }
#define R(x, y) {x, y}
  static const struct {
    const char *s;
    uint8_t     b:1;
  } _cases[] = {
    R("true",  1),
    R("false", 0),
  };
  static const size_t nr_cases = sizeof(_cases) / sizeof(_cases[0]);
#undef R

  for (size_t i=0; i<nr_cases; ++i) {
    const char *name = _cases[i].s;
    size_t      len  = strlen(name);
    uint8_t     b    = _cases[i].b;
    if (len != n) continue;
    if (strncasecmp(name, s, n)) continue;
    v->type       = TSDB_DATA_TYPE_BOOL;
    v->b          = !!b;
    return 0;
  }

  v->type       = TSDB_DATA_TYPE_BOOL;
  v->b          = !!taosStr2Int64(s, NULL, 0);

  return 0;
}

int tsdb_value_as_integer(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }

  int64_t  i64 = 0;
  uint64_t u64 = 0;

  char *end = NULL;
  errno = 0;
  i64 = taosStr2Int64(s, &end, 0);
  if (errno == ERANGE) goto try_u64;
  v->type = TSDB_DATA_TYPE_BIGINT;
  v->i64  = i64;
  return 0;

try_u64:
  errno = 0;
  u64 = taosStr2UInt64(s, &end, 0);
  if (errno == ERANGE) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%.*s] to interger overflow", (int)n, s);
    return -1;
  }
  v->type = TSDB_DATA_TYPE_UBIGINT;
  v->u64  = u64;
  return 0;
}

int tsdb_value_as_number(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }

  double   dbl = 0;

  char *end = NULL;
  errno = 0;
  dbl = taosStr2Double(s, &end);
  if (errno == ERANGE) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "convert [%.*s] to number overflow", (int)n, s);
    return -1;
  }
  v->type = TSDB_DATA_TYPE_DOUBLE;
  v->dbl  = dbl;
  return 0;
}

int tsdb_value_as_str_ref(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }

  v->type           = TSDB_DATA_TYPE_VARCHAR;
  v->varchar.str    = s;
  v->varchar.bytes  = strnlen(s, n);
  return 0;
}

int tsdb_value_as_str(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }

  if (n > v->cap) {
    size_t  cap = (n + 15) / 16 * 16;
    char   *p   = (char*)taosMemoryRealloc(v->buf, cap + 1);
    if (!p) {
      TD_SET_ERR(TSDB_CODE_OUT_OF_MEMORY, "");
      return -1;
    }
    v->buf   = p;
    v->cap   = cap;
    v->nr    = 0;
  }
  strncpy(v->buf, s, n);
  v->buf[n] = '\0';
  v->nr = n;

  v->type           = TSDB_DATA_TYPE_VARCHAR;
  v->varchar.str    = v->buf;
  v->varchar.bytes  = strnlen(v->buf, v->nr);
  return 0;
}

int tsdb_value_as_interval(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }

  static const char units[] = "buasmhdwny";

  int64_t  i64 = 0;
  uint64_t u64 = 0;

  char *end = NULL;
  errno = 0;
  i64 = taosStr2Int64(s, &end, 0);
  if (errno == ERANGE) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid interval: overflow/underflow", (int)n, s);
    return -1;
  }
  if (!end || !strchr(units, *end)) {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid interval: missing or invalid unit", (int)n, s);
    return -1;
  }
  v->type               = TSDB_DATA_TYPE_TIMESTAMP;
  v->ts.is_interval     = 1;
  memset(&v->ts.interval, 0, sizeof(v->ts.interval));
  switch (*end) {
    case 'b': v->ts.interval.nano      = i64;               break;
    case 'u': v->ts.interval.nano      = i64 * 1000;        break;
    case 'a': v->ts.interval.nano      = i64 * 1000000;     break;
    case 's': v->ts.interval.sec       = i64;               break;
    case 'm': v->ts.interval.min       = i64;               break;
    case 'h': v->ts.interval.hr        = i64;               break;
    case 'd': v->ts.interval.day       = i64;               break;
    case 'w': v->ts.interval.day       = i64 * 7;           break;
    case 'n': v->ts.interval.mon       = i64;               break;
    default:  v->ts.interval.yr        = i64;               break;
  }

  return 0;
}

int tsdb_value_as_ts(tsdb_value_t *v, const char *s, size_t n) {
  if (!s || n==0) {
    v->type == TSDB_DATA_TYPE_NULL;
    return 0;
  }

  time_t t = 0;
  struct tm tm = {0};
  int64_t frac = 0;
  const char *fmt = "%Y-%m-%d %H:%M:%D";
  int64_t offset = 0;

  char *p = taosStrpTime(s, fmt, &tm);
  if (!p) {
    fmt = "%Y-%m-%dT%H:%M:%D";
    p = taosStrpTime(s, fmt, &tm);
    if (!p) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp", (int)n, s);
      return -1;
    }
  }

  t = taosMktime(&tm);

  if (*p == '.') {
    ++p;
    char *end = NULL;
    errno = 0;
    frac = taosStr2UInt64(p, &end, 10);
    if (errno == ERANGE) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp: time fraction not valid or overflow", (int)n, s);
      return -1;
    }
    if (!end) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp: internal logic error", (int)n, s);
      return -1;
    }
    p = end;
  }
  if (*p == '+' || *p == '-') {
    ++p;
    char *end = NULL;
    offset = taosStr2UInt64(p, &end, 10);
    if (errno == ERANGE) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp: timezone not valid or overflow", (int)n, s);
      return -1;
    }
    if (!end) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp: internal logic error", (int)n, s);
      return -1;
    }
    if (end - p != 4) {
      TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp: timezone not valid or overflow", (int)n, s);
      return -1;
    }
    offset = (offset/100) * 3600 + (offset%100) * 60;
    p = end;
  } else if (*p == 'Z') {
    // empty
  } else if (!*p) {
    // TODO: what if timezone set in taos.cfg
    offset = tsTimezone * 3600;
  } else {
    TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "[%.*s] not a valid timestamp: timezone not valid or overflow", (int)n, s);
    return -1;
  }

  t -= offset;
  v->type           = TSDB_DATA_TYPE_TIMESTAMP;
  v->ts.is_interval = 0;
  v->ts.epoch_nano  = t * 1000000000 + frac;

  return 0;
}

int32_t tsdb_value_from_TAOS_MULTI_BIND(tsdb_value_t *v, TAOS_MULTI_BIND *mb, int row) {
  if (mb->is_null && mb->is_null[row]) {
    v->type = TSDB_DATA_TYPE_NULL;
    return 0;
  }
  switch (mb->buffer_type) {
    case TSDB_DATA_TYPE_BOOL: {
      v->type     = mb->buffer_type;
      v->b        = !!((char*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
      v->type     = mb->buffer_type;
      v->i8       = ((int8_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      v->type     = mb->buffer_type;
      v->i16      = ((int16_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_INT: {
      v->type     = mb->buffer_type;
      v->i32      = ((int32_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      v->type     = mb->buffer_type;
      v->i64      = ((int64_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      v->type     = mb->buffer_type;
      v->flt      = ((float*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      v->type     = mb->buffer_type;
      v->dbl      = ((double*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      v->type           = mb->buffer_type;
      v->varchar.str    = ((char*)(mb->buffer)) + mb->buffer_length * row;
      v->varchar.bytes  = mb->length ? mb->length[row] : strnlen(v->varchar.str, mb->buffer_length);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      v->type           = mb->buffer_type;
      v->ts.is_interval = 0;
      v->ts.epoch_nano  = ((int64_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      v->type     = mb->buffer_type;
      v->u8       = ((uint8_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      v->type     = mb->buffer_type;
      v->u16      = ((uint16_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_UINT: {
      v->type     = mb->buffer_type;
      v->u32      = ((uint32_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      v->type     = mb->buffer_type;
      v->u64      = ((uint64_t*)(mb->buffer))[row];
    } break;
    case TSDB_DATA_TYPE_JSON: {
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
    } break;
    case TSDB_DATA_TYPE_VARBINARY: {
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
    } break;
    case TSDB_DATA_TYPE_DECIMAL: {
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
    } break;
    case TSDB_DATA_TYPE_BLOB: {
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
    } break;
    case TSDB_DATA_TYPE_MEDIUMBLOB: {
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
    } break;
    // case TSDB_DATA_TYPE_BINARY: // TSDB_DATA_TYPE_VARCHAR
    case TSDB_DATA_TYPE_GEOMETRY:
    default:
      return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "can NOT process [0x%08x]%s", mb->buffer_type, taosDataTypeName(mb->buffer_type));
  }

  return 0;
}

int tsdb_value_type(const tsdb_value_t *v) {
  if (!v) return TSDB_DATA_TYPE_NULL;
  return v->type;
}

static int32_t do_tsdb_value_buffer(const tsdb_value_t *v, const char **data, int32_t *len) {

#define R(x) *len = sizeof(x); *data = (const char*)&(x); return 0
#define RE(e, fmt, ...) TD_SET_ERR(e, "[0x%08x]%s" fmt "", v->type, taosDataTypeName(v->type), ##__VA_ARGS__)

  *data = NULL;
  *len  = 0;
  if (!v) return 0;
  if (v->type == TSDB_DATA_TYPE_NULL) return 0;

  switch (v->type) {
    case TSDB_DATA_TYPE_BOOL: {
      R(v->b);
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
      R(v->i8);
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      R(v->i16);
    } break;
    case TSDB_DATA_TYPE_INT: {
      R(v->i32);
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      R(v->i64);
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      R(v->flt);
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      R(v->dbl);
    } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      *len  = v->varchar.bytes;
      *data = v->varchar.str;
      return 0;
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (v->ts.is_interval) {
        return RE(TSDB_CODE_PAR_NOT_SUPPORT, "(INTERVAL)");
      }
      R(v->ts.epoch_nano);
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      R(v->u8);
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      R(v->u16);
    } break;
    case TSDB_DATA_TYPE_UINT: {
      R(v->u32);
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      R(v->u64);
    } break;
    case TSDB_DATA_TYPE_JSON: {
    } break;
    case TSDB_DATA_TYPE_VARBINARY: {
    } break;
    case TSDB_DATA_TYPE_DECIMAL: {
    } break;
    case TSDB_DATA_TYPE_BLOB: {
    } break;
    case TSDB_DATA_TYPE_MEDIUMBLOB: {
    } break;
    // case TSDB_DATA_TYPE_BINARY: // TSDB_DATA_TYPE_VARCHAR
    case TSDB_DATA_TYPE_GEOMETRY: {
    } break;
    default: {
    } break;
  }

  return RE(TSDB_CODE_PAR_NOT_SUPPORT, "");

#undef RE
#undef R
}

int32_t tsdb_value_buffer(const tsdb_value_t *v, const char **data, int32_t *len) {
  const char       *p = NULL;
  int32_t           n = 0;

  int32_t code = do_tsdb_value_buffer(v, &p, &n);

  if (data) *data = p;
  if (len)  *len  = n;

  return code;
}

int tsdb_value_as_epoch_nano(tsdb_value_t *v, int64_t epoch_nano) {
  v->type            = TSDB_DATA_TYPE_TIMESTAMP;
  v->ts.is_interval  = 0;
  v->ts.epoch_nano   = epoch_nano;
  return 0;
}

void tsdb_dump(const tsdb_value_t *v) {
  if (!v) return;
  switch (v->type) {
    case TSDB_DATA_TYPE_BOOL: {
      fprintf(stderr, "%s", v->b ? "true" : "false");
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
      fprintf(stderr, "%d", v->i8);
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
      fprintf(stderr, "%d", v->i16);
    } break;
    case TSDB_DATA_TYPE_INT: {
      fprintf(stderr, "%d", v->i32);
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      fprintf(stderr, "%" PRId64 "", v->i64);
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      fprintf(stderr, "%lf", v->flt);
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      fprintf(stderr, "%lf", v->dbl);
    } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      fprintf(stderr, "[%.*s]", (int)v->varchar.bytes, v->varchar.str);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (v->ts.is_interval) {
        fprintf(stderr, "not supported yet");
      } else {
        fprintf(stderr, "%" PRId64 "", v->ts.epoch_nano);
      }
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
      fprintf(stderr, "%u", v->u8);
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
      fprintf(stderr, "%u", v->u16);
    } break;
    case TSDB_DATA_TYPE_UINT: {
      fprintf(stderr, "%u", v->u32);
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
      fprintf(stderr, "%" PRIu64 "", v->u64);
    } break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
    default:
      break;
  }
}

