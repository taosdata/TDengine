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

#include "parAst.h"
#include "insertnodes.h"
#include "tsdb_value.h"

const char* unescape_id(char *dst, size_t len, const char * const s, const size_t n) {
  // TODO: freemine, what'bout a better tokenizer to hackle these issues?
  int delimit = 0;

  int j = 0;
  for (int i=0; i<n; ++i) {
    char c = s[i];
    if (c == '`') {
      if (!delimit) {
        if (i == 0) {
          delimit = 1;
          continue;
        }
        return s + i;
      } else {
        if (i+1 == n) {
          // last character
          break;
        }
        if (s[i+1] != '`') {
          return s + i;
        }
        ++i;
      }
    } else {
      if (!isValidIdChar(c)) return s + i;
      if (!delimit) {
        c = tolower(c);
      }
    }
    if (j + 1 == len) {
      return UNESCAPE_ID_TOO_LONG;
    }
    dst[j++] = c;
  }

  dst[j++] = '\0';

  if (j == 1) return UNESCAPE_ID_EMPTY;

  return UNESCAPE_ID_OK;
}

typedef int (*value_eval_f)(value_t *value, int row, eval_env_t *env);
typedef void (*value_release_f)(value_t *value);

struct value_s {
  value_t                    *parent;
  value_t                    *next;
  value_t                    *first_child;

  SToken                      token;

  value_eval_f                eval;
  value_release_f             release;

  tsdb_value_t                tsdb;
  uint8_t                     folded:1;
  uint8_t                     questioned:1;
};

typedef struct value_func_s             value_func_t;
struct value_func_s {
  const char                *name;
  value_eval_f               eval;
  int                        min_args;
  int                        max_args;
  uint8_t                    visible:1;
};

value_t* value_first_post_order(value_t *start) {
  if (!start) return NULL;
  while (start->first_child) start = start->first_child;
  return start;
}

value_t* value_next_post_order(value_t *start) {
  if (!start) return NULL;
  if (!start->next) return start->parent;
  return value_first_post_order(start->next);
}

int value_eval(value_t *value, int row, eval_env_t *env) {
  int r = 0;

  value_t *end = value_next_post_order(value);

  value_t *p = NULL, *n = NULL;
  for_each_value_post_order(value, p, n) {
    if (p == end) break;
    if (p->folded) continue;
    if (p->eval) {
      r = p->eval(p, row, env);
    }
    if (r) return r;
  }

  return r;
}

tsdb_value_t* value_get_tsdb_value(value_t *value) {
  if (!value) return NULL;
  return &value->tsdb;
}

const tsdb_value_t* value_tsdb_value(const value_t *value) {
  if (!value) return NULL;
  return &value->tsdb;
}

static void do_value_release(value_t *value) {
  if (!value) return;
  tsdb_value_release(&value->tsdb);
}

static void value_release(value_t *value) {
  if (!value) return;
  value_t *end = value_next_post_order(value);
  value_t *p = NULL, *n = NULL;
  for_each_value_post_order(value, p, n) {
    if (p == end) break;
    assert(p->first_child == NULL);
    if (p->release) p->release(p);
    value_t *parent = p->parent;
    if (parent) {
      parent->first_child = p->next;
    }
    p->parent = NULL;
    p->next   = NULL;
    do_value_release(p);
    if (p != value) taosMemoryFree(p);
  }
}

void value_destroy(value_t *value) {
  if (!value) return;
  value_release(value);
  taosMemoryFree(value);
}

struct interval_value_s {
  value_t                    value;

  char                       unit;
};

struct question_value_s {
  value_t                    value;

  int                        idx; // idx of ? in sql statement
  TAOS_FIELD_E               param_type;
};

void intreval_value_destroy(interval_value_t *interval_value) {
  if (!interval_value) return;
  value_destroy(&interval_value->value);
}

void question_value_destroy(question_value_t *question_value) {
  if (!question_value) return;
  value_destroy(&question_value->value);
}

void value_hint(value_t *value, const TAOS_FIELD_E *field) {
  if (!value || !field) return;
  if (!value->questioned) return;
  question_value_t *qv    = (question_value_t*)value;
  TAOS_FIELD_E     *param = &qv->param_type;
  param->type             = field->type;
  param->precision        = field->precision;
  param->scale            = field->scale;
  param->bytes            = field->bytes;
}

value_t* value_next(value_t *value) {
  if (!value) return NULL;
  return value->next;
}

value_t* value_by_idx(value_t *value, int idx) {
  while (value && idx--) {
    value = value_next(value);
  }
  return value;
}

const TAOS_FIELD_E* value_param_type(value_t *value) {
  if (!value) return NULL;
  if (!value->questioned) return NULL;
  question_value_t *qv    = (question_value_t*)value;
  return &qv->param_type;
}

const SToken* value_token(const value_t *value) {
  if (!value) return NULL;
  return &value->token;
}

void value_set_token(value_t *value, const SToken *t1, const SToken *t2) {
  if (!value) return;
  value->token   = *t1;
  value->token.n = t2->z + t2->n - t1->z;
}

row_t* parser_create_row(SAstCreateContext *pCxt) {
  row_t *v = (row_t*)taosMemoryCalloc(1, sizeof(*v));
  CHECK_OUT_OF_MEM(v);
  return v;
}

static int table_value_eval(value_t *value, int row, eval_env_t *env) {
  int32_t code = TSDB_CODE_SUCCESS;
  value_t *v_db  = value->first_child->next ? value->first_child : NULL;
  value_t *v_tbl = v_db ? v_db->next : value->first_child;

  switch (v_tbl->tsdb.type) {
    case TSDB_DATA_TYPE_VARCHAR: {
    } break;
    default:
      return TD_SET_ERR(TSDB_CODE_PAR_INVALID_TBNAME, "can NOT convert [%d]%s into tbname", value->tsdb.type, taosDataTypeName(value->tsdb.type));
  }

  const char *str   = v_tbl->tsdb.varchar.str;
  size_t      bytes = v_tbl->tsdb.varchar.bytes;
  db_table_t db_tbl = {0};
  if (v_db) {
    snprintf(db_tbl.db, sizeof(db_tbl.db), "%.*s", (int)v_db->tsdb.varchar.bytes, v_db->tsdb.varchar.str);
  }

  const char *end = NULL;
  end = unescape_id(db_tbl.tbl, sizeof(db_tbl.tbl), str, bytes);
  if (end == UNESCAPE_ID_TOO_LONG) {
    return TD_SET_ERR(TSDB_CODE_PAR_INVALID_TBNAME, "table name too long:[%.*s]", (int)bytes, str);
  } else if (end == UNESCAPE_ID_EMPTY) {
    return TD_SET_ERR(TSDB_CODE_PAR_INVALID_TBNAME, "table name empty:[%.*s]", (int)bytes, str);
  } else if (end != UNESCAPE_ID_OK) {
    return TD_SET_ERR(TSDB_CODE_PAR_INVALID_TBNAME, "invalid table name:[%.*s]", (int)bytes, str);
  }
  snprintf(db_tbl.db_tbl, sizeof(db_tbl.db_tbl), "%s.%s", db_tbl.db, db_tbl.tbl);

  if (tsdb_value_as_str(&value->tsdb, db_tbl.db_tbl, strnlen(db_tbl.db_tbl, sizeof(db_tbl.db_tbl)))) {
    return TD_SET_ERR(TSDB_CODE_OUT_OF_MEMORY, "");
  }

  return 0;
}

static int from_token(SAstCreateContext *pCxt, char *dst, size_t len, SToken *token, const char *title) {
  const char *end = NULL;
  end = unescape_id(dst, len, token->z, token->n);
  if (end == UNESCAPE_ID_TOO_LONG) {
    GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR, "%s too long:[%.*s]", title, token->n, token->z);
    return -1;
  } else if (end == UNESCAPE_ID_EMPTY) {
    GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR, "%s empty:[%.*s]", title, token->n, token->z);
    return -1;
  } else if (end != UNESCAPE_ID_OK) {
    GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR, "invalid %s:[%.*s]", title, token->n, token->z);
    return -1;
  }

  return 0;
}

#define SET_ERR() do {                                                         \
  pCxt->errCode = td_errno();                                                  \
  snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "%s", td_errmsg()); \
} while (0)

static value_t* parser_create_db_str(SAstCreateContext *pCxt, SToken *token) {
  db_table_t db_tbl = {0};
  if (from_token(pCxt, db_tbl.db, sizeof(db_tbl.db), token, "db name")) return NULL;

  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_str(&value->tsdb, db_tbl.db, strnlen(db_tbl.db, sizeof(db_tbl.db)));
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded             = 1;

  return value;
  
}

value_t* parser_create_table_value(SAstCreateContext *pCxt, SToken *db, SToken *tbl) {
  value_t *v_db = NULL, *v_tbl = NULL;
  if (db) {
    v_db = parser_create_db_str(pCxt, db);
    if (!v_db) return NULL;
  }

  v_tbl = parser_create_question_value(pCxt, tbl);
  if (!v_tbl) {
    value_destroy(v_db);
    return NULL;
  }

  {
    question_value_t *qv = (question_value_t*)v_tbl;
    qv->param_type.type  = TSDB_DATA_TYPE_VARCHAR;
    qv->param_type.bytes = TSDB_TABLE_NAME_LEN - 1 + 2;     // FIXME: freemine: 192 or 194?
  }

  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  v_tbl->parent         = value;

  if (v_db) {
    v_db->parent        = value;
    v_db->next          = v_tbl;
    value->first_child  = v_db;
  } else {
    value->first_child  = v_tbl;
  }

  value->token.type = 0;  // NOTE: we don't care what it is
  value->token.z    = db ? db->z : tbl->z;
  value->token.n    = db ? tbl->z + tbl->n - db->z : tbl->n;

  value->eval    = table_value_eval;
  value->release = NULL;

  return value;
}

value_t* parser_create_integer_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_integer(&value->tsdb, token->z, token->n);
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded          = 1;

  return value;
}

value_t* parser_create_flt_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_number(&value->tsdb, token->z, token->n);
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded          = 1;

  return value;
}

value_t* parser_create_str_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_str_ref(&value->tsdb, token->z, token->n);
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded             = 1;

  return value;
}

value_t* parser_create_bool_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_bool(&value->tsdb, token->z, token->n);
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded          = 1;

  return value;
}

value_t* parser_create_ts_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_ts(&value->tsdb, token->z, token->n);
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded          = 1;

  return value;
}

multiple_targets_t* parser_create_multiple_targets(SAstCreateContext *pCxt) {
  multiple_targets_t *v = (multiple_targets_t*)taosMemoryCalloc(1, sizeof(*v));
  CHECK_OUT_OF_MEM(v);

  return v;
}

int parser_multiple_targets_append(SAstCreateContext *pCxt, multiple_targets_t *multiple_targets, simple_target_t *simple_target) {
  if (!multiple_targets->first) {
    multiple_targets->first = simple_target;
    return 0;
  }

  simple_target_t *p = multiple_targets->first;
  while (p->next) p = p->next;
  p->next = simple_target;
  return 0;
}

simple_target_t* parser_create_simple_target(SAstCreateContext *pCxt) {
  simple_target_t *v = (simple_target_t*)taosMemoryCalloc(1, sizeof(*v));
  CHECK_OUT_OF_MEM(v);

  return v;
}

using_clause_t* parser_create_using_clause(SAstCreateContext *pCxt) {
  using_clause_t *v = (using_clause_t*)taosMemoryCalloc(1, sizeof(*v));
  CHECK_OUT_OF_MEM(v);

  return v;
}

value_t* parser_create_interval_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  int rr = tsdb_value_as_interval(&value->tsdb, token->z, token->n);
  if (rr) {
    value_destroy(value);
    SET_ERR();
    return NULL;
  }

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->folded          = 1;

  return value;
}

value_t* parser_create_null_value(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  value->token   = *token;

  value->eval    = NULL;
  value->release = NULL;

  value->tsdb.type       = TSDB_DATA_TYPE_NULL;
  value->folded          = 1;

  return value;
}

static int question_eval(tsdb_value_t *tsdb, int row, int col, eval_env_t *env) {
  if (col >= env->nr_mbs) {
    return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
  if (row >= env->mbs->num) {
    return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
  TAOS_MULTI_BIND *mb = env->mbs + col;
  return tsdb_value_from_TAOS_MULTI_BIND(tsdb, mb, row);
}

static int now_eval(tsdb_value_t *tsdb, eval_env_t *env) {
  return tsdb_value_as_epoch_nano(tsdb, env->epoch_nano);
}

static int question_value_eval(value_t *value, int row, eval_env_t *env) {
  question_value_t *question_value = (question_value_t*)value;
  return question_eval(&value->tsdb, row, question_value->idx, env);
}

value_t* parser_create_question_value(SAstCreateContext *pCxt, SToken *token) {
  question_value_t *question_value = (question_value_t*)taosMemoryCalloc(1, sizeof(*question_value));
  CHECK_OUT_OF_MEM(question_value);

  value_t *value = &question_value->value;

  value->token   = *token;

  value->eval    = question_value_eval;
  value->release = NULL;

  question_value->idx  = pCxt->parse_result->placeholderNo++;
  value->questioned    = 1;

  question_value->param_type.name[0]   = '?';

  return value;
}

static int value_add(value_t *value, int row, eval_env_t *env) {
  value_t *l = value->first_child;
  value_t *r = l->next;

  tsdb_value_t *lv = &l->tsdb;
  tsdb_value_t *lr = &r->tsdb;

  return tsdb_value_add(&value->tsdb, lv, lr);
}

static int value_sub(value_t *value, int row, eval_env_t *env) {
  value_t *l = value->first_child;
  value_t *r = l->next;

  tsdb_value_t *lv = &l->tsdb;
  tsdb_value_t *lr = &r->tsdb;

  return tsdb_value_sub(&value->tsdb, lv, lr);
}

static int value_mul(value_t *value, int row, eval_env_t *env) {
  value_t *l = value->first_child;
  value_t *r = l->next;

  tsdb_value_t *lv = &l->tsdb;
  tsdb_value_t *lr = &r->tsdb;

  return tsdb_value_mul(&value->tsdb, lv, lr);
}

static int value_div(value_t *value, int row, eval_env_t *env) {
  value_t *l = value->first_child;
  value_t *r = l->next;

  tsdb_value_t *lv = &l->tsdb;
  tsdb_value_t *lr = &r->tsdb;

  return tsdb_value_div(&value->tsdb, lv, lr);
}

static int value_neg(value_t *value, int row, eval_env_t *env) {
  value_t *l = value->first_child;

  tsdb_value_t *xv = &l->tsdb;

  return tsdb_value_neg(&value->tsdb, xv);
}

static const value_func_t  _builtin_funcs[] = {
  {"add", value_add, 2, 2, 0},
  {"sub", value_sub, 2, 2, 0},
  {"mul", value_mul, 2, 2, 0},
  {"div", value_div, 2, 2, 0},
  {"neg", value_neg, 1, 1, 0},
  // {"primes", value_primes, 2, 2, 1},
};

const value_func_t* find_builtin_func(const char *name, size_t len) {
  static const size_t n = sizeof(_builtin_funcs) / sizeof(_builtin_funcs[0]);
  for (size_t i=0; i<n; ++i) {
    const value_func_t *func = _builtin_funcs + i;
    if (!func->visible) continue;
    if (len != strlen(func->name)) continue;
    if (strncasecmp(name, func->name, len)) continue;
    return func;
  }
  return NULL;
}

static int now_value_eval(value_t *value, int row, eval_env_t *env) {
  return now_eval(&value->tsdb, env);
}

value_t* parser_create_add_op(SAstCreateContext *pCxt, value_t *l, value_t *r) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  if (l->folded && r->folded) {
    tsdb_value_t *v  = &value->tsdb;
    tsdb_value_t *lv = &l->tsdb;
    tsdb_value_t *lr = &r->tsdb;
    int rr = tsdb_value_add(v, lv, lr);
    if (rr) {
      value_destroy(value);
      SET_ERR();
      return NULL;
    }
    value->folded = 1;
    value_destroy(l);
    value_destroy(r);
  } else {
    value->first_child = l;
    l->next            = r;

    l->parent    = value;
    r->parent    = value;

    TAOS_FIELD_E fld = {
      .type             = TSDB_DATA_TYPE_INT,
      .precision        = 0,
      .scale            = 0,
      .bytes            = sizeof(int32_t),
    };
    value_hint(l, &fld);
    value_hint(r, &fld);
  }

  value->token    = l->token;
  value->token.n  = r->token.z + r->token.n - l->token.z;

  value->eval    = value_add;
  value->release = NULL;

  return value;
}

value_t* parser_create_sub_op(SAstCreateContext *pCxt, value_t *l, value_t *r) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  if (l->folded && r->folded) {
    tsdb_value_t *v  = &value->tsdb;
    tsdb_value_t *lv = &l->tsdb;
    tsdb_value_t *lr = &r->tsdb;
    int rr = tsdb_value_sub(v, lv, lr);
    if (rr) {
      value_destroy(value);
      SET_ERR();
      return NULL;
    }
    value->folded = 1;
    value_destroy(l);
    value_destroy(r);
  } else {
    value->first_child = l;
    l->next            = r;

    l->parent    = value;
    r->parent    = value;

    TAOS_FIELD_E fld = {
      .type             = TSDB_DATA_TYPE_INT,
      .precision        = 0,
      .scale            = 0,
      .bytes            = sizeof(int32_t),
    };
    value_hint(l, &fld);
    value_hint(r, &fld);
  }

  value->token    = l->token;
  value->token.n  = r->token.z + r->token.n - l->token.z;

  value->eval    = value_sub;
  value->release = NULL;

  return value;
}

value_t* parser_create_mul_op(SAstCreateContext *pCxt, value_t *l, value_t *r) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  if (l->folded && r->folded) {
    tsdb_value_t *v  = &value->tsdb;
    tsdb_value_t *lv = &l->tsdb;
    tsdb_value_t *lr = &r->tsdb;
    int rr = tsdb_value_mul(v, lv, lr);
    if (rr) {
      value_destroy(value);
      SET_ERR();
      return NULL;
    }
    value->folded = 1;
    value_destroy(l);
    value_destroy(r);
  } else {
    value->first_child = l;
    l->next            = r;

    l->parent    = value;
    r->parent    = value;

    TAOS_FIELD_E fld = {
      .type             = TSDB_DATA_TYPE_INT,
      .precision        = 0,
      .scale            = 0,
      .bytes            = sizeof(int32_t),
    };
    value_hint(l, &fld);
    value_hint(r, &fld);
  }

  value->token    = l->token;
  value->token.n  = r->token.z + r->token.n - l->token.z;

  value->eval    = value_mul;
  value->release = NULL;

  return value;
}

value_t* parser_create_div_op(SAstCreateContext *pCxt, value_t *l, value_t *r) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  if (l->folded && r->folded) {
    tsdb_value_t *v  = &value->tsdb;
    tsdb_value_t *lv = &l->tsdb;
    tsdb_value_t *lr = &r->tsdb;
    int rr = tsdb_value_div(v, lv, lr);
    if (rr) {
      value_destroy(value);
      SET_ERR();
      return NULL;
    }
    value->folded = 1;
    value_destroy(l);
    value_destroy(r);
  } else {
    value->first_child = l;
    l->next            = r;

    l->parent    = value;
    r->parent    = value;

    TAOS_FIELD_E fld = {
      .type             = TSDB_DATA_TYPE_INT,
      .precision        = 0,
      .scale            = 0,
      .bytes            = sizeof(int32_t),
    };
    value_hint(l, &fld);
    value_hint(r, &fld);
  }

  value->token    = l->token;
  value->token.n  = r->token.z + r->token.n - l->token.z;

  value->eval    = value_div;
  value->release = NULL;

  return value;
}

value_t* parser_create_neg_op(SAstCreateContext *pCxt, SToken *neg, value_t *x) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  if (x->folded) {
    tsdb_value_t *v  = &value->tsdb;
    tsdb_value_t *xv = &x->tsdb;
    int rr = tsdb_value_neg(v, xv);
    if (rr) {
      value_destroy(value);
      SET_ERR();
      return NULL;
    }
    value->folded = 1;
    value_destroy(x);
  } else {
    value->first_child = x;

    x->parent    = value;

    TAOS_FIELD_E fld = {
      .type             = TSDB_DATA_TYPE_INT,
      .precision        = 0,
      .scale            = 0,
      .bytes            = sizeof(int32_t),
    };
    value_hint(x, &fld);
  }

  value->token    = *neg;
  value->token.n  = x->token.z + x->token.n - neg->z;

  value->eval    = value_neg;
  value->release = NULL;

  return value;
}

value_t* parser_create_now(SAstCreateContext *pCxt, SToken *token) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  value->token   = *token;

  value->eval    = now_value_eval;
  value->release = NULL;

  return value;
}

value_t* parser_create_cal_op(SAstCreateContext *pCxt, SToken *func, value_t *args) {
  value_t *value = (value_t*)taosMemoryCalloc(1, sizeof(*value));
  CHECK_OUT_OF_MEM(value);

  const value_func_t *bf = find_builtin_func(func->z, func->n);
  if (!bf) {
    value_destroy(value);
    pCxt->errCode = TSDB_CODE_PAR_FUNC_NOT_FOUND;
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "[%.*s]", func->n, func->z);
    return NULL;
  }

  size_t nr_args = 0;
  value_t *p = args;
  while (p) {
    ++nr_args;
    p = p->next;
  }

  if (nr_args < bf->min_args) {
    value_destroy(value);
    pCxt->errCode = TSDB_CODE_PAR_FUNC_MISSING_ARGS;
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "expecting at least %d args, but only got %zd args", bf->min_args, nr_args);
    return NULL;
  }

  if (nr_args > bf->max_args) {
    value_destroy(value);
    pCxt->errCode = TSDB_CODE_PAR_FUNC_TOO_MANY_ARGS;
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "expecting at most %d args, but got %zd args", bf->max_args, nr_args);
    return NULL;
  }


  int folded = 1;

  for (value_t *arg = args; arg; arg = arg->next) {
    if (!arg->folded) {
      folded = 0;
      break;
    }
  }

  if (1) {
    value_destroy(value);
    pCxt->errCode = TSDB_CODE_PAR_NOT_SUPPORT;
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "function [%.*s] not implemented yet", func->n, func->z);
    return NULL;
  }

  // if (folded) {
  // } else {
  //   value->first_child = args;

  //   x->parent    = value;
  // }

  // value->eval    = bf->eval;
  // value->release = NULL;

  // return value;
}

void values_destroy(value_t *values) {
  if (!values) return;
  value_t *p = values;
  while (p) {
    value_t *n = p->next;
    p->next = NULL;
    value_destroy(p);
    p = n;
  }
}

void values_append(value_t *values, value_t *value) {
  if (!value) return;
  value_t *p = values;
  while (p->next) p = p->next;
  p->next = value;
}

void row_destroy(row_t *row) {
  if (!row) return;
  values_destroy(row->first);
  taosMemoryFree(row);
}

size_t row_values_count(row_t *row) {
  return values_count(row->first);
}

size_t rows_count(row_t *rows) {
  size_t n = 0;
  for (row_t *p = rows; p; p = p->next) {
    ++n;
  }
  return n;
}

size_t values_count(value_t *values) {
  size_t n = 0;
  for (value_t *p = values; p; p = p->next) {
    ++n;
  }
  return n;
}

void rows_append(row_t *rows, row_t *row) {
  row_t *p = rows;
  while (p->next) p = p->next;
  p->next = row;
}

void rows_destroy(row_t *rows) {
  if (!rows) return;
  row_t *p = rows;
  while (p) {
    row_t *n = p->next;
    p->next = NULL;
    row_destroy(p);
    p = n;
  }
}

void using_clause_release(using_clause_t *using_clause) {
  if (!using_clause) return;
  nodesDestroyList(using_clause->tags); using_clause->tags = NULL;

  value_t *p = NULL, *n = NULL;
  for (p=using_clause->vals; n=p?p->next:NULL, p; p=n) {
    value_destroy(p);
  }
  using_clause->vals = NULL;
}

void using_clause_destroy(using_clause_t *using_clause) {
  if (!using_clause) return;
  using_clause_release(using_clause);

  taosMemoryFree(using_clause);
}

void simple_target_release(simple_target_t *simple_target) {
  if (!simple_target) return;
  using_clause_destroy(simple_target->using_clause);
  simple_target->using_clause = NULL;
  nodesDestroyList(simple_target->fields_clause);
  simple_target->fields_clause = NULL;
  rows_destroy(simple_target->rows);
  simple_target->rows = NULL;
}

void simple_target_destroy(simple_target_t *simple_target) {
  if (!simple_target) return;
  simple_target_release(simple_target);
  taosMemoryFree(simple_target);
}

void multiple_targets_release(multiple_targets_t *multiple_targets) {
  if (!multiple_targets) return;
  simple_target_t *p = multiple_targets->first;
  multiple_targets->first = NULL;
  for (simple_target_t *n = NULL; n = p ? p->next : NULL, p; p = n) {
    p->next = NULL;
    simple_target_destroy(p);
  }
}

void multiple_targets_destroy(multiple_targets_t *multiple_targets) {
  if (!multiple_targets) return;
  multiple_targets_release(multiple_targets);
  taosMemoryFree(multiple_targets);
}

int parser_db_table_token_normalize_from(SAstCreateContext *pCxt, db_table_token_t *db_tbl, SToken *db, SToken *tbl) {
  memset(db_tbl, 0, sizeof(*db_tbl));
  if (pCxt->errCode) return -1;
  db_tbl->db  = *db;
  db_tbl->tbl = *tbl;
  if (db->n) {
    if (from_token(pCxt, db_tbl->db_tbl.db, sizeof(db_tbl->db_tbl.db), db, "db name")) return -1;
  }
  if (from_token(pCxt, db_tbl->db_tbl.tbl, sizeof(db_tbl->db_tbl.tbl), tbl, "table name")) return -1;
  snprintf(db_tbl->db_tbl.db_tbl, sizeof(db_tbl->db_tbl.db_tbl), "%s.%s", db_tbl->db_tbl.db, db_tbl->db_tbl.tbl);
  return 0;
}

using_clause_t* parse_result_get_first_using_clause(parse_result_t *result) {
  if (!result || !result->meta.is_insert) return NULL;
  SNode *root = result->pRootNode;
  if (!root) return NULL;
  using_clause_t *using_clause = NULL;
  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
      SInsertMultiStmt   *pStmt            = (SInsertMultiStmt*)root;
      multiple_targets_t *multiple_targets = pStmt->multiple_targets;
      if (!multiple_targets) return NULL;
      simple_target_t    *first            = multiple_targets->first;
      if (!first) return NULL;

      return first->using_clause;
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT: {
      SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)root;

      return pStmt->using_clause;
    } break;
    default:
      return NULL;
  }
}

void parse_result_get_first_table_name(parse_result_t *parse_result, db_table_t *tbl) {
  tbl->db[0]  = '\0';
  tbl->tbl[0] = '\0';
  if (!parse_result || !parse_result->meta.is_insert) return;
  SNode *root = parse_result->pRootNode;
  if (!root) return;
  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
      SInsertMultiStmt   *pStmt            = (SInsertMultiStmt*)root;
      multiple_targets_t *multiple_targets = pStmt->multiple_targets;
      if (!multiple_targets) return;
      simple_target_t    *first            = multiple_targets->first;
      if (!first) return;
      *tbl = first->tbl.db_tbl;
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT:
      return;
    default:
      return;
  }
}

void parse_result_get_first_using_table_name(parse_result_t *parse_result, db_table_t *using_tbl) {
  using_tbl->db[0]  = '\0';
  using_tbl->tbl[0] = '\0';
  using_clause_t *using_clause = parse_result_get_first_using_clause(parse_result);
  if (!using_clause) return;
  *using_tbl = using_clause->supertable.db_tbl;
}

void InsertMultiStmtRelease(SInsertMultiStmt *pStmt) {
  if (!pStmt) return;
  multiple_targets_destroy(pStmt->multiple_targets);
  pStmt->multiple_targets = NULL;
}

void InsertQuestionStmtRelease(SInsertQuestionStmt *pStmt) {
  if (!pStmt) return;
  value_destroy(pStmt->table_value);            pStmt->table_value     = NULL;
  using_clause_destroy(pStmt->using_clause);    pStmt->using_clause    = NULL;
  nodesDestroyList(pStmt->fields_clause);       pStmt->fields_clause   = NULL;
  rows_destroy(pStmt->rows);                    pStmt->rows            = NULL;
}

SNode* createInsertMultiStmt(SAstCreateContext *pCxt, multiple_targets_t *multiple_targets) {
  CHECK_PARSER_STATUS(pCxt);
  SInsertMultiStmt* pStmt = (SInsertMultiStmt*)nodesMakeNode(QUERY_NODE_INSERT_MULTI_STMT);
  CHECK_OUT_OF_MEM(pStmt);

  pStmt->multiple_targets = multiple_targets;
  return (SNode*)pStmt;
}

SNode* createInsertQuestionStmt(SAstCreateContext *pCxt, value_t *question, using_clause_t *using_clause, SNodeList *fields_clause, row_t *rows) {
  CHECK_PARSER_STATUS(pCxt);

  if (using_clause) {
    db_table_t *supertable = &using_clause->supertable.db_tbl;

    if (question->first_child->next) {
      tsdb_value_t *db = &question->first_child->tsdb;
      if (supertable->db[0]) {
        if (db->varchar.bytes != strlen(supertable->db) || strncmp(db->varchar.str, supertable->db, db->varchar.bytes)) {
          GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR, "db missmatch:[%.*s.?] <> [%.*s.%.*s]",
              question->first_child->token.n, question->first_child->token.z,
              using_clause->supertable.db.n, using_clause->supertable.db.z,
              using_clause->supertable.tbl.n, using_clause->supertable.tbl.z);
          return NULL; 
        }
      }
    }
  }


  SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)nodesMakeNode(QUERY_NODE_INSERT_QUESTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);

  pStmt->table_value         = question;
  pStmt->using_clause        = using_clause;
  pStmt->fields_clause       = fields_clause;
  pStmt->rows                = rows;

  return (SNode*)pStmt;
}

