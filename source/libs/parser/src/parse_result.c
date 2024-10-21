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

#include "insertnodes.h"
#include "parAst.h"
#include "tsdb_value.h"
#include "parse_result.h"

static int32_t parse_result_get_using_tbl(parse_result_t *parse_result, desc_params_ctx_t *ctx, const SName *src) {
  int32_t code = TSDB_CODE_SUCCESS;

  SStmtAPI2 *api2 = ctx->api2;

  SUserAuthInfo authInfo = {0}; {
    snprintf(authInfo.user, sizeof(authInfo.user), "%s", ctx->current_user);
    authInfo.tbName = *src;
    authInfo.type = AUTH_TYPE_WRITE;
  }

  SUserAuthRes authRes = {0};
  code = catalogChkAuth(ctx->pCatalog, &ctx->connInfo, &authInfo, &authRes);
  if (code) return code;
  if (!authRes.pass[AUTH_RES_BASIC]) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  if (NULL != authRes.pCond[AUTH_RES_BASIC]) {
    api2->pTagCond = authRes.pCond[AUTH_RES_BASIC];
    // FIXME: freemine, what's *pTagCond?
    nodesDestroyNode(api2->pTagCond);
    api2->pTagCond = NULL;
  }

  code = catalogGetSTableMeta(ctx->pCatalog, &ctx->connInfo, src, &api2->pTableMeta);
  if (code) {
    nodesDestroyNode(api2->pTagCond);
    api2->pTagCond = NULL;
    taosMemoryFreeClear(api2->pTableMeta);
    return code;
  }

  return code;
}

void parse_result_normalize_db_table(db_table_t *db_tbl, const char *current_db) {
  // NOTE: it's caller's duty to guarantee the validity of both `db_tbl` and `current_db`
  if (!db_tbl->db[0] && current_db) {
    snprintf(db_tbl->db, sizeof(db_tbl->db), "%s", current_db);
  }
  snprintf(db_tbl->db_tbl, sizeof(db_tbl->db_tbl), "%s.%s", db_tbl->db, db_tbl->tbl);
}

static int32_t parse_result_describe_InsertQuestion(desc_params_ctx_t *ctx) {
  int32_t code = TSDB_CODE_SUCCESS;

  SStmtAPI2      *api2          = ctx->api2;
  parse_result_t *parse_result  = &api2->parse_result;

  SNode *root = parse_result->pRootNode;
  SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)root;
  using_clause_t *using_clause = pStmt->using_clause;
  if (!using_clause) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_NOT_SUPPORT, "missing USING");
  }
  db_table_t       *supertable = &using_clause->supertable.db_tbl;
  SName tbName = {0};
  parse_result_create_sname(parse_result, &tbName, supertable, ctx->acctId, ctx->current_db);
  code = parse_result_get_using_tbl(parse_result, ctx, &tbName);
  if (code) return code;
  STableComInfo *tableInfo = &api2->pTableMeta->tableInfo;
  SSchema *schema          = &api2->pTableMeta->schema[0];

  for (uint8_t i=0; i<tableInfo->numOfTags; ++i) {
    SSchema   *p             = schema + tableInfo->numOfColumns + i;
    SNodeList *tags          = using_clause->tags;
    value_t   *tag_val       = using_clause->vals;

    if (tags) {
      SNode *node  = NULL;
      FOREACH(node, tags) {
        SColumnNode *col = (SColumnNode*)node;
        if (strcmp(p->name, col->colName) == 0) break;
        tag_val = value_next(tag_val);
        if (!tag_val) break;
      }
    } else {
      tag_val = value_by_idx(tag_val, i);
    }

    if (!tag_val) {
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_SYNTAX_ERROR, "unknow tag:[%.*s]", (int)sizeof(p->name), p->name);
    }

    TAOS_FIELD_E fld = {
      .type                    = p->type,
      .precision               = tableInfo->precision,
      .scale                   = 0,
      .bytes                   = p->bytes,
    };
    snprintf(fld.name, sizeof(fld.name), "%.*s", (int)sizeof(p->name), p->name);
    value_hint(tag_val, &fld);
  }

  for (uint8_t i=0; i<tableInfo->numOfColumns; ++i) {
    SSchema   *p             = schema + i;
    SNodeList *cols          = pStmt->fields_clause;
    value_t   *col_val       = pStmt->rows->first;

    if (cols) {
      SNode *node  = NULL;
      FOREACH(node, cols) {
        SColumnNode *col = (SColumnNode*)node;
        if (strcmp(p->name, col->colName) == 0) break;
        col_val = value_next(col_val);
        if (!col_val) break;
      }
    } else {
      col_val = value_by_idx(col_val, i);
    }

    if (!col_val) {
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_SYNTAX_ERROR, "unknow col:[%.*s]", (int)sizeof(p->name), p->name);
    }

    TAOS_FIELD_E fld = {
      .type                    = p->type,
      .precision               = tableInfo->precision,
      .scale                   = 0,
      .bytes                   = p->bytes,
    };
    snprintf(fld.name, sizeof(fld.name), "%.*s", (int)sizeof(p->name), p->name);
    value_hint(col_val, &fld);
  }

  api2->tbname_required      = 1;
  api2->nr_tags              = tableInfo->numOfTags;
  api2->nr_cols              = tableInfo->numOfColumns;

  TAOS_FIELD_E *param = api2->params;
  TAOS_FIELD_E *end   = api2->params + api2->cap_params;

  const TAOS_FIELD_E *src = NULL;
  value_t *v = NULL;

/* { */
#define R(x)                                                  \
  if (param >= end) return code;                              \
  v = x;                                                      \
  for (int i=0; v ;++i) {                                     \
    value_t *p = NULL, *n = NULL;                             \
    for_each_value_post_order(v, p, n) {                      \
      src = value_param_type(p);                              \
      if (!src) continue;                                     \
      if (param >= end) return code;                          \
      param->type             = src->type;                    \
      param->precision        = src->precision;               \
      param->scale            = src->scale;                   \
      param->bytes            = src->bytes;                   \
      ++param;                                                \
    }                                                         \
    v = value_next(v);                                        \
  }                                                           \

  R(pStmt->table_value);
  R(using_clause->vals);
  R(pStmt->rows->first);

#undef R
/* } */

  return code;
}

int32_t parse_result_describe_params(desc_params_ctx_t *ctx) {
  SStmtAPI2      *api2          = ctx->api2;
  parse_result_t *parse_result  = &api2->parse_result;

  if (!parse_result || !parse_result->meta.is_insert) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }

  SNode *root = parse_result->pRootNode;
  if (!root) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_NOT_SUPPORT, "");
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT:
      return parse_result_describe_InsertQuestion(ctx);
    default:
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
}

int32_t parse_result_get_target_table(parse_result_t *parse_result, int row, const char **tbname, eval_env_t *env) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (!parse_result || !parse_result->meta.is_insert) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }

  SNode *root = parse_result->pRootNode;
  if (!root) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_NOT_SUPPORT, "");
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT: {
      SNode *root = parse_result->pRootNode;
      SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)root;
      value_t *table_value = pStmt->table_value;
      code = value_eval(table_value, row, env);
      if (code) return code;
      const tsdb_value_t *tsdb = value_tsdb_value(table_value);
      if (tsdb->type != TSDB_DATA_TYPE_VARCHAR) {
        return TD_SET_ERR(TSDB_CODE_PAR_INVALID_TBNAME, "can NOT convert [%d]%s into tbname", tsdb->type, taosDataTypeName(tsdb->type));
      }
      *tbname = tsdb->varchar.str;
      return 0;
    } break;
    default:
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
}

void parse_result_create_sname(parse_result_t *parse_result, SName* dst, db_table_t *src, int32_t acctId, const char* dbName) {
  // TODO: freemine, logic from insCreateSName

  // NOTE: freemine, strdequote is destructive
  if (src->db[0]) {
    // NOTE: freemine, no need to check return code, coz db_table_t is verified-internal-representation of db.table
    tNameSetDbName(dst, acctId, src->db, strlen(src->db));
  } else {
    // NOTE: freemine, dbName shall be verified by the caller and is internal-representation, such as lower or without wrapping by '`'
    tNameSetDbName(dst, acctId, dbName, strlen(dbName));
  }

  // NOTE: freemine, no need to check return code, coz db_table_t is verified-internal-representation of db.table
  tNameFromString(dst, src->tbl, T_NAME_TABLE);
}

void parse_result_post_fill_meta(parse_result_t *parse_result) {
  if (!parse_result) return;
  SNode *root = parse_result->pRootNode;
  if (!root) return;
  parse_result_meta_t *meta = &parse_result->meta;
  meta->nr_tags            = 0;
  meta->nr_cols            = 0;
  meta->is_insert          = 0;
  meta->tbname_required    = 0;
  meta->has_using_clause   = 0;

  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
      meta->is_insert           = 1;
      meta->tbname_required     = 0;
      SInsertMultiStmt   *pStmt            = (SInsertMultiStmt*)root;
      multiple_targets_t *multiple_targets = pStmt->multiple_targets;
      if (!multiple_targets) return;
      simple_target_t    *first            = multiple_targets->first;
      if (!first) return;
      using_clause_t     *using_clause     = first->using_clause;
      if (using_clause) {
        meta->has_using_clause  = 1;
        if (using_clause->tags) {
          meta->nr_tags         = using_clause->tags->length;
        } else if (using_clause->vals) {
          meta->nr_tags         = values_count(using_clause->vals);
        }
      }
      if (first->fields_clause) {
        meta->nr_cols           = first->fields_clause->length;
      } else if (first->rows) {
        meta->nr_cols           = row_values_count(first->rows);
      }
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT: {
      meta->is_insert           = 1;
      meta->tbname_required     = 1;
      SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)root;
      using_clause_t     *using_clause     = pStmt->using_clause;
      if (using_clause) {
        meta->has_using_clause  = 1;
        if (using_clause->tags) {
          meta->nr_tags         = using_clause->tags->length;
        } else if (using_clause->vals) {
          meta->nr_tags         = values_count(using_clause->vals);
        }
      }
      if (pStmt->fields_clause) {
        meta->nr_cols           = pStmt->fields_clause->length;
      } else if (pStmt->rows) {
        meta->nr_cols           = row_values_count(pStmt->rows);
      }
    } break;
    default:
      break;
  }
}

static int parse_result_sql_append_impl(parse_result_t *parse_result, const char *fmt, ...) {
  int r = -1;

  int n = 0;

  va_list ap;
  va_start(ap, fmt);

  va_list aq;
  va_copy(aq, ap);
  n = vsnprintf(parse_result->sql + parse_result->nr_sql, (parse_result->cap_sql - parse_result->nr_sql), fmt, aq);
  va_end(aq);

  if (n >= 0 && n < parse_result->cap_sql - parse_result->nr_sql) {
    parse_result->nr_sql += n;
    r = 0;
  } else {
    size_t cap = (parse_result->nr_sql + n + 1 + 255) / 256 * 256;
    char *p = taosMemoryRealloc(parse_result->sql, cap + 1);
    if (p) {
      memset(p + parse_result->nr_sql, 0, cap + 1 - parse_result->cap_sql);
      parse_result->cap_sql         = cap;
      parse_result->sql             = p;
    }
    n = vsnprintf(parse_result->sql + parse_result->nr_sql, (parse_result->cap_sql - parse_result->nr_sql), fmt, aq);
    if (n >= 0 && n < parse_result->cap_sql - parse_result->nr_sql) {
      parse_result->nr_sql += n;
      r = 0;
    }
  }

  va_end(ap);

  return r;
}

#define parse_result_sql_append(parse_result, fmt, ...)                        \
  (                                                                            \
    0 ? fprintf(stderr, fmt, ##__VA_ARGS__)                                    \
      : parse_result_sql_append_impl(parse_result, fmt, ##__VA_ARGS__)         \
  )

static int parse_result_templatify_sql_ID(parse_result_t *parse_result, const char *s, int n) {
  int r = 0;
  r = parse_result_sql_append(parse_result, "`");
  if (r) return -1;
  const char *p = s;
  for (int i=0; i<n; ++i) {
    const char c = s[i];
    if (c == '`') {
      r = parse_result_sql_append(parse_result, "%.*s", (int)(s + i - p), p);
      if (r) return -1;
      r = parse_result_sql_append(parse_result, "``");
      if (r) return -1;
      p = s + 1;
    }
  }
  r = parse_result_sql_append(parse_result, "%.*s", (int)(s + n - p), p);
  if (r) return -1;
  r = parse_result_sql_append(parse_result, "`");
  if (r) return -1;
  return 0;
}

static parse_result_templatify_sql_e parse_result_templatify_sql_InsertQuestion(parse_result_t *parse_result) {
  int r = 0;

  SNode *root = parse_result->pRootNode;
  SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)root;
  r = parse_result_sql_append(parse_result, "insert into ");
  if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
  value_t *table_value = pStmt->table_value;
  const SToken *t = value_token(table_value);
  if (!t) return PARSE_RESULT_TEMPLATIFY_SQL_INTERNAL_ERROR;
  r = parse_result_sql_append(parse_result, "%.*s ", t->n, t->z);
  if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;

  using_clause_t *using_clause = pStmt->using_clause;
  if (using_clause) {
    r = parse_result_sql_append(parse_result, "using ");
    if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;

    db_table_token_t *supertable = &using_clause->supertable;
    if (supertable->db.n) {
      r = parse_result_templatify_sql_ID(parse_result, supertable->db.z, supertable->db.n);
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      r = parse_result_sql_append(parse_result, ".");
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
    }
    if (supertable->tbl.n) {
      r = parse_result_templatify_sql_ID(parse_result, supertable->tbl.z, supertable->tbl.n);
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
    }

    r = parse_result_sql_append(parse_result, " ");
    if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;

    SNodeList *tags = using_clause->tags;
    if (tags && tags->length) {
      r = parse_result_sql_append(parse_result, "(" /* ) */);
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;

      int idx = 0;
      SNode *node = NULL;
      FOREACH(node, tags) {
        if (idx++) {
          r = parse_result_sql_append(parse_result, ",");
          if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
        }
        SColumnNode *col = (SColumnNode*)node;
        r = parse_result_templatify_sql_ID(parse_result, col->colName, (int)strnlen(col->colName, sizeof(col->colName)));
        if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      }

      r = parse_result_sql_append(parse_result, /* ( */ ") ");
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
    }
    value_t *vals = using_clause->vals;
    int n_vals = vals ? values_count(vals) : 0;
    if (n_vals) {
      r = parse_result_sql_append(parse_result, "tags (" /* ) */);
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      for (int i=0; i<n_vals; ++i) {
        if (i) {
          r = parse_result_sql_append(parse_result, ",");
          if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
        }
        r = parse_result_sql_append(parse_result, "?");
        if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      }
      r = parse_result_sql_append(parse_result, /* ( */ ") ");
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
    }

    SNodeList *fields_clause = pStmt->fields_clause;
    if (fields_clause && fields_clause->length) {
      r = parse_result_sql_append(parse_result, "(" /* ) */);
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;

      int idx = 0;
      SNode *node = NULL;
      FOREACH(node, fields_clause) {
        if (idx++) {
          r = parse_result_sql_append(parse_result, ",");
          if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
        }
        SColumnNode *col = (SColumnNode*)node;
        r = parse_result_templatify_sql_ID(parse_result, col->colName, (int)strnlen(col->colName, sizeof(col->colName)));
        if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      }

      r = parse_result_sql_append(parse_result, /* ( */ ") ");
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
    }

    row_t *rows = pStmt->rows;
    vals = rows ? rows->first : NULL;
    n_vals = vals ? values_count(vals) : 0;
    if (n_vals) {
      r = parse_result_sql_append(parse_result, "values (" /* ) */);
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      for (int i=0; i<n_vals; ++i) {
        if (i) {
          r = parse_result_sql_append(parse_result, ",");
          if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
        }
        r = parse_result_sql_append(parse_result, "?");
        if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
      }
      r = parse_result_sql_append(parse_result, /* ( */ ") ");
      if (r) return PARSE_RESULT_TEMPLATIFY_SQL_OOM;
    }
  }
  return PARSE_RESULT_TEMPLATIFY_SQL_OK;
}

parse_result_templatify_sql_e parse_result_templatify_sql(parse_result_t *parse_result) {
  if (!parse_result) return PARSE_RESULT_TEMPLATIFY_SQL_OK;
  if (parse_result->sql) {
    parse_result->sql[0] = '\0';
    parse_result->nr_sql = 0;
  }
  SNode *root = parse_result->pRootNode;
  if (!root) return PARSE_RESULT_TEMPLATIFY_SQL_OK;
  const parse_result_meta_t *meta = &parse_result->meta;
  if (!meta->is_insert) return PARSE_RESULT_TEMPLATIFY_SQL_OK;

  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT: {
      return parse_result_templatify_sql_InsertQuestion(parse_result);
    } break;
    default:
      break;
  }

  return PARSE_RESULT_TEMPLATIFY_SQL_OK;
}

static int32_t parse_result_visit_InsertQuestion(parse_result_t *parse_result, int row, int rows, parse_result_visit_ctx_t *ctx) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode *root = parse_result->pRootNode;
  SInsertQuestionStmt* pStmt = (SInsertQuestionStmt*)root;

  int r = 0;
  int abortion = 0;

  TAOS_MULTI_BIND *mbs = ctx->env->mbs;

  for (int i=row; ; ++i) {
    if (i-row >= rows || i >= mbs->num) {
      if (ctx->on_tbname) {
        code = ctx->on_tbname(NULL, 0, i, ctx->arg, &abortion);
      }
      return code;
    }
    if (ctx->on_tbname) {
      value_t *table_value = pStmt->table_value;
      r = value_eval(table_value, i, ctx->env);
      if (r) return td_errno();
      const tsdb_value_t *tsdb = value_tsdb_value(table_value);
      if (tsdb->type != TSDB_DATA_TYPE_VARCHAR) {
        return TD_SET_ERR(TSDB_CODE_PAR_INVALID_TBNAME, "can NOT convert [%d]%s into tbname", tsdb->type, taosDataTypeName(tsdb->type));
      }
      if (tsdb->varchar.str[0] == '.') {
        code = ctx->on_tbname(tsdb->varchar.str+1, tsdb->varchar.bytes-1, i, ctx->arg, &abortion);
      } else {
        code = ctx->on_tbname(tsdb->varchar.str, tsdb->varchar.bytes, i, ctx->arg, &abortion);
      }
      if (code || abortion) return code;
    }

    using_clause_t *using_clause = pStmt->using_clause;
    if (ctx->on_tag_val) {
      value_t   *tag_val = using_clause ? using_clause->vals : NULL;
      if (tag_val) {
        int j = 0;
        while (tag_val) {
          r = value_eval(tag_val, i, ctx->env);
          if (r) return td_errno();
          code = ctx->on_tag_val(tag_val, row, i, j++, ctx->arg, &abortion);
          if (code || abortion) return code;
          tag_val = value_next(tag_val);
        }
      }
    }

    if (ctx->on_col_val) {
      value_t   *col_val = pStmt->rows ? pStmt->rows->first : NULL;
      if (col_val) {
        int j = 0;
        while (col_val) {
          r = value_eval(col_val, i, ctx->env);
          if (r) return td_errno();
          code = ctx->on_col_val(col_val, row, i, j++, ctx->arg, &abortion);
          if (code || abortion) return code;
          col_val = value_next(col_val);
        }
      }
    }
  }
}

int32_t parse_result_visit(parse_result_t *parse_result, int row, int rows, parse_result_visit_ctx_t *ctx) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (!parse_result || !parse_result->meta.is_insert) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }

  SNode *root = parse_result->pRootNode;
  if (!root) {
    return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
  switch (root->type) {
    case QUERY_NODE_INSERT_MULTI_STMT: {
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_NOT_SUPPORT, "");
    } break;
    case QUERY_NODE_INSERT_QUESTION_STMT: {
      return parse_result_visit_InsertQuestion(parse_result, row, rows, ctx);
    } break;
    default:
      return parse_result_set_errmsg(parse_result, TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }
}

static int32_t dump_tbname(const char *tbname, size_t len, int row, void *arg, int *abortion) {
  *abortion = 0;
  if (tbname) fprintf(stderr, "\n%d:tbname:[%.*s]", row + 1, (int)len, tbname);
  return 0;
}

static int32_t dump_tag_val(const value_t *val, int start, int row, int col, void *arg, int *abortion) {
  *abortion = 0;
  if (col == 0) {
    fprintf(stderr, ";tagvals:");
  } else {
    fprintf(stderr, ",");
  }
  const tsdb_value_t *tsdb = value_tsdb_value(val);
  const SToken *token      = value_token(val);
  fprintf(stderr, "@%.*s@", token->n, token->z);
  tsdb_dump(tsdb);
  return 0;
}

static int32_t dump_col_val(const value_t *val, int start, int row, int col, void *arg, int *abortion) {
  *abortion = 0;
  if (col == 0) {
    fprintf(stderr, ";colvals:");
  } else {
    fprintf(stderr, ",");
  }
  const tsdb_value_t *tsdb = value_tsdb_value(val);
  const SToken *token      = value_token(val);
  fprintf(stderr, "(%.*s)=>", token->n, token->z);
  tsdb_dump(tsdb);
  return 0;
}

int32_t parse_result_dump_vals(parse_result_t *parse_result, eval_env_t *env) {
  int32_t code = TSDB_CODE_SUCCESS;

  parse_result_visit_ctx_t ctx = {
    .env           = env,
    .on_tbname     = dump_tbname,
    .on_tag_val    = dump_tag_val,
    .on_col_val    = dump_col_val,
  };
  code = parse_result_visit(parse_result, 0, env->mbs->num, &ctx);
  fprintf(stderr, "\n");
  return code;
}

