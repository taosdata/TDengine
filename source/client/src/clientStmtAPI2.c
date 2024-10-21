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


#include "clientInt.h"
#include "clientLog.h"
#include "tdef.h"

#include "clientStmtAPI2.h"
#include "parser.h"

void mbs_cache_reset(mbs_cache_t *mbs_cache) {
  if (!mbs_cache) return;
}

void mbs_cache_release(mbs_cache_t *mbs_cache) {
  if (!mbs_cache) return;
  mbs_cache_reset(mbs_cache);
  taosMemoryFreeClear(mbs_cache->mbs);
  mbs_cache->cap_mbs = 0;
  taosMemoryFreeClear(mbs_cache->buf);
  mbs_cache->cap_buf = 0;
}

static int32_t mbs_cache_keep(mbs_cache_t *mbs_cache, size_t nr) {
  if (nr > mbs_cache->cap_mbs) {
    size_t cap = (nr + 15) / 16 * 16;
    TAOS_MULTI_BIND *p = (TAOS_MULTI_BIND*)taosMemoryRealloc(mbs_cache->mbs, cap * sizeof(*p));
    if (!p) return TD_SET_ERR(TSDB_CODE_OUT_OF_MEMORY, "");
    memset(p + mbs_cache->cap_mbs, 0, (cap - mbs_cache->cap_mbs) * sizeof(*p));
    mbs_cache->mbs     = p;
    mbs_cache->cap_mbs = cap;
  }
  return 0;
}

static int32_t mbs_cache_keep_buf(mbs_cache_t *mbs_cache, size_t cap_buf) {
  if (cap_buf > mbs_cache->cap_buf) {
    char *p = (char*)taosMemoryRealloc(mbs_cache->buf, cap_buf);
    if (!p) return TD_SET_ERR(TSDB_CODE_OUT_OF_MEMORY, "");
    mbs_cache->buf     = p;
    mbs_cache->cap_buf = cap_buf;
  }
  return 0;
}

static void mbs_cache_zero(mbs_cache_t *mbs_cache) {
  memset(mbs_cache->buf, 0, mbs_cache->cap_buf);
}

void stmtExec2ClearRes(STscStmt *pStmt) {
  SStmtAPI2 *api2 = &pStmt->api2;

  if (api2->res_from_taos_query) {
    taos_free_result(api2->res_from_taos_query);
    api2->res_from_taos_query = NULL;
    api2->use_res_from_taos_query = 0;
  }
}

static void stmtResetPrepareInfo2(STscStmt *pStmt) {
  SStmtAPI2 *api2 = &pStmt->api2;
  parse_result_reset(&api2->parse_result);
  stmtExec2ClearRes(pStmt);
  mbs_cache_reset(&api2->mbs_tags);
  mbs_cache_reset(&api2->mbs_cols);
  api2->questions                  = 0;
  api2->scanned                    = 0;
  api2->is_insert                  = 0;
  api2->tbname_required            = 0;
  api2->without_using_clause       = 0;
  api2->nr_tags                    = 0;
  api2->nr_cols                    = 0;
  api2->prepared                   = 0;
  api2->options                    = 0;

  nodesDestroyNode(api2->pTagCond);
  api2->pTagCond = NULL;
  taosMemoryFreeClear(api2->pTableMeta);

  pStmt->api_type = STMT_API_UNKNOWN;
}

static void stmtReleaseMbs(STscStmt *pStmt) {
  SStmtAPI2 *api2 = &pStmt->api2;
  taosMemoryFreeClear(api2->mbs_cache);
  taosMemoryFreeClear(api2->conv_buf);
  api2->cap_mbs      = 0;
  api2->cap_conv_buf = 0;
}

int stmtKeepMbs(STscStmt *pStmt, int nr) {
  SStmtAPI2 *api2 = &pStmt->api2;
  if (nr > api2->cap_mbs) {
    TAOS_MULTI_BIND *p = NULL;
    p = (TAOS_MULTI_BIND*)taosMemoryRealloc(api2->mbs_cache, nr * 2 * sizeof(*p));
    if (!p) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    // memset(p, 0, nr * 2 * sizeof(*p));
    api2->mbs_cache     = p;
    api2->cap_mbs       = nr;
  }

  // memset(api2->mbs_cache, 0, api2->cap_mbs * sizeof(*api2->mbs_cache));

  return TSDB_CODE_SUCCESS;
}

void stmtReleasePrepareInfo2(STscStmt *pStmt) {
  stmtResetPrepareInfo2(pStmt);

  SStmtAPI2 *api2 = &pStmt->api2;
  parse_result_release(&pStmt->api2.parse_result);

  taosMemoryFreeClear(pStmt->api2.params);
  pStmt->api2.cap_params = 0;
  stmtReleaseMbs(pStmt);
  mbs_cache_release(&api2->mbs_tags);
  mbs_cache_release(&api2->mbs_cols);
}

void stmtUnbindParams2(STscStmt *pStmt) {
  pStmt->api2.mbs_from_app      = NULL;
  pStmt->api2.nr_mbs_from_app   = 0;
}

static int stmtReallocParams(TAOS_STMT *stmt, size_t nr_params) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (nr_params > pStmt->api2.cap_params) {
    TAOS_FIELD_E *p = (TAOS_FIELD_E*)taosMemoryRealloc(pStmt->api2.params, sizeof(*p) * nr_params);
    if (!p) STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);

    pStmt->api2.params           = p;
    pStmt->api2.cap_params       = nr_params;
  }

  return TSDB_CODE_SUCCESS;
}

static const TAOS_FIELD_E  _default_param = {
  .name           = "?",
  .type           = TSDB_DATA_TYPE_VARCHAR,
  .precision      = 0,
  .scale          = 0,
  // FIXME: who on-the-earth would provide a string longer than 1024 as a real parameter?
  .bytes          = 1024+2,
};

static const TAOS_FIELD_E  _default_tbname = {
  // FIXME: is `tbname` a valid one?
  .name           = "tbname",
  .type           = TSDB_DATA_TYPE_VARCHAR,
  .precision      = 0,
  .scale          = 0,
  // FIXME: check the TDengine design manual
  .bytes          = 192+2,
};

static const TAOS_FIELD_E  _default_null_param = {
  .name           = "?",
  .type           = TSDB_DATA_TYPE_NULL,
  .precision      = 0,
  .scale          = 0,
  // FIXME:
  .bytes          = 1024+2,
};


static int stmtDescribeNonInsertParams2(TAOS_STMT *stmt) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt* pStmt = (STscStmt*)stmt;

  int nr_params = 0;
  code = stmtGetParamNumInternal(stmt, &nr_params);
  if (TSDB_CODE_SUCCESS != code) return code;

  if (nr_params != pStmt->api2.questions) {
    // FIXME: there might be logic error in `qScanSql`
    //        fake TSDB_CODE_TSC_SQL_SYNTAX_ERROR for the moment
    STMT_ERR_RET(TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
  }
  STMT_ERR_RET(stmtReallocParams(stmt, nr_params));
  TAOS_FIELD_E *params = pStmt->api2.params;
  for (size_t i=0; i<pStmt->api2.questions; ++i) {
    *params++ = _default_param;
  }

  return TSDB_CODE_SUCCESS;
}

static int stmtGuessInsertParams2(TAOS_STMT *stmt, TAOS_FIELD_E **tags, int *nr_tags, TAOS_FIELD_E **cols, int *nr_cols) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt* pStmt = (STscStmt*)stmt;

  size_t nr_params = 0;

  const char *tbname = "__whatever_you_like__";

  code = stmt_get_tag_fields(stmt, nr_tags, tags);
  switch (code) {
    case TSDB_CODE_SUCCESS:
      nr_params += *nr_tags;
      code = stmt_get_col_fields(stmt, nr_cols, cols);
      nr_params += *nr_cols;
      break;
    case TSDB_CODE_TSC_STMT_TBNAME_ERROR:
      pStmt->api2.tbname_required = 1;
      nr_params += 1;
      code = stmt_set_tbname(stmt, tbname);
      switch (code) {
        case TSDB_CODE_SUCCESS:
          code = stmt_get_tag_fields(stmt, nr_tags, tags);
          if (TSDB_CODE_SUCCESS != code) return code;
          nr_params += *nr_tags;
          code = stmt_get_col_fields(stmt, nr_cols, cols);
          if (TSDB_CODE_SUCCESS != code) return code;
          nr_params += *nr_cols;
          break;
        case TSDB_CODE_PAR_TABLE_NOT_EXIST:
          pStmt->api2.without_using_clause = 1;
          nr_params = pStmt->api2.questions;
          break;
        default:
          STMT_ERR_RET(code);
          break;
      }
      break;
    case TSDB_CODE_TSC_STMT_API_ERROR:
      code = stmt_get_col_fields(stmt, nr_cols, cols);
      if (TSDB_CODE_SUCCESS != code) return code;
      nr_params += *nr_cols;
      break;
    default:
      STMT_ERR_RET(code);
      break;
  }

  if (!(pStmt->api2.options & TAOS_STMT_PREPARE2_OPTION_PURE_PARSE)) {
    if (nr_params != pStmt->api2.questions) {
      // FIXME: there might be logic error in `qScanSql`
      //        fake TSDB_CODE_TSC_SQL_SYNTAX_ERROR for the moment
      STMT_ERR_RET(TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
    }
  }

  STMT_ERR_RET(stmtReallocParams(stmt, nr_params));

  TAOS_FIELD_E *p = pStmt->api2.params;
  if (pStmt->api2.tbname_required) {
    *p++ = _default_tbname;
  }
  if (!pStmt->api2.without_using_clause) {
    for (size_t i=0; i<*nr_tags; ++i) {
      *p++ = (*tags)[i];
    }
    for (size_t i=0; i<*nr_cols; ++i) {
      *p++ = (*cols)[i];
    }
  } else {
    for (size_t i=1; i<nr_params; ++i) {
      // NOTE: let it be, we'll adjust according to real-tbname provided by user
      //       during taos_stmt_execute call
      *p++ = _default_null_param;
    }
  }

  pStmt->api2.nr_tags          = *nr_tags;
  pStmt->api2.nr_cols          = *nr_cols;

  return TSDB_CODE_SUCCESS;
}

static int stmtDescribeParams2(TAOS_STMT *stmt) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt       *pStmt        = (STscStmt*)stmt;
  SStmtAPI2      *api2         = &pStmt->api2;

  if (!pStmt->api2.is_insert) {
    return stmtDescribeNonInsertParams2(stmt);
  }

  TAOS_FIELD_E *tags          = NULL;
  int           nr_tags       = 0;
  TAOS_FIELD_E *cols          = NULL;
  int           nr_cols       = 0;

  code = stmtGuessInsertParams2(stmt, &tags, &nr_tags, &cols, &nr_cols);

  taosMemoryFreeClear(tags);
  taosMemoryFreeClear(cols);

  STMT_ERR_RET(code);
  return TSDB_CODE_SUCCESS;
}

static int stmtPostPrepare2Ext(TAOS_STMT* stmt) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt        *pStmt         = (STscStmt*)stmt;
  STscObj         *pObj          = pStmt->taos;
  SStmtAPI2       *api2          = &pStmt->api2;

  const char *sql    = pStmt->sql.originSql;
  int32_t     length = pStmt->sql.originLen;

  char db[TSDB_DB_FNAME_LEN+1]; *db = '\0';
  parse_result_t *parse_result = &api2->parse_result;

  stmt_current_db(stmt, db, sizeof(db));
  int n = snprintf(parse_result->current_db, sizeof(parse_result->current_db), "%.*s", (int)strnlen(db, sizeof(db)), db);
  if (n >= sizeof(parse_result->current_db)) {
    return TD_SET_ERR(TSDB_CODE_PAR_INTERNAL_ERROR, "");
  }

  code = qParsePure(stmt, sql, length, parse_result);
  if (code) {
    tscError("%s", parse_result->err_msg);
    XE("%s", parse_result->err_msg);
    return code;
  }

  uint8_t is_insert = 0;
  size_t  questions = 0;
  qScanSql(sql, length, &is_insert, &questions);
  api2->scanned        = 1;
  api2->is_insert      = is_insert;
  api2->questions      = questions;

  if (!!api2->is_insert != !!parse_result->meta.is_insert) {
    tscError("internal error: qScanSql does not comply with qParsePure");
    XE("internal error: qScanSql does not comply with qParsePure");
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (is_insert) {
    if (questions != parse_result->placeholderNo) {
      tscError("internal error: qScanSql does not comply with qParsePure");
      XE("internal error: qScanSql does not comply with qParsePure");
      XE("%zd <> %d", questions, parse_result->placeholderNo);
      return TSDB_CODE_TSC_INTERNAL_ERROR;
    }
  }
  if (is_insert) {
    int nr_params = questions;
    if (nr_params > api2->cap_params) {
      size_t cap = (nr_params + 15) / 16 * 16;

      TAOS_FIELD_E *params = (TAOS_FIELD_E*)taosMemoryRealloc(api2->params, cap * sizeof(*params));
      if (!params) {
        return parse_result_set_errmsg(parse_result, TSDB_CODE_OUT_OF_MEMORY, "");
      }
      memset(params, 0, sizeof(*params) * cap);

      api2->params       = params;
      api2->cap_params   = cap;
    }
  }

  parse_result_templatify_sql_e e = parse_result_templatify_sql(parse_result);
  if (e) {
    switch (e) {
      case PARSE_RESULT_TEMPLATIFY_SQL_OOM:
        code = TSDB_CODE_OUT_OF_MEMORY;
        tscError("[0x%08x]%s", code, tstrerror(code));
        XE("[0x%08x]%s", code, tstrerror(code));
        break;
      default:
        code = TSDB_CODE_PAR_INTERNAL_ERROR;
        tscError("[0x%08x]%s", code, tstrerror(code));
        XE("[0x%08x]%s", code, tstrerror(code));
        break;
    }
    return code;
  }
  // XE("==[%.*s]==", (int)parse_result->nr_sql, parse_result->sql);
  pStmt->sql.sqlStr        = parse_result->sql;
  pStmt->sql.sqlLen        = parse_result->nr_sql;
  if (1) {
    if (questions) code = stmtDescribeParams2(stmt);
    if (code == TSDB_CODE_SUCCESS) pStmt->api2.prepared = 1;

    return code;
  }

  {
    // NOTE: freemine, really tricky!!!
    char *sqlStr       = pStmt->sql.sqlStr;
    size_t      sqlLen = pStmt->sql.sqlLen;

    pStmt->sql.sqlStr  = parse_result->sql;
    pStmt->sql.sqlLen  = parse_result->nr_sql;

    code = stmtCreateRequest_for_parse_result(stmt);

    pStmt->sql.sqlStr  = sqlStr;
    pStmt->sql.sqlLen  = sqlLen;

    if (code) {
      tscError("[0x%08x]%s", code, tstrerror(code));
      XE("[0x%08x]%s", code, tstrerror(code));
      return code;
    }
  }

  SAppInstInfo      *pAppInfo       = pObj->pAppInfo;
  void              *pTransporter   = pAppInfo->pTransporter;
  const char        *user           = pObj->user;

  SCatalog          *pCatalog       = pStmt->pCatalog;
  SStmtExecInfo     *execInfo       = &pStmt->exec;
  SRequestObj       *pRequest       = execInfo->pRequest;

  SEpSet mgmtEpSet = getEpSet_s(&pObj->pAppInfo->mgmtEp);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  desc_params_ctx_t ctx = {
    .connInfo      = {
      .pTrans            = pTransporter,
      .requestId         = pRequest->requestId,
      .requestObjRefId   = pRequest->self,
      .mgmtEps           = mgmtEpSet,
    },
    .pCatalog            = pCatalog,
    .current_user        = user,
    .acctId              = pObj->acctId,
    .current_db          = pObj->db,

    .api2                = api2,
  };
  code = parse_result_describe_params(&ctx);
  STMT_ERR_RET(code);
  // STableMeta *pTableMeta = api2->pTableMeta;
  // if (pTableMeta) {
  //   STableComInfo *tableInfo = &pTableMeta->tableInfo;
  //   XE("numOfTags:%d", tableInfo->numOfTags);
  //   XE("precision:%d", tableInfo->precision);
  //   XE("numOfColumns:%d", tableInfo->numOfColumns);
  //   XE("numOfPKs:%d", tableInfo->numOfPKs);
  //   XE("rowSize:%d", tableInfo->rowSize);
  // }

  if (code == TSDB_CODE_SUCCESS) api2->prepared = 1;
  return code;
}

int stmtPostPrepare2(TAOS_STMT *stmt, taos_stmt_prepare2_option_e options) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt* pStmt = (STscStmt*)stmt;

  stmtResetPrepareInfo2(pStmt);
  pStmt->api_type = STMT_API_PREPARE2;
  pStmt->api2.options = options;

  if (options & TAOS_STMT_PREPARE2_OPTION_PURE_PARSE) {
    return stmtPostPrepare2Ext(stmt);
  }

  pStmt->sql.sqlStr = pStmt->sql.originSql;
  pStmt->sql.sqlLen = pStmt->sql.originLen;

  const char *sql    = pStmt->sql.sqlStr;
  int32_t     length = pStmt->sql.sqlLen;

  uint8_t is_insert = 0;
  size_t  questions = 0;
  qScanSql(sql, length, &is_insert, &questions);
  pStmt->api2.scanned        = 1;
  pStmt->api2.is_insert      = is_insert;
  pStmt->api2.questions      = questions;

  if (questions) code = stmtDescribeParams2(stmt);
  if (code == TSDB_CODE_SUCCESS) pStmt->api2.prepared = 1;

  return code;
}

int stmtGetParams2(TAOS_STMT *stmt, TAOS_FIELD_E *params, int nr_params, int *nr_real) {
  STscStmt             *pStmt = (STscStmt*)stmt;
  SStmtAPI2            *api2  = &pStmt->api2;

  if (!api2->prepared) {
    tscError("not prepared");
    fprintf(stderr, "not prepared\n");
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (params) {
    memcpy(params, api2->params, (nr_params > api2->questions ? api2->questions : nr_params) * sizeof(*params));
  }
  *nr_real = pStmt->api2.questions;
  return TSDB_CODE_SUCCESS;
}

int stmtGetParam2(TAOS_STMT* stmt, int idx, int* type, int* bytes) {
  STscStmt             *pStmt = (STscStmt*)stmt;
  SStmtAPI2            *api2  = &pStmt->api2;

  if (!api2->prepared) {
    tscError("not prepared");
    fprintf(stderr, "not prepared\n");
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  *type  = 0;
  *bytes = 0;

  if (idx < 0 || idx >= api2->questions) return TSDB_CODE_SUCCESS; // FIXME: what errcode to return?
  *type  = api2->params[idx].type;
  *bytes = api2->params[idx].bytes;
  return TSDB_CODE_SUCCESS;
}

int stmtBindParams2(TAOS_STMT *stmt, TAOS_MULTI_BIND *mbs, int nr_mbs) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (!pStmt->api2.prepared) {
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  if (mbs == (TAOS_MULTI_BIND*)-1) {
    stmtUnbindParams2(pStmt);
  } else {
    // TODO: won't use until taos_stmt_execute!
    pStmt->api2.mbs_from_app      = mbs;
    pStmt->api2.nr_mbs_from_app   = nr_mbs;
  }

  return TSDB_CODE_SUCCESS;
}

static int stmtExec2Literal(TAOS_STMT *stmt) {
  STscStmt*   pStmt = (STscStmt*)stmt;

  SStmtAPI2  *api2 = &pStmt->api2;
  const char *sql  = pStmt->sql.sqlStr;

  STscObj* pObj = (STscObj*)pStmt->taos;
  TAOS_RES *res = taosQueryImplWithConnId(pObj->id, sql, false, TD_REQ_FROM_APP);
  int e = taos_errno(res);
  api2->res_from_taos_query     = res;
  api2->use_res_from_taos_query = 1;
  if (e) {
    stmtExec2ClearRes(pStmt);
    STMT_ERR_RET(e);
  }

  int64_t rows64 = taos_affected_rows64(res);
  if (rows64 > INT32_MAX) {
    tscWarn("taos_affected_rows64 returns larger than INT32_MAX");
  }
  pStmt->exec.affectedRows = rows64;
  pStmt->affectedRows += rows64;
  if (pStmt->affectedRows < rows64) {
    tscWarn("affectedRows overflow(INT32_MAX)");
  }

  return TSDB_CODE_SUCCESS;
}

static size_t stmtCalcMbOffset(TAOS_MULTI_BIND *dst, TAOS_FIELD_E *fld, int nr_rows, size_t cap_buf) {
  dst->buffer_type   = fld->type;
  dst->buffer_length = fld->bytes;

  dst->buffer        = (void*)(uintptr_t)cap_buf;

  cap_buf += nr_rows * fld->bytes;
  cap_buf  = (cap_buf + 3) / 4 * 4;

  dst->length        = (int32_t*)(uintptr_t)cap_buf;
  cap_buf += nr_rows * 4;

  dst->is_null       = (char*)(uintptr_t)cap_buf;
  cap_buf += nr_rows * 1;
  cap_buf  = (cap_buf + 3) / 4 * 4;

  DBGE("dst/buffer_length:%p/%zd", dst, (size_t)dst->buffer_length);
  return cap_buf;
}

static void sliceTAOS_MULTI_BIND(TAOS_MULTI_BIND *dst, TAOS_MULTI_BIND *src, int row, int rows) {
  *dst = *src;
  dst->num        = rows;
  dst->buffer     = (char*)(dst->buffer) + row * dst->buffer_length;
  if (dst->length)  dst->length  += row;
  if (dst->is_null) dst->is_null += row;
}

static void* data_from_TAOS_MULTI_BIND_from_app(TAOS_MULTI_BIND *p, int row, int *len) {
  void *data = NULL;
  if (p->is_null && p->is_null[row]) {
    data  = NULL;
    *len  = 0;
    return data;
  }
  data = p->buffer ? ((char*)p->buffer) + row * p->buffer_length : NULL;
  // FIXME: is this the correct way to get the length of real payload
  switch (p->buffer_type) {
    case TSDB_DATA_TYPE_BOOL:              *len = 1; break;
    case TSDB_DATA_TYPE_TINYINT:           *len = 1; break;
    case TSDB_DATA_TYPE_SMALLINT:          *len = 2; break;
    case TSDB_DATA_TYPE_INT:               *len = 4; break;
    case TSDB_DATA_TYPE_BIGINT:            *len = 8; break;
    case TSDB_DATA_TYPE_FLOAT:             *len = 4; break;
    case TSDB_DATA_TYPE_DOUBLE:            *len = 8; break;
    case TSDB_DATA_TYPE_TIMESTAMP:         *len = 8; break;
    case TSDB_DATA_TYPE_UTINYINT:          *len = 1; break;
    case TSDB_DATA_TYPE_USMALLINT:         *len = 2; break;
    case TSDB_DATA_TYPE_UINT:              *len = 4; break;
    case TSDB_DATA_TYPE_UBIGINT:           *len = 8; break;
    default: {
      if (p->length) {
        *len = p->length[row];
      } else {
        *len = p->buffer_length;
      }
    } break;
  }
  return data;
}

typedef struct param_conv_ctx_s             param_conv_ctx_t;
struct param_conv_ctx_s {
  TAOS_FIELD_E    *fld;
  int              dst_type;
  char            *dst_data;
  int              dst_len;
  int              src_type;
  const char      *src_data;
  int              src_len;
  int32_t         *len;
};

static int stmtConvParamVarcharToTs(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(int64_t) && dst_len) {
    DBGE("internal logic error: dst is not int64_t[%d]", dst_len);
    tscError("internal logic error: dst is not int64_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  int64_t v = taosStr2Int64(buf, NULL, 10);

  *(int64_t*)dst_data = v;
  *len                = sizeof(int64_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToBit(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(int8_t) && dst_len) {
    DBGE("internal logic error: dst is not int8_t[%d]", dst_len);
    tscError("internal logic error: dst is not int8_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  int8_t v = taosStr2Int8(buf, NULL, 10);

  *(int8_t*)dst_data = !!v;
  *len               = sizeof(int8_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToI8(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(int8_t) && dst_len) {
    DBGE("internal logic error: dst is not int8_t[%d]", dst_len);
    tscError("internal logic error: dst is not int8_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  int8_t v = taosStr2Int8(buf, NULL, 10);

  *(int8_t*)dst_data = v;
  *len               = sizeof(int8_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToU8(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(uint8_t) && dst_len) {
    DBGE("internal logic error: dst is not uint8_t[%d]", dst_len);
    tscError("internal logic error: dst is not uint8_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  uint8_t v = taosStr2UInt8(buf, NULL, 10);

  *(uint8_t*)dst_data = v;
  *len                = sizeof(uint8_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToI16(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(int16_t) && dst_len) {
    DBGE("internal logic error: dst is not int16_t[%d]", dst_len);
    tscError("internal logic error: dst is not int16_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  int16_t v = taosStr2Int16(buf, NULL, 10);

  *(int16_t*)dst_data = v;
  *len                = sizeof(int16_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToU16(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(uint16_t) && dst_len) {
    DBGE("internal logic error: dst is not uint16_t[%d]", dst_len);
    tscError("internal logic error: dst is not uint16_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  uint16_t v = taosStr2UInt16(buf, NULL, 10);

  *(uint16_t*)dst_data = v;
  *len                 = sizeof(uint16_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToI32(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(int32_t) && dst_len) {
    DBGE("internal logic error: dst is not int32_t[%d]", dst_len);
    tscError("internal logic error: dst is not int32_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  int32_t v = taosStr2Int32(buf, NULL, 10);

  *(int32_t*)dst_data = v;
  *len                = sizeof(int32_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToU32(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(uint32_t) && dst_len) {
    DBGE("internal logic error: dst is not uint32_t[%d]", dst_len);
    tscError("internal logic error: dst is not uint32_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  uint32_t v = taosStr2UInt32(buf, NULL, 10);

  *(uint32_t*)dst_data = v;
  *len                 = sizeof(uint32_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToI64(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(int64_t) && dst_len) {
    DBGE("internal logic error: dst is not int64_t[%d]", dst_len);
    tscError("internal logic error: dst is not int64_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  int64_t v = taosStr2Int64(buf, NULL, 10);

  *(int64_t*)dst_data = v;
  *len                = sizeof(int64_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToU64(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(uint64_t) && dst_len) {
    DBGE("internal logic error: dst is not uint64_t[%d]", dst_len);
    tscError("internal logic error: dst is not uint64_t[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  uint64_t v = taosStr2UInt64(buf, NULL, 10);

  *(uint64_t*)dst_data = v;
  *len                 = sizeof(uint64_t);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToFlt(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(float) && dst_len) {
    DBGE("internal logic error: dst is not float[%d]", dst_len);
    tscError("internal logic error: dst is not float[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  float v = taosStr2Float(buf, NULL);

  *(float*)dst_data = v;
  *len              = sizeof(float);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarcharToDbl(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  if (dst_len != sizeof(double) && dst_len) {
    DBGE("internal logic error: dst is not double[%d]", dst_len);
    tscError("internal logic error: dst is not double[%d]", dst_len);
    STMT_RET(TSDB_CODE_TSC_INTERNAL_ERROR);
  }

  char buf[4096]; *buf = '\0'; // NOTE: big enough
  snprintf(buf, sizeof(buf), "%.*s", src_len, src_data);
  double v = taosStr2Double(buf, NULL);

  *(double*)dst_data = v;
  *len               = sizeof(double);

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamVarchar(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  STscStmt*   pStmt = (STscStmt*)stmt;

  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  switch (dst_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      STMT_RET(stmtConvParamVarcharToTs(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_BOOL:
      STMT_RET(stmtConvParamVarcharToBit(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      STMT_RET(stmtConvParamVarcharToI8(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      STMT_RET(stmtConvParamVarcharToU8(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      STMT_RET(stmtConvParamVarcharToI16(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      STMT_RET(stmtConvParamVarcharToU16(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_INT:
      STMT_RET(stmtConvParamVarcharToI32(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_UINT:
      STMT_RET(stmtConvParamVarcharToU32(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      STMT_RET(stmtConvParamVarcharToI64(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      STMT_RET(stmtConvParamVarcharToU64(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      STMT_RET(stmtConvParamVarcharToFlt(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      STMT_RET(stmtConvParamVarcharToDbl(stmt, ctx));
      break;
    default:
      fprintf(stderr, "%s[%d]:%s():converting from `%s` to `%s`: not supported yet\n", __FILE__, __LINE__, __func__, taos_data_type(src_type), taos_data_type(dst_type));
      tscError("converting from `%s` to `%s`: not supported yet", taos_data_type(src_type), taos_data_type(dst_type));
      STMT_RET(TSDB_CODE_PAR_NOT_SUPPORT);
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParamI64(TAOS_STMT *stmt, int64_t v, param_conv_ctx_t *ctx) {
  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  switch (dst_type) {
    case TSDB_DATA_TYPE_TIMESTAMP: {
    } break;
    case TSDB_DATA_TYPE_BOOL: {
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
    } break;
    case TSDB_DATA_TYPE_INT: {
      *(int32_t*)dst_data = v;
      return 0;
    } break;
    case TSDB_DATA_TYPE_UINT: {
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
    } break;
    default:
      break;
  }

  return TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "converting from %" PRId64 " to `0x[%08x]%s`",
      v, dst_type, taos_data_type(dst_type));
}

static int stmtConvParamEpochNano(TAOS_STMT *stmt, int64_t epoch_nano, param_conv_ctx_t *ctx) {
  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  switch (dst_type) {
    case TSDB_DATA_TYPE_TIMESTAMP: {
      switch (ctx->fld->precision) {
        case 0:
          *(int64_t*)dst_data = epoch_nano / 1000000;
          break;
        case 1:
          *(int64_t*)dst_data = epoch_nano / 1000;
          break;
        default:
          *(int64_t*)dst_data = epoch_nano;
          break;
      }
      return 0;
    } break;
    case TSDB_DATA_TYPE_BOOL: {
    } break;
    case TSDB_DATA_TYPE_TINYINT: {
    } break;
    case TSDB_DATA_TYPE_UTINYINT: {
    } break;
    case TSDB_DATA_TYPE_SMALLINT: {
    } break;
    case TSDB_DATA_TYPE_USMALLINT: {
    } break;
    case TSDB_DATA_TYPE_INT: {
    } break;
    case TSDB_DATA_TYPE_UINT: {
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
    } break;
    case TSDB_DATA_TYPE_UBIGINT: {
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
    } break;
    default:
      break;
  }

  return TD_SET_ERR(TSDB_CODE_PAR_NOT_SUPPORT, "converting from epoch_nano %" PRId64 " to `0x[%08x]%s`",
      epoch_nano, dst_type, taos_data_type(dst_type));
}

static int stmtConvParam(TAOS_STMT *stmt, param_conv_ctx_t *ctx) {
  STscStmt*   pStmt = (STscStmt*)stmt;
  int              dst_type     = ctx->dst_type;
  char            *dst_data     = ctx->dst_data;
  int              dst_len      = ctx->dst_len;
  int              src_type     = ctx->src_type;
  const char      *src_data     = ctx->src_data;
  int              src_len      = ctx->src_len;
  int32_t         *len          = ctx->len;

  switch (src_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      DBGE("varchar:[%.*s]", src_len, src_data);
      STMT_RET(stmtConvParamVarchar(stmt, ctx));
      break;
    case TSDB_DATA_TYPE_INT: {
      int64_t v = *(int32_t*)src_data;
      DBGE("int:[%" PRId64 "]", v);
      STMT_RET(stmtConvParamI64(stmt, v, ctx));
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t epoch_nano = *(int64_t*)src_data;
      DBGE("epoch_nan:[%" PRId64 "]", epoch_nano);
      STMT_RET(stmtConvParamEpochNano(stmt, epoch_nano, ctx));
    } break;
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)src_data;
      DBGE("int64_t:[%" PRId64 "]", v);
      STMT_RET(stmtConvParamI64(stmt, v, ctx));
    } break;
    default:
      fprintf(stderr, "%s[%d]:%s():converting from `%s` to `%s`: not supported yet\n", __FILE__, __LINE__, __func__, taos_data_type(src_type), taos_data_type(dst_type));
      tscError("converting from `%s` to `%s`: not supported yet", taos_data_type(src_type), taos_data_type(dst_type));
      STMT_RET(TSDB_CODE_PAR_NOT_SUPPORT);
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static void dump_TAOS_MULTI_BINDS(TAOS_MULTI_BIND *mbs, size_t nr_mbs, size_t rows) {
  for (size_t i=0; i<nr_mbs; ++i) {
    TAOS_MULTI_BIND *p = mbs + i;
    fprintf(stderr, "[%d]%s\n", p->buffer_type, taosDataTypeName(p->buffer_type));
    for (size_t j=0; j<rows; ++j) {
      if (p->is_null && p->is_null[j]) {
        fprintf(stderr, "x-null, %p/%p/%zd", p, p->is_null, j);
      } else {
        switch (p->buffer_type) {
          case TSDB_DATA_TYPE_NULL: {
            fprintf(stderr, "y-null");
          } break;
          case TSDB_DATA_TYPE_BOOL: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_TINYINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_INT: {
            fprintf(stderr, "%d", *(int32_t*)p->buffer);
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_FLOAT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_DOUBLE: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_VARCHAR: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_TIMESTAMP: {
            fprintf(stderr, "ts%" PRId64 "", *(int64_t*)p->buffer);
          } break;
          case TSDB_DATA_TYPE_NCHAR: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_UTINYINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_USMALLINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_UINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_UBIGINT: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_JSON: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_VARBINARY: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_DECIMAL: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_BLOB: {
            XA(0, "");
          } break;
          case TSDB_DATA_TYPE_MEDIUMBLOB: {
            XA(0, "");
          } break;
          // case TSDB_DATA_TYPE_BINARY: {
          // } break;
          case TSDB_DATA_TYPE_GEOMETRY: {
            XA(0, "");
          } break;
          default: {
            XA(0, "");
          } break;
        }
      }
      fprintf(stderr, ";");
    }
    fprintf(stderr, "\n");
  }
}

static int stmtExec2ParamsBind(TAOS_STMT *stmt, int tbname, TAOS_FIELD_E *tags, int nr_tags, TAOS_FIELD_E *cols, int nr_cols, int row, int nr_rows) {
  int code = TSDB_CODE_SUCCESS;
  STscStmt*   pStmt = (STscStmt*)stmt;
  SStmtAPI2  *api2  = &pStmt->api2;

  STMT_ERR_RET(stmtKeepMbs(stmt, nr_tags + nr_cols));

  TAOS_MULTI_BIND * const mbs     = api2->mbs_from_app;
  TAOS_MULTI_BIND * const cache   = api2->mbs_cache;
  size_t                  cap_buf = 0;

  for (int i = 0; i<nr_tags; ++i) {
    TAOS_MULTI_BIND *dst  = cache + i;
    TAOS_MULTI_BIND *src  = mbs + !!tbname + i;
    TAOS_FIELD_E    *tag  = tags + i;
    sliceTAOS_MULTI_BIND(dst, src, row, nr_rows);
    if (src->buffer_type != tag->type) {
      cap_buf = stmtCalcMbOffset(dst, tag, nr_rows, cap_buf);
    }
  }

  for (int i = 0; i<nr_cols; ++i) {
    TAOS_MULTI_BIND *dst  = cache + nr_tags + i;
    TAOS_MULTI_BIND *src  = mbs + !!tbname + nr_tags + i;
    TAOS_FIELD_E    *col  = cols + i;
    sliceTAOS_MULTI_BIND(dst, src, row, nr_rows);
    if (src->buffer_type != col->type) {
      cap_buf = stmtCalcMbOffset(dst, col, nr_rows, cap_buf);
    }
  }

  if (cap_buf > api2->cap_conv_buf) {
    char *p = (char*)taosMemoryRealloc(api2->conv_buf, cap_buf);
    if (!p) {
      STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    api2->conv_buf     = p;
    api2->cap_conv_buf = cap_buf;
  }

  if (cap_buf) {
    char *conv_buf = api2->conv_buf;
    for (int i = 0; i<nr_tags; ++i) {
      TAOS_MULTI_BIND *dst  = cache + i;
      TAOS_MULTI_BIND *src  = mbs + !!tbname + i;
      TAOS_FIELD_E    *tag  = tags + i;
      if (src->buffer_type != tag->type) {
        dst->buffer        = conv_buf + (size_t)(uintptr_t)dst->buffer;
        dst->length        = (int32_t*)(conv_buf + (size_t)(uintptr_t)dst->length);
        dst->is_null       = conv_buf + (size_t)(uintptr_t)dst->is_null;
        for (int j=0; j<nr_rows; ++j) {
          if (src->is_null && src->is_null[row + j]) {
            dst->is_null[j] = 1;
            continue;
          }
          dst->is_null[j] = 0;
          int32_t  src_len  = 0;
          char    *src_data = data_from_TAOS_MULTI_BIND_from_app(src, row + j, &src_len);
          // char    *src_data = (char*)src->buffer;
          // src_data += src->buffer_length * (row + j);
          // int32_t  src_len  = src->length[row + j];
          char    *dst_data = (char*)dst->buffer;
          dst_data += dst->buffer_length * j;
          int32_t  dst_len  = dst->buffer_length;
          param_conv_ctx_t ctx = {
            .dst_type       = dst->buffer_type,
            .dst_data       = dst_data,
            .dst_len        = dst_len,
            .src_type       = src->buffer_type,
            .src_data       = src_data,
            .src_len        = src_len,
            .len            = dst->length + j,
          };
          STMT_ERR_RET(stmtConvParam(stmt, &ctx));
        }
      }
    }
    for (int i = 0; i<nr_cols; ++i) {
      TAOS_MULTI_BIND *dst  = cache + i;
      TAOS_MULTI_BIND *src  = mbs + !!tbname + i;
      TAOS_FIELD_E    *col  = cols + i;
      if (src->buffer_type != col->type) {
        dst->buffer        = conv_buf + (size_t)(uintptr_t)dst->buffer;
        dst->length        = (int32_t*)(conv_buf + (size_t)(uintptr_t)dst->length);
        dst->is_null       = conv_buf + (size_t)(uintptr_t)dst->is_null;
        for (int j=0; j<nr_rows; ++j) {
          if (src->is_null && src->is_null[row + j]) {
            dst->is_null[j] = 1;
            continue;
          }
          dst->is_null[j] = 0;
          int32_t  src_len  = 0;
          char    *src_data = data_from_TAOS_MULTI_BIND_from_app(src, row + j, &src_len);
          // char    *src_data = (char*)src->buffer;
          // src_data += src->buffer_length * (row + j);
          // int32_t  src_len  = src->length[row + j];
          char    *dst_data = (char*)dst->buffer;
          dst_data += dst->buffer_length * j;
          int32_t  dst_len  = dst->buffer_length;
          param_conv_ctx_t ctx = {
            .dst_type       = dst->buffer_type,
            .dst_data       = dst_data,
            .dst_len        = dst_len,
            .src_type       = src->buffer_type,
            .src_data       = src_data,
            .src_len        = src_len,
            .len            = dst->length + j,
          };
          STMT_ERR_RET(stmtConvParam(stmt, &ctx));
        }
      }
    }
  }

  if (nr_tags) STMT_ERR_RET(stmt_set_tags(stmt, cache));
  if (nr_cols) STMT_ERR_RET(stmtBindBatch(stmt, cache + nr_tags , -1));
  STMT_ERR_RET(stmtAddBatch(stmt));

  return TSDB_CODE_SUCCESS;
}

static int stmtExec2NormalInsert(TAOS_STMT *stmt) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;
  SStmtAPI2  *api2  = &pStmt->api2;

  int tbname  = !!api2->tbname_required;
  int nr_tags = api2->nr_tags;
  int nr_cols = api2->nr_cols;

  TAOS_FIELD_E *tags = api2->params + !!tbname;
  TAOS_FIELD_E *cols = tags + nr_tags;

  STMT_ERR_RET(stmtExec2ParamsBind(stmt, tbname, tags, nr_tags, cols, nr_cols, 0, api2->mbs_from_app->num));
  STMT_ERR_RET(stmtExec1(stmt));
  return TSDB_CODE_SUCCESS;
}

static int stmtRefetch(TAOS_STMT *stmt, TAOS_FIELD_E **tags, int *nr_tags, TAOS_FIELD_E **cols, int *nr_cols) {
  int r = 0;
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  SStmtAPI2 *api2 = &pStmt->api2;

  switch (code) {
    case TSDB_CODE_SUCCESS:
      if (!api2->without_using_clause) {
        STMT_ERR_RET(stmt_get_tag_fields(stmt, nr_tags, tags));
      }
      STMT_ERR_RET(stmt_get_col_fields(stmt, nr_cols, cols));
      break;
    default:
      STMT_ERR_RET(TSDB_CODE_TSC_STMT_API_ERROR);
      break;
  }

  if (!api2->without_using_clause) {
    // NOTE: one last time check
    if (*nr_tags != api2->nr_tags || *nr_cols != api2->nr_cols) {
      STMT_ERR_RET(TSDB_CODE_INVALID_PARA);
    }
  } else {
    // NOTE: one last time check
    if (*nr_tags || *nr_cols + 1 != api2->questions) {
      STMT_ERR_RET(TSDB_CODE_INVALID_PARA);
    }
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct run_env_s    run_env_t;
struct run_env_s {
  TAOS_STMT         *stmt;
  eval_env_t        *env;
  TAOS_FIELD_E      *tags;
  TAOS_FIELD_E      *cols;
  int                nr_tags;
  int                nr_cols;

  size_t             cap_buf;
};

typedef struct run_s        run_t;
struct run_s {
  run_env_t         *env;
  char               tbname[4096];
  size_t             len;
  int                row;
};

static void run_env_release(run_env_t *run_env) {
  if (!run_env) return;
  taosMemoryFreeClear(run_env->tags);
  taosMemoryFreeClear(run_env->cols);
}

int32_t on_tag_val(const value_t *val, int start, int row, int col, void *arg, int *abortion) {
  int32_t code = TSDB_CODE_SUCCESS;

  run_env_t        *run_env    = (run_env_t*)arg;
  TAOS_STMT        *stmt       = run_env->stmt;
  STscStmt         *pStmt      = (STscStmt*)stmt;
  SStmtAPI2        *api2       = &pStmt->api2;
  TAOS_MULTI_BIND  *dst        = api2->mbs_tags.mbs + col;

  const tsdb_value_t *tsdb = value_tsdb_value(val);

  int32_t     src_len  = 0;
  const char *src_data = NULL;

  code = tsdb_value_buffer(tsdb, &src_data, &src_len);
  if (code) return code;

  if (!src_data) {
    dst->is_null[row-start] = 1;
    return 0;
  }
  dst->is_null[row-start] = 0;

  int src_type = tsdb_value_type(tsdb);

  char    *dst_data = (char*)dst->buffer;
  dst_data += dst->buffer_length * (row-start);
  int32_t  dst_len  = dst->buffer_length;

  param_conv_ctx_t ctx = {
    .fld            = run_env->tags + col,
    .dst_type       = dst->buffer_type,
    .dst_data       = dst_data,
    .dst_len        = dst_len,
    .src_type       = src_type,
    .src_data       = src_data,
    .src_len        = src_len,
    .len            = dst->length + (row-start),
  };
  return stmtConvParam(stmt, &ctx);
}

int32_t on_col_val(const value_t *val, int start, int row, int col, void *arg, int *abortion) {
  int32_t code = TSDB_CODE_SUCCESS;

  run_env_t        *run_env    = (run_env_t*)arg;
  TAOS_STMT        *stmt       = run_env->stmt;
  STscStmt         *pStmt      = (STscStmt*)stmt;
  SStmtAPI2        *api2       = &pStmt->api2;
  TAOS_MULTI_BIND  *dst        = api2->mbs_cols.mbs + col;

  const tsdb_value_t *tsdb = value_tsdb_value(val);

  int32_t     src_len  = 0;
  const char *src_data = NULL;

  code = tsdb_value_buffer(tsdb, &src_data, &src_len);
  if (code) return code;

  if (!src_data) {
    dst->is_null[row-start] = 1;
    return 0;
  }
  dst->is_null[row-start] = 0;

  int src_type = tsdb_value_type(tsdb);

  char    *dst_data = (char*)dst->buffer;
  dst_data += dst->buffer_length * (row-start);
  int32_t  dst_len  = dst->buffer_length;

  param_conv_ctx_t ctx = {
    .fld            = run_env->tags + col,
    .dst_type       = dst->buffer_type,
    .dst_data       = dst_data,
    .dst_len        = dst_len,
    .src_type       = src_type,
    .src_data       = src_data,
    .src_len        = src_len,
    .len            = dst->length + (row-start),
  };
  return stmtConvParam(stmt, &ctx);
}

int32_t build_cache(run_env_t *run_env, mbs_cache_t *mbs_cache, TAOS_FIELD_E *flds, size_t nr_flds, int rows) {
  int32_t code = TSDB_CODE_SUCCESS;

  TAOS_STMT  *stmt       = run_env->stmt;
  STscStmt   *pStmt      = (STscStmt*)stmt;
  SStmtAPI2  *api2       = &pStmt->api2;

  code = mbs_cache_keep(mbs_cache, nr_flds);
  if (code) return code;

  TAOS_MULTI_BIND *mbs = mbs_cache->mbs;

  size_t cap_buf = 0;
  for (int i=0; i<nr_flds; ++i) {
    TAOS_FIELD_E *p = flds + i;
    TAOS_MULTI_BIND *mb = mbs + i;
    cap_buf = stmtCalcMbOffset(mb, p, rows, cap_buf);
  }
  code = mbs_cache_keep_buf(mbs_cache, cap_buf);
  if (code) return code;

  char *conv_buf = mbs_cache->buf;
  for (int i = 0; i<nr_flds; ++i) {
    TAOS_MULTI_BIND *dst  = mbs + i;
    TAOS_FIELD_E    *tag  = flds + i;
    dst->buffer        = mbs_cache->buf + (size_t)(uintptr_t)dst->buffer;
    dst->length        = (int32_t*)(mbs_cache->buf + (size_t)(uintptr_t)dst->length);
    dst->is_null       = mbs_cache->buf + (size_t)(uintptr_t)dst->is_null;
    dst->num           = rows;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t process_batch(parse_result_t *parse_result, run_t *run, int end) {
  run_env_t  *run_env    = run->env;
  TAOS_STMT  *stmt       = run_env->stmt;
  STscStmt   *pStmt      = (STscStmt*)stmt;
  SStmtAPI2  *api2       = &pStmt->api2;

  pStmt->errCode = 0; // NOTE: clear previous errcode to let stmt_set_tbname continue

  int32_t code = stmt_set_tbname(stmt, run->tbname);

  if (!code && (run->row == 0 || api2->without_using_clause)) {
    taosMemoryFreeClear(run_env->tags);
    taosMemoryFreeClear(run_env->cols);
    run_env->nr_tags = 0;
    run_env->nr_cols = 0;
    code = stmtRefetch(stmt, &run_env->tags, &run_env->nr_tags, &run_env->cols, &run_env->nr_cols);
  }

  if (!code) {
    code = build_cache(run_env, &api2->mbs_tags, run_env->tags, run_env->nr_tags, 1);
  }

  if (!code && run_env->nr_tags) {
    parse_result_visit_ctx_t ctx = {
      .env               = run_env->env,
      .on_tag_val        = on_tag_val,
      .arg               = run_env,
    };
    code = parse_result_visit(parse_result, run->row, 1, &ctx);
    if (!code) {
      code = stmt_set_tags(stmt, api2->mbs_tags.mbs);
    }
  }

  if (!code) {
    code = build_cache(run_env, &api2->mbs_cols, run_env->cols, run_env->nr_cols, end - run->row);
  }

  if (!code && run_env->nr_cols) {
    parse_result_visit_ctx_t ctx = {
      .env               = run_env->env,
      .on_col_val        = on_col_val,
      .arg               = run_env,
    };
    code = parse_result_visit(parse_result, run->row, end - run->row, &ctx);
    if (!code) {
      code = stmtBindBatch(stmt, api2->mbs_cols.mbs, -1);
    }
    if (!code) {
      code = stmtAddBatch(stmt);
    }
  }

  if (code) return TD_SET_ERR(code, "==================");

  return 0;
}

static int32_t on_tbname(const char *tbname, size_t len, int row, void *arg, int *abortion) {
  *abortion = 0;
  run_t     *run     = (run_t*)arg;
  run_env_t *run_env = run->env;
  if (run->tbname[0] == '\0') {
    if (!tbname) return 0;
    snprintf(run->tbname, sizeof(run->tbname), "%.*s", (int)len, tbname);
    run->len = strlen(run->tbname);
    run->row = row;
    return 0;
  }
  if (tbname && run->len == len && strncmp(run->tbname, tbname, len) == 0) {
    return 0;
  }
  TAOS_STMT  *stmt  = run_env->stmt;
  STscStmt   *pStmt = (STscStmt*)stmt;
  SStmtAPI2  *api2  = &pStmt->api2;
  parse_result_t *parse_result = &api2->parse_result;
  int32_t code = process_batch(parse_result, run, row);
  if (tbname) {
    snprintf(run->tbname, sizeof(run->tbname), "%.*s", (int)len, tbname);
    run->len = strlen(run->tbname);
    run->row = row;
  }
  if (code) return TD_SET_ERR(code, "==================");
  return 0;
}

static int stmtExec2InsertParams(TAOS_STMT *stmt) {
  int r = 0;
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  SStmtAPI2      *api2         = &pStmt->api2;
  parse_result_t *parse_result = &api2->parse_result;
  SNode          *root         = parse_result->pRootNode;

  char             buf[4096]; *buf = '\0';
  TAOS_MULTI_BIND  tmp           = {0};
  const char      *tbname        = NULL;
  int              tbname_len    = 0;
  int              nr_tags       = api2->nr_tags;
  int              nr_cols       = api2->nr_cols;
  int              rows_in_batch = 0;

  TAOS_MULTI_BIND *mbs  = api2->mbs_from_app;
  TAOS_MULTI_BIND *end  = api2->mbs_from_app + api2->nr_mbs_from_app;

  int        row           = 0;
  const int  nr_total_rows = api2->mbs_from_app->num;
  int        nr_rows       = 0;
  int64_t    affectedRows  = 0;

  int i   = 0;

  TAOS_FIELD_E    *tags    = NULL;
  TAOS_FIELD_E    *cols    = NULL;
  tsdb_value_t    *value   = NULL;

  eval_env_t env = {
    .mbs          = mbs,
    .nr_mbs       = end - mbs,
    .current_db   = parse_result->current_db,
    .epoch_nano   = taosGetTimestampNs(),
  };

  if (1) {
    run_env_t run_env = {
      .stmt    = stmt,
      .env     = &env,
    };
    run_t run = {
      .env     = &run_env,
      .row     = 0,
    };
    parse_result_visit_ctx_t ctx = {
      .env               = &env,
      .on_tbname         = on_tbname,
      .arg               = &run,
    };
    code = parse_result_visit(parse_result, 0, ctx.env->mbs->num, &ctx);

    int64_t    affectedRows  = 0;
    if (!code) {
      mbs_cache_zero(&api2->mbs_tags);
      mbs_cache_zero(&api2->mbs_cols);
    }
    if (!code) code = stmtDoExec1(stmt, &affectedRows);
    if (!code) {
      pStmt->exec.affectedRows  = affectedRows;
      pStmt->affectedRows      += pStmt->exec.affectedRows;
    }

    run_env_release(&run_env);
    if (code) return code;
    return code;
  }
}

int stmtExec2(TAOS_STMT *stmt) {
  int r = 0;
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

  SStmtAPI2 *api2 = &pStmt->api2;

  if (api2->nr_mbs_from_app != api2->questions) {
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_INVALID_PARA);
  }

  if (api2->questions == 0) {
    stmtExec2ClearRes(pStmt);
    return stmtExec2Literal(stmt);
  }

  if (!api2->is_insert) {
    if (api2->mbs_from_app->num > 1) {
      tscError("only one row data allowed for query");
      STMT_ERR_RET(TSDB_CODE_INVALID_PARA);
    }

    STMT_ERR_RET(stmtBindBatch(stmt, api2->mbs_from_app, -1));
    STMT_ERR_RET(stmtAddBatch(stmt));
    return stmtExec1(stmt);
  }

  // if (api2->nr_mbs_from_app != !!api2->tbname_required + api2->nr_tags + api2->nr_cols) {
  //   if (!api2->without_using_clause) {
  //     STMT_ERR_RET_IMUTABLY(TSDB_CODE_INVALID_PARA);
  //   }
  // }

  // parameterized-insert-statement

  if (!api2->tbname_required) {
    stmtExec2ClearRes(pStmt);
    return stmtExec2NormalInsert(stmt);
  }

  if (!api2->mbs_from_app) {
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_INVALID_PARA);
  }

  if (api2->options & TAOS_STMT_PREPARE2_OPTION_PURE_PARSE) {
    STMT_ERR_RET(stmtExec2InsertParams(stmt));
    return TSDB_CODE_SUCCESS;
  }

  char             buf[4096]; *buf = '\0';
  TAOS_MULTI_BIND  tmp           = {0};
  const char      *tbname        = NULL;
  int              tbname_len    = 0;
  int              nr_tags       = api2->nr_tags;
  int              nr_cols       = api2->nr_cols;
  int              rows_in_batch = 0;

  TAOS_MULTI_BIND *mbs  = api2->mbs_from_app;
  TAOS_MULTI_BIND *end  = api2->mbs_from_app + api2->nr_mbs_from_app;

  int        row           = 0;
  const int  nr_total_rows = api2->mbs_from_app->num;
  int        nr_rows       = 0;
  int64_t    affectedRows  = 0;

  int i   = 0;

  TAOS_FIELD_E    *tags    = NULL;
  TAOS_FIELD_E    *cols    = NULL;

again:

  if (row >= nr_total_rows) {
    code = stmtDoExec1(stmt, &affectedRows);
    pStmt->exec.affectedRows  = affectedRows;
    pStmt->affectedRows      += pStmt->exec.affectedRows;
    goto end;
  }
  mbs = api2->mbs_from_app;

  tbname = (const char*)data_from_TAOS_MULTI_BIND_from_app(mbs, row, &tbname_len);
  if (!tbname || tbname_len == 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  snprintf(buf, sizeof(buf), "%.*s", tbname_len, tbname);

  i = row + 1;
  for (; i < nr_total_rows; ++i) {
    int         n = 0;
    const char *s = (const char*)data_from_TAOS_MULTI_BIND_from_app(mbs, i, &n);
    if (n != tbname_len) break;
    if (strncmp(s, tbname, n)) break;
  }

  pStmt->errCode = 0; // NOTE: clear previous errcode to let stmt_set_tbname continue
  code = stmt_set_tbname(stmt, tbname);
  if (code) goto end;

  if (row == 0 || api2->without_using_clause) {
    taosMemoryFreeClear(tags);
    taosMemoryFreeClear(cols);
    code = stmtRefetch(stmt, &tags, &nr_tags, &cols, &nr_cols);
    if (code) goto end;
  }
  code = stmtExec2ParamsBind(stmt, 1, tags, nr_tags, cols, nr_cols, row, i-row);
  if (code) goto end;

  row = i;

  goto again;

end:
  taosMemoryFreeClear(tags);
  taosMemoryFreeClear(cols);

  STMT_ERR_RET(code);
  return TSDB_CODE_SUCCESS;
}

