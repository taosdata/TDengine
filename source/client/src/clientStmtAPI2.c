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

void stmtExec2ClearRes(STscStmt *pStmt) {
  SStmtAPI2 *api2 = &pStmt->api2;

  if (api2->res_from_taos_query) {
    taos_free_result(api2->res_from_taos_query);
    api2->res_from_taos_query = NULL;
    api2->use_res_from_taos_query = 0;
  }
}

static void stmtResetPrepareInfo2(STscStmt *pStmt) {
  stmtExec2ClearRes(pStmt);
  pStmt->api2.questions                  = 0;
  pStmt->api2.scanned                    = 0;
  pStmt->api2.is_insert                  = 0;
  pStmt->api2.tbname_required            = 0;
  pStmt->api2.without_using_clause       = 0;
  pStmt->api2.nr_tags                    = 0;
  pStmt->api2.nr_cols                    = 0;
  pStmt->api2.prepared                   = 0;
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
  taosMemoryFreeClear(pStmt->api2.params);
  pStmt->api2.cap_params = 0;
  stmtReleaseMbs(pStmt);
}

void stmtUnbindParams2(STscStmt *pStmt) {
  pStmt->api2.mbs_from_app      = NULL;
  pStmt->api2.nr_mbs_from_app   = 0;
}

int stmtReallocParams(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (pStmt->api2.questions > pStmt->api2.cap_params) {
    TAOS_FIELD_E *p = (TAOS_FIELD_E*)taosMemoryRealloc(pStmt->api2.params, sizeof(*p) * pStmt->api2.questions);
    if (!p) STMT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);

    pStmt->api2.params           = p;
    pStmt->api2.cap_params       = pStmt->api2.questions;
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
  STMT_ERR_RET(stmtReallocParams(stmt));
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

  if (nr_params != pStmt->api2.questions) {
    // FIXME: there might be logic error in `qScanSql`
    //        fake TSDB_CODE_TSC_SQL_SYNTAX_ERROR for the moment
    STMT_ERR_RET(TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
  }

  STMT_ERR_RET(stmtReallocParams(stmt));

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

  STscStmt* pStmt = (STscStmt*)stmt;

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

int stmtPostPrepare2(TAOS_STMT* stmt) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt* pStmt = (STscStmt*)stmt;

  stmtResetPrepareInfo2(pStmt);
  pStmt->api_type = STMT_API_PREPARE2;

  pStmt->sql.sqlLen = strnlen(pStmt->sql.sqlStr, pStmt->sql.sqlLen);

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

typedef void (*stmt_visit_param2_f)(TAOS_STMT *stmt, TAOS_FIELD_E *param, int idx, int *go_on, void *arg);

static int stmtVisitParams2(TAOS_STMT *stmt, stmt_visit_param2_f visit, void *arg) {
  STscStmt* pStmt = (STscStmt*)stmt;

  TAOS_FIELD_E *p   = pStmt->api2.params;
  TAOS_FIELD_E *end = pStmt->api2.params + pStmt->api2.questions;

  int go_on = 1;
  int idx = 0;

  int i = 0;

  if (pStmt->api2.is_insert) {
    if (pStmt->api2.tbname_required) {
      visit(stmt, p++, idx++, &go_on, arg);
      if (!go_on) goto end;
      ++i;
    }

    if (!pStmt->api2.without_using_clause) {
      for (int i=0; i<pStmt->api2.nr_tags; ++i) {
        if (p >= end) break;
        visit(stmt, p++, idx++, &go_on, arg);
        if (!go_on) goto end;
      }

      for (int i=0; i<pStmt->api2.nr_cols; ++i) {
        if (p >= end) break;
        visit(stmt, p++, idx++, &go_on, arg);
        if (!go_on) goto end;
      }
      goto end;
    }
    // fall-through
  }

  for (; i<pStmt->api2.questions; ++i) {
    if (p >= end) break;
    visit(stmt, p++, idx++, &go_on, arg);
    if (!go_on) goto end;
  }

end:

  return TSDB_CODE_SUCCESS;
}

typedef struct stmtGetParams2_s stmtGetParams2_t;
struct stmtGetParams2_s {
  TAOS_FIELD_E *params;
  int           nr_params;
};

static void stmtGetParams2_visit(TAOS_STMT *stmt, TAOS_FIELD_E *param, int idx, int *go_on, void *arg) {
  stmtGetParams2_t *arg0 = (stmtGetParams2_t*)arg;
  if (arg0->params && idx < arg0->nr_params) {
    arg0->params[idx] = *param;
  }
  *go_on = (idx + 1 < arg0->nr_params);
}

int stmtGetParams2(TAOS_STMT *stmt, TAOS_FIELD_E *params, int nr_params, int *nr_real) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (!pStmt->api2.prepared) {
    tscError("not prepared");
    fprintf(stderr, "not prepared\n");
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_TSC_STMT_API_ERROR);
  }

  stmtGetParams2_t arg = {0};
  arg.params        = params;
  arg.nr_params     = nr_params;

  stmtVisitParams2(stmt, stmtGetParams2_visit, &arg);
  *nr_real = pStmt->api2.questions;
  return TSDB_CODE_SUCCESS;

  TAOS_FIELD_E *p = pStmt->api2.params;
  if (params) {
    TAOS_FIELD_E *end = params + nr_params;

    if (pStmt->api2.is_insert) {
      if (params < end) {
        if (pStmt->api2.tbname_required) {
          *params++ = *p++;
        }
      }

      for (int i=0; i<pStmt->api2.nr_tags; ++i) {
        if (params >= end) break;
        *params++ = *p++;
      }

      for (int i=0; i<pStmt->api2.nr_cols; ++i) {
        if (params >= end) break;
        *params++ = *p++;
      }
    } else {
      for (int i=0; i<pStmt->api2.questions; ++i) {
        if (params >= end) break;
        *params++ = *p++;
      }
    }
  }

  *nr_real = pStmt->api2.questions;

  return TSDB_CODE_SUCCESS;
}

typedef struct stmtGetParam2_s stmtGetParam2_t;
struct stmtGetParam2_s {
  int  idx;
  int *type;
  int *bytes;
};

static void stmtGetParam2_visit(TAOS_STMT *stmt, TAOS_FIELD_E *param, int idx, int *go_on, void *arg) {
  stmtGetParam2_t *arg0 = (stmtGetParam2_t*)arg;
  if (idx == arg0->idx) {
    if (arg0->type)  *arg0->type         = param->type;
    if (arg0->bytes) *arg0->bytes        = param->bytes;
  }
  *go_on = 1;
}

int stmtGetParam2(TAOS_STMT* stmt, int idx, int* type, int* bytes) {
  STscStmt* pStmt = (STscStmt*)stmt;

  stmtGetParam2_t arg = {0};
  arg.idx      = idx;
  arg.type     = type;
  arg.bytes    = bytes;

  stmtVisitParams2(stmt, stmtGetParam2_visit, &arg);
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

  size_t bytes = nr_rows * fld->bytes;
  bytes  = (bytes + 4 - 1) / 4 * 4; // NOTE: alignment, sizeof(*dst->length) == sizeof(int32_t) == 4
  dst->length        = (int32_t*)(uintptr_t)(cap_buf + bytes);

  bytes += nr_rows * 4; // NOTE: sizeof(*dst->length) == sizeof(int32_t) == 4
  dst->is_null       = (char*)(uintptr_t)(cap_buf + bytes);
  bytes += nr_rows * 1; // NOTE: sizeof(*src->is_null) == sizeof(char) == 1

  bytes  = (bytes + 4 - 1) / 4 * 4; // NOTE: alignment, int32_t
  cap_buf   += bytes;

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

static int stmtConvParamVarcharToTs(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToBit(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToI8(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToU8(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToI16(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToU16(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToI32(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToU32(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToI64(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToU64(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToFlt(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarcharToDbl(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;

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

static int stmtConvParamVarchar(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  STscStmt*   pStmt = (STscStmt*)stmt;

  switch (dst_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      STMT_RET(stmtConvParamVarcharToTs(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_BOOL:
      STMT_RET(stmtConvParamVarcharToBit(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      STMT_RET(stmtConvParamVarcharToI8(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      STMT_RET(stmtConvParamVarcharToU8(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      STMT_RET(stmtConvParamVarcharToI16(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      STMT_RET(stmtConvParamVarcharToU16(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_INT:
      STMT_RET(stmtConvParamVarcharToI32(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_UINT:
      STMT_RET(stmtConvParamVarcharToU32(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      STMT_RET(stmtConvParamVarcharToI64(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      STMT_RET(stmtConvParamVarcharToU64(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      STMT_RET(stmtConvParamVarcharToFlt(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      STMT_RET(stmtConvParamVarcharToDbl(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    default:
      fprintf(stderr, "%s[%d]:%s():converting from `%s` to `%s`: not supported yet\n", __FILE__, __LINE__, __func__, taos_data_type(src_type), taos_data_type(dst_type));
      tscError("converting from `%s` to `%s`: not supported yet", taos_data_type(src_type), taos_data_type(dst_type));
      STMT_RET(TSDB_CODE_PAR_NOT_SUPPORT);
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static int stmtConvParam(TAOS_STMT *stmt, int dst_type, char *dst_data, int dst_len, int src_type, const char *src_data, int src_len, int32_t *len) {
  STscStmt*   pStmt = (STscStmt*)stmt;
  switch (src_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      DBGE("varchar:[%.*s]", src_len, src_data);
      STMT_RET(stmtConvParamVarchar(stmt, dst_type, dst_data, dst_len, src_type, src_data, src_len, len));
      break;
    default:
      fprintf(stderr, "%s[%d]:%s():converting from `%s` to `%s`: not supported yet\n", __FILE__, __LINE__, __func__, taos_data_type(src_type), taos_data_type(dst_type));
      tscError("converting from `%s` to `%s`: not supported yet", taos_data_type(src_type), taos_data_type(dst_type));
      STMT_RET(TSDB_CODE_PAR_NOT_SUPPORT);
      break;
  }

  return TSDB_CODE_SUCCESS;
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
          STMT_ERR_RET(stmtConvParam(stmt, dst->buffer_type, dst_data, dst_len, src->buffer_type, src_data, src_len, dst->length+j));
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
          STMT_ERR_RET(stmtConvParam(stmt, dst->buffer_type, dst_data, dst_len, src->buffer_type, src_data, src_len, dst->length+j));
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

static int stmtExec2ParamInsert(TAOS_STMT *stmt, const char *tbname, const int row, const int nr_rows, int64_t *affectedRows) {
  int code = TSDB_CODE_SUCCESS;

  STscStmt*   pStmt = (STscStmt*)stmt;
  SStmtAPI2  *api2  = &pStmt->api2;

  int              nr_tags       = api2->nr_tags;
  int              nr_cols       = api2->nr_cols;

  pStmt->errCode = 0; // NOTE: clear previous errcode to let stmt_set_tbname continue
  STMT_ERR_RET(stmt_set_tbname(stmt, tbname));

  TAOS_FIELD_E    *tags    = NULL;
  TAOS_FIELD_E    *cols    = NULL;

  switch (code) {
    case TSDB_CODE_SUCCESS:
      if (!api2->without_using_clause) {
        code = stmt_get_tag_fields(stmt, &nr_tags, &tags);
        if (code) goto end;
      }
      code = stmt_get_col_fields(stmt, &nr_cols, &cols);
      if (code) goto end;
      break;
    default:
      goto end;
  }

  if (!api2->without_using_clause) {
    // NOTE: one last time check
    if (nr_tags != api2->nr_tags || nr_cols != api2->nr_cols) {
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }
  } else {
    // NOTE: one last time check
    if (nr_tags || nr_cols + 1 != api2->questions) {
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }
  }

  code = stmtExec2ParamsBind(stmt, 1, tags, nr_tags, cols, nr_cols, row, nr_rows);
  if (code) goto end;
  code = stmtDoExec1(stmt, affectedRows);

end:

  taosMemoryFreeClear(tags);
  taosMemoryFreeClear(cols);

  STMT_ERR_RET(code);
  return code;
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

  if (api2->nr_mbs_from_app != !!api2->tbname_required + api2->nr_tags + api2->nr_cols) {
    if (!api2->without_using_clause) {
      STMT_ERR_RET_IMUTABLY(TSDB_CODE_INVALID_PARA);
    }
  }

  // parameterized-insert-statement

  if (!api2->tbname_required) {
    stmtExec2ClearRes(pStmt);
    return stmtExec2NormalInsert(stmt);
  }

  if (!api2->mbs_from_app) {
    STMT_ERR_RET_IMUTABLY(TSDB_CODE_INVALID_PARA);
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

  int64_t    accumulatedAffectedRows = 0;
  int64_t    affectedRows            = 0;

  int i   = 0;

again:

  if (row >= nr_total_rows) {
    pStmt->exec.affectedRows  = accumulatedAffectedRows;
    pStmt->affectedRows      += pStmt->exec.affectedRows;
    return TSDB_CODE_SUCCESS;
  }
  mbs = api2->mbs_from_app;

  tbname = (const char*)data_from_TAOS_MULTI_BIND_from_app(mbs, row, &tbname_len);
  if (!tbname || tbname_len == 0) {
    STMT_ERR_RET(TSDB_CODE_INVALID_PARA);
    return TSDB_CODE_INVALID_PARA;
  }
  snprintf(buf, sizeof(buf), "%.*s", tbname_len, tbname);

  i = row + 1;
  for (; i < nr_total_rows; ++i) {
    int         n = 0;
    const char *s = (const char*)data_from_TAOS_MULTI_BIND_from_app(mbs, i, &n);
    if (n != tbname_len) break;
    if (strncmp(s, tbname, n)) break;
  }

  affectedRows = 0;
  STMT_ERR_RET(stmtExec2ParamInsert(stmt, tbname, row, i-row, &affectedRows));
  accumulatedAffectedRows += affectedRows;
  row = i;

  if (0 && row < nr_total_rows) {
    // TODO: not implemented yet
    STMT_ERR_RET(TSDB_CODE_PAR_NOT_SUPPORT);
    return TSDB_CODE_PAR_NOT_SUPPORT;
  }

  goto again;
}

