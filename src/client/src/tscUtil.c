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

#include "tscUtil.h"
#include "hash.h"
#include "os.h"
#include "taosmsg.h"
#include "texpr.h"
#include "tkey.h"
#include "tmd5.h"
#include "tscGlobalmerge.h"
#include "tscLog.h"
#include "tscProfile.h"
#include "tscSubquery.h"
#include "tsched.h"
#include "qTableMeta.h"
#include "tsclient.h"
#include "ttimer.h"
#include "ttokendef.h"
#include "cJSON.h"
#include "tscDelete.h"

#ifdef HTTP_EMBEDDED
#include "httpInt.h"
#endif

static void freeQueryInfoImpl(SQueryInfo* pQueryInfo);

int32_t converToStr(char *str, int type, void *buf, int32_t bufSize, int32_t *len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = sprintf(str, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = sprintf(str, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = sprintf(str, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = sprintf(str, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = sprintf(str, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = sprintf(str, "%" PRId64, *(int64_t*)buf);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      n = sprintf(str, "%d", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = sprintf(str, "%d", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = sprintf(str, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = sprintf(str, "%" PRIu64, *(uint64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = sprintf(str, "%.*e", DECIMAL_DIG, GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = sprintf(str, "%.*e", DECIMAL_DIG, GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
      if (bufSize < 0) {
        tscError("invalid buf size");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      int32_t escapeSize = 0;
      *str++ = '\'';
      ++escapeSize;
      char* data = buf;
      for (int32_t i = 0; i < bufSize; ++i) {
        if (data[i] == '\'' || data[i] == '"') {
          *str++ = '\\';
          ++escapeSize;
        }
        *str++ = data[i];
      }
      *str = '\'';
      ++escapeSize;
      n = bufSize + escapeSize;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
      if (bufSize < 0) {
        tscError("invalid buf size");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      bool squote = false;
      for (int32_t i = 0; i < bufSize; ++i) {
        if (((char *)buf)[i] == '\'') {
          squote = true;
          break;
        }
      }

      if (squote) {
        *str = '\"';
      } else {
        *str = '\'';
      }

      memcpy(str + 1, buf, bufSize);
      if (squote) {
        *(str + bufSize + 1) = '\"';
      } else {
        *(str + bufSize + 1) = '\'';
      }
      n = bufSize + 2;
      break;

    default:
      tscError("unsupported type:%d", type);
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  *len = n;

  return TSDB_CODE_SUCCESS;
}

UNUSED_FUNC static void tscStrToLower(char* str, int32_t n) {
  if (str == NULL || n <= 0) { return;}
  for (int32_t i = 0; i < n; i++) {
    if (str[i] >= 'A' && str[i] <= 'Z') {
        str[i] -= ('A' - 'a');
    }
  }
}
SCond* tsGetSTableQueryCond(STagCond* pTagCond, uint64_t uid) {
  if (pTagCond->pCond == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(pTagCond->pCond);
  for (int32_t i = 0; i < size; ++i) {
    SCond* pCond = taosArrayGet(pTagCond->pCond, i);

    if (uid == pCond->uid) {
      return pCond;
    }
  }

  return NULL;
}

STblCond* tsGetTableFilter(SArray* filters, uint64_t uid, int16_t idx) {
  if (filters == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(filters);
  for (int32_t i = 0; i < size; ++i) {
    STblCond* cond = taosArrayGet(filters, i);

    if (uid == cond->uid && (idx >= 0 && cond->idx == idx)) {
      return cond;
    }
  }

  return NULL;
}


void tsSetSTableQueryCond(STagCond* pTagCond, uint64_t uid, SBufferWriter* bw) {
  if (tbufTell(bw) == 0) {
    return;
  }

  SCond cond = {
    .uid = uid,
    .len = (int32_t)(tbufTell(bw)),
    .cond = NULL,
  };

  cond.cond = tbufGetData(bw, true);

  if (pTagCond->pCond == NULL) {
    pTagCond->pCond = taosArrayInit(3, sizeof(SCond));
  }

  taosArrayPush(pTagCond->pCond, &cond);
}

bool tscQueryTags(SQueryInfo* pQueryInfo) {
  int32_t numOfCols = (int32_t) tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    int32_t functId = pExpr->base.functionId;

    // "select count(tbname)" query
    if (functId == TSDB_FUNC_COUNT && pExpr->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
      continue;
    }

    if (functId != TSDB_FUNC_TAGPRJ && functId != TSDB_FUNC_TID_TAG && functId != TSDB_FUNC_BLKINFO) {
      return false;
    }
  }

  return true;
}

bool tscQueryBlockInfo(SQueryInfo* pQueryInfo) {
  int32_t numOfCols = (int32_t) tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    int32_t functId = pExpr->base.functionId;

    if (functId == TSDB_FUNC_BLKINFO) {
      return true;
    }
  }

  return false;
}

bool tscIsTwoStageSTableQuery(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (pQueryInfo == NULL) {
    return false;
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  if (pTableMetaInfo == NULL) {
    return false;
  }

  if ((pQueryInfo->type & TSDB_QUERY_TYPE_FREE_RESOURCE) == TSDB_QUERY_TYPE_FREE_RESOURCE) {
    return false;
  }

  // for ordered projection query, iterate all qualified vnodes sequentially
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }

  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_STABLE_SUBQUERY) && pQueryInfo->command == TSDB_SQL_SELECT) {
    return UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  }

  return false;
}

bool tscIsProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);

  /*
   * In following cases, return false for non ordered project query on super table
   * 1. failed to get tableMeta from server; 2. not a super table; 3. limitation is 0;
   * 4. show queries, instead of a select query
   */
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  if (pTableMetaInfo == NULL || !UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) ||
      pQueryInfo->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT || numOfExprs == 0) {
    return false;
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    int32_t functionId = tscExprGet(pQueryInfo, i)->base.functionId;

    if (functionId < 0) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
      if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
        return false;
      }

      continue;
    }

    if (functionId != TSDB_FUNC_PRJ &&
        functionId != TSDB_FUNC_TAGPRJ &&
        functionId != TSDB_FUNC_TAG &&
        functionId != TSDB_FUNC_TS &&
        functionId != TSDB_FUNC_SCALAR_EXPR &&
        functionId != TSDB_FUNC_TS_COMP &&
        functionId != TSDB_FUNC_TS_DUMMY &&
        functionId != TSDB_FUNC_TID_TAG &&
        !TSDB_FUNC_IS_SCALAR(functionId)) {
      return false;
    }
  }

  return true;
}

// not order by timestamp projection query on super table
bool tscNonOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (!tscIsProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }

  // order by columnIndex not exists, not a ordered projection query
  return pQueryInfo->order.orderColId < 0;
}

bool tscOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (!tscIsProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }

  // order by columnIndex exists, a ordered projection query
  return pQueryInfo->order.orderColId >= 0;
}

bool tscIsProjectionQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < size; ++i) {
    int32_t f = tscExprGet(pQueryInfo, i)->base.functionId;
    if (f == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if (f != TSDB_FUNC_PRJ && f != TSDB_FUNC_TAGPRJ && f != TSDB_FUNC_TAG &&
        f != TSDB_FUNC_TS && f != TSDB_FUNC_SCALAR_EXPR && f != TSDB_FUNC_DIFF &&
        f != TSDB_FUNC_DERIVATIVE && !TSDB_FUNC_IS_SCALAR(f) &&
        f != TSDB_FUNC_STATE_COUNT && f != TSDB_FUNC_STATE_DURATION) {
      return false;
    }
  }

  return true;
}

// these functions diff/derivative/csum/mavg will return the result computed on current row and history row/rows
// as the result for current row
bool tscIsDiffDerivLikeQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < size; ++i) {
    int32_t f = tscExprGet(pQueryInfo, i)->base.functionId;
    if (f == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if (f == TSDB_FUNC_DIFF || f == TSDB_FUNC_DERIVATIVE || f == TSDB_FUNC_CSUM || f == TSDB_FUNC_MAVG ||
        f == TSDB_FUNC_STATE_COUNT || f == TSDB_FUNC_STATE_DURATION) {
      return true;
    }
  }

  return false;
}

bool tscHasColumnFilter(SQueryInfo* pQueryInfo) {
  // filter on primary timestamp column
  if (pQueryInfo->window.skey != INT64_MIN || pQueryInfo->window.ekey != INT64_MAX) {
    return true;
  }

  size_t size = taosArrayGetSize(pQueryInfo->colList);
  for (int32_t i = 0; i < size; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    if (pCol->info.flist.numOfFilters > 0) {
      return true;
    }
  }

  return false;
}

bool tscIsPointInterpQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    assert(pExpr != NULL);

    int32_t functionId = pExpr->base.functionId;
    if (functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if (functionId != TSDB_FUNC_INTERP) {
      return false;
    }
  }

  return true;
}

bool tscNeedTableSeqScan(SQueryInfo* pQueryInfo) {
  return pQueryInfo->stableQuery && (tscQueryContainsFunction(pQueryInfo, TSDB_FUNC_TWA) || tscQueryContainsFunction(pQueryInfo, TSDB_FUNC_ELAPSED) || (pQueryInfo->tsBuf != NULL));
}

bool tscGetPointInterpQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    assert(pExpr != NULL);

    int32_t functionId = pExpr->base.functionId;

    if (functionId == TSDB_FUNC_INTERP) {
      return true;
    }
  }

  return false;
}

bool tsIsArithmeticQueryOnAggResult(SQueryInfo* pQueryInfo) {
  if (tscIsProjectionQuery(pQueryInfo)) {
    return false;
  }

  size_t numOfOutput = tscNumOfFields(pQueryInfo);
  for(int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, i)->pExpr;
    if (pExprInfo->pExpr != NULL) {
      return true;
    }
  }

  if (tscIsProjectionQuery(pQueryInfo)) {
    return false;
  }

  return false;
}

bool tscGroupbyColumn(SQueryInfo* pQueryInfo) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  int32_t         numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

  SGroupbyExpr* pGroupbyExpr = &pQueryInfo->groupbyExpr;
  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    SColIndex* pIndex = taosArrayGet(pGroupbyExpr->columnInfo, k);
    if (!TSDB_COL_IS_TAG(pIndex->flag) && pIndex->colIndex < numOfCols) {  // group by normal columns
      return true;
    }
  }

  return false;
}

bool tscGroupbyTag(SQueryInfo* pQueryInfo) {
  SGroupbyExpr* pGroupbyExpr = &pQueryInfo->groupbyExpr;
  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    SColIndex* pIndex = taosArrayGet(pGroupbyExpr->columnInfo, k);
    if (TSDB_COL_IS_TAG(pIndex->flag)) {  // group by tag
      return true;
    }
  }

  return false;
}


int32_t tscGetTopBotQueryExprIndex(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    if (pExpr->base.functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (pExpr->base.functionId == TSDB_FUNC_TOP || pExpr->base.functionId == TSDB_FUNC_BOTTOM) {
      return i;
    }
  }

  return -1;
}

bool tscIsTopBotQuery(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    if (pExpr->base.functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (pExpr->base.functionId == TSDB_FUNC_TOP || pExpr->base.functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

bool isTsCompQuery(SQueryInfo* pQueryInfo) {
  size_t    numOfExprs = tscNumOfExprs(pQueryInfo);
  SExprInfo* pExpr1 = tscExprGet(pQueryInfo, 0);
  if (numOfExprs != 1) {
    return false;
  }

  return pExpr1->base.functionId == TSDB_FUNC_TS_COMP;
}

bool hasTagValOutput(SQueryInfo* pQueryInfo) {
  size_t    numOfExprs = tscNumOfExprs(pQueryInfo);
  SExprInfo* pExpr1 = tscExprGet(pQueryInfo, 0);

  if (numOfExprs == 1 && pExpr1->base.functionId == TSDB_FUNC_TS_COMP) {
    return true;
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    // ts_comp column required the tag value for join filter
    if (TSDB_COL_IS_TAG(pExpr->base.colInfo.flag)) {
      return true;
    }
  }

  return false;
}

bool timeWindowInterpoRequired(SQueryInfo *pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->base.functionId;
    if (functionId == TSDB_FUNC_TWA || functionId == TSDB_FUNC_INTERP || functionId == TSDB_FUNC_ELAPSED) {
      return true;
    }
  }

  return false;
}

bool isStabledev(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->base.functionId;
    if (functionId == TSDB_FUNC_STDDEV_DST) {
      return true;
    }
  }

  return false;
}

bool tscIsTWAQuery(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    if (pExpr->base.functionId == TSDB_FUNC_TWA) {
      return true;
    }
  }

  return false;
}

bool tscIsIrateQuery(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    if (pExpr->base.functionId == TSDB_FUNC_IRATE) {
      return true;
    }
  }

  return false;
}

bool tscQueryContainsFunction(SQueryInfo* pQueryInfo, int16_t functionId) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    if (pExpr->base.functionId == functionId) {
      return true;
    }
  }

  return false;
}

bool tscIsSessionWindowQuery(SQueryInfo* pQueryInfo) {
  return pQueryInfo->sessionWindow.gap > 0;
}

bool tscNeedReverseScan(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->base.functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if ((functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_FIRST_DST) && pQueryInfo->order.order == TSDB_ORDER_DESC) {
      return true;
    }

    if (functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_LAST_DST) {
      // the scan order to acquire the last result of the specified column
      int32_t order = (int32_t)pExpr->base.param[0].i64;
      if (order != pQueryInfo->order.order) {
        return true;
      }
    }
  }

  return false;
}

bool isSimpleAggregateRv(SQueryInfo* pQueryInfo) {
  if (pQueryInfo->interval.interval > 0 || pQueryInfo->sessionWindow.gap > 0) {
    return false;
  }

  if (tscIsDiffDerivLikeQuery(pQueryInfo)) {
    return false;
  }

  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->base.functionId;
    if (functionId < 0) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
      if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
        return true;
      }

      continue;
    }

    if (TSDB_FUNC_IS_SCALAR(functionId)) {
      continue;
    }

    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if ((!IS_MULTIOUTPUT(aAggs[functionId].status)) ||
        (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM ||
         functionId == TSDB_FUNC_TS_COMP ||
         functionId == TSDB_FUNC_SAMPLE ||
         functionId == TSDB_FUNC_HISTOGRAM ||
         functionId == TSDB_FUNC_UNIQUE ||
         functionId == TSDB_FUNC_TAIL)) {
      return true;
    }
  }

  return false;
}

bool isBlockDistQuery(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  SExprInfo* pExpr = tscExprGet(pQueryInfo, 0);
  return (numOfExprs == 1 && pExpr->base.functionId == TSDB_FUNC_BLKINFO);
}

void tscClearInterpInfo(SQueryInfo* pQueryInfo) {
  if (!tscIsPointInterpQuery(pQueryInfo)) {
    return;
  }

  pQueryInfo->fillType = TSDB_FILL_NONE;
  tfree(pQueryInfo->fillVal);
}

int32_t tscCreateResPointerInfo(SSqlRes* pRes, SQueryInfo* pQueryInfo) {
  pRes->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;

  if (pRes->tsrow == NULL) {
    pRes->tsrow  = calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->urow   = calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->length = calloc(pRes->numOfCols, sizeof(int32_t));
    pRes->buffer = calloc(pRes->numOfCols, POINTER_BYTES);

    // not enough memory
    if (pRes->tsrow == NULL  || pRes->urow == NULL || pRes->length == NULL || (pRes->buffer == NULL && pRes->numOfCols > 0)) {
      tfree(pRes->tsrow);
      tfree(pRes->urow);
      tfree(pRes->length);
      tfree(pRes->buffer);

      pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return pRes->code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void setResRawPtrImpl(SSqlRes* pRes, SInternalField* pInfo, int32_t i, bool convertNchar, bool convertJson) {
  // generated the user-defined column result
  if (pInfo->pExpr->pExpr == NULL && TSDB_COL_IS_UD_COL(pInfo->pExpr->base.colInfo.flag)) {
    if (pInfo->pExpr->base.param[0].nType == TSDB_DATA_TYPE_NULL) {
      setNullN(pRes->urow[i], pInfo->field.type, pInfo->field.bytes, (int32_t) pRes->numOfRows);
    } else {
      if (pInfo->field.type == TSDB_DATA_TYPE_NCHAR || pInfo->field.type == TSDB_DATA_TYPE_BINARY) {
        assert(pInfo->pExpr->base.param[0].nLen <= pInfo->field.bytes);

        for (int32_t k = 0; k < pRes->numOfRows; ++k) {
          char* p = ((char**)pRes->urow)[i] + k * pInfo->field.bytes;

          memcpy(varDataVal(p), pInfo->pExpr->base.param[0].pz, pInfo->pExpr->base.param[0].nLen);
          varDataSetLen(p, pInfo->pExpr->base.param[0].nLen);
        }
      } else {
        for (int32_t k = 0; k < pRes->numOfRows; ++k) {
          char* p = ((char**)pRes->urow)[i] + k * pInfo->field.bytes;
          memcpy(p, &pInfo->pExpr->base.param[0].i64, pInfo->field.bytes);
        }
      }
    }

  } else if (convertNchar && (pInfo->field.type == TSDB_DATA_TYPE_NCHAR)) {
    // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
    char* buffer = realloc(pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);
    if (buffer == NULL) return;
    pRes->buffer[i] = buffer;
    // string terminated char for binary data
    memset(pRes->buffer[i], 0, pInfo->field.bytes * pRes->numOfRows);

    char* p = pRes->urow[i];
    for (int32_t k = 0; k < pRes->numOfRows; ++k) {
      char* dst = pRes->buffer[i] + k * pInfo->field.bytes;

      if (isNull(p, TSDB_DATA_TYPE_NCHAR)) {
        memcpy(dst, p, varDataTLen(p));
      } else if (varDataLen(p) > 0) {
        int32_t length = taosUcs4ToMbs(varDataVal(p), varDataLen(p), varDataVal(dst));
        if (length <= 0) {
          tscError("charset:%s to %s. val:%s convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)p);
        }
        if (length >= 0){
          varDataSetLen(dst, length);
        }
      } else {
        varDataSetLen(dst, 0);
      }

      p += pInfo->field.bytes;
    }
    memcpy(pRes->urow[i], pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);
  }else if (pInfo->field.type == TSDB_DATA_TYPE_JSON) {
      if (convertJson){
        // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
        char* buffer = realloc(pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);
        if (buffer == NULL) return;
        pRes->buffer[i] = buffer;
        // string terminated char for binary data
        memset(pRes->buffer[i], 0, pInfo->field.bytes * pRes->numOfRows);

        char* p = pRes->urow[i];
        for (int32_t k = 0; k < pRes->numOfRows; ++k) {
          char* dst = pRes->buffer[i] + k * pInfo->field.bytes;
          char type = *p;
          char* realData = p + CHAR_BYTES;
          if (type == TSDB_DATA_TYPE_JSON && isNull(realData, TSDB_DATA_TYPE_JSON)) {
            memcpy(dst, realData, varDataTLen(realData));
          } else if (type == TSDB_DATA_TYPE_BINARY) {
            assert(*(uint32_t*)varDataVal(realData) == TSDB_DATA_JSON_null);   // json null value
            assert(varDataLen(realData) == INT_BYTES);
            sprintf(varDataVal(dst), "%s", "null");
            varDataSetLen(dst, strlen(varDataVal(dst)));
          }else if (type == TSDB_DATA_TYPE_JSON) {
            int32_t length = taosUcs4ToMbs(varDataVal(realData), varDataLen(realData), varDataVal(dst));

            if (length <= 0) {
              tscError("charset:%s to %s. val:%s convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)p);
            }
            if (length >= 0){
              varDataSetLen(dst, length);
            }
          }else if (type == TSDB_DATA_TYPE_NCHAR) {   // value -> "value"
            *(char*)varDataVal(dst) = '\"';
            int32_t length = taosUcs4ToMbs(varDataVal(realData), varDataLen(realData), POINTER_SHIFT(varDataVal(dst), CHAR_BYTES));
            if (length <= 0) {
              tscError("charset:%s to %s. val:%s convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)p);
            }
            if (length >= 0){
              varDataSetLen(dst, length + CHAR_BYTES*2);
              *(char*)(POINTER_SHIFT(varDataVal(dst), length + CHAR_BYTES)) = '\"';
            }
          }else if (type == TSDB_DATA_TYPE_DOUBLE) {
            double jsonVd = *(double*)(realData);
            sprintf(varDataVal(dst), "%.9lf", jsonVd);
            varDataSetLen(dst, strlen(varDataVal(dst)));
          }else if (type == TSDB_DATA_TYPE_BIGINT) {
            int64_t jsonVd = *(int64_t*)(realData);
            sprintf(varDataVal(dst), "%" PRId64, jsonVd);
            varDataSetLen(dst, strlen(varDataVal(dst)));
          }else if (type == TSDB_DATA_TYPE_BOOL) {
            sprintf(varDataVal(dst), "%s", (*((char *)realData) == 1) ? "true" : "false");
            varDataSetLen(dst, strlen(varDataVal(dst)));
          }else {
            assert(0);
          }

          p += pInfo->field.bytes;
        }
        memcpy(pRes->urow[i], pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);
      }else{
        // if convertJson is false, json data as raw data used for stddev for the second round
      }
  }

  if (convertNchar) {
    pRes->dataConverted = true;
  }
}

void tscJson2String(char *src, char* dst){

}

void tscSetResRawPtr(SSqlRes* pRes, SQueryInfo* pQueryInfo, bool converted) {
  assert(pRes->numOfCols > 0);
  if (pRes->numOfRows == 0) {
    return;
  }
  int32_t offset = 0;
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SInternalField* pInfo = (SInternalField*)TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, i);

    pRes->urow[i] = pRes->data + offset * pRes->numOfRows;
    pRes->length[i] = pInfo->field.bytes;

    offset += pInfo->field.bytes;
    setResRawPtrImpl(pRes, pInfo, i, converted ? false : true, true);
  }
}

void tscSetResRawPtrRv(SSqlRes* pRes, SQueryInfo* pQueryInfo, SSDataBlock* pBlock, bool convertNchar, bool convertJson) {
  assert(pRes->numOfCols > 0);

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SInternalField* pInfo = (SInternalField*)TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, i);

    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);

    pRes->urow[i] = pColData->pData;
    pRes->length[i] = pInfo->field.bytes;

    setResRawPtrImpl(pRes, pInfo, i, convertNchar, convertJson);
    /*
    // generated the user-defined column result
    if (pInfo->pExpr->pExpr == NULL && TSDB_COL_IS_UD_COL(pInfo->pExpr->base.ColName.flag)) {
      if (pInfo->pExpr->base.param[1].nType == TSDB_DATA_TYPE_NULL) {
        setNullN(pRes->urow[i], pInfo->field.type, pInfo->field.bytes, (int32_t) pRes->numOfRows);
      } else {
        if (pInfo->field.type == TSDB_DATA_TYPE_NCHAR || pInfo->field.type == TSDB_DATA_TYPE_BINARY) {
          assert(pInfo->pExpr->base.param[1].nLen <= pInfo->field.bytes);

          for (int32_t k = 0; k < pRes->numOfRows; ++k) {
            char* p = ((char**)pRes->urow)[i] + k * pInfo->field.bytes;

            memcpy(varDataVal(p), pInfo->pExpr->base.param[1].pz, pInfo->pExpr->base.param[1].nLen);
            varDataSetLen(p, pInfo->pExpr->base.param[1].nLen);
          }
        } else {
          for (int32_t k = 0; k < pRes->numOfRows; ++k) {
            char* p = ((char**)pRes->urow)[i] + k * pInfo->field.bytes;
            memcpy(p, &pInfo->pExpr->base.param[1].i64, pInfo->field.bytes);
          }
        }
      }

    } else if (convertNchar && pInfo->field.type == TSDB_DATA_TYPE_NCHAR) {
      // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
      pRes->buffer[i] = realloc(pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);

      // string terminated char for binary data
      memset(pRes->buffer[i], 0, pInfo->field.bytes * pRes->numOfRows);

      char* p = pRes->urow[i];
      for (int32_t k = 0; k < pRes->numOfRows; ++k) {
        char* dst = pRes->buffer[i] + k * pInfo->field.bytes;

        if (isNull(p, TSDB_DATA_TYPE_NCHAR)) {
          memcpy(dst, p, varDataTLen(p));
        } else if (varDataLen(p) > 0) {
          int32_t length = taosUcs4ToMbs(varDataVal(p), varDataLen(p), varDataVal(dst));
          varDataSetLen(dst, length);

          if (length == 0) {
            tscError("charset:%s to %s. val:%s convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)p);
          }
        } else {
          varDataSetLen(dst, 0);
        }

        p += pInfo->field.bytes;
      }

      memcpy(pRes->urow[i], pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);
    }*/
  }
}

typedef struct SDummyInputInfo {
  SSDataBlock     *block;
  STableQueryInfo *pTableQueryInfo;
  SSqlObj         *pSql;  // refactor: remove it
  void            *pFilterInfo;
} SDummyInputInfo;

typedef struct SJoinStatus {
  SSDataBlock* pBlock;   // point to the upstream block
  int32_t      index;
  bool         completed;// current upstream is completed or not
} SJoinStatus;

typedef struct SJoinOperatorInfo {
  SSDataBlock   *pRes;
  SJoinStatus   *status;
  int32_t        numOfUpstream;
  SRspResultInfo resultInfo;  // todo refactor, add this info for each operator
} SJoinOperatorInfo;

static void doSetupSDataBlock(SSqlRes* pRes, SSDataBlock* pBlock, void* pFilterInfo) {
  int32_t offset = 0;
  char* pData = pRes->data;

  for(int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    if (pData != NULL) {
      pColData->pData = pData + offset * pBlock->info.rows;
    } else {
      pColData->pData = pRes->urow[i];
    }

    if (pColData->info.type == TSDB_DATA_TYPE_NCHAR) {
      int32_t rows = pBlock->info.rows;
      int32_t bytes = pColData->info.bytes;
      char *dstData = malloc(bytes * rows);
      char *srcData = pColData->pData;
      for (int32_t j = 0; j < rows; ++j) {
        char *srcRow = srcData + bytes * j;
        char *dstRow = dstData + bytes * j;
        if(isNull(srcRow, TSDB_DATA_TYPE_NCHAR)){
          varDataCopy(dstRow, srcRow);
          continue;
        }
        int32_t len = 0;
        taosMbsToUcs4(varDataVal(srcRow), varDataLen(srcRow), varDataVal(dstRow),
                      bytes-VARSTR_HEADER_SIZE, &len);
        varDataSetLen(dstRow, len);
      }
      memcpy(srcData, dstData, bytes*rows);
      free(dstData);
    }
    offset += pColData->info.bytes;
  }

  // filter data if needed
  if (pFilterInfo) {
    SColumnDataParam param = {.numOfCols = pBlock->info.numOfCols, .pDataBlock = pBlock->pDataBlock};
    filterSetColFieldData(pFilterInfo, &param, getColumnDataFromId);

    int8_t* p = NULL;
    //bool all = doFilterDataBlock(pFilterInfo, numOfFilterCols, pBlock->info.rows, p);
    bool all = filterExecute(pFilterInfo, pBlock->info.rows, &p, NULL, 0);
    if (!all) {
      if (p) {
        doCompactSDataBlock(pBlock, pBlock->info.rows, p);
      } else {
        pBlock->info.rows = 0;
        pBlock->pBlockStatis = NULL;  // clean the block statistics info
      }
    }

    tfree(p);
  }

  // todo refactor: extract method
  // set the timestamp range of current result data block
  SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, 0);
  if (pColData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    pBlock->info.window.skey = ((int64_t*)pColData->pData)[0];
    pBlock->info.window.ekey = ((int64_t*)pColData->pData)[pBlock->info.rows-1];
  }

  pRes->numOfRows = 0;
}

// NOTE: there is already exists data blocks before this function calls.
SSDataBlock* doGetDataBlock(void* param, bool* newgroup) {
  SOperatorInfo *pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SDummyInputInfo *pInput = pOperator->info;
  SSqlObj* pSql = pInput->pSql;
  SSqlRes* pRes = &pSql->res;

  SSDataBlock* pBlock = pInput->block;
  if (pOperator->pRuntimeEnv != NULL) {
    pOperator->pRuntimeEnv->current = pInput->pTableQueryInfo;
  }

  pBlock->info.rows = pRes->numOfRows;
  if (pRes->numOfRows != 0) {
    doSetupSDataBlock(pRes, pBlock, pInput->pFilterInfo);
    if (pBlock->info.rows > 0) {
      *newgroup = false;
      return pBlock;
    }
  }

  SSDataBlock* result = NULL;
  do {
    // No data block exists. So retrieve and transfer it into to SSDataBlock
    TAOS_ROW pRow = NULL;
    taos_fetch_block(pSql, &pRow);

    if (pRes->numOfRows == 0) {
      pOperator->status = OP_EXEC_DONE;
      result = NULL;
      break;
    }
    pBlock->info.rows = pRes->numOfRows;
    doSetupSDataBlock(pRes, pBlock, pInput->pFilterInfo);
    *newgroup = false;
    result = pBlock;
  } while (result->info.rows == 0);

  return result;
}

static void fetchNextBlockIfCompleted(SOperatorInfo* pOperator, bool* newgroup) {
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

  for (int32_t i = 0; i < pOperator->numOfUpstream; ++i) {
    SJoinStatus* pStatus = &pJoinInfo->status[i];
    if (pStatus->pBlock == NULL || pStatus->index >= pStatus->pBlock->info.rows) {
      tscDebug("Retrieve nest query result, index:%d, total:%d", i, pOperator->numOfUpstream);

      publishOperatorProfEvent(pOperator->upstream[0], QUERY_PROF_BEFORE_OPERATOR_EXEC);
      pStatus->pBlock = pOperator->upstream[i]->exec(pOperator->upstream[i], newgroup);
      publishOperatorProfEvent(pOperator->upstream[0], QUERY_PROF_AFTER_OPERATOR_EXEC);
      pStatus->index = 0;

      if (pStatus->pBlock == NULL) {
        pOperator->status = OP_EXEC_DONE;
        pJoinInfo->resultInfo.total += pJoinInfo->pRes->info.rows;
        break;
      }
    }
  }
}

SSDataBlock* doDataBlockJoin(void* param, bool* newgroup) {
  SOperatorInfo *pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  assert(pOperator->numOfUpstream > 1);

  SJoinOperatorInfo* pJoinInfo = pOperator->info;
  pJoinInfo->pRes->info.rows = 0;

  while(1) {
    fetchNextBlockIfCompleted(pOperator, newgroup);
    if (pOperator->status == OP_EXEC_DONE) {
      return pJoinInfo->pRes;
    }

    SJoinStatus* st0 = &pJoinInfo->status[0];
    SColumnInfoData* p0 = taosArrayGet(st0->pBlock->pDataBlock, 0);
    int64_t* ts0 = (int64_t*) p0->pData;

    if (st0->index >= st0->pBlock->info.rows) {
      continue;
    }

    bool prefixEqual = true;

    while(1) {
      prefixEqual = true;
      for (int32_t i = 1; i < pJoinInfo->numOfUpstream; ++i) {
        SJoinStatus* st = &pJoinInfo->status[i];
        ts0 = (int64_t*) p0->pData;

        SColumnInfoData* p = taosArrayGet(st->pBlock->pDataBlock, 0);
        int64_t*         ts = (int64_t*)p->pData;

        if (st->index >= st->pBlock->info.rows || st0->index >= st0->pBlock->info.rows) {
          fetchNextBlockIfCompleted(pOperator, newgroup);
          if (pOperator->status == OP_EXEC_DONE) {
            return pJoinInfo->pRes;
          }

          prefixEqual = false;
          break;
        }

        if (ts[st->index] < ts0[st0->index]) {  // less than the first
          prefixEqual = false;

          if ((++(st->index)) >= st->pBlock->info.rows) {
            fetchNextBlockIfCompleted(pOperator, newgroup);
            if (pOperator->status == OP_EXEC_DONE) {
              return pJoinInfo->pRes;
            }
          }
        } else if (ts[st->index] > ts0[st0->index]) {  // greater than the first;
          if (prefixEqual == true) {
            prefixEqual = false;
            for (int32_t j = 0; j < i; ++j) {
              SJoinStatus* stx = &pJoinInfo->status[j];
              if ((++(stx->index)) >= stx->pBlock->info.rows) {

                fetchNextBlockIfCompleted(pOperator, newgroup);
                if (pOperator->status == OP_EXEC_DONE) {
                  return pJoinInfo->pRes;
                }
              }
            }
          } else {
            if ((++(st0->index)) >= st0->pBlock->info.rows) {
              fetchNextBlockIfCompleted(pOperator, newgroup);
              if (pOperator->status == OP_EXEC_DONE) {
                return pJoinInfo->pRes;
              }
            }
          }
        }
      }

      if (prefixEqual) {
        int32_t offset = 0;
        bool completed = false;
        for (int32_t i = 0; i < pOperator->numOfUpstream; ++i) {
          SJoinStatus* st1 = &pJoinInfo->status[i];
          int32_t      rows = pJoinInfo->pRes->info.rows;

          for (int32_t j = 0; j < st1->pBlock->info.numOfCols; ++j) {
            SColumnInfoData* pCol1 = taosArrayGet(pJoinInfo->pRes->pDataBlock, j + offset);
            SColumnInfoData* pSrc = taosArrayGet(st1->pBlock->pDataBlock, j);

            int32_t bytes = pSrc->info.bytes;
            memcpy(pCol1->pData + rows * bytes, pSrc->pData + st1->index * bytes, bytes);
          }

          offset += st1->pBlock->info.numOfCols;
          if ((++(st1->index)) == st1->pBlock->info.rows) {
            completed = true;
          }
        }

        if ((++pJoinInfo->pRes->info.rows) >= pJoinInfo->resultInfo.capacity) {
          pJoinInfo->resultInfo.total += pJoinInfo->pRes->info.rows;
          return pJoinInfo->pRes;
        }

        if (completed == true) {
          break;
        }
      }
    }
/*
    while (st0->index < st0->pBlock->info.rows && st1->index < st1->pBlock->info.rows) {
      SColumnInfoData* p0 = taosArrayGet(st0->pBlock->pDataBlock, 0);
      SColumnInfoData* p1 = taosArrayGet(st1->pBlock->pDataBlock, 0);

      int64_t* ts0 = (int64_t*)p0->pData;
      int64_t* ts1 = (int64_t*)p1->pData;
      if (ts0[st0->index] == ts1[st1->index]) {  // add to the final result buffer
        // check if current output buffer is over the threshold to pause current loop
        int32_t rows = pJoinInfo->pRes->info.rows;
        for (int32_t j = 0; j < st0->pBlock->info.numOfCols; ++j) {
          SColumnInfoData* pCol1 = taosArrayGet(pJoinInfo->pRes->pDataBlock, j);
          SColumnInfoData* pSrc = taosArrayGet(st0->pBlock->pDataBlock, j);

          int32_t bytes = pSrc->info.bytes;
          memcpy(pCol1->pData + rows * bytes, pSrc->pData + st0->index * bytes, bytes);
        }

        for (int32_t j = 0; j < st1->pBlock->info.numOfCols; ++j) {
          SColumnInfoData* pCol1 = taosArrayGet(pJoinInfo->pRes->pDataBlock, j + st0->pBlock->info.numOfCols);
          SColumnInfoData* pSrc = taosArrayGet(st1->pBlock->pDataBlock, j);

          int32_t bytes = pSrc->info.bytes;
          memcpy(pCol1->pData + rows * bytes, pSrc->pData + st1->index * bytes, bytes);
        }

        st0->index++;
        st1->index++;

        if ((++pJoinInfo->pRes->info.rows) >= pJoinInfo->resultInfo.capacity) {
          pJoinInfo->resultInfo.total += pJoinInfo->pRes->info.rows;
          return pJoinInfo->pRes;
        }
      } else if (ts0[st0->index] < ts1[st1->index]) {
        st0->index++;
      } else {
        st1->index++;
      }
    }*/
  }
}

static void destroyDummyInputOperator(void* param, int32_t numOfOutput) {
  SDummyInputInfo* pInfo = (SDummyInputInfo*) param;

  // tricky
  for(int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pInfo->block->pDataBlock, i);
    pColInfoData->pData = NULL;
  }

  pInfo->block = destroyOutputBuf(pInfo->block);
  pInfo->pSql = NULL;

  filterFreeInfo(pInfo->pFilterInfo);

  cleanupResultRowInfo(&pInfo->pTableQueryInfo->resInfo);
  tfree(pInfo->pTableQueryInfo);
}

// todo this operator servers as the adapter for Operator tree and SqlRes result, remove it later
SOperatorInfo* createDummyInputOperator(SSqlObj* pSql, SSchema* pSchema, int32_t numOfCols, void* pFilters) {
  assert(numOfCols > 0);
  STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};

  SDummyInputInfo* pInfo = calloc(1, sizeof(SDummyInputInfo));

  pInfo->pSql            = pSql;
  pInfo->pFilterInfo     = pFilters;
  pInfo->pTableQueryInfo = createTmpTableQueryInfo(win);

  pInfo->block = calloc(numOfCols, sizeof(SSDataBlock));
  pInfo->block->info.numOfCols = numOfCols;

  pInfo->block->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData colData = {{0}};
    colData.info.bytes = pSchema[i].bytes;
    colData.info.type  = pSchema[i].type;
    colData.info.colId = pSchema[i].colId;

    taosArrayPush(pInfo->block->pDataBlock, &colData);
  }

  SOperatorInfo* pOptr = calloc(1, sizeof(SOperatorInfo));
  pOptr->name          = "DummyInputOperator";
  pOptr->operatorType  = OP_DummyInput;
  pOptr->numOfOutput   = numOfCols;
  pOptr->blockingOptr  = false;
  pOptr->info          = pInfo;
  pOptr->exec          = doGetDataBlock;
  pOptr->cleanup       = destroyDummyInputOperator;
  return pOptr;
}

static void destroyJoinOperator(void* param, int32_t numOfOutput) {
  SJoinOperatorInfo* pInfo = (SJoinOperatorInfo*) param;
  tfree(pInfo->status);

  pInfo->pRes = destroyOutputBuf(pInfo->pRes);
}

SOperatorInfo* createJoinOperatorInfo(SOperatorInfo** pUpstream, int32_t numOfUpstream, SSchema* pSchema, int32_t numOfOutput) {
  SJoinOperatorInfo* pInfo = calloc(1, sizeof(SJoinOperatorInfo));

  pInfo->numOfUpstream = numOfUpstream;
  pInfo->status = calloc(numOfUpstream, sizeof(SJoinStatus));

  SRspResultInfo* pResInfo = &pInfo->resultInfo;
  pResInfo->capacity  = 4096;
  pResInfo->threshold = (int32_t) (4096 * 0.8);

  pInfo->pRes = calloc(1, sizeof(SSDataBlock));
  pInfo->pRes->info.numOfCols = numOfOutput;
  pInfo->pRes->pDataBlock = taosArrayInit(numOfOutput, sizeof(SColumnInfoData));
  for(int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData colData = {{0}};
    colData.info.bytes = pSchema[i].bytes;
    colData.info.type  = pSchema[i].type;
    colData.info.colId = pSchema[i].colId;
    colData.pData = calloc(1, colData.info.bytes * pResInfo->capacity);

    taosArrayPush(pInfo->pRes->pDataBlock, &colData);
  }

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name          = "JoinOperator";
  pOperator->operatorType  = OP_Join;
  pOperator->numOfOutput   = numOfOutput;
  pOperator->blockingOptr  = false;
  pOperator->info          = pInfo;
  pOperator->exec          = doDataBlockJoin;
  pOperator->cleanup       = destroyJoinOperator;

  for(int32_t i = 0; i < numOfUpstream; ++i) {
    appendUpstream(pOperator, pUpstream[i]);
  }

  return pOperator;
}

void convertQueryResult(SSqlRes* pRes, SQueryInfo* pQueryInfo, uint64_t objId, bool convertNchar, bool convertJson) {
  // set the correct result
  SSDataBlock* p = pQueryInfo->pQInfo->runtimeEnv.outputBuf;
  pRes->numOfRows = (p != NULL)? p->info.rows: 0;

  if (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows > 0) {
    tscCreateResPointerInfo(pRes, pQueryInfo);
    tscSetResRawPtrRv(pRes, pQueryInfo, p, convertNchar, convertJson);
  }

  tscDebug("0x%"PRIx64" retrieve result in pRes, numOfRows:%d", objId, pRes->numOfRows);
  pRes->row = 0;
  pRes->completed = (pRes->numOfRows == 0);
}

/*
static void createInputDataFilterInfo(SQueryInfo* px, int32_t numOfCol1, int32_t* numOfFilterCols, SSingleColumnFilterInfo** pFilterInfo) {
  SColumnInfo* tableCols = calloc(numOfCol1, sizeof(SColumnInfo));
  for(int32_t i = 0; i < numOfCol1; ++i) {
    SColumn* pCol = taosArrayGetP(px->colList, i);
    if (pCol->info.flist.numOfFilters > 0) {
      (*numOfFilterCols) += 1;
    }

    tableCols[i] = pCol->info;
  }

  if ((*numOfFilterCols) > 0) {
    doCreateFilterInfo(tableCols, numOfCol1, (*numOfFilterCols), pFilterInfo, 0);
  }

  tfree(tableCols);
}
*/


void handleDownstreamOperator(SSqlObj** pSqlObjList, int32_t numOfUpstream, SQueryInfo* px, SSqlObj* pSql) {
  SSqlRes* pOutput = &pSql->res;

  // handle the following query process
  if (px->pQInfo == NULL) {
    STableMeta* pTableMeta = tscGetMetaInfo(px, 0)->pTableMeta;
    SSchema* pSchema = tscGetTableSchema(pTableMeta);

    STableGroupInfo tableGroupInfo = {
        .numOfTables = 1,
        .pGroupList = taosArrayInit(1, POINTER_BYTES),
    };

    SUdfInfo* pUdfInfo = NULL;
    
    size_t size = tscNumOfExprs(px);
    for (int32_t j = 0; j < size; ++j) {
      SExprInfo* pExprInfo = tscExprGet(px, j);

      int32_t functionId = pExprInfo->base.functionId;
      if (functionId < 0) {
        if (pUdfInfo) {
          pSql->res.code = tscInvalidOperationMsg(pSql->cmd.payload, "only one udf allowed", NULL);
          return;
        }
        
        pUdfInfo = taosArrayGet(px->pUdfInfo, -1 * functionId - 1);
        int32_t code = initUdfInfo(pUdfInfo);
        if (code != TSDB_CODE_SUCCESS) {
          pSql->res.code = code;
          return;
        }
      }
    }

    STableKeyInfo tableKeyInfo = {.pTable = NULL, .lastKey = INT64_MIN};

    SArray* group = taosArrayInit(1, sizeof(STableKeyInfo));
    taosArrayPush(group, &tableKeyInfo);

    taosArrayPush(tableGroupInfo.pGroupList, &group);

    // if it is a join query, create join operator here
    int32_t numOfCol1 = pTableMeta->tableInfo.numOfColumns;

    void *pFilters = NULL;
    STblCond *pCond = NULL;
    if (px->colCond) {
      pCond = tsGetTableFilter(px->colCond, pTableMeta->id.uid, 0);
      if (pCond && pCond->cond) {
        createQueryFilter(pCond->cond, pCond->len, &pFilters);
      }
    }

    SOperatorInfo* pSourceOperator = createDummyInputOperator(pSqlObjList[0], pSchema, numOfCol1, pFilters);

    pOutput->precision = pSqlObjList[0]->res.precision;

    SSchema* schema = NULL;
    if (px->numOfTables > 1) {
      SOperatorInfo** p = calloc(px->numOfTables, POINTER_BYTES);
      p[0] = pSourceOperator;

      int32_t num = (int32_t) taosArrayGetSize(px->colList);
      schema = calloc(num, sizeof(SSchema));
      memcpy(schema, pSchema, numOfCol1*sizeof(SSchema));

      int32_t offset = pSourceOperator->numOfOutput;

      for(int32_t i = 1; i < px->numOfTables; ++i) {
        STableMeta* pTableMeta1 = tscGetMetaInfo(px, i)->pTableMeta;
        numOfCol1 = pTableMeta1->tableInfo.numOfColumns;
        void *pFilters1 = NULL;

        SSchema* pSchema1 = tscGetTableSchema(pTableMeta1);
        int32_t n = pTableMeta1->tableInfo.numOfColumns;

        if (px->colCond) {
          pCond = tsGetTableFilter(px->colCond, pTableMeta1->id.uid, i);
          if (pCond && pCond->cond) {
            createQueryFilter(pCond->cond, pCond->len, &pFilters1);
          }
          //createInputDataFilterInfo(px, numOfCol1, &numOfFilterCols1, &pFilterInfo1);
        }

        p[i] = createDummyInputOperator(pSqlObjList[i], pSchema1, n, pFilters1);
        memcpy(&schema[offset], pSchema1, n * sizeof(SSchema));
        offset += n;
      }

      pSourceOperator = createJoinOperatorInfo(p, px->numOfTables, schema, num);
      tfree(p);
    } else {
      size_t num = taosArrayGetSize(px->colList);
      schema = calloc(num, sizeof(SSchema));
      memcpy(schema, pSchema, numOfCol1*sizeof(SSchema));
    }

    // update the exprinfo
    int32_t numOfOutput = (int32_t)tscNumOfExprs(px);
    for(int32_t i = 0; i < numOfOutput; ++i) {
      SExprInfo* pex = taosArrayGetP(px->exprList, i);
      int32_t colId = pex->base.colInfo.colId;
      for(int32_t j = 0; j < pSourceOperator->numOfOutput; ++j) {
        if (colId == schema[j].colId) {
          pex->base.colInfo.colIndex = j;
          break;
        }
      }

      // set input data order to param[1]
      if(pex->base.functionId == TSDB_FUNC_FIRST || pex->base.functionId  ==  TSDB_FUNC_FIRST_DST ||
         pex->base.functionId == TSDB_FUNC_LAST  || pex->base.functionId  ==  TSDB_FUNC_LAST_DST) {
        // set input order
        SQueryInfo* pInputQI = pSqlObjList[0]->cmd.pQueryInfo;
        if(pInputQI) {
          pex->base.numOfParams = 3;
          pex->base.param[2].nType = TSDB_DATA_TYPE_INT;
          pex->base.param[2].i64 = pInputQI->order.order;
        }
      }
    }

    tscDebug("0x%"PRIx64" create QInfo 0x%"PRIx64" to execute the main query while all nest queries are ready", pSql->self, pSql->self);
    px->pQInfo = createQInfoFromQueryNode(px, &tableGroupInfo, pSourceOperator, NULL, NULL, MASTER_SCAN, pSql->self);
    if (px->pQInfo == NULL) {
      tscAsyncResultOnError(pSql);
      pOutput->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      return;
    }

    px->pQInfo->runtimeEnv.udfIsCopy = true;
    px->pQInfo->runtimeEnv.pUdfInfo = pUdfInfo;
    
    tfree(schema);

    // set the pRuntimeEnv for pSourceOperator
    pSourceOperator->pRuntimeEnv = &px->pQInfo->runtimeEnv;
  }

  uint64_t qId = pSql->self;
  qTableQuery(px->pQInfo, &qId);
  convertQueryResult(pOutput, px, pSql->self, true, false);
}

static void tscDestroyResPointerInfo(SSqlRes* pRes) {
  if (pRes->buffer != NULL) { // free all buffers containing the multibyte string
    for (int i = 0; i < pRes->numOfCols; i++) {
      tfree(pRes->buffer[i]);
    }

    pRes->numOfCols = 0;
  }

  tfree(pRes->pRsp);

  tfree(pRes->tsrow);
  tfree(pRes->length);
  tfree(pRes->buffer);
  tfree(pRes->urow);

  tfree(pRes->pColumnIndex);
  tfree(pRes->final);

  pRes->data = NULL;  // pRes->data points to the buffer of pRsp, no need to free
}

void tscFreeQueryInfo(SSqlCmd* pCmd, bool removeCachedMeta, uint64_t id) {
  if (pCmd == NULL) {
    return;
  }

  SQueryInfo* pQueryInfo = pCmd->pQueryInfo;

  while(pQueryInfo != NULL) {
    SQueryInfo* p = pQueryInfo->sibling;

    size_t numOfUpstream = taosArrayGetSize(pQueryInfo->pUpstream);
    for(int32_t i = 0; i < numOfUpstream; ++i) {
      SQueryInfo* pUpQueryInfo = taosArrayGetP(pQueryInfo->pUpstream, i);
      freeQueryInfoImpl(pUpQueryInfo);

      clearAllTableMetaInfo(pUpQueryInfo, removeCachedMeta, id);
      if (pUpQueryInfo->pQInfo != NULL) {
        qDestroyQueryInfo(pUpQueryInfo->pQInfo);
        pUpQueryInfo->pQInfo = NULL;
      }

      tfree(pUpQueryInfo);
    }

    if (pQueryInfo->udfCopy) {
      pQueryInfo->pUdfInfo = taosArrayDestroy(&pQueryInfo->pUdfInfo);
    } else {
      pQueryInfo->pUdfInfo = tscDestroyUdfArrayList(pQueryInfo->pUdfInfo);
    }

    freeQueryInfoImpl(pQueryInfo);
    clearAllTableMetaInfo(pQueryInfo, removeCachedMeta, id);

    if (pQueryInfo->pQInfo != NULL) {
      qDestroyQueryInfo(pQueryInfo->pQInfo);
      pQueryInfo->pQInfo = NULL;
    }

    tfree(pQueryInfo);
    pQueryInfo = p;
  }

  pCmd->pQueryInfo = NULL;
  pCmd->active = NULL;
}

void destroyTableNameList(SInsertStatementParam* pInsertParam) {
  if (pInsertParam->numOfTables == 0) {
    assert(pInsertParam->pTableNameList == NULL);
    return;
  }

  for(int32_t i = 0; i < pInsertParam->numOfTables; ++i) {
    tfree(pInsertParam->pTableNameList[i]);
  }

  pInsertParam->numOfTables = 0;
  tfree(pInsertParam->pTableNameList);
}

void tscResetSqlCmd(SSqlCmd* pCmd, bool clearCachedMeta, uint64_t id) {
  SSqlObj *pSql = (SSqlObj*)taosAcquireRef(tscObjRef, id);
  pCmd->command   = 0;
  pCmd->numOfCols = 0;
  pCmd->count     = 0;
  pCmd->msgType   = 0;

  pCmd->insertParam.sql = NULL;
  destroyTableNameList(&pCmd->insertParam);

  pCmd->insertParam.pTableBlockHashList = tscDestroyBlockHashTable(pSql, pCmd->insertParam.pTableBlockHashList, clearCachedMeta);
  pCmd->insertParam.pDataBlocks = tscDestroyBlockArrayList(pSql, pCmd->insertParam.pDataBlocks);
  tfree(pCmd->insertParam.tagData.data);
  pCmd->insertParam.tagData.dataLen = 0;

  tscFreeQueryInfo(pCmd, clearCachedMeta, id);
  pCmd->pTableMetaMap = tscCleanupTableMetaMap(pCmd->pTableMetaMap);
  taosReleaseRef(tscObjRef, id);
}

void* tscCleanupTableMetaMap(SHashObj* pTableMetaMap) {
  if (pTableMetaMap == NULL) {
    return NULL;
  }

  STableMetaVgroupInfo* p = taosHashIterate(pTableMetaMap, NULL);
  while (p) {
    taosArrayDestroy(&p->vgroupIdList);
    tfree(p->pTableMeta);
    p = taosHashIterate(pTableMetaMap, p);
  }

  taosHashCleanup(pTableMetaMap);
  return NULL;
}

void tscFreeSqlResult(SSqlObj* pSql) {
  SSqlRes* pRes = &pSql->res;

  tscDestroyGlobalMerger(pRes->pMerger);
  pRes->pMerger = NULL;

  tscDestroyResPointerInfo(pRes);
  memset(&pSql->res, 0, sizeof(SSqlRes));
}

void tscFreeSubobj(SSqlObj* pSql) {
  if (pSql->subState.numOfSub == 0) {
    return;
  }

  tscDebug("0x%"PRIx64" start to free sub SqlObj, numOfSub:%d", pSql->self, pSql->subState.numOfSub);

  for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    if (pSql->pSubs[i] != NULL) {
      tscDebug("0x%"PRIx64" free sub SqlObj:0x%"PRIx64", index:%d", pSql->self, pSql->pSubs[i]->self, i);
    } else {
      /* just for python error test case */
      tscDebug("0x%"PRIx64" free sub SqlObj:0x0, index:%d", pSql->self, i);
    }
    taos_free_result(pSql->pSubs[i]);
    pSql->pSubs[i] = NULL;
  }

  if (pSql->subState.states) {
    pthread_mutex_destroy(&pSql->subState.mutex);
  }

  tfree(pSql->subState.states);
  pSql->subState.numOfSub = 0;
}

/**
 * The free operation will cause the pSql to be removed from hash table and free it in
 * the function of processmsgfromserver is impossible in this case, since it will fail
 * to retrieve pSqlObj in hashtable.
 *
 * @param pSql
 */
void tscFreeRegisteredSqlObj(void *pSql) {
  assert(pSql != NULL);

  SSqlObj* p = *(SSqlObj**)pSql;
  STscObj* pTscObj = p->pTscObj;
  assert(RID_VALID(p->self));

  int32_t num   = atomic_sub_fetch_32(&pTscObj->numOfObj, 1);
  int32_t total = atomic_sub_fetch_32(&tscNumOfObj, 1);

  tscDebug("0x%"PRIx64" free SqlObj, total in tscObj:%d, total:%d", p->self, num, total);
  tscFreeSqlObj(p);
  taosReleaseRef(tscRefId, pTscObj->rid);
}

void tscFreeMetaSqlObj(int64_t *rid){
  if (RID_VALID(*rid)) {
    SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, *rid);
    if (pSql) {
      taosRemoveRef(tscObjRef, *rid);
      taosReleaseRef(tscObjRef, *rid);
    }

    *rid = 0;
  }
}

void tscFreeSqlObj(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return;
  }

  int64_t sid = pSql->self;

  tscDebug("0x%"PRIx64" start to free sqlObj", pSql->self);

  pSql->res.code = TSDB_CODE_TSC_QUERY_CANCELLED;

  tscFreeMetaSqlObj(&pSql->metaRid);
  tscFreeMetaSqlObj(&pSql->svgroupRid);

  SSqlCmd* pCmd = &pSql->cmd;
  int32_t cmd = pCmd->command;
  if (cmd < TSDB_SQL_INSERT || cmd == TSDB_SQL_RETRIEVE_GLOBALMERGE || cmd == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      cmd == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscRemoveFromSqlList(pSql);
  }

  tscFreeSubobj(pSql);

  pSql->signature = NULL;
  pSql->fp = NULL;
  tfree(pSql->sqlstr);
  tfree(pSql->pBuf);

  tfree(pSql->pSubs);
  pSql->subState.numOfSub = 0;
  pSql->self = 0;

  tscFreeSqlResult(pSql);
  tscResetSqlCmd(pCmd, false, pSql->self);

  tfree(pCmd->payload);
  pCmd->allocSize = 0;

  tscDebug("0x%"PRIx64" addr:%p free completed", sid, pSql);

  tsem_destroy(&pSql->rspSem);
  memset(pSql, 0, sizeof(*pSql));
  free(pSql);
}

void tscDestroyBoundColumnInfo(SParsedDataColInfo* pColInfo) {
  tfree(pColInfo->boundedColumns);
  tfree(pColInfo->cols);
  tfree(pColInfo->colIdxInfo);
}

void tscDestroyDataBlock(SSqlObj *pSql, STableDataBlocks* pDataBlock, bool removeMeta) {
  if (pDataBlock == NULL) {
    return;
  }

  tfree(pDataBlock->pData);

  if (removeMeta) {
    char name[TSDB_TABLE_FNAME_LEN] = {0};
    tNameExtractFullName(&pDataBlock->tableName, name);

    taosHashRemove(UTIL_GET_TABLEMETA(pSql), name, strnlen(name, TSDB_TABLE_FNAME_LEN));
  }

  if (!pDataBlock->cloned) {
    tfree(pDataBlock->params);

    // free the refcount for metermeta
    if (pDataBlock->pTableMeta != NULL) {
      tfree(pDataBlock->pTableMeta);
    }

    tscDestroyBoundColumnInfo(&pDataBlock->boundColumnInfo);
  }

  tfree(pDataBlock);
}

SParamInfo* tscAddParamToDataBlock(STableDataBlocks* pDataBlock, char type, uint8_t timePrec, int16_t bytes,
                                   uint32_t offset) {
  uint32_t needed = pDataBlock->numOfParams + 1;
  if (needed > pDataBlock->numOfAllocedParams) {
    needed *= 2;
    void* tmp = realloc(pDataBlock->params, needed * sizeof(SParamInfo));
    if (tmp == NULL) {
      return NULL;
    }
    pDataBlock->params = (SParamInfo*)tmp;
    pDataBlock->numOfAllocedParams = needed;
  }

  SParamInfo* param = pDataBlock->params + pDataBlock->numOfParams;
  param->idx = -1;
  param->type = type;
  param->timePrec = timePrec;
  param->bytes = bytes;
  param->offset = offset;

  ++pDataBlock->numOfParams;
  return param;
}

void*  tscDestroyBlockArrayList(SSqlObj *pSql, SArray* pDataBlockList) {
  if (pDataBlockList == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(pDataBlockList);
  for (int32_t i = 0; i < size; i++) {
    void* d = taosArrayGetP(pDataBlockList, i);
    tscDestroyDataBlock(pSql, d, false);
  }

  taosArrayDestroy(&pDataBlockList);
  return NULL;
}


void freeUdfInfo(SUdfInfo* pUdfInfo) {
  if (pUdfInfo == NULL) {
    return;
  }

  if (pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY]) {
    (*(udfDestroyFunc)pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY])(&pUdfInfo->init);
  }

  tfree(pUdfInfo->name);

  if (pUdfInfo->path) {
    unlink(pUdfInfo->path);
  }

  tfree(pUdfInfo->path);

  tfree(pUdfInfo->content);

  taosCloseDll(pUdfInfo->handle);
}

// todo refactor
void*  tscDestroyUdfArrayList(SArray* pUdfList) {
  if (pUdfList == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(pUdfList);
  for (int32_t i = 0; i < size; i++) {
    SUdfInfo* udf = taosArrayGet(pUdfList, i);
    freeUdfInfo(udf);
  }

  taosArrayDestroy(&pUdfList);
  return NULL;
}



void* tscDestroyBlockHashTable(SSqlObj *pSql, SHashObj* pBlockHashTable, bool removeMeta) {
  if (pBlockHashTable == NULL) {
    return NULL;
  }

  STableDataBlocks** p = taosHashIterate(pBlockHashTable, NULL);
  while(p) {
    tscDestroyDataBlock(pSql, *p, removeMeta);
    p = taosHashIterate(pBlockHashTable, p);
  }

  taosHashCleanup(pBlockHashTable);
  return NULL;
}

int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, STableDataBlocks* pDataBlock) {
  SSqlCmd* pCmd = &pSql->cmd;
  assert(pDataBlock->pTableMeta != NULL && pDataBlock->size <= pDataBlock->nAllocSize && pDataBlock->size > sizeof(SMsgDesc));

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);

  // todo remove it later
  // set the correct table meta object, the table meta has been locked in pDataBlocks, so it must be in the cache
  if (pTableMetaInfo->pTableMeta != pDataBlock->pTableMeta) {
    tNameAssign(&pTableMetaInfo->name, &pDataBlock->tableName);

    if (pTableMetaInfo->pTableMeta != NULL) {
      tfree(pTableMetaInfo->pTableMeta);
    }

    pTableMetaInfo->pTableMeta    = tscTableMetaDup(pDataBlock->pTableMeta);
    pTableMetaInfo->tableMetaSize = tscGetTableMetaSize(pDataBlock->pTableMeta);
    pTableMetaInfo->tableMetaCapacity =  (size_t)(pTableMetaInfo->tableMetaSize);
  }

  /*
   * the format of submit message is as follows [RPC header|message body|digest]
   * the dataBlock only includes the RPC Header buffer and actual submit message body,
   * space for digest needs additional space.
   */
  int ret = tscAllocPayload(pCmd, pDataBlock->size);
  if (TSDB_CODE_SUCCESS != ret) {
    return ret;
  }

  memcpy(pCmd->payload, pDataBlock->pData, pDataBlock->size);

  //the payloadLen should be actual message body size, the old value of payloadLen is the allocated payload size
  pCmd->payloadLen = pDataBlock->size;
  assert(pCmd->allocSize >= (uint32_t)(pCmd->payloadLen));

  // NOTE: shell message size should not include SMsgDesc
  int32_t size = pCmd->payloadLen - sizeof(SMsgDesc);

  SMsgDesc* pMsgDesc        = (SMsgDesc*) pCmd->payload;
  pMsgDesc->numOfVnodes     = htonl(1);    // always for one vnode

  SSubmitMsg *pShellMsg     = (SSubmitMsg *)(pCmd->payload + sizeof(SMsgDesc));
  pShellMsg->header.vgId    = htonl(pDataBlock->pTableMeta->vgId);   // data in current block all routes to the same vgroup
  pShellMsg->header.contLen = htonl(size);                           // the length not includes the size of SMsgDesc
  pShellMsg->length         = pShellMsg->header.contLen;
  pShellMsg->numOfBlocks    = htonl(pDataBlock->numOfTables);  // the number of tables to be inserted

  tscDebug("0x%"PRIx64" submit msg built, vgId:%d numOfTables:%d", pSql->self, pDataBlock->pTableMeta->vgId, pDataBlock->numOfTables);
  return TSDB_CODE_SUCCESS;
}

SQueryInfo* tscGetQueryInfo(SSqlCmd* pCmd) {
  return pCmd->active;
}

/**
 * create the in-memory buffer for each table to keep the submitted data block
 * @param initialSize
 * @param rowSize
 * @param startOffset
 * @param name
 * @param dataBlocks
 * @return
 */
int32_t tscCreateDataBlock(size_t defaultSize, int32_t rowSize, int32_t startOffset, SName* name,
                           STableMeta* pTableMeta, STableDataBlocks** dataBlocks) {
  STableDataBlocks* dataBuf = (STableDataBlocks*)calloc(1, sizeof(STableDataBlocks));
  if (dataBuf == NULL) {
    tscError("failed to allocated memory, reason:%s", strerror(errno));
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t code = tscCreateDataBlockData(dataBuf, defaultSize, rowSize, startOffset);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(dataBuf);
    return code;
  }

  //Here we keep the tableMeta to avoid it to be remove by other threads.
  dataBuf->pTableMeta = tscTableMetaDup(pTableMeta);

  SParsedDataColInfo* pColInfo = &dataBuf->boundColumnInfo;
  SSchema* pSchema = tscGetTableSchema(dataBuf->pTableMeta);
  tscSetBoundColumnInfo(pColInfo, pSchema, dataBuf->pTableMeta->tableInfo.numOfColumns);

  dataBuf->vgId     = dataBuf->pTableMeta->vgId;

  tNameAssign(&dataBuf->tableName, name);

  assert(defaultSize > 0 && pTableMeta != NULL && dataBuf->pTableMeta != NULL);

  *dataBlocks = dataBuf;
  return TSDB_CODE_SUCCESS;
}

int32_t tscCreateDataBlockData(STableDataBlocks* dataBuf, size_t defaultSize, int32_t rowSize, int32_t startOffset) {
  assert(dataBuf != NULL);

  dataBuf->nAllocSize = (uint32_t)defaultSize;
  dataBuf->headerSize = startOffset;

  // the header size will always be the startOffset value, reserved for the subumit block header
  if (dataBuf->nAllocSize <= dataBuf->headerSize) {
    dataBuf->nAllocSize = dataBuf->headerSize * 2;
  }

  //dataBuf->pData = calloc(1, dataBuf->nAllocSize);
  dataBuf->pData = malloc(dataBuf->nAllocSize);
  if (dataBuf->pData == NULL) {
    tscError("failed to allocated memory, reason:%s", strerror(errno));
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  memset(dataBuf->pData, 0, sizeof(SSubmitBlk));

  dataBuf->ordered  = true;
  dataBuf->prevTS   = INT64_MIN;
  dataBuf->rowSize  = rowSize;
  dataBuf->size     = startOffset;
  dataBuf->tsSource = -1;

  return TSDB_CODE_SUCCESS;
}

int32_t tscGetDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize,
                                SName* name, STableMeta* pTableMeta, STableDataBlocks** dataBlocks,
                                SArray* pBlockList) {
  *dataBlocks = NULL;
  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pHashList, (const char*)&id, sizeof(id));
  if (t1 != NULL) {
    *dataBlocks = *t1;
  }

  if (*dataBlocks == NULL) {
    int32_t ret = tscCreateDataBlock((size_t)size, rowSize, startOffset, name, pTableMeta, dataBlocks);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    taosHashPut(pHashList, (const char*)&id, sizeof(int64_t), (char*)dataBlocks, POINTER_BYTES);
    if (pBlockList) {
      taosArrayPush(pBlockList, dataBlocks);
    }
  }

  return TSDB_CODE_SUCCESS;
}

// Erase the empty space reserved for binary data
static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock, SInsertStatementParam* insertParam,
                         SBlockKeyTuple* blkKeyTuple) {
  // TODO: optimize this function, handle the case while binary is not presented
  STableMeta*     pTableMeta = pTableDataBlock->pTableMeta;
  STableComInfo   tinfo = tscGetTableInfo(pTableMeta);
  SSchema*        pSchema = tscGetTableSchema(pTableMeta);

  SSubmitBlk* pBlock = pDataBlock;
  memcpy(pDataBlock, pTableDataBlock->pData, sizeof(SSubmitBlk));
  pDataBlock = (char*)pDataBlock + sizeof(SSubmitBlk);

  int32_t flen = 0;  // original total length of row

  // schema needs to be included into the submit data block
  if (insertParam->schemaAttached) {
    int32_t numOfCols = tscGetNumOfColumns(pTableDataBlock->pTableMeta);
    for(int32_t j = 0; j < numOfCols; ++j) {
      STColumn* pCol = (STColumn*) pDataBlock;
      pCol->colId = htons(pSchema[j].colId);
      pCol->type  = pSchema[j].type;
      pCol->bytes = htons(pSchema[j].bytes);
      pCol->offset = 0;

      pDataBlock = (char*)pDataBlock + sizeof(STColumn);
      flen += TYPE_BYTES[pSchema[j].type];
    }

    int32_t schemaSize = sizeof(STColumn) * numOfCols;
    pBlock->schemaLen = schemaSize;
  } else {
    if (IS_RAW_PAYLOAD(insertParam->payloadType)) {
      for (int32_t j = 0; j < tinfo.numOfColumns; ++j) {
        flen += TYPE_BYTES[pSchema[j].type];
      }
    }
    pBlock->schemaLen = 0;
  }

  char* p = pTableDataBlock->pData + sizeof(SSubmitBlk);
  pBlock->dataLen = 0;
  int32_t numOfRows = htons(pBlock->numOfRows);

  if (IS_RAW_PAYLOAD(insertParam->payloadType)) {
    for (int32_t i = 0; i < numOfRows; ++i) {
      SMemRow memRow = (SMemRow)pDataBlock;
      memRowSetType(memRow, SMEM_ROW_DATA);
      SDataRow trow = memRowDataBody(memRow);
      dataRowSetLen(trow, (uint16_t)(TD_DATA_ROW_HEAD_SIZE + flen));
      dataRowSetVersion(trow, pTableMeta->sversion);

      int toffset = 0;
      for (int32_t j = 0; j < tinfo.numOfColumns; j++) {
        tdAppendColVal(trow, p, pSchema[j].type, toffset);
        toffset += TYPE_BYTES[pSchema[j].type];
        p += pSchema[j].bytes;
      }

      pDataBlock = (char*)pDataBlock + memRowTLen(memRow);
      pBlock->dataLen += memRowTLen(memRow);
    }
  } else {
    for (int32_t i = 0; i < numOfRows; ++i) {
      char*      payload = (blkKeyTuple + i)->payloadAddr;
      TDRowTLenT rowTLen = memRowTLen(payload);
      memcpy(pDataBlock, payload, rowTLen);
      pDataBlock = POINTER_SHIFT(pDataBlock, rowTLen);
      pBlock->dataLen += rowTLen;
    }
  }

  int32_t len = pBlock->dataLen + pBlock->schemaLen;
  pBlock->dataLen = htonl(pBlock->dataLen);
  pBlock->schemaLen = htonl(pBlock->schemaLen);

  return len;
}

static int32_t getRowExpandSize(STableMeta* pTableMeta) {
  int32_t  result = TD_MEM_ROW_DATA_HEAD_SIZE;
  int32_t  columns = tscGetNumOfColumns(pTableMeta);
  SSchema* pSchema = tscGetTableSchema(pTableMeta);
  for (int32_t i = 0; i < columns; i++) {
    if (IS_VAR_DATA_TYPE((pSchema + i)->type)) {
      result += TYPE_BYTES[TSDB_DATA_TYPE_BINARY];
    }
  }
  return result;
}

static void extractTableNameList(SSqlObj *pSql, SInsertStatementParam *pInsertParam, bool freeBlockMap) {
  pInsertParam->numOfTables = (int32_t) taosHashGetSize(pInsertParam->pTableBlockHashList);
  if (pInsertParam->pTableNameList == NULL) {
    pInsertParam->pTableNameList = malloc(pInsertParam->numOfTables * POINTER_BYTES);
  }

  STableDataBlocks **p1 = taosHashIterate(pInsertParam->pTableBlockHashList, NULL);
  int32_t i = 0;
  while(p1) {
    STableDataBlocks* pBlocks = *p1;
    //tfree(pInsertParam->pTableNameList[i]);

    pInsertParam->pTableNameList[i++] = tNameDup(&pBlocks->tableName);
    p1 = taosHashIterate(pInsertParam->pTableBlockHashList, p1);
  }

  if (freeBlockMap) {
    pInsertParam->pTableBlockHashList = tscDestroyBlockHashTable(pSql, pInsertParam->pTableBlockHashList, false);
  }
}

int32_t tscMergeTableDataBlocks(SSqlObj *pSql, SInsertStatementParam *pInsertParam, bool freeBlockMap) {
  const int INSERT_HEAD_SIZE = sizeof(SMsgDesc) + sizeof(SSubmitMsg);
  int       code = 0;
  bool      isRawPayload = IS_RAW_PAYLOAD(pInsertParam->payloadType);
  void*     pVnodeDataBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  SArray*   pVnodeDataBlockList = taosArrayInit(8, POINTER_BYTES);

  STableDataBlocks** p = taosHashIterate(pInsertParam->pTableBlockHashList, NULL);

  STableDataBlocks* pOneTableBlock = *p;

  SBlockKeyInfo blkKeyInfo = {0};  // share by pOneTableBlock

  while(pOneTableBlock) {
    SSubmitBlk* pBlocks = (SSubmitBlk*) pOneTableBlock->pData;
    if (pBlocks->numOfRows > 0) {
      // the maximum expanded size in byte when a row-wise data is converted to SDataRow format
      int32_t           expandSize = isRawPayload ? getRowExpandSize(pOneTableBlock->pTableMeta) : 0;
      STableDataBlocks* dataBuf = NULL;

      int32_t ret = tscGetDataBlockFromList(pVnodeDataBlockHashList, pOneTableBlock->vgId, TSDB_PAYLOAD_SIZE,
                                  INSERT_HEAD_SIZE, 0, &pOneTableBlock->tableName, pOneTableBlock->pTableMeta, &dataBuf, pVnodeDataBlockList);
      if (ret != TSDB_CODE_SUCCESS) {
        tscError("0x%"PRIx64" failed to prepare the data block buffer for merging table data, code:%d", pInsertParam->objectId, ret);
        taosHashCleanup(pVnodeDataBlockHashList);
        tscDestroyBlockArrayList(pSql, pVnodeDataBlockList);
        tfree(blkKeyInfo.pKeyTuple);
        return ret;
      }

      int64_t destSize = dataBuf->size + pOneTableBlock->size + pBlocks->numOfRows * expandSize +
                         sizeof(STColumn) * tscGetNumOfColumns(pOneTableBlock->pTableMeta);

      if (dataBuf->nAllocSize < destSize) {
        dataBuf->nAllocSize = (uint32_t)(destSize * 1.5);

        char* tmp = realloc(dataBuf->pData, dataBuf->nAllocSize);
        if (tmp != NULL) {
          dataBuf->pData = tmp;
          //memset(dataBuf->pData + dataBuf->size, 0, dataBuf->nAllocSize - dataBuf->size);
        } else {  // failed to allocate memory, free already allocated memory and return error code
          tscError("0x%"PRIx64" failed to allocate memory for merging submit block, size:%d", pInsertParam->objectId, dataBuf->nAllocSize);

          taosHashCleanup(pVnodeDataBlockHashList);
          tscDestroyBlockArrayList(pSql, pVnodeDataBlockList);
          tfree(dataBuf->pData);
          tfree(blkKeyInfo.pKeyTuple);

          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }

      if (isRawPayload) {
        tscSortRemoveDataBlockDupRowsRaw(pOneTableBlock);
        char* ekey = (char*)pBlocks->data + pOneTableBlock->rowSize * (pBlocks->numOfRows - 1);

        tscDebug("0x%" PRIx64 " name:%s, tid:%d rows:%d sversion:%d skey:%" PRId64 ", ekey:%" PRId64,
                 pInsertParam->objectId, tNameGetTableName(&pOneTableBlock->tableName), pBlocks->tid,
                 pBlocks->numOfRows, pBlocks->sversion, GET_INT64_VAL(pBlocks->data), GET_INT64_VAL(ekey));
      } else {
        if ((code = tscSortRemoveDataBlockDupRows(pOneTableBlock, &blkKeyInfo)) != 0) {
          taosHashCleanup(pVnodeDataBlockHashList);
          tscDestroyBlockArrayList(pSql, pVnodeDataBlockList);
          tfree(dataBuf->pData);
          tfree(blkKeyInfo.pKeyTuple);
          return code;
        }
        ASSERT(blkKeyInfo.pKeyTuple != NULL && pBlocks->numOfRows > 0);

        SBlockKeyTuple* pLastKeyTuple = blkKeyInfo.pKeyTuple + pBlocks->numOfRows - 1;
        tscDebug("0x%" PRIx64 " name:%s, tid:%d rows:%d sversion:%d skey:%" PRId64 ", ekey:%" PRId64,
                 pInsertParam->objectId, tNameGetTableName(&pOneTableBlock->tableName), pBlocks->tid,
                 pBlocks->numOfRows, pBlocks->sversion, blkKeyInfo.pKeyTuple->skey, pLastKeyTuple->skey);
      }

      int32_t len = pBlocks->numOfRows *
                        (isRawPayload ? (pOneTableBlock->rowSize + expandSize) : getExtendedRowSize(pOneTableBlock)) +
                    sizeof(STColumn) * tscGetNumOfColumns(pOneTableBlock->pTableMeta);

      pBlocks->tid = htonl(pBlocks->tid);
      pBlocks->uid = htobe64(pBlocks->uid);
      pBlocks->sversion = htonl(pBlocks->sversion);
      pBlocks->numOfRows = htons(pBlocks->numOfRows);
      pBlocks->schemaLen = 0;

      // erase the empty space reserved for binary data
      int32_t finalLen = trimDataBlock(dataBuf->pData + dataBuf->size, pOneTableBlock, pInsertParam, blkKeyInfo.pKeyTuple);
      assert(finalLen <= len);

      dataBuf->size += (finalLen + sizeof(SSubmitBlk));
      assert(dataBuf->size <= dataBuf->nAllocSize);

      // the length does not include the SSubmitBlk structure
      pBlocks->dataLen = htonl(finalLen);
      dataBuf->numOfTables += 1;

      pBlocks->numOfRows = 0;
    }else {
      tscDebug("0x%"PRIx64" table %s data block is empty", pInsertParam->objectId, pOneTableBlock->tableName.tname);
    }

    p = taosHashIterate(pInsertParam->pTableBlockHashList, p);
    if (p == NULL) {
      break;
    }

    pOneTableBlock = *p;
  }

  extractTableNameList(pSql, pInsertParam, freeBlockMap);

  // free the table data blocks;
  pInsertParam->pDataBlocks = pVnodeDataBlockList;
  taosHashCleanup(pVnodeDataBlockHashList);
  tfree(blkKeyInfo.pKeyTuple);

  return TSDB_CODE_SUCCESS;
}

// TODO: all subqueries should be freed correctly before close this connection.
void tscCloseTscObj(void *param) {
  STscObj *pObj = param;

  pObj->signature = NULL;
  taosTmrStopA(&(pObj->pTimer));

  tfree(pObj->tscCorMgmtEpSet);
  tscReleaseRpc(pObj->pRpcObj);
  pthread_mutex_destroy(&pObj->mutex);
  tscReleaseClusterInfo(pObj->clusterId);

  tfree(pObj);
}

bool tscIsInsertData(char* sqlstr) {
  int32_t index = 0;

  do {
    SStrToken t0 = tStrGetToken(sqlstr, &index, false);
    if (t0.type != TK_LP) {
      return t0.type == TK_INSERT || t0.type == TK_IMPORT;
    }
  } while (1);
}

int32_t tscAllocPayloadFast(SSqlCmd *pCmd, size_t size) {
  if (pCmd->payload == NULL) {
    assert(pCmd->allocSize == 0);

    pCmd->payload = malloc(size);
    pCmd->allocSize = (uint32_t) size;
  } else if (pCmd->allocSize < size) {
    char* tmp = realloc(pCmd->payload, size);
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    pCmd->payload = tmp;
    pCmd->allocSize = (uint32_t) size;
  }

  assert(pCmd->allocSize >= size);
  return TSDB_CODE_SUCCESS;
}

int32_t tscAllocPayload(SSqlCmd* pCmd, int size) {
  assert(size > 0);

  int32_t code = tscAllocPayloadFast(pCmd, (size_t) size);
  if (code == TSDB_CODE_SUCCESS) {
    memset(pCmd->payload, 0, pCmd->allocSize);
  }

  return code;
}

TAOS_FIELD tscCreateField(int8_t type, const char* name, int16_t bytes) {
  TAOS_FIELD f = { .type = type, .bytes = bytes, };
  tstrncpy(f.name, name, sizeof(f.name));
  return f;
}

int32_t tscGetFirstInvisibleFieldPos(SQueryInfo* pQueryInfo) {
  if (pQueryInfo->fieldsInfo.numOfOutput <= 0 || pQueryInfo->fieldsInfo.internalField == NULL) {
    return 0;
  }

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SInternalField* pField = taosArrayGet(pQueryInfo->fieldsInfo.internalField, i);
    if (!pField->visible) {
      return i;
    }
  }

  return pQueryInfo->fieldsInfo.numOfOutput;
}


SInternalField* tscFieldInfoAppend(SFieldInfo* pFieldInfo, TAOS_FIELD* pField) {
  assert(pFieldInfo != NULL);
  pFieldInfo->numOfOutput++;

  struct SInternalField info = { .pExpr = NULL, .visible = true };

  info.field = *pField;
  return taosArrayPush(pFieldInfo->internalField, &info);
}

SInternalField* tscFieldInfoInsert(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* field) {
  pFieldInfo->numOfOutput++;
  struct SInternalField info = { .pExpr = NULL, .visible = true };

  info.field = *field;
  return taosArrayInsert(pFieldInfo->internalField, index, &info);
}

void tscFieldInfoUpdateOffset(SQueryInfo* pQueryInfo) {
  int32_t offset = 0;
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* p = taosArrayGetP(pQueryInfo->exprList, i);

    p->base.offset = offset;
    offset += p->base.resBytes;
  }
}

SInternalField* tscFieldInfoGetInternalField(SFieldInfo* pFieldInfo, int32_t index) {
  assert(index < pFieldInfo->numOfOutput);
  return TARRAY_GET_ELEM(pFieldInfo->internalField, index);
}

TAOS_FIELD* tscFieldInfoGetField(SFieldInfo* pFieldInfo, int32_t index) {
  assert(index < pFieldInfo->numOfOutput);
  return &((SInternalField*)TARRAY_GET_ELEM(pFieldInfo->internalField, index))->field;
}

int32_t tscFieldInfoGetOffset(SQueryInfo* pQueryInfo, int32_t index) {
  SInternalField* pInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, index);
  assert(pInfo != NULL && pInfo->pExpr->pExpr == NULL);

  return pInfo->pExpr->base.offset;
}

int32_t tscFieldInfoCompare(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2, int32_t *diffSize) {
  assert(pFieldInfo1 != NULL && pFieldInfo2 != NULL);

  if (pFieldInfo1->numOfOutput != pFieldInfo2->numOfOutput) {
    return pFieldInfo1->numOfOutput - pFieldInfo2->numOfOutput;
  }

  for (int32_t i = 0; i < pFieldInfo1->numOfOutput; ++i) {
    TAOS_FIELD* pField1 = tscFieldInfoGetField((SFieldInfo*) pFieldInfo1, i);
    TAOS_FIELD* pField2 = tscFieldInfoGetField((SFieldInfo*) pFieldInfo2, i);

    if (pField1->type != pField2->type ||
        strcasecmp(pField1->name, pField2->name) != 0) {
      return 1;
    }

    if (pField1->bytes != pField2->bytes) {
      *diffSize = 1;

      if (pField2->bytes > pField1->bytes) {
        assert(IS_VAR_DATA_TYPE(pField1->type));
        pField1->bytes = pField2->bytes;
      }
    }
  }

  return 0;
}

int32_t tscFieldInfoSetSize(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2) {
  assert(pFieldInfo1 != NULL && pFieldInfo2 != NULL);

  for (int32_t i = 0; i < pFieldInfo1->numOfOutput; ++i) {
    TAOS_FIELD* pField1 = tscFieldInfoGetField((SFieldInfo*) pFieldInfo1, i);
    TAOS_FIELD* pField2 = tscFieldInfoGetField((SFieldInfo*) pFieldInfo2, i);

    pField2->bytes = pField1->bytes;
  }

  return 0;
}


int32_t tscGetResRowLength(SArray* pExprList) {
  size_t num = taosArrayGetSize(pExprList);
  if (num == 0) {
    return 0;
  }

  int32_t size = 0;
  for(int32_t i = 0; i < num; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprList, i);
    size += pExpr->base.resBytes;
  }

  return size;
}

static void destroyFilterInfo(SColumnFilterList* pFilterList) {
  if (pFilterList->filterInfo == NULL) {
    pFilterList->numOfFilters = 0;
    return;
  }

  for(int32_t i = 0; i < pFilterList->numOfFilters; ++i) {
    if (pFilterList->filterInfo[i].filterstr) {
      tfree(pFilterList->filterInfo[i].pz);
    }
  }

  tfree(pFilterList->filterInfo);
  pFilterList->numOfFilters = 0;
}

void* sqlExprDestroy(SExprInfo* pExpr) {
  if (pExpr == NULL) {
    return NULL;
  }

  SSqlExpr* p = &pExpr->base;
  for(int32_t i = 0; i < tListLen(p->param); ++i) {
    tVariantDestroy(&p->param[i]);
  }

  if (p->flist.numOfFilters > 0) {
    tfree(p->flist.filterInfo);
  }

  if (pExpr->pExpr != NULL) {
    tExprTreeDestroy(pExpr->pExpr, NULL);
  }

  tfree(pExpr);
  return NULL;
}

void tscFieldInfoClear(SFieldInfo* pFieldInfo) {
  if (pFieldInfo == NULL) {
    return;
  }

  if (pFieldInfo->internalField != NULL) {
    size_t num = taosArrayGetSize(pFieldInfo->internalField);
    for (int32_t i = 0; i < num; ++i) {
      SInternalField* pfield = taosArrayGet(pFieldInfo->internalField, i);
      if (pfield->pExpr != NULL && pfield->pExpr->pExpr != NULL) {
        sqlExprDestroy(pfield->pExpr);
      }
    }
  }

  taosArrayDestroy(&pFieldInfo->internalField);
  tfree(pFieldInfo->final);

  memset(pFieldInfo, 0, sizeof(SFieldInfo));
}

void tscFieldInfoCopy(SFieldInfo* pFieldInfo, const SFieldInfo* pSrc, const SArray* pExprList) {
  assert(pFieldInfo != NULL && pSrc != NULL && pExprList != NULL);
  pFieldInfo->numOfOutput = pSrc->numOfOutput;

  if (pSrc->final != NULL) {
    pFieldInfo->final = calloc(pSrc->numOfOutput, sizeof(TAOS_FIELD));
    memcpy(pFieldInfo->final, pSrc->final, sizeof(TAOS_FIELD) * pSrc->numOfOutput);
  }

  if (pSrc->internalField != NULL) {
    size_t num = taosArrayGetSize(pSrc->internalField);
    size_t numOfExpr = taosArrayGetSize(pExprList);

    for (int32_t i = 0; i < num; ++i) {
      SInternalField* pfield = taosArrayGet(pSrc->internalField, i);

      SInternalField p = {.visible = pfield->visible, .field = pfield->field};

      bool found = false;
      int32_t resColId = pfield->pExpr->base.resColId;
      for(int32_t j = 0; j < numOfExpr; ++j) {
        SExprInfo* pExpr = taosArrayGetP(pExprList, j);
        if (pExpr->base.resColId == resColId) {
          p.pExpr = pExpr;
          found = true;
          break;
        }
      }

      if (!found) {
        assert(pfield->pExpr->pExpr != NULL);
        p.pExpr = calloc(1, sizeof(SExprInfo));
        tscExprAssign(p.pExpr, pfield->pExpr);
      }

      taosArrayPush(pFieldInfo->internalField, &p);
    }
  }
}


SExprInfo* tscExprCreate(STableMetaInfo* pTableMetaInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                         int16_t size, int16_t resColId, int32_t interSize, int32_t colType) {
  SExprInfo* pExpr = calloc(1, sizeof(SExprInfo));
  if (pExpr == NULL) {
    return NULL;
  }

  SSqlExpr* p = &pExpr->base;
  p->functionId = functionId;

  // set the correct columnIndex index
  if (pColIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    SSchema* s = tGetTbnameColumnSchema();
    p->colInfo.colId = TSDB_TBNAME_COLUMN_INDEX;
    p->colBytes = s->bytes;
    p->colType  = s->type;
  } else if (TSDB_COL_IS_TSWIN_COL(pColIndex->columnIndex)) {
    SSchema* s = tGetTimeWindowColumnSchema(pColIndex->columnIndex);
    p->colInfo.colId = s->colId;
    p->colBytes = s->bytes;
    p->colType  = s->type;
  } else if (pColIndex->columnIndex <= TSDB_UD_COLUMN_INDEX) {
    p->colInfo.colId = pColIndex->columnIndex;
    p->colBytes = size;
    p->colType = type;
  } else if (functionId == TSDB_FUNC_BLKINFO) {
    p->colInfo.colId = pColIndex->columnIndex;
    p->colBytes = TSDB_MAX_BINARY_LEN;
    p->colType  = TSDB_DATA_TYPE_BINARY;
  } else {
    int32_t len = tListLen(p->colInfo.name);
    if (TSDB_COL_IS_TAG(colType)) {
      SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
      p->colInfo.colId = pSchema[pColIndex->columnIndex].colId;
      p->colBytes = pSchema[pColIndex->columnIndex].bytes;
      p->colType = pSchema[pColIndex->columnIndex].type;
      snprintf(p->colInfo.name, len, "%s.%s", pTableMetaInfo->aliasName, pSchema[pColIndex->columnIndex].name);
    } else if (pTableMetaInfo->pTableMeta != NULL) {
      // in handling select database/version/server_status(), the pTableMeta is NULL
      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pColIndex->columnIndex);
      p->colInfo.colId = pSchema->colId;
      p->colBytes = pSchema->bytes;
      p->colType  = pSchema->type;
      snprintf(p->colInfo.name, len, "%s.%s", pTableMetaInfo->aliasName, pSchema->name);
    }
  }

  p->colInfo.flag     = colType;
  p->colInfo.colIndex = pColIndex->columnIndex;

  p->resType       = type;
  p->resBytes      = size;
  p->resColId      = resColId;
  p->interBytes    = interSize;

  if (pTableMetaInfo->pTableMeta) {
    p->uid = pTableMetaInfo->pTableMeta->id.uid;
  }

  return pExpr;
}

SExprInfo* tscExprInsert(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t resColId, int32_t interSize, bool isTagCol) {
  int32_t num = (int32_t)taosArrayGetSize(pQueryInfo->exprList);
  if (index == num) {
    return tscExprAppend(pQueryInfo, functionId, pColIndex, type, size, resColId, interSize, isTagCol);
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pColIndex->tableIndex);
  SExprInfo* pExpr = tscExprCreate(pTableMetaInfo, functionId, pColIndex, type, size, resColId, interSize, isTagCol);
  taosArrayInsert(pQueryInfo->exprList, index, &pExpr);
  return pExpr;
}

SExprInfo* tscExprAppend(SQueryInfo* pQueryInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t resColId, int32_t interSize, bool isTagCol) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pColIndex->tableIndex);
  SExprInfo* pExpr = tscExprCreate(pTableMetaInfo, functionId, pColIndex, type, size, resColId, interSize, isTagCol);
  taosArrayPush(pQueryInfo->exprList, &pExpr);
  return pExpr;
}

SExprInfo* tscExprUpdate(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, int16_t srcColumnIndex,
                           int16_t type, int32_t size) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SExprInfo* pExpr = tscExprGet(pQueryInfo, index);
  if (pExpr == NULL) {
    return NULL;
  }

  SSqlExpr* pse = &pExpr->base;
  pse->functionId = functionId;

  pse->colInfo.colIndex = srcColumnIndex;
  pse->colInfo.colId = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, srcColumnIndex)->colId;

  pse->resType = type;
  pse->resBytes = size;

  return pExpr;
}

bool tscMultiRoundQuery(SQueryInfo* pQueryInfo, int32_t index) {
  if (!UTIL_TABLE_IS_SUPER_TABLE(pQueryInfo->pTableMetaInfo[index])) {
    return false;
  }

  int32_t numOfExprs = (int32_t) tscNumOfExprs(pQueryInfo);
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_STDDEV_DST) {
      return true;
    }
  }

  return false;
}

size_t tscNumOfExprs(SQueryInfo* pQueryInfo) {
  return taosArrayGetSize(pQueryInfo->exprList);
}

int32_t tscExprTopBottomIndex(SQueryInfo* pQueryInfo){
  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr == NULL)
      continue;
    if (pExpr->base.functionId == TSDB_FUNC_TOP 
      || pExpr->base.functionId == TSDB_FUNC_BOTTOM 
      || pExpr->base.functionId == TSDB_FUNC_SAMPLE 
      || pExpr->base.functionId == TSDB_FUNC_UNIQUE 
      || pExpr->base.functionId == TSDB_FUNC_TAIL) {
      return i;
    }
  }
  return -1;
}

// todo REFACTOR
void tscExprAddParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes) {
  assert (pExpr != NULL || argument != NULL || bytes != 0);

  // set parameter value
  // transfer to tVariant from byte data/no ascii data
  tVariantCreateFromBinary(&pExpr->param[pExpr->numOfParams], argument, bytes, type);
  pExpr->numOfParams += 1;

  assert(pExpr->numOfParams <= 3);
}

SExprInfo* tscExprGet(SQueryInfo* pQueryInfo, int32_t index) {
  return taosArrayGetP(pQueryInfo->exprList, index);
}

/*
 * NOTE: Does not release SExprInfo here.
 */
void tscExprDestroy(SArray* pExprInfo) {
  size_t size = taosArrayGetSize(pExprInfo);

  for(int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprInfo, i);
    sqlExprDestroy(pExpr);
  }

  taosArrayDestroy(&pExprInfo);
}

int32_t tscExprCopy(SArray* dst, const SArray* src, uint64_t uid, bool deepcopy) {
  assert(src != NULL && dst != NULL);

  size_t size = taosArrayGetSize(src);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(src, i);

    if (pExpr->base.uid == uid) {
      if (deepcopy) {
        SExprInfo* p1 = calloc(1, sizeof(SExprInfo));
        tscExprAssign(p1, pExpr);

        taosArrayPush(dst, &p1);
      } else {
        taosArrayPush(dst, &pExpr);
      }
    }
  }

  return 0;
}

int32_t tscExprCopyAll(SArray* dst, const SArray* src, bool deepcopy) {
  assert(src != NULL && dst != NULL);

  size_t size = taosArrayGetSize(src);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(src, i);

    if (deepcopy) {
      SExprInfo* p1 = calloc(1, sizeof(SExprInfo));
      tscExprAssign(p1, pExpr);

      taosArrayPush(dst, &p1);
    } else {
      taosArrayPush(dst, &pExpr);
    }
  }

  return 0;
}

// ignore the tbname columnIndex to be inserted into source list
int32_t tscColumnExists(SArray* pColumnList, int32_t columnId, uint64_t uid) {
  size_t numOfCols = taosArrayGetSize(pColumnList);

  int32_t i = 0;
  while (i < numOfCols) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if ((pCol->info.colId != columnId) || (pCol->tableUid != uid)) {
      ++i;
      continue;
    } else {
      break;
    }
  }

  if (i >= numOfCols || numOfCols == 0) {
    return -1;
  }

  return i;
}

void tscExprAssign(SExprInfo* dst, const SExprInfo* src) {
  assert(dst != NULL && src != NULL);

  *dst = *src;

  if (src->base.flist.numOfFilters > 0) {
    dst->base.flist.filterInfo = calloc(src->base.flist.numOfFilters, sizeof(SColumnFilterInfo));
    memcpy(dst->base.flist.filterInfo, src->base.flist.filterInfo, sizeof(SColumnFilterInfo) * src->base.flist.numOfFilters);
  }

  dst->pExpr = exprdup(src->pExpr);

  memset(dst->base.param, 0, sizeof(tVariant) * tListLen(dst->base.param));
  for (int32_t j = 0; j < src->base.numOfParams; ++j) {
    tVariantAssign(&dst->base.param[j], &src->base.param[j]);
  }
}

SColumn* tscColumnListInsert(SArray* pColumnList, int32_t columnIndex, uint64_t uid, SSchema* pSchema) {
  // ignore the tbname columnIndex to be inserted into source list
  if (columnIndex < 0) {
    return NULL;
  }

  size_t numOfCols = taosArrayGetSize(pColumnList);

  int32_t i = 0;
  while (i < numOfCols) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if (pCol->columnIndex < columnIndex) {
      i++;
    } else if (pCol->tableUid < uid) {
      i++;
    } else {
      break;
    }
  }

  if (i >= numOfCols || numOfCols == 0) {
    SColumn* b = calloc(1, sizeof(SColumn));
    if (b == NULL) {
      return NULL;
    }

    b->columnIndex = columnIndex;
    b->tableUid    = uid;
    b->info.colId  = pSchema->colId;
    b->info.bytes  = pSchema->bytes;
    b->info.type   = pSchema->type;

    taosArrayInsert(pColumnList, i, &b);
  } else {
    SColumn* pCol = taosArrayGetP(pColumnList, i);

    if (i < numOfCols && (pCol->columnIndex > columnIndex || pCol->tableUid != uid)) {
      SColumn* b = calloc(1, sizeof(SColumn));
      if (b == NULL) {
        return NULL;
      }

      b->columnIndex = columnIndex;
      b->tableUid    = uid;
      b->info.colId = pSchema->colId;
      b->info.bytes = pSchema->bytes;
      b->info.type  = pSchema->type;

      taosArrayInsert(pColumnList, i, &b);
    }
  }

  return taosArrayGetP(pColumnList, i);
}



SColumn* tscColumnClone(const SColumn* src) {
  assert(src != NULL);

  SColumn* dst = calloc(1, sizeof(SColumn));
  if (dst == NULL) {
    return NULL;
  }

  tscColumnCopy(dst, src);
  return dst;
}

static void tscColumnDestroy(SColumn* pCol) {
  destroyFilterInfo(&pCol->info.flist);
  free(pCol);
}

void tscColumnCopy(SColumn* pDest, const SColumn* pSrc) {
  destroyFilterInfo(&pDest->info.flist);

  pDest->columnIndex       = pSrc->columnIndex;
  pDest->tableUid          = pSrc->tableUid;
  pDest->info.flist.numOfFilters = pSrc->info.flist.numOfFilters;
  pDest->info.flist.filterInfo   = tFilterInfoDup(pSrc->info.flist.filterInfo, pSrc->info.flist.numOfFilters);
  pDest->info.type        = pSrc->info.type;
  pDest->info.colId        = pSrc->info.colId;
  pDest->info.bytes      = pSrc->info.bytes;
}

void tscColumnListCopy(SArray* dst, const SArray* src, uint64_t tableUid) {
  assert(src != NULL && dst != NULL);

  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);

    if (pCol->tableUid == tableUid) {
      SColumn* p = tscColumnClone(pCol);
      taosArrayPush(dst, &p);
    }
  }
}

void tscColumnListCopyAll(SArray* dst, const SArray* src) {
  assert(src != NULL && dst != NULL);

  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);
    SColumn* p = tscColumnClone(pCol);
    taosArrayPush(dst, &p);
  }
}


void tscColumnListDestroy(SArray* pColumnList) {
  if (pColumnList == NULL) {
    return;
  }

  size_t num = taosArrayGetSize(pColumnList);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    tscColumnDestroy(pCol);
  }

  taosArrayDestroy(&pColumnList);
}

/*
 * 1. normal name, not a keyword or number
 * 2. name with quote
 * 3. string with only one delimiter '.'.
 *
 * only_one_part
 * 'only_one_part'
 * first_part.second_part
 * first_part.'second_part'
 * 'first_part'.second_part
 * 'first_part'.'second_part'
 * 'first_part.second_part'
 *
 */
static int32_t validateQuoteToken(SStrToken* pToken, bool escapeEnabled, bool *dbIncluded) {
  if(pToken->z[0] != TS_BACKQUOTE_CHAR) pToken->n = stringProcess(pToken->z, pToken->n);

  int32_t k = tGetToken(pToken->z, &pToken->type);

  if (pToken->type == TK_STRING) {
    return tscValidateName(pToken, escapeEnabled, dbIncluded);
  }

  if (k != pToken->n || pToken->type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tscValidateName(SStrToken* pToken, bool escapeEnabled, bool *dbIncluded) {
  if (pToken == NULL || pToken->z == NULL
     || (pToken->type != TK_STRING && pToken->type != TK_ID)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if ((!escapeEnabled) && pToken->type == TK_ID) {
    if (pToken->z[0] == TS_BACKQUOTE_CHAR) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  char* sep = NULL;

  if (escapeEnabled) {
    sep = tableNameGetPosition(pToken, TS_PATH_DELIMITER[0]);
  } else {
    sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  }

  if (sep == NULL) {  // single part
    if (dbIncluded) *dbIncluded = false;

    if (pToken->type == TK_STRING) {

      if(pToken->z[0] != TS_BACKQUOTE_CHAR) pToken->n = stringProcess(pToken->z, pToken->n);
      // tscStrToLower(pToken->z, pToken->n);
      strntolower(pToken->z, pToken->z, pToken->n);
      //pToken->n = (uint32_t)strtrim(pToken->z);

      int len = tGetToken(pToken->z, &pToken->type);

      // single token, validate it
      if (len == pToken->n) {
        return validateQuoteToken(pToken, escapeEnabled, NULL);
      } else {
        sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
        if (sep == NULL) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        if (dbIncluded)  *dbIncluded = true;

        return tscValidateName(pToken, escapeEnabled, NULL);
      }
    } else if (pToken->type == TK_ID) {
      if(pToken->z[0] == TS_BACKQUOTE_CHAR) pToken->n = stringProcess(pToken->z, pToken->n);

      if (pToken->n == 0) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      return TSDB_CODE_SUCCESS;
    } else {
      if (isNumber(pToken)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;
    bool firstPartQuote = false;

    if (dbIncluded) *dbIncluded = true;

    if (pToken->type == TK_SPACE) {
      pToken->n = (uint32_t)strtrim(pToken->z);
    }

    pToken->n = tGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (pToken->type != TK_STRING && pToken->type != TK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (pToken->type == TK_STRING) {
      if (validateQuoteToken(pToken, escapeEnabled, NULL) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      } else {
        // tscStrToLower(pToken->z,pToken->n);
        strntolower(pToken->z, pToken->z, pToken->n);
        firstPartQuote = true;
      }
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = (uint32_t)(oldLen - (sep - pStr) - 1);
    int32_t len = tGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || (pToken->type != TK_STRING && pToken->type != TK_ID)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (pToken->type == TK_STRING) {
      if (validateQuoteToken(pToken, escapeEnabled, NULL) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      } else {
        // tscStrToLower(pToken->z,pToken->n);
        strntolower(pToken->z, pToken->z, pToken->n);
      }
    }

    if (escapeEnabled && pToken->type == TK_ID) {
      if(pToken->z[0] == TS_BACKQUOTE_CHAR) pToken->n = stringProcess(pToken->z, pToken->n);
    }

    // re-build the whole name string
    if (!firstPartQuote) {
      // first part do not have quote do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      uint32_t offset = (uint32_t)(pToken->z - (pStr + firstPartLen + 1));
      memset(pToken->z + pToken->n - offset, ' ', offset);
    }
    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;
  }

  return TSDB_CODE_SUCCESS;
}

void tscIncStreamExecutionCount(void* pStream) {
  if (pStream == NULL) {
    return;
  }

  SSqlStream* ps = (SSqlStream*)pStream;
  ps->num += 1;
}

bool tscValidateColumnId(STableMetaInfo* pTableMetaInfo, int32_t colId) {
  if (pTableMetaInfo->pTableMeta == NULL) {
    return false;
  }

  if (colId == TSDB_TBNAME_COLUMN_INDEX || TSDB_COL_IS_TSWIN_COL(colId) ||
      colId <= TSDB_UD_COLUMN_INDEX) {
    return true;
  }

  SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);

  int32_t  numOfTotal = tinfo.numOfTags + tinfo.numOfColumns;

  for (int32_t i = 0; i < numOfTotal; ++i) {
    if (pSchema[i].colId == colId) {
      return true;
    }
  }

  return false;
}

int32_t tscTagCondCopy(STagCond* dest, const STagCond* src) {
  memset(dest, 0, sizeof(STagCond));

  dest->joinInfo.hasJoin = src->joinInfo.hasJoin;

  for (int32_t i = 0; i < TSDB_MAX_JOIN_TABLE_NUM; ++i) {
    if (src->joinInfo.joinTables[i]) {
      dest->joinInfo.joinTables[i] = calloc(1, sizeof(SJoinNode));

      memcpy(dest->joinInfo.joinTables[i], src->joinInfo.joinTables[i], sizeof(SJoinNode));

      if (src->joinInfo.joinTables[i]->tsJoin) {
        dest->joinInfo.joinTables[i]->tsJoin = taosArrayDup(src->joinInfo.joinTables[i]->tsJoin);
      }

      if (src->joinInfo.joinTables[i]->tagJoin) {
        dest->joinInfo.joinTables[i]->tagJoin = taosArrayDup(src->joinInfo.joinTables[i]->tagJoin);
      }
    }
  }

  if (src->pCond == NULL) {
    return 0;
  }

  size_t s = taosArrayGetSize(src->pCond);
  dest->pCond = taosArrayInit(s, sizeof(SCond));

  for (int32_t i = 0; i < s; ++i) {
    SCond* pCond = taosArrayGet(src->pCond, i);

    SCond c = {0};
    c.len = pCond->len;
    c.uid = pCond->uid;

    if (pCond->len > 0) {
      assert(pCond->cond != NULL);
      c.cond = malloc(c.len);
      if (c.cond == NULL) {
        return -1;
      }

      memcpy(c.cond, pCond->cond, c.len);
    }

    taosArrayPush(dest->pCond, &c);
  }

  return 0;
}

int32_t tscColCondCopy(SArray** dest, const SArray* src, uint64_t uid, int16_t tidx) {
  if (src == NULL) {
    return 0;
  }

  size_t s = taosArrayGetSize(src);
  *dest = taosArrayInit(s, sizeof(SCond));

  for (int32_t i = 0; i < s; ++i) {
    STblCond* pCond = taosArrayGet(src, i);
    STblCond c = {0};

    if (tidx > 0) {
      if (!(pCond->uid == uid && pCond->idx == tidx)) {
        continue;
      }

      c.idx = 0;
    } else {
      c.idx = pCond->idx;
    }

    c.len = pCond->len;
    c.uid = pCond->uid;

    if (pCond->len > 0) {
      assert(pCond->cond != NULL);
      c.cond = malloc(c.len);
      if (c.cond == NULL) {
        return -1;
      }

      memcpy(c.cond, pCond->cond, c.len);
    }

    taosArrayPush(*dest, &c);
  }

  return 0;
}

void tscColCondRelease(SArray** pCond) {
  if (*pCond == NULL) {
    return;
  }

  size_t s = taosArrayGetSize(*pCond);
  for (int32_t i = 0; i < s; ++i) {
    STblCond* p = taosArrayGet(*pCond, i);
    tfree(p->cond);
  }

  taosArrayDestroy(pCond);

  *pCond = NULL;
}


void tscTagCondRelease(STagCond* pTagCond) {
  if (pTagCond->pCond != NULL) {
    size_t s = taosArrayGetSize(pTagCond->pCond);
    for (int32_t i = 0; i < s; ++i) {
      SCond* p = taosArrayGet(pTagCond->pCond, i);
      tfree(p->cond);
    }

    taosArrayDestroy(&pTagCond->pCond);
  }

  for (int32_t i = 0; i < TSDB_MAX_JOIN_TABLE_NUM; ++i) {
    SJoinNode *node = pTagCond->joinInfo.joinTables[i];
    if (node == NULL) {
      continue;
    }

    if (node->tsJoin != NULL) {
      taosArrayDestroy(&node->tsJoin);
    }

    if (node->tagJoin != NULL) {
      taosArrayDestroy(&node->tagJoin);
    }

    tfree(node);
  }

  memset(pTagCond, 0, sizeof(STagCond));
}

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SQueryInfo* pQueryInfo) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSchema*        pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    pColInfo[i].functionId = pExpr->base.functionId;

    if (TSDB_COL_IS_TAG(pExpr->base.colInfo.flag)) {
      SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);

      int16_t index = pExpr->base.colInfo.colIndex;
      pColInfo[i].type = (index != -1) ? pTagSchema[index].type : TSDB_DATA_TYPE_BINARY;
    } else {
      pColInfo[i].type = pSchema[pExpr->base.colInfo.colIndex].type;
    }
  }
}

/*
 * the following four kinds of SqlObj should not be freed
 * 1. SqlObj for stream computing
 * 2. main SqlObj
 * 3. heartbeat SqlObj
 * 4. SqlObj for subscription
 *
 * If res code is error and SqlObj does not belong to above types, it should be
 * automatically freed for async query, ignoring that connection should be kept.
 *
 * If connection need to be recycled, the SqlObj also should be freed.
 */
bool tscShouldBeFreed(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return false;
  }

  STscObj* pTscObj = pSql->pTscObj;
  if (pSql->pStream != NULL || pTscObj->hbrid == pSql->self || pSql->pSubscription != NULL) {
    return false;
  }

  // only the table meta and super table vgroup query will free resource automatically
  int32_t command = pSql->cmd.command;
  if (command == TSDB_SQL_META || command == TSDB_SQL_STABLEVGROUP) {
    return true;
  }

  return false;
}

/**
 *
 * @param pCmd
 * @param clauseIndex denote the index of the union sub clause, usually are 0, if no union query exists.
 * @param tableIndex  denote the table index for join query, where more than one table exists
 * @return
 */
STableMetaInfo* tscGetTableMetaInfoFromCmd(SSqlCmd* pCmd, int32_t tableIndex) {
  assert(pCmd != NULL);
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  if(pQueryInfo == NULL) {
    return NULL;
  }
  return tscGetMetaInfo(pQueryInfo, tableIndex);
}

STableMetaInfo* tscGetMetaInfo(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  assert(pQueryInfo != NULL);

  if (pQueryInfo->pTableMetaInfo == NULL) {
    assert(pQueryInfo->numOfTables == 0);
    return NULL;
  }

  assert(tableIndex >= 0 && tableIndex <= pQueryInfo->numOfTables && pQueryInfo->pTableMetaInfo != NULL);

  return pQueryInfo->pTableMetaInfo[tableIndex];
}

SQueryInfo* tscGetQueryInfoS(SSqlCmd* pCmd) {
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  int32_t ret = TSDB_CODE_SUCCESS;

  while ((pQueryInfo) == NULL) {
    if ((ret = tscAddQueryInfo(pCmd)) != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return NULL;
    }

    pQueryInfo = tscGetQueryInfo(pCmd);
  }

  return pQueryInfo;
}

STableMetaInfo* tscGetTableMetaInfoByUid(SQueryInfo* pQueryInfo, uint64_t uid, int32_t* index) {
  int32_t k = -1;

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    if (pQueryInfo->pTableMetaInfo[i]->pTableMeta->id.uid == uid) {
      k = i;
      break;
    }
  }

  if (index != NULL) {
    *index = k;
  }

  assert(k != -1);
  return tscGetMetaInfo(pQueryInfo, k);
}

// todo refactor
void tscInitQueryInfo(SQueryInfo* pQueryInfo) {
  assert(pQueryInfo->fieldsInfo.internalField == NULL);
  pQueryInfo->fieldsInfo.internalField = taosArrayInit(4, sizeof(SInternalField));

  assert(pQueryInfo->exprList == NULL);

  pQueryInfo->exprList       = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->colList        = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->udColumnId     = TSDB_UD_COLUMN_INDEX;
  pQueryInfo->limit.limit    = -1;
  pQueryInfo->limit.offset   = 0;

  pQueryInfo->slimit.limit   = -1;
  pQueryInfo->slimit.offset  = 0;
  pQueryInfo->pUpstream      = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->window         = TSWINDOW_INITIALIZER;
  pQueryInfo->multigroupResult = true;
}

int32_t tscAddQueryInfo(SSqlCmd* pCmd) {
  assert(pCmd != NULL);
  SQueryInfo* pQueryInfo = calloc(1, sizeof(SQueryInfo));

  if (pQueryInfo == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscInitQueryInfo(pQueryInfo);
  pQueryInfo->msg = pCmd->payload;  // pointer to the parent error message buffer

  if (pCmd->pQueryInfo == NULL) {
    pCmd->pQueryInfo = pQueryInfo;
  } else {
    SQueryInfo* p = pCmd->pQueryInfo;
    while(p->sibling != NULL) {
      p = p->sibling;
    }

    p->sibling = pQueryInfo;
  }

  pCmd->active = pQueryInfo;
  return TSDB_CODE_SUCCESS;
}

static void freeQueryInfoImpl(SQueryInfo* pQueryInfo) {
  tscTagCondRelease(&pQueryInfo->tagCond);
  tscColCondRelease(&pQueryInfo->colCond);
  tscFieldInfoClear(&pQueryInfo->fieldsInfo);

  tscExprDestroy(pQueryInfo->exprList);
  pQueryInfo->exprList = NULL;

  if (pQueryInfo->exprList1 != NULL) {
    tscExprDestroy(pQueryInfo->exprList1);
    pQueryInfo->exprList1 = NULL;
  }

  tscColumnListDestroy(pQueryInfo->colList);
  pQueryInfo->colList = NULL;

  if (pQueryInfo->groupbyExpr.columnInfo != NULL) {
    taosArrayDestroy(&pQueryInfo->groupbyExpr.columnInfo);
    pQueryInfo->groupbyExpr.columnInfo = NULL;
    pQueryInfo->groupbyExpr.numOfGroupCols = 0;
  }

  pQueryInfo->tsBuf = tsBufDestroy(pQueryInfo->tsBuf);
  pQueryInfo->fillType = 0;

  tfree(pQueryInfo->fillVal);
  pQueryInfo->fillType = 0;
  tfree(pQueryInfo->buf);

  taosArrayDestroy(&pQueryInfo->pUpstream);
  pQueryInfo->pUpstream = NULL;
  pQueryInfo->bufLen = 0;
}

void tscClearSubqueryInfo(SSqlCmd* pCmd) {
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  while (pQueryInfo != NULL) {
    SQueryInfo* p = pQueryInfo->sibling;
    freeQueryInfoImpl(pQueryInfo);
    pQueryInfo = p;
  }
}

int32_t tscQueryInfoCopy(SQueryInfo* pQueryInfo, const SQueryInfo* pSrc) {
  assert(pQueryInfo != NULL && pSrc != NULL);
  int32_t code = TSDB_CODE_SUCCESS;

  memcpy(&pQueryInfo->interval, &pSrc->interval, sizeof(pQueryInfo->interval));

  pQueryInfo->command        = pSrc->command;
  pQueryInfo->type           = pSrc->type;
  pQueryInfo->window         = pSrc->window;
  pQueryInfo->limit          = pSrc->limit;
  pQueryInfo->slimit         = pSrc->slimit;
  pQueryInfo->order          = pSrc->order;
  pQueryInfo->vgroupLimit    = pSrc->vgroupLimit;
  pQueryInfo->tsBuf          = NULL;
  pQueryInfo->fillType       = pSrc->fillType;
  pQueryInfo->fillVal        = NULL;
  pQueryInfo->numOfFillVal   = 0;;
  pQueryInfo->clauseLimit    = pSrc->clauseLimit;
  pQueryInfo->prjOffset      = pSrc->prjOffset;
  pQueryInfo->numOfTables    = 0;
  pQueryInfo->range          = pSrc->range;
  pQueryInfo->sessionWindow  = pSrc->sessionWindow;
  pQueryInfo->pTableMetaInfo = NULL;
  pQueryInfo->multigroupResult = pSrc->multigroupResult;
  pQueryInfo->stateWindow    = pSrc->stateWindow;

  pQueryInfo->bufLen         = pSrc->bufLen;
  pQueryInfo->orderProjectQuery = pSrc->orderProjectQuery;
  pQueryInfo->arithmeticOnAgg   = pSrc->arithmeticOnAgg;
  pQueryInfo->buf            = malloc(pSrc->bufLen);
  if (pQueryInfo->buf == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pSrc->bufLen > 0) {
    memcpy(pQueryInfo->buf, pSrc->buf, pSrc->bufLen);
  }

  pQueryInfo->groupbyExpr = pSrc->groupbyExpr;
  if (pSrc->groupbyExpr.columnInfo != NULL) {
    pQueryInfo->groupbyExpr.columnInfo = taosArrayDup(pSrc->groupbyExpr.columnInfo);
    if (pQueryInfo->groupbyExpr.columnInfo == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  }

  if (tscTagCondCopy(&pQueryInfo->tagCond, &pSrc->tagCond) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (tscColCondCopy(&pQueryInfo->colCond, pSrc->colCond, 0, -1) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pSrc->fillType != TSDB_FILL_NONE) {
    pQueryInfo->fillVal = calloc(1, pSrc->fieldsInfo.numOfOutput * sizeof(int64_t));
    if (pQueryInfo->fillVal == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
    pQueryInfo->numOfFillVal = pSrc->fieldsInfo.numOfOutput;

    memcpy(pQueryInfo->fillVal, pSrc->fillVal, pSrc->fieldsInfo.numOfOutput * sizeof(int64_t));
  }

  if (tscExprCopyAll(pQueryInfo->exprList, pSrc->exprList, true) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pQueryInfo->arithmeticOnAgg) {
    pQueryInfo->exprList1 = taosArrayInit(4, POINTER_BYTES);
    if (tscExprCopyAll(pQueryInfo->exprList1, pSrc->exprList1, true) != 0) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  }

  tscColumnListCopyAll(pQueryInfo->colList, pSrc->colList);
  tscFieldInfoCopy(&pQueryInfo->fieldsInfo, &pSrc->fieldsInfo, pQueryInfo->exprList);

  for(int32_t i = 0; i < pSrc->numOfTables; ++i) {
    STableMetaInfo* p1 = tscGetMetaInfo((SQueryInfo*) pSrc, i);

    STableMeta* pMeta = tscTableMetaDup(p1->pTableMeta);
    if (pMeta == NULL) {
      // todo handle the error
    }

    tscAddTableMetaInfo(pQueryInfo, &p1->name, pMeta, p1->vgroupList, p1->tagColList, p1->pVgroupTables);
  }

  SArray *pUdfInfo = NULL;
  if (pSrc->pUdfInfo) {
    pUdfInfo = taosArrayDup(pSrc->pUdfInfo);
  }

  pQueryInfo->pUdfInfo = pUdfInfo;
  pQueryInfo->udfCopy = true;

  _error:
  return code;
}

void tscFreeVgroupTableInfo(SArray* pVgroupTables) {
  if (pVgroupTables == NULL) {
    return;
  }

  size_t num = taosArrayGetSize(pVgroupTables);
  for (size_t i = 0; i < num; i++) {
    SVgroupTableInfo* pInfo = taosArrayGet(pVgroupTables, i);
#if 0
    for(int32_t j = 0; j < pInfo->vgInfo.numOfEps; ++j) {
      tfree(pInfo->vgInfo.epAddr[j].fqdn);
    }
#endif
    taosArrayDestroy(&pInfo->itemList);
  }

  taosArrayDestroy(&pVgroupTables);
}

void tscRemoveVgroupTableGroup(SArray* pVgroupTable, int32_t index) {
  assert(pVgroupTable != NULL && index >= 0);

  size_t size = taosArrayGetSize(pVgroupTable);
  assert(size > index);

  SVgroupTableInfo* pInfo = taosArrayGet(pVgroupTable, index);
//  for(int32_t j = 0; j < pInfo->vgInfo.numOfEps; ++j) {
//    tfree(pInfo->vgInfo.epAddr[j].fqdn);
//  }

  taosArrayDestroy(&pInfo->itemList);
  taosArrayRemove(pVgroupTable, index);
}

void tscVgroupTableCopy(SVgroupTableInfo* info, SVgroupTableInfo* pInfo) {
  memset(info, 0, sizeof(SVgroupTableInfo));

  info->vgInfo = pInfo->vgInfo;

#if 0
  for(int32_t j = 0; j < pInfo->vgInfo.numOfEps; ++j) {
    info->vgInfo.epAddr[j].fqdn = strdup(pInfo->vgInfo.epAddr[j].fqdn);
  }
#endif

  if (pInfo->itemList) {
    info->itemList = taosArrayDup(pInfo->itemList);
  }
}

SArray* tscVgroupTableInfoDup(SArray* pVgroupTables) {
  if (pVgroupTables == NULL) {
    return NULL;
  }

  size_t num = taosArrayGetSize(pVgroupTables);
  SArray* pa = taosArrayInit(num, sizeof(SVgroupTableInfo));

  SVgroupTableInfo info;
  for (size_t i = 0; i < num; i++) {
    SVgroupTableInfo* pInfo = taosArrayGet(pVgroupTables, i);
    tscVgroupTableCopy(&info, pInfo);

    taosArrayPush(pa, &info);
  }

  return pa;
}

void clearAllTableMetaInfo(SQueryInfo* pQueryInfo, bool removeMeta, uint64_t id) {
  for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (removeMeta) {
      tscRemoveCachedTableMeta(pTableMetaInfo, id);
    }

    tscFreeVgroupTableInfo(pTableMetaInfo->pVgroupTables);
    tscClearTableMetaInfo(pTableMetaInfo);
  }

  tfree(pQueryInfo->pTableMetaInfo);
}

STableMetaInfo* tscAddTableMetaInfo(SQueryInfo* pQueryInfo, SName* name, STableMeta* pTableMeta,
                                    SVgroupsInfo* vgroupList, SArray* pTagCols, SArray* pVgroupTables) {
  void* tmp = realloc(pQueryInfo->pTableMetaInfo, (pQueryInfo->numOfTables + 1) * POINTER_BYTES);
  if (tmp == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryInfo->pTableMetaInfo = tmp;
  STableMetaInfo* pTableMetaInfo = calloc(1, sizeof(STableMetaInfo));

  if (pTableMetaInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables] = pTableMetaInfo;

  if (name != NULL) {
    tNameAssign(&pTableMetaInfo->name, name);
  }

  pTableMetaInfo->pTableMeta = pTableMeta;
  pTableMetaInfo->tableMetaSize = (pTableMetaInfo->pTableMeta == NULL)? 0:tscGetTableMetaSize(pTableMeta);

  pTableMetaInfo->tableMetaCapacity = (size_t)(pTableMetaInfo->tableMetaSize);

  if (vgroupList != NULL) {
    pTableMetaInfo->vgroupList = tscVgroupInfoClone(vgroupList);
  }

  // TODO handle malloc failure
  pTableMetaInfo->tagColList = taosArrayInit(4, POINTER_BYTES);
  if (pTableMetaInfo->tagColList == NULL) {
    return NULL;
  }

  if (pTagCols != NULL && pTableMetaInfo->pTableMeta != NULL) {
    tscColumnListCopy(pTableMetaInfo->tagColList, pTagCols, pTableMetaInfo->pTableMeta->id.uid);
  }

  pTableMetaInfo->pVgroupTables = tscVgroupTableInfoDup(pVgroupTables);

  pQueryInfo->numOfTables += 1;
  return pTableMetaInfo;
}

STableMetaInfo* tscAddEmptyMetaInfo(SQueryInfo* pQueryInfo) {
  return tscAddTableMetaInfo(pQueryInfo, NULL, NULL, NULL, NULL, NULL);
}

void tscClearTableMetaInfo(STableMetaInfo* pTableMetaInfo) {
  if (pTableMetaInfo == NULL) {
    return;
  }

  tfree(pTableMetaInfo->pTableMeta);
  pTableMetaInfo->vgroupList = tscVgroupInfoClear(pTableMetaInfo->vgroupList);

  tscColumnListDestroy(pTableMetaInfo->tagColList);
  pTableMetaInfo->tagColList = NULL;

  free(pTableMetaInfo);
}

void tscResetForNextRetrieve(SSqlRes* pRes) {
  if (pRes == NULL) {
    return;
  }

  pRes->row = 0;
  pRes->numOfRows = 0;
  pRes->dataConverted = false;
}

void tscInitResForMerge(SSqlRes* pRes) {
  pRes->qId = 1;      // hack to pass the safety check in fetch_row function
  pRes->rspType = 0;  // used as a flag to denote if taos_retrieved() has been called yet
  tscResetForNextRetrieve(pRes);

  assert(pRes->pMerger != NULL);
  pRes->data = pRes->pMerger->buf;
}

void registerSqlObj(SSqlObj* pSql) {
  taosAcquireRef(tscRefId, pSql->pTscObj->rid);
  pSql->self = taosAddRef(tscObjRef, pSql);

  int32_t num   = atomic_add_fetch_32(&pSql->pTscObj->numOfObj, 1);
  int32_t total = atomic_add_fetch_32(&tscNumOfObj, 1);
  tscDebug("0x%"PRIx64" new SqlObj from %p, total in tscObj:%d, total:%d", pSql->self, pSql->pTscObj, num, total);
}

SSqlObj* createSimpleSubObj(SSqlObj* pSql, __async_cb_func_t fp, void* param, int32_t cmd) {
  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("0x%"PRIx64" new subquery failed, tableIndex:%d", pSql->self, 0);
    return NULL;
  }

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->rootObj = pSql->rootObj;

  SSqlCmd* pCmd = &pNew->cmd;
  pCmd->command = cmd;
  tsem_init(&pNew->rspSem, 0 ,0);

  if (tscAddQueryInfo(pCmd) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return NULL;
  }

  pNew->fp      = fp;
  pNew->fetchFp = fp;
  pNew->param   = param;
  pNew->sqlstr  = NULL;
  pNew->maxRetry = TSDB_MAX_REPLICA;

  SQueryInfo* pQueryInfo = tscGetQueryInfoS(pCmd);
  STableMetaInfo* pMasterTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0);

  tscAddTableMetaInfo(pQueryInfo, &pMasterTableMetaInfo->name, NULL, NULL, NULL, NULL);
  registerSqlObj(pNew);

  return pNew;
}

static void doSetSqlExprAndResultFieldInfo(SQueryInfo* pNewQueryInfo, int64_t uid) {
  int32_t numOfOutput = (int32_t)tscNumOfExprs(pNewQueryInfo);
  if (numOfOutput == 0) {
    return;
  }

  // set the field info in pNewQueryInfo object according to sqlExpr information
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = tscExprGet(pNewQueryInfo, i);

    TAOS_FIELD f = tscCreateField((int8_t) pExpr->base.resType, pExpr->base.aliasName, pExpr->base.resBytes);
    SInternalField* pInfo1 = tscFieldInfoAppend(&pNewQueryInfo->fieldsInfo, &f);
    pInfo1->pExpr = pExpr;
  }

  // update the pSqlExpr pointer in SInternalField according the field name
  // make sure the pSqlExpr point to the correct SqlExpr in pNewQueryInfo, not SqlExpr in pQueryInfo
  for (int32_t f = 0; f < pNewQueryInfo->fieldsInfo.numOfOutput; ++f) {
    TAOS_FIELD* field = tscFieldInfoGetField(&pNewQueryInfo->fieldsInfo, f);

    bool matched = false;
    for (int32_t k1 = 0; k1 < numOfOutput; ++k1) {
      SExprInfo* pExpr1 = tscExprGet(pNewQueryInfo, k1);

      if (strcmp(field->name, pExpr1->base.aliasName) == 0) {  // establish link according to the result field name
        SInternalField* pInfo = tscFieldInfoGetInternalField(&pNewQueryInfo->fieldsInfo, f);
        pInfo->pExpr = pExpr1;

        matched = true;
        break;
      }
    }

    assert(matched);
    (void)matched;
  }

  tscFieldInfoUpdateOffset(pNewQueryInfo);
}

SSqlObj* createSubqueryObj(SSqlObj* pSql, int16_t tableIndex, __async_cb_func_t fp, void* param, int32_t cmd, SSqlObj* pPrevSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("0x%"PRIx64" new subquery failed, tableIndex:%d", pSql->self, tableIndex);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[tableIndex];

  pNew->pTscObj   = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->sqlstr    = strdup(pSql->sqlstr);
  pNew->rootObj   = pSql->rootObj;
  tsem_init(&pNew->rspSem, 0, 0);

  SSqlCmd* pnCmd  = &pNew->cmd;
  memcpy(pnCmd, pCmd, sizeof(SSqlCmd));

  pnCmd->command = cmd;
  pnCmd->payload = NULL;
  pnCmd->allocSize = 0;
  pnCmd->pTableMetaMap = NULL;

  pnCmd->pQueryInfo  = NULL;
  pnCmd->insertParam.pDataBlocks = NULL;

  pnCmd->insertParam.numOfTables = 0;
  pnCmd->insertParam.pTableNameList = NULL;
  pnCmd->insertParam.pTableBlockHashList = NULL;
  pnCmd->insertParam.objectId = pNew->self;

  memset(&pnCmd->insertParam.tagData, 0, sizeof(STagData));

  if (tscAddQueryInfo(pnCmd) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  SQueryInfo* pNewQueryInfo = tscGetQueryInfo(pnCmd);

  if (pQueryInfo->pUdfInfo) {
    pNewQueryInfo->pUdfInfo = taosArrayDup(pQueryInfo->pUdfInfo);
    pNewQueryInfo->udfCopy = true;
  }

  pNewQueryInfo->command = pQueryInfo->command;
  pnCmd->active = pNewQueryInfo;

  memcpy(&pNewQueryInfo->interval, &pQueryInfo->interval, sizeof(pNewQueryInfo->interval));
  pNewQueryInfo->type         = pQueryInfo->type;
  pNewQueryInfo->window       = pQueryInfo->window;
  pNewQueryInfo->range        = pQueryInfo->range;
  pNewQueryInfo->limit        = pQueryInfo->limit;
  pNewQueryInfo->slimit       = pQueryInfo->slimit;
  pNewQueryInfo->order        = pQueryInfo->order;
  pNewQueryInfo->tsBuf        = NULL;
  pNewQueryInfo->fillType     = pQueryInfo->fillType;
  pNewQueryInfo->fillVal      = NULL;
  pNewQueryInfo->clauseLimit  = pQueryInfo->clauseLimit;
  pNewQueryInfo->prjOffset    = pQueryInfo->prjOffset;
  pNewQueryInfo->numOfFillVal = 0;
  pNewQueryInfo->numOfTables  = 0;
  pNewQueryInfo->pTableMetaInfo = NULL;
  pNewQueryInfo->bufLen       = pQueryInfo->bufLen;
  pNewQueryInfo->vgroupLimit  = pQueryInfo->vgroupLimit;
  pNewQueryInfo->distinct     =  pQueryInfo->distinct;
  pNewQueryInfo->multigroupResult = pQueryInfo->multigroupResult;

  pNewQueryInfo->buf          = malloc(pQueryInfo->bufLen);
  if (pNewQueryInfo->buf == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pQueryInfo->bufLen > 0) {
    memcpy(pNewQueryInfo->buf, pQueryInfo->buf, pQueryInfo->bufLen);
  }

  pNewQueryInfo->groupbyExpr = pQueryInfo->groupbyExpr;
  if (pQueryInfo->groupbyExpr.columnInfo != NULL) {
    pNewQueryInfo->groupbyExpr.columnInfo = taosArrayDup(pQueryInfo->groupbyExpr.columnInfo);
    if (pNewQueryInfo->groupbyExpr.columnInfo == NULL) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  }

  if (tscTagCondCopy(&pNewQueryInfo->tagCond, &pQueryInfo->tagCond) != 0) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (tscColCondCopy(&pNewQueryInfo->colCond, pQueryInfo->colCond, pTableMetaInfo->pTableMeta->id.uid, tableIndex) != 0) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
    //just make memory memory sanitizer happy
    //refactor later
    pNewQueryInfo->fillVal = calloc(1, pQueryInfo->fieldsInfo.numOfOutput * sizeof(int64_t));
    if (pNewQueryInfo->fillVal == NULL) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }

    pNewQueryInfo->numOfFillVal = pQueryInfo->fieldsInfo.numOfOutput;
    memcpy(pNewQueryInfo->fillVal, pQueryInfo->fillVal, pQueryInfo->fieldsInfo.numOfOutput * sizeof(int64_t));
  }

  if (tscAllocPayload(pnCmd, TSDB_DEFAULT_PAYLOAD_SIZE) != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" new subquery failed, tableIndex:%d, vgroupIndex:%d", pSql->self, tableIndex, pTableMetaInfo->vgroupIndex);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  uint64_t uid = pTableMetaInfo->pTableMeta->id.uid;
  tscColumnListCopy(pNewQueryInfo->colList, pQueryInfo->colList, uid);

  // set the correct query type
  if (pPrevSql != NULL) {
    SQueryInfo* pPrevQueryInfo = tscGetQueryInfo(&pPrevSql->cmd);
    pNewQueryInfo->type = pPrevQueryInfo->type;
  } else {
    TSDB_QUERY_SET_TYPE(pNewQueryInfo->type, TSDB_QUERY_TYPE_SUBQUERY);// it must be the subquery
  }

  if (tscExprCopy(pNewQueryInfo->exprList, pQueryInfo->exprList, uid, true) != 0) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  doSetSqlExprAndResultFieldInfo(pNewQueryInfo, uid);

  pNew->fp      = fp;
  pNew->fetchFp = fp;
  pNew->param   = param;
  pNew->maxRetry = TSDB_MAX_REPLICA;

  STableMetaInfo* pFinalInfo = NULL;

  if (pPrevSql == NULL) {
    STableMeta* pTableMeta = tscTableMetaDup(pTableMetaInfo->pTableMeta);
    assert(pTableMeta != NULL);

    pFinalInfo = tscAddTableMetaInfo(pNewQueryInfo, &pTableMetaInfo->name, pTableMeta, pTableMetaInfo->vgroupList,
                                     pTableMetaInfo->tagColList, pTableMetaInfo->pVgroupTables);
  } else {  // transfer the ownership of pTableMeta to the newly create sql object.
    STableMetaInfo* pPrevInfo = tscGetTableMetaInfoFromCmd(&pPrevSql->cmd, 0);
    if (pPrevInfo->pTableMeta && pPrevInfo->pTableMeta->tableType < 0) {
      terrno = TSDB_CODE_TSC_APP_ERROR;
      goto _error;
    }

    STableMeta*  pPrevTableMeta = tscTableMetaDup(pPrevInfo->pTableMeta);
    SVgroupsInfo* pVgroupsInfo = pPrevInfo->vgroupList;
    pFinalInfo = tscAddTableMetaInfo(pNewQueryInfo, &pTableMetaInfo->name, pPrevTableMeta, pVgroupsInfo,
                                     pTableMetaInfo->tagColList, pTableMetaInfo->pVgroupTables);
  }

  // this case cannot be happened
  if (pFinalInfo->pTableMeta == NULL) {
    tscError("0x%"PRIx64" new subquery failed since no tableMeta, name:%s", pSql->self, tNameGetTableName(&pTableMetaInfo->name));

    if (pPrevSql != NULL) { // pass the previous error to client
      assert(pPrevSql->res.code != TSDB_CODE_SUCCESS);
      terrno = pPrevSql->res.code;
    } else {
      terrno = TSDB_CODE_TSC_APP_ERROR;
    }

    goto _error;
  }

  assert(pNewQueryInfo->numOfTables == 1);

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    assert(pFinalInfo->vgroupList != NULL);
  }

  registerSqlObj(pNew);

  if (cmd == TSDB_SQL_SELECT) {
    size_t size = taosArrayGetSize(pNewQueryInfo->colList);

    tscDebug("0x%"PRIx64" new subquery:0x%"PRIx64", tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%" PRIzu ", colList:%" PRIzu ","
        "fieldInfo:%d, name:%s, qrang:%" PRId64 " - %" PRId64 " order:%d, limit:%" PRId64,
        pSql->self, pNew->self, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscNumOfExprs(pNewQueryInfo),
        size, pNewQueryInfo->fieldsInfo.numOfOutput, tNameGetTableName(&pFinalInfo->name), pNewQueryInfo->window.skey,
        pNewQueryInfo->window.ekey, pNewQueryInfo->order.order, pNewQueryInfo->limit.limit);

    tscPrintSelNodeList(pNew, 0);
  } else {
    tscDebug("0x%"PRIx64" new sub insertion: %p, vnodeIdx:%d", pSql->self, pNew, pTableMetaInfo->vgroupIndex);
  }

  return pNew;

_error:
  tscFreeSqlObj(pNew);
  return NULL;
}

void doExecuteQuery(SSqlObj* pSql, SQueryInfo* pQueryInfo) {
  uint16_t type = pQueryInfo->type;
  if (QUERY_IS_JOIN_QUERY(type) && !TSDB_QUERY_HAS_TYPE(type, TSDB_QUERY_TYPE_SUBQUERY)) {
    tscHandleMasterJoinQuery(pSql);
  } else if (tscMultiRoundQuery(pQueryInfo, 0) && pQueryInfo->round == 0) {
    tscHandleFirstRoundStableQuery(pSql);                // todo lock?
  } else if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {  // super table query
    tscLockByThread(&pSql->squeryLock);
    tscHandleMasterSTableQuery(pSql);
    tscUnlockByThread(&pSql->squeryLock);
  } else if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT)) {
    if (TSDB_QUERY_HAS_TYPE(pSql->cmd.insertParam.insertType, TSDB_QUERY_TYPE_FILE_INSERT)) {
      tscImportDataFromFile(pSql);
    } else {
      tscHandleMultivnodeInsert(pSql);
    }
  } else if (pSql->cmd.command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
  } else { // send request to server directly
    tscBuildAndSendRequest(pSql, pQueryInfo);
  }
}

void doRetrieveSubqueryData(SSchedMsg *pMsg) {
  SSqlObj* pSql = (SSqlObj*) pMsg->ahandle;
  if (pSql == NULL || pSql->signature != pSql) {
    tscDebug("%p SqlObj is freed, not add into queue async res", pMsg->ahandle);
    return;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  handleDownstreamOperator(pSql->pSubs, pSql->subState.numOfSub, pQueryInfo, pSql);

  pSql->res.qId = -1;
  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, pSql->res.numOfRows);
  } else {
    tscAsyncResultOnError(pSql);
  }
}

// NOTE: the blocking query can not be executed in the rpc message handler thread
static void tscSubqueryRetrieveCallback(void* param, TAOS_RES* tres, int code) {
  // handle the pDownStream process
  SRetrieveSupport* ps = param;
  SSqlObj* pParentSql = ps->pParentSql;
  SSqlObj* pSql = tres;

  int32_t index = ps->subqueryIndex;
  bool ret = subAndCheckDone(pSql, pParentSql, index);

  // TODO refactor
  tfree(ps);
  pSql->param = NULL;

  if (!ret) {
    tscDebug("0x%"PRIx64" sub:0x%"PRIx64" orderOfSub:%d completed, not all subquery finished", pParentSql->self, pSql->self, index);
    return;
  }

  pParentSql->cmd.active = pParentSql->cmd.pQueryInfo;
  pParentSql->res.qId = -1;
  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pParentSql->param, pParentSql, pParentSql->res.numOfRows);
  } else {
    pParentSql->res.code = pSql->res.code;
    tscAsyncResultOnError(pParentSql);
  }
}

static void tscSubqueryCompleteCallback(void* param, TAOS_RES* tres, int code) {
  SSqlObj* pSql = tres;
  SRetrieveSupport* ps = param;

  if (pSql->res.code != TSDB_CODE_SUCCESS) {
    SSqlObj* pParentSql = ps->pParentSql;

    int32_t index = ps->subqueryIndex;
    bool ret = subAndCheckDone(pSql, pParentSql, index);

    tscFreeRetrieveSup(&pSql->param);

    if (!ret) {
      tscDebug("0x%"PRIx64" sub:0x%"PRIx64" orderOfSub:%d completed, not all subquery finished", pParentSql->self, pSql->self, index);
      return;
    }

    // todo refactor
    tscDebug("0x%"PRIx64" all subquery response received, retry", pParentSql->self);
    SSqlObj *rootObj = pParentSql->rootObj;

    if (code && !((code == TSDB_CODE_TDB_INVALID_TABLE_ID || code == TSDB_CODE_VND_INVALID_VGROUP_ID) && rootObj->retry < rootObj->maxRetry)) {
      pParentSql->res.code = code;

      tscAsyncResultOnError(pParentSql);
      return;
    }

    tscFreeSubobj(pParentSql);
    tfree(pParentSql->pSubs);

    tscFreeSubobj(rootObj);
    tfree(rootObj->pSubs);

    rootObj->res.code = TSDB_CODE_SUCCESS;
    rootObj->retry++;

    tscDebug("0x%"PRIx64" retry parse sql and send query, prev error: %s, retry:%d", rootObj->self,
             tstrerror(code), rootObj->retry);


    tscResetSqlCmd(&rootObj->cmd, true, rootObj->self);

    code = tsParseSql(rootObj, true);
    if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
      return;
    }

    if (code != TSDB_CODE_SUCCESS) {
      rootObj->res.code = code;
      tscAsyncResultOnError(rootObj);
      return;
    }

    SQueryInfo *pQueryInfo = tscGetQueryInfo(&pParentSql->cmd);
    executeQuery(pParentSql, pQueryInfo);
    return;
  }

  if (pSql->cmd.command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    SSqlObj* pParentSql = ps->pParentSql;

    pParentSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;

    (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
    return;
  }


  taos_fetch_rows_a(tres, tscSubqueryRetrieveCallback, param);
}

int32_t doInitSubState(SSqlObj* pSql, int32_t numOfSubqueries) {
  assert(pSql->subState.numOfSub == 0 && pSql->pSubs == NULL && pSql->subState.states == NULL);
  pSql->subState.numOfSub = numOfSubqueries;

  pSql->pSubs = calloc(pSql->subState.numOfSub, POINTER_BYTES);
  pSql->subState.states = calloc(pSql->subState.numOfSub, sizeof(int8_t));

  int32_t code = pthread_mutex_init(&pSql->subState.mutex, NULL);
  if (pSql->pSubs == NULL || pSql->subState.states == NULL || code != 0) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

// do execute the query according to the query execution plan
void executeQuery(SSqlObj* pSql, SQueryInfo* pQueryInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numOfInit = 0;

  if (pSql->cmd.command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    (*pSql->fp)(pSql->param, pSql, 0);
    return;
  } else if (pSql->cmd.command == TSDB_SQL_DELETE_DATA) {
    code = executeDelete(pSql, pQueryInfo);
    if (code != TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, 0);
    }
    return ;
  } 

  if (pSql->cmd.command == TSDB_SQL_SELECT) {
    tscAddIntoSqlList(pSql);
  }

  if (taosArrayGetSize(pQueryInfo->pUpstream) > 0) {  // nest query. do execute it firstly
    code = doInitSubState(pSql, (int32_t) taosArrayGetSize(pQueryInfo->pUpstream));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SQueryInfo* pSub = taosArrayGetP(pQueryInfo->pUpstream, i);

      pSql->cmd.active = pSub;
      pSql->cmd.command = TSDB_SQL_SELECT;

      SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
      if (pNew == NULL) {
        code = TSDB_CODE_TSC_OUT_OF_MEMORY;
        goto _error;
      }

      pNew->pTscObj   = pSql->pTscObj;
      pNew->signature = pNew;
      pNew->sqlstr    = strdup(pSql->sqlstr);
      pNew->fp        = tscSubqueryCompleteCallback;
      pNew->fetchFp   = tscSubqueryCompleteCallback;
      pNew->maxRetry  = pSql->maxRetry;
      pNew->rootObj   = pSql->rootObj;

      pNew->cmd.resColumnId = TSDB_RES_COL_ID;

      tsem_init(&pNew->rspSem, 0, 0);

      SRetrieveSupport* ps = calloc(1, sizeof(SRetrieveSupport));  // todo use object id
      if (ps == NULL) {
        tscFreeSqlObj(pNew);
        goto _error;
      }

      ps->pParentSql = pSql;
      ps->subqueryIndex = i;

      pNew->param = ps;
      pSql->pSubs[i] = pNew;

      SSqlCmd* pCmd = &pNew->cmd;
      pCmd->command = TSDB_SQL_SELECT;
      if ((code = tscAddQueryInfo(pCmd)) != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      SQueryInfo* pNewQueryInfo = tscGetQueryInfo(pCmd);
      tscQueryInfoCopy(pNewQueryInfo, pSub);

      TSDB_QUERY_SET_TYPE(pNewQueryInfo->type, TSDB_QUERY_TYPE_NEST_SUBQUERY);
      numOfInit++;
    }

    for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SSqlObj* psub = pSql->pSubs[i];
      registerSqlObj(psub);

      // create sub query to handle the sub query.
      SQueryInfo* pq = tscGetQueryInfo(&psub->cmd);
      STableMetaInfo* pSubMeta = tscGetMetaInfo(pq, 0);
      if (UTIL_TABLE_IS_SUPER_TABLE(pSubMeta) &&
          pq->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
        psub->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      }
      executeQuery(psub, pq);
    }

    return;
  } else if (hasMoreClauseToTry(pSql)) {
    if (pthread_mutex_init(&pSql->subState.mutex, NULL) != 0) {
      goto _error;
    }
  }

  pSql->cmd.active = pQueryInfo;
  doExecuteQuery(pSql, pQueryInfo);
  return;

  _error:
  for(int32_t i = 0; i < numOfInit; ++i) {
    SSqlObj* p = pSql->pSubs[i];
    tscFreeSqlObj(p);
  }

  pSql->res.code = code;
  pSql->subState.numOfSub = 0;   // not initialized sub query object will not be freed
  tfree(pSql->subState.states);
  tfree(pSql->pSubs);
  tscAsyncResultOnError(pSql);
}

int16_t tscGetJoinTagColIdByUid(STagCond* pTagCond, uint64_t uid) {
  int32_t i = 0;
  while (i < TSDB_MAX_JOIN_TABLE_NUM) {
    SJoinNode* node = pTagCond->joinInfo.joinTables[i];
    if (node && node->uid == uid) {
      return node->tagColId;
    }

    i++;
  }

  assert(0);
  return -1;
}


int16_t tscGetTagColIndexById(STableMeta* pTableMeta, int16_t colId) {
  int32_t numOfTags = tscGetNumOfTags(pTableMeta);

  SSchema* pSchema = tscGetTableTagSchema(pTableMeta);
  for(int32_t i = 0; i < numOfTags; ++i) {
    if (pSchema[i].colId == colId) {
      return i;
    }
  }

  // can not reach here
  assert(0);
  return INT16_MIN;
}

bool tscIsUpdateQuery(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  return ((pCmd->command >= TSDB_SQL_INSERT && pCmd->command <= TSDB_SQL_DROP_DNODE) || TSDB_SQL_RESET_CACHE == pCmd->command || TSDB_SQL_USE_DB == pCmd->command);
}

bool tscIsDeleteQuery(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return false;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  return pCmd->command == TSDB_SQL_DELETE_DATA;
}

char* tscGetSqlStr(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return NULL;
  }

  return pSql->sqlstr;
}

bool tscIsQueryWithLimit(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return false;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo* pqi = tscGetQueryInfo(pCmd);
  while(pqi != NULL) {
    if (pqi->limit.limit > 0) {
      return true;
    }

    pqi = pqi->sibling;
  }

  return false;
}


int32_t tscSQLSyntaxErrMsg(char* msg, const char* additionalInfo,  const char* sql) {
  const char* msgFormat1 = "syntax error near \'%s\'";
  const char* msgFormat2 = "syntax error near \'%s\' (%s)";
  const char* msgFormat3 = "%s";

  const char* prefix = "syntax error";
  const int32_t BACKWARD_CHAR_STEP = 0;

  if (sql == NULL) {
    assert(additionalInfo != NULL);
    sprintf(msg, msgFormat1, additionalInfo);
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    sprintf(msg, msgFormat2, buf, additionalInfo);
  } else {
    const char* msgFormat = (0 == strncmp(sql, prefix, strlen(prefix))) ? msgFormat3 : msgFormat1;
    sprintf(msg, msgFormat, buf);
  }

  return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
}

int32_t tscInvalidOperationMsg(char* msg, const char* additionalInfo, const char* sql) {
  const char* msgFormat1 = "invalid operation: %s";
  const char* msgFormat2 = "invalid operation: \'%s\' (%s)";
  const char* msgFormat3 = "invalid operation: \'%s\'";

  const int32_t BACKWARD_CHAR_STEP = 0;

  if (sql == NULL) {
    assert(additionalInfo != NULL);
    sprintf(msg, msgFormat1, additionalInfo);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    sprintf(msg, msgFormat2, buf, additionalInfo);
  } else {
    sprintf(msg, msgFormat3, buf);  // no additional information for invalid sql error
  }

  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t tscErrorMsgWithCode(int32_t code, char* dstBuffer, const char* errMsg, const char* sql) {
  const char* msgFormat1 = "%s:%s";
  const char* msgFormat2 = "%s:\'%s\' (%s)";
  const char* msgFormat3 = "%s:\'%s\'";

  const int32_t BACKWARD_CHAR_STEP = 0;

  if (sql == NULL) {
    assert(errMsg != NULL);
    sprintf(dstBuffer, msgFormat1, tstrerror(code), errMsg);
    return code;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);

  if (errMsg != NULL) {
    sprintf(dstBuffer, msgFormat2, tstrerror(code), buf, errMsg);
  } else {
    sprintf(dstBuffer, msgFormat3, tstrerror(code), buf);  // no additional information for invalid sql error
  }

  return code;
}

bool tscHasReachLimitation(SQueryInfo* pQueryInfo, SSqlRes* pRes) {
  assert(pQueryInfo != NULL && pQueryInfo->clauseLimit != 0);
  return (pQueryInfo->clauseLimit > 0 && pRes->numOfClauseTotal >= pQueryInfo->clauseLimit);
}

char* tscGetErrorMsgPayload(SSqlCmd* pCmd) { return pCmd->payload; }

int32_t tscGetErrorMsgLength(SSqlCmd* pCmd) { return (int32_t)strlen(pCmd->payload); }

/**
 *  If current vnode query does not return results anymore (pRes->numOfRows == 0), try the next vnode if exists,
 *  while multi-vnode super table projection query and the result does not reach the limitation.
 */
bool hasMoreVnodesToTry(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  if (pCmd->command != TSDB_SQL_FETCH) {
    return false;
  }

  assert(pRes->completed);
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // for normal table, no need to try any more if results are all retrieved from one vnode
  if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) || (pTableMetaInfo->vgroupList == NULL)) {
    return false;
  }

  int32_t numOfVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
  if (pTableMetaInfo->pVgroupTables != NULL) {
    numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
  }

  return tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) &&
         (!tscHasReachLimitation(pQueryInfo, pRes)) && (pTableMetaInfo->vgroupIndex < numOfVgroups - 1);
}

bool hasMoreClauseToTry(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  return pCmd->active->sibling != NULL;
}

void tscTryQueryNextVnode(SSqlObj* pSql, __async_cb_func_t fp) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  /*
   * no result returned from the current virtual node anymore, try the next vnode if exists
   * if case of: multi-vnode super table projection query
   */
  assert(pRes->numOfRows == 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && !tscHasReachLimitation(pQueryInfo, pRes));
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
  if (++pTableMetaInfo->vgroupIndex < totalVgroups) {
    tscDebug("0x%"PRIx64" results from vgroup index:%d completed, try next:%d. total vgroups:%d. current numOfRes:%" PRId64, pSql->self,
             pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups, pRes->numOfClauseTotal);

    /*
     * update the limit and offset value for the query on the next vnode,
     * according to current retrieval results
     *
     * NOTE:
     * if the pRes->offset is larger than 0, the start returned position has not reached yet.
     * Therefore, the pRes->numOfRows, as well as pRes->numOfClauseTotal, must be 0.
     * The pRes->offset value will be updated by virtual node, during query execution.
     */
    if (pQueryInfo->clauseLimit >= 0) {
      pQueryInfo->limit.limit = pQueryInfo->clauseLimit - pRes->numOfClauseTotal;
    }

    pQueryInfo->limit.offset = pRes->offset;
    assert((pRes->offset >= 0 && pRes->numOfRows == 0) || (pRes->offset == 0 && pRes->numOfRows >= 0));

    tscDebug("0x%"PRIx64" new query to next vgroup, index:%d, limit:%" PRId64 ", offset:%" PRId64 ", glimit:%" PRId64,
        pSql->self, pTableMetaInfo->vgroupIndex, pQueryInfo->limit.limit, pQueryInfo->limit.offset, pQueryInfo->clauseLimit);

    /*
     * For project query with super table join, the numOfSub is equalled to the number of all subqueries.
     * Therefore, we need to reset the value of numOfSubs to be 0.
     *
     * For super table join with projection query, if anyone of the subquery is exhausted, the query completed.
     */
    pSql->subState.numOfSub = 0;
    pCmd->command = TSDB_SQL_SELECT;

    tscResetForNextRetrieve(pRes);

    // set the callback function
    pSql->fp = fp;
    tscBuildAndSendRequest(pSql, NULL);
  } else {
    tscDebug("0x%"PRIx64" try all %d vnodes, query complete. current numOfRes:%" PRId64, pSql->self, totalVgroups, pRes->numOfClauseTotal);
  }
}

void tscTryQueryNextClause(SSqlObj* pSql, __async_cb_func_t fp) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  pSql->cmd.command = pQueryInfo->command;

  //backup the total number of result first
  int64_t num = pRes->numOfTotal + pRes->numOfClauseTotal;

  // DON't free final since it may be recoreded and used later in APP
  TAOS_FIELD* finalBk = pRes->final;
  pRes->final = NULL;
  tscFreeSqlResult(pSql);

  pRes->final = finalBk;
  pRes->numOfTotal = num;

  pthread_mutex_lock(&pSql->subState.mutex);
  for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    taos_free_result(pSql->pSubs[i]);
  }

  tfree(pSql->pSubs);
  tfree(pSql->subState.states);
  pSql->subState.numOfSub = 0;
  pthread_mutex_unlock(&pSql->subState.mutex);
  pthread_mutex_destroy(&pSql->subState.mutex);

  pSql->fp = fp;

  tscDebug("0x%"PRIx64" try data in the next subclause", pSql->self);
  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
  } else {
    executeQuery(pSql, pQueryInfo);
  }
}

void* malloc_throw(size_t size) {
  void* p = malloc(size);
  if (p == NULL) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

void* calloc_throw(size_t nmemb, size_t size) {
  void* p = calloc(nmemb, size);
  if (p == NULL) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

char* strdup_throw(const char* str) {
  char* p = strdup(str);
  if (p == NULL) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

int tscSetMgmtEpSetFromCfg(const char *first, const char *second, SRpcCorEpSet *corMgmtEpSet) {
  corMgmtEpSet->version = 0;
  // init mgmt ip set
  SRpcEpSet *mgmtEpSet = &(corMgmtEpSet->epSet);
  mgmtEpSet->numOfEps = 0;
  mgmtEpSet->inUse = 0;

  if (first && first[0] != 0) {
    if (strlen(first) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }
    taosGetFqdnPortFromEp(first, mgmtEpSet->fqdn[mgmtEpSet->numOfEps], &(mgmtEpSet->port[mgmtEpSet->numOfEps]));
    mgmtEpSet->numOfEps++;
  }

  if (second && second[0] != 0) {
    if (strlen(second) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }
    taosGetFqdnPortFromEp(second, mgmtEpSet->fqdn[mgmtEpSet->numOfEps], &(mgmtEpSet->port[mgmtEpSet->numOfEps]));
    mgmtEpSet->numOfEps++;
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  return 0;
}

bool tscSetSqlOwner(SSqlObj* pSql) {
  SSqlRes* pRes = &pSql->res;

  // set the sql object owner
  int64_t threadId = taosGetSelfPthreadId();
  if (atomic_val_compare_exchange_64(&pSql->owner, 0, threadId) != 0) {
    pRes->code = TSDB_CODE_QRY_IN_EXEC;
    return false;
  }

  return true;
}

void tscClearSqlOwner(SSqlObj* pSql) {
  atomic_store_64(&pSql->owner, 0);
}

SVgroupsInfo* tscVgroupInfoClone(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

  size_t size = sizeof(SVgroupsInfo) + sizeof(SVgroupMsg) * vgroupList->numOfVgroups;
  SVgroupsInfo* pNew = malloc(size);
  if (pNew == NULL) {
    return NULL;
  }

  pNew->numOfVgroups = vgroupList->numOfVgroups;

  for(int32_t i = 0; i < vgroupList->numOfVgroups; ++i) {
    SVgroupMsg* pNewVInfo = &pNew->vgroups[i];

    SVgroupMsg* pvInfo = &vgroupList->vgroups[i];
    pNewVInfo->vgId = pvInfo->vgId;
    pNewVInfo->numOfEps = pvInfo->numOfEps;

    for(int32_t j = 0; j < pvInfo->numOfEps; ++j) {
      pNewVInfo->epAddr[j].port = pvInfo->epAddr[j].port;
      tstrncpy(pNewVInfo->epAddr[j].fqdn, pvInfo->epAddr[j].fqdn, TSDB_FQDN_LEN);
    }
  }

  return pNew;
}

void* tscVgroupInfoClear(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

#if 0
  for(int32_t i = 0; i < vgroupList->numOfVgroups; ++i) {
    SVgroupMsg* pVgroupInfo = &vgroupList->vgroups[i];

    for(int32_t j = 0; j < pVgroupInfo->numOfEps; ++j) {
      tfree(pVgroupInfo->epAddr[j].fqdn);
    }

    for(int32_t j = pVgroupInfo->numOfEps; j < TSDB_MAX_REPLICA; j++) {
      assert( pVgroupInfo->epAddr[j].fqdn == NULL );
    }
  }

#endif
  tfree(vgroupList);
  return NULL;
}
# if 0
void tscSVgroupInfoCopy(SVgroupInfo* dst, const SVgroupInfo* src) {
  dst->vgId = src->vgId;
  dst->numOfEps = src->numOfEps;
  for(int32_t i = 0; i < dst->numOfEps; ++i) {
    tfree(dst->epAddr[i].fqdn);
    dst->epAddr[i].port = src->epAddr[i].port;
    assert(dst->epAddr[i].fqdn == NULL);

    dst->epAddr[i].fqdn = strdup(src->epAddr[i].fqdn);
  }
}

#endif

char* serializeTagData(STagData* pTagData, char* pMsg) {
  int32_t n = (int32_t) strlen(pTagData->name);
  *(int32_t*) pMsg = htonl(n);
  pMsg += sizeof(n);

  memcpy(pMsg, pTagData->name, n);
  pMsg += n;

  *(int32_t*)pMsg = htonl(pTagData->dataLen);
  pMsg += sizeof(int32_t);

  memcpy(pMsg, pTagData->data, pTagData->dataLen);
  pMsg += pTagData->dataLen;

  return pMsg;
}

int32_t copyTagData(STagData* dst, const STagData* src) {
  dst->dataLen = src->dataLen;
  tstrncpy(dst->name, src->name, tListLen(dst->name));

  if (dst->dataLen > 0) {
    dst->data = malloc(dst->dataLen);
    if (dst->data == NULL) {
      return -1;
    }

    memcpy(dst->data, src->data, dst->dataLen);
  }

  return 0;
}

STableMeta* createSuperTableMeta(STableMetaMsg* pChild) {
  assert(pChild != NULL);
  int32_t total = pChild->numOfColumns + pChild->numOfTags;

  STableMeta* pTableMeta = calloc(1, sizeof(STableMeta) + sizeof(SSchema) * total);
  pTableMeta->tableType = TSDB_SUPER_TABLE;
  pTableMeta->tableInfo.numOfTags = pChild->numOfTags;
  pTableMeta->tableInfo.numOfColumns = pChild->numOfColumns;
  pTableMeta->tableInfo.precision = pChild->precision;
  pTableMeta->tableInfo.update = pChild->update;

  pTableMeta->id.tid = 0;
  pTableMeta->id.uid = pChild->suid;
  pTableMeta->tversion = pChild->tversion;
  pTableMeta->sversion = pChild->sversion;

  memcpy(pTableMeta->schema, pChild->schema, sizeof(SSchema) * total);

  int32_t num = pTableMeta->tableInfo.numOfColumns;
  for(int32_t i = 0; i < num; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }

  return pTableMeta;
}

uint32_t tscGetTableMetaSize(STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);

  int32_t totalCols = 0;
  if (pTableMeta->tableInfo.numOfColumns >= 0) {
    totalCols = pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags;
  }

  return sizeof(STableMeta) + totalCols * sizeof(SSchema);
}

CChildTableMeta* tscCreateChildMeta(STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);

  CChildTableMeta* cMeta = calloc(1, sizeof(CChildTableMeta));

  cMeta->tableType = TSDB_CHILD_TABLE;
  cMeta->vgId      = pTableMeta->vgId;
  cMeta->id        = pTableMeta->id;
  cMeta->suid      = pTableMeta->suid;
  tstrncpy(cMeta->sTableName, pTableMeta->sTableName, TSDB_TABLE_FNAME_LEN);

  return cMeta;
}

int32_t tscCreateTableMetaFromSTableMeta(SSqlObj *pSql, STableMeta** ppChild, const char* name, size_t *tableMetaCapacity, STableMeta**ppSTable) {
  assert(*ppChild != NULL);
  STableMeta* p      = *ppSTable;
  STableMeta* pChild = *ppChild;

  size_t sz = (p != NULL) ? tscGetTableMetaSize(p) : 0; //ppSTableBuf actually capacity may larger than sz, dont care
  if (p != NULL && sz != 0) {
    memset((char *)p, 0, sz);
  }

  if (NULL == taosHashGetCloneExt(UTIL_GET_TABLEMETA(pSql), pChild->sTableName, strnlen(pChild->sTableName, TSDB_TABLE_FNAME_LEN), NULL, (void **)&p, &sz)) {
    tfree(p);
  }
  *ppSTable = p;

  // tableMeta exists, build child table meta according to the super table meta
  // the uid need to be checked in addition to the general name of the super table.
  if (p && p->id.uid > 0 && pChild->suid == p->id.uid) {
    int32_t totalBytes    = (p->tableInfo.numOfColumns + p->tableInfo.numOfTags) * sizeof(SSchema);
    int32_t tableMetaSize =  sizeof(STableMeta)  + totalBytes;
    if (*tableMetaCapacity < tableMetaSize) {
      STableMeta* pChild1 = realloc(pChild, tableMetaSize);
      if(pChild1 == NULL) return -1;
      pChild = pChild1;
      *tableMetaCapacity = (size_t)tableMetaSize;
    }

    pChild->sversion = p->sversion;
    pChild->tversion = p->tversion;
    memcpy(&pChild->tableInfo, &p->tableInfo, sizeof(STableComInfo));
    memcpy(pChild->schema, p->schema, totalBytes);

    *ppChild = pChild;
    return TSDB_CODE_SUCCESS;
  } else { // super table has been removed, current tableMeta is also expired. remove it here
    taosHashRemove(UTIL_GET_TABLEMETA(pSql), name, strnlen(name, TSDB_TABLE_FNAME_LEN));
    return -1;
  }
}

uint32_t tscGetTableMetaMaxSize() {
  return sizeof(STableMeta) + TSDB_MAX_COLUMNS * sizeof(SSchema);
}

STableMeta* tscTableMetaDup(STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  size_t size = tscGetTableMetaSize(pTableMeta);

  STableMeta* p = malloc(size);
  memcpy(p, pTableMeta, size);
  return p;
}

SVgroupsInfo* tscVgroupsInfoDup(SVgroupsInfo* pVgroupsInfo) {
  assert(pVgroupsInfo != NULL);

  size_t size = sizeof(SVgroupMsg) * pVgroupsInfo->numOfVgroups + sizeof(SVgroupsInfo);
  SVgroupsInfo* pInfo = calloc(1, size);
  pInfo->numOfVgroups = pVgroupsInfo->numOfVgroups;
  for (int32_t m = 0; m < pVgroupsInfo->numOfVgroups; ++m) {
    memcpy(&pInfo->vgroups[m], &pVgroupsInfo->vgroups[m], sizeof(SVgroupMsg));
//    tscSVgroupInfoCopy(&pInfo->vgroups[m], &pVgroupsInfo->vgroups[m]);
  }
  return pInfo;
}

int32_t createProjectionExpr(SQueryInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SExprInfo*** pExpr, int32_t* num) {
  if (!pQueryInfo->arithmeticOnAgg) {
    return TSDB_CODE_SUCCESS;
  }

  *num = tscNumOfFields(pQueryInfo);
  *pExpr = calloc(*(num), POINTER_BYTES);
  if ((*pExpr) == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < (*num); ++i) {
    SInternalField* pField = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, i);
    SExprInfo* pSource = pField->pExpr;

    SExprInfo* px = calloc(1, sizeof(SExprInfo));
    (*pExpr)[i] = px;

    SSqlExpr *pse = &px->base;
    pse->uid      = pTableMetaInfo->pTableMeta->id.uid;
    pse->resColId = pSource->base.resColId;
    strncpy(pse->aliasName, pSource->base.aliasName, tListLen(pse->aliasName));
    strncpy(pse->token, pSource->base.token, tListLen(pse->token));

    if (pSource->base.functionId != TSDB_FUNC_SCALAR_EXPR) {  // this should be switched to projection query
      pse->numOfParams = 0;      // no params for projection query
      pse->functionId  = TSDB_FUNC_PRJ;
      pse->colInfo.colId = pSource->base.resColId;

      int32_t numOfOutput = (int32_t) taosArrayGetSize(pQueryInfo->exprList);
      for (int32_t j = 0; j < numOfOutput; ++j) {
        SExprInfo* p = taosArrayGetP(pQueryInfo->exprList, j);
        if (p->base.resColId == pse->colInfo.colId) {
          pse->colInfo.colIndex = j;
          break;
        }
      }

      if (!pQueryInfo->stableQuery && TSDB_COL_IS_TAG(pSource->base.colInfo.flag)) {
        pse->colInfo.flag = (pSource->base.colInfo.flag) & (~TSDB_COL_TAG);
      } else {
        pse->colInfo.flag = pSource->base.colInfo.flag;
      }
      pse->resType  = pSource->base.resType;
      pse->resBytes = pSource->base.resBytes;
      strncpy(pse->colInfo.name, pSource->base.aliasName, tListLen(pse->colInfo.name));

      // TODO restore refactor
      int32_t functionId = pSource->base.functionId;
      if (pSource->base.functionId == TSDB_FUNC_FIRST_DST) {
        functionId = TSDB_FUNC_FIRST;
      } else if (pSource->base.functionId == TSDB_FUNC_LAST_DST) {
        functionId = TSDB_FUNC_LAST;
      } else if (pSource->base.functionId == TSDB_FUNC_STDDEV_DST) {
        functionId = TSDB_FUNC_STDDEV;
      }

      SUdfInfo* pUdfInfo = NULL;
      if (functionId < 0) {
         pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
      }

      int32_t inter = 0;
      getResultDataInfo(pSource->base.colType, pSource->base.colBytes, functionId, 0, &pse->resType,
          &pse->resBytes, &inter, 0, false, pUdfInfo);
      pse->colType  = pse->resType;
      pse->colBytes = pse->resBytes;

    } else {  // arithmetic expression
      pse->colInfo.colId = pSource->base.colInfo.colId;
      pse->colType  = pSource->base.colType;
      pse->colBytes = pSource->base.colBytes;

      pse->functionId = pSource->base.functionId;
      pse->numOfParams = pSource->base.numOfParams;

      for (int32_t j = 0; j < pSource->base.numOfParams; ++j) {
        tVariantAssign(&pse->param[j], &pSource->base.param[j]);
        buildScalarExprFromMsg(px, NULL);
      }

      pse->resBytes = px->pExpr->resultBytes;
      pse->resType  = px->pExpr->resultType;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createGlobalAggregateExpr(SQueryAttr* pQueryAttr, SQueryInfo* pQueryInfo) {
  assert(tscIsTwoStageSTableQuery(pQueryInfo, 0));

  pQueryAttr->numOfExpr3 = (int32_t) tscNumOfExprs(pQueryInfo);
  pQueryAttr->pExpr3 = calloc(pQueryAttr->numOfExpr3, sizeof(SExprInfo));
  if (pQueryAttr->pExpr3 == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pQueryAttr->numOfExpr3; ++i) {
    SExprInfo* pExpr = &pQueryAttr->pExpr1[i];
    SSqlExpr*  pse = &pQueryAttr->pExpr3[i].base;

    tscExprAssign(&pQueryAttr->pExpr3[i], pExpr);
    pse->colInfo.colId = pExpr->base.resColId;
    pse->colInfo.colIndex = i;

    pse->colType = pExpr->base.resType;
    if(pExpr->base.resBytes > INT16_MAX &&
        (pExpr->base.functionId == TSDB_FUNC_UNIQUE || pExpr->base.functionId == TSDB_FUNC_MODE
         || pExpr->base.functionId == TSDB_FUNC_TAIL || pExpr->base.functionId == TSDB_FUNC_SAMPLE)){
      pQueryAttr->interBytesForGlobal = pExpr->base.resBytes;
    }else{
      pse->colBytes = pExpr->base.resBytes;
    }
  }

  {
    for (int32_t i = 0; i < pQueryAttr->numOfExpr3; ++i) {
      SExprInfo* pExpr = &pQueryAttr->pExpr1[i];
      SSqlExpr*  pse = &pQueryAttr->pExpr3[i].base;

      // the final result size and type in the same as query on single table.
      // so here, set the flag to be false;
      int32_t inter = 0;

      int32_t functionId = pExpr->base.functionId;
      if (functionId >= TSDB_FUNC_TS && functionId <= TSDB_FUNC_DIFF) {
        continue;
      }

      if (functionId == TSDB_FUNC_FIRST_DST) {
        functionId = TSDB_FUNC_FIRST;
      } else if (functionId == TSDB_FUNC_LAST_DST) {
        functionId = TSDB_FUNC_LAST;
      } else if (functionId == TSDB_FUNC_STDDEV_DST) {
        functionId = TSDB_FUNC_STDDEV;
      }

      SUdfInfo* pUdfInfo = NULL;

      if (functionId < 0) {
        pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
      }

      getResultDataInfo(pExpr->base.colType, pExpr->base.colBytes, functionId, 0, &pse->resType, &pse->resBytes, &inter,
                        0, false, pUdfInfo);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createTagColumnInfo(SQueryAttr* pQueryAttr, SQueryInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo) {
  if (pTableMetaInfo->tagColList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pQueryAttr->numOfTags = (int16_t)taosArrayGetSize(pTableMetaInfo->tagColList);
  if (pQueryAttr->numOfTags == 0) {
    return TSDB_CODE_SUCCESS;
  }

  STableMeta* pTableMeta = pQueryInfo->pTableMetaInfo[0]->pTableMeta;

  int32_t numOfTagColumns = tscGetNumOfTags(pTableMeta);

  pQueryAttr->tagColList = calloc(pQueryAttr->numOfTags, sizeof(SColumnInfo));
  if (pQueryAttr->tagColList == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SSchema* pSchema = tscGetTableTagSchema(pTableMeta);
  for (int32_t i = 0; i < pQueryAttr->numOfTags; ++i) {
    SColumn* pCol = taosArrayGetP(pTableMetaInfo->tagColList, i);
    SSchema* pColSchema = &pSchema[pCol->columnIndex];

    if ((pCol->columnIndex >= numOfTagColumns || pCol->columnIndex < TSDB_TBNAME_COLUMN_INDEX) ||
        (!isValidDataType(pColSchema->type))) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    SColumnInfo* pTagCol = &pQueryAttr->tagColList[i];

    pTagCol->colId = pColSchema->colId;
    pTagCol->bytes = pColSchema->bytes;
    pTagCol->type  = pColSchema->type;
    pTagCol->flist.numOfFilters = 0;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscGetColFilterSerializeLen(SQueryInfo* pQueryInfo) {
  int16_t numOfCols = (int16_t)taosArrayGetSize(pQueryInfo->colList);
  int32_t len = 0;

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    for (int32_t j = 0; j < pCol->info.flist.numOfFilters; ++j) {
      len += sizeof(SColumnFilterInfo);
      if (pCol->info.flist.filterInfo[j].filterstr) {
        len += (int32_t)pCol->info.flist.filterInfo[j].len + 1 * TSDB_NCHAR_SIZE;
      }
    }
  }
  return len;
}

int32_t tscGetTagFilterSerializeLen(SQueryInfo* pQueryInfo) {
  // serialize tag column query condition
  if (pQueryInfo->tagCond.pCond != NULL && taosArrayGetSize(pQueryInfo->tagCond.pCond) > 0) {
    STagCond* pTagCond = &pQueryInfo->tagCond;

    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
    SCond *pCond = tsGetSTableQueryCond(pTagCond, pTableMeta->id.uid);
    if (pCond != NULL && pCond->cond != NULL) {
      return pCond->len;
    }
  }
  return 0;
}

int32_t tscCreateQueryFromQueryInfo(SQueryInfo* pQueryInfo, SQueryAttr* pQueryAttr, void* addr) {
  memset(pQueryAttr, 0, sizeof(SQueryAttr));

  int16_t numOfCols        = (int16_t) taosArrayGetSize(pQueryInfo->colList);
  int16_t numOfOutput      = (int16_t) tscNumOfExprs(pQueryInfo);

  pQueryAttr->topBotQuery       = tscIsTopBotQuery(pQueryInfo);
  pQueryAttr->hasTagResults     = hasTagValOutput(pQueryInfo);
  pQueryAttr->stabledev         = isStabledev(pQueryInfo);
  pQueryAttr->tsCompQuery       = isTsCompQuery(pQueryInfo);
  pQueryAttr->diffQuery         = tscIsDiffDerivLikeQuery(pQueryInfo);
  pQueryAttr->simpleAgg         = isSimpleAggregateRv(pQueryInfo);
  pQueryAttr->needReverseScan   = tscNeedReverseScan(pQueryInfo);
  pQueryAttr->stableQuery       = QUERY_IS_STABLE_QUERY(pQueryInfo->type);
  pQueryAttr->groupbyColumn     = (!pQueryInfo->stateWindow) && tscGroupbyColumn(pQueryInfo);
  pQueryAttr->queryBlockDist    = isBlockDistQuery(pQueryInfo);
  pQueryAttr->pointInterpQuery  = tscIsPointInterpQuery(pQueryInfo);
  pQueryAttr->needTableSeqScan  = tscNeedTableSeqScan(pQueryInfo);
  pQueryAttr->timeWindowInterpo = timeWindowInterpoRequired(pQueryInfo);
  pQueryAttr->distinct          = pQueryInfo->distinct;
  pQueryAttr->sw                = pQueryInfo->sessionWindow;
  pQueryAttr->stateWindow       = pQueryInfo->stateWindow;
  pQueryAttr->multigroupResult  = pQueryInfo->multigroupResult;

  pQueryAttr->numOfCols         = numOfCols;
  pQueryAttr->numOfOutput       = numOfOutput;
  pQueryAttr->limit             = pQueryInfo->limit;
  pQueryAttr->slimit            = pQueryInfo->slimit;
  pQueryAttr->order             = pQueryInfo->order;
  pQueryAttr->fillType          = pQueryInfo->fillType;
  pQueryAttr->havingNum         = pQueryInfo->havingFieldNum;
  pQueryAttr->pUdfInfo          = pQueryInfo->pUdfInfo;
  pQueryAttr->range             = pQueryInfo->range;

  if (pQueryInfo->order.order == TSDB_ORDER_ASC) {   // TODO refactor
    pQueryAttr->window = pQueryInfo->window;
  } else {
    pQueryAttr->window.skey = pQueryInfo->window.ekey;
    pQueryAttr->window.ekey = pQueryInfo->window.skey;
  }

  memcpy(&pQueryAttr->interval, &pQueryInfo->interval, sizeof(pQueryAttr->interval));

  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    pQueryAttr->pGroupbyExpr    = calloc(1, sizeof(SGroupbyExpr));
    *(pQueryAttr->pGroupbyExpr) = pQueryInfo->groupbyExpr;
    pQueryAttr->pGroupbyExpr->columnInfo = taosArrayDup(pQueryInfo->groupbyExpr.columnInfo);
  } else {
    assert(pQueryInfo->groupbyExpr.columnInfo == NULL);
  }

  pQueryAttr->pExpr1 = calloc(pQueryAttr->numOfOutput, sizeof(SExprInfo));
  for(int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    tscExprAssign(&pQueryAttr->pExpr1[i], pExpr);

    if (pQueryAttr->pExpr1[i].base.functionId == TSDB_FUNC_SCALAR_EXPR) {
      for (int32_t j = 0; j < pQueryAttr->pExpr1[i].base.numOfParams; ++j) {
        buildScalarExprFromMsg(&pQueryAttr->pExpr1[i], NULL);
      }
    }
  }

  pQueryAttr->tableCols = calloc(numOfCols, sizeof(SColumnInfo));
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    if (!isValidDataType(pCol->info.type) || pCol->info.type == TSDB_DATA_TYPE_NULL) {
      assert(0);
    }

    pQueryAttr->tableCols[i] = pCol->info;
    pQueryAttr->tableCols[i].flist.filterInfo = tFilterInfoDup(pCol->info.flist.filterInfo, pQueryAttr->tableCols[i].flist.numOfFilters);
  }

  // global aggregate query
  if (pQueryAttr->stableQuery && (pQueryAttr->simpleAgg || pQueryAttr->interval.interval > 0 || pQueryAttr->sw.gap > 0)
      && tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    createGlobalAggregateExpr(pQueryAttr, pQueryInfo);
  }

  // for simple table, not for super table
  if (pQueryInfo->arithmeticOnAgg) {
    pQueryAttr->numOfExpr2 = (int32_t) taosArrayGetSize(pQueryInfo->exprList1);
    pQueryAttr->pExpr2 = calloc(pQueryAttr->numOfExpr2, sizeof(SExprInfo));
    for(int32_t i = 0; i < pQueryAttr->numOfExpr2; ++i) {
      SExprInfo* p = taosArrayGetP(pQueryInfo->exprList1, i);
      tscExprAssign(&pQueryAttr->pExpr2[i], p);
    }
  }

  // tag column info
  int32_t code = createTagColumnInfo(pQueryAttr, pQueryInfo, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pQueryAttr->fillType != TSDB_FILL_NONE) {
    pQueryAttr->fillVal = calloc(pQueryInfo->numOfFillVal, sizeof(int64_t));
    memcpy(pQueryAttr->fillVal, pQueryInfo->fillVal, pQueryInfo->numOfFillVal * sizeof(int64_t));
  }

  pQueryAttr->srcRowSize = 0;
  pQueryAttr->maxTableColumnWidth = 0;
  for (int16_t i = 0; i < numOfCols; ++i) {
    pQueryAttr->srcRowSize += pQueryAttr->tableCols[i].bytes;
    if (pQueryAttr->maxTableColumnWidth < pQueryAttr->tableCols[i].bytes) {
      pQueryAttr->maxTableColumnWidth = pQueryAttr->tableCols[i].bytes;
    }
  }

  pQueryAttr->interBufSize = getOutputInterResultBufSize(pQueryAttr);

  if (pQueryAttr->numOfCols <= 0 && !tscQueryTags(pQueryInfo) && !pQueryAttr->queryBlockDist) {
        tscError("%p illegal value of numOfCols in query msg: %" PRIu64 ", table cols:%d", addr,
        (uint64_t)pQueryAttr->numOfCols, numOfCols);

    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (pQueryAttr->interval.interval < 0) {
    tscError("%p illegal value of aggregation time interval in query msg: %" PRId64, addr,
             (int64_t)pQueryInfo->interval.interval);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (pQueryAttr->pGroupbyExpr != NULL && pQueryAttr->pGroupbyExpr->numOfGroupCols < 0) {
    tscError("%p illegal value of numOfGroupCols in query msg: %d", addr, pQueryInfo->groupbyExpr.numOfGroupCols);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doAddTableName(char* nextStr, char** str, SArray* pNameArray, SSqlObj* pSql) {
  int32_t code = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;

  char  tablename[TSDB_TABLE_FNAME_LEN] = {0};
  int32_t len = 0;

  if (nextStr == NULL) {
    tstrncpy(tablename, *str, TSDB_TABLE_FNAME_LEN);
  } else {
    len = (int32_t)(nextStr - (*str));
    if (len >= TSDB_TABLE_NAME_LEN) {
      sprintf(pCmd->payload, "table name too long");
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    memcpy(tablename, *str, nextStr - (*str));
    tablename[len] = '\0';
  }

  (*str) = nextStr + 1;
  len = (int32_t)strtrim(tablename);

  SStrToken sToken = {.n = len, .type = TK_ID, .z = tablename};
  tGetToken(tablename, &sToken.type);

  bool dbIncluded = false;

  // Check if the table name available or not
  if (tscValidateName(&sToken, true, &dbIncluded) != TSDB_CODE_SUCCESS) {
    sprintf(pCmd->payload, "table name is invalid");
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  SName name = {0};
  if ((code = tscSetTableFullName(&name, &sToken, pSql, dbIncluded)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  memset(tablename, 0, tListLen(tablename));
  tNameExtractFullName(&name, tablename);

  char* p = strdup(tablename);
  taosArrayPush(pNameArray, &p);
  return TSDB_CODE_SUCCESS;
}

int32_t nameComparFn(const void* n1, const void* n2) {
  int32_t ret = strcmp(*(char**)n1, *(char**)n2);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0? 1:-1;
  }
}

static void freeContent(void* p) {
  char* ptr = *(char**)p;
  tfree(ptr);
}

int tscTransferTableNameList(SSqlObj *pSql, const char *pNameList, int32_t length, SArray* pNameArray) {
  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->command = TSDB_SQL_MULTI_META;
  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLES_META;

  int   code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  char *str = (char *)pNameList;

  SQueryInfo *pQueryInfo = tscGetQueryInfoS(pCmd);
  if (pQueryInfo == NULL) {
    pSql->res.code = terrno;
    return terrno;
  }

  char *nextStr;
  while (1) {
    nextStr = strchr(str, ',');
    if (nextStr == NULL) {
      code = doAddTableName(nextStr, &str, pNameArray, pSql);
      break;
    }

    code = doAddTableName(nextStr, &str, pNameArray, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (taosArrayGetSize(pNameArray) > TSDB_MULTI_TABLEMETA_MAX_NUM) {
      code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
      sprintf(pCmd->payload, "tables over the max number");
      return code;
    }
  }

  size_t len = taosArrayGetSize(pNameArray);
  if (len == 1) {
    return TSDB_CODE_SUCCESS;
  }

  if (len > TSDB_MULTI_TABLEMETA_MAX_NUM) {
    code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
    sprintf(pCmd->payload, "tables over the max number");
    return code;
  }

  taosArraySort(pNameArray, nameComparFn);
  taosArrayRemoveDuplicate(pNameArray, nameComparFn, freeContent);
  return TSDB_CODE_SUCCESS;
}

bool vgroupInfoIdentical(SNewVgroupInfo *pExisted, SVgroupMsg* src) {
  assert(pExisted != NULL && src != NULL);
  if (pExisted->numOfEps != src->numOfEps) {
    return false;
  }

  for(int32_t i = 0; i < pExisted->numOfEps; ++i) {
    if (pExisted->ep[i].port != src->epAddr[i].port) {
      return false;
    }

    if (strncmp(pExisted->ep[i].fqdn, src->epAddr[i].fqdn, tListLen(pExisted->ep[i].fqdn)) != 0) {
      return false;
    }
  }

  return true;
}

SNewVgroupInfo createNewVgroupInfo(SVgroupMsg *pVgroupMsg) {
  assert(pVgroupMsg != NULL);

  SNewVgroupInfo info = {0};
  info.numOfEps = pVgroupMsg->numOfEps;
  info.vgId     = pVgroupMsg->vgId;
  info.inUse    = 0;   // 0 is the default value of inUse in case of multiple replica

  assert(info.numOfEps >= 1 && info.vgId >= 1);
  for(int32_t i = 0; i < pVgroupMsg->numOfEps; ++i) {
    tstrncpy(info.ep[i].fqdn, pVgroupMsg->epAddr[i].fqdn, TSDB_FQDN_LEN);
    info.ep[i].port = pVgroupMsg->epAddr[i].port;
  }

  return info;
}

void tscRemoveCachedTableMeta(STableMetaInfo* pTableMetaInfo, uint64_t id) {
  char fname[TSDB_TABLE_FNAME_LEN] = {0};
  SSqlObj *p = (SSqlObj *)taosAcquireRef(tscObjRef, id);

  tNameExtractFullName(&pTableMetaInfo->name, fname);
  int32_t len = (int32_t) strnlen(fname, TSDB_TABLE_FNAME_LEN);
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    void* pv = taosCacheAcquireByKey(UTIL_GET_VGROUPLIST(p), fname, len);
    if (pv != NULL) {
      taosCacheRelease(UTIL_GET_VGROUPLIST(p), &pv, true);
    }
  }

  taosHashRemove(UTIL_GET_TABLEMETA(p), fname, len);
  tscDebug("0x%"PRIx64" remove table meta %s, numOfRemain:%d", id, fname, (int32_t) taosHashGetSize(UTIL_GET_TABLEMETA(p)));
  taosReleaseRef(tscObjRef, id);
}

char* cloneCurrentDBName(SSqlObj* pSql) {
  char        *p = NULL;
#ifdef HTTP_EMBEDDED
  HttpContext *pCtx = NULL;
#endif

  pthread_mutex_lock(&pSql->pTscObj->mutex);
  STscObj *pTscObj = pSql->pTscObj;
  switch (pTscObj->from) {
#ifdef HTTP_EMBEDDED
  case TAOS_REQ_FROM_HTTP:
    pCtx = pSql->param;
    if (pCtx && pCtx->db[0] != '\0') {
      char db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN] = {0};
      int32_t len = sprintf(db, "%s%s%s", pTscObj->acctId, TS_PATH_DELIMITER, pCtx->db);
      assert(len <= sizeof(db));

      p = strdup(db);
    }
    break;
#endif
  default:
    break;
  }
  if (p == NULL) {
    p = strdup(pSql->pTscObj->db);
  }
  pthread_mutex_unlock(&pSql->pTscObj->mutex);

  return p;
}

int parseJsontoTagData(char* json, uint32_t jsonLength, SKVRowBuilder* kvRowBuilder, char* errMsg, int16_t startColId){
  // set json NULL data
  uint8_t nullTypeVal[CHAR_BYTES + VARSTR_HEADER_SIZE + INT_BYTES] = {0};
  uint32_t jsonNULL = TSDB_DATA_JSON_NULL;
  int jsonIndex = startColId + 1;
  char nullTypeKey[VARSTR_HEADER_SIZE + INT_BYTES] = {0};
  varDataSetLen(nullTypeKey, INT_BYTES);
  nullTypeVal[0] = TSDB_DATA_TYPE_JSON;
  varDataSetLen(nullTypeVal + CHAR_BYTES, INT_BYTES);
  *(uint32_t*)(varDataVal(nullTypeKey)) = jsonNULL;
  tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_NCHAR, nullTypeKey, false);   // add json null type
  if (!json || strtrim(json) == 0 || (jsonLength == strlen("null") && strncasecmp(json, "null", 4) == 0)){
    *(uint32_t*)(varDataVal(nullTypeVal + CHAR_BYTES)) = jsonNULL;
    tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_NCHAR, nullTypeVal, true);   // add json null value
    return TSDB_CODE_SUCCESS;
  }
  int32_t jsonNotNull = TSDB_DATA_JSON_NOT_NULL;
  *(uint32_t*)(varDataVal(nullTypeVal + CHAR_BYTES)) = jsonNotNull;
  tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_NCHAR, nullTypeVal, true);   // add json type

  // set json real data
  cJSON *root = cJSON_Parse(json);
  if (root == NULL){
    tscError("json parse error:%s", json);
    return tscSQLSyntaxErrMsg(errMsg, "json parse error", NULL);
  }

  int size = cJSON_GetArraySize(root);
  if(!cJSON_IsObject(root)){
    tscError("json error invalide value");
    return tscSQLSyntaxErrMsg(errMsg, "json error invalide value", NULL);
  }

  int retCode = 0;
  SHashObj* keyHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
  for(int i = 0; i < size; i++) {
    cJSON* item = cJSON_GetArrayItem(root, i);
    if (!item) {
      tscError("json inner error:%d", i);
      retCode =  tscSQLSyntaxErrMsg(errMsg, "json inner error", NULL);
      goto end;
    }

    char *jsonKey = item->string;
    if(!isValidateTag(jsonKey)){
      tscError("json key not validate");
      retCode =  tscSQLSyntaxErrMsg(errMsg, "json key not validate", NULL);
      goto end;
    }
    if(strlen(jsonKey) > TSDB_MAX_JSON_KEY_LEN){
      tscError("json key too long error");
      retCode =  tscSQLSyntaxErrMsg(errMsg, "json key too long, more than 256", NULL);
      goto end;
    }
    if(strlen(jsonKey) == 0 || taosHashGet(keyHash, jsonKey, strlen(jsonKey)) != NULL){
      continue;
    }

    // json key encode by binary
    char tagKey[TSDB_MAX_JSON_KEY_LEN + VARSTR_HEADER_SIZE] = {0};
    strncpy(varDataVal(tagKey), jsonKey, strlen(jsonKey));
    int32_t outLen = (int32_t)strlen(jsonKey);
    taosHashPut(keyHash, jsonKey, outLen, &outLen, CHAR_BYTES);  // add key to hash to remove dumplicate, value is useless

    varDataSetLen(tagKey, outLen);
    tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_NCHAR, tagKey, false);   // add json key

    if(item->type == cJSON_String){     // add json value  format: type|data
      char *jsonValue = item->valuestring;
      outLen = 0;
      char *tagVal = calloc(strlen(jsonValue) * TSDB_NCHAR_SIZE + TSDB_NCHAR_SIZE, 1);
      *tagVal = jsonType2DbType(0, item->type);     // type
      char* tagData = POINTER_SHIFT(tagVal,CHAR_BYTES);
      if (strlen(jsonValue) > 0 && !taosMbsToUcs4(jsonValue, strlen(jsonValue), varDataVal(tagData),
                         (int32_t)(strlen(jsonValue) * TSDB_NCHAR_SIZE), &outLen)) {
        tscError("charset:%s to %s. val:%s, errno:%s, convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, jsonValue, strerror(errno));
        retCode = tscSQLSyntaxErrMsg(errMsg, "charset convert json error", NULL);
        free(tagVal);
        goto end;
      }

      varDataSetLen(tagData, outLen);
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_NCHAR, tagVal, true);
      free(tagVal);
    }else if(item->type == cJSON_Number){
      if(!isfinite(item->valuedouble)){
        tscError("json value is invalidate");
        retCode =  tscSQLSyntaxErrMsg(errMsg, "json value number is illegal", NULL);
        goto end;
      }
      char tagVal[LONG_BYTES + CHAR_BYTES] = {0};
      *tagVal = jsonType2DbType(item->valuedouble, item->type);    // type
      char* tagData = POINTER_SHIFT(tagVal,CHAR_BYTES);
      if(*tagVal == TSDB_DATA_TYPE_DOUBLE) *((double *)tagData) = item->valuedouble;
      else if(*tagVal == TSDB_DATA_TYPE_BIGINT) *((int64_t *)tagData) = item->valueint;
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_BIGINT, tagVal, true);
    }else if(item->type == cJSON_True || item->type == cJSON_False){
      char tagVal[CHAR_BYTES + CHAR_BYTES] = {0};
      *tagVal = jsonType2DbType((double)(item->valueint), item->type);    // type
      char* tagData = POINTER_SHIFT(tagVal,CHAR_BYTES);
      *tagData = (char)(item->valueint);
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_BOOL, tagVal, true);
    }else if(item->type == cJSON_NULL){
      char tagVal[CHAR_BYTES + VARSTR_HEADER_SIZE + INT_BYTES] = {0};
      *tagVal = jsonType2DbType(0, item->type);    // type
      int32_t* tagData = POINTER_SHIFT(tagVal,CHAR_BYTES);
      varDataSetLen(tagData, INT_BYTES);
      *(uint32_t*)(varDataVal(tagData)) = TSDB_DATA_JSON_null;
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, TSDB_DATA_TYPE_BINARY, tagVal, true);
    }
    else{
      retCode =  tscSQLSyntaxErrMsg(errMsg, "invalidate json value", NULL);
      goto end;
    }
  }

  if(taosHashGetSize(keyHash) == 0){  // set json NULL true
    *(uint32_t*)(varDataVal(nullTypeVal + CHAR_BYTES)) = jsonNULL;
    memcpy(POINTER_SHIFT(kvRowBuilder->buf, kvRowBuilder->pColIdx[2].offset), nullTypeVal, CHAR_BYTES + VARSTR_HEADER_SIZE + INT_BYTES);
  }

end:
  taosHashCleanup(keyHash);
  cJSON_Delete(root);
  return retCode;
}

int8_t jsonType2DbType(double data, int jsonType){
  switch(jsonType){
    case cJSON_Number:
      if (data - (int64_t)data == 0) return TSDB_DATA_TYPE_BIGINT; else return TSDB_DATA_TYPE_DOUBLE;
    case cJSON_String:
      return TSDB_DATA_TYPE_NCHAR;
    case cJSON_NULL:
      return TSDB_DATA_TYPE_BINARY;
    case cJSON_True:
    case cJSON_False:
      return TSDB_DATA_TYPE_BOOL;
  }
  return TSDB_DATA_TYPE_NULL;
}

// get key from json->'key'
void getJsonKey(SStrToken *t0){
  while(true){
    t0->n = tGetToken(t0->z, &t0->type);
    if (t0->type == TK_STRING){
      t0->z++;
      t0->n -= 2;
      break;
    }else if (t0->type == TK_ILLEGAL){
      assert(0);
    }
    t0->z += t0->n;
  }
}
