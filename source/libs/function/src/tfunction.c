#include "os.h"
#include "tarray.h"
#include "function.h"
#include "thash.h"
#include "taggfunction.h"
#include "tscalarfunction.h"

static SHashObj* functionHashTable = NULL;

static void doInitFunctionHashTable() {
  int numOfEntries = tListLen(aggFunc);
  functionHashTable = taosHashInit(numOfEntries, MurmurHash3_32, false, false);
  for (int32_t i = 0; i < numOfEntries; i++) {
    int32_t len = (uint32_t)strlen(aggFunc[i].name);

    SAggFunctionInfo* ptr = &aggFunc[i];
    taosHashPut(functionHashTable, aggFunc[i].name, len, (void*)&ptr, POINTER_BYTES);
  }

  numOfEntries = tListLen(scalarFunc);
  for(int32_t i = 0; i < numOfEntries; ++i) {
    int32_t len = (int32_t) strlen(scalarFunc[i].name);
    SScalarFunctionInfo* ptr = &scalarFunc[i];
    taosHashPut(functionHashTable, scalarFunc[i].name, len, (void*)&ptr, POINTER_BYTES);
  }
}

static pthread_once_t functionHashTableInit = PTHREAD_ONCE_INIT;

int32_t qIsBuiltinFunction(const char* name, int32_t len) {
  pthread_once(&functionHashTableInit, doInitFunctionHashTable);

  SAggFunctionInfo** pInfo = taosHashGet(functionHashTable, name, len);
  if (pInfo != NULL) {
    return (*pInfo)->functionId;
  } else {
    return -1;
  }
}

bool qIsValidUdf(SArray* pUdfInfo, const char* name, int32_t len, int32_t* functionId) {
  return true;
}

const char* qGetFunctionName(int32_t functionId) {

}

bool isTagsQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int16_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);

    // "select count(tbname)" query
//    if (functId == FUNCTION_COUNT && pExpr->base.colpDesc->colId == TSDB_TBNAME_COLUMN_INDEX) {
//      continue;
//    }

    if (f != FUNCTION_TAGPRJ && f != FUNCTION_TID_TAG) {
      return false;
    }
  }

  return true;
}

//bool tscMultiRoundQuery(SArray* pFunctionIdList, int32_t index) {
//  if (!UTIL_TABLE_IS_SUPER_TABLE(pQueryInfo->pTableMetaInfo[index])) {
//    return false;
//  }
//
//  size_t numOfExprs = (int32_t) getNumOfExprs(pQueryInfo);
//  for(int32_t i = 0; i < numOfExprs; ++i) {
//    SExprInfo* pExpr = getExprInfo(pQueryInfo, i);
//    if (pExpr->base.functionId == FUNCTION_STDDEV_DST) {
//      return true;
//    }
//  }
//
//  return false;
//}

bool isProjectionQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_TS_DUMMY) {
      continue;
    }

    if (f != FUNCTION_PRJ &&
        f != FUNCTION_TAGPRJ &&
        f != FUNCTION_TAG &&
        f != FUNCTION_TS &&
        f != FUNCTION_ARITHM &&
        f != FUNCTION_DIFF &&
        f != FUNCTION_DERIVATIVE) {
      return false;
    }
  }

  return true;
}

bool isDiffDerivativeQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_TS_DUMMY) {
      continue;
    }

    if (f == FUNCTION_DIFF || f == FUNCTION_DERIVATIVE) {
      return true;
    }
  }

  return false;
}

bool isInterpQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_TAG || f == FUNCTION_TS) {
      continue;
    }

    if (f != FUNCTION_INTERP) {
      return false;
    }
  }

  return true;
}

bool isArithmeticQueryOnAggResult(SArray* pFunctionIdList) {
  if (isProjectionQuery(pFunctionIdList)) {
    return false;
  }

  assert(0);

//  size_t numOfOutput = getNumOfFields(pQueryInfo);
//  for(int32_t i = 0; i < numOfOutput; ++i) {
//    SExprInfo* pExprInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, i)->pExpr;
//    if (pExprInfo->pExpr != NULL) {
//      return true;
//    }
//  }

  return false;
}

bool isGroupbyColumn(SArray* pFunctionIdList) {
//  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
//  int32_t         numOfCols = getNumOfColumns(pTableMetaInfo->pTableMeta);
//
//  SGroupbyExpr* pGroupbyExpr = &pQueryInfo->groupbyExpr;
//  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
//    SColIndex* pIndex = taosArrayGet(pGroupbyExpr->columnInfo, k);
//    if (!TSDB_COL_IS_TAG(pIndex->flag) && pIndex->colIndex < numOfCols) {  // group by normal columns
//      return true;
//    }
//  }

  return false;
}

bool isTopBotQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_TS) {
      continue;
    }

    if (f == FUNCTION_TOP || f == FUNCTION_BOTTOM) {
      return true;
    }
  }

  return false;
}

bool isTsCompQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  if (num != 1) {
    return false;
  }

  int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, 0);
  return f == FUNCTION_TS_COMP;
}

bool isTWAQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_TWA) {
      return true;
    }
  }

  return false;
}

bool isIrateQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_IRATE) {
      return true;
    }
  }

  return false;
}

bool isStabledev(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_STDDEV_DST) {
      return true;
    }
  }

  return false;
}

bool needReverseScan(SArray* pFunctionIdList) {
  assert(0);
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
    if (f == FUNCTION_TS || f == FUNCTION_TS_DUMMY || f == FUNCTION_TAG) {
      continue;
    }

//    if ((f == FUNCTION_FIRST || f == FUNCTION_FIRST_DST) && pQueryInfo->order.order == TSDB_ORDER_DESC) {
//      return true;
//    }

    if (f == FUNCTION_LAST || f == FUNCTION_LAST_DST) {
      // the scan order to acquire the last result of the specified column
//      int32_t order = (int32_t)pExpr->base.param[0].i64;
//      if (order != pQueryInfo->order.order) {
//        return true;
//      }
    }
  }

  return false;
}

bool isSimpleAggregateRv(SArray* pFunctionIdList) {
//  if (pQueryInfo->interval.interval > 0 || pQueryInfo->sessionWindow.gap > 0) {
//    return false;
//  }
//
//  if (tscIsDiffDerivQuery(pQueryInfo)) {
//    return false;
//  }
//
//  size_t numOfExprs = getNumOfExprs(pQueryInfo);
//  for (int32_t i = 0; i < numOfExprs; ++i) {
//    SExprInfo* pExpr = getExprInfo(pQueryInfo, i);
//    if (pExpr == NULL) {
//      continue;
//    }
//
//    int32_t functionId = pExpr->base.functionId;
//    if (functionId < 0) {
//      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
//      if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
//        return true;
//      }
//
//      continue;
//    }
//
//    if (functionId == FUNCTION_TS || functionId == FUNCTION_TS_DUMMY) {
//      continue;
//    }
//
//    if ((!IS_MULTIOUTPUT(aAggs[functionId].status)) ||
//        (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_TS_COMP)) {
//      return true;
//    }
//  }

  return false;
}

bool isBlockDistQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, 0);
  return (num == 1 && f == FUNCTION_BLKINFO);
}

bool isTwoStageSTableQuery(SArray* pFunctionIdList, int32_t tableIndex) {
//  if (pQueryInfo == NULL) {
//    return false;
//  }
//
//  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
//  if (pTableMetaInfo == NULL) {
//    return false;
//  }
//
//  if ((pQueryInfo->type & TSDB_QUERY_TYPE_FREE_RESOURCE) == TSDB_QUERY_TYPE_FREE_RESOURCE) {
//    return false;
//  }
//
//  // for ordered projection query, iterate all qualified vnodes sequentially
//  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
//    return false;
//  }
//
//  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_STABLE_SUBQUERY) && pQueryInfo->command == TSDB_SQL_SELECT) {
//    return UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
//  }

  return false;
}

bool isProjectionQueryOnSTable(SArray* pFunctionIdList, int32_t tableIndex) {
//  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
//
//  /*
//   * In following cases, return false for non ordered project query on super table
//   * 1. failed to get tableMeta from server; 2. not a super table; 3. limitation is 0;
//   * 4. show queries, instead of a select query
//   */
//  size_t numOfExprs = getNumOfExprs(pQueryInfo);
//  if (pTableMetaInfo == NULL || !UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) ||
//      pQueryInfo->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT || numOfExprs == 0) {
//    return false;
//  }
//
//  for (int32_t i = 0; i < numOfExprs; ++i) {
//    int32_t functionId = getExprInfo(pQueryInfo, i)->base.functionId;
//
//    if (functionId < 0) {
//      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
//      if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
//        return false;
//      }
//
//      continue;
//    }
//
//    if (functionId != FUNCTION_PRJ &&
//        functionId != FUNCTION_TAGPRJ &&
//        functionId != FUNCTION_TAG &&
//        functionId != FUNCTION_TS &&
//        functionId != FUNCTION_ARITHM &&
//        functionId != FUNCTION_TS_COMP &&
//        functionId != FUNCTION_DIFF &&
//        functionId != FUNCTION_DERIVATIVE &&
//        functionId != FUNCTION_TS_DUMMY &&
//        functionId != FUNCTION_TID_TAG) {
//      return false;
//    }
//  }

  return true;
}

bool hasTagValOutput(SArray* pFunctionIdList) {
  size_t size = taosArrayGetSize(pFunctionIdList);

  //  if (numOfExprs == 1 && pExpr1->base.functionId == FUNCTION_TS_COMP) {
//    return true;
//  }

  for (int32_t i = 0; i < size; ++i) {
    int32_t functionId = *(int16_t*) taosArrayGet(pFunctionIdList, i);

    // ts_comp column required the tag value for join filter
    if (functionId == FUNCTION_TAG || functionId == FUNCTION_TAGPRJ) {
      return true;
    }
  }

  return false;
}

//bool timeWindowInterpoRequired(SArray* pFunctionIdList) {
//  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
//  for (int32_t i = 0; i < num; ++i) {
//    int32_t f = *(int16_t*) taosArrayGet(pFunctionIdList, i);
//    if (f == FUNCTION_TWA || f == FUNCTION_INTERP) {
//      return true;
//    }
//  }
//
//  return false;
//}

void extractFunctionDesc(SArray* pFunctionIdList, SMultiFunctionsDesc* pDesc) {
  assert(pFunctionIdList != NULL);


  pDesc->blockDistribution = isBlockDistQuery(pFunctionIdList);
  if (pDesc->blockDistribution) {
    return;
  }

  pDesc->projectionQuery = isProjectionQuery(pFunctionIdList);
  pDesc->onlyTagQuery    = isTagsQuery(pFunctionIdList);
  pDesc->interpQuery     = isInterpQuery(pFunctionIdList);
}
