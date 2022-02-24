#include "os.h"
#include "tarray.h"
#include "function.h"
#include "thash.h"
#include "taggfunction.h"

static SHashObj* functionHashTable = NULL;
static SHashObj* udfHashTable = NULL;

static void doInitFunctionHashTable() {
  int numOfEntries = tListLen(aggFunc);
  functionHashTable = taosHashInit(numOfEntries, MurmurHash3_32, false, false);
  for (int32_t i = 0; i < numOfEntries; i++) {
    int32_t len = (uint32_t)strlen(aggFunc[i].name);

    SAggFunctionInfo* ptr = &aggFunc[i];
    taosHashPut(functionHashTable, aggFunc[i].name, len, (void*)&ptr, POINTER_BYTES);
  }

/*
  numOfEntries = tListLen(scalarFunc);
  for(int32_t i = 0; i < numOfEntries; ++i) {
    int32_t len = (int32_t) strlen(scalarFunc[i].name);
    SScalarFunctionInfo* ptr = &scalarFunc[i];
    taosHashPut(functionHashTable, scalarFunc[i].name, len, (void*)&ptr, POINTER_BYTES);
  }
*/

  udfHashTable = taosHashInit(numOfEntries, MurmurHash3_32, true, true);
}

static pthread_once_t functionHashTableInit = PTHREAD_ONCE_INIT;

int32_t qIsBuiltinFunction(const char* name, int32_t len, bool* scalarFunction) {
  pthread_once(&functionHashTableInit, doInitFunctionHashTable);

  SAggFunctionInfo** pInfo = taosHashGet(functionHashTable, name, len);
  if (pInfo != NULL) {
    *scalarFunction = ((*pInfo)->type == FUNCTION_TYPE_SCALAR);
    return (*pInfo)->functionId;
  } else {
    return -1;
  }
}

bool qIsValidUdf(SArray* pUdfInfo, const char* name, int32_t len, int32_t* functionId) {
  return true;
}

bool qIsAggregateFunction(const char* functionName) {
  assert(functionName != NULL);
  bool scalarfunc = false;
  qIsBuiltinFunction(functionName, strlen(functionName), &scalarfunc);

  return !scalarfunc;
}

bool qIsSelectivityFunction(const char* functionName) {
  assert(functionName != NULL);
  pthread_once(&functionHashTableInit, doInitFunctionHashTable);

  size_t len = strlen(functionName);
  SAggFunctionInfo** pInfo = taosHashGet(functionHashTable, functionName, len);
  if (pInfo != NULL) {
    return ((*pInfo)->status | FUNCSTATE_SELECTIVITY) != 0;
  }

  return false;
}

SAggFunctionInfo* qGetFunctionInfo(const char* name, int32_t len) {
  pthread_once(&functionHashTableInit, doInitFunctionHashTable);

  SAggFunctionInfo** pInfo = taosHashGet(functionHashTable, name, len);
  if (pInfo != NULL) {
    return (*pInfo);
  } else {
    return NULL;
  }
}

void qAddUdfInfo(uint64_t id, SUdfInfo* pUdfInfo) {
  int32_t len = (uint32_t)strlen(pUdfInfo->name);
  taosHashPut(udfHashTable, pUdfInfo->name, len, (void*)&pUdfInfo, POINTER_BYTES);
}

void qRemoveUdfInfo(uint64_t id, SUdfInfo* pUdfInfo) {
  int32_t len = (uint32_t)strlen(pUdfInfo->name);
  taosHashRemove(udfHashTable, pUdfInfo->name, len);
}

bool isTagsQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    char* f = *(char**) taosArrayGet(pFunctionIdList, i);

    // todo handle count(tbname) query
    if (strcmp(f, "project") != 0 && strcmp(f, "count") != 0) {
      return false;
    }

    // "select count(tbname)" query
//    if (functId == FUNCTION_COUNT && pExpr->base.colpDesc->colId == TSDB_TBNAME_COLUMN_INDEX) {
//      continue;
//    }
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
    char* f = *(char**) taosArrayGet(pFunctionIdList, i);
    if (strcmp(f, "project") == 0) {
      return true;
    }
  }

  return false;
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

bool isGroupbyColumn(SGroupbyExpr* pGroupby) {
  return !pGroupby->groupbyTag;
}

bool isTopBotQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < num; ++i) {
    char* f = *(char**) taosArrayGet(pFunctionIdList, i);
    if (strcmp(f, "project") == 0) {
      continue;
    }

    if (strcmp(f, "top") == 0 || strcmp(f, "bottom") == 0) {
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

bool isAgg(SArray* pFunctionIdList) {
  size_t size = taosArrayGetSize(pFunctionIdList);
  for (int32_t i = 0; i < size; ++i) {
    char* f = *(char**) taosArrayGet(pFunctionIdList, i);
    if (strcmp(f, "project") == 0) {
      return false;
    }

    if (qIsAggregateFunction(f)) {
      return true;
    }
  }

  return false;
}

bool isBlockDistQuery(SArray* pFunctionIdList) {
  int32_t num = (int32_t) taosArrayGetSize(pFunctionIdList);
  char* f = *(char**) taosArrayGet(pFunctionIdList, 0);
  return (num == 1 && strcmp(f, "block_dist") == 0);
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
//      if (pUdfInfo->funcType == TSDB_FUNC_TYPE_AGGREGATE) {
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

//  pDesc->projectionQuery = isProjectionQuery(pFunctionIdList);
//  pDesc->onlyTagQuery    = isTagsQuery(pFunctionIdList);
  pDesc->interpQuery     = isInterpQuery(pFunctionIdList);
  pDesc->topbotQuery     = isTopBotQuery(pFunctionIdList);
  pDesc->agg             = isAgg(pFunctionIdList);
}
