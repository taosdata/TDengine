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

#include "functionMgt.h"

#include "builtins.h"
#include "builtinsimpl.h"
#include "functionMgtInt.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"
#include "tudf.h"

typedef struct SFuncMgtService {
  SHashObj* pFuncNameHashTable;
} SFuncMgtService;

static SFuncMgtService gFunMgtService;
static TdThreadOnce    functionHashTableInit = PTHREAD_ONCE_INIT;
static int32_t         initFunctionCode = 0;

static void doInitFunctionTable() {
  gFunMgtService.pFuncNameHashTable =
      taosHashInit(funcMgtBuiltinsNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == gFunMgtService.pFuncNameHashTable) {
    initFunctionCode = TSDB_CODE_FAILED;
    return;
  }

  for (int32_t i = 0; i < funcMgtBuiltinsNum; ++i) {
    if (TSDB_CODE_SUCCESS != taosHashPut(gFunMgtService.pFuncNameHashTable, funcMgtBuiltins[i].name,
                                         strlen(funcMgtBuiltins[i].name), &i, sizeof(int32_t))) {
      initFunctionCode = TSDB_CODE_FAILED;
      return;
    }
  }
}

static bool isSpecificClassifyFunc(int32_t funcId, uint64_t classification) {
  if (fmIsUserDefinedFunc(funcId)) {
    return FUNC_MGT_AGG_FUNC == classification
               ? FUNC_AGGREGATE_UDF_ID == funcId
               : (FUNC_MGT_SCALAR_FUNC == classification ? FUNC_SCALAR_UDF_ID == funcId : false);
  }
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNC_MGT_TEST_MASK(funcMgtBuiltins[funcId].classification, classification);
}

int32_t fmFuncMgtInit() {
  taosThreadOnce(&functionHashTableInit, doInitFunctionTable);
  return initFunctionCode;
}

int32_t fmGetFuncInfo(SFunctionNode* pFunc, char* pMsg, int32_t msgLen) {
  if (NULL != gFunMgtService.pFuncNameHashTable) {
    void* pVal = taosHashGet(gFunMgtService.pFuncNameHashTable, pFunc->functionName, strlen(pFunc->functionName));
    if (NULL != pVal) {
      pFunc->funcId = *(int32_t*)pVal;
      pFunc->funcType = funcMgtBuiltins[pFunc->funcId].type;
      return funcMgtBuiltins[pFunc->funcId].translateFunc(pFunc, pMsg, msgLen);
    }
    return TSDB_CODE_FUNC_NOT_BUILTIN_FUNTION;
  }
  for (int32_t i = 0; i < funcMgtBuiltinsNum; ++i) {
    if (0 == strcmp(funcMgtBuiltins[i].name, pFunc->functionName)) {
      pFunc->funcId = i;
      pFunc->funcType = funcMgtBuiltins[pFunc->funcId].type;
      return funcMgtBuiltins[pFunc->funcId].translateFunc(pFunc, pMsg, msgLen);
    }
  }
  return TSDB_CODE_FUNC_NOT_BUILTIN_FUNTION;
}

EFuncReturnRows fmGetFuncReturnRows(SFunctionNode* pFunc) {
  if (NULL != funcMgtBuiltins[pFunc->funcId].estimateReturnRowsFunc) {
    return funcMgtBuiltins[pFunc->funcId].estimateReturnRowsFunc(pFunc);
  }
  return (fmIsIndefiniteRowsFunc(pFunc->funcId) || fmIsMultiRowsFunc(pFunc->funcId)) ? FUNC_RETURN_ROWS_INDEFINITE
                                                                                     : FUNC_RETURN_ROWS_NORMAL;
}

bool fmIsBuiltinFunc(const char* pFunc) {
  return NULL != taosHashGet(gFunMgtService.pFuncNameHashTable, pFunc, strlen(pFunc));
}

EFunctionType fmGetFuncType(const char* pFunc) {
  void* pVal = taosHashGet(gFunMgtService.pFuncNameHashTable, pFunc, strlen(pFunc));
  if (NULL != pVal) {
    return funcMgtBuiltins[*(int32_t*)pVal].type;
  }
  return FUNCTION_TYPE_UDF;
}

EFuncDataRequired fmFuncDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  if (fmIsUserDefinedFunc(pFunc->funcId) || pFunc->funcId < 0 || pFunc->funcId >= funcMgtBuiltinsNum) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
  if (NULL == funcMgtBuiltins[pFunc->funcId].dataRequiredFunc) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
  return funcMgtBuiltins[pFunc->funcId].dataRequiredFunc(pFunc, pTimeWindow);
}

EFuncDataRequired fmFuncDynDataRequired(int32_t funcId, void* pRes, STimeWindow* pTimeWindow) {
  if (fmIsUserDefinedFunc(funcId) || funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }

  const char* name = funcMgtBuiltins[funcId].name;
  if ((strcmp(name, "_group_key") == 0) || (strcmp(name, "_select_value") == 0)) {
    return FUNC_DATA_REQUIRED_NOT_LOAD;
  }

  if (funcMgtBuiltins[funcId].dynDataRequiredFunc == NULL) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  } else {
    return funcMgtBuiltins[funcId].dynDataRequiredFunc(pRes, pTimeWindow);
  }
}

int32_t fmGetFuncExecFuncs(int32_t funcId, SFuncExecFuncs* pFpSet) {
  if (fmIsUserDefinedFunc(funcId) || funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->getEnv = funcMgtBuiltins[funcId].getEnvFunc;
  pFpSet->init = funcMgtBuiltins[funcId].initFunc;
  pFpSet->process = funcMgtBuiltins[funcId].processFunc;
  pFpSet->finalize = funcMgtBuiltins[funcId].finalizeFunc;
  pFpSet->combine = funcMgtBuiltins[funcId].combineFunc;
  return TSDB_CODE_SUCCESS;
}

int32_t fmGetUdafExecFuncs(int32_t funcId, SFuncExecFuncs* pFpSet) {
  if (!fmIsUserDefinedFunc(funcId)) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->getEnv = udfAggGetEnv;
  pFpSet->init = udfAggInit;
  pFpSet->process = udfAggProcess;
  pFpSet->finalize = udfAggFinalize;
  return TSDB_CODE_SUCCESS;
}

int32_t fmGetScalarFuncExecFuncs(int32_t funcId, SScalarFuncExecFuncs* pFpSet) {
  if (fmIsUserDefinedFunc(funcId) || funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->process = funcMgtBuiltins[funcId].sprocessFunc;
  pFpSet->getEnv = funcMgtBuiltins[funcId].getEnvFunc;
  return TSDB_CODE_SUCCESS;
}

bool fmIsAggFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_AGG_FUNC); }

bool fmIsScalarFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_SCALAR_FUNC); }

bool fmIsVectorFunc(int32_t funcId) { return !fmIsScalarFunc(funcId) && !fmIsPseudoColumnFunc(funcId); }

bool fmIsSelectFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_SELECT_FUNC); }

bool fmIsTimelineFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_TIMELINE_FUNC); }

bool fmIsDateTimeFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_DATETIME_FUNC); }

bool fmIsPseudoColumnFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_PSEUDO_COLUMN_FUNC); }

bool fmIsScanPseudoColumnFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_SCAN_PC_FUNC); }

bool fmIsWindowPseudoColumnFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_WINDOW_PC_FUNC); }

bool fmIsWindowClauseFunc(int32_t funcId) { return fmIsAggFunc(funcId) || fmIsWindowPseudoColumnFunc(funcId); }

bool fmIsIndefiniteRowsFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_INDEFINITE_ROWS_FUNC); }


bool fmIsSpecialDataRequiredFunc(int32_t funcId) {
  return isSpecificClassifyFunc(funcId, FUNC_MGT_SPECIAL_DATA_REQUIRED);
}

bool fmIsDynamicScanOptimizedFunc(int32_t funcId) {
  return isSpecificClassifyFunc(funcId, FUNC_MGT_DYNAMIC_SCAN_OPTIMIZED);
}

bool fmIsMultiResFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_MULTI_RES_FUNC); }

bool fmIsRepeatScanFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_REPEAT_SCAN_FUNC); }

bool fmIsUserDefinedFunc(int32_t funcId) { return funcId > FUNC_UDF_ID_START; }

bool fmIsForbidFillFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_FORBID_FILL_FUNC); }

bool fmIsIntervalInterpoFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_INTERVAL_INTERPO_FUNC); }

bool fmIsForbidStreamFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_FORBID_STREAM_FUNC); }

bool fmIsSystemInfoFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_SYSTEM_INFO_FUNC); }

bool fmIsImplicitTsFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_IMPLICIT_TS_FUNC); }

bool fmIsClientPseudoColumnFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_CLIENT_PC_FUNC); }

bool fmIsMultiRowsFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_MULTI_ROWS_FUNC); }

bool fmIsKeepOrderFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_KEEP_ORDER_FUNC); }

bool fmIsCumulativeFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_CUMULATIVE_FUNC); }

bool fmIsForbidSysTableFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_FORBID_SYSTABLE_FUNC); }

bool fmIsInterpFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNCTION_TYPE_INTERP == funcMgtBuiltins[funcId].type;
}

bool fmIsInterpPseudoColumnFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_INTERP_PC_FUNC); }

bool fmIsLastRowFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNCTION_TYPE_LAST_ROW == funcMgtBuiltins[funcId].type;
}

bool fmIsNotNullOutputFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNCTION_TYPE_LAST == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_LAST_PARTIAL == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_LAST_MERGE == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_FIRST == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_FIRST_PARTIAL == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_FIRST_MERGE == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_COUNT == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_HYPERLOGLOG == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_HYPERLOGLOG_PARTIAL == funcMgtBuiltins[funcId].type ||
         FUNCTION_TYPE_HYPERLOGLOG_MERGE == funcMgtBuiltins[funcId].type;
}

bool fmIsSelectValueFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNCTION_TYPE_SELECT_VALUE == funcMgtBuiltins[funcId].type;
}

bool fmIsGroupKeyFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNCTION_TYPE_GROUP_KEY == funcMgtBuiltins[funcId].type;
}

bool fmIsBlockDistFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNCTION_TYPE_BLOCK_DIST == funcMgtBuiltins[funcId].type;
}

void fmFuncMgtDestroy() {
  void* m = gFunMgtService.pFuncNameHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr((void**)&gFunMgtService.pFuncNameHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}

#ifdef BUILD_NO_CALL
int32_t fmSetInvertFunc(int32_t funcId, SFuncExecFuncs* pFpSet) {
  if (fmIsUserDefinedFunc(funcId) || funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->process = funcMgtBuiltins[funcId].invertFunc;
  return TSDB_CODE_SUCCESS;
}

int32_t fmSetNormalFunc(int32_t funcId, SFuncExecFuncs* pFpSet) {
  if (fmIsUserDefinedFunc(funcId) || funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->process = funcMgtBuiltins[funcId].processFunc;
  return TSDB_CODE_SUCCESS;
}

bool fmIsInvertible(int32_t funcId) {
  bool res = false;
  switch (funcMgtBuiltins[funcId].type) {
    case FUNCTION_TYPE_COUNT:
    case FUNCTION_TYPE_SUM:
    case FUNCTION_TYPE_STDDEV:
    case FUNCTION_TYPE_AVG:
    case FUNCTION_TYPE_WSTART:
    case FUNCTION_TYPE_WEND:
    case FUNCTION_TYPE_WDURATION:
      res = true;
      break;
    default:
      break;
  }
  return res;
}
#endif

// function has same input/output type
bool fmIsSameInOutType(int32_t funcId) {
  bool res = false;
  switch (funcMgtBuiltins[funcId].type) {
    case FUNCTION_TYPE_MAX:
    case FUNCTION_TYPE_MIN:
    case FUNCTION_TYPE_TOP:
    case FUNCTION_TYPE_BOTTOM:
    case FUNCTION_TYPE_FIRST:
    case FUNCTION_TYPE_LAST:
    case FUNCTION_TYPE_SAMPLE:
    case FUNCTION_TYPE_TAIL:
    case FUNCTION_TYPE_UNIQUE:
      res = true;
      break;
    default:
      break;
  }
  return res;
}

bool fmIsConstantResFunc(SFunctionNode* pFunc) {
  SNode* pNode;
  FOREACH(pNode, pFunc->pParameterList) {
    if (nodeType(pNode) != QUERY_NODE_VALUE) {
      return false;
    }
  }
  return true;
}

bool fmIsSkipScanCheckFunc(int32_t funcId) {
  return isSpecificClassifyFunc(funcId, FUNC_MGT_SKIP_SCAN_CHECK_FUNC);
}

void getLastCacheDataType(SDataType* pType) {
  pType->bytes = getFirstLastInfoSize(pType->bytes) + VARSTR_HEADER_SIZE;
  pType->type = TSDB_DATA_TYPE_BINARY;
}

static int32_t getFuncInfo(SFunctionNode* pFunc) {
  char msg[128] = {0};
  return fmGetFuncInfo(pFunc, msg, sizeof(msg));
}

SFunctionNode* createFunction(const char* pName, SNodeList* pParameterList) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }
  snprintf(pFunc->functionName, sizeof(pFunc->functionName), "%s", pName);
  pFunc->pParameterList = pParameterList;
  if (TSDB_CODE_SUCCESS != getFuncInfo(pFunc)) {
    pFunc->pParameterList = NULL;
    nodesDestroyNode((SNode*)pFunc);
    return NULL;
  }
  return pFunc;
}

static SNode* createColumnByFunc(const SFunctionNode* pFunc) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  strcpy(pCol->colName, pFunc->node.aliasName);
  pCol->node.resType = pFunc->node.resType;
  return (SNode*)pCol;
}

bool fmIsDistExecFunc(int32_t funcId) {
  if (fmIsUserDefinedFunc(funcId)) {
    return false;
  }
  if (!fmIsVectorFunc(funcId)) {
    return true;
  }
  return (NULL != funcMgtBuiltins[funcId].pPartialFunc && NULL != funcMgtBuiltins[funcId].pMergeFunc);
}

static int32_t createPartialFunction(const SFunctionNode* pSrcFunc, SFunctionNode** pPartialFunc) {
  SNodeList* pParameterList = nodesCloneList(pSrcFunc->pParameterList);
  if (NULL == pParameterList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pPartialFunc = createFunction(funcMgtBuiltins[pSrcFunc->funcId].pPartialFunc, pParameterList);
  if (NULL == *pPartialFunc) {
    nodesDestroyList(pParameterList);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  char name[TSDB_FUNC_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_POINTER_PRINT_BYTES + 1] = {0};
  int32_t len = snprintf(name, sizeof(name) - 1, "%s.%p", (*pPartialFunc)->functionName, pSrcFunc);
  taosCreateMD5Hash(name, len);
  strncpy((*pPartialFunc)->node.aliasName, name, TSDB_COL_NAME_LEN - 1);
  return TSDB_CODE_SUCCESS;
}

static int32_t createMergeFuncPara(const SFunctionNode* pSrcFunc, const SFunctionNode* pPartialFunc,
                                   SNodeList** pParameterList) {
  SNode* pRes = createColumnByFunc(pPartialFunc);
  if (NULL != funcMgtBuiltins[pSrcFunc->funcId].createMergeParaFuc) {
    return funcMgtBuiltins[pSrcFunc->funcId].createMergeParaFuc(pSrcFunc->pParameterList, pRes, pParameterList);
  } else {
    return nodesListMakeStrictAppend(pParameterList, pRes);
  }
}

static int32_t createMidFunction(const SFunctionNode* pSrcFunc, const SFunctionNode* pPartialFunc,
                                   SFunctionNode** pMidFunc) {
  SNodeList*     pParameterList = NULL;
  SFunctionNode* pFunc = NULL;

  int32_t code = createMergeFuncPara(pSrcFunc, pPartialFunc, &pParameterList);
  if (TSDB_CODE_SUCCESS == code) {
    if(funcMgtBuiltins[pSrcFunc->funcId].pMiddleFunc != NULL){
      pFunc = createFunction(funcMgtBuiltins[pSrcFunc->funcId].pMiddleFunc, pParameterList);
    }else{
      pFunc = createFunction(funcMgtBuiltins[pSrcFunc->funcId].pMergeFunc, pParameterList);
    }
    if (NULL == pFunc) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    strcpy(pFunc->node.aliasName, pPartialFunc->node.aliasName);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pMidFunc = pFunc;
  } else {
    nodesDestroyList(pParameterList);
  }

  return code;
}

static int32_t createMergeFunction(const SFunctionNode* pSrcFunc, const SFunctionNode* pPartialFunc,
                                   SFunctionNode** pMergeFunc) {
  SNodeList*     pParameterList = NULL;
  SFunctionNode* pFunc = NULL;

  int32_t code = createMergeFuncPara(pSrcFunc, pPartialFunc, &pParameterList);
  if (TSDB_CODE_SUCCESS == code) {
    pFunc = createFunction(funcMgtBuiltins[pSrcFunc->funcId].pMergeFunc, pParameterList);
    if (NULL == pFunc) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    // overwrite function restype set by translate function
    if (fmIsSameInOutType(pSrcFunc->funcId)) {
      pFunc->node.resType = pSrcFunc->node.resType;
    }
    strcpy(pFunc->node.aliasName, pSrcFunc->node.aliasName);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pMergeFunc = pFunc;
  } else {
    nodesDestroyList(pParameterList);
  }

  return code;
}

int32_t fmGetDistMethod(const SFunctionNode* pFunc, SFunctionNode** pPartialFunc, SFunctionNode** pMidFunc, SFunctionNode** pMergeFunc) {
  if (!fmIsDistExecFunc(pFunc->funcId)) {
    return TSDB_CODE_FAILED;
  }

  int32_t code = createPartialFunction(pFunc, pPartialFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = createMidFunction(pFunc, *pPartialFunc, pMidFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createMergeFunction(pFunc, *pPartialFunc, pMergeFunc);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)*pPartialFunc);
    nodesDestroyNode((SNode*)*pMidFunc);
    nodesDestroyNode((SNode*)*pMergeFunc);
  }

  return code;
}

char* fmGetFuncName(int32_t funcId) {
  if (fmIsUserDefinedFunc(funcId) || funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return taosStrdup("invalid function");
  }
  return  taosStrdup(funcMgtBuiltins[funcId].name);
}
