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
#include "functionMgtInt.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"
#include "tudf.h"

typedef struct SFuncMgtService {
  SHashObj* pFuncNameHashTable;
} SFuncMgtService;

typedef struct SUdfInfo {
  SDataType outputDt;
  int8_t    funcType;
} SUdfInfo;

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
  void* pVal = taosHashGet(gFunMgtService.pFuncNameHashTable, pFunc->functionName, strlen(pFunc->functionName));
  if (NULL != pVal) {
    pFunc->funcId = *(int32_t*)pVal;
    pFunc->funcType = funcMgtBuiltins[pFunc->funcId].type;
    return funcMgtBuiltins[pFunc->funcId].translateFunc(pFunc, pMsg, msgLen);
  }
  return TSDB_CODE_FUNC_NOT_BUILTIN_FUNTION;
}

bool fmIsBuiltinFunc(const char* pFunc) {
  return NULL != taosHashGet(gFunMgtService.pFuncNameHashTable, pFunc, strlen(pFunc));
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

bool fmIsVectorFunc(int32_t funcId) { return !fmIsScalarFunc(funcId); }

bool fmIsSelectFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_SELECT_FUNC); }

bool fmIsTimelineFunc(int32_t funcId) { return isSpecificClassifyFunc(funcId, FUNC_MGT_TIMELINE_FUNC); }

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

void fmFuncMgtDestroy() {
  void* m = gFunMgtService.pFuncNameHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr((void**)&gFunMgtService.pFuncNameHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}

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
    case FUNCTION_TYPE_WSTARTTS:
    case FUNCTION_TYPE_WENDTS:
    case FUNCTION_TYPE_WDURATION:
      res = true;
      break;
    default:
      break;
  }
  return res;
}
