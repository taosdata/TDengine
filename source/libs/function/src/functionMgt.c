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

#include "functionMgtInt.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"
#include "builtins.h"

typedef struct SFuncMgtService {
  SHashObj* pFuncNameHashTable;
} SFuncMgtService;

static SFuncMgtService gFunMgtService;
static TdThreadOnce functionHashTableInit = PTHREAD_ONCE_INIT;
static int32_t initFunctionCode = 0;

static void doInitFunctionHashTable() {
  gFunMgtService.pFuncNameHashTable = taosHashInit(funcMgtBuiltinsNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == gFunMgtService.pFuncNameHashTable) {
    initFunctionCode = TSDB_CODE_FAILED;
    return;
  }

  for (int32_t i = 0; i < funcMgtBuiltinsNum; ++i) {
    if (TSDB_CODE_SUCCESS != taosHashPut(gFunMgtService.pFuncNameHashTable, funcMgtBuiltins[i].name, strlen(funcMgtBuiltins[i].name), &i, sizeof(int32_t))) {
      initFunctionCode = TSDB_CODE_FAILED;
      return;
    }
  }
}

int32_t fmFuncMgtInit() {
  taosThreadOnce(&functionHashTableInit, doInitFunctionHashTable);
  return initFunctionCode;
}

int32_t fmGetFuncInfo(const char* pFuncName, int32_t* pFuncId, int32_t* pFuncType) {
  void* pVal = taosHashGet(gFunMgtService.pFuncNameHashTable, pFuncName, strlen(pFuncName));
  if (NULL == pVal) {
    return TSDB_CODE_FAILED;
  }
  *pFuncId = *(int32_t*)pVal;
  if (*pFuncId < 0 || *pFuncId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  *pFuncType = funcMgtBuiltins[*pFuncId].type;
  return TSDB_CODE_SUCCESS;
}

int32_t fmGetFuncResultType(SFunctionNode* pFunc) {
  if (pFunc->funcId < 0 || pFunc->funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  return funcMgtBuiltins[pFunc->funcId].checkFunc(pFunc);
}

int32_t fmGetFuncExecFuncs(int32_t funcId, SFuncExecFuncs* pFpSet) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->getEnv = funcMgtBuiltins[funcId].getEnvFunc;
  pFpSet->init = funcMgtBuiltins[funcId].initFunc;
  pFpSet->process = funcMgtBuiltins[funcId].processFunc;
  pFpSet->finalize = funcMgtBuiltins[funcId].finalizeFunc;
  return TSDB_CODE_SUCCESS;
}

int32_t fmGetScalarFuncExecFuncs(int32_t funcId, SScalarFuncExecFuncs* pFpSet) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return TSDB_CODE_FAILED;
  }
  pFpSet->process = funcMgtBuiltins[funcId].sprocessFunc;
  return TSDB_CODE_SUCCESS;
}

bool fmIsAggFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNC_MGT_TEST_MASK(funcMgtBuiltins[funcId].classification, FUNC_MGT_AGG_FUNC);
}

void fmFuncMgtDestroy() {
  void* m = gFunMgtService.pFuncNameHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr((void**)&gFunMgtService.pFuncNameHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}
