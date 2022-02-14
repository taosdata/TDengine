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

int32_t fmFuncMgtInit() {
  gFunMgtService.pFuncNameHashTable = taosHashInit(funcMgtBuiltinsNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == gFunMgtService.pFuncNameHashTable) {
    return TSDB_CODE_FAILED;
  }
  for (int32_t i = 0; i < funcMgtBuiltinsNum; ++i) {
    if (TSDB_CODE_SUCCESS != taosHashPut(gFunMgtService.pFuncNameHashTable, funcMgtBuiltins[i].name, strlen(funcMgtBuiltins[i].name), &i, sizeof(int32_t))) {
      return TSDB_CODE_FAILED;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t fmGetHandle(FuncMgtHandle* pHandle) {
  *pHandle = &gFunMgtService;
  return TSDB_CODE_SUCCESS;
}

int32_t fmGetFuncInfo(FuncMgtHandle handle, const char* pFuncName, int32_t* pFuncId, int32_t* pFuncType) {
  SFuncMgtService* pService = (SFuncMgtService*)handle;
  void* pVal = taosHashGet(pService->pFuncNameHashTable, pFuncName, strlen(pFuncName));
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

bool fmIsAggFunc(int32_t funcId) {
  if (funcId < 0 || funcId >= funcMgtBuiltinsNum) {
    return false;
  }
  return FUNC_MGT_TEST_MASK(funcMgtBuiltins[funcId].classification, FUNC_MGT_AGG_FUNC);
}
