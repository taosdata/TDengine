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

typedef struct SFuncMgtService {
  SHashObj* pFuncNameHashTable;
} SFuncMgtService;

static SFuncMgtService gFunMgtService;

int32_t fmFuncMgtInit() {
  gFunMgtService.pFuncNameHashTable = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == gFunMgtService.pFuncNameHashTable) {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct SFuncDef {
  char name[TSDB_FUNC_NAME_LEN];
  int32_t maxNumOfParams;
  SFuncExecFuncs execFuncs;
} SFuncDef ;

FuncDef createFuncDef(const char* name, int32_t maxNumOfParams) {
  SFuncDef* pDef = calloc(1, sizeof(SFuncDef));
  if (NULL == pDef) {
    return NULL;
  }
  strcpy(pDef->name, name);
  pDef->maxNumOfParams = maxNumOfParams;
  return pDef;
}

FuncDef setOneParamSignature(FuncDef def, int64_t resDataType, int64_t paramDataType) {
  // todo
}

FuncDef setTwoParamsSignature(FuncDef def, int64_t resDataType, int64_t p1DataType, int64_t p2DataType) {
  // todo
}

FuncDef setFollowParamSignature(FuncDef def, int64_t paramDataType) {
  // todo
}

FuncDef setFollowParamsSignature(FuncDef def, int64_t p1DataType, int64_t p2DataType, int32_t followNo) {
  // todo
}

FuncDef setExecFuncs(FuncDef def, FExecGetEnv getEnv, FExecInit init, FExecProcess process, FExecFinalize finalize) {
  SFuncDef* pDef = (SFuncDef*)def;
  pDef->execFuncs.getEnv = getEnv;
  pDef->execFuncs.init = init;
  pDef->execFuncs.process = process;
  pDef->execFuncs.finalize = finalize;
  return def;
}

int32_t registerFunc(FuncDef func) {

}

int32_t fmGetFuncResultType(FuncMgtHandle handle, SFunctionNode* pFunc) {
  return TSDB_CODE_SUCCESS;
}

bool fmIsAggFunc(int32_t funcId) {
  return false;
}
