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

#ifndef _TD_BUILDINS_H_
#define _TD_BUILDINS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "functionMgtInt.h"

struct SFunctionParaInfo;

typedef int32_t (*FTranslateFunc)(SFunctionNode* pFunc, char* pErrBuf, int32_t len);
typedef EFuncDataRequired (*FFuncDataRequired)(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
typedef int32_t (*FCreateMergeFuncParameters)(SNodeList* pRawParameters, SNode* pPartialRes, SNodeList** pParameters);
typedef EFuncDataRequired (*FFuncDynDataRequired)(void* pRes, SDataBlockInfo* pBlocInfo);
typedef EFuncReturnRows (*FEstimateReturnRows)(SFunctionNode* pFunc);

#define MAX_FUNC_PARA_NUM 16
#define MAX_FUNC_PARA_FIXED_VALUE_NUM 16
typedef struct SParamRange {
  int64_t  iMinVal;
  int64_t  iMaxVal;
} SParamRange;

typedef struct SParamInfo {
  bool        isLastParam;
  int8_t      startParam;
  int8_t      endParam;
  uint64_t    validDataType;
  uint64_t    validNodeType;
  uint64_t    paramAttribute;
  uint8_t     valueRangeFlag; // 0 for no range and no fixed value, 1 for value has range, 2 for fixed value
  uint8_t     fixedValueSize;
  char*       fixedStrValue[MAX_FUNC_PARA_FIXED_VALUE_NUM]; // used for input parameter
  int64_t     fixedNumValue[MAX_FUNC_PARA_FIXED_VALUE_NUM]; // used for input parameter
  SParamRange range;
} SParamInfo;

typedef struct SFunctionParaInfo {
  int8_t     minParamNum;
  int8_t     maxParamNum;
  uint8_t    paramInfoPattern;
  SParamInfo inputParaInfo[MAX_FUNC_PARA_NUM][MAX_FUNC_PARA_NUM];
  SParamInfo outputParaInfo;
} SFunctionParaInfo;

typedef struct SBuiltinFuncDefinition {
  const char*                name;
  EFunctionType              type;
  uint64_t                   classification;
  SFunctionParaInfo          parameters;
  FTranslateFunc             translateFunc;
  FFuncDataRequired          dataRequiredFunc;
  FFuncDynDataRequired       dynDataRequiredFunc;
  FExecGetEnv                getEnvFunc;
  FExecInit                  initFunc;
  FExecProcess               processFunc;
  FScalarExecProcess         sprocessFunc;
  FExecFinalize              finalizeFunc;
  FExecCleanUp               cleanupFunc;
#ifdef BUILD_NO_CALL
  FExecProcess               invertFunc;
#endif
  FExecCombine               combineFunc;
  const char*                pPartialFunc;
  const char*                pMiddleFunc;
  const char*                pMergeFunc;
  const char*                pStateFunc;
  FCreateMergeFuncParameters createMergeParaFuc;
  FEstimateReturnRows        estimateReturnRowsFunc;
  processFuncByRow           processFuncByRow;
} SBuiltinFuncDefinition;

extern const SBuiltinFuncDefinition funcMgtBuiltins[];
extern const int32_t                funcMgtBuiltinsNum;

#ifdef __cplusplus
}
#endif

#endif /*_TD_BUILDINS_H_*/
