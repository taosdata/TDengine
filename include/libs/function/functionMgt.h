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

#ifndef _TD_FUNCTION_MGT_H_
#define _TD_FUNCTION_MGT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"

struct SqlFunctionCtx;
struct SResultRowEntryInfo;
struct STimeWindow;

typedef struct SFuncExecEnv {
  int32_t calcMemSize;
} SFuncExecEnv;

typedef void* FuncMgtHandle;
typedef bool (*FExecGetEnv)(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
typedef bool (*FExecInit)(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo* pResultCellInfo);
typedef void (*FExecProcess)(struct SqlFunctionCtx *pCtx);
typedef void (*FExecFinalize)(struct SqlFunctionCtx *pCtx);

typedef struct SFuncExecFuncs {
  FExecGetEnv getEnv;
  FExecInit init;
  FExecProcess process;
  FExecFinalize finalize;
} SFuncExecFuncs;

int32_t fmFuncMgtInit();

int32_t fmGetHandle(FuncMgtHandle* pHandle);

int32_t fmGetFuncId(FuncMgtHandle handle, const char* name);
int32_t fmGetFuncResultType(FuncMgtHandle handle, SFunctionNode* pFunc);
bool fmIsAggFunc(int32_t funcId);
bool fmIsStringFunc(int32_t funcId);
bool fmIsTimestampFunc(int32_t funcId);
bool fmIsTimelineFunc(int32_t funcId);
bool fmIsTimeorderFunc(int32_t funcId);
bool fmIsNonstandardSQLFunc(int32_t funcId);
int32_t fmFuncScanType(int32_t funcId);

int32_t fmGetFuncExecFuncs(FuncMgtHandle handle, int32_t funcId, SFuncExecFuncs* pFpSet);

#ifdef __cplusplus
}
#endif

#endif  // _TD_FUNCTION_MGT_H_
