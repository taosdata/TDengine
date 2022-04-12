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

#ifndef TDENGINE_BUILTINSIMPL_H
#define TDENGINE_BUILTINSIMPL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "function.h"
#include "functionMgt.h"

bool functionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
void functionFinalize(SqlFunctionCtx *pCtx);

EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool getCountFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t countFunction(SqlFunctionCtx *pCtx);

bool getSumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t sumFunction(SqlFunctionCtx *pCtx);

bool minFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
bool maxFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
bool getMinmaxFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t minFunction(SqlFunctionCtx* pCtx);
int32_t maxFunction(SqlFunctionCtx *pCtx);

bool getStddevFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool stddevFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stddevFunction(SqlFunctionCtx* pCtx);
void stddevFinalize(SqlFunctionCtx* pCtx);

bool getPercentileFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool percentileFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t percentileFunction(SqlFunctionCtx *pCtx);
void percentileFinalize(SqlFunctionCtx* pCtx);

bool getDiffFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool diffFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo);
int32_t diffFunction(SqlFunctionCtx *pCtx);

bool getFirstLastFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t firstFunction(SqlFunctionCtx *pCtx);
int32_t lastFunction(SqlFunctionCtx *pCtx);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_BUILTINSIMPL_H
