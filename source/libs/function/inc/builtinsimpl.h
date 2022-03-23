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

bool functionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
void functionFinalize(SqlFunctionCtx *pCtx);

bool getCountFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
void countFunction(SqlFunctionCtx *pCtx);

bool getSumFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
void sumFunction(SqlFunctionCtx *pCtx);

bool minFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
bool maxFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
bool getMinmaxFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
void minFunction(SqlFunctionCtx* pCtx);
void maxFunction(SqlFunctionCtx *pCtx);

bool getStddevFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
void stddevFunction(SqlFunctionCtx* pCtx);
void stddevFinalize(SqlFunctionCtx* pCtx);

bool getFirstLastFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
void firstFunction(SqlFunctionCtx *pCtx);
void lastFunction(SqlFunctionCtx *pCtx);

void valFunction(SqlFunctionCtx *pCtx);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_BUILTINSIMPL_H
