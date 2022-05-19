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
int32_t functionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t dummyProcess(SqlFunctionCtx* UNUSED_PARAM(pCtx));
int32_t functionFinalizeWithResultBuf(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, char* finalResult);

EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool getCountFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t countFunction(SqlFunctionCtx *pCtx);
int32_t countInvertFunction(SqlFunctionCtx *pCtx);

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool getSumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t sumFunction(SqlFunctionCtx *pCtx);
int32_t sumInvertFunction(SqlFunctionCtx *pCtx);

bool minmaxFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
bool getMinmaxFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t minFunction(SqlFunctionCtx* pCtx);
int32_t maxFunction(SqlFunctionCtx *pCtx);
int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getAvgFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool avgFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t avgFunction(SqlFunctionCtx* pCtx);
int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t avgInvertFunction(SqlFunctionCtx* pCtx);

bool getStddevFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool stddevFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stddevFunction(SqlFunctionCtx* pCtx);
int32_t stddevFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t stddevInvertFunction(SqlFunctionCtx* pCtx);

bool getLeastSQRFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool leastSQRFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t leastSQRFunction(SqlFunctionCtx* pCtx);
int32_t leastSQRFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t leastSQRInvertFunction(SqlFunctionCtx* pCtx);

bool getPercentileFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool percentileFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t percentileFunction(SqlFunctionCtx *pCtx);
int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getDiffFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool diffFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo);
int32_t diffFunction(SqlFunctionCtx *pCtx);

bool getFirstLastFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t firstFunction(SqlFunctionCtx *pCtx);
int32_t lastFunction(SqlFunctionCtx *pCtx);
int32_t lastFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getTopBotFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv);
int32_t topFunction(SqlFunctionCtx *pCtx);
int32_t bottomFunction(SqlFunctionCtx *pCtx);
int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getSpreadFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool spreadFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t spreadFunction(SqlFunctionCtx* pCtx);
int32_t spreadFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getElapsedFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool elapsedFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t elapsedFunction(SqlFunctionCtx* pCtx);
int32_t elapsedFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getHistogramFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool histogramFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t histogramFunction(SqlFunctionCtx* pCtx);
int32_t histogramFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getHLLFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t hllFunction(SqlFunctionCtx* pCtx);
int32_t hllFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getStateFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool stateFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stateCountFunction(SqlFunctionCtx* pCtx);
int32_t stateDurationFunction(SqlFunctionCtx* pCtx);

bool getCsumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t csumFunction(SqlFunctionCtx* pCtx);

bool getMavgFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool mavgFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t mavgFunction(SqlFunctionCtx* pCtx);

bool getSampleFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool sampleFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t sampleFunction(SqlFunctionCtx* pCtx);
//int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getTailFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool tailFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo);
int32_t tailFunction(SqlFunctionCtx* pCtx);
int32_t tailFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getSelectivityFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_BUILTINSIMPL_H
