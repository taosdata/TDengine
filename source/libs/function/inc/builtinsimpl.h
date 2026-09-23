/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "functionResInfoInt.h"

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc, int32_t* nElems);
int32_t i8VectorCmpAVX2(const void* pData, int32_t numOfRows, bool isMinFunc, bool signVal, int64_t* res);
int32_t i16VectorCmpAVX2(const void* pData, int32_t numOfRows, bool isMinFunc, bool signVal, int64_t* res);
int32_t i32VectorCmpAVX2(const void* pData, int32_t numOfRows, bool isMinFunc, bool signVal, int64_t* res);
int32_t floatVectorCmpAVX2(const float* pData, int32_t numOfRows, bool isMinFunc, float* res);
int32_t doubleVectorCmpAVX2(const double* pData, int32_t numOfRows, bool isMinFunc, double* res);

int32_t saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);
int32_t updateTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);
int32_t loadTupleData(SqlFunctionCtx* pCtx, const STuplePos* pPos, char** value);

int32_t functionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t functionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t functionFinalizeWithResultBuf(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, char* finalResult);
int32_t combineFunction(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

// ---- shared DISTINCT dedup for aggregates that keep a result struct in their inter-buffer ----
//
// The set handle is appended after the function's own state, so that state keeps offset 0 and its
// existing accessors are unaffected:
//
//   [ SSumRes | SAvgRes | ... ]  [ SDistinctValueSet ]
//
// Each result row owns its own set, so per-window and per-group dedup fall out naturally, and two
// DISTINCT aggregates on different columns each keep their own set rather than contending over one
// shared DistinctFilter key.
//
// Memory is bounded by SSpillDistinctSet, which spills to tsTempDir past a byte budget. That makes
// accumulation two-phase:
//
//   * while resident, distinctValueSetCheckAdd() answers authoritatively and the caller accumulates
//     inline as usual;
//   * once spilled it can no longer answer, so it reports "do not accumulate" for every row and the
//     values are recorded on disk instead. The caller must then zero its accumulator before
//     folding the true distinct values in its finalize step via distinctValueSetReplay().
//
// distinctValueSetNeedsReplay() reports which phase applies.
typedef struct SDistinctValueSet {
  struct SSpillDistinctSet* pSet;
} SDistinctValueSet;

// Fold one distinct value into the aggregate's state. Called once per value from the replay, and
// also directly from each aggregate's in-line accumulate loop.
//
// pData is borrowed for the duration of the call only: on the replay path it points into a page
// buffer that is released once the page has been walked, and in-line it points into the input
// column. Accumulate from it; do not retain it.
//
// No length is passed: the aggregates using this hook (SUM/AVG) accept numeric and decimal input
// exclusively, so every value is fixed-width and the width is recoverable from pCtx->inputType.
// Taking a length here would be actively unsafe -- the in-line accumulate loop has no stored length
// to give, so it would have to pass a placeholder while the spilled replay passed the real one, and
// an implementation that read it would then behave differently on the two paths. A var-width
// aggregate must add the parameter and make BOTH call sites supply a truthful value.
typedef int32_t (*FDistinctAccumFn)(SqlFunctionCtx* pCtx, const char* pData);

// Extra bytes a DISTINCT variant adds to a function's calcMemSize (0 when not DISTINCT).
int32_t distinctValueSetExtraSize(const struct SFunctionNode* pFunc);
// Returns NULL when this ctx is not a DISTINCT variant, so it doubles as the "is distinct" test.
// baseSize is the function's own state size, used only as a lower bound.
SDistinctValueSet* distinctValueSetGet(SqlFunctionCtx* pCtx, int32_t baseSize);
// Records the value at rowIdx and reports via *pAccumulate whether the caller should accumulate it
// now: true only when the set is still resident and the value was absent. Always false once
// spilled -- see distinctValueSetNeedsReplay().
//
// Returns a code so a failure cannot be mistaken for "duplicate". *pAccumulate is only meaningful
// on TSDB_CODE_SUCCESS; on any error the value was not recorded and the whole accumulation must
// abort.
int32_t distinctValueSetCheckAdd(SDistinctValueSet* pSet, SColumnInfoData* pCol, int32_t rowIdx,
                                 SqlFunctionCtx* pCtx, bool* pAccumulate);
// True when the set spilled, so inline accumulation is incomplete and finalize must replay.
bool    distinctValueSetNeedsReplay(const SDistinctValueSet* pSet);
// Replays every distinct value through fn. Call from finalize when needsReplay() is true; the
// caller is responsible for having zeroed its accumulator beforehand.
int32_t distinctValueSetReplay(SqlFunctionCtx* pCtx, SDistinctValueSet* pSet, FDistinctAccumFn fn);
void    distinctValueSetCleanup(SqlFunctionCtx* pCtx, int32_t baseSize);


EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool              getCountFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t           countFunction(SqlFunctionCtx* pCtx);
void              countFunctionCleanup(SqlFunctionCtx* pCtx);
int32_t           countDistinctFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
EFuncDataRequired distinctStatisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool              getSumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
void              sumFunctionCleanup(SqlFunctionCtx* pCtx);
void              avgFunctionCleanup(SqlFunctionCtx* pCtx);
int32_t           sumFunction(SqlFunctionCtx* pCtx);
int32_t           sumDistinctFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

int32_t sumCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

int32_t minmaxFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
bool    getMinmaxFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t minFunction(SqlFunctionCtx* pCtx);
int32_t maxFunction(SqlFunctionCtx* pCtx);
int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t minCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t maxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getAvgFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t avgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t avgFunction(SqlFunctionCtx* pCtx);
int32_t avgFunctionMerge(SqlFunctionCtx* pCtx);
int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t avgDistinctFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t avgPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

int32_t avgCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getAvgInfoSize(SFunctionNode* pFunc);

bool    getStdFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t stdFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stdFunction(SqlFunctionCtx* pCtx);
int32_t stdFunctionMerge(SqlFunctionCtx* pCtx);
int32_t stddevFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t stdvarFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t stddevsampFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t stdvarsampFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t stdPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

int32_t stdCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getStdInfoSize();

bool    gconcatGetFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t gconcatFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t gconcatFunction(SqlFunctionCtx* pCtx);
int32_t gconcatFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getLeastSQRFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t leastSQRFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t leastSQRFunction(SqlFunctionCtx* pCtx);
int32_t leastSQRFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t leastSQRCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getPercentileFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t percentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t percentileFunction(SqlFunctionCtx* pCtx);
int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
void    percentileFunctionCleanupExt(SqlFunctionCtx* pCtx);

bool    getApercentileFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t apercentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t apercentileFunction(SqlFunctionCtx* pCtx);
int32_t apercentileFunctionMerge(SqlFunctionCtx* pCtx);
int32_t apercentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t apercentilePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t apercentileCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getApercentileMaxSize();

bool    getDiffFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t diffFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t diffFunction(SqlFunctionCtx* pCtx);
int32_t diffFunctionByRow(SArray* pCtx);

bool    getLagFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t lagFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t lagFunction(SqlFunctionCtx* pCtx);
int32_t lagFunctionByRow(SArray* pCtx);
void    lagLeadFunctionCleanupExt(SqlFunctionCtx* pCtx);

bool    getLeadFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t leadFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t leadFunction(SqlFunctionCtx* pCtx);
int32_t leadFunctionByRow(SArray* pCtx);

bool    getFillforwardFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t fillforwardFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t fillforwardFunction(SqlFunctionCtx* pCtx);
int32_t fillforwardFunctionByRow(SArray* pCtx);
void    fillforwardFunctionCleanupExt(SqlFunctionCtx* pCtx);

bool    getForecastConfEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv);
bool    getCorrFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t corrFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t corrFunction(SqlFunctionCtx* pCtx);
int32_t corrFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t corrScalarFunction(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput);
int32_t corrPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getCorrInfoSize();
int32_t corrFuncMerge(SqlFunctionCtx* pCtx);
int32_t hasNullFunction(SqlFunctionCtx* pCtx);

bool    getDerivativeFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t derivativeFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t derivativeFunction(SqlFunctionCtx* pCtx);
int32_t derivativeFunctionByRow(SArray* pCtxArray);

bool    getIrateFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t irateFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t irateFunction(SqlFunctionCtx* pCtx);
int32_t irateFunctionMerge(SqlFunctionCtx* pCtx);
int32_t irateFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t iratePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getIrateInfoSize(int32_t pkBytes);

int32_t cachedLastRowFunction(SqlFunctionCtx* pCtx);

bool              getFirstLastFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t           firstLastFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t           firstFunction(SqlFunctionCtx* pCtx);
int32_t           firstFunctionMerge(SqlFunctionCtx* pCtx);
int32_t           lastFunction(SqlFunctionCtx* pCtx);
int32_t           lastFunctionMerge(SqlFunctionCtx* pCtx);
int32_t           firstLastFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t           firstLastPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t           firstCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t           lastCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t           getFirstLastInfoSize(int32_t resBytes, int32_t pkBytes);
int32_t           getMaxValueBytes(int32_t type, int32_t bytes);
EFuncDataRequired maxDynDataReq(void* pRes, SDataBlockInfo* pBlockInfo);
EFuncDataRequired firstDynDataReq(void* pRes, SDataBlockInfo* pBlockInfo);
EFuncDataRequired lastDynDataReq(void* pRes, SDataBlockInfo* pBlockInfo);

int32_t lastRowFunction(SqlFunctionCtx* pCtx);

bool    getTopBotFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv);
int32_t topBotFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t topFunction(SqlFunctionCtx* pCtx);
int32_t bottomFunction(SqlFunctionCtx* pCtx);
int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t topCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t bottomCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getTopBotInfoSize(int64_t numOfItems);

bool    getSpreadFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t spreadFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t spreadFunction(SqlFunctionCtx* pCtx);
int32_t spreadFunctionMerge(SqlFunctionCtx* pCtx);
int32_t spreadFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t spreadPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getSpreadInfoSize();
int32_t spreadCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getElapsedFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t elapsedFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t elapsedFunction(SqlFunctionCtx* pCtx);
int32_t elapsedFunctionMerge(SqlFunctionCtx* pCtx);
int32_t elapsedFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t elapsedPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getElapsedInfoSize();
int32_t elapsedCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getHistogramFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t histogramFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t histogramFunction(SqlFunctionCtx* pCtx);
int32_t histogramFunctionPartial(SqlFunctionCtx* pCtx);
int32_t histogramFunctionMerge(SqlFunctionCtx* pCtx);
int32_t histogramFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t histogramPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getHistogramInfoSize();
int32_t histogramCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getHLLFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t hllFunction(SqlFunctionCtx* pCtx);
int32_t hllFunctionMerge(SqlFunctionCtx* pCtx);
int32_t hllFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t hllPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getHLLInfoSize();
int32_t hllCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getStateFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t stateFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stateCountFunction(SqlFunctionCtx* pCtx);
int32_t stateDurationFunction(SqlFunctionCtx* pCtx);

bool    getCsumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t csumFunction(SqlFunctionCtx* pCtx);

bool    getMavgFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t mavgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t mavgFunction(SqlFunctionCtx* pCtx);

bool    getSampleFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t sampleFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t sampleFunction(SqlFunctionCtx* pCtx);
int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getTailFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t tailFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t tailFunction(SqlFunctionCtx* pCtx);
int32_t tailFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getUniqueFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t uniqueFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t uniqueFunction(SqlFunctionCtx* pCtx);

bool    getModeFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t modeFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t modeFunction(SqlFunctionCtx* pCtx);
int32_t modeFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
void    modeFunctionCleanupExt(SqlFunctionCtx* pCtx);

bool    getTwaFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t twaFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t twaFunction(SqlFunctionCtx* pCtx);
int32_t twaFinalize(struct SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getSelectivityFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool getHasNullFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);

int32_t blockDistSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t blockDistFunction(SqlFunctionCtx* pCtx);
int32_t blockDistFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getGroupKeyFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t groupKeyFunction(SqlFunctionCtx* pCtx);
int32_t groupKeyFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t groupKeyCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t groupConstValueFunction(SqlFunctionCtx* pCtx);
int32_t groupConstValueFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

int32_t blockDBUsageSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t blockDBUsageFunction(SqlFunctionCtx* pCtx);
int32_t blockDBUsageFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_BUILTINSIMPL_H
