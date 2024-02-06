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

typedef struct SSumRes {
  union {
    int64_t  isum;
    uint64_t usum;
    double   dsum;
  };
  int16_t type;
  int64_t prevTs;
  bool    isPrevTsSet;
  bool    overflow;  // if overflow is true, dsum to be used for any type;
} SSumRes;

typedef struct SMinmaxResInfo {
  bool      assign;  // assign the first value or not
  int64_t   v;
  STuplePos tuplePos;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;
  int16_t   type;
} SMinmaxResInfo;

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc, int32_t* nElems);

int32_t     saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);
int32_t     updateTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);
const char* loadTupleData(SqlFunctionCtx* pCtx, const STuplePos* pPos);

bool    functionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t functionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t functionFinalizeWithResultBuf(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, char* finalResult);
int32_t combineFunction(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool              getCountFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t           countFunction(SqlFunctionCtx* pCtx);

#ifdef BUILD_NO_CALL
int32_t           countInvertFunction(SqlFunctionCtx* pCtx);
#endif

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
bool              getSumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t           sumFunction(SqlFunctionCtx* pCtx);

#ifdef BUILD_NO_CALL
int32_t           sumInvertFunction(SqlFunctionCtx* pCtx);
#endif

int32_t           sumCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    minmaxFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
bool    getMinmaxFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t minFunction(SqlFunctionCtx* pCtx);
int32_t maxFunction(SqlFunctionCtx* pCtx);
int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t minCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t maxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getAvgFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    avgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t avgFunction(SqlFunctionCtx* pCtx);
int32_t avgFunctionMerge(SqlFunctionCtx* pCtx);
int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t avgPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

#ifdef BUILD_NO_CALL
int32_t avgInvertFunction(SqlFunctionCtx* pCtx);
#endif

int32_t avgCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getAvgInfoSize();

bool    getStddevFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    stddevFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stddevFunction(SqlFunctionCtx* pCtx);
int32_t stddevFunctionMerge(SqlFunctionCtx* pCtx);
int32_t stddevFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t stddevPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

#ifdef BUILD_NO_CALL
int32_t stddevInvertFunction(SqlFunctionCtx* pCtx);
#endif

int32_t stddevCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getStddevInfoSize();

bool    getLeastSQRFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    leastSQRFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t leastSQRFunction(SqlFunctionCtx* pCtx);
int32_t leastSQRFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t leastSQRCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getPercentileFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    percentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t percentileFunction(SqlFunctionCtx* pCtx);
int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getApercentileFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    apercentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t apercentileFunction(SqlFunctionCtx* pCtx);
int32_t apercentileFunctionMerge(SqlFunctionCtx* pCtx);
int32_t apercentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t apercentilePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t apercentileCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getApercentileMaxSize();

bool    getDiffFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    diffFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t diffFunction(SqlFunctionCtx* pCtx);

bool    getDerivativeFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    derivativeFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t derivativeFunction(SqlFunctionCtx* pCtx);

bool    getIrateFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    irateFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo);
int32_t irateFunction(SqlFunctionCtx* pCtx);
int32_t irateFunctionMerge(SqlFunctionCtx* pCtx);
int32_t irateFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t iratePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getIrateInfoSize();

int32_t cachedLastRowFunction(SqlFunctionCtx* pCtx);

bool              getFirstLastFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t           firstFunction(SqlFunctionCtx* pCtx);
int32_t           firstFunctionMerge(SqlFunctionCtx* pCtx);
int32_t           lastFunction(SqlFunctionCtx* pCtx);
int32_t           lastFunctionMerge(SqlFunctionCtx* pCtx);
int32_t           firstLastFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t           firstLastPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t           firstCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t           lastCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t           getFirstLastInfoSize(int32_t resBytes);
EFuncDataRequired firstDynDataReq(void* pRes, STimeWindow* pTimeWindow);
EFuncDataRequired lastDynDataReq(void* pRes, STimeWindow* pTimeWindow);

int32_t lastRowFunction(SqlFunctionCtx* pCtx);

bool    getTopBotFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv);
bool    topBotFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t topFunction(SqlFunctionCtx* pCtx);
int32_t bottomFunction(SqlFunctionCtx* pCtx);
int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t topCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t bottomCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);
int32_t getTopBotInfoSize(int64_t numOfItems);

bool    getSpreadFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    spreadFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t spreadFunction(SqlFunctionCtx* pCtx);
int32_t spreadFunctionMerge(SqlFunctionCtx* pCtx);
int32_t spreadFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t spreadPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getSpreadInfoSize();
int32_t spreadCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getElapsedFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    elapsedFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t elapsedFunction(SqlFunctionCtx* pCtx);
int32_t elapsedFunctionMerge(SqlFunctionCtx* pCtx);
int32_t elapsedFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t elapsedPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t getElapsedInfoSize();
int32_t elapsedCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

bool    getHistogramFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    histogramFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
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
bool    stateFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t stateCountFunction(SqlFunctionCtx* pCtx);
int32_t stateDurationFunction(SqlFunctionCtx* pCtx);

bool    getCsumFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t csumFunction(SqlFunctionCtx* pCtx);

bool    getMavgFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    mavgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t mavgFunction(SqlFunctionCtx* pCtx);

bool    getSampleFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    sampleFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t sampleFunction(SqlFunctionCtx* pCtx);
int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getTailFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    tailFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t tailFunction(SqlFunctionCtx* pCtx);

bool    getUniqueFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    uniqueFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t uniqueFunction(SqlFunctionCtx* pCtx);

bool    getModeFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    modeFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t modeFunction(SqlFunctionCtx* pCtx);
int32_t modeFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getTwaFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool    twaFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t twaFunction(SqlFunctionCtx* pCtx);
int32_t twaFinalize(struct SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool getSelectivityFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);

bool    blockDistSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo);
int32_t blockDistFunction(SqlFunctionCtx* pCtx);
int32_t blockDistFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);

bool    getGroupKeyFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv);
int32_t groupKeyFunction(SqlFunctionCtx* pCtx);
int32_t groupKeyFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock);
int32_t groupKeyCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_BUILTINSIMPL_H
