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
#ifndef STREAM_INTERVAL_H
#define STREAM_INTERVAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
#include "operator.h"

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
  STimeWindow calWin;
} SPullWindowInfo;

typedef struct STimeFillRange {
  TSKEY    skey;
  TSKEY    ekey;
  uint64_t groupId;
  void*    pStartRow;
  void*    pEndRow;
} STimeFillRange;

int32_t     doStreamIntervalNonblockAggImpl(struct SOperatorInfo* pOperator, SSDataBlock* pBlock);
STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval);
int32_t     getNextQualifiedFinalWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                        TSKEY* primaryKeys, int32_t prevPosition);
int32_t     copyNewResult(SSHashObj** ppWinUpdated, SArray* pUpdated, __compar_fn_t compar);
int32_t     copyRecDataToBuff(TSKEY calStart, TSKEY calEnd, uint64_t uid, uint64_t version, EStreamType mode,
                              const SColumnInfoData* pPkColDataInfo, int32_t rowId, SRecDataInfo* pValueBuff,
                              int32_t buffLen);
int32_t     saveRecWindowToDisc(SSessionKey* pWinKey, uint64_t uid, EStreamType mode, STableTsDataState* pTsDataState,
                                SStreamAggSupporter* pAggSup);
int32_t     initNonBlockAggSupptor(SNonBlockAggSupporter* pNbSup, SInterval* pInterval, SOperatorInfo* downstream);
void        destroyNonBlockAggSupptor(SNonBlockAggSupporter* pNbSup);
int32_t     buildRetriveRequest(SExecTaskInfo* pTaskInfo, SStreamAggSupporter* pAggSup, STableTsDataState* pTsDataState,
                                SNonBlockAggSupporter* pNbSup);
int32_t     getChildIndex(SSDataBlock* pBlock);
void        adjustDownstreamBasicInfo(SOperatorInfo* downstream, struct SSteamOpBasicInfo* pBasic);
int32_t     processDataPullOver(SSDataBlock* pBlock, SSHashObj* pPullMap, SExecTaskInfo* pTaskInfo);
int32_t     doStreamNonblockFillNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
int32_t     doApplyStreamScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pSrcBlock, SSDataBlock* pDstBlock);
void        resetStreamFillInfo(SStreamFillOperatorInfo* pInfo);
void        removeDuplicateResult(SArray* pTsArrray, __compar_fn_t fn);
int32_t     keepBlockRowInStateBuf(SStreamFillOperatorInfo* pInfo, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock,
                                   TSKEY* tsCol, int32_t rowId, uint64_t groupId, int32_t rowSize);
void        setTimeSliceFillRule(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, TSKEY ts);
void        doStreamTimeSliceFillRange(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pRes);
void        resetTimeSlicePrevAndNextWindow(SStreamFillSupporter* pFillSup);
TSKEY       adustPrevTsKey(TSKEY pointTs, TSKEY rowTs, SInterval* pInterval);
TSKEY       adustEndTsKey(TSKEY pointTs, TSKEY rowTs, SInterval* pInterval);
void        destroyStreamFillOperatorInfo(void* param);
void        destroyStreamNonblockFillOperatorInfo(void* param);
int32_t     buildDeleteResult(SOperatorInfo* pOperator, TSKEY startTs, TSKEY endTs, uint64_t groupId, SSDataBlock* delRes);
void        setDeleteFillValueInfo(TSKEY start, TSKEY end, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo);
void        doStreamFillRange(SStreamFillInfo* pFillInfo, SStreamFillSupporter* pFillSup, SSDataBlock* pRes);
int32_t     initFillSupRowInfo(SStreamFillSupporter* pFillSup, SSDataBlock* pRes);
void        getStateKeepInfo(SNonBlockAggSupporter* pNbSup, bool isRecOp, int32_t* pNumRes, TSKEY* pTsRes);
int32_t     initStreamFillOperatorColumnMapInfo(SExprSupp* pExprSup, SOperatorInfo* pOperator);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_INTERVAL_H
