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

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
  STimeWindow calWin;
} SPullWindowInfo;

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
int32_t     initNonBlockAggSupptor(SNonBlockAggSupporter* pNbSup, SInterval* pInterval);
void        destroyNonBlockAggSupptor(SNonBlockAggSupporter* pNbSup);
int32_t     buildRetriveRequest(SExecTaskInfo* pTaskInfo, SStreamAggSupporter* pAggSup, STableTsDataState* pTsDataState,
                                SNonBlockAggSupporter* pNbSup);
int32_t checkAndSaveWinStateToDisc(int32_t startIndex, SArray* pUpdated, uint64_t uid, STableTsDataState* pTsDataState,
                                   SStreamAggSupporter* pAggSup);
int32_t getChildIndex(SSDataBlock* pBlock);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_INTERVAL_H
