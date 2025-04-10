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
#ifndef STREAM_SESSION_H
#define STREAM_SESSION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
#include "operator.h"
#include "streamexecutorInt.h"


int32_t doStreamSessionNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock);
void streamSessionNonblockReleaseState(SOperatorInfo* pOperator);
void streamSessionNonblockReloadState(SOperatorInfo* pOperator);

void streamStateNonblockReleaseState(SOperatorInfo* pOperator);
void streamStateNonblockReloadState(SOperatorInfo* pOperator);

void streamEventNonblockReleaseState(SOperatorInfo* pOperator);
void streamEventNonblockReloadState(SOperatorInfo* pOperator);

int32_t doStreamSessionNonblockAggNextImpl(SOperatorInfo* pOperator, SOptrBasicInfo* pBInfo, SSteamOpBasicInfo* pBasic,
                                           SStreamAggSupporter* pAggSup, STimeWindowAggSupp* pTwAggSup,
                                           SGroupResInfo* pGroupResInfo, SNonBlockAggSupporter* pNbSup,
                                           SExprSupp* pScalarSupp, SArray* pHistoryWins, SSDataBlock** ppRes);
int32_t updateSessionWindowInfo(SStreamAggSupporter* pAggSup, SResultWindowInfo* pWinInfo, TSKEY* pStartTs,
                                TSKEY* pEndTs, uint64_t groupId, int32_t rows, int32_t start, int64_t gap,
                                SSHashObj* pResultRows, SSHashObj* pStUpdated, SSHashObj* pStDeleted,
                                int32_t* pWinRos);
int32_t setSessionOutputBuf(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                            SResultWindowInfo* pCurWin, int32_t* pWinCode);
void streamSessionReleaseState(SOperatorInfo* pOperator);
void streamStateReleaseState(SOperatorInfo* pOperator);
void streamEventReleaseState(SOperatorInfo* pOperator);
int32_t compactSessionWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SSHashObj* pStUpdated,
                             SSHashObj* pStDeleted, bool addGap, int32_t* pWinNum, bool* pIsEnd);
int32_t compactStateWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SResultWindowInfo* pNextWin,
                           SSHashObj* pStUpdated, SSHashObj* pStDeleted);
int32_t getStateWindowInfoByKey(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SStateWindowInfo* pCurWin,
                                SStateWindowInfo* pNextWin);
bool compareWinStateKey(SStateKeys* left, SStateKeys* right);
void getNextStateWin(const SStreamAggSupporter* pAggSup, SStateWindowInfo* pNextWin, bool asc);
int32_t updateStateWindowInfo(SStreamAggSupporter* pAggSup, SStateWindowInfo* pWinInfo, SStateWindowInfo* pNextWin,
                              TSKEY* pTs, uint64_t groupId, SColumnInfoData* pKeyCol, int32_t rows, int32_t start,
                              bool* allEqual, SSHashObj* pResultRows, SSHashObj* pSeUpdated, SSHashObj* pSeDeleted,
                              int32_t* pWinRows);
int32_t setStateOutputBuf(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, char* pKeyData,
                          SStateWindowInfo* pCurWin, SStateWindowInfo* pNextWin, int32_t* pWinCode);
int32_t doStreamStateNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock);
int32_t doStreamEventNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock);
int32_t compactEventWindow(SOperatorInfo* pOperator, SEventWindowInfo* pCurWin, SSHashObj* pStUpdated,
                           SSHashObj* pStDeleted, bool* pIsEnd);
void setEventWindowFlag(SStreamAggSupporter* pAggSup, SEventWindowInfo* pWinInfo);
int32_t updateEventWindowInfo(SStreamAggSupporter* pAggSup, SEventWindowInfo* pWinInfo, SSessionKey* pNextWinKey,
                              TSKEY* pTsData, bool* starts, bool* ends, int32_t rows, int32_t start,
                              SSHashObj* pResultRows, SSHashObj* pStUpdated, SSHashObj* pStDeleted, bool* pRebuild,
                              int32_t* pWinRow);
int32_t setEventOutputBuf(SStreamAggSupporter* pAggSup, TSKEY* pTs, uint64_t groupId, bool* pStart, bool* pEnd,
                          int32_t index, int32_t rows, SEventWindowInfo* pCurWin, SSessionKey* pNextWinKey,
                          int32_t* pWinCode);
bool isWindowIncomplete(SEventWindowInfo* pWinInfo);
void setEventWindowInfo(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SRowBuffPos* pPos,
                        SEventWindowInfo* pWinInfo);


// stream client
int32_t streamClientGetResultRange(SStreamRecParam* pParam, SSHashObj* pRangeMap, SArray* pRangeRes);
int32_t streamClientGetFillRange(SStreamRecParam* pParam, SWinKey* pKey, SArray* pRangeRes, void* pEmptyRow, int32_t size, int32_t* pOffsetInfo, int32_t numOfCols);
int32_t streamClientCheckCfg(SStreamRecParam* pParam);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_SESSION_H
