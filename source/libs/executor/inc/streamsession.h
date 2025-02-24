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

int32_t doStreamSessionNonblockAggNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock);
void streamSessionNonblockReleaseState(SOperatorInfo* pOperator);
void streamSessionNonblockReloadState(SOperatorInfo* pOperator);
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
int32_t compactSessionWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SSHashObj* pStUpdated,
                             SSHashObj* pStDeleted, bool addGap, int32_t* pWinNum, bool* pIsEnd);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_SESSION_H
