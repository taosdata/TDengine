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
#ifndef STREAM_EXECUTORINT_H
#define STREAM_EXECUTORINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
#include "tutil.h"

#define FILL_POS_INVALID 0
#define FILL_POS_START   1
#define FILL_POS_MID     2
#define FILL_POS_END     3

void setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type);
bool needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo);
void saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo);

int64_t getDeleteMarkFromOption(SStreamOption* pOption);
void    removeDeleteResults(SSHashObj* pUpdatedMap, SArray* pDelWins);
int32_t copyIntervalDeleteKey(SSHashObj* pMap, SArray* pWins);
bool    hasSrcPrimaryKeyCol(SSteamOpBasicInfo* pInfo);
void    transBlockToResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SResultRowData* pRowVal);
int32_t getNexWindowPos(SInterval* pInterval, SDataBlockInfo* pBlockInfo, TSKEY* tsCols, int32_t startPos, TSKEY eKey,
                        STimeWindow* pNextWin);
int32_t saveWinResult(SWinKey* pKey, SRowBuffPos* pPos, SSHashObj* pUpdatedMap);
void    resetPrevAndNextWindow(SStreamFillSupporter* pFillSup);
void    doBuildDeleteResultImpl(SStateStore* pAPI, SStreamState* pState, SArray* pWins, int32_t* index,
                                SSDataBlock* pBlock);

SResultCellData* getResultCell(SResultRowData* pRaw, int32_t index);
int32_t          initResultBuf(SStreamFillSupporter* pFillSup);
void             destroyStreamFillSupporter(SStreamFillSupporter* pFillSup);
void             calcRowDeltaData(SResultRowData* pEndRow, SArray* pEndPoins, SFillColInfo* pFillCol, int32_t numOfCol);
void             resetFillWindow(SResultRowData* pRowData);
bool             hasPrevWindow(SStreamFillSupporter* pFillSup);
bool             hasNextWindow(SStreamFillSupporter* pFillSup);
void             copyNotFillExpData(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo);
void             setFillKeyInfo(TSKEY start, TSKEY end, SInterval* pInterval, SStreamFillInfo* pFillInfo);
int32_t          setRowCell(SColumnInfoData* pCol, int32_t rowId, const SResultCellData* pCell);
bool             hasRemainCalc(SStreamFillInfo* pFillInfo);
void             destroySPoint(void* ptr);

int winPosCmprImpl(const void* pKey1, const void* pKey2);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_EXECUTORINT_H
