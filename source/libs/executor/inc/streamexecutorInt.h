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

#define HAS_NON_ROW_DATA(pRowData)           (pRowData->key == INT64_MIN)
#define HAS_ROW_DATA(pRowData)               (pRowData && pRowData->key != INT64_MIN)

#define IS_INVALID_WIN_KEY(ts)               ((ts) == INT64_MIN)
#define IS_VALID_WIN_KEY(ts)               ((ts) != INT64_MIN)
#define SET_WIN_KEY_INVALID(ts)              ((ts) = INT64_MIN)

#define IS_NORMAL_INTERVAL_OP(op)                                    \
  ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL || \
   (op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL)

#define IS_CONTINUE_INTERVAL_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_INTERVAL)

typedef struct SSliceRowData {
  TSKEY key;
  char  pRowVal[];
} SSliceRowData;

typedef struct SSlicePoint {
  SWinKey        key;
  SSliceRowData* pLeftRow;
  SSliceRowData* pRightRow;
  SRowBuffPos*   pResPos;
} SSlicePoint;

void setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type);
bool needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo);
void saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo);
void initStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo);

int64_t getDeleteMarkFromOption(SStreamNodeOption* pOption);
void    removeDeleteResults(SSHashObj* pUpdatedMap, SArray* pDelWins);
int32_t copyIntervalDeleteKey(SSHashObj* pMap, SArray* pWins);
bool    hasSrcPrimaryKeyCol(SSteamOpBasicInfo* pInfo);
int32_t getNexWindowPos(SInterval* pInterval, SDataBlockInfo* pBlockInfo, TSKEY* tsCols, int32_t startPos, TSKEY eKey,
                        STimeWindow* pNextWin);
int32_t saveWinResult(SWinKey* pKey, SRowBuffPos* pPos, SSHashObj* pUpdatedMap);
void    doBuildDeleteResultImpl(SStateStore* pAPI, SStreamState* pState, SArray* pWins, int32_t* index,
                                SSDataBlock* pBlock);

SStreamFillInfo* initStreamFillInfo(SStreamFillSupporter* pFillSup, SSDataBlock* pRes);
SResultCellData* getResultCell(SResultRowData* pRaw, int32_t index);

void    destroyStreamFillSupporter(SStreamFillSupporter* pFillSup);
bool    hasCurWindow(SStreamFillSupporter* pFillSup);
bool    hasPrevWindow(SStreamFillSupporter* pFillSup);
bool    hasNextWindow(SStreamFillSupporter* pFillSup);
void    copyNotFillExpData(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo);
int32_t setRowCell(SColumnInfoData* pCol, int32_t rowId, const SResultCellData* pCell);
bool    hasRemainCalc(SStreamFillInfo* pFillInfo);
void    destroySPoint(void* ptr);
void    destroyStreamFillInfo(SStreamFillInfo* pFillInfo);
int32_t checkResult(SStreamFillSupporter* pFillSup, TSKEY ts, uint64_t groupId, bool* pRes);
void    resetStreamFillSup(SStreamFillSupporter* pFillSup);
void    setPointBuff(SSlicePoint* pPoint, SStreamFillSupporter* pFillSup);

int32_t saveTimeSliceWinResult(SWinKey* pKey, SSHashObj* pUpdatedMap);

int winPosCmprImpl(const void* pKey1, const void* pKey2);

void             reuseOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI);
SResultCellData* getSliceResultCell(SResultCellData* pRowVal, int32_t index);
int32_t          getDownstreamRes(struct SOperatorInfo* downstream, SSDataBlock** ppRes, SColumnInfo** ppPkCol);
void             destroyFlusedppPos(void* ppRes);
void             doBuildStreamIntervalResult(struct SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock,
                                             SGroupResInfo* pGroupResInfo);
void             transBlockToSliceResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SSliceRowData* pRowVal,
                                            int32_t rowSize, void* pPkData, SColumnInfoData* pPkCol);
int32_t getQualifiedRowNumDesc(SExprSupp* pExprSup, SSDataBlock* pBlock, TSKEY* tsCols, int32_t rowId, bool ignoreNull);

int32_t createStreamIntervalSliceOperatorInfo(struct SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                              SExecTaskInfo* pTaskInfo, SReadHandle* pHandle,
                                              struct SOperatorInfo** ppOptInfo);
int32_t buildAllResultKey(SStreamAggSupporter* pAggSup, TSKEY ts, SArray* pUpdated);
void removeDuplicateTs(SArray* pTsArrray);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_EXECUTORINT_H
