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

#define IS_FILL_CONST_VALUE(type) ((type == TSDB_FILL_NULL || type == TSDB_FILL_NULL_F || type == TSDB_FILL_SET_VALUE ||  type == TSDB_FILL_SET_VALUE_F))

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

typedef struct SInervalSlicePoint {
  SSessionKey      winKey;
  bool             *pFinished;
  SSliceRowData*   pLastRow;
  SRowBuffPos*     pResPos;
} SInervalSlicePoint;

typedef enum SIntervalSliceType {
  INTERVAL_SLICE_START = 1,
  INTERVAL_SLICE_END = 2,
} SIntervalSliceType;

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
SResultCellData* getSliceResultCell(SResultCellData* pRowVal, int32_t index, int32_t* pCellOffsetInfo);
int32_t          getDownstreamRes(struct SOperatorInfo* downstream, SSDataBlock** ppRes, SColumnInfo** ppPkCol);
void             destroyFlusedppPos(void* ppRes);
void             doBuildStreamIntervalResult(struct SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock,
                                             SGroupResInfo* pGroupResInfo);
void             transBlockToSliceResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SSliceRowData* pRowVal,
                                            int32_t rowSize, void* pPkData, SColumnInfoData* pPkCol, int32_t* pCellOffsetInfo);
int32_t getQualifiedRowNumDesc(SExprSupp* pExprSup, SSDataBlock* pBlock, TSKEY* tsCols, int32_t rowId, bool ignoreNull);

int32_t createStreamIntervalSliceOperatorInfo(struct SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                              SExecTaskInfo* pTaskInfo, SReadHandle* pHandle,
                                              struct SOperatorInfo** ppOptInfo);
int32_t buildAllResultKey(SStateStore* pStateStore, SStreamState* pState, TSKEY ts, SArray* pUpdated);
int32_t initOffsetInfo(int32_t** ppOffset, SSDataBlock* pRes);
TSKEY   compareTs(void* pKey);
void doStreamIntervalSaveCheckpoint(struct SOperatorInfo* pOperator);
int32_t getIntervalSliceCurStateBuf(SStreamAggSupporter* pAggSup, SInterval* pInterval, bool needPrev, STimeWindow* pTWin, int64_t groupId,
                                    SInervalSlicePoint* pCurPoint, SInervalSlicePoint* pPrevPoint, int32_t* pWinCode);
int32_t getIntervalSlicePrevStateBuf(SStreamAggSupporter* pAggSup, SInterval* pInterval, SWinKey* pCurKey,
                                     SInervalSlicePoint* pPrevPoint);
bool isInterpoWindowFinished(SInervalSlicePoint* pPoint);
void resetIntervalSliceFunctionKey(SqlFunctionCtx* pCtx, int32_t numOfOutput);
int32_t setIntervalSliceOutputBuf(SStreamAggSupporter* pAggSup, SInervalSlicePoint* pPoint, SqlFunctionCtx* pCtx,
                                  int32_t numOfOutput, int32_t* rowEntryInfoOffset);
void doSetElapsedEndKey(TSKEY winKey, SExprSupp* pSup);
void doStreamSliceInterpolation(SSliceRowData* pPrevWinVal, TSKEY winKey, TSKEY curTs, SSDataBlock* pDataBlock,
                                int32_t curRowIndex, SExprSupp* pSup, SIntervalSliceType type, int32_t* pOffsetInfo);
void setInterpoWindowFinished(SInervalSlicePoint* pPoint);
int32_t doStreamIntervalNonblockAggNext(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);

int32_t filterDelBlockByUid(SSDataBlock* pDst, const SSDataBlock* pSrc, SStreamScanInfo* pInfo);
int32_t setBlockGroupIdByUid(struct SStreamScanInfo* pInfo, SSDataBlock* pBlock);
int32_t rebuildDeleteBlockData(struct SSDataBlock* pBlock, STimeWindow* pWindow, const char* id);
int32_t deletePartName(struct SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t *deleteNum);
int32_t setBlockIntoRes(struct SStreamScanInfo* pInfo, const SSDataBlock* pBlock, STimeWindow* pTimeWindow, bool filter);
int32_t doTableScanNext(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);
int32_t calBlockTbName(struct SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t rowId);
void streamScanOperatorSaveCheckpoint(struct SStreamScanInfo* pInfo);
int32_t doStreamDataScanNext(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);
bool hasPrimaryKeyCol(SStreamScanInfo* pInfo);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_EXECUTORINT_H
