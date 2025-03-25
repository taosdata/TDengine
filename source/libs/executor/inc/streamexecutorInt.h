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

#include "cJSON.h"
#include "cmdnodes.h"
#include "executorInt.h"
#include "querytask.h"
#include "tutil.h"

#define FILL_POS_INVALID 0
#define FILL_POS_START   1
#define FILL_POS_MID     2
#define FILL_POS_END     3

#define HAS_NON_ROW_DATA(pRowData) (pRowData->key == INT64_MIN)
#define HAS_ROW_DATA(pRowData)     (pRowData && pRowData->key != INT64_MIN)

#define IS_INVALID_WIN_KEY(ts)  ((ts) == INT64_MIN)
#define IS_VALID_WIN_KEY(ts)    ((ts) != INT64_MIN)
#define SET_WIN_KEY_INVALID(ts) ((ts) = INT64_MIN)

#define IS_NORMAL_INTERVAL_OP(op)                                    \
  ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL || \
   (op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL)

#define IS_NORMAL_SESSION_OP(op)                                    \
  ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION || \
   (op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION)

#define IS_NORMAL_STATE_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE)

#define IS_NORMAL_EVENT_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT)

#define IS_NORMAL_COUNT_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT)

#define IS_CONTINUE_INTERVAL_OP(op) ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_INTERVAL)

#define IS_FILL_CONST_VALUE(type) \
  ((type == TSDB_FILL_NULL || type == TSDB_FILL_NULL_F || type == TSDB_FILL_SET_VALUE || type == TSDB_FILL_SET_VALUE_F))

#define IS_INVALID_RANGE(range)             (range.pGroupIds == NULL)

// 
enum {
  STREAM_NORMAL_OPERATOR = 0,
  STREAM_HISTORY_OPERATOR = 1,
  STREAM_RECALCUL_OPERATOR = 2,
};

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

typedef struct SStateWindowInfo {
  SResultWindowInfo winInfo;
  SStateKeys*       pStateKey;
} SStateWindowInfo;

typedef struct SEventWinfowFlag {
  bool startFlag;
  bool endFlag;
} SEventWinfowFlag;

typedef struct SEventWindowInfo {
  SResultWindowInfo winInfo;
  SEventWinfowFlag* pWinFlag;
} SEventWindowInfo;


void    setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type);
bool    needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo);
void    saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo);
int32_t initStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo, const struct SOperatorInfo* pOperator);
void    destroyStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo);
int32_t encodeStreamBasicInfo(void** buf, const SSteamOpBasicInfo* pBasicInfo);
int32_t decodeStreamBasicInfo(void** buf, SSteamOpBasicInfo* pBasicInfo);
void setFillHistoryOperatorFlag(SSteamOpBasicInfo* pBasicInfo);
bool isHistoryOperator(SSteamOpBasicInfo* pBasicInfo);
void setFinalOperatorFlag(SSteamOpBasicInfo* pBasicInfo);
bool isFinalOperator(SSteamOpBasicInfo* pBasicInfo);
bool needBuildAllResult(SSteamOpBasicInfo* pBasicInfo);
void setSemiOperatorFlag(SSteamOpBasicInfo* pBasicInfo);
bool isSemiOperator(SSteamOpBasicInfo* pBasicInfo);
void setRecalculateOperatorFlag(SSteamOpBasicInfo* pBasicInfo);
void unsetRecalculateOperatorFlag(SSteamOpBasicInfo* pBasicInfo);
bool isRecalculateOperator(SSteamOpBasicInfo* pBasicInfo);
void setSingleOperatorFlag(SSteamOpBasicInfo* pBasicInfo);
bool isSingleOperator(SSteamOpBasicInfo* pBasicInfo);

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
                                             SGroupResInfo* pGroupResInfo, SArray* pSessionKeys);
void             transBlockToSliceResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SSliceRowData* pRowVal,
                                            int32_t rowSize, void* pPkData, SColumnInfoData* pPkCol, int32_t* pCellOffsetInfo);
int32_t getQualifiedRowNumDesc(SExprSupp* pExprSup, SSDataBlock* pBlock, TSKEY* tsCols, int32_t rowId, bool ignoreNull);

int32_t buildAllResultKey(SStateStore* pStateStore, SStreamState* pState, TSKEY ts, SArray* pUpdated);
int32_t initOffsetInfo(int32_t** ppOffset, SSDataBlock* pRes);
TSKEY   compareTs(void* pKey);
void    clearGroupResArray(SGroupResInfo* pGroupResInfo);
void    clearSessionGroupResInfo(SGroupResInfo* pGroupResInfo);
void    destroyResultWinInfo(void* pRes);

int32_t addEventAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t ri,
                               SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat);
int32_t addStateAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               const SStateKeys* pCurState, const SStateKeys* pAnotherState, bool onlyUpdate,
                               SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat);
int32_t addIntervalAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                                  SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat);
int32_t addSessionAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                                 SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat);
int32_t addCountAggNotifyEvent(EStreamNotifyEventType eventType, const SSessionKey* pSessionKey,
                               SStreamNotifyEventSupp* sup, STaskNotifyEventStat* pNotifyEventStat);
int32_t addAggResultNotifyEvent(const SSDataBlock* pResultBlock, const SArray* pSessionKeys,
                                const SSchemaWrapper* pSchemaWrapper, SStreamNotifyEventSupp* sup,
                                STaskNotifyEventStat* pNotifyEventStat);
int32_t addAggDeleteNotifyEvent(const SSDataBlock* pDeleteBlock, SStreamNotifyEventSupp* sup,
                                STaskNotifyEventStat* pNotifyEventStat);
int32_t buildNotifyEventBlock(const SExecTaskInfo* pTaskInfo, SStreamNotifyEventSupp* sup,
                              STaskNotifyEventStat* pNotifyEventStat);
int32_t removeOutdatedNotifyEvents(STimeWindowAggSupp* pTwSup, SStreamNotifyEventSupp* sup,
                                   STaskNotifyEventStat* pNotifyEventStat);
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
void streamIntervalNonblockReleaseState(struct SOperatorInfo* pOperator);
void streamIntervalNonblockReloadState(struct SOperatorInfo* pOperator);
int32_t compareWinKey(void* pKey, void* data, int32_t index);
int32_t buildIntervalSliceResult(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);

int32_t filterDelBlockByUid(SSDataBlock* pDst, const SSDataBlock* pSrc, STqReader* pReader, SStoreTqReader* pReaderFn);
int32_t rebuildDeleteBlockData(struct SSDataBlock* pBlock, STimeWindow* pWindow, const char* id);
int32_t deletePartName(SStateStore* pStore, SStreamState* pState, SSDataBlock* pBlock, int32_t *deleteNum);
int32_t doTableScanNext(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);
void streamScanOperatorSaveCheckpoint(struct SStreamScanInfo* pInfo);
int32_t doStreamDataScanNext(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);
void streamDataScanReleaseState(struct SOperatorInfo* pOperator);
void streamDataScanReloadState(struct SOperatorInfo* pOperator);
int32_t extractTableIdList(const STableListInfo* pTableListInfo, SArray** ppArrayRes);
int32_t colIdComparFn(const void* param1, const void* param2);
int32_t doBlockDataWindowFilter(SSDataBlock* pBlock, int32_t tsIndex, STimeWindow* pWindow, const char* id);
STimeWindow getSlidingWindow(TSKEY* startTsCol, TSKEY* endTsCol, uint64_t* gpIdCol, SInterval* pInterval,
                             SDataBlockInfo* pDataBlockInfo, int32_t* pRowIndex, bool hasGroup);
int32_t appendPkToSpecialBlock(SSDataBlock* pBlock, TSKEY* pTsArray, SColumnInfoData* pPkCol, int32_t rowId,
                               uint64_t* pUid, uint64_t* pGp, void* pTbName);
int32_t appendOneRowToSpecialBlockImpl(SSDataBlock* pBlock, TSKEY* pStartTs, TSKEY* pEndTs, TSKEY* pCalStartTs,
                                       TSKEY* pCalEndTs, uint64_t* pUid, uint64_t* pGp, void* pTbName, void* pPkData);
SSDataBlock* readPreVersionData(struct SOperatorInfo* pTableScanOp, uint64_t tbUid, TSKEY startTs, TSKEY endTs,
                                int64_t maxVersion);
bool comparePrimaryKey(SColumnInfoData* pCol, int32_t rowId, void* pVal);
void releaseFlusedPos(void* pRes);
typedef int32_t (*__compare_fn_t)(void* pKey, void* data, int32_t index);
int32_t binarySearchCom(void* keyList, int num, void* pKey, int order, __compare_fn_t comparefn);
void resetTableScanInfo(STableScanInfo* pTableScanInfo, STimeWindow* pWin, int64_t ver);
int32_t calBlockTbName(SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t rowId);
int32_t setBlockIntoRes(SStreamScanInfo* pInfo, const SSDataBlock* pBlock, STimeWindow* pTimeWindow,
                        bool filter);
int32_t setBlockGroupIdByUid(SStreamScanInfo* pInfo, SSDataBlock* pBlock);
int32_t createStreamDataScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode, SNode* pTagCond,
                                         STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                         struct SOperatorInfo** pOptrInfo);
int32_t saveRecalculateData(SStateStore* pStateStore, STableTsDataState* pTsDataState, SSDataBlock* pSrcBlock, EStreamType mode);
void doBuildPullDataBlock(SArray* array, int32_t* pIndex, SSDataBlock* pBlock);
int32_t doRangeScan(SStreamScanInfo* pInfo, SSDataBlock* pSDB, int32_t tsColIndex, int32_t* pRowIndex,
                    SSDataBlock** ppRes);
void prepareRangeScan(SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t* pRowIndex, bool* pRes);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_EXECUTORINT_H
