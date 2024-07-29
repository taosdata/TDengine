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
#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "storageapi.h"
#include "streamexecutorInt.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

#define STREAM_TIME_SLICE_OP_STATE_NAME      "StreamTimeSliceHistoryState"
#define STREAM_TIME_SLICE_OP_CHECKPOINT_NAME "StreamTimeSliceOperator_Checkpoint"
#define HAS_NON_ROW_DATA(pRowData)           (pRowData->key == INT64_MIN)

typedef struct SSlicePoint {
  SWinKey         key;
  SResultRowData* pLeftRow;
  SResultRowData* pRightRow;
  SRowBuffPos*    pResPos;
} SSlicePoint;

void streamTimeSliceReleaseState(SOperatorInfo* pOperator) {
  // todo(liuyao) add
}

void streamTimeSliceReloadState(SOperatorInfo* pOperator) {
  // todo(liuyao) add
}

void destroyStreamTimeSliceOperatorInfo(void* param) {
  SStreamTimeSliceOperatorInfo* pInfo = (SStreamTimeSliceOperatorInfo*)param;
  clearGroupResInfo(&pInfo->groupResInfo);
  taosArrayDestroyP(pInfo->pUpdated, destroyFlusedPos);
  pInfo->pUpdated = NULL;

  taosArrayDestroy(pInfo->pDelWins);
  blockDataDestroy(pInfo->pDelRes);
  destroyStreamAggSupporter(&pInfo->streamAggSup);

  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;
  tSimpleHashCleanup(pInfo->pDeletedMap);

  blockDataDestroy(pInfo->pCheckpointRes);
  // todo(liuyao) 看是否有遗漏
  taosMemoryFreeClear(param);
}

static void doStreamTimeSliceSaveCheckpoint(SOperatorInfo* pOperator) {
  // todo(liuyao) add
}

static bool fillPointResult(SStreamFillSupporter* pFillSup, SResultRowData* pResRow, TSKEY ts, SSDataBlock* pBlock) {
  if (pBlock->info.rows >= pBlock->info.capacity) {
    return false;
  }
  for (int32_t i = 0; i < pFillSup->numOfAllCols; i++) {
    SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
    int32_t          dstSlotId = GET_DEST_SLOT_ID(pFillCol);
    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);

    if (isIrowtsPseudoColumn(pFillCol->pExpr)) {
      colDataSetVal(pDstCol, pBlock->info.rows, (char*)&ts, false);
    } else if (isIsfilledPseudoColumn(pFillCol->pExpr)) {
      bool isFilled = false;
      colDataSetVal(pDstCol, pBlock->info.rows, (char*)&isFilled, false);
    } else {
      int32_t          srcSlot = pFillCol->pExpr->base.pParam[0].pCol->slotId;
      SResultCellData* pCell = getResultCell(pResRow, srcSlot);
      setRowCell(pDstCol, pBlock->info.rows, pCell);
    }
  }

  pBlock->info.rows += 1;
  return true;
}

static void fillNormalRange(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    STimeWindow st = {.skey = pFillInfo->current, .ekey = pFillInfo->current};
    if (inWinRange(&pFillSup->winRange, &st)) {
      fillPointResult(pFillSup, pFillInfo->pResRow, pFillInfo->current, pBlock);
    }
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
  }
}

static void fillLinearRange(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
      int32_t          dstSlotId = GET_DEST_SLOT_ID(pFillCol);
      int32_t          srcSlot = pFillCol->pExpr->base.pParam[0].pCol->slotId;
      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      int16_t          type = pDstCol->info.type;
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, srcSlot);
      int32_t          index = pBlock->info.rows;
      if (isIrowtsPseudoColumn(pFillCol->pExpr)) {
        colDataSetVal(pDstCol, pBlock->info.rows, (char*)&pFillInfo->current, false);
      } else if (isIsfilledPseudoColumn(pFillCol->pExpr)) {
        bool isFilled = true;
        colDataSetVal(pDstCol, pBlock->info.rows, (char*)&isFilled, false);
      } else {
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pCell->isNull) {
          colDataSetNULL(pDstCol, index);
          continue;
        }
        SPoint* pEnd = taosArrayGet(pFillInfo->pLinearInfo->pEndPoints, srcSlot);
        double  vCell = 0;
        SPoint  start = {0};
        start.key = pFillInfo->pResRow->key;
        start.val = pCell->pData;

        SPoint cur = {0};
        cur.key = pFillInfo->current;
        cur.val = taosMemoryCalloc(1, pCell->bytes);
        taosGetLinearInterpolationVal(&cur, pCell->type, &start, pEnd, pCell->type);
        colDataSetVal(pDstCol, index, (const char*)cur.val, false);
        destroySPoint(&cur);
      }
    }
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
    pBlock->info.rows++;
  }
}

static void doStreamFillRange(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pRes) {
  if (pFillInfo->needFill == false) {
    fillPointResult(pFillSup, &pFillSup->cur, pFillSup->cur.key, pRes);
    return;
  }

  if (pFillInfo->pos == FILL_POS_START) {
    if (fillPointResult(pFillSup, &pFillSup->cur, pFillSup->cur.key, pRes)) {
      pFillInfo->pos = FILL_POS_INVALID;
    }
  }
  if (pFillInfo->type != TSDB_FILL_LINEAR) {
    fillNormalRange(pFillSup, pFillInfo, pRes);
  } else {
    fillLinearRange(pFillSup, pFillInfo, pRes);

    if (pFillInfo->pos == FILL_POS_MID) {
      if (fillPointResult(pFillSup, &pFillSup->cur, pFillSup->cur.key, pRes)) {
        pFillInfo->pos = FILL_POS_INVALID;
      }
    }

    if (pFillInfo->current > pFillInfo->end && pFillInfo->pLinearInfo->hasNext) {
      pFillInfo->pLinearInfo->hasNext = false;
      taosArraySwap(pFillInfo->pLinearInfo->pEndPoints, pFillInfo->pLinearInfo->pNextEndPoints);
      pFillInfo->pResRow = &pFillSup->cur;
      setFillKeyInfo(pFillSup->cur.key, pFillInfo->pLinearInfo->nextEnd, &pFillSup->interval, pFillInfo);
      fillLinearRange(pFillSup, pFillInfo, pRes);
    }
  }
  if (pFillInfo->pos == FILL_POS_END) {
    if (fillPointResult(pFillSup, &pFillSup->cur, pFillSup->cur.key, pRes)) {
      pFillInfo->pos = FILL_POS_INVALID;
    }
  }
}

static int32_t getQualifiedRowNumAsc(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t rowId, bool ignoreNull) {
  if (!ignoreNull) {
    return rowId;
  }

  for (int32_t i = rowId; rowId < pBlock->info.rows; i++) {
    if (!checkNullRow(pExprSup, pBlock, rowId, ignoreNull)) {
      return i;
    }
  }
  return -1;
}

static int32_t getQualifiedRowNumDesc(SExprSupp* pExprSup, SSDataBlock* pBlock, TSKEY* tsCols, int32_t rowId,
                                      bool ignoreNull) {
  TSKEY   ts = tsCols[rowId];
  int32_t resRow = -1;
  for (; rowId >= 0; rowId--) {
    if (checkNullRow(pExprSup, pBlock, rowId, ignoreNull)) {
      continue;
    }

    if (ts != tsCols[rowId]) {
      if (resRow >= 0) {
        break;
      } else {
        ts = tsCols[rowId];
      }
    }
    resRow = rowId;
  }
  return resRow;
}

static void getPointRowDataFromState(SStreamAggSupporter* pAggSup, SStreamFillSupporter* pFillSup,
                                     SSlicePoint* pPoint) {
  int32_t curVLen = 0;
  int32_t code =
      pAggSup->stateStore.streamStateFillGet(pAggSup->pState, &pPoint->key, (void**)&pPoint->pResPos, &curVLen);
  pPoint->pLeftRow = pPoint->pResPos->pRowBuff;
  if (pFillSup->type == TSDB_FILL_LINEAR) {
    pPoint->pRightRow = POINTER_SHIFT(pPoint->pResPos->pRowBuff, pFillSup->rowSize);
  } else {
    pPoint->pRightRow = NULL;
  }
}

static void getPointInfoFromState(SStreamAggSupporter* pAggSup, SStreamFillSupporter* pFillSup, TSKEY ts,
                                  int64_t groupId) {
  void* pState = pAggSup->pState;
  resetPrevAndNextWindow(pFillSup);

  SWinKey key = {.ts = ts, .groupId = groupId};
  void*   curVal = NULL;
  int32_t curVLen = 0;
  int32_t code = pAggSup->stateStore.streamStateFillGet(pState, &key, (void**)&curVal, &curVLen);
  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->cur.key = key.ts;
    pFillSup->cur.pRowVal = curVal;
  } else {
    qDebug("streamStateFillGet key failed, Data may be deleted. ts:%" PRId64 ", groupId:%" PRId64, ts, groupId);
    pFillSup->cur.key = ts;
    pFillSup->cur.pRowVal = NULL;
  }

  SWinKey preKey = {.ts = INT64_MIN, .groupId = groupId};
  void*   preVal = NULL;
  int32_t preVLen = 0;
  code = pAggSup->stateStore.streamStateFillGetPrev(pState, &key, &preKey, &preVal, &preVLen);
  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->prev.key = preKey.ts;
    pFillSup->prev.pRowVal = preVal;
  }

  SWinKey nextKey = {.ts = INT64_MIN, .groupId = groupId};
  void*   nextVal = NULL;
  int32_t nextVLen = 0;
  code = pAggSup->stateStore.streamStateFillGetNext(pState, &key, &nextKey, &nextVal, &nextVLen);
  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->next.key = nextKey.ts;
    pFillSup->next.pRowVal = nextVal;
  }
}

static void setTimeSliceFillInfo(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, TSKEY ts) {
  if (!hasPrevWindow(pFillSup) && !hasNextWindow(pFillSup)) {
    pFillInfo->needFill = false;
    pFillInfo->pos = FILL_POS_START;
    return;
  }
  TSKEY prevWKey = INT64_MIN;
  TSKEY nextWKey = INT64_MIN;
  if (hasPrevWindow(pFillSup)) {
    prevWKey = pFillSup->prev.key;
  }
  if (hasNextWindow(pFillSup)) {
    nextWKey = pFillSup->next.key;
  }

  pFillInfo->needFill = true;
  pFillInfo->pos = FILL_POS_INVALID;
  switch (pFillInfo->type) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_NULL_F:
    case TSDB_FILL_SET_VALUE:
    case TSDB_FILL_SET_VALUE_F: {
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        pFillInfo->needFill = false;
        pFillInfo->pos = FILL_POS_START;
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
      } else {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
      copyNotFillExpData(pFillSup, pFillInfo);
    } break;
    case TSDB_FILL_PREV: {
      if (hasNextWindow(pFillSup)) {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        resetFillWindow(&pFillSup->prev);
        pFillSup->prev.key = pFillSup->cur.key;
        pFillSup->prev.pRowVal = pFillSup->cur.pRowVal;
      } else {
        ASSERT(hasPrevWindow(pFillSup));
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillInfo->preRowKey = INT64_MIN;
      }
      pFillInfo->pResRow = &pFillSup->prev;
    } break;
    case TSDB_FILL_NEXT: {
      if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        resetFillWindow(&pFillSup->next);
        pFillSup->next.key = pFillSup->cur.key;
        pFillSup->next.pRowVal = pFillSup->cur.pRowVal;
        pFillInfo->preRowKey = INT64_MIN;
      } else {
        ASSERT(hasNextWindow(pFillSup));
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
      pFillInfo->pResRow = &pFillSup->next;
    } break;
    case TSDB_FILL_LINEAR: {
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_MID;
        pFillInfo->pLinearInfo->nextEnd = nextWKey;
        calcRowDeltaData(&pFillSup->cur, pFillInfo->pLinearInfo->pEndPoints, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols);
        pFillInfo->pResRow = &pFillSup->prev;

        calcRowDeltaData(&pFillSup->next, pFillInfo->pLinearInfo->pNextEndPoints, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols);
        pFillInfo->pLinearInfo->hasNext = true;
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
        calcRowDeltaData(&pFillSup->cur, pFillInfo->pLinearInfo->pEndPoints, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols);
        pFillInfo->pResRow = &pFillSup->prev;
        pFillInfo->pLinearInfo->hasNext = false;
      } else {
        ASSERT(hasNextWindow(pFillSup));
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
        calcRowDeltaData(&pFillSup->next, pFillInfo->pLinearInfo->pEndPoints, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols);
        pFillInfo->pResRow = &pFillSup->cur;
        pFillInfo->pLinearInfo->hasNext = false;
      }
    } break;
    default:
      ASSERT(0);
      break;
  }
  ASSERT(pFillInfo->pos != FILL_POS_INVALID);
}

static bool needAdjValue(SSlicePoint* pPoint, TSKEY ts, bool isLeft, int32_t fillType) {
  switch (fillType) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_NULL_F:
    case TSDB_FILL_SET_VALUE:
    case TSDB_FILL_SET_VALUE_F: {
      if (HAS_NON_ROW_DATA(pPoint->pRightRow) && HAS_NON_ROW_DATA(pPoint->pLeftRow)) {
        return true;
      }
    } break;
    case TSDB_FILL_PREV: {
      if (isLeft && (HAS_NON_ROW_DATA(pPoint->pLeftRow) || pPoint->pLeftRow->key < ts)) {
        return true;
      }
    } break;
    case TSDB_FILL_NEXT: {
      if (!isLeft && (HAS_NON_ROW_DATA(pPoint->pRightRow) || pPoint->pRightRow->key > ts)) {
        return true;
      }
    } break;
    case TSDB_FILL_LINEAR: {
      if (isLeft && (HAS_NON_ROW_DATA(pPoint->pLeftRow) || pPoint->pLeftRow->key < ts)) {
        return true;
      } else if (!isLeft && (HAS_NON_ROW_DATA(pPoint->pRightRow) || pPoint->pRightRow->key > ts)) {
        return true;
      }
    } break;
    default:
      ASSERT(0);
  }
  return false;
}

static void doStreamTimeSliceImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SStreamTimeSliceOperatorInfo* pInfo = (SStreamTimeSliceOperatorInfo*)pOperator->info;
  SExecTaskInfo*                pTaskInfo = pOperator->pTaskInfo;
  SStreamAggSupporter*          pAggSup = &pInfo->streamAggSup;
  SExprSupp*                    pExprSup = &pOperator->exprSupp;
  int32_t                       numOfOutput = pExprSup->numOfExprs;
  SColumnInfoData*              pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*                        tsCols = (int64_t*)pColDataInfo->pData;
  void*                         pPkVal = NULL;
  int32_t                       pkLen = 0;
  int64_t                       groupId = pBlock->info.id.groupId;
  SColumnInfoData*              pPkColDataInfo = NULL;
  SStreamFillSupporter*         pFillSup = pInfo->pFillSup;
  SStreamFillInfo*              pFillInfo = pInfo->pFillInfo;
  if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
    pPkColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->basic.primaryPkIndex);
  }

  pFillSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pFillSup->winRange.ekey <= 0) {
    pFillSup->winRange.ekey = INT64_MIN;
  }

  int32_t startPos = 0;
  for (; startPos < pBlock->info.rows; startPos++) {
    if (hasSrcPrimaryKeyCol(&pInfo->basic) && pInfo->ignoreExpiredData) {
      pPkVal = colDataGetData(pPkColDataInfo, startPos);
      pkLen = colDataGetRowLength(pPkColDataInfo, startPos);
    }

    if (pInfo->ignoreExpiredData && checkExpiredData(&pAggSup->stateStore, pAggSup->pUpdateInfo, &pInfo->twAggSup,
                                                     pBlock->info.id.uid, tsCols[startPos], pPkVal, pkLen)) {
      qDebug("===stream===ignore expired data, window end ts:%" PRId64 ", maxts - wartermak:%" PRId64, tsCols[startPos],
             pInfo->twAggSup.maxTs - pInfo->twAggSup.waterMark);
      continue;
    }

    if (checkNullRow(pExprSup, pBlock, startPos, pInfo->ignoreNull)) {
      continue;
    }
  }

  if (startPos >= pBlock->info.rows) {
    return;
  }

  SResultRowInfo dumyInfo = {0};
  dumyInfo.cur.pageId = -1;
  STimeWindow curWin = getActiveTimeWindow(NULL, &dumyInfo, tsCols[startPos], &pFillSup->interval, TSDB_ORDER_ASC);
  SSlicePoint point = {.key.ts = curWin.skey, .key.groupId = groupId};
  getPointRowDataFromState(pAggSup, pFillSup, &point);
  if (needAdjValue(&point, tsCols[startPos], true, pFillSup->type)) {
    transBlockToResultRow(pBlock, startPos, tsCols[startPos], point.pLeftRow);
    saveWinResult(&point.key, point.pResPos, pInfo->pUpdatedMap);
  }

  while (startPos < pBlock->info.rows) {
    int32_t numOfWin = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, curWin.ekey, binarySearchForKey, NULL,
                                                TSDB_ORDER_ASC);
    startPos += numOfWin;
    int32_t leftRowId = getQualifiedRowNumDesc(pExprSup, pBlock, tsCols, startPos - 1, pInfo->ignoreNull);
    startPos = getQualifiedRowNumAsc(pExprSup, pBlock, startPos, pInfo->ignoreNull);
    if (startPos < 0) {
      break;
    }
    curWin = getActiveTimeWindow(NULL, &dumyInfo, tsCols[startPos], &pFillSup->interval, TSDB_ORDER_ASC);
    getPointInfoFromState(pAggSup, pFillSup, curWin.skey, groupId);
    bool left = needAdjValue(&point, tsCols[leftRowId], true, pFillSup->type);
    if (left) {
      transBlockToResultRow(pBlock, leftRowId, tsCols[leftRowId], point.pLeftRow);
    }
    bool right = needAdjValue(&point, tsCols[startPos], false, pFillSup->type);
    if (right) {
      transBlockToResultRow(pBlock, startPos, tsCols[startPos], point.pRightRow);
    }

    if (left || right) {
      saveWinResult(&point.key, point.pResPos, pInfo->pUpdatedMap);
    }
  }
}

void doBuildTimeSlicePointResult(SStreamAggSupporter* pAggSup, SStreamFillSupporter* pFillSup,
                                 SStreamFillInfo* pFillInfo, SSDataBlock* pBlock, SGroupResInfo* pGroupResInfo) {
  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
  for (; pGroupResInfo->index < numOfRows; pGroupResInfo->index++) {
    SRowBuffPos* pPos = *(SRowBuffPos**)taosArrayGet(pGroupResInfo->pRows, pGroupResInfo->index);
    // todo(liuyao) fill 增加接口，get buff from pos设置pFillSup->cur
    SWinKey* pKey = (SWinKey*)pPos->pKey;
    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = pKey->groupId;
    } else if (pBlock->info.id.groupId != pKey->groupId) {
      pGroupResInfo->index--;
      break;
    }
    getPointInfoFromState(pAggSup, pFillSup, pKey->ts, pKey->groupId);
    setTimeSliceFillInfo(pFillSup, pFillInfo, pKey->ts);
    doStreamFillRange(pFillSup, pFillInfo, pBlock);
  }
}

static SSDataBlock* buildTimeSliceResult(SOperatorInfo* pOperator) {
  SStreamTimeSliceOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                pTaskInfo = pOperator->pTaskInfo;
  uint16_t                      opType = pOperator->operatorType;
  SStreamAggSupporter*          pAggSup = &pInfo->streamAggSup;

  doBuildDeleteResultImpl(&pAggSup->stateStore, pAggSup->pState, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    return pInfo->pDelRes;
  }

  doBuildTimeSlicePointResult(pAggSup, pInfo->pFillSup, pInfo->pFillInfo, pInfo->pRes, &pInfo->groupResInfo);
  if (pInfo->pRes->info.rows != 0) {
    printDataBlock(pInfo->pRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    return pInfo->pRes;
  }

  return NULL;
}

static SSDataBlock* doStreamTimeSlice(SOperatorInfo* pOperator) {
  SStreamTimeSliceOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                pTaskInfo = pOperator->pTaskInfo;
  SStreamAggSupporter*          pAggSup = &pInfo->streamAggSup;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    if (hasRemainCalc(pInfo->pFillInfo) ||
        (pInfo->pFillInfo->pos != FILL_POS_INVALID && pInfo->pFillInfo->needFill == true)) {
      blockDataCleanup(pInfo->pRes);
      doStreamFillRange(pInfo->pFillSup, pInfo->pFillInfo, pInfo->pRes);
      if (pInfo->pRes->info.rows > 0) {
        printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        return pInfo->pRes;
      }
    }

    SSDataBlock* resBlock = buildTimeSliceResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
    }

    if (pInfo->recvCkBlock) {
      pInfo->recvCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return pInfo->pCheckpointRes;
    }

    setStreamOperatorCompleted(pOperator);
    return NULL;
  }

  SSDataBlock*   fillResult = NULL;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64, getStreamOpName(pOperator->operatorType),
             pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }
    pInfo->numOfDatapack++;
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    switch (pBlock->info.type) {
      case STREAM_DELETE_RESULT: {
        // todo(liuyao) add
      } break;
      case STREAM_NORMAL:
      case STREAM_INVALID: {
        SExprSupp* pExprSup = &pInfo->scalarSup;
        if (pExprSup->pExprInfo != NULL) {
          projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
        }
      } break;
      case STREAM_CHECKPOINT: {
        pInfo->recvCkBlock = true;
        pAggSup->stateStore.streamStateCommit(pAggSup->pState);
        doStreamTimeSliceSaveCheckpoint(pOperator);
        pInfo->recvCkBlock = true;
        copyDataBlock(pInfo->pCheckpointRes, pBlock);
        continue;
      } break;
      case STREAM_CREATE_CHILD_TABLE: {
        return pBlock;
      } break;
      default:
        ASSERTS(false, "invalid SSDataBlock type");
    }

    doStreamTimeSliceImpl(pOperator, pBlock);
  }

  if (!pInfo->destHasPrimaryKey) {
    removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  } else {
    copyIntervalDeleteKey(pInfo->pDeletedMap, pInfo->pDelWins);
  }

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->pUpdatedMap, pIte, &iter)) != NULL) {
    taosArrayPush(pInfo->pUpdated, pIte);
  }
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;

  return buildTimeSliceResult(pOperator);
}

int32_t doStreamTimeSliceEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator) {
  // todo(liuyao) add
  return 0;
}

void* doStreamTimeSliceDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator) {
  // todo(liuyao) add
  return NULL;
}

static SStreamFillSupporter* initTimeSliceFillSup(SStreamInterpFuncPhysiNode* pPhyFillNode, SExprInfo* pExprInfo,
                                                  int32_t numOfExprs) {
  SStreamFillSupporter* pFillSup = taosMemoryCalloc(1, sizeof(SStreamFillSupporter));
  if (!pFillSup) {
    return NULL;
  }
  pFillSup->numOfFillCols = numOfExprs;
  int32_t numOfNotFillCols = 0;
  pFillSup->pAllColInfo = createFillColInfo(pExprInfo, pFillSup->numOfFillCols, NULL, numOfNotFillCols,
                                            (const SNodeListNode*)(pPhyFillNode->pFillValues));
  pFillSup->type = convertFillType(pPhyFillNode->fillMode);
  pFillSup->numOfAllCols = pFillSup->numOfFillCols + numOfNotFillCols;
  pFillSup->interval.interval = pPhyFillNode->interval;
  // todo(liuyao) 初始化 pFillSup->interval其他属性
  pFillSup->pAPI = NULL;

  int32_t code = initResultBuf(pFillSup);
  if (code != TSDB_CODE_SUCCESS) {
    destroyStreamFillSupporter(pFillSup);
    return NULL;
  }
  pFillSup->pResMap = NULL;
  pFillSup->hasDelete = false;
  return pFillSup;
}

int32_t createStreamTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                          SReadHandle* pHandle, SOperatorInfo** ppOptInfo) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  int32_t                       lino = 0;
  SStreamTimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamTimeSliceOperatorInfo));
  SOperatorInfo*                pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    goto _error;
  }
  SStreamInterpFuncPhysiNode* pInterpPhyNode = (SStreamInterpFuncPhysiNode*)pPhyNode;
  pOperator->pTaskInfo = pTaskInfo;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  SExprSupp* pExpSup = &pOperator->exprSupp;
  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pInterpPhyNode->pFuncs, NULL, &numOfExprs);
  code = initExprSupp(pExpSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pInterpPhyNode->pExprs, NULL, &num);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pInterpPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pInterpPhyNode->streamOption.watermark,
      .calTrigger = pInterpPhyNode->streamOption.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMarkFromOption(&pInterpPhyNode->streamOption),
  };

  pInfo->primaryTsIndex = ((SColumnNode*)pInterpPhyNode->pTimeSeries)->slotId;
  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfExprs, 0, pTaskInfo->streamInfo.pState,
                                sizeof(COUNT_TYPE), 0, &pTaskInfo->storageAPI.stateStore, pHandle, &pInfo->twAggSup,
                                GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI, pInfo->primaryTsIndex);
  QUERY_CHECK_CODE(code, lino, _error);

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  pInfo->delIndex = 0;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->pDelRes = NULL;
  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pDeletedMap = tSimpleHashInit(4096, hashFn);

  pInfo->ignoreExpiredData = pInterpPhyNode->streamOption.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pUpdatedMap = NULL;
  pInfo->historyPoints = taosArrayInit(4, sizeof(SWinKey));
  QUERY_CHECK_NULL(pInfo->historyPoints, code, lino, _error, terrno);

  pInfo->recvCkBlock = false;
  pInfo->pCheckpointRes = NULL;
  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->destHasPrimaryKey = pInterpPhyNode->streamOption.destHasPrimaryKey;
  pInfo->numOfDatapack = 0;
  pInfo->pFillSup = initTimeSliceFillSup(pInterpPhyNode, pExprInfo, numOfExprs);

  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERP_FUNC;
  setOperatorInfo(pOperator, getStreamOpName(pOperator->operatorType), QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERP_FUNC,
                  true, OP_NOT_OPENED, pInfo, pTaskInfo);
  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pTaskInfo->storageAPI.stateStore.streamStateGetInfo(
      pTaskInfo->streamInfo.pState, STREAM_TIME_SLICE_OP_CHECKPOINT_NAME, strlen(STREAM_TIME_SLICE_OP_CHECKPOINT_NAME),
      &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamTimeSliceDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
  }
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamTimeSlice, NULL, destroyStreamTimeSliceOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamTimeSliceReleaseState, streamTimeSliceReloadState);

  if (downstream) {
    if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
      SStreamScanInfo* pScanInfo = downstream->info;
      pScanInfo->igCheckUpdate = true;
    }
    initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup,
                   &pInfo->basic);
    code = appendDownstream(pOperator, &downstream, 1);
  }
  (*ppOptInfo) = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroyStreamTimeSliceOperatorInfo(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  (*ppOptInfo) = NULL;
  return code;
}
