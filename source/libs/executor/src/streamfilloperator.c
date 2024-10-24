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

#include "filter.h"
#include "os.h"
#include "query.h"
#include "taosdef.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorInt.h"
#include "tcommon.h"
#include "thash.h"
#include "ttime.h"

#include "function.h"
#include "operator.h"
#include "querynodes.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tfill.h"

#define FILL_POS_INVALID 0
#define FILL_POS_START   1
#define FILL_POS_MID     2
#define FILL_POS_END     3

typedef struct STimeRange {
  TSKEY    skey;
  TSKEY    ekey;
  uint64_t groupId;
} STimeRange;

TSKEY getNextWindowTs(TSKEY ts, SInterval* pInterval) {
  STimeWindow win = {.skey = ts, .ekey = ts};
  getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
  return win.skey;
}

TSKEY getPrevWindowTs(TSKEY ts, SInterval* pInterval) {
  STimeWindow win = {.skey = ts, .ekey = ts};
  getNextTimeWindow(pInterval, &win, TSDB_ORDER_DESC);
  return win.skey;
}

int32_t setRowCell(SColumnInfoData* pCol, int32_t rowId, const SResultCellData* pCell) {
  return colDataSetVal(pCol, rowId, pCell->pData, pCell->isNull);
}

SResultCellData* getResultCell(SResultRowData* pRaw, int32_t index) {
  if (!pRaw || !pRaw->pRowVal) {
    return NULL;
  }
  char*            pData = (char*)pRaw->pRowVal;
  SResultCellData* pCell = pRaw->pRowVal;
  for (int32_t i = 0; i < index; i++) {
    pData += (pCell->bytes + sizeof(SResultCellData));
    pCell = (SResultCellData*)pData;
  }
  return pCell;
}

void* destroyFillColumnInfo(SFillColInfo* pFillCol, int32_t start, int32_t end) {
  for (int32_t i = start; i < end; i++) {
    destroyExprInfo(pFillCol[i].pExpr, 1);
    taosVariantDestroy(&pFillCol[i].fillVal);
  }
  taosMemoryFreeClear(pFillCol[start].pExpr);
  taosMemoryFree(pFillCol);
  return NULL;
}

void destroyStreamFillSupporter(SStreamFillSupporter* pFillSup) {
  pFillSup->pAllColInfo = destroyFillColumnInfo(pFillSup->pAllColInfo, pFillSup->numOfFillCols, pFillSup->numOfAllCols);
  tSimpleHashCleanup(pFillSup->pResMap);
  pFillSup->pResMap = NULL;
  cleanupExprSupp(&pFillSup->notFillExprSup);
  if (pFillSup->cur.pRowVal != pFillSup->prev.pRowVal && pFillSup->cur.pRowVal != pFillSup->next.pRowVal) {
    taosMemoryFree(pFillSup->cur.pRowVal);
  }
  taosMemoryFree(pFillSup->prev.pRowVal);
  taosMemoryFree(pFillSup->next.pRowVal);
  taosMemoryFree(pFillSup->nextNext.pRowVal);

  taosMemoryFree(pFillSup);
}

void destroySPoint(void* ptr) {
  SPoint* point = (SPoint*)ptr;
  taosMemoryFreeClear(point->val);
}

void destroyStreamFillLinearInfo(SStreamFillLinearInfo* pFillLinear) {
  taosArrayDestroyEx(pFillLinear->pEndPoints, destroySPoint);
  taosArrayDestroyEx(pFillLinear->pNextEndPoints, destroySPoint);
  taosMemoryFree(pFillLinear);
}

void destroyStreamFillInfo(SStreamFillInfo* pFillInfo) {
  if (pFillInfo->type == TSDB_FILL_SET_VALUE || pFillInfo->type == TSDB_FILL_SET_VALUE_F ||
      pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {
    taosMemoryFreeClear(pFillInfo->pResRow->pRowVal);
    taosMemoryFreeClear(pFillInfo->pResRow);
  }
  destroyStreamFillLinearInfo(pFillInfo->pLinearInfo);
  pFillInfo->pLinearInfo = NULL;

  taosArrayDestroy(pFillInfo->delRanges);
  taosMemoryFree(pFillInfo);
}

static void destroyStreamFillOperatorInfo(void* param) {
  SStreamFillOperatorInfo* pInfo = (SStreamFillOperatorInfo*)param;
  destroyStreamFillInfo(pInfo->pFillInfo);
  destroyStreamFillSupporter(pInfo->pFillSup);
  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;
  blockDataDestroy(pInfo->pSrcBlock);
  pInfo->pSrcBlock = NULL;
  blockDataDestroy(pInfo->pDelRes);
  pInfo->pDelRes = NULL;
  taosArrayDestroy(pInfo->matchInfo.pList);
  pInfo->matchInfo.pList = NULL;
  taosMemoryFree(pInfo);
}

static void resetFillWindow(SResultRowData* pRowData) {
  pRowData->key = INT64_MIN;
  taosMemoryFreeClear(pRowData->pRowVal);
}

void resetPrevAndNextWindow(SStreamFillSupporter* pFillSup, void* pState, SStorageAPI* pAPI) {
  if (pFillSup->cur.pRowVal != pFillSup->prev.pRowVal && pFillSup->cur.pRowVal != pFillSup->next.pRowVal) {
    resetFillWindow(&pFillSup->cur);
  } else {
    pFillSup->cur.key = INT64_MIN;
    pFillSup->cur.pRowVal = NULL;
  }
  resetFillWindow(&pFillSup->prev);
  resetFillWindow(&pFillSup->next);
  resetFillWindow(&pFillSup->nextNext);
}

void getWindowFromDiscBuf(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId, SStreamFillSupporter* pFillSup) {
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;
  void*        pState = pOperator->pTaskInfo->streamInfo.pState;
  resetPrevAndNextWindow(pFillSup, pState, pAPI);

  SWinKey key = {.ts = ts, .groupId = groupId};
  void*   curVal = NULL;
  int32_t curVLen = 0;
  bool    hasCurKey = true;
  int32_t code = pAPI->stateStore.streamStateFillGet(pState, &key, (void**)&curVal, &curVLen);
  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->cur.key = key.ts;
    pFillSup->cur.pRowVal = curVal;
  } else {
    qDebug("streamStateFillGet key failed, Data may be deleted. ts:%" PRId64 ", groupId:%" PRId64, ts, groupId);
    pFillSup->cur.key = ts;
    pFillSup->cur.pRowVal = NULL;
    hasCurKey = false;
  }

  SStreamStateCur* pCur = pAPI->stateStore.streamStateFillSeekKeyPrev(pState, &key);
  SWinKey          preKey = {.ts = INT64_MIN, .groupId = groupId};
  void*            preVal = NULL;
  int32_t          preVLen = 0;
  code = pAPI->stateStore.streamStateGetGroupKVByCur(pCur, &preKey, (const void**)&preVal, &preVLen);

  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->prev.key = preKey.ts;
    pFillSup->prev.pRowVal = preVal;

    if (hasCurKey) {
      pAPI->stateStore.streamStateCurNext(pState, pCur);
    }

    pAPI->stateStore.streamStateCurNext(pState, pCur);
  } else {
    pAPI->stateStore.streamStateFreeCur(pCur);
    pCur = pAPI->stateStore.streamStateFillSeekKeyNext(pState, &key);
  }

  SWinKey nextKey = {.ts = INT64_MIN, .groupId = groupId};
  void*   nextVal = NULL;
  int32_t nextVLen = 0;
  code = pAPI->stateStore.streamStateGetGroupKVByCur(pCur, &nextKey, (const void**)&nextVal, &nextVLen);
  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->next.key = nextKey.ts;
    pFillSup->next.pRowVal = nextVal;
    if (pFillSup->type == TSDB_FILL_PREV || pFillSup->type == TSDB_FILL_NEXT) {
      pAPI->stateStore.streamStateCurNext(pState, pCur);
      SWinKey nextNextKey = {.groupId = groupId};
      void*   nextNextVal = NULL;
      int32_t nextNextVLen = 0;
      code = pAPI->stateStore.streamStateGetGroupKVByCur(pCur, &nextNextKey, (const void**)&nextNextVal, &nextNextVLen);
      if (code == TSDB_CODE_SUCCESS) {
        pFillSup->nextNext.key = nextNextKey.ts;
        pFillSup->nextNext.pRowVal = nextNextVal;
      }
    }
  }
  pAPI->stateStore.streamStateFreeCur(pCur);
}

static bool hasPrevWindow(SStreamFillSupporter* pFillSup) { return pFillSup->prev.key != INT64_MIN; }
static bool hasNextWindow(SStreamFillSupporter* pFillSup) { return pFillSup->next.key != INT64_MIN; }
static bool hasNextNextWindow(SStreamFillSupporter* pFillSup) {
  return pFillSup->nextNext.key != INT64_MIN;
  return false;
}

static void transBlockToResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SResultRowData* pRowVal) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    SResultCellData* pCell = getResultCell(pRowVal, i);
    if (!colDataIsNull_s(pColData, rowId)) {
      pCell->isNull = false;
      pCell->type = pColData->info.type;
      pCell->bytes = pColData->info.bytes;
      char* val = colDataGetData(pColData, rowId);
      if (IS_VAR_DATA_TYPE(pCell->type)) {
        memcpy(pCell->pData, val, varDataTLen(val));
      } else {
        memcpy(pCell->pData, val, pCell->bytes);
      }
    } else {
      pCell->isNull = true;
    }
  }
  pRowVal->key = ts;
}

static void calcDeltaData(SSDataBlock* pBlock, int32_t rowId, SResultRowData* pRowVal, SArray* pDelta,
                          SFillColInfo* pFillCol, int32_t numOfCol, int32_t winCount, int32_t order) {
  for (int32_t i = 0; i < numOfCol; i++) {
    if (!pFillCol[i].notFillCol) {
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol + i);
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
      char*            var = colDataGetData(pCol, rowId);
      double           start = 0;
      GET_TYPED_DATA(start, double, pCol->info.type, var);
      SResultCellData* pCell = getResultCell(pRowVal, slotId);
      double           end = 0;
      GET_TYPED_DATA(end, double, pCell->type, pCell->pData);
      double delta = 0;
      if (order == TSDB_ORDER_ASC) {
        delta = (end - start) / winCount;
      } else {
        delta = (start - end) / winCount;
      }
      taosArraySet(pDelta, slotId, &delta);
    }
  }
}

static void calcRowDeltaData(SResultRowData* pEndRow, SArray* pEndPoins, SFillColInfo* pFillCol, int32_t numOfCol) {
  for (int32_t i = 0; i < numOfCol; i++) {
    if (!pFillCol[i].notFillCol) {
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol + i);
      SResultCellData* pECell = getResultCell(pEndRow, slotId);
      SPoint*          pPoint = taosArrayGet(pEndPoins, slotId);
      pPoint->key = pEndRow->key;
      memcpy(pPoint->val, pECell->pData, pECell->bytes);
    }
  }
}

static void setFillInfoStart(TSKEY ts, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  ts = taosTimeAdd(ts, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
  pFillInfo->start = ts;
}

static void setFillInfoEnd(TSKEY ts, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  ts = taosTimeAdd(ts, pInterval->sliding * -1, pInterval->slidingUnit, pInterval->precision);
  pFillInfo->end = ts;
}

static void setFillKeyInfo(TSKEY start, TSKEY end, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  setFillInfoStart(start, pInterval, pFillInfo);
  pFillInfo->current = pFillInfo->start;
  setFillInfoEnd(end, pInterval, pFillInfo);
}

void setDeleteFillValueInfo(TSKEY start, TSKEY end, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo) {
  if (!hasPrevWindow(pFillSup) || !hasNextWindow(pFillSup)) {
    pFillInfo->needFill = false;
    return;
  }

  TSKEY realStart = taosTimeAdd(pFillSup->prev.key, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                pFillSup->interval.precision);

  pFillInfo->needFill = true;
  pFillInfo->start = realStart;
  pFillInfo->current = pFillInfo->start;
  pFillInfo->end = end;
  pFillInfo->pos = FILL_POS_INVALID;
  switch (pFillInfo->type) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_NULL_F:
    case TSDB_FILL_SET_VALUE:
    case TSDB_FILL_SET_VALUE_F:
      break;
    case TSDB_FILL_PREV:
      pFillInfo->pResRow = &pFillSup->prev;
      break;
    case TSDB_FILL_NEXT:
      pFillInfo->pResRow = &pFillSup->next;
      break;
    case TSDB_FILL_LINEAR: {
      setFillKeyInfo(pFillSup->prev.key, pFillSup->next.key, &pFillSup->interval, pFillInfo);
      pFillInfo->pLinearInfo->hasNext = false;
      pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
      calcRowDeltaData(&pFillSup->next, pFillInfo->pLinearInfo->pEndPoints, pFillSup->pAllColInfo,
                       pFillSup->numOfAllCols);
      pFillInfo->pResRow = &pFillSup->prev;
      pFillInfo->pLinearInfo->winIndex = 0;
    } break;
    default:
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR));
      break;
  }
}

void copyNotFillExpData(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo) {
  for (int32_t i = pFillSup->numOfFillCols; i < pFillSup->numOfAllCols; ++i) {
    SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
    int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
    SResultCellData* pCell = getResultCell(pFillInfo->pResRow, slotId);
    SResultCellData* pCurCell = getResultCell(&pFillSup->cur, slotId);
    pCell->isNull = pCurCell->isNull;
    if (!pCurCell->isNull) {
      memcpy(pCell->pData, pCurCell->pData, pCell->bytes);
    }
  }
}

void setFillValueInfo(SSDataBlock* pBlock, TSKEY ts, int32_t rowId, SStreamFillSupporter* pFillSup,
                      SStreamFillInfo* pFillInfo) {
  pFillInfo->preRowKey = pFillSup->cur.key;
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
      if (pFillSup->prev.key == pFillInfo->preRowKey) {
        resetFillWindow(&pFillSup->prev);
      }
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        if (pFillSup->next.key == pFillInfo->nextRowKey) {
          pFillInfo->preRowKey = INT64_MIN;
          setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
          pFillInfo->pos = FILL_POS_END;
        } else {
          pFillInfo->needFill = false;
          pFillInfo->pos = FILL_POS_START;
        }
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
      if (hasNextWindow(pFillSup) && ((pFillSup->next.key != pFillInfo->nextRowKey) ||
                                      (pFillSup->next.key == pFillInfo->nextRowKey && hasNextNextWindow(pFillSup)) ||
                                      (pFillSup->next.key == pFillInfo->nextRowKey && !hasPrevWindow(pFillSup)))) {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        resetFillWindow(&pFillSup->prev);
        pFillSup->prev.key = pFillSup->cur.key;
        pFillSup->prev.pRowVal = pFillSup->cur.pRowVal;
      } else if (hasPrevWindow(pFillSup)) {
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
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
      pFillInfo->pResRow = &pFillSup->next;
    } break;
    case TSDB_FILL_LINEAR: {
      pFillInfo->pLinearInfo->winIndex = 0;
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
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR));
      break;
  }
}

static int32_t checkResult(SStreamFillSupporter* pFillSup, TSKEY ts, uint64_t groupId, bool* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SWinKey key = {.groupId = groupId, .ts = ts};
  if (tSimpleHashGet(pFillSup->pResMap, &key, sizeof(SWinKey)) != NULL) {
    (*pRes) = false;
    goto _end;
  }
  code = tSimpleHashPut(pFillSup->pResMap, &key, sizeof(SWinKey), NULL, 0);
  QUERY_CHECK_CODE(code, lino, _end);
  (*pRes) = true;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildFillResult(SResultRowData* pResRow, SStreamFillSupporter* pFillSup, TSKEY ts, SSDataBlock* pBlock,
                               bool* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pBlock->info.rows >= pBlock->info.capacity) {
    (*pRes) = false;
    goto _end;
  }
  uint64_t groupId = pBlock->info.id.groupId;
  bool     ckRes = true;
  code = checkResult(pFillSup, ts, groupId, &ckRes);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pFillSup->hasDelete && !ckRes) {
    (*pRes) = true;
    goto _end;
  }
  for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
    SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
    int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
    SFillInfo        tmpInfo = {
               .currentKey = ts,
               .order = TSDB_ORDER_ASC,
               .interval = pFillSup->interval,
    };
    bool filled = fillIfWindowPseudoColumn(&tmpInfo, pFillCol, pColData, pBlock->info.rows);
    if (!filled) {
      SResultCellData* pCell = getResultCell(pResRow, slotId);
      code = setRowCell(pColData, pBlock->info.rows, pCell);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  pBlock->info.rows++;
  (*pRes) = true;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static bool hasRemainCalc(SStreamFillInfo* pFillInfo) {
  if (pFillInfo->current != INT64_MIN && pFillInfo->current <= pFillInfo->end) {
    return true;
  }
  return false;
}

static void doStreamFillNormal(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    STimeWindow st = {.skey = pFillInfo->current, .ekey = pFillInfo->current};
    if (inWinRange(&pFillSup->winRange, &st)) {
      bool res = true;
      code = buildFillResult(pFillInfo->pResRow, pFillSup, pFillInfo->current, pBlock, &res);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

static void doStreamFillLinear(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    uint64_t    groupId = pBlock->info.id.groupId;
    SWinKey     key = {.groupId = groupId, .ts = pFillInfo->current};
    STimeWindow st = {.skey = pFillInfo->current, .ekey = pFillInfo->current};
    bool        ckRes = true;
    code = checkResult(pFillSup, pFillInfo->current, groupId, &ckRes);
    QUERY_CHECK_CODE(code, lino, _end);

    if ((pFillSup->hasDelete && !ckRes) || !inWinRange(&pFillSup->winRange, &st)) {
      pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                       pFillSup->interval.precision);
      pFillInfo->pLinearInfo->winIndex++;
      continue;
    }
    pFillInfo->pLinearInfo->winIndex++;
    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SFillColInfo* pFillCol = pFillSup->pAllColInfo + i;
      SFillInfo     tmp = {
              .currentKey = pFillInfo->current,
              .order = TSDB_ORDER_ASC,
              .interval = pFillSup->interval,
      };

      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
      int16_t          type = pColData->info.type;
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, slotId);
      int32_t          index = pBlock->info.rows;
      if (pFillCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(&tmp, pFillCol, pColData, index);
        if (!filled) {
          code = setRowCell(pColData, index, pCell);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else {
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pCell->isNull) {
          colDataSetNULL(pColData, index);
          continue;
        }
        SPoint* pEnd = taosArrayGet(pFillInfo->pLinearInfo->pEndPoints, slotId);
        double  vCell = 0;
        SPoint  start = {0};
        start.key = pFillInfo->pResRow->key;
        start.val = pCell->pData;

        SPoint cur = {0};
        cur.key = pFillInfo->current;
        cur.val = taosMemoryCalloc(1, pCell->bytes);
        QUERY_CHECK_NULL(cur.val, code, lino, _end, terrno);
        taosGetLinearInterpolationVal(&cur, pCell->type, &start, pEnd, pCell->type);
        code = colDataSetVal(pColData, index, (const char*)cur.val, false);
        QUERY_CHECK_CODE(code, lino, _end);
        destroySPoint(&cur);
      }
    }
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
    pBlock->info.rows++;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

static void keepResultInDiscBuf(SOperatorInfo* pOperator, uint64_t groupId, SResultRowData* pRow, int32_t len) {
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SWinKey key = {.groupId = groupId, .ts = pRow->key};
  int32_t code = pAPI->stateStore.streamStateFillPut(pOperator->pTaskInfo->streamInfo.pState, &key, pRow->pRowVal, len);
  qDebug("===stream===fill operator save key ts:%" PRId64 " group id:%" PRIu64 "  code:%d", key.ts, key.groupId, code);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
}

static void doStreamFillRange(SStreamFillInfo* pFillInfo, SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    res = false;
  if (pFillInfo->needFill == false) {
    code = buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes, &res);
    QUERY_CHECK_CODE(code, lino, _end);
    return;
  }

  if (pFillInfo->pos == FILL_POS_START) {
    code = buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes, &res);
    QUERY_CHECK_CODE(code, lino, _end);
    if (res) {
      pFillInfo->pos = FILL_POS_INVALID;
    }
  }
  if (pFillInfo->type != TSDB_FILL_LINEAR) {
    doStreamFillNormal(pFillSup, pFillInfo, pRes);
  } else {
    doStreamFillLinear(pFillSup, pFillInfo, pRes);

    if (pFillInfo->pos == FILL_POS_MID) {
      code = buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes, &res);
      QUERY_CHECK_CODE(code, lino, _end);
      if (res) {
        pFillInfo->pos = FILL_POS_INVALID;
      }
    }

    if (pFillInfo->current > pFillInfo->end && pFillInfo->pLinearInfo->hasNext) {
      pFillInfo->pLinearInfo->hasNext = false;
      pFillInfo->pLinearInfo->winIndex = 0;
      taosArraySwap(pFillInfo->pLinearInfo->pEndPoints, pFillInfo->pLinearInfo->pNextEndPoints);
      pFillInfo->pResRow = &pFillSup->cur;
      setFillKeyInfo(pFillSup->cur.key, pFillInfo->pLinearInfo->nextEnd, &pFillSup->interval, pFillInfo);
      doStreamFillLinear(pFillSup, pFillInfo, pRes);
    }
  }
  if (pFillInfo->pos == FILL_POS_END) {
    code = buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes, &res);
    QUERY_CHECK_CODE(code, lino, _end);
    if (res) {
      pFillInfo->pos = FILL_POS_INVALID;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

int32_t keepBlockRowInDiscBuf(SOperatorInfo* pOperator, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock, TSKEY* tsCol,
                           int32_t rowId, uint64_t groupId, int32_t rowSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  TSKEY ts = tsCol[rowId];
  pFillInfo->nextRowKey = ts;
  SResultRowData tmpNextRow = {.key = ts};
  tmpNextRow.pRowVal = taosMemoryCalloc(1, rowSize);
  QUERY_CHECK_NULL(tmpNextRow.pRowVal, code, lino, _end, terrno);
  transBlockToResultRow(pBlock, rowId, ts, &tmpNextRow);
  keepResultInDiscBuf(pOperator, groupId, &tmpNextRow, rowSize);
  taosMemoryFreeClear(tmpNextRow.pRowVal);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void doFillResults(SOperatorInfo* pOperator, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo,
                          SSDataBlock* pBlock, TSKEY* tsCol, int32_t rowId, SSDataBlock* pRes) {
  uint64_t groupId = pBlock->info.id.groupId;
  getWindowFromDiscBuf(pOperator, tsCol[rowId], groupId, pFillSup);
  if (pFillSup->prev.key == pFillInfo->preRowKey) {
    resetFillWindow(&pFillSup->prev);
  }
  setFillValueInfo(pBlock, tsCol[rowId], rowId, pFillSup, pFillInfo);
  doStreamFillRange(pFillInfo, pFillSup, pRes);
}

static void doStreamFillImpl(SOperatorInfo* pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcBlock;
  uint64_t                 groupId = pBlock->info.id.groupId;
  SSDataBlock*             pRes = pInfo->pRes;
  SColumnInfoData*         pTsCol = taosArrayGet(pInfo->pSrcBlock->pDataBlock, pInfo->primaryTsCol);
  TSKEY*                   tsCol = (TSKEY*)pTsCol->pData;
  pRes->info.id.groupId = groupId;
  pInfo->srcRowIndex++;

  if (pInfo->srcRowIndex == 0) {
    code = keepBlockRowInDiscBuf(pOperator, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex, groupId, pFillSup->rowSize);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->srcRowIndex++;
  }

  while (pInfo->srcRowIndex < pBlock->info.rows) {
    TSKEY ts = tsCol[pInfo->srcRowIndex];
    code = keepBlockRowInDiscBuf(pOperator, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex, groupId, pFillSup->rowSize);
    QUERY_CHECK_CODE(code, lino, _end);
    doFillResults(pOperator, pFillSup, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex - 1, pRes);
    if (pInfo->pRes->info.rows == pInfo->pRes->info.capacity) {
      code = blockDataUpdateTsWindow(pRes, pInfo->primaryTsCol);
      QUERY_CHECK_CODE(code, lino, _end);
      return;
    }
    pInfo->srcRowIndex++;
  }
  doFillResults(pOperator, pFillSup, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex - 1, pRes);
  code = blockDataUpdateTsWindow(pRes, pInfo->primaryTsCol);
  QUERY_CHECK_CODE(code, lino, _end);
  blockDataCleanup(pInfo->pSrcBlock);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
}

static int32_t buildDeleteRange(SOperatorInfo* pOp, TSKEY start, TSKEY end, uint64_t groupId, SSDataBlock* delRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStorageAPI*     pAPI = &pOp->pTaskInfo->storageAPI;
  void*            pState = pOp->pTaskInfo->streamInfo.pState;
  SExecTaskInfo*   pTaskInfo = pOp->pTaskInfo;
  SSDataBlock*     pBlock = delRes;
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  SColumnInfoData* pTbNameCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);
  code = colDataSetVal(pStartCol, pBlock->info.rows, (const char*)&start, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = colDataSetVal(pEndCol, pBlock->info.rows, (const char*)&end, false);
  QUERY_CHECK_CODE(code, lino, _end);

  colDataSetNULL(pUidCol, pBlock->info.rows);
  code = colDataSetVal(pGroupCol, pBlock->info.rows, (const char*)&groupId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  colDataSetNULL(pCalStartCol, pBlock->info.rows);
  colDataSetNULL(pCalEndCol, pBlock->info.rows);

  SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

  void*   tbname = NULL;
  int32_t winCode = TSDB_CODE_SUCCESS;
  code = pAPI->stateStore.streamStateGetParName(pOp->pTaskInfo->streamInfo.pState, groupId, &tbname, false, &winCode);
  QUERY_CHECK_CODE(code, lino, _end);
  if (winCode != TSDB_CODE_SUCCESS) {
    colDataSetNULL(pTableCol, pBlock->info.rows);
  } else {
    char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
    STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
    code = colDataSetVal(pTableCol, pBlock->info.rows, (const char*)parTbName, false);
    QUERY_CHECK_CODE(code, lino, _end);
    pAPI->stateStore.streamStateFreeVal(tbname);
  }

  pBlock->info.rows++;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t buildDeleteResult(SOperatorInfo* pOperator, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                                 SSDataBlock* delRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  if (hasPrevWindow(pFillSup)) {
    TSKEY start = getNextWindowTs(pFillSup->prev.key, &pFillSup->interval);
    code = buildDeleteRange(pOperator, start, endTs, groupId, delRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (hasNextWindow(pFillSup)) {
    TSKEY end = getPrevWindowTs(pFillSup->next.key, &pFillSup->interval);
    code = buildDeleteRange(pOperator, startTs, end, groupId, delRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = buildDeleteRange(pOperator, startTs, endTs, groupId, delRes);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t doDeleteFillResultImpl(SOperatorInfo* pOperator, TSKEY startTs, TSKEY endTs, uint64_t groupId) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStorageAPI*             pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  getWindowFromDiscBuf(pOperator, startTs, groupId, pInfo->pFillSup);
  setDeleteFillValueInfo(startTs, endTs, pInfo->pFillSup, pInfo->pFillInfo);
  SWinKey key = {.ts = startTs, .groupId = groupId};
  pAPI->stateStore.streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &key);
  if (!pInfo->pFillInfo->needFill) {
    code = buildDeleteResult(pOperator, startTs, endTs, groupId, pInfo->pDelRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    STimeRange tw = {
        .skey = startTs,
        .ekey = endTs,
        .groupId = groupId,
    };
    void* tmp = taosArrayPush(pInfo->pFillInfo->delRanges, &tw);
    if (!tmp) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static void getWindowInfoByKey(SStorageAPI* pAPI, void* pState, TSKEY ts, int64_t groupId, SResultRowData* pWinData) {
  SWinKey key = {.ts = ts, .groupId = groupId};
  void*   val = NULL;
  int32_t len = 0;
  int32_t code = pAPI->stateStore.streamStateFillGet(pState, &key, (void**)&val, &len);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("get window info by key failed, Data may be deleted, try next window. ts:%" PRId64 ", groupId:%" PRId64, ts,
           groupId);
    SStreamStateCur* pCur = pAPI->stateStore.streamStateFillSeekKeyNext(pState, &key);
    code = pAPI->stateStore.streamStateGetGroupKVByCur(pCur, &key, (const void**)&val, &len);
    pAPI->stateStore.streamStateFreeCur(pCur);
    qDebug("get window info by key ts:%" PRId64 ", groupId:%" PRId64 ", res%d", ts, groupId, code);
  }

  if (code == TSDB_CODE_SUCCESS) {
    resetFillWindow(pWinData);
    pWinData->key = key.ts;
    pWinData->pRowVal = val;
  }
}

static void doDeleteFillFinalize(SOperatorInfo* pOperator) {
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  int32_t                  size = taosArrayGetSize(pFillInfo->delRanges);
  while (pFillInfo->delIndex < size) {
    STimeRange* range = taosArrayGet(pFillInfo->delRanges, pFillInfo->delIndex);
    if (pInfo->pRes->info.id.groupId != 0 && pInfo->pRes->info.id.groupId != range->groupId) {
      return;
    }
    getWindowFromDiscBuf(pOperator, range->skey, range->groupId, pInfo->pFillSup);
    TSKEY realEnd = range->ekey + 1;
    if (pInfo->pFillInfo->type == TSDB_FILL_NEXT && pInfo->pFillSup->next.key != realEnd) {
      getWindowInfoByKey(pAPI, pOperator->pTaskInfo->streamInfo.pState, realEnd, range->groupId,
                         &pInfo->pFillSup->next);
    }
    setDeleteFillValueInfo(range->skey, range->ekey, pInfo->pFillSup, pInfo->pFillInfo);
    pFillInfo->delIndex++;
    if (pInfo->pFillInfo->needFill) {
      doStreamFillRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
      pInfo->pRes->info.id.groupId = range->groupId;
    }
  }
}

static int32_t doDeleteFillResult(SOperatorInfo* pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStorageAPI*             pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcDelBlock;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsStarts = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIds = (uint64_t*)pGroupCol->pData;
  while (pInfo->srcDelRowIndex < pBlock->info.rows) {
    TSKEY            ts = tsStarts[pInfo->srcDelRowIndex];
    TSKEY            endTs = ts;
    uint64_t         groupId = groupIds[pInfo->srcDelRowIndex];
    SWinKey          key = {.ts = ts, .groupId = groupId};
    SStreamStateCur* pCur = pAPI->stateStore.streamStateGetAndCheckCur(pOperator->pTaskInfo->streamInfo.pState, &key);

    if (!pCur) {
      pInfo->srcDelRowIndex++;
      continue;
    }

    SWinKey nextKey = {.groupId = groupId, .ts = ts};
    while (pInfo->srcDelRowIndex < pBlock->info.rows) {
      TSKEY    delTs = tsStarts[pInfo->srcDelRowIndex];
      uint64_t delGroupId = groupIds[pInfo->srcDelRowIndex];
      int32_t  winCode = TSDB_CODE_SUCCESS;
      if (groupId != delGroupId) {
        break;
      }
      if (delTs > nextKey.ts) {
        break;
      }

      SWinKey delKey = {.groupId = delGroupId, .ts = delTs};
      if (delTs == nextKey.ts) {
        pAPI->stateStore.streamStateCurNext(pOperator->pTaskInfo->streamInfo.pState, pCur);
        winCode = pAPI->stateStore.streamStateGetGroupKVByCur(pCur, &nextKey, NULL, NULL);
        // ts will be deleted later
        if (delTs != ts) {
          pAPI->stateStore.streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &delKey);
          pAPI->stateStore.streamStateFreeCur(pCur);
          pCur = pAPI->stateStore.streamStateGetAndCheckCur(pOperator->pTaskInfo->streamInfo.pState, &nextKey);
        }
        endTs = TMAX(delTs, nextKey.ts - 1);
        if (winCode != TSDB_CODE_SUCCESS) {
          break;
        }
      }
      pInfo->srcDelRowIndex++;
    }

    pAPI->stateStore.streamStateFreeCur(pCur);
    code = doDeleteFillResultImpl(pOperator, ts, endTs, groupId);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pFillInfo->current = pFillInfo->end + 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static void resetStreamFillInfo(SStreamFillOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->pFillSup->pResMap);
  pInfo->pFillSup->hasDelete = false;
  taosArrayClear(pInfo->pFillInfo->delRanges);
  pInfo->pFillInfo->delIndex = 0;
}

static int32_t doApplyStreamScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pSrcBlock,
                                              SSDataBlock* pDstBlock) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  blockDataCleanup(pDstBlock);
  code = blockDataEnsureCapacity(pDstBlock, pSrcBlock->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  code = setInputDataBlock(pSup, pSrcBlock, TSDB_ORDER_ASC, MAIN_SCAN, false);
  QUERY_CHECK_CODE(code, lino, _end);
  code = projectApplyFunctions(pSup->pExprInfo, pDstBlock, pSrcBlock, pSup->pCtx, pSup->numOfExprs, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  pDstBlock->info.rows = 0;
  pSup = &pInfo->pFillSup->notFillExprSup;
  code = setInputDataBlock(pSup, pSrcBlock, TSDB_ORDER_ASC, MAIN_SCAN, false);
  QUERY_CHECK_CODE(code, lino, _end);
  code = projectApplyFunctions(pSup->pExprInfo, pDstBlock, pSrcBlock, pSup->pCtx, pSup->numOfExprs, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  pDstBlock->info.id.groupId = pSrcBlock->info.id.groupId;

  code = blockDataUpdateTsWindow(pDstBlock, pInfo->primaryTsCol);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t doStreamFillNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }
  blockDataCleanup(pInfo->pRes);
  if (hasRemainCalc(pInfo->pFillInfo) ||
      (pInfo->pFillInfo->pos != FILL_POS_INVALID && pInfo->pFillInfo->needFill == true)) {
    doStreamFillRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
    if (pInfo->pRes->info.rows > 0) {
      printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pRes;
      return code;
    }
  }
  if (pOperator->status == OP_RES_TO_RETURN) {
    doDeleteFillFinalize(pOperator);
    if (pInfo->pRes->info.rows > 0) {
      printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pRes;
      return code;
    }
    setOperatorCompleted(pOperator);
    resetStreamFillInfo(pInfo);
    (*ppRes) = NULL;
    return code;
  }

  SSDataBlock*   fillResult = NULL;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    if (pInfo->srcRowIndex >= pInfo->pSrcBlock->info.rows || pInfo->pSrcBlock->info.rows == 0) {
      // If there are delete datablocks, we receive  them first.
      SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
      if (pBlock == NULL) {
        pOperator->status = OP_RES_TO_RETURN;
        pInfo->pFillInfo->preRowKey = INT64_MIN;
        if (pInfo->pRes->info.rows > 0) {
          printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
          (*ppRes) = pInfo->pRes;
          return code;
        }
        break;
      }
      printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

      if (pInfo->pFillInfo->curGroupId != pBlock->info.id.groupId) {
        pInfo->pFillInfo->curGroupId = pBlock->info.id.groupId;
        pInfo->pFillInfo->preRowKey = INT64_MIN;
      }

      pInfo->pFillSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
      if (pInfo->pFillSup->winRange.ekey <= 0) {
        pInfo->pFillSup->winRange.ekey = INT64_MAX;
      }

      switch (pBlock->info.type) {
        case STREAM_RETRIEVE:
          (*ppRes) = pBlock;
          return code;
        case STREAM_DELETE_RESULT: {
          pInfo->pSrcDelBlock = pBlock;
          pInfo->srcDelRowIndex = 0;
          blockDataCleanup(pInfo->pDelRes);
          pInfo->pFillSup->hasDelete = true;
          code = doDeleteFillResult(pOperator);
          QUERY_CHECK_CODE(code, lino, _end);

          if (pInfo->pDelRes->info.rows > 0) {
            printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
            (*ppRes) = pInfo->pDelRes;
            return code;
          }
          continue;
        } break;
        case STREAM_NORMAL:
        case STREAM_INVALID:
        case STREAM_PULL_DATA: {
          code = doApplyStreamScalarCalculation(pOperator, pBlock, pInfo->pSrcBlock);
          QUERY_CHECK_CODE(code, lino, _end);

          memcpy(pInfo->pSrcBlock->info.parTbName, pBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
          pInfo->srcRowIndex = -1;
        } break;
        case STREAM_CHECKPOINT:
        case STREAM_CREATE_CHILD_TABLE: {
          (*ppRes) = pBlock;
          return code;
        } break;
        default:
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
    }

    doStreamFillImpl(pOperator);
    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    memcpy(pInfo->pRes->info.parTbName, pInfo->pSrcBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
    pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;
    if (pInfo->pRes->info.rows > 0) {
      break;
    }
  }
  if (pOperator->status == OP_RES_TO_RETURN) {
    doDeleteFillFinalize(pOperator);
  }

  if (pInfo->pRes->info.rows == 0) {
    setOperatorCompleted(pOperator);
    resetStreamFillInfo(pInfo);
    (*ppRes) = NULL;
    return code;
  }

  pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;
  printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  (*ppRes) = pInfo->pRes;
  return code;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  setOperatorCompleted(pOperator);
  resetStreamFillInfo(pInfo);
  (*ppRes) = NULL;
  return code;
}

static int32_t initResultBuf(SSDataBlock* pInputRes, SStreamFillSupporter* pFillSup) {
  int32_t numOfCols = taosArrayGetSize(pInputRes->pDataBlock);
  pFillSup->rowSize = sizeof(SResultCellData) * numOfCols;
  for (int i = 0; i < numOfCols; i++) {
    SColumnInfoData* pCol = taosArrayGet(pInputRes->pDataBlock, i);
    pFillSup->rowSize += pCol->info.bytes;
  }
  pFillSup->next.key = INT64_MIN;
  pFillSup->nextNext.key = INT64_MIN;
  pFillSup->prev.key = INT64_MIN;
  pFillSup->cur.key = INT64_MIN;
  pFillSup->next.pRowVal = NULL;
  pFillSup->nextNext.pRowVal = NULL;
  pFillSup->prev.pRowVal = NULL;
  pFillSup->cur.pRowVal = NULL;

  return TSDB_CODE_SUCCESS;
}

static SStreamFillSupporter* initStreamFillSup(SStreamFillPhysiNode* pPhyFillNode, SInterval* pInterval,
                                               SExprInfo* pFillExprInfo, int32_t numOfFillCols, SStorageAPI* pAPI, SSDataBlock* pInputRes) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamFillSupporter* pFillSup = taosMemoryCalloc(1, sizeof(SStreamFillSupporter));
  if (!pFillSup) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pFillSup->numOfFillCols = numOfFillCols;
  int32_t    numOfNotFillCols = 0;
  SExprInfo* noFillExprInfo = NULL;

  code = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &noFillExprInfo, &numOfNotFillCols);
  QUERY_CHECK_CODE(code, lino, _end);

  pFillSup->pAllColInfo = createFillColInfo(pFillExprInfo, pFillSup->numOfFillCols, noFillExprInfo, numOfNotFillCols,
                                            (const SNodeListNode*)(pPhyFillNode->pValues));
  if (pFillSup->pAllColInfo == NULL) {
    code = terrno;
    lino = __LINE__;
    destroyExprInfo(noFillExprInfo, numOfNotFillCols);
    goto _end;
  }

  pFillSup->type = convertFillType(pPhyFillNode->mode);
  pFillSup->numOfAllCols = pFillSup->numOfFillCols + numOfNotFillCols;
  pFillSup->interval = *pInterval;
  pFillSup->pAPI = pAPI;

  code = initResultBuf(pInputRes, pFillSup);
  QUERY_CHECK_CODE(code, lino, _end);

  SExprInfo* noFillExpr = NULL;
  code = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &noFillExpr, &numOfNotFillCols);
  QUERY_CHECK_CODE(code, lino, _end);

  code = initExprSupp(&pFillSup->notFillExprSup, noFillExpr, numOfNotFillCols, &pAPI->functionStore);
  QUERY_CHECK_CODE(code, lino, _end);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pFillSup->pResMap = tSimpleHashInit(16, hashFn);
  QUERY_CHECK_NULL(pFillSup->pResMap, code, lino, _end, terrno);
  pFillSup->hasDelete = false;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    destroyStreamFillSupporter(pFillSup);
    pFillSup = NULL;
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return pFillSup;
}

SStreamFillInfo* initStreamFillInfo(SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamFillInfo* pFillInfo = taosMemoryCalloc(1, sizeof(SStreamFillInfo));
  if (!pFillInfo) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pFillInfo->start = INT64_MIN;
  pFillInfo->current = INT64_MIN;
  pFillInfo->end = INT64_MIN;
  pFillInfo->preRowKey = INT64_MIN;
  pFillInfo->needFill = false;
  pFillInfo->pLinearInfo = taosMemoryCalloc(1, sizeof(SStreamFillLinearInfo));
  if (!pFillInfo) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pFillInfo->pLinearInfo->hasNext = false;
  pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
  pFillInfo->pLinearInfo->pEndPoints = NULL;
  pFillInfo->pLinearInfo->pNextEndPoints = NULL;
  if (pFillSup->type == TSDB_FILL_LINEAR) {
    pFillInfo->pLinearInfo->pEndPoints = taosArrayInit(pFillSup->numOfAllCols, sizeof(SPoint));
    if (!pFillInfo->pLinearInfo->pEndPoints) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pFillInfo->pLinearInfo->pNextEndPoints = taosArrayInit(pFillSup->numOfAllCols, sizeof(SPoint));
    if (!pFillInfo->pLinearInfo->pNextEndPoints) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    for (int32_t i = 0; i < pFillSup->numOfAllCols; i++) {
      SColumnInfoData* pColData = taosArrayGet(pRes->pDataBlock, i);
      SPoint           value = {0};
      value.val = taosMemoryCalloc(1, pColData->info.bytes);
      if (!value.val) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      void* tmpRes = taosArrayPush(pFillInfo->pLinearInfo->pEndPoints, &value);
      if (!tmpRes) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      value.val = taosMemoryCalloc(1, pColData->info.bytes);
      if (!value.val) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      tmpRes = taosArrayPush(pFillInfo->pLinearInfo->pNextEndPoints, &value);
      if (!tmpRes) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }
  pFillInfo->pLinearInfo->winIndex = 0;

  pFillInfo->pResRow = NULL;
  if (pFillSup->type == TSDB_FILL_SET_VALUE || pFillSup->type == TSDB_FILL_SET_VALUE_F ||
      pFillSup->type == TSDB_FILL_NULL || pFillSup->type == TSDB_FILL_NULL_F) {
    pFillInfo->pResRow = taosMemoryCalloc(1, sizeof(SResultRowData));
    if (!pFillInfo->pResRow) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pFillInfo->pResRow->key = INT64_MIN;
    pFillInfo->pResRow->pRowVal = taosMemoryCalloc(1, pFillSup->rowSize);
    if (!pFillInfo->pResRow->pRowVal) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SColumnInfoData* pColData = taosArrayGet(pRes->pDataBlock, i);
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, i);
      pCell->bytes = pColData->info.bytes;
      pCell->type = pColData->info.type;
    }
  }

  pFillInfo->type = pFillSup->type;
  pFillInfo->delRanges = taosArrayInit(16, sizeof(STimeRange));
  if (!pFillInfo->delRanges) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pFillInfo->delIndex = 0;
  pFillInfo->curGroupId = 0;
  return pFillInfo;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  destroyStreamFillInfo(pFillInfo);
  return NULL;
}

int32_t createStreamFillOperatorInfo(SOperatorInfo* downstream, SStreamFillPhysiNode* pPhyFillNode,
                                            SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamFillOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SInterval* pInterval = &((SStreamIntervalOperatorInfo*)downstream->info)->interval;
  int32_t    numOfFillCols = 0;
  SExprInfo* pFillExprInfo = NULL;

  code = createExprInfo(pPhyFillNode->pFillExprs, NULL, &pFillExprInfo, &numOfFillCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(&pOperator->exprSupp, pFillExprInfo, numOfFillCols, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pSrcBlock = createDataBlockFromDescNode(pPhyFillNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pSrcBlock, code, lino, _error, terrno);

  pInfo->pFillSup = initStreamFillSup(pPhyFillNode, pInterval, pFillExprInfo, numOfFillCols, &pTaskInfo->storageAPI,
                                      pInfo->pSrcBlock);
  if (!pInfo->pFillSup) {
    code = TSDB_CODE_FAILED;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  pInfo->pRes = createDataBlockFromDescNode(pPhyFillNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = blockDataEnsureCapacity(pInfo->pSrcBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pFillInfo = initStreamFillInfo(pInfo->pFillSup, pInfo->pRes);
  if (!pInfo->pFillInfo) {
    goto _error;
  }

  if (pInfo->pFillInfo->type == TSDB_FILL_SET_VALUE || pInfo->pFillInfo->type == TSDB_FILL_SET_VALUE_F) {
    for (int32_t i = 0; i < pInfo->pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pInfo->pFillSup->pAllColInfo + i;
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SResultCellData* pCell = getResultCell(pInfo->pFillInfo->pResRow, slotId);
      SVariant*        pVar = &(pFillCol->fillVal);
      if (pCell->type == TSDB_DATA_TYPE_FLOAT) {
        float v = 0;
        GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else if (IS_FLOAT_TYPE(pCell->type)) {
        double v = 0;
        GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else if (IS_INTEGER_TYPE(pCell->type)) {
        int64_t v = 0;
        GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else {
        pCell->isNull = true;
      }
    }
  } else if (pInfo->pFillInfo->type == TSDB_FILL_NULL || pInfo->pFillInfo->type == TSDB_FILL_NULL_F) {
    for (int32_t i = 0; i < pInfo->pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pInfo->pFillSup->pAllColInfo + i;
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SResultCellData* pCell = getResultCell(pInfo->pFillInfo->pResRow, slotId);
      pCell->isNull = true;
    }
  }

  code = createSpecialDataBlock(STREAM_DELETE_RESULT, &pInfo->pDelRes);
  QUERY_CHECK_CODE(code, lino, _error);

  code = blockDataEnsureCapacity(pInfo->pDelRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->primaryTsCol = ((STargetNode*)pPhyFillNode->pWStartTs)->slotId;
  pInfo->primarySrcSlotId = ((SColumnNode*)((STargetNode*)pPhyFillNode->pWStartTs)->pExpr)->slotId;

  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pPhyFillNode->pFillExprs, pPhyFillNode->node.pOutputDataBlockDesc, &numOfOutputCols,
                             COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pPhyFillNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->srcRowIndex = -1;
  setOperatorInfo(pOperator, "StreamFillOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamFillNext, NULL, destroyStreamFillOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamOpReleaseState, streamOpReloadState);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));

  if (pInfo != NULL) destroyStreamFillOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}
