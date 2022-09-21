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
#include "executorimpl.h"
#include "function.h"
#include "functionMgt.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP = 2,
} SResultTsInterpType;

#define IS_FINAL_OP(op) ((op)->isFinal)

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
} SPullWindowInfo;

typedef struct SOpenWindowInfo {
  SResultRowPosition pos;
  uint64_t           groupId;
} SOpenWindowInfo;

static SSDataBlock* doStreamSessionAgg(SOperatorInfo* pOperator);

static int64_t* extractTsCol(SSDataBlock* pBlock, const SIntervalAggOperatorInfo* pInfo);

static SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult,
                                              uint64_t groupId);
static void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult);

static TSKEY getStartTsKey(STimeWindow* win, const TSKEY* tsCols) { return tsCols == NULL ? win->skey : tsCols[0]; }

static int32_t setTimeWindowOutputBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, bool masterscan,
                                      SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                      int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup,
                                      SExecTaskInfo* pTaskInfo) {
  SResultRow* pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                                  masterscan, tableGroupId, pTaskInfo, true, pAggSup);

  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // set time window for current result
  pResultRow->win = (*win);

  *pResult = pResultRow;
  setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);

  return TSDB_CODE_SUCCESS;
}

static void updateTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pWin, bool includeEndpoint) {
  int64_t* ts = (int64_t*)pColData->pData;
  int32_t  delta = includeEndpoint ? 1 : 0;

  int64_t duration = pWin->ekey - pWin->skey + delta;
  ts[2] = duration;            // set the duration
  ts[3] = pWin->skey;          // window start key
  ts[4] = pWin->ekey + delta;  // window end key
}

static void doKeepTuple(SWindowRowsSup* pRowSup, int64_t ts, uint64_t groupId) {
  pRowSup->win.ekey = ts;
  pRowSup->prevTs = ts;
  pRowSup->numOfRows += 1;
  pRowSup->groupId = groupId;
}

static void doKeepNewWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, int32_t rowIndex,
                                     uint64_t groupId) {
  pRowSup->startRowIndex = rowIndex;
  pRowSup->numOfRows = 0;
  pRowSup->win.skey = tsList[rowIndex];
  pRowSup->groupId = groupId;
}

static FORCE_INLINE int32_t getForwardStepsInBlock(int32_t numOfRows, __block_search_fn_t searchFn, TSKEY ekey,
                                                   int16_t pos, int16_t order, int64_t* pData) {
  int32_t forwardRows = 0;

  if (order == TSDB_ORDER_ASC) {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardRows = end;

      if (pData[end + pos] == ekey) {
        forwardRows += 1;
      }
    }
  } else {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardRows = end;

      if (pData[end + pos] == ekey) {
        forwardRows += 1;
      }
    }
    //    int32_t end = searchFn((char*)pData, pos + 1, ekey, order);
    //    if (end >= 0) {
    //      forwardRows = pos - end;
    //
    //      if (pData[end] == ekey) {
    //        forwardRows += 1;
    //      }
    //    }
  }

  assert(forwardRows >= 0);
  return forwardRows;
}

int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order) {
  int32_t midPos = -1;
  int32_t numOfRows;

  if (num <= 0) {
    return -1;
  }

  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);

  TSKEY*  keyList = (TSKEY*)pValue;
  int32_t firstPos = 0;
  int32_t lastPos = num - 1;

  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key < keyList[lastPos]) {
        lastPos += 1;
        if (lastPos >= num) {
          return -1;
        } else {
          return lastPos;
        }
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < keyList[midPos]) {
        firstPos = midPos + 1;
      } else if (key > keyList[midPos]) {
        lastPos = midPos - 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger than the key
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1u) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos, TSKEY ekey,
                                 __block_search_fn_t searchFn, STableQueryInfo* item, int32_t order) {
  assert(startPos >= 0 && startPos < pDataBlockInfo->rows);

  int32_t num = -1;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (order == TSDB_ORDER_ASC) {
    if (ekey < pDataBlockInfo->window.ekey && pPrimaryColumn) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (item != NULL) {
        item->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
      }
    } else {
      num = pDataBlockInfo->rows - startPos;
      if (item != NULL) {
        item->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  } else {  // desc
    if (ekey > pDataBlockInfo->window.skey && pPrimaryColumn) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (item != NULL) {
        item->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
      }
    } else {
      num = pDataBlockInfo->rows - startPos;
      if (item != NULL) {
        item->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  }

  assert(num >= 0);
  return num;
}

static void getNextTimeWindow(SInterval* pInterval, int32_t precision, int32_t order, STimeWindow* tw) {
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(order);
  if (pInterval->intervalUnit != 'n' && pInterval->intervalUnit != 'y') {
    tw->skey += pInterval->sliding * factor;
    tw->ekey = tw->skey + pInterval->interval - 1;
    return;
  }

  int64_t key = tw->skey, interval = pInterval->interval;
  // convert key to second
  key = convertTimePrecision(key, precision, TSDB_TIME_PRECISION_MILLI) / 1000;

  if (pInterval->intervalUnit == 'y') {
    interval *= 12;
  }

  struct tm tm;
  time_t    t = (time_t)key;
  taosLocalTime(&t, &tm);

  int mon = (int)(tm.tm_year * 12 + tm.tm_mon + interval * factor);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->skey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000LL, TSDB_TIME_PRECISION_MILLI, precision);

  mon = (int)(mon + interval);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->ekey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000LL, TSDB_TIME_PRECISION_MILLI, precision);

  tw->ekey -= 1;
}

void doTimeWindowInterpolation(SArray* pPrevValues, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY curTs,
                               int32_t curRowIndex, TSKEY windowKey, int32_t type, SExprSupp* pSup) {
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t index = 1;
  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    if (!fmIsIntervalInterpoFunc(pCtx[k].functionId)) {
      pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SFunctParam*     pParam = &pCtx[k].param[0];
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, pParam->pCol->slotId);

    ASSERT(pColInfo->info.type == pParam->pCol->type && curTs != windowKey);

    double v1 = 0, v2 = 0, v = 0;
    if (prevRowIndex == -1) {
      SGroupKeys* p = taosArrayGet(pPrevValues, index);
      GET_TYPED_DATA(v1, double, pColInfo->info.type, p->pData);
    } else {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, colDataGetData(pColInfo, prevRowIndex));
    }

    GET_TYPED_DATA(v2, double, pColInfo->info.type, colDataGetData(pColInfo, curRowIndex));

#if 0
    if (functionId == FUNCTION_INTERP) {
      if (type == RESULT_ROW_START_INTERP) {
        pCtx[k].start.key = prevTs;
        pCtx[k].start.val = v1;

        pCtx[k].end.key = curTs;
        pCtx[k].end.val = v2;

        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
          if (prevRowIndex == -1) {
            //            pCtx[k].start.ptr = (char*)pRuntimeEnv->prevRow[index];
          } else {
            pCtx[k].start.ptr = (char*)pColInfo->pData + prevRowIndex * pColInfo->info.bytes;
          }

          pCtx[k].end.ptr = (char*)pColInfo->pData + curRowIndex * pColInfo->info.bytes;
        }
      }
    } else if (functionId == FUNCTION_TWA) {
#endif

    SPoint point1 = (SPoint){.key = prevTs, .val = &v1};
    SPoint point2 = (SPoint){.key = curTs, .val = &v2};
    SPoint point = (SPoint){.key = windowKey, .val = &v};

    taosGetLinearInterpolationVal(&point, TSDB_DATA_TYPE_DOUBLE, &point1, &point2, TSDB_DATA_TYPE_DOUBLE);

    if (type == RESULT_ROW_START_INTERP) {
      pCtx[k].start.key = point.key;
      pCtx[k].start.val = v;
    } else {
      pCtx[k].end.key = point.key;
      pCtx[k].end.val = v;
    }

    index += 1;
  }
#if 0
  }
#endif
}

static void setNotInterpoWindowKey(SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t type) {
  if (type == RESULT_ROW_START_INTERP) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      pCtx[k].start.key = INT64_MIN;
    }
  } else {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      pCtx[k].end.key = INT64_MIN;
    }
  }
}

static bool setTimeWindowInterpolationStartTs(SIntervalAggOperatorInfo* pInfo, int32_t pos, SSDataBlock* pBlock,
                                              const TSKEY* tsCols, STimeWindow* win, SExprSupp* pSup) {
  bool ascQuery = (pInfo->inputOrder == TSDB_ORDER_ASC);

  TSKEY curTs = tsCols[pos];

  SGroupKeys* pTsKey = taosArrayGet(pInfo->pPrevValues, 0);
  TSKEY       lastTs = *(int64_t*)pTsKey->pData;

  // lastTs == INT64_MIN and pos == 0 means this is the first time window, interpolation is not needed.
  // start exactly from this point, no need to do interpolation
  TSKEY key = ascQuery ? win->skey : win->ekey;
  if (key == curTs) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_START_INTERP);
    return true;
  }

  // it is the first time window, no need to do interpolation
  if (pTsKey->isNull && pos == 0) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_START_INTERP);
  } else {
    TSKEY prevTs = ((pos == 0) ? lastTs : tsCols[pos - 1]);
    doTimeWindowInterpolation(pInfo->pPrevValues, pBlock->pDataBlock, prevTs, pos - 1, curTs, pos, key,
                              RESULT_ROW_START_INTERP, pSup);
  }

  return true;
}

static bool setTimeWindowInterpolationEndTs(SIntervalAggOperatorInfo* pInfo, SExprSupp* pSup, int32_t endRowIndex,
                                            SArray* pDataBlock, const TSKEY* tsCols, TSKEY blockEkey,
                                            STimeWindow* win) {
  int32_t order = pInfo->inputOrder;

  TSKEY actualEndKey = tsCols[endRowIndex];
  TSKEY key = (order == TSDB_ORDER_ASC) ? win->ekey : win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey && (order == TSDB_ORDER_ASC)) || (key < blockEkey && (order == TSDB_ORDER_DESC))) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_END_INTERP);
    return false;
  }

  // there is actual end point of current time window, no interpolation needs
  if (key == actualEndKey) {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_END_INTERP);
    return true;
  }

  int32_t nextRowIndex = endRowIndex + 1;
  assert(nextRowIndex >= 0);

  TSKEY nextKey = tsCols[nextRowIndex];
  doTimeWindowInterpolation(pInfo->pPrevValues, pDataBlock, actualEndKey, endRowIndex, nextKey, nextRowIndex, key,
                            RESULT_ROW_END_INTERP, pSup);
  return true;
}

bool inSlidingWindow(SInterval* pInterval, STimeWindow* pWin, SDataBlockInfo* pBlockInfo) {
  if (pInterval->interval != pInterval->sliding &&
      (pWin->ekey < pBlockInfo->calWin.skey || pWin->skey > pBlockInfo->calWin.ekey)) {
    return false;
  }
  return true;
}

static int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                      TSKEY* primaryKeys, int32_t prevPosition, int32_t order) {
  bool ascQuery = (order == TSDB_ORDER_ASC);

  int32_t precision = pInterval->precision;
  getNextTimeWindow(pInterval, precision, order, pNext);

  // next time window is not in current block
  if ((pNext->skey > pDataBlockInfo->window.ekey && order == TSDB_ORDER_ASC) ||
      (pNext->ekey < pDataBlockInfo->window.skey && order == TSDB_ORDER_DESC)) {
    return -1;
  }

  if (!inSlidingWindow(pInterval, pNext, pDataBlockInfo) && order == TSDB_ORDER_ASC) {
    return -1;
  }

  TSKEY   skey = ascQuery ? pNext->skey : pNext->ekey;
  int32_t startPos = 0;

  // tumbling time window query, a special case of sliding time window query
  if (pInterval->sliding == pInterval->interval && prevPosition != -1) {
    startPos = prevPosition + 1;
  } else {
    if ((skey <= pDataBlockInfo->window.skey && ascQuery) || (skey >= pDataBlockInfo->window.ekey && !ascQuery)) {
      startPos = 0;
    } else {
      startPos = binarySearchForKey((char*)primaryKeys, pDataBlockInfo->rows, skey, order);
    }
  }

  /* interp query with fill should not skip time window */
  //  if (pQueryAttr->pointInterpQuery && pQueryAttr->fillType != TSDB_FILL_NONE) {
  //    return startPos;
  //  }

  /*
   * This time window does not cover any data, try next time window,
   * this case may happen when the time window is too small
   */
  if (primaryKeys == NULL) {
    if (ascQuery) {
      assert(pDataBlockInfo->window.skey <= pNext->ekey);
    } else {
      assert(pDataBlockInfo->window.ekey >= pNext->skey);
    }
  } else {
    if (ascQuery && primaryKeys[startPos] > pNext->ekey) {
      TSKEY next = primaryKeys[startPos];
      if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, pInterval, precision);
        pNext->ekey = taosTimeAdd(pNext->skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
      } else {
        pNext->ekey += ((next - pNext->ekey + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
        pNext->skey = pNext->ekey - pInterval->interval + 1;
      }
    } else if ((!ascQuery) && primaryKeys[startPos] < pNext->skey) {
      TSKEY next = primaryKeys[startPos];
      if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, pInterval, precision);
        pNext->ekey = taosTimeAdd(pNext->skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
      } else {
        pNext->skey -= ((pNext->skey - next + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
        pNext->ekey = pNext->skey + pInterval->interval - 1;
      }
    }
  }

  return startPos;
}

static bool isResultRowInterpolated(SResultRow* pResult, SResultTsInterpType type) {
  ASSERT(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
  if (type == RESULT_ROW_START_INTERP) {
    return pResult->startInterp == true;
  } else {
    return pResult->endInterp == true;
  }
}

static void setResultRowInterpo(SResultRow* pResult, SResultTsInterpType type) {
  assert(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
  if (type == RESULT_ROW_START_INTERP) {
    pResult->startInterp = true;
  } else {
    pResult->endInterp = true;
  }
}

static void doWindowBorderInterpolation(SIntervalAggOperatorInfo* pInfo, SSDataBlock* pBlock, SResultRow* pResult,
                                        STimeWindow* win, int32_t startPos, int32_t forwardRows, SExprSupp* pSup) {
  if (!pInfo->timeWindowInterpo) {
    return;
  }

  ASSERT(pBlock != NULL);
  if (pBlock->pDataBlock == NULL) {
    //    tscError("pBlock->pDataBlock == NULL");
    return;
  }

  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);

  TSKEY* tsCols = (TSKEY*)(pColInfo->pData);
  bool   done = isResultRowInterpolated(pResult, RESULT_ROW_START_INTERP);
  if (!done) {  // it is not interpolated, now start to generated the interpolated value
    bool interp = setTimeWindowInterpolationStartTs(pInfo, startPos, pBlock, tsCols, win, pSup);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_START_INTERP);
  }

  // point interpolation does not require the end key time window interpolation.
  //  if (pointInterpQuery) {
  //    return;
  //  }

  // interpolation query does not generate the time window end interpolation
  done = isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP);
  if (!done) {
    int32_t endRowIndex = startPos + forwardRows - 1;

    TSKEY endKey = (pInfo->inputOrder == TSDB_ORDER_ASC) ? pBlock->info.window.ekey : pBlock->info.window.skey;
    bool  interp = setTimeWindowInterpolationEndTs(pInfo, pSup, endRowIndex, pBlock->pDataBlock, tsCols, endKey, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pSup->pCtx, pSup->numOfExprs, RESULT_ROW_END_INTERP);
  }
}

static void saveDataBlockLastRow(SArray* pPrevKeys, const SSDataBlock* pBlock, SArray* pCols) {
  if (pBlock->pDataBlock == NULL) {
    return;
  }

  size_t num = taosArrayGetSize(pPrevKeys);
  for (int32_t k = 0; k < num; ++k) {
    SColumn* pc = taosArrayGet(pCols, k);

    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, pc->slotId);

    SGroupKeys* pkey = taosArrayGet(pPrevKeys, k);
    for (int32_t i = pBlock->info.rows - 1; i >= 0; --i) {
      if (colDataIsNull_s(pColInfo, i)) {
        continue;
      }

      char* val = colDataGetData(pColInfo, i);
      if (IS_VAR_DATA_TYPE(pkey->type)) {
        memcpy(pkey->pData, val, varDataTLen(val));
        ASSERT(varDataTLen(val) <= pkey->bytes);
      } else {
        memcpy(pkey->pData, val, pkey->bytes);
      }

      break;
    }
  }
}

static void doInterpUnclosedTimeWindow(SOperatorInfo* pOperatorInfo, int32_t numOfExprs, SResultRowInfo* pResultRowInfo,
                                       SSDataBlock* pBlock, int32_t scanFlag, int64_t* tsCols, SResultRowPosition* p) {
  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;

  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)pOperatorInfo->info;
  SExprSupp*                pSup = &pOperatorInfo->exprSupp;

  int32_t startPos = 0;
  int32_t numOfOutput = pSup->numOfExprs;

  SResultRow* pResult = NULL;

  while (1) {
    SListNode*          pn = tdListGetHead(pResultRowInfo->openWindow);
    SOpenWindowInfo*    pOpenWin = (SOpenWindowInfo*)pn->data;
    uint64_t            groupId = pOpenWin->groupId;
    SResultRowPosition* p1 = &pOpenWin->pos;
    if (p->pageId == p1->pageId && p->offset == p1->offset) {
      break;
    }

    SResultRow* pr = getResultRowByPos(pInfo->aggSup.pResultBuf, p1, false);
    ASSERT(pr->offset == p1->offset && pr->pageId == p1->pageId);

    if (pr->closed) {
      ASSERT(isResultRowInterpolated(pr, RESULT_ROW_START_INTERP) &&
             isResultRowInterpolated(pr, RESULT_ROW_END_INTERP));
      SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
      taosMemoryFree(pNode);
      continue;
    }

    STimeWindow w = pr->win;
    int32_t     ret = setTimeWindowOutputBuf(pResultRowInfo, &w, (scanFlag == MAIN_SCAN), &pResult, groupId, pSup->pCtx,
                                             numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    ASSERT(!isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

    SGroupKeys* pTsKey = taosArrayGet(pInfo->pPrevValues, 0);
    int64_t     prevTs = *(int64_t*)pTsKey->pData;
    if (groupId == pBlock->info.groupId) {
      doTimeWindowInterpolation(pInfo->pPrevValues, pBlock->pDataBlock, prevTs, -1, tsCols[startPos], startPos, w.ekey,
                                RESULT_ROW_END_INTERP, pSup);
    }

    setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    setNotInterpoWindowKey(pSup->pCtx, numOfExprs, RESULT_ROW_START_INTERP);

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &w, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, 0, pBlock->info.rows,
                     numOfExprs);

    if (isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
      closeResultRow(pr);
      SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
      taosMemoryFree(pNode);
    } else {  // the remains are can not be closed yet.
      break;
    }
  }
}

void printDataBlock(SSDataBlock* pBlock, const char* flag) {
  if (!pBlock || pBlock->info.rows == 0) {
    qDebug("===stream===printDataBlock: Block is Null or Empty");
    return;
  }
  char* pBuf = NULL;
  qDebug("%s", dumpBlockData(pBlock, flag, &pBuf));
  taosMemoryFree(pBuf);
}

typedef int32_t (*__compare_fn_t)(void* pKey, void* data, int32_t index);

int32_t binarySearchCom(void* keyList, int num, void* pKey, int order, __compare_fn_t comparefn) {
  int firstPos = 0, lastPos = num - 1, midPos = -1;
  int numOfRows = 0;

  if (num <= 0) return -1;
  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller or equal than the key
    while (1) {
      if (comparefn(pKey, keyList, lastPos) >= 0) return lastPos;
      if (comparefn(pKey, keyList, firstPos) == 0) return firstPos;
      if (comparefn(pKey, keyList, firstPos) < 0) return firstPos - 1;

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (comparefn(pKey, keyList, midPos) < 0) {
        lastPos = midPos - 1;
      } else if (comparefn(pKey, keyList, midPos) > 0) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger or equal than the key
    while (1) {
      if (comparefn(pKey, keyList, firstPos) <= 0) return firstPos;
      if (comparefn(pKey, keyList, lastPos) == 0) return lastPos;

      if (comparefn(pKey, keyList, lastPos) > 0) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (comparefn(pKey, keyList, midPos) < 0) {
        lastPos = midPos - 1;
      } else if (comparefn(pKey, keyList, midPos) > 0) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

typedef int64_t (*__get_value_fn_t)(void* data, int32_t index);

int32_t binarySearch(void* keyList, int num, TSKEY key, int order, __get_value_fn_t getValuefn) {
  int firstPos = 0, lastPos = num - 1, midPos = -1;
  int numOfRows = 0;

  if (num <= 0) return -1;
  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller or equal than the key
    while (1) {
      if (key >= getValuefn(keyList, lastPos)) return lastPos;
      if (key == getValuefn(keyList, firstPos)) return firstPos;
      if (key < getValuefn(keyList, firstPos)) return firstPos - 1;

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < getValuefn(keyList, midPos)) {
        lastPos = midPos - 1;
      } else if (key > getValuefn(keyList, midPos)) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger or equal than the key
    while (1) {
      if (key <= getValuefn(keyList, firstPos)) return firstPos;
      if (key == getValuefn(keyList, lastPos)) return lastPos;

      if (key > getValuefn(keyList, lastPos)) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < getValuefn(keyList, midPos)) {
        lastPos = midPos - 1;
      } else if (key > getValuefn(keyList, midPos)) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

int32_t comparePullWinKey(void* pKey, void* data, int32_t index) {
  SArray*          res = (SArray*)data;
  SPullWindowInfo* pos = taosArrayGet(res, index);
  SPullWindowInfo* pData = (SPullWindowInfo*)pKey;
  if (pData->window.skey == pos->window.skey) {
    if (pData->groupId > pos->groupId) {
      return 1;
    } else if (pData->groupId < pos->groupId) {
      return -1;
    }
    return 0;
  } else if (pData->window.skey > pos->window.skey) {
    return 1;
  }
  return -1;
}

static int32_t savePullWindow(SPullWindowInfo* pPullInfo, SArray* pPullWins) {
  int32_t size = taosArrayGetSize(pPullWins);
  int32_t index = binarySearchCom(pPullWins, size, pPullInfo, TSDB_ORDER_DESC, comparePullWinKey);
  if (index == -1) {
    index = 0;
  } else {
    if (comparePullWinKey(pPullInfo, pPullWins, index) > 0) {
      index++;
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }
  if (taosArrayInsert(pPullWins, index, pPullInfo) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t compareResKey(void* pKey, void* data, int32_t index) {
  SArray*     res = (SArray*)data;
  SResKeyPos* pos = taosArrayGetP(res, index);
  SWinKey*    pData = (SWinKey*)pKey;
  if (pData->ts == *(int64_t*)pos->key) {
    if (pData->groupId > pos->groupId) {
      return 1;
    } else if (pData->groupId < pos->groupId) {
      return -1;
    }
    return 0;
  } else if (pData->ts > *(int64_t*)pos->key) {
    return 1;
  }
  return -1;
}

static int32_t saveResult(int64_t ts, int32_t pageId, int32_t offset, uint64_t groupId, SArray* pUpdated) {
  int32_t size = taosArrayGetSize(pUpdated);
  SWinKey data = {.ts = ts, .groupId = groupId};
  int32_t index = binarySearchCom(pUpdated, size, &data, TSDB_ORDER_DESC, compareResKey);
  if (index == -1) {
    index = 0;
  } else {
    if (compareResKey(&data, pUpdated, index) > 0) {
      index++;
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }

  SResKeyPos* newPos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
  if (newPos == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  newPos->groupId = groupId;
  newPos->pos = (SResultRowPosition){.pageId = pageId, .offset = offset};
  *(int64_t*)newPos->key = ts;
  if (taosArrayInsert(pUpdated, index, &newPos) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t saveWinResult(int64_t ts, int32_t pageId, int32_t offset, uint64_t groupId, SHashObj* pUpdatedMap) {
  SResKeyPos* newPos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
  if (newPos == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  newPos->groupId = groupId;
  newPos->pos = (SResultRowPosition){.pageId = pageId, .offset = offset};
  *(int64_t*)newPos->key = ts;
  SWinKey key = {.ts = ts, .groupId = groupId};
  if (taosHashPut(pUpdatedMap, &key, sizeof(SWinKey), &newPos, sizeof(void*)) != TSDB_CODE_SUCCESS) {
    taosMemoryFree(newPos);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t saveWinResultRow(SResultRow* result, uint64_t groupId, SHashObj* pUpdatedMap) {
  return saveWinResult(result->win.skey, result->pageId, result->offset, groupId, pUpdatedMap);
}

static int32_t saveWinResultInfo(TSKEY ts, uint64_t groupId, SHashObj* pUpdatedMap) {
  return saveWinResult(ts, -1, -1, groupId, pUpdatedMap);
}

static int32_t saveResultRow(SResultRow* result, uint64_t groupId, SArray* pUpdated) {
  return saveResult(result->win.skey, result->pageId, result->offset, groupId, pUpdated);
}

static void removeResults(SArray* pWins, SHashObj* pUpdatedMap) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey* pW = taosArrayGet(pWins, i);
    taosHashRemove(pUpdatedMap, pW, sizeof(SWinKey));
  }
}

int64_t getWinReskey(void* data, int32_t index) {
  SArray*  res = (SArray*)data;
  SWinKey* pos = taosArrayGet(res, index);
  return pos->ts;
}

int32_t compareWinRes(void* pKey, void* data, int32_t index) {
  SArray*     res = (SArray*)data;
  SWinKey*    pos = taosArrayGet(res, index);
  SResKeyPos* pData = (SResKeyPos*)pKey;
  if (*(int64_t*)pData->key == pos->ts) {
    if (pData->groupId > pos->groupId) {
      return 1;
    } else if (pData->groupId < pos->groupId) {
      return -1;
    }
    return 0;
  } else if (*(int64_t*)pData->key > pos->ts) {
    return 1;
  }
  return -1;
}

static void removeDeleteResults(SHashObj* pUpdatedMap, SArray* pDelWins) {
  int32_t delSize = taosArrayGetSize(pDelWins);
  if (taosHashGetSize(pUpdatedMap) == 0 || delSize == 0) {
    return;
  }
  void* pIte = NULL;
  while ((pIte = taosHashIterate(pUpdatedMap, pIte)) != NULL) {
    SResKeyPos* pResKey = *(SResKeyPos**)pIte;
    int32_t     index = binarySearchCom(pDelWins, delSize, pResKey, TSDB_ORDER_DESC, compareWinRes);
    if (index >= 0 && 0 == compareWinRes(pResKey, pDelWins, index)) {
      taosArrayRemove(pDelWins, index);
      delSize = taosArrayGetSize(pDelWins);
    }
  }
}

bool isOverdue(TSKEY ekey, STimeWindowAggSupp* pTwSup) {
  ASSERT(pTwSup->maxTs == INT64_MIN || pTwSup->maxTs > 0);
  return pTwSup->maxTs != INT64_MIN && ekey < pTwSup->maxTs - pTwSup->waterMark;
}

bool isCloseWindow(STimeWindow* pWin, STimeWindowAggSupp* pTwSup) { return isOverdue(pWin->ekey, pTwSup); }

bool needDeleteWindowBuf(STimeWindow* pWin, STimeWindowAggSupp* pTwSup) {
  return pTwSup->maxTs != INT64_MIN && pWin->ekey < pTwSup->maxTs - pTwSup->deleteMark;
}

static void hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                            int32_t scanFlag) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;

  int32_t     startPos = 0;
  int32_t     numOfOutput = pSup->numOfExprs;
  int64_t*    tsCols = extractTsCol(pBlock, pInfo);
  uint64_t    tableGroupId = pBlock->info.groupId;
  bool        ascScan = (pInfo->inputOrder == TSDB_ORDER_ASC);
  TSKEY       ts = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow* pResult = NULL;

  STimeWindow win =
      getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, pInfo->inputOrder);
  int32_t ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->inputOrder);
  ASSERT(forwardRows > 0);

  // prev time window not interpolation yet.
  if (pInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult, tableGroupId);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                 numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    // window start key interpolation
    doWindowBorderInterpolation(pInfo, pBlock, pResult, &win, startPos, forwardRows, pSup);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, true);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, pBlock->info.rows,
                   numOfOutput);

  doCloseWindow(pResultRowInfo, pInfo, pResult);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, pInfo->inputOrder);
    if (startPos < 0) {
      break;
    }
    // null data, failed to allocate more memory buffer
    int32_t code = setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                          pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->inputOrder);
    // window start(end) key interpolation
    doWindowBorderInterpolation(pInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pSup);
    // TODO: add to open window? how to close the open windows after input blocks exhausted?
#if 0
    if ((ascScan && ekey <= pBlock->info.window.ekey) ||
        (!ascScan && ekey >= pBlock->info.window.skey)) {
      // window start(end) key interpolation
      doWindowBorderInterpolation(pInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pSup);
    } else if (pInfo->timeWindowInterpo) {
      addToOpenWindowList(pResultRowInfo, pResult, tableGroupId);
    }
#endif
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, pBlock->info.rows,
                     numOfOutput);
    doCloseWindow(pResultRowInfo, pInfo, pResult);
  }

  if (pInfo->timeWindowInterpo) {
    saveDataBlockLastRow(pInfo->pPrevValues, pBlock, pInfo->pInterpCols);
  }
}

void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult) {
  // current result is done in computing final results.
  if (pInfo->timeWindowInterpo && isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
    closeResultRow(pResult);
    tdListPopHead(pResultRowInfo->openWindow);
  }
}

SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult, uint64_t groupId) {
  SOpenWindowInfo openWin = {0};
  openWin.pos.pageId = pResult->pageId;
  openWin.pos.offset = pResult->offset;
  openWin.groupId = groupId;
  SListNode* pn = tdListGetTail(pResultRowInfo->openWindow);
  if (pn == NULL) {
    tdListAppend(pResultRowInfo->openWindow, &openWin);
    return openWin.pos;
  }

  SOpenWindowInfo* px = (SOpenWindowInfo*)pn->data;
  if (px->pos.pageId != openWin.pos.pageId || px->pos.offset != openWin.pos.offset || px->groupId != openWin.groupId) {
    tdListAppend(pResultRowInfo->openWindow, &openWin);
  }

  return openWin.pos;
}

int64_t* extractTsCol(SSDataBlock* pBlock, const SIntervalAggOperatorInfo* pInfo) {
  TSKEY* tsCols = NULL;

  if (pBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;

    // no data in primary ts
    if (tsCols[0] == 0 && tsCols[pBlock->info.rows - 1] == 0) {
      return NULL;
    }

    if (tsCols[0] != 0 && (pBlock->info.window.skey == 0 && pBlock->info.window.ekey == 0)) {
      blockDataUpdateTsWindow(pBlock, pInfo->primaryTsIndex);
    }
  }

  return tsCols;
}

static int32_t doOpenIntervalAgg(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExprSupp*                pSup = &pOperator->exprSupp;

  int32_t scanFlag = MAIN_SCAN;

  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    getTableScanInfo(pOperator, &pInfo->inputOrder, &scanFlag);

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, pInfo->inputOrder, scanFlag, true);
    blockDataUpdateTsWindow(pBlock, pInfo->primaryTsIndex);

    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, scanFlag);
  }

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, pInfo->resultTsOrder);
  OPTR_SET_OPENED(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return TSDB_CODE_SUCCESS;
}

static bool compareVal(const char* v, const SStateKeys* pKey) {
  if (IS_VAR_DATA_TYPE(pKey->type)) {
    if (varDataLen(v) != varDataLen(pKey->pData)) {
      return false;
    } else {
      return strncmp(varDataVal(v), varDataVal(pKey->pData), varDataLen(v)) == 0;
    }
  } else {
    return memcmp(pKey->pData, v, pKey->bytes) == 0;
  }
}

static void doStateWindowAggImpl(SOperatorInfo* pOperator, SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SColumnInfoData* pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->stateCol.slotId);
  int64_t          gid = pBlock->info.groupId;

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;
  int16_t bytes = pStateColInfoData->info.bytes;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;

  struct SColumnDataAgg* pAgg = NULL;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    pAgg = (pBlock->pBlockAgg != NULL) ? pBlock->pBlockAgg[pInfo->stateCol.slotId] : NULL;
    if (colDataIsNull(pStateColInfoData, pBlock->info.rows, j, pAgg)) {
      continue;
    }

    char* val = colDataGetData(pStateColInfoData, j);

    if (gid != pRowSup->groupId || !pInfo->hasKey) {
      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->stateKey.type)) {
        varDataCopy(pInfo->stateKey.pData, val);
      } else {
        memcpy(pInfo->stateKey.pData, val, bytes);
      }

      pInfo->hasKey = true;

      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);
    } else if (compareVal(val, &pInfo->stateKey)) {
      doKeepTuple(pRowSup, tsList[j], gid);
      if (j == 0 && pRowSup->startRowIndex != 0) {
        pRowSup->startRowIndex = 0;
      }
    } else {  // a new state window started
      SResultRow* pResult = NULL;

      // keep the time window for the closed time window.
      STimeWindow window = pRowSup->win;

      pRowSup->win.ekey = pRowSup->win.skey;
      int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pSup->pCtx,
                                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                       pRowSup->numOfRows, pBlock->info.rows, numOfOutput);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);

      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->stateKey.type)) {
        varDataCopy(pInfo->stateKey.pData, val);
      } else {
        memcpy(pInfo->stateKey.pData, val, bytes);
      }
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows,
                   pBlock->info.rows, numOfOutput);
}

static SSDataBlock* doStateWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SStateWindowOperatorInfo* pInfo = pOperator->info;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SOptrBasicInfo* pBInfo = &pInfo->binfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    while (1) {
      doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
      doFilter(pInfo->pCondition, pBInfo->pRes, NULL);

      bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
      if (!hasRemain) {
        doSetOperatorCompleted(pOperator);
        break;
      }

      if (pBInfo->pRes->info.rows > 0) {
        break;
      }
    }
    pOperator->resultInfo.totalRows += pBInfo->pRes->info.rows;
    return (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  }

  int32_t order = TSDB_ORDER_ASC;
  int64_t st = taosGetTimestampUs();

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    doStateWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pInfo->pCondition, pBInfo->pRes, NULL);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    if (pBInfo->pRes->info.rows > 0) {
      break;
    }
  }
  pOperator->resultInfo.totalRows += pBInfo->pRes->info.rows;
  return (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
}

static SSDataBlock* doBuildIntervalResult(SOperatorInfo* pOperator) {
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSDataBlock* pBlock = pInfo->binfo.pRes;

  if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    return pOperator->fpSet.getStreamResFn(pOperator);
  } else {
    pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
    if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
      return NULL;
    }

    blockDataEnsureCapacity(pBlock, pOperator->resultInfo.capacity);
    while (1) {
      doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
      doFilter(pInfo->pCondition, pBlock, NULL);

      bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
      if (!hasRemain) {
        doSetOperatorCompleted(pOperator);
        break;
      }

      if (pBlock->info.rows > 0) {
        break;
      }
    }

    size_t rows = pBlock->info.rows;
    pOperator->resultInfo.totalRows += rows;

    return (rows == 0) ? NULL : pBlock;
  }
}

// todo merged with the build group result.
static void finalizeUpdatedResult(int32_t numOfOutput, SDiskbasedBuf* pBuf, SArray* pUpdateList,
                                  int32_t* rowEntryInfoOffset) {
  size_t num = taosArrayGetSize(pUpdateList);

  for (int32_t i = 0; i < num; ++i) {
    SResKeyPos* pPos = taosArrayGetP(pUpdateList, i);

    SFilePage*  bufPage = getBufPage(pBuf, pPos->pos.pageId);
    SResultRow* pRow = (SResultRow*)((char*)bufPage + pPos->pos.offset);

    for (int32_t j = 0; j < numOfOutput; ++j) {
      SResultRowEntryInfo* pEntry = getResultEntryInfo(pRow, j, rowEntryInfoOffset);
      if (pRow->numOfRows < pEntry->numOfRes) {
        pRow->numOfRows = pEntry->numOfRes;
      }
    }

    releaseBufPage(pBuf, bufPage);
  }
}
static void setInverFunction(SqlFunctionCtx* pCtx, int32_t num, EStreamType type) {
  for (int i = 0; i < num; i++) {
    if (type == STREAM_INVERT) {
      fmSetInvertFunc(pCtx[i].functionId, &(pCtx[i].fpSet));
    } else if (type == STREAM_NORMAL) {
      fmSetNormalFunc(pCtx[i].functionId, &(pCtx[i].fpSet));
    }
  }
}

static void doClearWindowImpl(SResultRowPosition* p1, SDiskbasedBuf* pResultBuf, SExprSupp* pSup, int32_t numOfOutput) {
  SResultRow*     pResult = getResultRowByPos(pResultBuf, p1, false);
  SqlFunctionCtx* pCtx = pSup->pCtx;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultEntryInfo(pResult, i, pSup->rowEntryInfoOffset);
    struct SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (fmIsWindowPseudoColumnFunc(pCtx[i].functionId)) {
      continue;
    }
    pResInfo->initialized = false;
    if (pCtx[i].functionId != -1) {
      pCtx[i].fpSet.init(&pCtx[i], pResInfo);
    }
  }
  SFilePage* bufPage = getBufPage(pResultBuf, p1->pageId);
  setBufPageDirty(bufPage, true);
  releaseBufPage(pResultBuf, bufPage);
}

static bool doClearWindow(SAggSupporter* pAggSup, SExprSupp* pSup, char* pData, int16_t bytes, uint64_t groupId,
                          int32_t numOfOutput) {
  SET_RES_WINDOW_KEY(pAggSup->keyBuf, pData, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)tSimpleHashGet(pAggSup->pResultRowHashTable, pAggSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  if (!p1) {
    // window has been closed
    return false;
  }
  doClearWindowImpl(p1, pAggSup->pResultBuf, pSup, numOfOutput);
  return true;
}

static bool doDeleteWindow(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId, int32_t numOfOutput) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SWinKey                      key = {.ts = ts, .groupId = groupId};
  tSimpleHashRemove(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey));
  streamStateDel(pOperator->pTaskInfo->streamInfo.pState, &key);
  return true;
}

static void doDeleteWindows(SOperatorInfo* pOperator, SInterval* pInterval, int32_t numOfOutput, SSDataBlock* pBlock,
                            SArray* pUpWins, SHashObj* pUpdatedMap) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        pGpDatas = (uint64_t*)pGpCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTsCols[i], pInterval, TSDB_ORDER_ASC);
    while (win.ekey <= endTsCols[i]) {
      uint64_t winGpId = pGpDatas[i];
      bool     res = doDeleteWindow(pOperator, win.skey, winGpId, numOfOutput);
      SWinKey  winRes = {.ts = win.skey, .groupId = winGpId};
      if (pUpWins && res) {
        taosArrayPush(pUpWins, &winRes);
      }
      if (pUpdatedMap) {
        taosHashRemove(pUpdatedMap, &winRes, sizeof(SWinKey));
      }
      getNextTimeWindow(pInterval, pInterval->precision, TSDB_ORDER_ASC, &win);
    }
  }
}

bool doDeleteIntervalWindow(SAggSupporter* pAggSup, TSKEY ts, uint64_t groupId) {
  size_t bytes = sizeof(TSKEY);
  SET_RES_WINDOW_KEY(pAggSup->keyBuf, &ts, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)tSimpleHashGet(pAggSup->pResultRowHashTable, pAggSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  if (!p1) {
    // window has been closed
    return false;
  }
  tSimpleHashRemove(pAggSup->pResultRowHashTable, pAggSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  return true;
}

static void doDeleteSpecifyIntervalWindow(SAggSupporter* pAggSup, STimeWindowAggSupp* pTwSup, SSDataBlock* pBlock,
                                          SArray* pDelWins, SInterval* pInterval, SHashObj* pUpdatedMap) {
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsStarts = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           tsEnds = (TSKEY*)pEndCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIds = (uint64_t*)pGroupCol->pData;
  int64_t          numOfWin = tSimpleHashGetSize(pAggSup->pResultRowHashTable);
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    TSKEY          startTs = TMAX(tsStarts[i], pTwSup->minTs);
    TSKEY          endTs = TMIN(tsEnds[i], pTwSup->maxTs);
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTs, pInterval, TSDB_ORDER_ASC);
    do {
      doDeleteIntervalWindow(pAggSup, win.skey, groupIds[i]);
      SWinKey winRes = {.ts = win.skey, .groupId = groupIds[i]};
      if (pDelWins) {
        taosArrayPush(pDelWins, &winRes);
      }
      if (pUpdatedMap) {
        taosHashRemove(pUpdatedMap, &winRes, sizeof(SWinKey));
      }
      getNextTimeWindow(pInterval, pInterval->precision, TSDB_ORDER_ASC, &win);
    } while (win.skey <= endTs);
  }
}

static void doClearWindows(SAggSupporter* pAggSup, SExprSupp* pSup1, SInterval* pInterval, int32_t numOfOutput,
                           SSDataBlock* pBlock, SArray* pUpWins) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        pGpDatas = (uint64_t*)pGpCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTsCols[i], pInterval, TSDB_ORDER_ASC);
    while (win.ekey <= endTsCols[i]) {
      uint64_t winGpId = pGpDatas[i];
      bool     res = doClearWindow(pAggSup, pSup1, (char*)&win.skey, sizeof(TSKEY), winGpId, numOfOutput);
      if (pUpWins && res) {
        SWinKey winRes = {.ts = win.skey, .groupId = winGpId};
        taosArrayPush(pUpWins, &winRes);
      }
      getNextTimeWindow(pInterval, pInterval->precision, TSDB_ORDER_ASC, &win);
    }
  }
}

static int32_t getAllIntervalWindow(SSHashObj* pHashMap, SHashObj* resWins) {
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*    key = tSimpleHashGetKey(pIte, &keyLen);
    uint64_t groupId = *(uint64_t*)key;
    ASSERT(keyLen == GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY)));
    TSKEY               ts = *(int64_t*)((char*)key + sizeof(uint64_t));
    SResultRowPosition* pPos = (SResultRowPosition*)pIte;
    int32_t             code = saveWinResult(ts, pPos->pageId, pPos->offset, groupId, resWins);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t closeIntervalWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pSup, SInterval* pInterval,
                                   SHashObj* pPullDataMap, SHashObj* closeWins, SArray* pRecyPages,
                                   SDiskbasedBuf* pDiscBuf) {
  qDebug("===stream===close interval window");
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*    key = tSimpleHashGetKey(pIte, &keyLen);
    uint64_t groupId = *(uint64_t*)key;
    ASSERT(keyLen == GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY)));
    TSKEY       ts = *(int64_t*)((char*)key + sizeof(uint64_t));
    STimeWindow win;
    win.skey = ts;
    win.ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
    SWinKey winRe = {
        .ts = win.skey,
        .groupId = groupId,
    };
    void* chIds = taosHashGet(pPullDataMap, &winRe, sizeof(SWinKey));
    if (isCloseWindow(&win, pSup)) {
      if (chIds && pPullDataMap) {
        SArray* chAy = *(SArray**)chIds;
        int32_t size = taosArrayGetSize(chAy);
        qDebug("===stream===window %" PRId64 " wait child size:%d", win.skey, size);
        for (int32_t i = 0; i < size; i++) {
          qDebug("===stream===window %" PRId64 " wait child id:%d", win.skey, *(int32_t*)taosArrayGet(chAy, i));
        }
        continue;
      } else if (pPullDataMap) {
        qDebug("===stream===close window %" PRId64, win.skey);
      }
      SResultRowPosition* pPos = (SResultRowPosition*)pIte;
      if (pSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
        int32_t code = saveWinResult(ts, pPos->pageId, pPos->offset, groupId, closeWins);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        ASSERT(pRecyPages != NULL);
        taosArrayPush(pRecyPages, &pPos->pageId);
      } else {
        // SFilePage* bufPage = getBufPage(pDiscBuf, pPos->pageId);
        // dBufSetBufPageRecycled(pDiscBuf, bufPage);
      }
      char keyBuf[GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY))];
      SET_RES_WINDOW_KEY(keyBuf, &ts, sizeof(TSKEY), groupId);
      tSimpleHashIterateRemove(pHashMap, keyBuf, keyLen, &pIte, &iter);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t closeStreamIntervalWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SInterval* pInterval,
                                         SHashObj* pPullDataMap, SHashObj* closeWins, SOperatorInfo* pOperator) {
  qDebug("===stream===close interval window");
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*       key = tSimpleHashGetKey(pIte, &keyLen);
    SWinKey*    pWinKey = (SWinKey*)key;
    void*       chIds = taosHashGet(pPullDataMap, pWinKey, sizeof(SWinKey));
    STimeWindow win = {
        .skey = pWinKey->ts,
        .ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1,
    };
    if (isCloseWindow(&win, pTwSup)) {
      if (chIds && pPullDataMap) {
        SArray* chAy = *(SArray**)chIds;
        int32_t size = taosArrayGetSize(chAy);
        qDebug("===stream===window %" PRId64 " wait child size:%d", pWinKey->ts, size);
        for (int32_t i = 0; i < size; i++) {
          qDebug("===stream===window %" PRId64 " wait child id:%d", pWinKey->ts, *(int32_t*)taosArrayGet(chAy, i));
        }
        continue;
      } else if (pPullDataMap) {
        qDebug("===stream===close window %" PRId64, pWinKey->ts);
      }

      if (pTwSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
        int32_t code = saveWinResultInfo(pWinKey->ts, pWinKey->groupId, closeWins);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      tSimpleHashIterateRemove(pHashMap, pWinKey, sizeof(SWinKey), &pIte, &iter);

      if (needDeleteWindowBuf(&win, pTwSup)) {
        streamStateDel(pOperator->pTaskInfo->streamInfo.pState, pWinKey);
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void closeChildIntervalWindow(SArray* pChildren, TSKEY maxTs) {
  int32_t size = taosArrayGetSize(pChildren);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo*                    pChildOp = taosArrayGetP(pChildren, i);
    SStreamFinalIntervalOperatorInfo* pChInfo = pChildOp->info;
    ASSERT(pChInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE);
    pChInfo->twAggSup.maxTs = TMAX(pChInfo->twAggSup.maxTs, maxTs);
    closeIntervalWindow(pChInfo->aggSup.pResultRowHashTable, &pChInfo->twAggSup, &pChInfo->interval, NULL, NULL, NULL,
                        pChInfo->aggSup.pResultBuf);
  }
}

static void freeAllPages(SArray* pageIds, SDiskbasedBuf* pDiskBuf) {
  int32_t size = taosArrayGetSize(pageIds);
  for (int32_t i = 0; i < size; i++) {
    int32_t pageId = *(int32_t*)taosArrayGet(pageIds, i);
    // SFilePage* bufPage = getBufPage(pDiskBuf, pageId);
    // dBufSetBufPageRecycled(pDiskBuf, bufPage);
  }
  taosArrayClear(pageIds);
}

static void doBuildDeleteResult(SArray* pWins, int32_t* index, SSDataBlock* pBlock) {
  blockDataCleanup(pBlock);
  int32_t size = taosArrayGetSize(pWins);
  if (*index == size) {
    *index = 0;
    taosArrayClear(pWins);
    return;
  }
  blockDataEnsureCapacity(pBlock, size - *index);
  uint64_t uid = 0;
  for (int32_t i = *index; i < size; i++) {
    SWinKey* pWin = taosArrayGet(pWins, i);
    appendOneRow(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId);
    (*index)++;
  }
}

static void destroyStateWindowOperatorInfo(void* param) {
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->stateKey.pData);

  taosMemoryFreeClear(param);
}

static void freeItem(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

void destroyIntervalOperatorInfo(void* param) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSupp);

  tdListFree(pInfo->binfo.resultRowInfo.openWindow);

  pInfo->pRecycledPages = taosArrayDestroy(pInfo->pRecycledPages);
  pInfo->pInterpCols = taosArrayDestroy(pInfo->pInterpCols);
  taosArrayDestroyEx(pInfo->pPrevValues, freeItem);

  pInfo->pPrevValues = NULL;
  pInfo->pDelWins = taosArrayDestroy(pInfo->pDelWins);
  pInfo->pDelRes = blockDataDestroy(pInfo->pDelRes);

  cleanupGroupResInfo(&pInfo->groupResInfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  taosMemoryFreeClear(param);
}

void destroyStreamFinalIntervalOperatorInfo(void* param) {
  SStreamFinalIntervalOperatorInfo* pInfo = (SStreamFinalIntervalOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  // it should be empty.
  taosHashCleanup(pInfo->pPullDataMap);
  taosArrayDestroy(pInfo->pPullWins);
  blockDataDestroy(pInfo->pPullDataRes);
  taosArrayDestroy(pInfo->pRecycledPages);
  blockDataDestroy(pInfo->pUpdateRes);
  taosArrayDestroy(pInfo->pDelWins);
  blockDataDestroy(pInfo->pDelRes);

  if (pInfo->pChildren) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, i);
      destroyStreamFinalIntervalOperatorInfo(pChildOp->info);
      taosMemoryFree(pChildOp->pDownstream);
      cleanupExprSupp(&pChildOp->exprSupp);
      taosMemoryFreeClear(pChildOp);
    }
    taosArrayDestroy(pInfo->pChildren);
  }
  nodesDestroyNode((SNode*)pInfo->pPhyNode);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosMemoryFreeClear(param);
}

static bool allInvertible(SqlFunctionCtx* pFCtx, int32_t numOfCols) {
  for (int32_t i = 0; i < numOfCols; i++) {
    if (fmIsUserDefinedFunc(pFCtx[i].functionId) || !fmIsInvertible(pFCtx[i].functionId)) {
      return false;
    }
  }
  return true;
}

static bool timeWindowinterpNeeded(SqlFunctionCtx* pCtx, int32_t numOfCols, SIntervalAggOperatorInfo* pInfo) {
  // the primary timestamp column
  bool needed = false;
  pInfo->pInterpCols = taosArrayInit(4, sizeof(SColumn));
  pInfo->pPrevValues = taosArrayInit(4, sizeof(SGroupKeys));

  {  // ts column
    SColumn c = {0};
    c.colId = 1;
    c.slotId = pInfo->primaryTsIndex;
    c.type = TSDB_DATA_TYPE_TIMESTAMP;
    c.bytes = sizeof(int64_t);
    taosArrayPush(pInfo->pInterpCols, &c);

    SGroupKeys key = {0};
    key.bytes = c.bytes;
    key.type = c.type;
    key.isNull = true;  // to denote no value is assigned yet
    key.pData = taosMemoryCalloc(1, c.bytes);
    taosArrayPush(pInfo->pPrevValues, &key);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = pCtx[i].pExpr;

    if (fmIsIntervalInterpoFunc(pCtx[i].functionId)) {
      SFunctParam* pParam = &pExpr->base.pParam[0];

      SColumn c = *pParam->pCol;
      taosArrayPush(pInfo->pInterpCols, &c);
      needed = true;

      SGroupKeys key = {0};
      key.bytes = c.bytes;
      key.type = c.type;
      key.isNull = false;
      key.pData = taosMemoryCalloc(1, c.bytes);
      taosArrayPush(pInfo->pPrevValues, &key);
    }
  }

  return needed;
}

void initIntervalDownStream(SOperatorInfo* downstream, uint16_t type, SAggSupporter* pSup, SInterval* pInterval,
                            STimeWindowAggSupp* pTwSup) {
  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    initIntervalDownStream(downstream->pDownstream[0], type, pSup, pInterval, pTwSup);
    return;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup.parentType = type;
  pScanInfo->windowSup.pIntervalAggSup = pSup;
  pScanInfo->pUpdateInfo = updateInfoInitP(pInterval, pTwSup->waterMark);
  pScanInfo->interval = *pInterval;
  pScanInfo->twAggSup = *pTwSup;
}

void initStreamFunciton(SqlFunctionCtx* pCtx, int32_t numOfExpr) {
  for (int32_t i = 0; i < numOfExpr; i++) {
    pCtx[i].isStream = true;
  }
}

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          STimeWindowAggSupp* pTwAggSupp, SIntervalPhysiNode* pPhyNode,
                                          SExecTaskInfo* pTaskInfo, bool isStream) {
  SIntervalAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  pInfo->win = pTaskInfo->window;
  pInfo->inputOrder = (pPhyNode->window.inputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  pInfo->resultTsOrder = (pPhyNode->window.outputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  pInfo->interval = *pInterval;
  pInfo->execModel = pTaskInfo->execModel;
  pInfo->twAggSup = *pTwAggSupp;
  pInfo->ignoreExpiredData = pPhyNode->window.igExpired;
  pInfo->pCondition = pPhyNode->window.node.pConditions;
  pInfo->binfo.mergeResultBlock = pPhyNode->window.mergeDataBlock;

  if (pPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pPhyNode->window.pExprs, NULL, &numOfScalar);
    int32_t    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->primaryTsIndex = primaryTsSlotId;
  SExprSupp* pSup = &pOperator->exprSupp;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t code = initAggInfo(pSup, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);

  if (isStream) {
    ASSERT(numOfCols > 0);
    initStreamFunciton(pSup->pCtx, pSup->numOfExprs);
  }

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);

  pInfo->invertible = allInvertible(pSup->pCtx, numOfCols);
  pInfo->invertible = false;  // Todo(liuyao): Dependent TSDB API

  pInfo->timeWindowInterpo = timeWindowinterpNeeded(pSup->pCtx, numOfCols, pInfo);
  if (pInfo->timeWindowInterpo) {
    pInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
    if (pInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }

  pInfo->pRecycledPages = taosArrayInit(4, sizeof(int32_t));
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->delIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pOperator->name = "TimeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(doOpenIntervalAgg, doBuildIntervalResult, NULL, NULL,
                                         destroyIntervalOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyIntervalOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

// todo handle multiple timeline cases. assume no timeline interweaving
static void doSessionWindowAggImpl(SOperatorInfo* pOperator, SSessionAggOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;
  int64_t gid = pBlock->info.groupId;

  int64_t gap = pInfo->gap;

  if (!pInfo->reptScan) {
    pInfo->reptScan = true;
    pInfo->winSup.prevTs = INT64_MIN;
  }

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;

  // In case of ascending or descending order scan data, only one time window needs to be kepted for each table.
  TSKEY* tsList = (TSKEY*)pColInfoData->pData;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    if (gid != pRowSup->groupId || pInfo->winSup.prevTs == INT64_MIN) {
      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);
    } else if (((tsList[j] - pRowSup->prevTs >= 0) && (tsList[j] - pRowSup->prevTs <= gap)) ||
               ((pRowSup->prevTs - tsList[j] >= 0) && (pRowSup->prevTs - tsList[j] <= gap))) {
      // The gap is less than the threshold, so it belongs to current session window that has been opened already.
      doKeepTuple(pRowSup, tsList[j], gid);
      if (j == 0 && pRowSup->startRowIndex != 0) {
        pRowSup->startRowIndex = 0;
      }
    } else {  // start a new session window
      SResultRow* pResult = NULL;

      // keep the time window for the closed time window.
      STimeWindow window = pRowSup->win;

      pRowSup->win.ekey = pRowSup->win.skey;
      int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pSup->pCtx,
                                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      // pInfo->numOfRows data belong to the current session window
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                       pRowSup->numOfRows, pBlock->info.rows, numOfOutput);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j, gid);
      doKeepTuple(pRowSup, tsList[j], gid);
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows,
                   pBlock->info.rows, numOfOutput);
}

static SSDataBlock* doSessionWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*          pBInfo = &pInfo->binfo;
  SExprSupp*               pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_RES_TO_RETURN) {
    while (1) {
      doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
      doFilter(pInfo->pCondition, pBInfo->pRes, NULL);

      bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
      if (!hasRemain) {
        doSetOperatorCompleted(pOperator);
        break;
      }

      if (pBInfo->pRes->info.rows > 0) {
        break;
      }
    }
    pOperator->resultInfo.totalRows += pBInfo->pRes->info.rows;
    return (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
  }

  int64_t st = taosGetTimestampUs();
  int32_t order = TSDB_ORDER_ASC;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    doSessionWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pInfo->pCondition, pBInfo->pRes, NULL);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    if (pBInfo->pRes->info.rows > 0) {
      break;
    }
  }
  pOperator->resultInfo.totalRows += pBInfo->pRes->info.rows;
  return (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
}

static void doKeepPrevRows(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    // null data should not be kept since it can not be used to perform interpolation
    if (!colDataIsNull_s(pColInfoData, i)) {
      SGroupKeys* pkey = taosArrayGet(pSliceInfo->pPrevRow, i);

      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      memcpy(pkey->pData, val, pkey->bytes);
    }
  }

  pSliceInfo->isPrevRowSet = true;
}

static void doKeepNextRows(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    // null data should not be kept since it can not be used to perform interpolation
    if (!colDataIsNull_s(pColInfoData, i)) {
      SGroupKeys* pkey = taosArrayGet(pSliceInfo->pNextRow, i);

      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      memcpy(pkey->pData, val, pkey->bytes);
    }
  }

  pSliceInfo->isNextRowSet = true;
}

static void doKeepLinearInfo(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex,
                             bool isLastRow) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  bool    fillLastPoint = pSliceInfo->fillLastPoint;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
    SFillLinearInfo* pLinearInfo = taosArrayGet(pSliceInfo->pLinearInfo, i);

    // null data should not be kept since it can not be used to perform interpolation
    if (!colDataIsNull_s(pColInfoData, i)) {
      if (isLastRow) {
        pLinearInfo->start.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        memcpy(pLinearInfo->start.val, colDataGetData(pColInfoData, rowIndex), pLinearInfo->bytes);
      } else if (fillLastPoint) {
        pLinearInfo->end.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        memcpy(pLinearInfo->end.val, colDataGetData(pColInfoData, rowIndex), pLinearInfo->bytes);
      } else {
        pLinearInfo->start.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        pLinearInfo->end.key = *(int64_t*)colDataGetData(pTsCol, rowIndex + 1);

        char* val;
        val = colDataGetData(pColInfoData, rowIndex);
        memcpy(pLinearInfo->start.val, val, pLinearInfo->bytes);
        val = colDataGetData(pColInfoData, rowIndex + 1);
        memcpy(pLinearInfo->end.val, val, pLinearInfo->bytes);
      }

      pLinearInfo->hasNull = false;
    } else {
      pLinearInfo->hasNull = true;
    }
  }

  pSliceInfo->fillLastPoint = isLastRow ? true : false;
}

static void genInterpolationResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pResBlock) {
  int32_t rows = pResBlock->info.rows;
  blockDataEnsureCapacity(pResBlock, rows + 1);
  // todo set the correct primary timestamp column

  // output the result
  bool hasInterp = true;
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];
    int32_t    srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
    int32_t    dstSlot = pExprInfo->base.resSchema.slotId;

    // SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, srcSlot);
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    switch (pSliceInfo->fillType) {
      case TSDB_FILL_NULL: {
        colDataAppendNULL(pDst, rows);
        break;
      }

      case TSDB_FILL_SET_VALUE: {
        SVariant* pVar = &pSliceInfo->pFillColInfo[j].fillVal;

        if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
          float v = 0;
          GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char*)&v, false);
        } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
          double v = 0;
          GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char*)&v, false);
        } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
          int64_t v = 0;
          GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char*)&v, false);
        }
        break;
      }

      case TSDB_FILL_LINEAR: {
        SFillLinearInfo* pLinearInfo = taosArrayGet(pSliceInfo->pLinearInfo, srcSlot);

        SPoint start = pLinearInfo->start;
        SPoint end = pLinearInfo->end;
        SPoint current = {.key = pSliceInfo->current};
        current.val = taosMemoryCalloc(pLinearInfo->bytes, 1);

        // before interp range, do not fill
        if (start.key == INT64_MIN || end.key == INT64_MAX) {
          hasInterp = false;
          break;
        }

        if (pLinearInfo->hasNull) {
          colDataAppendNULL(pDst, rows);
        } else {
          taosGetLinearInterpolationVal(&current, pLinearInfo->type, &start, &end, pLinearInfo->type);
          colDataAppend(pDst, rows, (char*)current.val, false);
        }

        taosMemoryFree(current.val);
        break;
      }
      case TSDB_FILL_PREV: {
        if (!pSliceInfo->isPrevRowSet) {
          hasInterp = false;
          break;
        }

        SGroupKeys* pkey = taosArrayGet(pSliceInfo->pPrevRow, srcSlot);
        colDataAppend(pDst, rows, pkey->pData, false);
        break;
      }

      case TSDB_FILL_NEXT: {
        if (!pSliceInfo->isNextRowSet) {
          hasInterp = false;
          break;
        }

        SGroupKeys* pkey = taosArrayGet(pSliceInfo->pNextRow, srcSlot);
        colDataAppend(pDst, rows, pkey->pData, false);
        break;
      }

      case TSDB_FILL_NONE:
      default:
        break;
    }
  }

  if (hasInterp) {
    pResBlock->info.rows += 1;
  }
}

static int32_t initPrevRowsKeeper(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  if (pInfo->pPrevRow != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pPrevRow = taosArrayInit(4, sizeof(SGroupKeys));
  if (pInfo->pPrevRow == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys key = {0};
    key.bytes = pColInfo->info.bytes;
    key.type = pColInfo->info.type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pColInfo->info.bytes);
    taosArrayPush(pInfo->pPrevRow, &key);
  }

  pInfo->isPrevRowSet = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t initNextRowsKeeper(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  if (pInfo->pNextRow != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pNextRow = taosArrayInit(4, sizeof(SGroupKeys));
  if (pInfo->pNextRow == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys key = {0};
    key.bytes = pColInfo->info.bytes;
    key.type = pColInfo->info.type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pColInfo->info.bytes);
    taosArrayPush(pInfo->pNextRow, &key);
  }

  pInfo->isNextRowSet = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t initFillLinearInfo(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  if (pInfo->pLinearInfo != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pLinearInfo = taosArrayInit(4, sizeof(SFillLinearInfo));
  if (pInfo->pLinearInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SFillLinearInfo linearInfo = {0};
    linearInfo.start.key = INT64_MIN;
    linearInfo.end.key = INT64_MAX;
    linearInfo.start.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    linearInfo.end.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    linearInfo.hasNull = false;
    linearInfo.type = pColInfo->info.type;
    linearInfo.bytes = pColInfo->info.bytes;
    taosArrayPush(pInfo->pLinearInfo, &linearInfo);
  }

  pInfo->fillLastPoint = false;

  return TSDB_CODE_SUCCESS;
}

static bool needToFillLastPoint(STimeSliceOperatorInfo* pSliceInfo) {
  return (pSliceInfo->fillLastPoint == true && pSliceInfo->fillType == TSDB_FILL_LINEAR);
}

static int32_t initKeeperInfo(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t code;
  code = initPrevRowsKeeper(pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initNextRowsKeeper(pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initFillLinearInfo(pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doTimeslice(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SSDataBlock*            pResBlock = pSliceInfo->pRes;
  SExprSupp*              pSup = &pOperator->exprSupp;

  //  if (pOperator->status == OP_RES_TO_RETURN) {
  //    //    doBuildResultDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);
  //    if (pResBlock->info.rows == 0 || !hasRemainResults(&pSliceInfo->groupResInfo)) {
  //      doSetOperatorCompleted(pOperator);
  //    }
  //
  //    return pResBlock;
  //  }

  int32_t        order = TSDB_ORDER_ASC;
  SInterval*     pInterval = &pSliceInfo->interval;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  blockDataCleanup(pResBlock);

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    int32_t code = initKeeperInfo(pSliceInfo, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, MAIN_SCAN, true);

    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);

      if (i == 0 && needToFillLastPoint(pSliceInfo)) {  // first row in current block
        doKeepLinearInfo(pSliceInfo, pBlock, i, false);
        while (pSliceInfo->current < ts && pSliceInfo->current <= pSliceInfo->win.ekey) {
          genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (pResBlock->info.rows >= pResBlock->info.capacity) {
            break;
          }
        }
      }

      if (pSliceInfo->current > pSliceInfo->win.ekey) {
        doSetOperatorCompleted(pOperator);
        break;
      }

      if (ts == pSliceInfo->current) {
        for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
          SExprInfo* pExprInfo = &pOperator->exprSupp.pExprInfo[j];
          int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
          int32_t    srcSlot = pExprInfo->base.pParam[0].pCol->slotId;

          SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, srcSlot);
          SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

          if (colDataIsNull_s(pSrc, i)) {
            colDataAppendNULL(pDst, pResBlock->info.rows);
            continue;
          }

          char* v = colDataGetData(pSrc, i);
          colDataAppend(pDst, pResBlock->info.rows, v, false);
        }

        pResBlock->info.rows += 1;
        doKeepPrevRows(pSliceInfo, pBlock, i);

        // for linear interpolation, always fill value between this and next points;
        // if its the first point in data block, also fill values between previous(if there's any) and this point;
        // if its the last point in data block, no need to fill, but reserve this point as the start value and do
        // the interpolation when processing next data block.
        if (pSliceInfo->fillType == TSDB_FILL_LINEAR) {
          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (i < pBlock->info.rows - 1) {
            doKeepLinearInfo(pSliceInfo, pBlock, i, false);
            int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
            if (nextTs > pSliceInfo->current) {
              while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
                pSliceInfo->current = taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit,
                                                  pInterval->precision);
                if (pResBlock->info.rows >= pResBlock->info.capacity) {
                  break;
                }
              }

              if (pSliceInfo->current > pSliceInfo->win.ekey) {
                doSetOperatorCompleted(pOperator);
                break;
              }
            }
          } else {  // it is the last row of current block
            // store ts value as start, and calculate interp value when processing next block
            doKeepLinearInfo(pSliceInfo, pBlock, i, true);
          }
        } else {  // non-linear interpolation
          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (pSliceInfo->current > pSliceInfo->win.ekey) {
            doSetOperatorCompleted(pOperator);
            break;
          }

          if (pResBlock->info.rows >= pResBlock->info.capacity) {
            break;
          }
        }
      } else if (ts < pSliceInfo->current) {
        // in case of interpolation window starts and ends between two datapoints, fill(prev) need to interpolate
        doKeepPrevRows(pSliceInfo, pBlock, i);

        if (pSliceInfo->fillType == TSDB_FILL_LINEAR) {
          // no need to increate pSliceInfo->current here
          // pSliceInfo->current =
          //    taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (i < pBlock->info.rows - 1) {
            doKeepLinearInfo(pSliceInfo, pBlock, i, false);
            int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
            if (nextTs > pSliceInfo->current) {
              while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
                pSliceInfo->current = taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit,
                                                  pInterval->precision);
                if (pResBlock->info.rows >= pResBlock->info.capacity) {
                  break;
                }
              }

              if (pSliceInfo->current > pSliceInfo->win.ekey) {
                doSetOperatorCompleted(pOperator);
                break;
              }
            }
          }
        } else {  // non-linear interpolation
          if (i < pBlock->info.rows - 1) {
            // in case of interpolation window starts and ends between two datapoints, fill(next) need to interpolate
            doKeepNextRows(pSliceInfo, pBlock, i + 1);
            int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
            if (nextTs > pSliceInfo->current) {
              while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
                pSliceInfo->current = taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit,
                                                  pInterval->precision);
                if (pResBlock->info.rows >= pResBlock->info.capacity) {
                  break;
                }
              }

              if (pSliceInfo->current > pSliceInfo->win.ekey) {
                doSetOperatorCompleted(pOperator);
                break;
              }
            } else {
              // ignore current row, and do nothing
            }
          } else {  // it is the last row of current block
            doKeepPrevRows(pSliceInfo, pBlock, i);
          }
        }
      } else {  // ts > pSliceInfo->current
        // in case of interpolation window starts and ends between two datapoints, fill(next) need to interpolate
        doKeepNextRows(pSliceInfo, pBlock, i);

        while (pSliceInfo->current < ts && pSliceInfo->current <= pSliceInfo->win.ekey) {
          genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (pResBlock->info.rows >= pResBlock->info.capacity) {
            break;
          }
        }

        // add current row if timestamp match
        if (ts == pSliceInfo->current && pSliceInfo->current <= pSliceInfo->win.ekey) {
          for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
            SExprInfo* pExprInfo = &pOperator->exprSupp.pExprInfo[j];
            int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
            int32_t    srcSlot = pExprInfo->base.pParam[0].pCol->slotId;

            SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, srcSlot);
            SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

            char* v = colDataGetData(pSrc, i);
            colDataAppend(pDst, pResBlock->info.rows, v, false);
          }

          pResBlock->info.rows += 1;
          doKeepPrevRows(pSliceInfo, pBlock, i);

          if (pSliceInfo->fillType == TSDB_FILL_LINEAR) {
            pSliceInfo->current =
                taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
            if (i < pBlock->info.rows - 1) {
              doKeepLinearInfo(pSliceInfo, pBlock, i, false);
              int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
              if (nextTs > pSliceInfo->current) {
                while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                  genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
                  pSliceInfo->current = taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit,
                                                    pInterval->precision);
                  if (pResBlock->info.rows >= pResBlock->info.capacity) {
                    break;
                  }
                }

                if (pSliceInfo->current > pSliceInfo->win.ekey) {
                  doSetOperatorCompleted(pOperator);
                  break;
                }
              }
            }
          } else {  // non-linear interpolation
            pSliceInfo->current =
                taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);

            if (pResBlock->info.rows >= pResBlock->info.capacity) {
              break;
            }
          }
        }

        if (pSliceInfo->current > pSliceInfo->win.ekey) {
          doSetOperatorCompleted(pOperator);
          break;
        }
      }
    }
  }

  // check if need to interpolate after last datablock
  // except for fill(next), fill(linear)
  while (pSliceInfo->current <= pSliceInfo->win.ekey && pSliceInfo->fillType != TSDB_FILL_NEXT &&
         pSliceInfo->fillType != TSDB_FILL_LINEAR) {
    genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock);
    pSliceInfo->current =
        taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
    if (pResBlock->info.rows >= pResBlock->info.capacity) {
      break;
    }
  }

  // restore the value
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  if (pResBlock->info.rows == 0) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pResBlock->info.rows == 0 ? NULL : pResBlock;
}

void destroyTimeSliceOperatorInfo(void* param) {
  STimeSliceOperatorInfo* pInfo = (STimeSliceOperatorInfo*)param;

  pInfo->pRes = blockDataDestroy(pInfo->pRes);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pPrevRow); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pPrevRow, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pInfo->pPrevRow);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pNextRow); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pNextRow, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pInfo->pNextRow);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SFillLinearInfo* pKey = taosArrayGet(pInfo->pLinearInfo, i);
    taosMemoryFree(pKey->start.val);
    taosMemoryFree(pKey->end.val);
  }
  taosArrayDestroy(pInfo->pLinearInfo);

  taosMemoryFree(pInfo->pFillColInfo);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo) {
  STimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    goto _error;
  }

  SInterpFuncPhysiNode* pInterpPhyNode = (SInterpFuncPhysiNode*)pPhyNode;
  SExprSupp*            pSup = &pOperator->exprSupp;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pInterpPhyNode->pFuncs, NULL, &numOfExprs);
  int32_t    code = initExprSupp(pSup, pExprInfo, numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pInterpPhyNode->pExprs, NULL, &num);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->tsCol = extractColumnFromColumnNode((SColumnNode*)pInterpPhyNode->pTimeSeries);
  pInfo->fillType = convertFillType(pInterpPhyNode->fillMode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pFillColInfo = createFillColInfo(pExprInfo, numOfExprs, NULL, 0, (SNodeListNode*)pInterpPhyNode->pFillValues);
  pInfo->pLinearInfo = NULL;
  pInfo->pRes = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
  pInfo->win = pInterpPhyNode->timeRange;
  pInfo->interval.interval = pInterpPhyNode->interval;
  pInfo->current = pInfo->win.skey;

  pOperator->name = "TimeSliceOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doTimeslice, NULL, NULL, destroyTimeSliceOperatorInfo, NULL, NULL, NULL);

  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SStateWinodwPhysiNode* pStateNode,
                                             SExecTaskInfo* pTaskInfo) {
  SStateWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStateWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t      num = 0;
  SExprInfo*   pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &num);
  SSDataBlock* pResBlock = createResDataBlock(pStateNode->window.node.pOutputDataBlockDesc);
  int32_t      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;

  SColumnNode* pColNode = (SColumnNode*)((STargetNode*)pStateNode->pStateKey)->pExpr;

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  pInfo->stateKey.type = pInfo->stateCol.type;
  pInfo->stateKey.bytes = pInfo->stateCol.bytes;
  pInfo->stateKey.pData = taosMemoryCalloc(1, pInfo->stateCol.bytes);
  pInfo->pCondition = pStateNode->window.node.pConditions;
  if (pInfo->stateKey.pData == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  int32_t code = initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->twAggSup =
      (STimeWindowAggSupp){.waterMark = pStateNode->window.watermark, .calTrigger = pStateNode->window.triggerType};
  ;
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = tsSlotId;
  pOperator->name = "StateWindowOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doStateWindowAgg, NULL, NULL,
                                         destroyStateWindowOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyStateWindowOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroySWindowOperatorInfo(void* param) {
  SSessionAggOperatorInfo* pInfo = (SSessionAggOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  cleanupAggSup(&pInfo->aggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SSessionWinodwPhysiNode* pSessionNode,
                                            SExecTaskInfo* pTaskInfo) {
  SSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSessionAggOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createResDataBlock(pSessionNode->window.node.pOutputDataBlockDesc);

  int32_t code = initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);

  pInfo->twAggSup.waterMark = pSessionNode->window.watermark;
  pInfo->twAggSup.calTrigger = pSessionNode->window.triggerType;
  pInfo->gap = pSessionNode->gap;

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  pInfo->binfo.pRes = pResBlock;
  pInfo->winSup.prevTs = INT64_MIN;
  pInfo->reptScan = false;
  pInfo->pCondition = pSessionNode->window.node.pConditions;

  pOperator->name = "SessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doSessionWindowAgg, NULL, NULL,
                                         destroySWindowOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);
  pOperator->pTaskInfo = pTaskInfo;
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroySWindowOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void compactFunctions(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t numOfOutput,
                      SExecTaskInfo* pTaskInfo, SColumnInfoData* pTimeWindowData) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    if (fmIsWindowPseudoColumnFunc(pDestCtx[k].functionId)) {
      if (!pTimeWindowData) {
        continue;
      }

      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pDestCtx[k]);
      char*                p = GET_ROWCELL_INTERBUF(pEntryInfo);
      SColumnInfoData      idata = {0};
      idata.info.type = TSDB_DATA_TYPE_BIGINT;
      idata.info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      idata.pData = p;

      SScalarParam out = {.columnData = &idata};
      SScalarParam tw = {.numOfRows = 5, .columnData = pTimeWindowData};
      pDestCtx[k].sfp.process(&tw, 1, &out);
      pEntryInfo->numOfRes = 1;
    } else if (functionNeedToExecute(&pDestCtx[k]) && pDestCtx[k].fpSet.combine != NULL) {
      int32_t code = pDestCtx[k].fpSet.combine(&pDestCtx[k], &pSourceCtx[k]);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s apply functions error, code: %s", GET_TASKID(pTaskInfo), tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }
  }
}

bool hasIntervalWindow(SAggSupporter* pSup, TSKEY ts, uint64_t groupId) {
  int32_t bytes = sizeof(TSKEY);
  SET_RES_WINDOW_KEY(pSup->keyBuf, &ts, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)tSimpleHashGet(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  return p1 != NULL;
}

STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {.skey = ts, .ekey = INT64_MAX};
  w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  return w;
}

static void rebuildIntervalWindow(SStreamFinalIntervalOperatorInfo* pInfo, SExprSupp* pSup, SArray* pWinArray,
                                  int32_t groupId, int32_t numOfOutput, SExecTaskInfo* pTaskInfo,
                                  SHashObj* pUpdatedMap) {
  int32_t size = taosArrayGetSize(pWinArray);
  if (!pInfo->pChildren) {
    return;
  }
  for (int32_t i = 0; i < size; i++) {
    SWinKey*    pWinRes = taosArrayGet(pWinArray, i);
    SResultRow* pCurResult = NULL;
    STimeWindow parentWin = getFinalTimeWindow(pWinRes->ts, &pInfo->interval);
    if (isDeletedWindow(&parentWin, pWinRes->groupId, &pInfo->aggSup) && isCloseWindow(&parentWin, &pInfo->twAggSup)) {
      continue;
    }
    setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &parentWin, true, &pCurResult, pWinRes->groupId, pSup->pCtx,
                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    int32_t numOfChildren = taosArrayGetSize(pInfo->pChildren);
    int32_t num = 0;
    for (int32_t j = 0; j < numOfChildren; j++) {
      SOperatorInfo*            pChildOp = taosArrayGetP(pInfo->pChildren, j);
      SIntervalAggOperatorInfo* pChInfo = pChildOp->info;
      SExprSupp*                pChildSup = &pChildOp->exprSupp;
      if (!hasIntervalWindow(&pChInfo->aggSup, pWinRes->ts, pWinRes->groupId)) {
        continue;
      }
      num++;
      SResultRow* pChResult = NULL;
      setTimeWindowOutputBuf(&pChInfo->binfo.resultRowInfo, &parentWin, true, &pChResult, pWinRes->groupId,
                             pChildSup->pCtx, pChildSup->numOfExprs, pChildSup->rowEntryInfoOffset, &pChInfo->aggSup,
                             pTaskInfo);
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &parentWin, true);
      compactFunctions(pSup->pCtx, pChildSup->pCtx, numOfOutput, pTaskInfo, &pInfo->twAggSup.timeWindowData);
    }
    if (num > 0 && pUpdatedMap) {
      saveWinResultRow(pCurResult, pWinRes->groupId, pUpdatedMap);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pInfo->binfo.resultRowInfo.cur);
    }
  }
}

bool isDeletedWindow(STimeWindow* pWin, uint64_t groupId, SAggSupporter* pSup) {
  SET_RES_WINDOW_KEY(pSup->keyBuf, &pWin->skey, sizeof(int64_t), groupId);
  SResultRowPosition* p1 = (SResultRowPosition*)tSimpleHashGet(pSup->pResultRowHashTable, pSup->keyBuf,
                                                               GET_RES_WINDOW_KEY_LEN(sizeof(int64_t)));
  return p1 == NULL;
}

bool isDeletedStreamWindow(STimeWindow* pWin, uint64_t groupId, SOperatorInfo* pOperator, STimeWindowAggSupp* pTwSup) {
  if (pWin->ekey < pTwSup->maxTs - pTwSup->deleteMark) {
    SWinKey key = {.ts = pWin->skey, .groupId = groupId};
    void*   pVal = NULL;
    int32_t size = 0;
    if (streamStateGet(pOperator->pTaskInfo->streamInfo.pState, &key, &pVal, &size) < 0) {
      return false;
    }
    streamStateReleaseBuf(pOperator->pTaskInfo->streamInfo.pState, &key, pVal);
  }
  return false;
}

int32_t getNexWindowPos(SInterval* pInterval, SDataBlockInfo* pBlockInfo, TSKEY* tsCols, int32_t startPos, TSKEY eKey,
                        STimeWindow* pNextWin) {
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(pBlockInfo, tsCols, startPos, eKey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
  int32_t prevEndPos = forwardRows - 1 + startPos;
  return getNextQualifiedWindow(pInterval, pNextWin, pBlockInfo, tsCols, prevEndPos, TSDB_ORDER_ASC);
}

void addPullWindow(SHashObj* pMap, SWinKey* pWinRes, int32_t size) {
  SArray* childIds = taosArrayInit(8, sizeof(int32_t));
  for (int32_t i = 0; i < size; i++) {
    taosArrayPush(childIds, &i);
  }
  taosHashPut(pMap, pWinRes, sizeof(SWinKey), &childIds, sizeof(void*));
}

static int32_t getChildIndex(SSDataBlock* pBlock) { return pBlock->info.childId; }

static void doHashIntervalAgg(SOperatorInfo* pOperatorInfo, SSDataBlock* pSDataBlock, uint64_t tableGroupId,
                              SHashObj* pUpdatedMap) {
  SStreamFinalIntervalOperatorInfo* pInfo = (SStreamFinalIntervalOperatorInfo*)pOperatorInfo->info;
  SResultRowInfo*                   pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*                    pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*                        pSup = &pOperatorInfo->exprSupp;
  int32_t                           numOfOutput = pSup->numOfExprs;
  int32_t                           step = 1;
  TSKEY*                            tsCols = NULL;
  SResultRow*                       pResult = NULL;
  int32_t                           forwardRows = 0;

  ASSERT(pSDataBlock->pDataBlock != NULL);
  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  tsCols = (int64_t*)pColDataInfo->pData;

  int32_t     startPos = 0;
  TSKEY       ts = getStartTsKey(&pSDataBlock->info.window, tsCols);
  STimeWindow nextWin = {0};
  if (IS_FINAL_OP(pInfo)) {
    nextWin = getFinalTimeWindow(ts, &pInfo->interval);
  } else {
    nextWin = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, pInfo->order);
  }
  while (1) {
    bool isClosed = isCloseWindow(&nextWin, &pInfo->twAggSup);
    if ((pInfo->ignoreExpiredData && isClosed) || !inSlidingWindow(&pInfo->interval, &nextWin, &pSDataBlock->info)) {
      startPos = getNexWindowPos(&pInfo->interval, &pSDataBlock->info, tsCols, startPos, nextWin.ekey, &nextWin);
      if (startPos < 0) {
        break;
      }
      continue;
    }
    if (IS_FINAL_OP(pInfo) && isClosed && pInfo->pChildren) {
      bool    ignore = true;
      SWinKey winRes = {
          .ts = nextWin.skey,
          .groupId = tableGroupId,
      };
      void* chIds = taosHashGet(pInfo->pPullDataMap, &winRes, sizeof(SWinKey));
      if (isDeletedWindow(&nextWin, tableGroupId, &pInfo->aggSup) && !chIds) {
        SPullWindowInfo pull = {.window = nextWin, .groupId = tableGroupId};
        // add pull data request
        savePullWindow(&pull, pInfo->pPullWins);
        int32_t size = taosArrayGetSize(pInfo->pChildren);
        addPullWindow(pInfo->pPullDataMap, &winRes, size);
        qDebug("===stream===prepare retrive %" PRId64 ", size:%d", winRes.ts, size);
      } else {
        int32_t index = -1;
        SArray* chArray = NULL;
        int32_t chId = 0;
        if (chIds) {
          chArray = *(void**)chIds;
          chId = getChildIndex(pSDataBlock);
          index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
        }
        if (index == -1 || pSDataBlock->info.type == STREAM_PULL_DATA) {
          ignore = false;
        }
      }

      if (ignore) {
        startPos = getNexWindowPos(&pInfo->interval, &pSDataBlock->info, tsCols, startPos, nextWin.ekey, &nextWin);
        if (startPos < 0) {
          break;
        }
        continue;
      }
    }

    int32_t code = setTimeWindowOutputBuf(pResultRowInfo, &nextWin, true, &pResult, tableGroupId, pSup->pCtx,
                                          numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (IS_FINAL_OP(pInfo)) {
      forwardRows = 1;
    } else {
      forwardRows = getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, nextWin.ekey, binarySearchForKey,
                                             NULL, TSDB_ORDER_ASC);
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pUpdatedMap) {
      saveWinResultRow(pResult, tableGroupId, pUpdatedMap);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pResultRowInfo->cur);
    }
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                     pSDataBlock->info.rows, numOfOutput);
    int32_t prevEndPos = (forwardRows - 1) * step + startPos;
    ASSERT(pSDataBlock->info.window.skey > 0 && pSDataBlock->info.window.ekey > 0);
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, pInfo->order);
    if (startPos < 0) {
      break;
    }
  }
}

static void clearStreamIntervalOperator(SStreamFinalIntervalOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->aggSup.pResultRowHashTable);
  clearDiskbasedBuf(pInfo->aggSup.pResultBuf);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->aggSup.currentPageId = -1;
}

static void clearSpecialDataBlock(SSDataBlock* pBlock) {
  if (pBlock->info.rows <= 0) {
    return;
  }
  blockDataCleanup(pBlock);
}

void copyUpdateDataBlock(SSDataBlock* pDest, SSDataBlock* pSource, int32_t tsColIndex) {
  // ASSERT(pDest->info.capacity >= pSource->info.rows);
  blockDataEnsureCapacity(pDest, pSource->info.rows);
  clearSpecialDataBlock(pDest);
  SColumnInfoData* pDestCol = taosArrayGet(pDest->pDataBlock, 0);
  SColumnInfoData* pSourceCol = taosArrayGet(pSource->pDataBlock, tsColIndex);

  // copy timestamp column
  colDataAssign(pDestCol, pSourceCol, pSource->info.rows, &pDest->info);
  for (int32_t i = 1; i < taosArrayGetSize(pDest->pDataBlock); i++) {
    SColumnInfoData* pCol = taosArrayGet(pDest->pDataBlock, i);
    colDataAppendNNULL(pCol, 0, pSource->info.rows);
  }

  pDest->info.rows = pSource->info.rows;
  pDest->info.groupId = pSource->info.groupId;
  pDest->info.type = pSource->info.type;
  blockDataUpdateTsWindow(pDest, 0);
}

static void doBuildPullDataBlock(SArray* array, int32_t* pIndex, SSDataBlock* pBlock) {
  clearSpecialDataBlock(pBlock);
  int32_t size = taosArrayGetSize(array);
  if (size - (*pIndex) == 0) {
    return;
  }
  blockDataEnsureCapacity(pBlock, size - (*pIndex));
  ASSERT(3 <= taosArrayGetSize(pBlock->pDataBlock));
  SColumnInfoData* pStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGroupId = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  for (; (*pIndex) < size; (*pIndex)++) {
    SPullWindowInfo* pWin = taosArrayGet(array, (*pIndex));
    colDataAppend(pStartTs, pBlock->info.rows, (const char*)&pWin->window.skey, false);
    colDataAppend(pEndTs, pBlock->info.rows, (const char*)&pWin->window.ekey, false);
    colDataAppend(pGroupId, pBlock->info.rows, (const char*)&pWin->groupId, false);
    colDataAppend(pCalStartTs, pBlock->info.rows, (const char*)&pWin->window.skey, false);
    colDataAppend(pCalEndTs, pBlock->info.rows, (const char*)&pWin->window.ekey, false);
    pBlock->info.rows++;
  }
  if ((*pIndex) == size) {
    *pIndex = 0;
    taosArrayClear(array);
  }
  blockDataUpdateTsWindow(pBlock, 0);
}

void processPullOver(SSDataBlock* pBlock, SHashObj* pMap) {
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsData = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIdData = (uint64_t*)pGroupCol->pData;
  int32_t          chId = getChildIndex(pBlock);
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SWinKey winRes = {.ts = tsData[i], .groupId = groupIdData[i]};
    void*   chIds = taosHashGet(pMap, &winRes, sizeof(SWinKey));
    if (chIds) {
      SArray* chArray = *(SArray**)chIds;
      int32_t index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
      if (index != -1) {
        qDebug("===stream===window %" PRId64 " delete child id %d", winRes.ts, chId);
        taosArrayRemove(chArray, index);
        if (taosArrayGetSize(chArray) == 0) {
          // pull data is over
          taosArrayDestroy(chArray);
          taosHashRemove(pMap, &winRes, sizeof(SWinKey));
        }
      }
    }
  }
}

static void addRetriveWindow(SArray* wins, SStreamFinalIntervalOperatorInfo* pInfo) {
  int32_t size = taosArrayGetSize(wins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey*    winKey = taosArrayGet(wins, i);
    STimeWindow nextWin = getFinalTimeWindow(winKey->ts, &pInfo->interval);
    if (isCloseWindow(&nextWin, &pInfo->twAggSup) && !pInfo->ignoreExpiredData) {
      void* chIds = taosHashGet(pInfo->pPullDataMap, winKey, sizeof(SWinKey));
      if (!chIds) {
        SPullWindowInfo pull = {.window = nextWin, .groupId = winKey->groupId};
        // add pull data request
        savePullWindow(&pull, pInfo->pPullWins);
        int32_t size1 = taosArrayGetSize(pInfo->pChildren);
        addPullWindow(pInfo->pPullDataMap, winKey, size1);
        qDebug("===stream===prepare retrive for delete %" PRId64 ", size:%d", winKey->ts, size1);
      }
    }
  }
}

static void clearFunctionContext(SExprSupp* pSup) {
  for (int32_t i = 0; i < pSup->numOfExprs; i++) {
    pSup->pCtx[i].saveHandle.currentPage = -1;
  }
}

static SSDataBlock* doStreamFinalIntervalAgg(SOperatorInfo* pOperator) {
  SStreamFinalIntervalOperatorInfo* pInfo = pOperator->info;

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  TSKEY          maxTs = INT64_MIN;
  TSKEY          minTs = INT64_MAX;

  SExprSupp* pSup = &pOperator->exprSupp;

  qDebug("interval status %d %s", pOperator->status, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildPullDataBlock(pInfo->pPullWins, &pInfo->pullIndex, pInfo->pPullDataRes);
    if (pInfo->pPullDataRes->info.rows != 0) {
      // process the rest of the data
      ASSERT(IS_FINAL_OP(pInfo));
      printDataBlock(pInfo->pPullDataRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      return pInfo->pPullDataRes;
    }

    doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
    if (pInfo->pDelRes->info.rows != 0) {
      // process the rest of the data
      printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      return pInfo->pDelRes;
    }

    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pInfo->binfo.pRes->info.rows == 0) {
      pOperator->status = OP_EXEC_DONE;
      if (!IS_FINAL_OP(pInfo)) {
        clearFunctionContext(&pOperator->exprSupp);
        // semi interval operator clear disk buffer
        clearStreamIntervalOperator(pInfo);
        qDebug("===stream===clear semi operator");
      } else {
        freeAllPages(pInfo->pRecycledPages, pInfo->aggSup.pResultBuf);
      }
      return NULL;
    }
    printDataBlock(pInfo->binfo.pRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->binfo.pRes;
  } else {
    if (!IS_FINAL_OP(pInfo)) {
      doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
      if (pInfo->binfo.pRes->info.rows != 0) {
        printDataBlock(pInfo->binfo.pRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
        return pInfo->binfo.pRes;
      }
    }
    if (pInfo->pUpdateRes->info.rows != 0 && pInfo->returnUpdate) {
      pInfo->returnUpdate = false;
      ASSERT(!IS_FINAL_OP(pInfo));
      printDataBlock(pInfo->pUpdateRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      // process the rest of the data
      return pInfo->pUpdateRes;
    }
    doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
    if (pInfo->pDelRes->info.rows != 0) {
      // process the rest of the data
      printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      return pInfo->pDelRes;
    }
  }

  SArray*    pUpdated = taosArrayInit(4, POINTER_BYTES);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*  pUpdatedMap = taosHashInit(1024, hashFn, false, HASH_NO_LOCK);
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      clearSpecialDataBlock(pInfo->pUpdateRes);
      removeDeleteResults(pUpdatedMap, pInfo->pDelWins);
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("%s return data", IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      break;
    }
    printDataBlock(pBlock, IS_FINAL_OP(pInfo) ? "interval final recv" : "interval semi recv");

    ASSERT(pBlock->info.type != STREAM_INVERT);
    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_CLEAR) {
      SArray* pUpWins = taosArrayInit(8, sizeof(SWinKey));
      doClearWindows(&pInfo->aggSup, pSup, &pInfo->interval, pOperator->exprSupp.numOfExprs, pBlock, pUpWins);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                           childIndex = getChildIndex(pBlock);
        SOperatorInfo*                    pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamFinalIntervalOperatorInfo* pChildInfo = pChildOp->info;
        SExprSupp*                        pChildSup = &pChildOp->exprSupp;

        doClearWindows(&pChildInfo->aggSup, pChildSup, &pChildInfo->interval, pChildSup->numOfExprs, pBlock, NULL);
        rebuildIntervalWindow(pInfo, pSup, pUpWins, pInfo->binfo.pRes->info.groupId, pOperator->exprSupp.numOfExprs,
                              pOperator->pTaskInfo, NULL);
        taosArrayDestroy(pUpWins);
        continue;
      }
      removeResults(pUpWins, pUpdatedMap);
      copyDataBlock(pInfo->pUpdateRes, pBlock);
      pInfo->returnUpdate = true;
      taosArrayDestroy(pUpWins);
      break;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      doDeleteSpecifyIntervalWindow(&pInfo->aggSup, &pInfo->twAggSup, pBlock, delWins, &pInfo->interval, pUpdatedMap);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                           childIndex = getChildIndex(pBlock);
        SOperatorInfo*                    pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamFinalIntervalOperatorInfo* pChildInfo = pChildOp->info;
        SExprSupp*                        pChildSup = &pChildOp->exprSupp;
        doDeleteSpecifyIntervalWindow(&pChildInfo->aggSup, &pInfo->twAggSup, pBlock, NULL, &pChildInfo->interval, NULL);
        rebuildIntervalWindow(pInfo, pSup, delWins, pInfo->binfo.pRes->info.groupId, pOperator->exprSupp.numOfExprs,
                              pOperator->pTaskInfo, pUpdatedMap);
        addRetriveWindow(delWins, pInfo);
        taosArrayAddAll(pInfo->pDelWins, delWins);
        taosArrayDestroy(delWins);
        continue;
      }
      removeResults(delWins, pUpdatedMap);
      taosArrayAddAll(pInfo->pDelWins, delWins);
      taosArrayDestroy(delWins);
      break;
    } else if (pBlock->info.type == STREAM_GET_ALL && IS_FINAL_OP(pInfo)) {
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_RETRIEVE && !IS_FINAL_OP(pInfo)) {
      SArray* pUpWins = taosArrayInit(8, sizeof(SWinKey));
      doClearWindows(&pInfo->aggSup, pSup, &pInfo->interval, pOperator->exprSupp.numOfExprs, pBlock, pUpWins);
      removeResults(pUpWins, pUpdatedMap);
      taosArrayDestroy(pUpWins);
      if (taosArrayGetSize(pUpdated) > 0) {
        break;
      }
      continue;
    } else if (pBlock->info.type == STREAM_PULL_OVER && IS_FINAL_OP(pInfo)) {
      processPullOver(pBlock, pInfo->pPullDataMap);
      continue;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, pInfo->order, MAIN_SCAN, true);
    doHashIntervalAgg(pOperator, pBlock, pBlock->info.groupId, pUpdatedMap);
    if (IS_FINAL_OP(pInfo)) {
      int32_t chIndex = getChildIndex(pBlock);
      int32_t size = taosArrayGetSize(pInfo->pChildren);
      // if chIndex + 1 - size > 0, add new child
      for (int32_t i = 0; i < chIndex + 1 - size; i++) {
        SOperatorInfo* pChildOp = createStreamFinalIntervalOperatorInfo(NULL, pInfo->pPhyNode, pOperator->pTaskInfo, 0);
        if (!pChildOp) {
          T_LONG_JMP(pOperator->pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
        SStreamFinalIntervalOperatorInfo* pTmpInfo = pChildOp->info;
        pTmpInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
        taosArrayPush(pInfo->pChildren, &pChildOp);
        qDebug("===stream===add child, id:%d", chIndex);
      }
      SOperatorInfo*                    pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      SStreamFinalIntervalOperatorInfo* pChInfo = pChildOp->info;
      setInputDataBlock(pChildOp, pChildOp->exprSupp.pCtx, pBlock, pChInfo->order, MAIN_SCAN, true);
      doHashIntervalAgg(pChildOp, pBlock, pBlock->info.groupId, NULL);
    }
    maxTs = TMAX(maxTs, pBlock->info.window.ekey);
    maxTs = TMAX(maxTs, pBlock->info.watermark);
    minTs = TMIN(minTs, pBlock->info.window.skey);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, minTs);
  if (IS_FINAL_OP(pInfo)) {
    closeIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, pInfo->pPullDataMap,
                        pUpdatedMap, pInfo->pRecycledPages, pInfo->aggSup.pResultBuf);
    closeChildIntervalWindow(pInfo->pChildren, pInfo->twAggSup.maxTs);
  } else {
    pInfo->binfo.pRes->info.watermark = pInfo->twAggSup.maxTs;
  }

  void* pIte = NULL;
  while ((pIte = taosHashIterate(pUpdatedMap, pIte)) != NULL) {
    taosArrayPush(pUpdated, pIte);
  }
  taosHashCleanup(pUpdatedMap);
  taosArraySort(pUpdated, resultrowComparAsc);

  finalizeUpdatedResult(pOperator->exprSupp.numOfExprs, pInfo->aggSup.pResultBuf, pUpdated, pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  doBuildPullDataBlock(pInfo->pPullWins, &pInfo->pullIndex, pInfo->pPullDataRes);
  if (pInfo->pPullDataRes->info.rows != 0) {
    // process the rest of the data
    ASSERT(IS_FINAL_OP(pInfo));
    printDataBlock(pInfo->pPullDataRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->pPullDataRes;
  }

  // we should send result first.
  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->binfo.pRes;
  }

  if (pInfo->pUpdateRes->info.rows != 0 && pInfo->returnUpdate) {
    pInfo->returnUpdate = false;
    ASSERT(!IS_FINAL_OP(pInfo));
    printDataBlock(pInfo->pUpdateRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    // process the rest of the data
    return pInfo->pUpdateRes;
  }

  doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->pDelRes;
  }
  return NULL;
}

SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                     SExecTaskInfo* pTaskInfo, int32_t numOfChild) {
  SIntervalPhysiNode*               pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;
  SStreamFinalIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamFinalIntervalOperatorInfo));
  SOperatorInfo*                    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  pInfo->order = TSDB_ORDER_ASC;
  pInfo->interval = (SInterval){.interval = pIntervalPhyNode->interval,
                                .sliding = pIntervalPhyNode->sliding,
                                .intervalUnit = pIntervalPhyNode->intervalUnit,
                                .slidingUnit = pIntervalPhyNode->slidingUnit,
                                .offset = pIntervalPhyNode->offset,
                                .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};
  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pIntervalPhyNode->window.watermark,
      .calTrigger = pIntervalPhyNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
  };
  ASSERT(pInfo->twAggSup.calTrigger != STREAM_TRIGGER_MAX_DELAY);
  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &numOfScalar);
    int32_t    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);

  int32_t code = initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initStreamFunciton(pOperator->exprSupp.pCtx, pOperator->exprSupp.numOfExprs);
  initBasicInfo(&pInfo->binfo, pResBlock);

  ASSERT(numOfCols > 0);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->pChildren = NULL;
  if (numOfChild > 0) {
    pInfo->pChildren = taosArrayInit(numOfChild, sizeof(void*));
    for (int32_t i = 0; i < numOfChild; i++) {
      SOperatorInfo* pChildOp = createStreamFinalIntervalOperatorInfo(NULL, pPhyNode, pTaskInfo, 0);
      if (pChildOp) {
        SStreamFinalIntervalOperatorInfo* pChInfo = pChildOp->info;
        pChInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
        taosArrayPush(pInfo->pChildren, &pChildOp);
        continue;
      }
      goto _error;
    }
  }
  pInfo->pUpdateRes = createSpecialDataBlock(STREAM_CLEAR);
  blockDataEnsureCapacity(pInfo->pUpdateRes, 128);
  pInfo->returnUpdate = false;

  pInfo->pPhyNode = (SPhysiNode*)nodesCloneNode((SNode*)pPhyNode);

  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL) {
    pInfo->isFinal = true;
    pOperator->name = "StreamFinalIntervalOperator";
  } else {
    // semi interval operator does not catch result
    pInfo->isFinal = false;
    pOperator->name = "StreamSemiIntervalOperator";
    ASSERT(pInfo->aggSup.currentPageId == -1);
  }

  if (!IS_FINAL_OP(pInfo) || numOfChild == 0) {
    pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
  }
  pInfo->pPullWins = taosArrayInit(8, sizeof(SPullWindowInfo));
  pInfo->pullIndex = 0;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pPullDataMap = taosHashInit(64, hashFn, false, HASH_NO_LOCK);
  pInfo->pPullDataRes = createSpecialDataBlock(STREAM_RETRIEVE);
  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->delIndex = 0;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->pRecycledPages = taosArrayInit(4, sizeof(int32_t));

  pOperator->operatorType = pPhyNode->type;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  pOperator->fpSet =
      createOperatorFpSet(NULL, doStreamFinalIntervalAgg, NULL, NULL, destroyStreamFinalIntervalOperatorInfo,
                          aggEncodeResultRow, aggDecodeResultRow, NULL);
  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL) {
    initIntervalDownStream(downstream, pPhyNode->type, &pInfo->aggSup, &pInfo->interval, &pInfo->twAggSup);
  }
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyStreamFinalIntervalOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyStreamAggSupporter(SStreamAggSupporter* pSup) {
  taosMemoryFreeClear(pSup->pKeyBuf);
  void** pIte = NULL;
  while ((pIte = taosHashIterate(pSup->pResultRows, pIte)) != NULL) {
    SArray* pWins = (SArray*)(*pIte);
    taosArrayDestroy(pWins);
  }
  taosHashCleanup(pSup->pResultRows);
  destroyDiskbasedBuf(pSup->pResultBuf);
  blockDataDestroy(pSup->pScanBlock);
}

void destroyStateWinInfo(void* ptr) {
  if (ptr == NULL) {
    return;
  }
  SStateWindowInfo* pWin = (SStateWindowInfo*)ptr;
  taosMemoryFreeClear(pWin->stateKey.pData);
}

void destroyStateStreamAggSupporter(SStreamAggSupporter* pSup) {
  taosMemoryFreeClear(pSup->pKeyBuf);
  void** pIte = NULL;
  while ((pIte = taosHashIterate(pSup->pResultRows, pIte)) != NULL) {
    SArray* pWins = (SArray*)(*pIte);
    taosArrayDestroyEx(pWins, (FDelete)destroyStateWinInfo);
  }
  taosHashCleanup(pSup->pResultRows);
  destroyDiskbasedBuf(pSup->pResultBuf);
  blockDataDestroy(pSup->pScanBlock);
}

void destroyStreamSessionAggOperatorInfo(void* param) {
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, i);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      destroyStreamSessionAggOperatorInfo(pChInfo);
      taosMemoryFreeClear(pChild);
    }
  }
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  blockDataDestroy(pInfo->pWinBlock);
  blockDataDestroy(pInfo->pUpdateRes);
  destroySqlFunctionCtx(pInfo->pDummyCtx, 0);
  taosHashCleanup(pInfo->pStDeleted);

  taosMemoryFreeClear(param);
}

int32_t initBasicInfoEx(SOptrBasicInfo* pBasicInfo, SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfCols,
                        SSDataBlock* pResultBlock) {
  int32_t code = initExprSupp(pSup, pExprInfo, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  initStreamFunciton(pSup->pCtx, pSup->numOfExprs);

  initBasicInfo(pBasicInfo, pResultBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    pSup->pCtx[i].saveHandle.pBuf = NULL;
  }

  ASSERT(numOfCols > 0);
  return TSDB_CODE_SUCCESS;
}

void initDummyFunction(SqlFunctionCtx* pDummy, SqlFunctionCtx* pCtx, int32_t nums) {
  for (int i = 0; i < nums; i++) {
    pDummy[i].functionId = pCtx[i].functionId;
  }
}

void initDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, int64_t gap, int64_t waterMark,
                    uint16_t type, int32_t tsColIndex) {
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION) {
    SStreamPartitionOperatorInfo* pScanInfo = downstream->info;
    pScanInfo->tsColIndex = tsColIndex;
  }

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    initDownStream(downstream->pDownstream[0], pAggSup, gap, waterMark, type, tsColIndex);
    return;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup = (SWindowSupporter){.pStreamAggSup = pAggSup, .gap = gap, .parentType = type};
  pScanInfo->pUpdateInfo = updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, waterMark);
}

int32_t initSessionAggSupporter(SStreamAggSupporter* pSup, const char* pKey, SqlFunctionCtx* pCtx,
                                int32_t numOfOutput) {
  return initStreamAggSupporter(pSup, pKey, pCtx, numOfOutput, sizeof(SResultWindowInfo));
}

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                  SExecTaskInfo* pTaskInfo) {
  SSessionWinodwPhysiNode*       pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;
  int32_t                        numOfCols = 0;
  SExprInfo*                     pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock*                   pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
  int32_t                        code = TSDB_CODE_OUT_OF_MEMORY;
  SStreamSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamSessionAggOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pSessionNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pSessionNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  SExprSupp* pSup = &pOperator->exprSupp;

  code = initBasicInfoEx(&pInfo->binfo, pSup, pExprInfo, numOfCols, pResBlock);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initSessionAggSupporter(&pInfo->streamAggSup, "StreamSessionAggOperatorInfo", pSup->pCtx, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pDummyCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfCols, sizeof(SqlFunctionCtx));
  if (pInfo->pDummyCtx == NULL) {
    goto _error;
  }
  initDummyFunction(pInfo->pDummyCtx, pSup->pCtx, numOfCols);

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pSessionNode->window.watermark,
      .calTrigger = pSessionNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
  };

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  if (pSessionNode->window.pTsEnd) {
    pInfo->endTsIndex = ((SColumnNode*)pSessionNode->window.pTsEnd)->slotId;
  }
  pInfo->gap = pSessionNode->gap;
  pInfo->binfo.pRes = pResBlock;
  pInfo->order = TSDB_ORDER_ASC;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pStDeleted = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pChildren = NULL;
  pInfo->isFinal = false;
  pInfo->pPhyNode = pPhyNode;
  pInfo->ignoreExpiredData = pSessionNode->window.igExpired;

  pOperator->name = "StreamSessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doStreamSessionAgg, NULL, NULL, destroyStreamSessionAggOperatorInfo,
                          aggEncodeResultRow, aggDecodeResultRow, NULL);
  if (downstream) {
    initDownStream(downstream, &pInfo->streamAggSup, pInfo->gap, pInfo->twAggSup.waterMark, pOperator->operatorType,
                   pInfo->primaryTsIndex);
    code = appendDownstream(pOperator, &downstream, 1);
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

int64_t getSessionWindowEndkey(void* data, int32_t index) {
  SArray*            pWinInfos = (SArray*)data;
  SResultWindowInfo* pWin = taosArrayGet(pWinInfos, index);
  return pWin->win.ekey;
}

bool isInTimeWindow(STimeWindow* pWin, TSKEY ts, int64_t gap) {
  if (ts + gap >= pWin->skey && ts - gap <= pWin->ekey) {
    return true;
  }
  return false;
}

bool isInWindow(SResultWindowInfo* pWinInfo, TSKEY ts, int64_t gap) { return isInTimeWindow(&pWinInfo->win, ts, gap); }

static SResultWindowInfo* insertNewSessionWindow(SArray* pWinInfos, TSKEY startTs, TSKEY endTs, int32_t index) {
  SResultWindowInfo win = {
      .pos.offset = -1, .pos.pageId = -1, .win.skey = startTs, .win.ekey = endTs, .isOutput = false};
  return taosArrayInsert(pWinInfos, index, &win);
}

static SResultWindowInfo* addNewSessionWindow(SArray* pWinInfos, TSKEY startTs, TSKEY endTs) {
  SResultWindowInfo win = {
      .pos.offset = -1, .pos.pageId = -1, .win.skey = startTs, .win.ekey = endTs, .isOutput = false};
  return taosArrayPush(pWinInfos, &win);
}

SArray* getWinInfos(SStreamAggSupporter* pAggSup, uint64_t groupId) {
  void**  ite = taosHashGet(pAggSup->pResultRows, &groupId, sizeof(uint64_t));
  SArray* pWinInfos = NULL;
  if (ite == NULL) {
    pWinInfos = taosArrayInit(1024, pAggSup->valueSize);
    taosHashPut(pAggSup->pResultRows, &groupId, sizeof(uint64_t), &pWinInfos, sizeof(void*));
  } else {
    pWinInfos = *ite;
  }
  return pWinInfos;
}

// don't add new window
SResultWindowInfo* getCurSessionWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                                       int64_t gap, int32_t* pIndex) {
  STimeWindow searchWin = {.skey = startTs, .ekey = endTs};
  SArray*     pWinInfos = getWinInfos(pAggSup, groupId);
  pAggSup->pCurWins = pWinInfos;

  int32_t size = taosArrayGetSize(pWinInfos);
  if (size == 0) {
    return NULL;
  }
  // find the first position which is smaller than the key
  int32_t            index = binarySearch(pWinInfos, size, startTs, TSDB_ORDER_DESC, getSessionWindowEndkey);
  SResultWindowInfo* pWin = NULL;
  if (index >= 0) {
    pWin = taosArrayGet(pWinInfos, index);
    if (isInWindow(pWin, startTs, gap) || isInTimeWindow(&searchWin, pWin->win.skey, gap)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index + 1 < size) {
    pWin = taosArrayGet(pWinInfos, index + 1);
    if (isInWindow(pWin, startTs, gap) || isInTimeWindow(&searchWin, pWin->win.skey, gap)) {
      *pIndex = index + 1;
      return pWin;
    } else if (endTs != INT64_MIN && isInWindow(pWin, endTs, gap)) {
      *pIndex = index + 1;
      return pWin;
    }
  }

  return NULL;
}

SResultWindowInfo* getSessionTimeWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                                        int64_t gap, int32_t* pIndex) {
  SArray* pWinInfos = getWinInfos(pAggSup, groupId);
  pAggSup->pCurWins = pWinInfos;

  int32_t size = taosArrayGetSize(pWinInfos);
  if (size == 0) {
    *pIndex = 0;
    return addNewSessionWindow(pWinInfos, startTs, endTs);
  }
  // find the first position which is smaller than the key
  int32_t            index = binarySearch(pWinInfos, size, startTs, TSDB_ORDER_DESC, getSessionWindowEndkey);
  SResultWindowInfo* pWin = NULL;
  if (index >= 0) {
    pWin = taosArrayGet(pWinInfos, index);
    if (isInWindow(pWin, startTs, gap)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index + 1 < size) {
    pWin = taosArrayGet(pWinInfos, index + 1);
    if (isInWindow(pWin, startTs, gap)) {
      *pIndex = index + 1;
      return pWin;
    } else if (endTs != INT64_MIN && isInWindow(pWin, endTs, gap)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index == size - 1) {
    *pIndex = taosArrayGetSize(pWinInfos);
    return addNewSessionWindow(pWinInfos, startTs, endTs);
  }
  *pIndex = index + 1;
  return insertNewSessionWindow(pWinInfos, startTs, endTs, index + 1);
}

int32_t updateSessionWindowInfo(SResultWindowInfo* pWinInfo, TSKEY* pStartTs, TSKEY* pEndTs, uint64_t groupId,
                                int32_t rows, int32_t start, int64_t gap, SHashObj* pStDeleted) {
  for (int32_t i = start; i < rows; ++i) {
    if (!isInWindow(pWinInfo, pStartTs[i], gap) && (!pEndTs || !isInWindow(pWinInfo, pEndTs[i], gap))) {
      return i - start;
    }
    if (pWinInfo->win.skey > pStartTs[i]) {
      if (pStDeleted && pWinInfo->isOutput) {
        SWinKey res = {.ts = pWinInfo->win.skey, .groupId = groupId};
        taosHashPut(pStDeleted, &res, sizeof(SWinKey), &res, sizeof(SWinKey));
        pWinInfo->isOutput = false;
      }
      pWinInfo->win.skey = pStartTs[i];
    }
    pWinInfo->win.ekey = TMAX(pWinInfo->win.ekey, pStartTs[i]);
    if (pEndTs) {
      pWinInfo->win.ekey = TMAX(pWinInfo->win.ekey, pEndTs[i]);
    }
  }
  return rows - start;
}

static int32_t setWindowOutputBuf(SResultWindowInfo* pWinInfo, SResultRow** pResult, SqlFunctionCtx* pCtx,
                                  uint64_t groupId, int32_t numOfOutput, int32_t* rowEntryInfoOffset,
                                  SStreamAggSupporter* pAggSup, SExecTaskInfo* pTaskInfo) {
  assert(pWinInfo->win.skey <= pWinInfo->win.ekey);
  // too many time window in query
  int32_t size = taosArrayGetSize(pAggSup->pCurWins);
  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_BATCH && size > MAX_INTERVAL_TIME_WINDOW) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  if (pWinInfo->pos.pageId == -1) {
    *pResult = getNewResultRow(pAggSup->pResultBuf, &pAggSup->currentPageId, pAggSup->resultRowSize);
    if (*pResult == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    // add a new result set for a new group
    pWinInfo->pos.pageId = (*pResult)->pageId;
    pWinInfo->pos.offset = (*pResult)->offset;
  } else {
    *pResult = getResultRowByPos(pAggSup->pResultBuf, &pWinInfo->pos, true);
    if (!(*pResult)) {
      qError("getResultRowByPos return NULL, TID:%s", GET_TASKID(pTaskInfo));
      return TSDB_CODE_FAILED;
    }
  }

  // set time window for current result
  (*pResult)->win = pWinInfo->win;
  setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static int32_t doOneWindowAggImpl(int32_t tsColId, SOptrBasicInfo* pBinfo, SStreamAggSupporter* pAggSup,
                                  SColumnInfoData* pTimeWindowData, SSDataBlock* pSDataBlock,
                                  SResultWindowInfo* pCurWin, SResultRow** pResult, int32_t startIndex, int32_t winRows,
                                  int32_t numOutput, SOperatorInfo* pOperator) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, tsColId);
  TSKEY*           tsCols = (int64_t*)pColDataInfo->pData;
  int32_t          code = setWindowOutputBuf(pCurWin, pResult, pSup->pCtx, pSDataBlock->info.groupId, numOutput,
                                             pSup->rowEntryInfoOffset, pAggSup, pTaskInfo);
  if (code != TSDB_CODE_SUCCESS || (*pResult) == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  updateTimeWindowInfo(pTimeWindowData, &pCurWin->win, false);
  doApplyFunctions(pTaskInfo, pSup->pCtx, pTimeWindowData, startIndex, winRows, pSDataBlock->info.rows, numOutput);
  SFilePage* bufPage = getBufPage(pAggSup->pResultBuf, pCurWin->pos.pageId);
  setBufPageDirty(bufPage, true);
  releaseBufPage(pAggSup->pResultBuf, bufPage);
  return TSDB_CODE_SUCCESS;
}

static int32_t doOneWindowAgg(SStreamSessionAggOperatorInfo* pInfo, SSDataBlock* pSDataBlock,
                              SResultWindowInfo* pCurWin, SResultRow** pResult, int32_t startIndex, int32_t winRows,
                              int32_t numOutput, SOperatorInfo* pOperator) {
  return doOneWindowAggImpl(pInfo->primaryTsIndex, &pInfo->binfo, &pInfo->streamAggSup, &pInfo->twAggSup.timeWindowData,
                            pSDataBlock, pCurWin, pResult, startIndex, winRows, numOutput, pOperator);
}

static int32_t doOneStateWindowAgg(SStreamStateAggOperatorInfo* pInfo, SSDataBlock* pSDataBlock,
                                   SResultWindowInfo* pCurWin, SResultRow** pResult, int32_t startIndex,
                                   int32_t winRows, int32_t numOutput, SOperatorInfo* pOperator) {
  return doOneWindowAggImpl(pInfo->primaryTsIndex, &pInfo->binfo, &pInfo->streamAggSup, &pInfo->twAggSup.timeWindowData,
                            pSDataBlock, pCurWin, pResult, startIndex, winRows, numOutput, pOperator);
}

int32_t getNumCompactWindow(SArray* pWinInfos, int32_t startIndex, int64_t gap) {
  SResultWindowInfo* pCurWin = taosArrayGet(pWinInfos, startIndex);
  int32_t            size = taosArrayGetSize(pWinInfos);
  // Just look for the window behind StartIndex
  for (int32_t i = startIndex + 1; i < size; i++) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pWinInfos, i);
    if (!isInWindow(pCurWin, pWinInfo->win.skey, gap)) {
      return i - startIndex - 1;
    }
  }

  return size - startIndex - 1;
}

void compactTimeWindow(SStreamSessionAggOperatorInfo* pInfo, int32_t startIndex, int32_t num, uint64_t groupId,
                       int32_t numOfOutput, SHashObj* pStUpdated, SHashObj* pStDeleted, SOperatorInfo* pOperator) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SResultWindowInfo* pCurWin = taosArrayGet(pInfo->streamAggSup.pCurWins, startIndex);
  SResultRow*        pCurResult = NULL;
  setWindowOutputBuf(pCurWin, &pCurResult, pSup->pCtx, groupId, numOfOutput, pSup->rowEntryInfoOffset,
                     &pInfo->streamAggSup, pTaskInfo);
  num += startIndex + 1;
  ASSERT(num <= taosArrayGetSize(pInfo->streamAggSup.pCurWins));
  // Just look for the window behind StartIndex
  for (int32_t i = startIndex + 1; i < num; i++) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pInfo->streamAggSup.pCurWins, i);
    SResultRow*        pWinResult = NULL;
    setWindowOutputBuf(pWinInfo, &pWinResult, pInfo->pDummyCtx, groupId, numOfOutput, pSup->rowEntryInfoOffset,
                       &pInfo->streamAggSup, pTaskInfo);
    pCurWin->win.ekey = TMAX(pCurWin->win.ekey, pWinInfo->win.ekey);
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pCurWin->win, true);
    compactFunctions(pSup->pCtx, pInfo->pDummyCtx, numOfOutput, pTaskInfo, &pInfo->twAggSup.timeWindowData);
    taosHashRemove(pStUpdated, &pWinInfo->pos, sizeof(SResultRowPosition));
    if (pWinInfo->isOutput && pStDeleted) {
      SWinKey res = {.ts = pWinInfo->win.skey, .groupId = groupId};
      taosHashPut(pStDeleted, &res, sizeof(SWinKey), &res, sizeof(SWinKey));
      pWinInfo->isOutput = false;
    }
    taosArrayRemove(pInfo->streamAggSup.pCurWins, i);
    SFilePage* tmpPage = getBufPage(pInfo->streamAggSup.pResultBuf, pWinInfo->pos.pageId);
    releaseBufPage(pInfo->streamAggSup.pResultBuf, tmpPage);
  }
  SFilePage* bufPage = getBufPage(pInfo->streamAggSup.pResultBuf, pCurWin->pos.pageId);
  ASSERT(num > 0);
  setBufPageDirty(bufPage, true);
  releaseBufPage(pInfo->streamAggSup.pResultBuf, bufPage);
}

static void doStreamSessionAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SHashObj* pStUpdated,
                                   SHashObj* pStDeleted, bool hasEndTs) {
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  bool                           masterScan = true;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                       groupId = pSDataBlock->info.groupId;
  int64_t                        gap = pInfo->gap;
  int64_t                        code = TSDB_CODE_SUCCESS;

  int32_t     step = 1;
  bool        ascScan = true;
  TSKEY*      startTsCols = NULL;
  TSKEY*      endTsCols = NULL;
  SResultRow* pResult = NULL;
  int32_t     winRows = 0;

  ASSERT(pSDataBlock->pDataBlock);
  SColumnInfoData* pStartTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  startTsCols = (int64_t*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = NULL;
  if (hasEndTs) {
    pEndTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->endTsIndex);
  } else {
    pEndTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  }
  endTsCols = (int64_t*)pEndTsCol->pData;

  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;
  for (int32_t i = 0; i < pSDataBlock->info.rows;) {
    if (pInfo->ignoreExpiredData && isOverdue(endTsCols[i], &pInfo->twAggSup)) {
      i++;
      continue;
    }
    int32_t            winIndex = 0;
    SResultWindowInfo* pCurWin = getSessionTimeWindow(pAggSup, startTsCols[i], endTsCols[i], groupId, gap, &winIndex);
    winRows = updateSessionWindowInfo(pCurWin, startTsCols, endTsCols, groupId, pSDataBlock->info.rows, i, pInfo->gap,
                                      pStDeleted);
    code = doOneWindowAgg(pInfo, pSDataBlock, pCurWin, &pResult, i, winRows, numOfOutput, pOperator);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    int32_t winNum = getNumCompactWindow(pAggSup->pCurWins, winIndex, gap);
    if (winNum > 0) {
      compactTimeWindow(pInfo, winIndex, winNum, groupId, numOfOutput, pStUpdated, pStDeleted, pOperator);
    }
    pCurWin->isClosed = false;
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pStUpdated) {
      SWinKey value = {.ts = pCurWin->win.skey, .groupId = groupId};
      code = taosHashPut(pStUpdated, &pCurWin->pos, sizeof(SResultRowPosition), &value, sizeof(SWinKey));
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      pCurWin->isOutput = true;
    }
    i += winRows;
  }
}

void deleteWindow(SArray* pWinInfos, int32_t index, FDelete fp) {
  ASSERT(index >= 0 && index < taosArrayGetSize(pWinInfos));
  if (fp) {
    void* ptr = taosArrayGet(pWinInfos, index);
    fp(ptr);
  }
  taosArrayRemove(pWinInfos, index);
}

static void doDeleteTimeWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, int64_t gap, SArray* result,
                                FDelete fp) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startDatas = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endDatas = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpDatas = (uint64_t*)pGroupCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    int32_t            winIndex = 0;
    SResultWindowInfo* pCurWin = getCurSessionWindow(pAggSup, startDatas[i], endDatas[i], gpDatas[i], gap, &winIndex);
    if (!pCurWin) {
      continue;
    }

    do {
      SResultWindowInfo delWin = *pCurWin;
      deleteWindow(pAggSup->pCurWins, winIndex, fp);
      if (result) {
        delWin.groupId = gpDatas[i];
        taosArrayPush(result, &delWin);
      }
      if (winIndex >= taosArrayGetSize(pAggSup->pCurWins)) {
        break;
      }
      pCurWin = taosArrayGet(pAggSup->pCurWins, winIndex);
    } while (pCurWin->win.skey <= endDatas[i]);
  }
}

static void doClearSessionWindows(SStreamAggSupporter* pAggSup, SExprSupp* pSup, SSDataBlock* pBlock, int32_t tsIndex,
                                  int32_t numOfOutput, int64_t gap, SArray* result) {
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, tsIndex);
  TSKEY*           tsCols = (TSKEY*)pColDataInfo->pData;
  SColumnInfoData* pGpDataInfo = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpCols = (uint64_t*)pGpDataInfo->pData;
  int32_t          step = 0;
  for (int32_t i = 0; i < pBlock->info.rows; i += step) {
    int32_t            winIndex = 0;
    SResultWindowInfo* pCurWin = getCurSessionWindow(pAggSup, tsCols[i], INT64_MIN, gpCols[i], gap, &winIndex);
    if (!pCurWin || pCurWin->pos.pageId == -1) {
      // window has been closed.
      step = 1;
      continue;
    }
    step = updateSessionWindowInfo(pCurWin, tsCols, NULL, 0, pBlock->info.rows, i, gap, NULL);
    ASSERT(isInWindow(pCurWin, tsCols[i], gap));
    doClearWindowImpl(&pCurWin->pos, pAggSup->pResultBuf, pSup, numOfOutput);
    if (result) {
      pCurWin->groupId = gpCols[i];
      taosArrayPush(result, pCurWin);
    }
  }
}

static int32_t copyUpdateResult(SHashObj* pStUpdated, SArray* pUpdated) {
  void*  pData = NULL;
  size_t keyLen = 0;
  while ((pData = taosHashIterate(pStUpdated, pData)) != NULL) {
    void* key = taosHashGetKey(pData, &keyLen);
    ASSERT(keyLen == sizeof(SResultRowPosition));
    SResKeyPos* pos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
    if (pos == NULL) {
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
    pos->groupId = ((SWinKey*)pData)->groupId;
    pos->pos = *(SResultRowPosition*)key;
    *(int64_t*)pos->key = ((SWinKey*)pData)->ts;
    taosArrayPush(pUpdated, &pos);
  }
  taosArraySort(pUpdated, resultrowComparAsc);
  return TSDB_CODE_SUCCESS;
}

void doBuildDeleteDataBlock(SHashObj* pStDeleted, SSDataBlock* pBlock, void** Ite) {
  blockDataCleanup(pBlock);
  int32_t size = taosHashGetSize(pStDeleted);
  if (size == 0) {
    return;
  }
  blockDataEnsureCapacity(pBlock, size);
  size_t keyLen = 0;
  while (((*Ite) = taosHashIterate(pStDeleted, *Ite)) != NULL) {
    SWinKey*         res = *Ite;
    SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
    colDataAppend(pStartTsCol, pBlock->info.rows, (const char*)&res->ts, false);
    SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
    colDataAppend(pEndTsCol, pBlock->info.rows, (const char*)&res->ts, false);
    SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
    colDataAppendNULL(pUidCol, pBlock->info.rows);
    SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
    colDataAppend(pGpCol, pBlock->info.rows, (const char*)&res->groupId, false);
    SColumnInfoData* pCalStCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
    colDataAppendNULL(pCalStCol, pBlock->info.rows);
    SColumnInfoData* pCalEdCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
    colDataAppendNULL(pCalEdCol, pBlock->info.rows);
    pBlock->info.rows += 1;
    if (pBlock->info.rows + 1 >= pBlock->info.capacity) {
      break;
    }
  }
  if ((*Ite) == NULL) {
    taosHashClear(pStDeleted);
  }
}

static void rebuildTimeWindow(SStreamSessionAggOperatorInfo* pInfo, SArray* pWinArray, int32_t numOfOutput,
                              SOperatorInfo* pOperator, SHashObj* pStUpdated) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t        size = taosArrayGetSize(pWinArray);
  ASSERT(pInfo->pChildren);

  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo* pParentWin = taosArrayGet(pWinArray, i);
    uint64_t           groupId = pParentWin->groupId;
    int32_t            numOfChildren = taosArrayGetSize(pInfo->pChildren);
    for (int32_t j = 0; j < numOfChildren; j++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, j);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      SArray*                        pChWins = getWinInfos(&pChInfo->streamAggSup, groupId);
      int32_t                        chWinSize = taosArrayGetSize(pChWins);
      int32_t index = binarySearch(pChWins, chWinSize, pParentWin->win.skey, TSDB_ORDER_DESC, getSessionWindowEndkey);
      if (index < 0) {
        index = 0;
      }
      for (int32_t k = index; k < chWinSize; k++) {
        SResultWindowInfo* pChWin = taosArrayGet(pChWins, k);
        if (pParentWin->win.skey <= pChWin->win.skey && pChWin->win.ekey <= pParentWin->win.ekey) {
          int32_t            winIndex = 0;
          SResultWindowInfo* pNewParWin =
              getSessionTimeWindow(&pInfo->streamAggSup, pChWin->win.skey, pChWin->win.ekey, groupId, 0, &winIndex);
          SResultRow* pPareResult = NULL;
          setWindowOutputBuf(pNewParWin, &pPareResult, pSup->pCtx, groupId, numOfOutput, pSup->rowEntryInfoOffset,
                             &pInfo->streamAggSup, pTaskInfo);
          SResultRow* pChResult = NULL;
          setWindowOutputBuf(pChWin, &pChResult, pChild->exprSupp.pCtx, groupId, numOfOutput,
                             pChild->exprSupp.rowEntryInfoOffset, &pChInfo->streamAggSup, pTaskInfo);
          updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pNewParWin->win, true);
          compactFunctions(pSup->pCtx, pChild->exprSupp.pCtx, numOfOutput, pTaskInfo, &pInfo->twAggSup.timeWindowData);

          int32_t winNum = getNumCompactWindow(pInfo->streamAggSup.pCurWins, winIndex, pInfo->gap);
          if (winNum > 0) {
            compactTimeWindow(pInfo, winIndex, winNum, groupId, numOfOutput, pStUpdated, NULL, pOperator);
          }

          SFilePage* bufPage = getBufPage(pChInfo->streamAggSup.pResultBuf, pChWin->pos.pageId);
          releaseBufPage(pChInfo->streamAggSup.pResultBuf, bufPage);

          bufPage = getBufPage(pInfo->streamAggSup.pResultBuf, pNewParWin->pos.pageId);
          setBufPageDirty(bufPage, true);
          releaseBufPage(pInfo->streamAggSup.pResultBuf, bufPage);
          SWinKey value = {.ts = pNewParWin->win.skey, .groupId = groupId};
          taosHashPut(pStUpdated, &pNewParWin->pos, sizeof(SResultRowPosition), &value, sizeof(SWinKey));
        } else if (!pChWin->isClosed) {
          break;
        }
      }
    }
  }
}

typedef SResultWindowInfo* (*__get_win_info_)(void*);
SResultWindowInfo* getResWinForSession(void* pData) { return (SResultWindowInfo*)pData; }
SResultWindowInfo* getResWinForState(void* pData) { return &((SStateWindowInfo*)pData)->winInfo; }

int32_t closeSessionWindow(SHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SArray* pClosed, __get_win_info_ fn,
                           bool delete, FDelete fp) {
  // Todo(liuyao) save window to tdb
  void** pIte = NULL;
  size_t keyLen = 0;
  while ((pIte = taosHashIterate(pHashMap, pIte)) != NULL) {
    uint64_t* pGroupId = taosHashGetKey(pIte, &keyLen);
    SArray*   pWins = (SArray*)(*pIte);
    int32_t   size = taosArrayGetSize(pWins);
    for (int32_t i = 0; i < size; i++) {
      void*              pWin = taosArrayGet(pWins, i);
      SResultWindowInfo* pSeWin = fn(pWin);
      if (isCloseWindow(&pSeWin->win, pTwSup)) {
        if (!pSeWin->isClosed) {
          pSeWin->isClosed = true;
          if (pTwSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE && pClosed) {
            int32_t code = saveResult(pSeWin->win.skey, pSeWin->pos.pageId, pSeWin->pos.offset, *pGroupId, pClosed);
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }
            pSeWin->isOutput = true;
          }
          if (delete) {
            deleteWindow(pWins, i, fp);
            i--;
            size = taosArrayGetSize(pWins);
          }
        }
        continue;
      }
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void closeChildSessionWindow(SArray* pChildren, TSKEY maxTs, bool delete, FDelete fp) {
  int32_t size = taosArrayGetSize(pChildren);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo*                 pChildOp = taosArrayGetP(pChildren, i);
    SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
    pChInfo->twAggSup.maxTs = TMAX(pChInfo->twAggSup.maxTs, maxTs);
    closeSessionWindow(pChInfo->streamAggSup.pResultRows, &pChInfo->twAggSup, NULL, getResWinForSession, delete, fp);
  }
}

int32_t getAllSessionWindow(SHashObj* pHashMap, SArray* pClosed, __get_win_info_ fn) {
  void** pIte = NULL;
  while ((pIte = taosHashIterate(pHashMap, pIte)) != NULL) {
    SArray* pWins = (SArray*)(*pIte);
    int32_t size = taosArrayGetSize(pWins);
    for (int32_t i = 0; i < size; i++) {
      void*              pWin = taosArrayGet(pWins, i);
      SResultWindowInfo* pSeWin = fn(pWin);
      if (!pSeWin->isClosed) {
        int32_t code = saveResult(pSeWin->win.skey, pSeWin->pos.pageId, pSeWin->pos.offset, 0, pClosed);
        pSeWin->isOutput = true;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void copyDeleteWindowInfo(SArray* pResWins, SHashObj* pStDeleted) {
  int32_t size = taosArrayGetSize(pResWins);
  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pResWins, i);
    SWinKey            res = {.ts = pWinInfo->win.skey, .groupId = pWinInfo->groupId};
    taosHashPut(pStDeleted, &res, sizeof(SWinKey), &res, sizeof(SWinKey));
  }
}

static void removeSessionResults(SHashObj* pHashMap, SArray* pWins) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo* pWin = taosArrayGet(pWins, i);
    taosHashRemove(pHashMap, &pWin->pos, sizeof(SResultRowPosition));
  }
}

int32_t compareWinKey(void* pKey, void* data, int32_t index) {
  SArray*     res = (SArray*)data;
  SResKeyPos* pos = taosArrayGetP(res, index);
  SWinKey*    pData = (SWinKey*)pKey;
  if (pData->ts == *(int64_t*)pos->key) {
    if (pData->groupId > pos->groupId) {
      return 1;
    } else if (pData->groupId < pos->groupId) {
      return -1;
    }
    return 0;
  } else if (pData->ts > *(int64_t*)pos->key) {
    return 1;
  }
  return -1;
}

static void removeSessionDeleteResults(SArray* update, SHashObj* pStDeleted) {
  int32_t size = taosHashGetSize(pStDeleted);
  if (size == 0) {
    return;
  }

  int32_t num = taosArrayGetSize(update);
  for (int32_t i = 0; i < num; i++) {
    SResKeyPos* pos = taosArrayGetP(update, i);
    SWinKey     winKey = {.ts = *(int64_t*)pos->key, .groupId = pos->groupId};
    taosHashRemove(pStDeleted, &winKey, sizeof(SWinKey));
  }
}

static SSDataBlock* doStreamSessionAgg(SOperatorInfo* pOperator) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  TSKEY                          maxTs = INT64_MIN;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
      return pInfo->pDelRes;
    }
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
    if (pBInfo->pRes->info.rows == 0 || !hasRemainResults(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }
    printDataBlock(pBInfo->pRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
    return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
  }

  _hash_fn_t     hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*      pStUpdated = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SArray*        pUpdated = taosArrayInit(16, POINTER_BYTES);  // SResKeyPos
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, IS_FINAL_OP(pInfo) ? "final session recv" : "single session recv");

    if (pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doClearSessionWindows(&pInfo->streamAggSup, &pOperator->exprSupp, pBlock, START_TS_COLUMN_INDEX,
                            pOperator->exprSupp.numOfExprs, 0, pWins);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                        childIndex = getChildIndex(pBlock);
        SOperatorInfo*                 pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        doClearSessionWindows(&pChildInfo->streamAggSup, &pChildOp->exprSupp, pBlock, START_TS_COLUMN_INDEX,
                              pChildOp->exprSupp.numOfExprs, 0, NULL);
        rebuildTimeWindow(pInfo, pWins, pOperator->exprSupp.numOfExprs, pOperator, pStUpdated);
      }
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      // gap must be 0
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, 0, pWins, NULL);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                        childIndex = getChildIndex(pBlock);
        SOperatorInfo*                 pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        // gap must be 0
        doDeleteTimeWindows(&pChildInfo->streamAggSup, pBlock, 0, NULL, NULL);
        rebuildTimeWindow(pInfo, pWins, pOperator->exprSupp.numOfExprs, pOperator, pStUpdated);
      }
      copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
      removeSessionResults(pStUpdated, pWins);
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pUpdated, getResWinForSession);
      continue;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamSessionAggImpl(pOperator, pBlock, pStUpdated, pInfo->pStDeleted, IS_FINAL_OP(pInfo));
    if (IS_FINAL_OP(pInfo)) {
      int32_t chIndex = getChildIndex(pBlock);
      int32_t size = taosArrayGetSize(pInfo->pChildren);
      // if chIndex + 1 - size > 0, add new child
      for (int32_t i = 0; i < chIndex + 1 - size; i++) {
        SOperatorInfo* pChildOp =
            createStreamFinalSessionAggOperatorInfo(NULL, pInfo->pPhyNode, pOperator->pTaskInfo, 0);
        if (!pChildOp) {
          T_LONG_JMP(pOperator->pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
        taosArrayPush(pInfo->pChildren, &pChildOp);
      }
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      setInputDataBlock(pChildOp, pChildOp->exprSupp.pCtx, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
      doStreamSessionAggImpl(pChildOp, pBlock, NULL, NULL, true);
    }
    maxTs = TMAX(maxTs, pBlock->info.window.ekey);
    maxTs = TMAX(maxTs, pBlock->info.watermark);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pUpdated, getResWinForSession,
                     pInfo->ignoreExpiredData, NULL);
  closeChildSessionWindow(pInfo->pChildren, pInfo->twAggSup.maxTs, pInfo->ignoreExpiredData, NULL);
  copyUpdateResult(pStUpdated, pUpdated);
  removeSessionDeleteResults(pUpdated, pInfo->pStDeleted);
  taosHashCleanup(pStUpdated);

  finalizeUpdatedResult(pSup->numOfExprs, pInfo->streamAggSup.pResultBuf, pUpdated, pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
    return pInfo->pDelRes;
  }
  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
  printDataBlock(pBInfo->pRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
  return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
}

static void clearStreamSessionOperator(SStreamSessionAggOperatorInfo* pInfo) {
  void** pIte = NULL;
  while ((pIte = taosHashIterate(pInfo->streamAggSup.pResultRows, pIte)) != NULL) {
    SArray* pWins = (SArray*)(*pIte);
    int32_t size = taosArrayGetSize(pWins);
    for (int32_t i = 0; i < size; i++) {
      SResultWindowInfo* pWin = (SResultWindowInfo*)taosArrayGet(pWins, i);
      pWin->pos.pageId = -1;
      pWin->pos.offset = -1;
    }
  }
  clearDiskbasedBuf(pInfo->streamAggSup.pResultBuf);
  pInfo->streamAggSup.currentPageId = -1;
}

static SSDataBlock* doStreamSessionSemiAgg(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  TSKEY                          maxTs = INT64_MIN;
  SExprSupp*                     pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  {
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
    if (pBInfo->pRes->info.rows > 0) {
      printDataBlock(pBInfo->pRes, "semi session");
      return pBInfo->pRes;
    }

    doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, "semi session");
      return pInfo->pDelRes;
    }

    if (pInfo->pUpdateRes->info.rows > 0 && pInfo->returnUpdate) {
      pInfo->returnUpdate = false;
      // process the rest of the data
      printDataBlock(pInfo->pUpdateRes, "semi session");
      return pInfo->pUpdateRes;
    }

    if (pOperator->status == OP_RES_TO_RETURN) {
      clearFunctionContext(&pOperator->exprSupp);
      // semi interval operator clear disk buffer
      clearStreamSessionOperator(pInfo);
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }
  }

  _hash_fn_t     hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*      pStUpdated = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SArray*        pUpdated = taosArrayInit(16, POINTER_BYTES);
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      clearSpecialDataBlock(pInfo->pUpdateRes);
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }
    printDataBlock(pBlock, "semi session recv");

    if (pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doClearSessionWindows(&pInfo->streamAggSup, pSup, pBlock, START_TS_COLUMN_INDEX, pSup->numOfExprs, 0, pWins);
      removeSessionResults(pStUpdated, pWins);
      taosArrayDestroy(pWins);
      copyDataBlock(pInfo->pUpdateRes, pBlock);
      pInfo->returnUpdate = true;
      break;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      // gap must be 0
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, 0, pWins, NULL);
      copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
      removeSessionResults(pStUpdated, pWins);
      taosArrayDestroy(pWins);
      break;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pUpdated, getResWinForSession);
      continue;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamSessionAggImpl(pOperator, pBlock, pStUpdated, NULL, false);
    maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pBInfo->pRes->info.watermark = pInfo->twAggSup.maxTs;

  copyUpdateResult(pStUpdated, pUpdated);
  removeSessionDeleteResults(pUpdated, pInfo->pStDeleted);
  taosHashCleanup(pStUpdated);

  finalizeUpdatedResult(pOperator->exprSupp.numOfExprs, pInfo->streamAggSup.pResultBuf, pUpdated,
                        pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);

  doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, "semi session");
    return pBInfo->pRes;
  }

  doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, "semi session");
    return pInfo->pDelRes;
  }

  if (pInfo->pUpdateRes->info.rows > 0 && pInfo->returnUpdate) {
    pInfo->returnUpdate = false;
    // process the rest of the data
    printDataBlock(pInfo->pUpdateRes, "semi session");
    return pInfo->pUpdateRes;
  }

  pOperator->status = OP_EXEC_DONE;
  return NULL;
}

SOperatorInfo* createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                       SExecTaskInfo* pTaskInfo, int32_t numOfChild) {
  int32_t        code = TSDB_CODE_OUT_OF_MEMORY;
  SOperatorInfo* pOperator = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo);
  if (pOperator == NULL) {
    goto _error;
  }
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;

  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
    pInfo->isFinal = true;
    pOperator->name = "StreamSessionFinalAggOperator";
  } else {
    pInfo->isFinal = false;
    pInfo->pUpdateRes = createSpecialDataBlock(STREAM_CLEAR);
    blockDataEnsureCapacity(pInfo->pUpdateRes, 128);
    pOperator->name = "StreamSessionSemiAggOperator";
    pOperator->fpSet =
        createOperatorFpSet(operatorDummyOpenFn, doStreamSessionSemiAgg, NULL, NULL,
                            destroyStreamSessionAggOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);
  }
  pOperator->operatorType = pPhyNode->type;
  if (numOfChild > 0) {
    pInfo->pChildren = taosArrayInit(numOfChild, sizeof(void*));
    for (int32_t i = 0; i < numOfChild; i++) {
      SOperatorInfo* pChild = createStreamFinalSessionAggOperatorInfo(NULL, pPhyNode, pTaskInfo, 0);
      if (pChild == NULL) {
        goto _error;
      }
      taosArrayPush(pInfo->pChildren, &pChild);
    }
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyStreamStateOperatorInfo(void* param) {
  SStreamStateAggOperatorInfo* pInfo = (SStreamStateAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStateStreamAggSupporter(&pInfo->streamAggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, i);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      destroyStreamSessionAggOperatorInfo(pChInfo);
      taosMemoryFreeClear(pChild);
      taosMemoryFreeClear(pChInfo);
    }
  }
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  taosHashCleanup(pInfo->pSeDeleted);
  destroySqlFunctionCtx(pInfo->pDummyCtx, 0);

  taosMemoryFreeClear(param);
}

int64_t getStateWinTsKey(void* data, int32_t index) {
  SStateWindowInfo* pStateWin = taosArrayGet(data, index);
  return pStateWin->winInfo.win.ekey;
}

SStateWindowInfo* addNewStateWindow(SArray* pWinInfos, TSKEY ts, char* pKeyData, SColumn* pCol) {
  SStateWindowInfo win = {
      .stateKey.bytes = pCol->bytes,
      .stateKey.type = pCol->type,
      .stateKey.pData = taosMemoryCalloc(1, pCol->bytes),
      .winInfo.pos.offset = -1,
      .winInfo.pos.pageId = -1,
      .winInfo.win.skey = ts,
      .winInfo.win.ekey = ts,
      .winInfo.isOutput = false,
      .winInfo.isClosed = false,
  };
  if (IS_VAR_DATA_TYPE(win.stateKey.type)) {
    varDataCopy(win.stateKey.pData, pKeyData);
  } else {
    memcpy(win.stateKey.pData, pKeyData, win.stateKey.bytes);
  }
  return taosArrayPush(pWinInfos, &win);
}

SStateWindowInfo* insertNewStateWindow(SArray* pWinInfos, TSKEY ts, char* pKeyData, int32_t index, SColumn* pCol) {
  SStateWindowInfo win = {
      .stateKey.bytes = pCol->bytes,
      .stateKey.type = pCol->type,
      .stateKey.pData = taosMemoryCalloc(1, pCol->bytes),
      .winInfo.pos.offset = -1,
      .winInfo.pos.pageId = -1,
      .winInfo.win.skey = ts,
      .winInfo.win.ekey = ts,
      .winInfo.isOutput = false,
      .winInfo.isClosed = false,
  };
  if (IS_VAR_DATA_TYPE(win.stateKey.type)) {
    varDataCopy(win.stateKey.pData, pKeyData);
  } else {
    memcpy(win.stateKey.pData, pKeyData, win.stateKey.bytes);
  }
  return taosArrayInsert(pWinInfos, index, &win);
}

bool isTsInWindow(SStateWindowInfo* pWin, TSKEY ts) {
  if (pWin->winInfo.win.skey <= ts && ts <= pWin->winInfo.win.ekey) {
    return true;
  }
  return false;
}

bool isEqualStateKey(SStateWindowInfo* pWin, char* pKeyData) {
  return pKeyData && compareVal(pKeyData, &pWin->stateKey);
}

SStateWindowInfo* getStateWindowByTs(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, int32_t* pIndex) {
  SArray* pWinInfos = getWinInfos(pAggSup, groupId);
  pAggSup->pCurWins = pWinInfos;
  int32_t           size = taosArrayGetSize(pWinInfos);
  int32_t           index = binarySearch(pWinInfos, size, ts, TSDB_ORDER_DESC, getStateWinTsKey);
  SStateWindowInfo* pWin = NULL;
  if (index >= 0) {
    pWin = taosArrayGet(pWinInfos, index);
    if (isTsInWindow(pWin, ts)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index + 1 < size) {
    pWin = taosArrayGet(pWinInfos, index + 1);
    if (isTsInWindow(pWin, ts)) {
      *pIndex = index + 1;
      return pWin;
    }
  }
  *pIndex = 0;
  return NULL;
}

SStateWindowInfo* getStateWindow(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, char* pKeyData,
                                 SColumn* pCol, int32_t* pIndex) {
  SArray* pWinInfos = getWinInfos(pAggSup, groupId);
  pAggSup->pCurWins = pWinInfos;
  int32_t size = taosArrayGetSize(pWinInfos);
  if (size == 0) {
    *pIndex = 0;
    return addNewStateWindow(pWinInfos, ts, pKeyData, pCol);
  }
  int32_t           index = binarySearch(pWinInfos, size, ts, TSDB_ORDER_DESC, getStateWinTsKey);
  SStateWindowInfo* pWin = NULL;
  if (index >= 0) {
    pWin = taosArrayGet(pWinInfos, index);
    if (isTsInWindow(pWin, ts)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index + 1 < size) {
    pWin = taosArrayGet(pWinInfos, index + 1);
    if (isTsInWindow(pWin, ts) || isEqualStateKey(pWin, pKeyData)) {
      *pIndex = index + 1;
      return pWin;
    }
  }

  if (index >= 0) {
    pWin = taosArrayGet(pWinInfos, index);
    if (isEqualStateKey(pWin, pKeyData)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index == size - 1) {
    *pIndex = taosArrayGetSize(pWinInfos);
    return addNewStateWindow(pWinInfos, ts, pKeyData, pCol);
  }
  *pIndex = index + 1;
  return insertNewStateWindow(pWinInfos, ts, pKeyData, index + 1, pCol);
}

int32_t updateStateWindowInfo(SArray* pWinInfos, int32_t winIndex, TSKEY* pTs, uint64_t groupId,
                              SColumnInfoData* pKeyCol, int32_t rows, int32_t start, bool* allEqual,
                              SHashObj* pSeDeleted) {
  *allEqual = true;
  SStateWindowInfo* pWinInfo = taosArrayGet(pWinInfos, winIndex);
  for (int32_t i = start; i < rows; ++i) {
    char* pKeyData = colDataGetData(pKeyCol, i);
    if (!isTsInWindow(pWinInfo, pTs[i])) {
      if (isEqualStateKey(pWinInfo, pKeyData)) {
        int32_t size = taosArrayGetSize(pWinInfos);
        if (winIndex + 1 < size) {
          SStateWindowInfo* pNextWin = taosArrayGet(pWinInfos, winIndex + 1);
          // ts belongs to the next window
          if (pTs[i] >= pNextWin->winInfo.win.skey) {
            return i - start;
          }
        }
      } else {
        return i - start;
      }
    }
    if (pWinInfo->winInfo.win.skey > pTs[i]) {
      if (pSeDeleted && pWinInfo->winInfo.isOutput) {
        SWinKey res = {.ts = pWinInfo->winInfo.win.skey, .groupId = groupId};
        taosHashPut(pSeDeleted, &res, sizeof(SWinKey), &res, sizeof(SWinKey));
        pWinInfo->winInfo.isOutput = false;
      }
      pWinInfo->winInfo.win.skey = pTs[i];
    }
    pWinInfo->winInfo.win.ekey = TMAX(pWinInfo->winInfo.win.ekey, pTs[i]);
    if (!isEqualStateKey(pWinInfo, pKeyData)) {
      *allEqual = false;
    }
  }
  return rows - start;
}

static void doClearStateWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SHashObj* pSeUpdated,
                                SHashObj* pSeDeleted) {
  SColumnInfoData* pTsColInfo = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pGroupColInfo = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  TSKEY*           tsCol = (TSKEY*)pTsColInfo->pData;
  bool             allEqual = false;
  int32_t          step = 1;
  uint64_t*        gpCol = (uint64_t*)pGroupColInfo->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i += step) {
    int32_t           winIndex = 0;
    SStateWindowInfo* pCurWin = getStateWindowByTs(pAggSup, tsCol[i], gpCol[i], &winIndex);
    if (!pCurWin) {
      continue;
    }
    updateSessionWindowInfo(&pCurWin->winInfo, tsCol, NULL, 0, pBlock->info.rows, i, 0, NULL);
    taosHashRemove(pSeUpdated, &pCurWin->winInfo.pos, sizeof(SResultRowPosition));
    deleteWindow(pAggSup->pCurWins, winIndex, destroyStateWinInfo);
  }
}

static void doStreamStateAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SHashObj* pSeUpdated,
                                 SHashObj* pStDeleted) {
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  bool                         masterScan = true;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  int64_t                      groupId = pSDataBlock->info.groupId;
  int64_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      step = 1;
  bool                         ascScan = true;
  TSKEY*                       tsCols = NULL;
  SResultRow*                  pResult = NULL;
  int32_t                      winRows = 0;
  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return;
  }

  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;
  blockDataEnsureCapacity(pAggSup->pScanBlock, pSDataBlock->info.rows);
  SColumnInfoData* pKeyColInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->stateCol.slotId);
  for (int32_t i = 0; i < pSDataBlock->info.rows; i += winRows) {
    if (pInfo->ignoreExpiredData && isOverdue(tsCols[i], &pInfo->twAggSup)) {
      i++;
      continue;
    }
    char*             pKeyData = colDataGetData(pKeyColInfo, i);
    int32_t           winIndex = 0;
    bool              allEqual = true;
    SStateWindowInfo* pCurWin = getStateWindow(pAggSup, tsCols[i], groupId, pKeyData, &pInfo->stateCol, &winIndex);
    winRows = updateStateWindowInfo(pAggSup->pCurWins, winIndex, tsCols, groupId, pKeyColInfo, pSDataBlock->info.rows,
                                    i, &allEqual, pStDeleted);
    if (!allEqual) {
      uint64_t uid = 0;
      appendOneRow(pAggSup->pScanBlock, &pCurWin->winInfo.win.skey, &pCurWin->winInfo.win.ekey, &uid, &groupId);
      taosHashRemove(pSeUpdated, &pCurWin->winInfo.pos, sizeof(SResultRowPosition));
      deleteWindow(pAggSup->pCurWins, winIndex, destroyStateWinInfo);
      continue;
    }
    code = doOneStateWindowAgg(pInfo, pSDataBlock, &pCurWin->winInfo, &pResult, i, winRows, numOfOutput, pOperator);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    pCurWin->winInfo.isClosed = false;
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      SWinKey value = {.ts = pCurWin->winInfo.win.skey, .groupId = groupId};
      code = taosHashPut(pSeUpdated, &pCurWin->winInfo.pos, sizeof(SResultRowPosition), &value, sizeof(SWinKey));
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      pCurWin->winInfo.isOutput = true;
    }
  }
}

static SSDataBlock* doStreamStateAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExprSupp*                   pSup = &pOperator->exprSupp;
  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  int64_t                      maxTs = INT64_MIN;
  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteDataBlock(pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, "single state");
      return pInfo->pDelRes;
    }
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
    if (pBInfo->pRes->info.rows == 0 || !hasRemainResults(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }
    printDataBlock(pBInfo->pRes, "single state");
    return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
  }

  _hash_fn_t     hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*      pSeUpdated = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SArray*        pUpdated = taosArrayInit(16, POINTER_BYTES);
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, "single state recv");

    if (pBlock->info.type == STREAM_CLEAR) {
      doClearStateWindows(&pInfo->streamAggSup, pBlock, pSeUpdated, pInfo->pSeDeleted);
      continue;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, 0, pWins, destroyStateWinInfo);
      copyDeleteWindowInfo(pWins, pInfo->pSeDeleted);
      removeSessionResults(pSeUpdated, pWins);
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pUpdated, getResWinForState);
      continue;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamStateAggImpl(pOperator, pBlock, pSeUpdated, pInfo->pSeDeleted);
    maxTs = TMAX(maxTs, pBlock->info.window.ekey);
  }
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pUpdated, getResWinForState,
                     pInfo->ignoreExpiredData, destroyStateWinInfo);
  // closeChildSessionWindow(pInfo->pChildren, pInfo->twAggSup.maxTs, pInfo->ignoreExpiredData, destroyStateWinInfo);
  copyUpdateResult(pSeUpdated, pUpdated);
  taosHashCleanup(pSeUpdated);

  finalizeUpdatedResult(pOperator->exprSupp.numOfExprs, pInfo->streamAggSup.pResultBuf, pUpdated,
                        pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  doBuildDeleteDataBlock(pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, "single state");
    return pInfo->pDelRes;
  }
  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
  printDataBlock(pBInfo->pRes, "single state");
  return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
}

int32_t initStateAggSupporter(SStreamAggSupporter* pSup, const char* pKey, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  return initStreamAggSupporter(pSup, pKey, pCtx, numOfOutput, sizeof(SStateWindowInfo));
}

SOperatorInfo* createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo) {
  SStreamStateWinodwPhysiNode* pStateNode = (SStreamStateWinodwPhysiNode*)pPhyNode;
  SSDataBlock*                 pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
  int32_t                      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;
  SColumnNode*                 pColNode = (SColumnNode*)((STargetNode*)pStateNode->pStateKey)->pExpr;
  int32_t                      code = TSDB_CODE_OUT_OF_MEMORY;

  SStreamStateAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamStateAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SExprSupp* pSup = &pOperator->exprSupp;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &numOfCols);

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pStateNode->window.pExprs, NULL, &numOfScalar);
    int32_t    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pStateNode->window.watermark,
      .calTrigger = pStateNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
  };
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  code = initBasicInfoEx(&pInfo->binfo, pSup, pExprInfo, numOfCols, pResBlock);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initStateAggSupporter(&pInfo->streamAggSup, "StreamStateAggOperatorInfo", pSup->pCtx, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pDummyCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfCols, sizeof(SqlFunctionCtx));
  if (pInfo->pDummyCtx == NULL) {
    goto _error;
  }

  initDummyFunction(pInfo->pDummyCtx, pSup->pCtx, numOfCols);
  pInfo->primaryTsIndex = tsSlotId;
  pInfo->order = TSDB_ORDER_ASC;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pSeDeleted = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pChildren = NULL;
  pInfo->ignoreExpiredData = pStateNode->window.igExpired;

  pOperator->name = "StreamStateAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->info = pInfo;
  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doStreamStateAgg, NULL, NULL,
                                         destroyStreamStateOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);
  initDownStream(downstream, &pInfo->streamAggSup, 0, pInfo->twAggSup.waterMark, pOperator->operatorType,
                 pInfo->primaryTsIndex);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  destroyStreamStateOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyMAIOperatorInfo(void* param) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = (SMergeAlignedIntervalAggOperatorInfo*)param;
  destroyIntervalOperatorInfo(miaInfo->intervalAggOperatorInfo);
  taosMemoryFreeClear(param);
}

static SResultRow* doSetSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, SAggSupporter* pSup) {
  SResultRow* pResult = getNewResultRow(pSup->pResultBuf, &pSup->currentPageId, pSup->resultRowSize);
  pResultRowInfo->cur = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};
  return pResult;
}

static int32_t setSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, SResultRow** pResult,
                                       SExprSupp* pExprSup, SAggSupporter* pAggSup) {
  if (*pResult == NULL) {
    *pResult = doSetSingleOutputTupleBuf(pResultRowInfo, pAggSup);
    if (*pResult == NULL) {
      return terrno;
    }
  }

  // set time window for current result
  (*pResult)->win = (*win);
  setResultRowInitCtx((*pResult), pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static void doMergeAlignedIntervalAggImpl(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo,
                                          SSDataBlock* pBlock, SSDataBlock* pResultBlock) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*             iaInfo = miaInfo->intervalAggOperatorInfo;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;
  SInterval*     pInterval = &iaInfo->interval;

  int32_t  startPos = 0;
  int64_t* tsCols = extractTsCol(pBlock, iaInfo);

  TSKEY ts = getStartTsKey(&pBlock->info.window, tsCols);

  // there is an result exists
  if (miaInfo->curTs != INT64_MIN) {
    if (ts != miaInfo->curTs) {
      finalizeResultRows(iaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pTaskInfo);
      resetResultRow(miaInfo->pResultRow, iaInfo->aggSup.resultRowSize - sizeof(SResultRow));
      miaInfo->curTs = ts;
    }
  } else {
    miaInfo->curTs = ts;
  }

  STimeWindow win = {0};
  win.skey = miaInfo->curTs;
  win.ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;

  int32_t ret = setSingleOutputTupleBuf(pResultRowInfo, &win, &miaInfo->pResultRow, pSup, &iaInfo->aggSup);
  if (ret != TSDB_CODE_SUCCESS || miaInfo->pResultRow == NULL) {
    T_LONG_JMP(pTaskInfo->env, ret);
  }

  int32_t currPos = startPos;

  STimeWindow currWin = win;
  while (++currPos < pBlock->info.rows) {
    if (tsCols[currPos] == miaInfo->curTs) {
      continue;
    }

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, currPos - startPos,
                     pBlock->info.rows, pSup->numOfExprs);

    finalizeResultRows(iaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pResultBlock, pTaskInfo);
    resetResultRow(miaInfo->pResultRow, iaInfo->aggSup.resultRowSize - sizeof(SResultRow));
    miaInfo->curTs = tsCols[currPos];

    currWin.skey = miaInfo->curTs;
    currWin.ekey = taosTimeAdd(currWin.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;

    startPos = currPos;
    ret = setSingleOutputTupleBuf(pResultRowInfo, &win, &miaInfo->pResultRow, pSup, &iaInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS || miaInfo->pResultRow == NULL) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    miaInfo->curTs = currWin.skey;
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, true);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, currPos - startPos,
                   pBlock->info.rows, pSup->numOfExprs);
}

static void cleanupAfterGroupResultGen(SMergeAlignedIntervalAggOperatorInfo* pMiaInfo, SSDataBlock* pRes) {
  pRes->info.groupId = pMiaInfo->groupId;
  pMiaInfo->curTs = INT64_MIN;
  pMiaInfo->groupId = 0;
}

static void doMergeAlignedIntervalAgg(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             pIaInfo = pMiaInfo->intervalAggOperatorInfo;

  SExprSupp*      pSup = &pOperator->exprSupp;
  SSDataBlock*    pRes = pIaInfo->binfo.pRes;
  SResultRowInfo* pResultRowInfo = &pIaInfo->binfo.resultRowInfo;
  SOperatorInfo*  downstream = pOperator->pDownstream[0];
  int32_t         scanFlag = MAIN_SCAN;

  while (1) {
    SSDataBlock* pBlock = NULL;
    if (pMiaInfo->prefetchedBlock == NULL) {
      pBlock = downstream->fpSet.getNextFn(downstream);
    } else {
      pBlock = pMiaInfo->prefetchedBlock;
      pMiaInfo->prefetchedBlock = NULL;

      pMiaInfo->groupId = pBlock->info.groupId;
    }

    // no data exists, all query processing is done
    if (pBlock == NULL) {
      // close last unclosed time window
      if (pMiaInfo->curTs != INT64_MIN) {
        finalizeResultRows(pIaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMiaInfo->pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));
        cleanupAfterGroupResultGen(pMiaInfo, pRes);
      }

      doSetOperatorCompleted(pOperator);
      break;
    }

    if (pMiaInfo->groupId == 0) {
      if (pMiaInfo->groupId != pBlock->info.groupId) {
        pMiaInfo->groupId = pBlock->info.groupId;
      }
    } else {
      if (pMiaInfo->groupId != pBlock->info.groupId) {
        // if there are unclosed time window, close it firstly.
        ASSERT(pMiaInfo->curTs != INT64_MIN);
        finalizeResultRows(pIaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMiaInfo->pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));

        pMiaInfo->prefetchedBlock = pBlock;
        cleanupAfterGroupResultGen(pMiaInfo, pRes);
        break;
      } else {
        // continue
      }
    }

    getTableScanInfo(pOperator, &pIaInfo->inputOrder, &scanFlag);
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, pIaInfo->inputOrder, scanFlag, true);
    doMergeAlignedIntervalAggImpl(pOperator, &pIaInfo->binfo.resultRowInfo, pBlock, pRes);

    doFilter(pMiaInfo->pCondition, pRes, NULL);
    if (pRes->info.rows >= pOperator->resultInfo.capacity) {
      break;
    }
  }
}

static SSDataBlock* mergeAlignedIntervalAgg(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SMergeAlignedIntervalAggOperatorInfo* pMiaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             iaInfo = pMiaInfo->intervalAggOperatorInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSDataBlock* pRes = iaInfo->binfo.pRes;
  blockDataCleanup(pRes);

  if (iaInfo->binfo.mergeResultBlock) {
    while (1) {
      if (pOperator->status == OP_EXEC_DONE) {
        break;
      }

      if (pRes->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }

      doMergeAlignedIntervalAgg(pOperator);
    }
  } else {
    doMergeAlignedIntervalAgg(pOperator);
  }

  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;
  return (rows == 0) ? NULL : pRes;
}

SOperatorInfo* createMergeAlignedIntervalOperatorInfo(SOperatorInfo* downstream, SMergeAlignedIntervalPhysiNode* pNode,
                                                      SExecTaskInfo* pTaskInfo) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = taosMemoryCalloc(1, sizeof(SMergeAlignedIntervalAggOperatorInfo));
  SOperatorInfo*                        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (miaInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  miaInfo->intervalAggOperatorInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  if (miaInfo->intervalAggOperatorInfo == NULL) {
    goto _error;
  }

  int32_t      num = 0;
  SExprInfo*   pExprInfo = createExprInfo(pNode->window.pFuncs, NULL, &num);
  SSDataBlock* pResBlock = createResDataBlock(pNode->window.node.pOutputDataBlockDesc);

  SInterval interval = {.interval = pNode->interval,
                        .sliding = pNode->sliding,
                        .intervalUnit = pNode->intervalUnit,
                        .slidingUnit = pNode->slidingUnit,
                        .offset = pNode->offset,
                        .precision = ((SColumnNode*)pNode->window.pTspk)->node.resType.precision};

  SIntervalAggOperatorInfo* iaInfo = miaInfo->intervalAggOperatorInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;

  miaInfo->pCondition = pNode->window.node.pConditions;
  miaInfo->curTs = INT64_MIN;
  iaInfo->win = pTaskInfo->window;
  iaInfo->inputOrder = TSDB_ORDER_ASC;
  iaInfo->interval = interval;
  iaInfo->execModel = pTaskInfo->execModel;
  iaInfo->primaryTsIndex = ((SColumnNode*)pNode->window.pTspk)->slotId;
  iaInfo->binfo.mergeResultBlock = pNode->window.mergeDataBlock;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t code = initAggInfo(&pOperator->exprSupp, &iaInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&iaInfo->binfo, pResBlock);
  initExecTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &iaInfo->win);

  iaInfo->timeWindowInterpo = timeWindowinterpNeeded(pSup->pCtx, num, iaInfo);
  if (iaInfo->timeWindowInterpo) {
    iaInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
  }

  initResultRowInfo(&iaInfo->binfo.resultRowInfo);
  blockDataEnsureCapacity(iaInfo->binfo.pRes, pOperator->resultInfo.capacity);

  pOperator->name = "TimeMergeAlignedIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->info = miaInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, mergeAlignedIntervalAgg, NULL, NULL,
                                         destroyMAIOperatorInfo, NULL, NULL, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyMAIOperatorInfo(miaInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

//=====================================================================================================================
// merge interval operator
typedef struct SMergeIntervalAggOperatorInfo {
  SIntervalAggOperatorInfo intervalAggOperatorInfo;
  SList*                   groupIntervals;
  SListIter                groupIntervalsIter;
  bool                     hasGroupId;
  uint64_t                 groupId;
  SSDataBlock*             prefetchedBlock;
  bool                     inputBlocksFinished;
} SMergeIntervalAggOperatorInfo;

typedef struct SGroupTimeWindow {
  uint64_t    groupId;
  STimeWindow window;
} SGroupTimeWindow;

void destroyMergeIntervalOperatorInfo(void* param) {
  SMergeIntervalAggOperatorInfo* miaInfo = (SMergeIntervalAggOperatorInfo*)param;
  tdListFree(miaInfo->groupIntervals);
  destroyIntervalOperatorInfo(&miaInfo->intervalAggOperatorInfo);

  taosMemoryFreeClear(param);
}

static int32_t finalizeWindowResult(SOperatorInfo* pOperatorInfo, uint64_t tableGroupId, STimeWindow* win,
                                    SSDataBlock* pResultBlock) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  SExecTaskInfo*                 pTaskInfo = pOperatorInfo->pTaskInfo;
  bool                           ascScan = (iaInfo->inputOrder == TSDB_ORDER_ASC);
  SExprSupp*                     pExprSup = &pOperatorInfo->exprSupp;

  SET_RES_WINDOW_KEY(iaInfo->aggSup.keyBuf, &win->skey, TSDB_KEYSIZE, tableGroupId);
  SResultRowPosition* p1 = (SResultRowPosition*)tSimpleHashGet(
      iaInfo->aggSup.pResultRowHashTable, iaInfo->aggSup.keyBuf, GET_RES_WINDOW_KEY_LEN(TSDB_KEYSIZE));
  ASSERT(p1 != NULL);
  //  finalizeResultRows(iaInfo->aggSup.pResultBuf, p1, pResultBlock, pTaskInfo);
  tSimpleHashRemove(iaInfo->aggSup.pResultRowHashTable, iaInfo->aggSup.keyBuf, GET_RES_WINDOW_KEY_LEN(TSDB_KEYSIZE));
  return TSDB_CODE_SUCCESS;
}

static int32_t outputPrevIntervalResult(SOperatorInfo* pOperatorInfo, uint64_t tableGroupId, SSDataBlock* pResultBlock,
                                        STimeWindow* newWin) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  bool                           ascScan = (iaInfo->inputOrder == TSDB_ORDER_ASC);

  SGroupTimeWindow groupTimeWindow = {.groupId = tableGroupId, .window = *newWin};
  tdListAppend(miaInfo->groupIntervals, &groupTimeWindow);

  SListIter iter = {0};
  tdListInitIter(miaInfo->groupIntervals, &iter, TD_LIST_FORWARD);
  SListNode* listNode = NULL;
  while ((listNode = tdListNext(&iter)) != NULL) {
    SGroupTimeWindow* prevGrpWin = (SGroupTimeWindow*)listNode->data;
    if (prevGrpWin->groupId != tableGroupId) {
      continue;
    }

    STimeWindow* prevWin = &prevGrpWin->window;
    if ((ascScan && newWin->skey > prevWin->ekey) || ((!ascScan) && newWin->skey < prevWin->ekey)) {
      //      finalizeWindowResult(pOperatorInfo, tableGroupId, prevWin, pResultBlock);
      tdListPopNode(miaInfo->groupIntervals, listNode);
    }
  }

  return 0;
}

static void doMergeIntervalAggImpl(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                                   int32_t scanFlag, SSDataBlock* pResultBlock) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pExprSup = &pOperatorInfo->exprSupp;

  int32_t     startPos = 0;
  int32_t     numOfOutput = pExprSup->numOfExprs;
  int64_t*    tsCols = extractTsCol(pBlock, iaInfo);
  uint64_t    tableGroupId = pBlock->info.groupId;
  bool        ascScan = (iaInfo->inputOrder == TSDB_ORDER_ASC);
  TSKEY       blockStartTs = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow* pResult = NULL;

  STimeWindow win = getActiveTimeWindow(iaInfo->aggSup.pResultBuf, pResultRowInfo, blockStartTs, &iaInfo->interval,
                                        iaInfo->inputOrder);

  int32_t ret =
      setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                             numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, iaInfo->inputOrder);
  ASSERT(forwardRows > 0);

  // prev time window not interpolation yet.
  if (iaInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult, tableGroupId);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                                 numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    // window start key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &win, startPos, forwardRows, pExprSup);
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &win, true);
  doApplyFunctions(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows,
                   pBlock->info.rows, numOfOutput);
  doCloseWindow(pResultRowInfo, iaInfo, pResult);

  // output previous interval results after this interval (&win) is closed
  outputPrevIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, &win);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos =
        getNextQualifiedWindow(&iaInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, iaInfo->inputOrder);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    int32_t code =
        setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                               pExprSup->pCtx, numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, iaInfo->inputOrder);

    // window start(end) key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pExprSup);

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows,
                     pBlock->info.rows, numOfOutput);
    doCloseWindow(pResultRowInfo, iaInfo, pResult);

    // output previous interval results after this interval (&nextWin) is closed
    outputPrevIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, &nextWin);
  }

  if (iaInfo->timeWindowInterpo) {
    saveDataBlockLastRow(iaInfo->pPrevValues, pBlock, iaInfo->pInterpCols);
  }
}

static SSDataBlock* doMergeIntervalAgg(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SMergeIntervalAggOperatorInfo* miaInfo = pOperator->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  SExprSupp*                     pExpSupp = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSDataBlock* pRes = iaInfo->binfo.pRes;
  blockDataCleanup(pRes);
  blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);

  if (!miaInfo->inputBlocksFinished) {
    SOperatorInfo* downstream = pOperator->pDownstream[0];
    int32_t        scanFlag = MAIN_SCAN;
    while (1) {
      SSDataBlock* pBlock = NULL;
      if (miaInfo->prefetchedBlock == NULL) {
        pBlock = downstream->fpSet.getNextFn(downstream);
      } else {
        pBlock = miaInfo->prefetchedBlock;
        miaInfo->groupId = pBlock->info.groupId;
        miaInfo->prefetchedBlock = NULL;
      }

      if (pBlock == NULL) {
        tdListInitIter(miaInfo->groupIntervals, &miaInfo->groupIntervalsIter, TD_LIST_FORWARD);
        miaInfo->inputBlocksFinished = true;
        break;
      }

      if (!miaInfo->hasGroupId) {
        miaInfo->hasGroupId = true;
        miaInfo->groupId = pBlock->info.groupId;
      } else if (miaInfo->groupId != pBlock->info.groupId) {
        miaInfo->prefetchedBlock = pBlock;
        break;
      }

      getTableScanInfo(pOperator, &iaInfo->inputOrder, &scanFlag);
      setInputDataBlock(pOperator, pExpSupp->pCtx, pBlock, iaInfo->inputOrder, scanFlag, true);
      doMergeIntervalAggImpl(pOperator, &iaInfo->binfo.resultRowInfo, pBlock, scanFlag, pRes);

      if (pRes->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }
    }

    pRes->info.groupId = miaInfo->groupId;
  }

  if (miaInfo->inputBlocksFinished) {
    SListNode* listNode = tdListNext(&miaInfo->groupIntervalsIter);

    if (listNode != NULL) {
      SGroupTimeWindow* grpWin = (SGroupTimeWindow*)(listNode->data);
      //      finalizeWindowResult(pOperator, grpWin->groupId, &grpWin->window, pRes);
      pRes->info.groupId = grpWin->groupId;
    }
  }

  if (pRes->info.rows == 0) {
    doSetOperatorCompleted(pOperator);
  }

  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;
  return (rows == 0) ? NULL : pRes;
}

SOperatorInfo* createMergeIntervalOperatorInfo(SOperatorInfo* downstream, SMergeIntervalPhysiNode* pIntervalPhyNode,
                                               SExecTaskInfo* pTaskInfo) {
  SMergeIntervalAggOperatorInfo* pMergeIntervalInfo = taosMemoryCalloc(1, sizeof(SMergeIntervalAggOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pMergeIntervalInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t      num = 0;
  SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &num);
  SSDataBlock* pResBlock = createResDataBlock(pIntervalPhyNode->window.node.pOutputDataBlockDesc);

  SInterval interval = {.interval = pIntervalPhyNode->interval,
                        .sliding = pIntervalPhyNode->sliding,
                        .intervalUnit = pIntervalPhyNode->intervalUnit,
                        .slidingUnit = pIntervalPhyNode->slidingUnit,
                        .offset = pIntervalPhyNode->offset,
                        .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};

  pMergeIntervalInfo->groupIntervals = tdListNew(sizeof(SGroupTimeWindow));

  SIntervalAggOperatorInfo* pIntervalInfo = &pMergeIntervalInfo->intervalAggOperatorInfo;
  pIntervalInfo->win = pTaskInfo->window;
  pIntervalInfo->inputOrder = TSDB_ORDER_ASC;
  pIntervalInfo->interval = interval;
  pIntervalInfo->execModel = pTaskInfo->execModel;
  pIntervalInfo->binfo.mergeResultBlock = pIntervalPhyNode->window.mergeDataBlock;
  pIntervalInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;

  SExprSupp* pExprSupp = &pOperator->exprSupp;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t code = initAggInfo(pExprSupp, &pIntervalInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pIntervalInfo->binfo, pResBlock);
  initExecTimeWindowInfo(&pIntervalInfo->twAggSup.timeWindowData, &pIntervalInfo->win);

  pIntervalInfo->timeWindowInterpo = timeWindowinterpNeeded(pExprSupp->pCtx, num, pIntervalInfo);
  if (pIntervalInfo->timeWindowInterpo) {
    pIntervalInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
    if (pIntervalInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }

  initResultRowInfo(&pIntervalInfo->binfo.resultRowInfo);

  pOperator->name = "TimeMergeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->info = pMergeIntervalInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doMergeIntervalAgg, NULL, NULL,
                                         destroyMergeIntervalOperatorInfo, NULL, NULL, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyMergeIntervalOperatorInfo(pMergeIntervalInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static void doStreamIntervalAggImpl(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                                    int32_t scanFlag, SHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;

  int32_t          startPos = 0;
  int32_t          numOfOutput = pSup->numOfExprs;
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*           tsCols = (TSKEY*)pColDataInfo->pData;
  uint64_t         tableGroupId = pBlock->info.groupId;
  bool             ascScan = true;
  TSKEY            ts = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow*      pResult = NULL;

  STimeWindow win = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, TSDB_ORDER_ASC);
  int32_t     ret = TSDB_CODE_SUCCESS;
  if ((!pInfo->ignoreExpiredData || !isCloseWindow(&win, &pInfo->twAggSup)) &&
      inSlidingWindow(&pInfo->interval, &win, &pBlock->info)) {
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                 numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      saveWinResultRow(pResult, tableGroupId, pUpdatedMap);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pResultRowInfo->cur);
    }
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
  ASSERT(forwardRows > 0);

  if ((!pInfo->ignoreExpiredData || !isCloseWindow(&win, &pInfo->twAggSup)) &&
      inSlidingWindow(&pInfo->interval, &win, &pBlock->info)) {
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, pBlock->info.rows,
                     numOfOutput);
  }

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, TSDB_ORDER_ASC);
    if (startPos < 0) {
      break;
    }
    if (pInfo->ignoreExpiredData && isCloseWindow(&nextWin, &pInfo->twAggSup)) {
      ekey = ascScan ? nextWin.ekey : nextWin.skey;
      forwardRows =
          getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
      continue;
    }

    // null data, failed to allocate more memory buffer
    int32_t code = setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                          pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      saveWinResultRow(pResult, tableGroupId, pUpdatedMap);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pResultRowInfo->cur);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, pBlock->info.rows,
                     numOfOutput);
  }
}

static void doStreamIntervalAggImpl2(SOperatorInfo* pOperatorInfo, SSDataBlock* pSDataBlock, uint64_t tableGroupId,
                                     SHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)pOperatorInfo->info;

  SResultRowInfo* pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*  pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*      pSup = &pOperatorInfo->exprSupp;
  int32_t         numOfOutput = pSup->numOfExprs;
  int32_t         step = 1;
  TSKEY*          tsCols = NULL;
  SResultRow*     pResult = NULL;
  int32_t         forwardRows = 0;
  int32_t         aa = 4;

  ASSERT(pSDataBlock->pDataBlock != NULL);
  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  tsCols = (int64_t*)pColDataInfo->pData;

  int32_t     startPos = 0;
  TSKEY       ts = getStartTsKey(&pSDataBlock->info.window, tsCols);
  STimeWindow nextWin =
      getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, TSDB_ORDER_ASC);
  while (1) {
    bool isClosed = isCloseWindow(&nextWin, &pInfo->twAggSup);
    if ((pInfo->ignoreExpiredData && isClosed) || !inSlidingWindow(&pInfo->interval, &nextWin, &pSDataBlock->info)) {
      startPos = getNexWindowPos(&pInfo->interval, &pSDataBlock->info, tsCols, startPos, nextWin.ekey, &nextWin);
      if (startPos < 0) {
        break;
      }
      continue;
    }

    int32_t code = setOutputBuf(&nextWin, &pResult, tableGroupId, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset,
                                &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    forwardRows = getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, nextWin.ekey, binarySearchForKey, NULL,
                                           TSDB_ORDER_ASC);
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pUpdatedMap) {
      saveWinResultInfo(pResult->win.skey, tableGroupId, pUpdatedMap);
    }
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                     pSDataBlock->info.rows, numOfOutput);
    SWinKey key = {
        .ts = nextWin.skey,
        .groupId = tableGroupId,
    };
    saveOutputBuf(pTaskInfo, &key, pResult, pInfo->aggSup.resultRowSize);
    releaseOutputBuf(pTaskInfo, &key, pResult);
    int32_t prevEndPos = (forwardRows - 1) * step + startPos;
    ASSERT(pSDataBlock->info.window.skey > 0 && pSDataBlock->info.window.ekey > 0);
    startPos =
        getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, TSDB_ORDER_ASC);
    if (startPos < 0) {
      break;
    }
  }
}

void doBuildResult(SOperatorInfo* pOperator, SSDataBlock* pBlock, SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.groupId = 0;
  buildDataBlockFromGroupRes(pTaskInfo, pBlock, &pOperator->exprSupp, pGroupResInfo);
}

static SSDataBlock* doStreamIntervalAgg(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  int64_t                      maxTs = INT64_MIN;
  int64_t                      minTs = INT64_MAX;
  SExprSupp*                   pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, "single interval");
      return pInfo->pDelRes;
    }

    doBuildResult(pOperator, pInfo->binfo.pRes, &pInfo->groupResInfo);
    // doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainResults(&pInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
      qDebug("===stream===single interval is done");
      freeAllPages(pInfo->pRecycledPages, pInfo->aggSup.pResultBuf);
    }
    printDataBlock(pInfo->binfo.pRes, "single interval");
    return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  SArray*    pUpdated = taosArrayInit(4, POINTER_BYTES);  // SResKeyPos
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*  pUpdatedMap = taosHashInit(1024, hashFn, false, HASH_NO_LOCK);

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, "single interval recv");

    if (pBlock->info.type == STREAM_CLEAR) {
      doDeleteWindows(pOperator, &pInfo->interval, pOperator->exprSupp.numOfExprs, pBlock, NULL, NULL);
      qDebug("%s clear existed time window results for updates checked", GET_TASKID(pTaskInfo));
      continue;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      // doDeleteSpecifyIntervalWindow(&pInfo->aggSup, &pInfo->twAggSup, pBlock, pInfo->pDelWins, &pInfo->interval,
      //                               pUpdatedMap);
      doDeleteWindows(pOperator, &pInfo->interval, pOperator->exprSupp.numOfExprs, pBlock, pInfo->pDelWins,
                      pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pUpdatedMap);
      continue;
    }

    if (pBlock->info.type == STREAM_NORMAL && pBlock->info.version != 0) {
      // set input version
      pTaskInfo->version = pBlock->info.version;
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }

    // The timewindow that overlaps the timestamps of the input pBlock need to be recalculated and return to the
    // caller. Note that all the time window are not close till now.
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    if (pInfo->invertible) {
      setInverFunction(pSup->pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.type);
    }

    maxTs = TMAX(maxTs, pBlock->info.window.ekey);
    minTs = TMIN(minTs, pBlock->info.window.skey);
    // doStreamIntervalAggImpl(pOperator, &pInfo->binfo.resultRowInfo, pBlock, MAIN_SCAN, pUpdatedMap);
    // new disc buf
    doStreamIntervalAggImpl2(pOperator, pBlock, pBlock->info.groupId, pUpdatedMap);
  }
  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, minTs);
  pOperator->status = OP_RES_TO_RETURN;
  closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, NULL, pUpdatedMap,
                            pOperator);

  void* pIte = NULL;
  while ((pIte = taosHashIterate(pUpdatedMap, pIte)) != NULL) {
    taosArrayPush(pUpdated, pIte);
  }
  taosArraySort(pUpdated, resultrowComparAsc);

  // new disc buf
  // finalizeUpdatedResult(pOperator->exprSupp.numOfExprs, pInfo->aggSup.pResultBuf, pUpdated,
  // pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  removeDeleteResults(pUpdatedMap, pInfo->pDelWins);
  taosHashCleanup(pUpdatedMap);
  doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, "single interval");
    return pInfo->pDelRes;
  }

  // doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
  // new disc buf
  doBuildResult(pOperator, pInfo->binfo.pRes, &pInfo->groupResInfo);
  printDataBlock(pInfo->binfo.pRes, "single interval");
  return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
}

void destroyStreamIntervalOperatorInfo(void* param) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  pInfo->pRecycledPages = taosArrayDestroy(pInfo->pRecycledPages);

  pInfo->pDelWins = taosArrayDestroy(pInfo->pDelWins);
  pInfo->pDelRes = blockDataDestroy(pInfo->pDelRes);

  cleanupGroupResInfo(&pInfo->groupResInfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo) {
  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }
  SStreamIntervalPhysiNode* pIntervalPhyNode = (SStreamIntervalPhysiNode*)pPhyNode;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &numOfCols);
  ASSERT(numOfCols > 0);
  SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
  SInterval    interval = {
         .interval = pIntervalPhyNode->interval,
         .sliding = pIntervalPhyNode->sliding,
         .intervalUnit = pIntervalPhyNode->intervalUnit,
         .slidingUnit = pIntervalPhyNode->slidingUnit,
         .offset = pIntervalPhyNode->offset,
         .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision,
  };
  STimeWindowAggSupp twAggSupp = {
      .waterMark = pIntervalPhyNode->window.watermark,
      .calTrigger = pIntervalPhyNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = INT64_MAX,
  };
  ASSERT(twAggSupp.calTrigger != STREAM_TRIGGER_MAX_DELAY);
  pOperator->pTaskInfo = pTaskInfo;
  pInfo->interval = interval;
  pInfo->twAggSup = twAggSupp;
  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->isFinal = false;

  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &numOfScalar);
    int32_t    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  SExprSupp* pSup = &pOperator->exprSupp;
  size_t     keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t    code = initAggInfo(pSup, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);
  initStreamFunciton(pSup->pCtx, pSup->numOfExprs);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->invertible = allInvertible(pSup->pCtx, numOfCols);
  pInfo->invertible = false;  // Todo(liuyao): Dependent TSDB API
  pInfo->pRecycledPages = taosArrayInit(4, sizeof(int32_t));
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->delIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pOperator->name = "StreamIntervalOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doStreamIntervalAgg, NULL, NULL, destroyStreamIntervalOperatorInfo,
                          aggEncodeResultRow, aggDecodeResultRow, NULL);

  initIntervalDownStream(downstream, pPhyNode->type, &pInfo->aggSup, &pInfo->interval, &pInfo->twAggSup);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyStreamIntervalOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}
