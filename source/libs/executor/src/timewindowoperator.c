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
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP = 2,
} SResultTsInterpType;

#define IS_FINAL_OP(op) ((op)->isFinal)

typedef struct SWinRes {
  TSKEY    ts;
  uint64_t groupId;
} SWinRes;

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
} SPullWindowInfo;

static SSDataBlock* doStreamSessionAgg(SOperatorInfo* pOperator);

static int64_t* extractTsCol(SSDataBlock* pBlock, const SIntervalAggOperatorInfo* pInfo);

static SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult);
static void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult);

///*
// * There are two cases to handle:
// *
// * 1. Query range is not set yet (queryRangeSet = 0). we need to set the query range info, including
// * pQueryAttr->lastKey, pQueryAttr->window.skey, and pQueryAttr->eKey.
// * 2. Query range is set and query is in progress. There may be another result with the same query ranges to be
// *    merged during merge stage. In this case, we need the pTableQueryInfo->lastResRows to decide if there
// *    is a previous result generated or not.
// */
// static void setIntervalQueryRange(STableQueryInfo* pTableQueryInfo, TSKEY key, STimeWindow* pQRange) {
//  // do nothing
//}

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

static void doKeepTuple(SWindowRowsSup* pRowSup, int64_t ts) {
  pRowSup->win.ekey = ts;
  pRowSup->prevTs = ts;
  pRowSup->numOfRows += 1;
}

static void doKeepNewWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, int32_t rowIndex) {
  pRowSup->startRowIndex = rowIndex;
  pRowSup->numOfRows = 0;
  pRowSup->win.skey = tsList[rowIndex];
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

  int32_t  startPos = 0;
  int32_t  numOfOutput = pSup->numOfExprs;
  uint64_t groupId = pBlock->info.groupId;

  SResultRow* pResult = NULL;

  while (1) {
    SListNode* pn = tdListGetHead(pResultRowInfo->openWindow);

    SResultRowPosition* p1 = (SResultRowPosition*)pn->data;
    if (p->pageId == p1->pageId && p->offset == p1->offset) {
      break;
    }

    SResultRow* pr = getResultRowByPos(pInfo->aggSup.pResultBuf, p1, false);
    ASSERT(pr->offset == p1->offset && pr->pageId == p1->pageId);

    if (pr->closed) {
      ASSERT(isResultRowInterpolated(pr, RESULT_ROW_START_INTERP) &&
             isResultRowInterpolated(pr, RESULT_ROW_END_INTERP));
      tdListPopHead(pResultRowInfo->openWindow);
      continue;
    }

    STimeWindow w = pr->win;
    int32_t     ret = setTimeWindowOutputBuf(pResultRowInfo, &w, (scanFlag == MAIN_SCAN), &pResult, groupId, pSup->pCtx,
                                             numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    ASSERT(!isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

    SGroupKeys* pTsKey = taosArrayGet(pInfo->pPrevValues, 0);
    int64_t     prevTs = *(int64_t*)pTsKey->pData;
    doTimeWindowInterpolation(pInfo->pPrevValues, pBlock->pDataBlock, prevTs, -1, tsCols[startPos], startPos, w.ekey,
                              RESULT_ROW_END_INTERP, pSup);

    setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    setNotInterpoWindowKey(pSup->pCtx, numOfExprs, RESULT_ROW_START_INTERP);

    doApplyFunctions(pTaskInfo, pSup->pCtx, &w, &pInfo->twAggSup.timeWindowData, startPos, 0, tsCols, pBlock->info.rows,
                     numOfExprs, pInfo->inputOrder);

    if (isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
      closeResultRow(pr);
      tdListPopHead(pResultRowInfo->openWindow);
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
  SWinRes*    pData = (SWinRes*)pKey;
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
  SWinRes data = {.ts = ts, .groupId = groupId};
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

static int32_t saveResultRow(SResultRow* result, uint64_t groupId, SArray* pUpdated) {
  return saveResult(result->win.skey, result->pageId, result->offset, groupId, pUpdated);
}

static void removeResult(SArray* pUpdated, SWinRes* pKey) {
  int32_t size = taosArrayGetSize(pUpdated);
  int32_t index = binarySearchCom(pUpdated, size, pKey, TSDB_ORDER_DESC, compareResKey);
  if (index >= 0 && 0 == compareResKey(pKey, pUpdated, index)) {
    taosArrayRemove(pUpdated, index);
  }
}

static void removeResults(SArray* pWins, SArray* pUpdated) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SWinRes* pW = taosArrayGet(pWins, i);
    removeResult(pUpdated, pW);
  }
}

int64_t getWinReskey(void* data, int32_t index) {
  SArray*  res = (SArray*)data;
  SWinRes* pos = taosArrayGet(res, index);
  return pos->ts;
}

int32_t compareWinRes(void* pKey, void* data, int32_t index) {
  SArray*     res = (SArray*)data;
  SWinRes*    pos = taosArrayGetP(res, index);
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

static void removeDeleteResults(SArray* pUpdated, SArray* pDelWins) {
  int32_t upSize = taosArrayGetSize(pUpdated);
  int32_t delSize = taosArrayGetSize(pDelWins);
  for (int32_t i = 0; i < upSize; i++) {
    SResKeyPos* pResKey = taosArrayGetP(pUpdated, i);
    int32_t     index = binarySearchCom(pDelWins, delSize, pResKey, TSDB_ORDER_DESC, compareWinRes);
    if (index >= 0 && 0 == compareWinRes(pResKey, pDelWins, index)) {
      taosArrayRemove(pDelWins, index);
    }
  }
}

bool isOverdue(TSKEY ts, STimeWindowAggSupp* pSup) {
  ASSERT(pSup->maxTs == INT64_MIN || pSup->maxTs > 0);
  return pSup->maxTs != INT64_MIN && ts < pSup->maxTs - pSup->waterMark;
}

bool isCloseWindow(STimeWindow* pWin, STimeWindowAggSupp* pSup) { return isOverdue(pWin->ekey, pSup); }

static void hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                            int32_t scanFlag, SArray* pUpdated) {
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
  int32_t ret = TSDB_CODE_SUCCESS;
  if ((!pInfo->ignoreExpiredData || !isCloseWindow(&win, &pInfo->twAggSup)) &&
      inSlidingWindow(&pInfo->interval, &win, &pBlock->info)) {
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                 numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM && pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      saveResultRow(pResult, tableGroupId, pUpdated);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pResultRowInfo->cur);
    }
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->inputOrder);
  ASSERT(forwardRows > 0);

  // prev time window not interpolation yet.
  if (pInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                 numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    // window start key interpolation
    doWindowBorderInterpolation(pInfo, pBlock, pResult, &win, startPos, forwardRows, pSup);
  }

  if ((!pInfo->ignoreExpiredData || !isCloseWindow(&win, &pInfo->twAggSup)) &&
      inSlidingWindow(&pInfo->interval, &win, &pBlock->info)) {
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &win, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, tsCols,
                     pBlock->info.rows, numOfOutput, pInfo->inputOrder);
  }

  doCloseWindow(pResultRowInfo, pInfo, pResult);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, pInfo->inputOrder);
    if (startPos < 0) {
      break;
    }
    if (pInfo->ignoreExpiredData && isCloseWindow(&nextWin, &pInfo->twAggSup)) {
      ekey = ascScan ? nextWin.ekey : nextWin.skey;
      forwardRows =
          getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->inputOrder);
      continue;
    }

    // null data, failed to allocate more memory buffer
    int32_t code = setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                          pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM && pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      saveResultRow(pResult, tableGroupId, pUpdated);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pResultRowInfo->cur);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->inputOrder);

    // window start(end) key interpolation
    doWindowBorderInterpolation(pInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pSup);

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &nextWin, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, tsCols,
                     pBlock->info.rows, numOfOutput, pInfo->inputOrder);
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

SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult) {
  SResultRowPosition pos = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};
  SListNode*         pn = tdListGetTail(pResultRowInfo->openWindow);
  if (pn == NULL) {
    tdListAppend(pResultRowInfo->openWindow, &pos);
    return pos;
  }

  SResultRowPosition* px = (SResultRowPosition*)pn->data;
  if (px->pageId != pos.pageId || px->offset != pos.offset) {
    tdListAppend(pResultRowInfo->openWindow, &pos);
  }

  return pos;
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

    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, scanFlag, NULL);
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

    if (!pInfo->hasKey) {
      // todo extract method
      if (IS_VAR_DATA_TYPE(pInfo->stateKey.type)) {
        varDataCopy(pInfo->stateKey.pData, val);
      } else {
        memcpy(pInfo->stateKey.pData, val, bytes);
      }

      pInfo->hasKey = true;

      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    } else if (compareVal(val, &pInfo->stateKey)) {
      doKeepTuple(pRowSup, tsList[j]);
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
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      doApplyFunctions(pTaskInfo, pSup->pCtx, &window, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                       pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);

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
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &pRowSup->win, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                   pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
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

void doClearWindowImpl(SResultRowPosition* p1, SDiskbasedBuf* pResultBuf, SExprSupp* pSup, int32_t numOfOutput) {
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

bool doClearWindow(SAggSupporter* pAggSup, SExprSupp* pSup, char* pData, int16_t bytes, uint64_t groupId,
                   int32_t numOfOutput) {
  SET_RES_WINDOW_KEY(pAggSup->keyBuf, pData, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)taosHashGet(pAggSup->pResultRowHashTable, pAggSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  if (!p1) {
    // window has been closed
    return false;
  }
  doClearWindowImpl(p1, pAggSup->pResultBuf, pSup, numOfOutput);
  return true;
}

bool doDeleteIntervalWindow(SAggSupporter* pAggSup, TSKEY ts, uint64_t groupId) {
  size_t bytes = sizeof(TSKEY);
  SET_RES_WINDOW_KEY(pAggSup->keyBuf, &ts, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)taosHashGet(pAggSup->pResultRowHashTable, pAggSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  if (!p1) {
    // window has been closed
    return false;
  }
  // SFilePage* bufPage = getBufPage(pAggSup->pResultBuf, p1->pageId);
  // dBufSetBufPageRecycled(pAggSup->pResultBuf, bufPage);
  taosHashRemove(pAggSup->pResultRowHashTable, pAggSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  return true;
}

void doDeleteSpecifyIntervalWindow(SAggSupporter* pAggSup, SSDataBlock* pBlock, SArray* pUpWins, SInterval* pInterval) {
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsStarts = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIds = (uint64_t*)pGroupCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, tsStarts[i], pInterval, TSDB_ORDER_ASC);
    doDeleteIntervalWindow(pAggSup, win.skey, groupIds[i]);
    if (pUpWins) {
      SWinRes winRes = {.ts = win.skey, .groupId = groupIds[i]};
      taosArrayPush(pUpWins, &winRes);
    }
  }
}

static void doClearWindows(SAggSupporter* pAggSup, SExprSupp* pSup1, SInterval* pInterval, int32_t numOfOutput,
                           SSDataBlock* pBlock, SArray* pUpWins) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endTsCols = (TSKEY*)pEndTsCol->pData;
  uint64_t*        pGpDatas = NULL;
  if (pBlock->info.type == STREAM_RETRIEVE) {
    SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
    pGpDatas = (uint64_t*)pGpCol->pData;
  }
  int32_t step = 0;
  int32_t startPos = 0;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTsCols[i], pInterval, TSDB_ORDER_ASC);
    while (win.ekey <= endTsCols[i]) {
      uint64_t winGpId = pGpDatas ? pGpDatas[startPos] : pBlock->info.groupId;
      bool     res = doClearWindow(pAggSup, pSup1, (char*)&win.skey, sizeof(TSKEY), winGpId, numOfOutput);
      if (pUpWins && res) {
        SWinRes winRes = {.ts = win.skey, .groupId = winGpId};
        taosArrayPush(pUpWins, &winRes);
      }
      getNextTimeWindow(pInterval, pInterval->precision, TSDB_ORDER_ASC, &win);
    }
  }
}

static int32_t getAllIntervalWindow(SHashObj* pHashMap, SArray* resWins) {
  void*  pIte = NULL;
  size_t keyLen = 0;
  while ((pIte = taosHashIterate(pHashMap, pIte)) != NULL) {
    void*    key = taosHashGetKey(pIte, &keyLen);
    uint64_t groupId = *(uint64_t*)key;
    ASSERT(keyLen == GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY)));
    TSKEY               ts = *(int64_t*)((char*)key + sizeof(uint64_t));
    SResultRowPosition* pPos = (SResultRowPosition*)pIte;
    int32_t             code = saveResult(ts, pPos->pageId, pPos->offset, groupId, resWins);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t closeIntervalWindow(SHashObj* pHashMap, STimeWindowAggSupp* pSup, SInterval* pInterval,
                                   SHashObj* pPullDataMap, SArray* closeWins, SArray* pRecyPages,
                                   SDiskbasedBuf* pDiscBuf) {
  qDebug("===stream===close interval window");
  void*  pIte = NULL;
  size_t keyLen = 0;
  while ((pIte = taosHashIterate(pHashMap, pIte)) != NULL) {
    void*    key = taosHashGetKey(pIte, &keyLen);
    uint64_t groupId = *(uint64_t*)key;
    ASSERT(keyLen == GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY)));
    TSKEY       ts = *(int64_t*)((char*)key + sizeof(uint64_t));
    STimeWindow win;
    win.skey = ts;
    win.ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
    SWinRes winRe = {
        .ts = win.skey,
        .groupId = groupId,
    };
    void* chIds = taosHashGet(pPullDataMap, &winRe, sizeof(SWinRes));
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
        int32_t code = saveResult(ts, pPos->pageId, pPos->offset, groupId, closeWins);
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
      taosHashRemove(pHashMap, keyBuf, keyLen);
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
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  for (int32_t i = *index; i < size; i++) {
    SWinRes* pWin = taosArrayGet(pWins, i);
    colDataAppend(pTsCol, pBlock->info.rows, (const char*)&pWin->ts, false);
    colDataAppend(pGroupCol, pBlock->info.rows, (const char*)&pWin->groupId, false);
    pBlock->info.rows++;
    (*index)++;
  }
}

static SSDataBlock* doStreamIntervalAgg(SOperatorInfo* pOperator) {
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  pInfo->inputOrder = TSDB_ORDER_ASC;
  SExprSupp* pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
    if (pInfo->pDelRes->info.rows > 0) {
      return pInfo->pDelRes;
    }

    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainResults(&pInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
      qDebug("===stream===single interval is done");
      freeAllPages(pInfo->pRecycledPages, pInfo->aggSup.pResultBuf);
    }
    printDataBlock(pInfo->binfo.pRes, "single interval");
    return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  SArray* pUpdated = taosArrayInit(4, POINTER_BYTES);  // SResKeyPos
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, "single interval recv");

    if (pBlock->info.type == STREAM_CLEAR) {
      doClearWindows(&pInfo->aggSup, &pOperator->exprSupp, &pInfo->interval, pOperator->exprSupp.numOfExprs, pBlock,
                     NULL);
      qDebug("%s clear existed time window results for updates checked", GET_TASKID(pTaskInfo));
      continue;
    }
    if (pBlock->info.type == STREAM_DELETE_DATA) {
      doDeleteSpecifyIntervalWindow(&pInfo->aggSup, pBlock, pInfo->pDelWins, &pInfo->interval);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pUpdated);
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
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, pInfo->inputOrder, MAIN_SCAN, true);
    if (pInfo->invertible) {
      setInverFunction(pSup->pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.type);
    }

    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, MAIN_SCAN, pUpdated);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, NULL, pUpdated,
                      pInfo->pRecycledPages, pInfo->aggSup.pResultBuf);

  finalizeUpdatedResult(pOperator->exprSupp.numOfExprs, pInfo->aggSup.pResultBuf, pUpdated, pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  removeDeleteResults(pUpdated, pInfo->pDelWins);
  doBuildDeleteResult(pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows > 0) {
    return pInfo->pDelRes;
  }

  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
  printDataBlock(pInfo->binfo.pRes, "single interval");
  return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
}

static void destroyStateWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->stateKey.pData);

  taosMemoryFreeClear(param);
}

static void freeItem(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

void destroyIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
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

void destroyStreamFinalIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  SStreamFinalIntervalOperatorInfo* pInfo = (SStreamFinalIntervalOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  // it should be empty.
  taosHashCleanup(pInfo->pPullDataMap);
  taosArrayDestroy(pInfo->pPullWins);
  blockDataDestroy(pInfo->pPullDataRes);
  taosArrayDestroy(pInfo->pRecycledPages);
  blockDataDestroy(pInfo->pUpdateRes);

  if (pInfo->pChildren) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, i);
      destroyStreamFinalIntervalOperatorInfo(pChildOp->info, numOfOutput);
      taosMemoryFreeClear(pChildOp);
    }
  }
  nodesDestroyNode((SNode*)pInfo->pPhyNode);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  taosMemoryFreeClear(param);
}

static bool allInvertible(SqlFunctionCtx* pFCtx, int32_t numOfCols) {
  for (int32_t i = 0; i < numOfCols; i++) {
    if (!fmIsInvertible(pFCtx[i].functionId)) {
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

void increaseTs(SqlFunctionCtx* pCtx) {
  if (pCtx[0].pExpr->pExpr->_function.pFunctNode->funcType == FUNCTION_TYPE_WSTART) {
    pCtx[0].increase = true;
  }
}

void initIntervalDownStream(SOperatorInfo* downstream, uint8_t type, SAggSupporter* pSup) {
  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    // Todo(liuyao) support partition by column
    return;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->sessionSup.parentType = type;
  pScanInfo->sessionSup.pIntervalAggSup = pSup;
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
  initBasicInfo(&pInfo->binfo, pResBlock);

  if (isStream) {
    ASSERT(numOfCols > 0);
    increaseTs(pSup->pCtx);
  }

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);

  pInfo->invertible = allInvertible(pSup->pCtx, numOfCols);
  pInfo->invertible = false;  // Todo(liuyao): Dependent TSDB API

  pInfo->timeWindowInterpo = timeWindowinterpNeeded(pSup->pCtx, numOfCols, pInfo);
  if (pInfo->timeWindowInterpo) {
    pInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SResultRowPosition));
    if (pInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }
  pInfo->pRecycledPages = taosArrayInit(4, sizeof(int32_t));
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinRes));
  pInfo->delIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pOperator->name = "TimeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(doOpenIntervalAgg, doBuildIntervalResult, doStreamIntervalAgg, NULL,
                                         destroyIntervalOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);

  if (nodeType(pPhyNode) == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL) {
    initIntervalDownStream(downstream, QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL, &pInfo->aggSup);
  }

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyIntervalOperatorInfo(pInfo, numOfCols);
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                                SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                                STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  SIntervalAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  pInfo->inputOrder = TSDB_ORDER_ASC;
  pInfo->interval = *pInterval;
  pInfo->execModel = OPTR_EXEC_MODEL_STREAM;
  pInfo->win = pTaskInfo->window;
  pInfo->twAggSup = *pTwAggSupp;
  pInfo->primaryTsIndex = primaryTsSlotId;

  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(&pOperator->resultInfo, numOfRows);
  int32_t code = initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  initBasicInfo(&pInfo->binfo, pResBlock);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);

  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pOperator->name = "StreamTimeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(doOpenIntervalAgg, doStreamIntervalAgg, doStreamIntervalAgg, NULL,
                                         destroyIntervalOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyIntervalOperatorInfo(pInfo, numOfCols);
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

// todo handle multiple tables cases.
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
    if (pInfo->winSup.prevTs == INT64_MIN) {
      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    } else if (tsList[j] - pRowSup->prevTs <= gap && (tsList[j] - pRowSup->prevTs) >= 0) {
      // The gap is less than the threshold, so it belongs to current session window that has been opened already.
      doKeepTuple(pRowSup, tsList[j]);
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
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      // pInfo->numOfRows data belong to the current session window
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      doApplyFunctions(pTaskInfo, pSup->pCtx, &window, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                       pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &pRowSup->win, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                   pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
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

static void doKeepLinearInfo(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
    SFillLinearInfo* pLinearInfo = taosArrayGet(pSliceInfo->pLinearInfo, i);

    // null data should not be kept since it can not be used to perform interpolation
    if (!colDataIsNull_s(pColInfoData, i)) {
      int64_t startKey = *(int64_t*)colDataGetData(pTsCol, rowIndex);
      int64_t endKey   = *(int64_t*)colDataGetData(pTsCol, rowIndex + 1);
      pLinearInfo->start.key = startKey;
      pLinearInfo->end.key   = endKey;

      char* val;
      val = colDataGetData(pColInfoData, rowIndex);
      memcpy(pLinearInfo->start.val, val, pLinearInfo->bytes);
      val = colDataGetData(pColInfoData, rowIndex + 1);
      memcpy(pLinearInfo->end.val, val, pLinearInfo->bytes);

      pLinearInfo->hasNull = false;
    } else {
      pLinearInfo->hasNull = true;
    }
  }

}

static void genInterpolationResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pBlock,
                                   SSDataBlock* pResBlock) {
  int32_t rows = pResBlock->info.rows;

  // todo set the correct primary timestamp column

  // output the result
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];
    int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
    int32_t    srcSlot = pExprInfo->base.pParam[0].pCol->slotId;

    SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, srcSlot);
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    switch (pSliceInfo->fillType) {
      case TSDB_FILL_NULL: {
        colDataAppendNULL(pDst, rows);
        pResBlock->info.rows += 1;
        break;
      }

      case TSDB_FILL_SET_VALUE: {
        SVariant* pVar = &pSliceInfo->pFillColInfo[j].fillVal;

        if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
          float v = 0;
          GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char *)&v, false);
        } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
          double v = 0;
          GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char *)&v, false);
        } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
          int64_t v = 0;
          GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char *)&v, false);
        }
        pResBlock->info.rows += 1;
        break;
      }

      case TSDB_FILL_LINEAR: {
        SFillLinearInfo* pLinearInfo = taosArrayGet(pSliceInfo->pLinearInfo, srcSlot);

        SPoint start   = pLinearInfo->start;
        SPoint end     = pLinearInfo->end;
        SPoint current = {.key = pSliceInfo->current};
        current.val = taosMemoryCalloc(pLinearInfo->bytes, 1);

        // before interp range, do not fill
        if (start.key == INT64_MIN || end.key == INT64_MAX) {
          break;
        }

        if (pLinearInfo->hasNull) {
          colDataAppendNULL(pDst, rows);
        } else {
          taosGetLinearInterpolationVal(&current, pLinearInfo->type, &start, &end, pLinearInfo->type);
          colDataAppend(pDst, rows, (char *)current.val, false);
        }

        pResBlock->info.rows += 1;
        break;
      }
      case TSDB_FILL_PREV: {
        if (!pSliceInfo->isPrevRowSet) {
          break;
        }

        SGroupKeys* pkey = taosArrayGet(pSliceInfo->pPrevRow, srcSlot);
        colDataAppend(pDst, rows, pkey->pData, false);
        pResBlock->info.rows += 1;
        break;
      }

      case TSDB_FILL_NEXT: {
        if (!pSliceInfo->isNextRowSet) {
          break;
        }

        SGroupKeys* pkey = taosArrayGet(pSliceInfo->pNextRow, srcSlot);
        colDataAppend(pDst, rows, pkey->pData, false);
        pResBlock->info.rows += 1;
        break;
      }

      case TSDB_FILL_NONE:
      default:
        break;
    }
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
  if (pInfo->pNextRow == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SFillLinearInfo linearInfo = {0};
    linearInfo.start.key = INT64_MIN;
    linearInfo.end.key   = INT64_MAX;
    linearInfo.start.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    linearInfo.end.val   = taosMemoryCalloc(1, pColInfo->info.bytes);
    linearInfo.hasNull   = false;
    linearInfo.fillLastPoint = false;
    linearInfo.type  = pColInfo->info.type;
    linearInfo.bytes = pColInfo->info.bytes;
    taosArrayPush(pInfo->pLinearInfo, &linearInfo);
  }

  return TSDB_CODE_SUCCESS;
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
      longjmp(pTaskInfo->env, code);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, MAIN_SCAN, true);

    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);

      if (ts == pSliceInfo->current) {
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

        // for linear interpolation, always fill value between this and next points;
        // if its the first point in data block, also fill values between previous(if there's any) and this point;
        // if its the last point in data block, no need to fill, but reserve this point as the start value for next data block.
        if (pSliceInfo->fillType == TSDB_FILL_LINEAR) {
          doKeepLinearInfo(pSliceInfo, pBlock, i);
          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (i < pBlock->info.rows - 1) {
            int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
            if (nextTs > pSliceInfo->current) {
              while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pBlock, pResBlock);
                pSliceInfo->current =
                    taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
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
        } else { // non-linear interpolation
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
          doKeepLinearInfo(pSliceInfo, pBlock, i);
          // no need to increate pSliceInfo->current here
          //pSliceInfo->current =
          //    taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          if (i < pBlock->info.rows - 1) {
            int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
            if (nextTs > pSliceInfo->current) {
              while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pBlock, pResBlock);
                pSliceInfo->current =
                    taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
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
        } else { // non-linear interpolation
          if (i < pBlock->info.rows - 1) {
            // in case of interpolation window starts and ends between two datapoints, fill(next) need to interpolate
            doKeepNextRows(pSliceInfo, pBlock, i + 1);
            int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
            if (nextTs > pSliceInfo->current) {
              while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pBlock, pResBlock);
                pSliceInfo->current =
                    taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
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
          genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pBlock, pResBlock);
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
            doKeepLinearInfo(pSliceInfo, pBlock, i);
            pSliceInfo->current =
                taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
            if (i < pBlock->info.rows - 1) {
              int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
              if (nextTs > pSliceInfo->current) {
                while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
                  genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pBlock, pResBlock);
                  pSliceInfo->current =
                      taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
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
          } else { // non-linear interpolation
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

    // check if need to interpolate after ts range
    // except for fill(next), fill(linear)
    while (pSliceInfo->current <= pSliceInfo->win.ekey && pSliceInfo->fillType != TSDB_FILL_NEXT && pSliceInfo->fillType != TSDB_FILL_LINEAR) {
      genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pBlock, pResBlock);
      pSliceInfo->current =
          taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
      if (pResBlock->info.rows >= pResBlock->info.capacity) {
        break;
      }
    }
  }

  // restore the value
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  if (pResBlock->info.rows == 0) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pResBlock->info.rows == 0 ? NULL : pResBlock;
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

  pInfo->pPrevRow = NULL;
  pInfo->pNextRow = NULL;
  pInfo->pLinearInfo = NULL;
  pInfo->pFillColInfo = createFillColInfo(pExprInfo, numOfExprs, (SNodeListNode*)pInterpPhyNode->pFillValues);
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
      createOperatorFpSet(operatorDummyOpenFn, doTimeslice, NULL, NULL, destroyBasicOperatorInfo, NULL, NULL, NULL);

  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                             SSDataBlock* pResBlock, STimeWindowAggSupp* pTwAggSup, int32_t tsSlotId,
                                             SColumn* pStateKeyCol, SNode* pCondition, SExecTaskInfo* pTaskInfo) {
  SStateWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStateWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->stateCol = *pStateKeyCol;
  pInfo->stateKey.type = pInfo->stateCol.type;
  pInfo->stateKey.bytes = pInfo->stateCol.bytes;
  pInfo->stateKey.pData = taosMemoryCalloc(1, pInfo->stateCol.bytes);
  pInfo->pCondition = pCondition;
  if (pInfo->stateKey.pData == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExpr, numOfCols, keyBufSize, pTaskInfo->id.str);
  initBasicInfo(&pInfo->binfo, pResBlock);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->twAggSup = *pTwAggSup;
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = tsSlotId;
  pOperator->name = "StateWindowOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExpr;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doStateWindowAgg, NULL, NULL,
                                         destroyStateWindowOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_SUCCESS;
  return NULL;
}

void destroySWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SSessionAggOperatorInfo* pInfo = (SSessionAggOperatorInfo*)param;
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
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doSessionWindowAgg, NULL, NULL,
                                         destroySWindowOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);
  pOperator->pTaskInfo = pTaskInfo;

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroySWindowOperatorInfo(pInfo, numOfCols);
  }

  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void compactFunctions(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t numOfOutput,
                      SExecTaskInfo* pTaskInfo) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    if (fmIsWindowPseudoColumnFunc(pDestCtx[k].functionId)) {
      continue;
    }
    int32_t code = TSDB_CODE_SUCCESS;
    if (functionNeedToExecute(&pDestCtx[k]) && pDestCtx[k].fpSet.combine != NULL) {
      code = pDestCtx[k].fpSet.combine(&pDestCtx[k], &pSourceCtx[k]);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s apply functions error, code: %s", GET_TASKID(pTaskInfo), tstrerror(code));
        pTaskInfo->code = code;
        longjmp(pTaskInfo->env, code);
      }
    }
  }
}

bool hasIntervalWindow(SAggSupporter* pSup, TSKEY ts, uint64_t groupId) {
  int32_t bytes = sizeof(TSKEY);
  SET_RES_WINDOW_KEY(pSup->keyBuf, &ts, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)taosHashGet(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  return p1 != NULL;
}

static void rebuildIntervalWindow(SStreamFinalIntervalOperatorInfo* pInfo, SExprSupp* pSup, SArray* pWinArray,
                                  int32_t groupId, int32_t numOfOutput, SExecTaskInfo* pTaskInfo, SArray* pUpdated) {
  int32_t size = taosArrayGetSize(pWinArray);
  if (!pInfo->pChildren) {
    return;
  }
  for (int32_t i = 0; i < size; i++) {
    SWinRes*    pWinRes = taosArrayGet(pWinArray, i);
    SResultRow* pCurResult = NULL;
    STimeWindow ParentWin = {.skey = pWinRes->ts, .ekey = pWinRes->ts + 1};
    setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &ParentWin, true, &pCurResult, pWinRes->groupId, pSup->pCtx,
                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    int32_t numOfChildren = taosArrayGetSize(pInfo->pChildren);
    bool    find = true;
    for (int32_t j = 0; j < numOfChildren; j++) {
      SOperatorInfo*            pChildOp = taosArrayGetP(pInfo->pChildren, j);
      SIntervalAggOperatorInfo* pChInfo = pChildOp->info;
      SExprSupp*                pChildSup = &pChildOp->exprSupp;
      if (!hasIntervalWindow(&pChInfo->aggSup, pWinRes->ts, pWinRes->groupId)) {
        continue;
      }
      find = true;
      SResultRow* pChResult = NULL;
      setTimeWindowOutputBuf(&pChInfo->binfo.resultRowInfo, &ParentWin, true, &pChResult, pWinRes->groupId,
                             pChildSup->pCtx, pChildSup->numOfExprs, pChildSup->rowEntryInfoOffset, &pChInfo->aggSup,
                             pTaskInfo);
      compactFunctions(pSup->pCtx, pChildSup->pCtx, numOfOutput, pTaskInfo);
    }
    if (find && pUpdated) {
      saveResultRow(pCurResult, pWinRes->groupId, pUpdated);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pInfo->binfo.resultRowInfo.cur);
    }
  }
}

bool isDeletedWindow(STimeWindow* pWin, uint64_t groupId, SAggSupporter* pSup) {
  SET_RES_WINDOW_KEY(pSup->keyBuf, &pWin->skey, sizeof(int64_t), groupId);
  SResultRowPosition* p1 = (SResultRowPosition*)taosHashGet(pSup->pResultRowHashTable, pSup->keyBuf,
                                                            GET_RES_WINDOW_KEY_LEN(sizeof(int64_t)));
  return p1 == NULL;
}

int32_t getNexWindowPos(SInterval* pInterval, SDataBlockInfo* pBlockInfo, TSKEY* tsCols, int32_t startPos, TSKEY eKey,
                        STimeWindow* pNextWin) {
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(pBlockInfo, tsCols, startPos, eKey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
  int32_t prevEndPos = forwardRows - 1 + startPos;
  return getNextQualifiedWindow(pInterval, pNextWin, pBlockInfo, tsCols, prevEndPos, TSDB_ORDER_ASC);
}

void addPullWindow(SHashObj* pMap, SWinRes* pWinRes, int32_t size) {
  SArray* childIds = taosArrayInit(8, sizeof(int32_t));
  for (int32_t i = 0; i < size; i++) {
    taosArrayPush(childIds, &i);
  }
  taosHashPut(pMap, pWinRes, sizeof(SWinRes), &childIds, sizeof(void*));
}

static int32_t getChildIndex(SSDataBlock* pBlock) { return pBlock->info.childId; }

STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {.skey = ts, .ekey = INT64_MAX};
  w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  return w;
}

static void doHashInterval(SOperatorInfo* pOperatorInfo, SSDataBlock* pSDataBlock, uint64_t tableGroupId,
                           SArray* pUpdated) {
  SStreamFinalIntervalOperatorInfo* pInfo = (SStreamFinalIntervalOperatorInfo*)pOperatorInfo->info;
  SResultRowInfo*                   pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*                    pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*                        pSup = &pOperatorInfo->exprSupp;
  int32_t                           numOfOutput = pSup->numOfExprs;
  int32_t                           step = 1;
  bool                              ascScan = true;
  TSKEY*                            tsCols = NULL;
  SResultRow*                       pResult = NULL;
  int32_t                           forwardRows = 0;

  ASSERT(pSDataBlock->pDataBlock != NULL);
  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  tsCols = (int64_t*)pColDataInfo->pData;

  int32_t     startPos = ascScan ? 0 : (pSDataBlock->info.rows - 1);
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
      SWinRes winRes = {
          .ts = nextWin.skey,
          .groupId = tableGroupId,
      };
      void* chIds = taosHashGet(pInfo->pPullDataMap, &winRes, sizeof(SWinRes));
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
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (IS_FINAL_OP(pInfo)) {
      forwardRows = 1;
    } else {
      forwardRows = getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, nextWin.ekey, binarySearchForKey,
                                             NULL, TSDB_ORDER_ASC);
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pUpdated) {
      saveResultRow(pResult, tableGroupId, pUpdated);
      setResultBufPageDirty(pInfo->aggSup.pResultBuf, &pResultRowInfo->cur);
    }
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &nextWin, &pInfo->twAggSup.timeWindowData, startPos, forwardRows, tsCols,
                     pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
    int32_t prevEndPos = (forwardRows - 1) * step + startPos;
    ASSERT(pSDataBlock->info.window.skey > 0 && pSDataBlock->info.window.ekey > 0);
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, pInfo->order);
    if (startPos < 0) {
      break;
    }
  }
}

static void clearStreamIntervalOperator(SStreamFinalIntervalOperatorInfo* pInfo) {
  taosHashClear(pInfo->aggSup.pResultRowHashTable);
  clearDiskbasedBuf(pInfo->aggSup.pResultBuf);
  cleanupResultRowInfo(&pInfo->binfo.resultRowInfo);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
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
    SWinRes winRes = {.ts = tsData[i], .groupId = groupIdData[i]};
    void*   chIds = taosHashGet(pMap, &winRes, sizeof(SWinRes));
    if (chIds) {
      SArray* chArray = *(SArray**)chIds;
      int32_t index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
      if (index != -1) {
        qDebug("===stream===window %" PRId64 " delete child id %d", winRes.ts, chId);
        taosArrayRemove(chArray, index);
        if (taosArrayGetSize(chArray) == 0) {
          // pull data is over
          taosHashRemove(pMap, &winRes, sizeof(SWinRes));
        }
      }
    }
  }
}

static SSDataBlock* doStreamFinalIntervalAgg(SOperatorInfo* pOperator) {
  SStreamFinalIntervalOperatorInfo* pInfo = pOperator->info;
  SOperatorInfo*                    downstream = pOperator->pDownstream[0];
  SArray*                           pUpdated = taosArrayInit(4, POINTER_BYTES);
  TSKEY                             maxTs = INT64_MIN;

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

    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pInfo->binfo.pRes->info.rows == 0) {
      pOperator->status = OP_EXEC_DONE;
      if (!IS_FINAL_OP(pInfo)) {
        // semi interval operator clear disk buffer
        clearStreamIntervalOperator(pInfo);
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

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      clearSpecialDataBlock(pInfo->pUpdateRes);
      removeDeleteResults(pUpdated, pInfo->pDelWins);
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("%s return data", IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      break;
    }
    printDataBlock(pBlock, IS_FINAL_OP(pInfo) ? "interval final recv" : "interval semi recv");
    maxTs = TMAX(maxTs, pBlock->info.window.ekey);
    maxTs = TMAX(maxTs, pBlock->info.watermark);

    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA ||
        pBlock->info.type == STREAM_INVALID) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_CLEAR) {
      SArray* pUpWins = taosArrayInit(8, sizeof(SWinRes));
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
      removeResults(pUpWins, pUpdated);
      copyDataBlock(pInfo->pUpdateRes, pBlock);
      // copyUpdateDataBlock(pInfo->pUpdateRes, pBlock, pInfo->primaryTsIndex);
      pInfo->returnUpdate = true;
      taosArrayDestroy(pUpWins);
      break;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      doDeleteSpecifyIntervalWindow(&pInfo->aggSup, pBlock, pInfo->pDelWins, &pInfo->interval);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                           childIndex = getChildIndex(pBlock);
        SOperatorInfo*                    pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamFinalIntervalOperatorInfo* pChildInfo = pChildOp->info;
        SExprSupp*                        pChildSup = &pChildOp->exprSupp;
        doDeleteSpecifyIntervalWindow(&pChildInfo->aggSup, pBlock, NULL, &pChildInfo->interval);
        rebuildIntervalWindow(pInfo, pSup, pInfo->pDelWins, pInfo->binfo.pRes->info.groupId,
                              pOperator->exprSupp.numOfExprs, pOperator->pTaskInfo, pUpdated);
        continue;
      }
      removeResults(pInfo->pDelWins, pUpdated);
      break;
    } else if (pBlock->info.type == STREAM_GET_ALL && IS_FINAL_OP(pInfo)) {
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_RETRIEVE && !IS_FINAL_OP(pInfo)) {
      SArray* pUpWins = taosArrayInit(8, sizeof(SWinRes));
      doClearWindows(&pInfo->aggSup, pSup, &pInfo->interval, pOperator->exprSupp.numOfExprs, pBlock, pUpWins);
      removeResults(pUpWins, pUpdated);
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
    doHashInterval(pOperator, pBlock, pBlock->info.groupId, pUpdated);
    if (IS_FINAL_OP(pInfo)) {
      int32_t chIndex = getChildIndex(pBlock);
      int32_t size = taosArrayGetSize(pInfo->pChildren);
      // if chIndex + 1 - size > 0, add new child
      for (int32_t i = 0; i < chIndex + 1 - size; i++) {
        SOperatorInfo* pChildOp = createStreamFinalIntervalOperatorInfo(NULL, pInfo->pPhyNode, pOperator->pTaskInfo, 0);
        if (!pChildOp) {
          longjmp(pOperator->pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
        SStreamFinalIntervalOperatorInfo* pTmpInfo = pChildOp->info;
        pTmpInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
        taosArrayPush(pInfo->pChildren, &pChildOp);
        qDebug("===stream===add child, id:%d", chIndex);
      }
      SOperatorInfo*                    pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      SStreamFinalIntervalOperatorInfo* pChInfo = pChildOp->info;
      setInputDataBlock(pChildOp, pChildOp->exprSupp.pCtx, pBlock, pChInfo->order, MAIN_SCAN, true);
      doHashInterval(pChildOp, pBlock, pBlock->info.groupId, NULL);
    }
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  if (IS_FINAL_OP(pInfo)) {
    closeIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, pInfo->pPullDataMap,
                        pUpdated, pInfo->pRecycledPages, pInfo->aggSup.pResultBuf);
    closeChildIntervalWindow(pInfo->pChildren, pInfo->twAggSup.maxTs);
  } else {
    pInfo->binfo.pRes->info.watermark = pInfo->twAggSup.maxTs;
  }

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

SSDataBlock* createSpecialDataBlock(EStreamType type) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.hasVarCol = false;
  pBlock->info.groupId = 0;
  pBlock->info.rows = 0;
  pBlock->info.type = type;
  pBlock->info.rowSize =
      sizeof(TSKEY) + sizeof(TSKEY) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(TSKEY) + sizeof(TSKEY);
  pBlock->info.watermark = INT64_MIN;

  pBlock->pDataBlock = taosArrayInit(6, sizeof(SColumnInfoData));
  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_TIMESTAMP;
  infoData.info.bytes = sizeof(TSKEY);
  // window start ts
  taosArrayPush(pBlock->pDataBlock, &infoData);
  // window end ts
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_UBIGINT;
  infoData.info.bytes = sizeof(uint64_t);
  // uid
  taosArrayPush(pBlock->pDataBlock, &infoData);
  // group id
  taosArrayPush(pBlock->pDataBlock, &infoData);

  // calculate start ts
  taosArrayPush(pBlock->pDataBlock, &infoData);
  // calculate end ts
  taosArrayPush(pBlock->pDataBlock, &infoData);

  return pBlock;
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
  initBasicInfo(&pInfo->binfo, pResBlock);

  ASSERT(numOfCols > 0);
  increaseTs(pOperator->exprSupp.pCtx);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
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
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinRes));
  pInfo->pRecycledPages = taosArrayInit(4, sizeof(int32_t));

  pOperator->operatorType = pPhyNode->type;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet =
      createOperatorFpSet(NULL, doStreamFinalIntervalAgg, NULL, NULL, destroyStreamFinalIntervalOperatorInfo,
                          aggEncodeResultRow, aggDecodeResultRow, NULL);
  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL) {
    initIntervalDownStream(downstream, pPhyNode->type, &pInfo->aggSup);
  }
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyStreamFinalIntervalOperatorInfo(pInfo, numOfCols);
  taosMemoryFreeClear(pInfo);
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

void destroyStreamSessionAggOperatorInfo(void* param, int32_t numOfOutput) {
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, i);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      destroyStreamSessionAggOperatorInfo(pChInfo, numOfOutput);
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

  initBasicInfo(pBasicInfo, pResultBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    pSup->pCtx[i].pBuf = NULL;
  }

  ASSERT(numOfCols > 0);
  increaseTs(pSup->pCtx);
  return TSDB_CODE_SUCCESS;
}

void initDummyFunction(SqlFunctionCtx* pDummy, SqlFunctionCtx* pCtx, int32_t nums) {
  for (int i = 0; i < nums; i++) {
    pDummy[i].functionId = pCtx[i].functionId;
  }
}

void initDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, int64_t gap, int64_t waterMark,
                    uint8_t type) {
  ASSERT(downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->sessionSup = (SessionWindowSupporter){.pStreamAggSup = pAggSup, .gap = gap, .parentType = type};
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
    int32_t    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar);
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
      .waterMark = pSessionNode->window.watermark, .calTrigger = pSessionNode->window.triggerType, .maxTs = INT64_MIN};

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
  // pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pDelRes = createOneDataBlock(pInfo->binfo.pRes, false);  // todo(liuyao) for delete
  pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;               // todo(liuyao) for delete
  pInfo->pChildren = NULL;
  pInfo->isFinal = false;
  pInfo->pPhyNode = pPhyNode;
  pInfo->ignoreExpiredData = pSessionNode->window.igExpired;
  pInfo->returnDelete = false;

  pOperator->name = "StreamSessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = pInfo;
  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doStreamSessionAgg, NULL, NULL, destroyStreamSessionAggOperatorInfo,
                          aggEncodeResultRow, aggDecodeResultRow, NULL);
  if (downstream) {
    initDownStream(downstream, &pInfo->streamAggSup, pInfo->gap, pInfo->twAggSup.waterMark, pOperator->operatorType);
    code = appendDownstream(pOperator, &downstream, 1);
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStreamSessionAggOperatorInfo(pInfo, numOfCols);
  }

  taosMemoryFreeClear(pInfo);
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

static SResultWindowInfo* insertNewSessionWindow(SArray* pWinInfos, TSKEY ts, int32_t index) {
  SResultWindowInfo win = {.pos.offset = -1, .pos.pageId = -1, .win.skey = ts, .win.ekey = ts, .isOutput = false};
  return taosArrayInsert(pWinInfos, index, &win);
}

static SResultWindowInfo* addNewSessionWindow(SArray* pWinInfos, TSKEY ts) {
  SResultWindowInfo win = {.pos.offset = -1, .pos.pageId = -1, .win.skey = ts, .win.ekey = ts, .isOutput = false};
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
  SArray* pWinInfos = getWinInfos(pAggSup, groupId);
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
    return addNewSessionWindow(pWinInfos, startTs);
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
    return addNewSessionWindow(pWinInfos, startTs);
  }
  *pIndex = index + 1;
  return insertNewSessionWindow(pWinInfos, startTs, index + 1);
}

int32_t updateSessionWindowInfo(SResultWindowInfo* pWinInfo, TSKEY* pStartTs, TSKEY* pEndTs, int32_t rows,
                                int32_t start, int64_t gap, SHashObj* pStDeleted) {
  for (int32_t i = start; i < rows; ++i) {
    if (!isInWindow(pWinInfo, pStartTs[i], gap) && (!pEndTs || !isInWindow(pWinInfo, pEndTs[i], gap))) {
      return i - start;
    }
    if (pWinInfo->win.skey > pStartTs[i]) {
      if (pStDeleted && pWinInfo->isOutput) {
        taosHashPut(pStDeleted, &pWinInfo->pos, sizeof(SResultRowPosition), &pWinInfo->win.skey, sizeof(TSKEY));
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
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  if (pWinInfo->pos.pageId == -1) {
    *pResult = getNewResultRow(pAggSup->pResultBuf, groupId, pAggSup->resultRowSize);
    if (*pResult == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    initResultRow(*pResult);

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
  doApplyFunctions(pTaskInfo, pSup->pCtx, &pCurWin->win, pTimeWindowData, startIndex, winRows, tsCols,
                   pSDataBlock->info.rows, numOutput, TSDB_ORDER_ASC);
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
    compactFunctions(pSup->pCtx, pInfo->pDummyCtx, numOfOutput, pTaskInfo);
    taosHashRemove(pStUpdated, &pWinInfo->pos, sizeof(SResultRowPosition));
    if (pWinInfo->isOutput) {
      taosHashPut(pStDeleted, &pWinInfo->pos, sizeof(SResultRowPosition), &pWinInfo->win.skey, sizeof(TSKEY));
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
    winRows =
        updateSessionWindowInfo(pCurWin, startTsCols, endTsCols, pSDataBlock->info.rows, i, pInfo->gap, pStDeleted);
    code = doOneWindowAgg(pInfo, pSDataBlock, pCurWin, &pResult, i, winRows, numOfOutput, pOperator);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    int32_t winNum = getNumCompactWindow(pAggSup->pCurWins, winIndex, gap);
    if (winNum > 0) {
      compactTimeWindow(pInfo, winIndex, winNum, groupId, numOfOutput, pStUpdated, pStDeleted, pOperator);
    }
    pCurWin->isClosed = false;
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pStUpdated) {
      SWinRes value = {.ts = pCurWin->win.skey, .groupId = groupId};
      code = taosHashPut(pStUpdated, &pCurWin->pos, sizeof(SResultRowPosition), &value, sizeof(SWinRes));
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
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
    int32_t winIndex = 0;
    while (1) {
      SResultWindowInfo* pCurWin = getCurSessionWindow(pAggSup, startDatas[i], endDatas[i], gpDatas[i], gap, &winIndex);
      if (!pCurWin) {
        break;
      }
      deleteWindow(pAggSup->pCurWins, winIndex, fp);
      if (result) {
        taosArrayPush(result, pCurWin);
      }
    }
  }
}

static void doClearSessionWindows(SStreamAggSupporter* pAggSup, SExprSupp* pSup, SSDataBlock* pBlock, int32_t tsIndex,
                                  int32_t numOfOutput, int64_t gap, SArray* result) {
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, tsIndex);
  TSKEY*           tsCols = (TSKEY*)pColDataInfo->pData;
  int32_t          step = 0;
  for (int32_t i = 0; i < pBlock->info.rows; i += step) {
    int32_t            winIndex = 0;
    SResultWindowInfo* pCurWin =
        getCurSessionWindow(pAggSup, tsCols[i], INT64_MIN, pBlock->info.groupId, gap, &winIndex);
    if (!pCurWin || pCurWin->pos.pageId == -1) {
      // window has been closed.
      step = 1;
      continue;
    }
    step = updateSessionWindowInfo(pCurWin, tsCols, NULL, pBlock->info.rows, i, gap, NULL);
    ASSERT(isInWindow(pCurWin, tsCols[i], gap));
    doClearWindowImpl(&pCurWin->pos, pAggSup->pResultBuf, pSup, numOfOutput);
    if (result) {
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
    pos->groupId = ((SWinRes*)pData)->groupId;
    pos->pos = *(SResultRowPosition*)key;
    *(int64_t*)pos->key = ((SWinRes*)pData)->ts;
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
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
    colDataAppend(pColInfoData, pBlock->info.rows, *Ite, false);
    for (int32_t i = 1; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
      pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
      colDataAppendNULL(pColInfoData, pBlock->info.rows);
    }
    pBlock->info.rows += 1;
    if (pBlock->info.rows + 1 >= pBlock->info.capacity) {
      break;
    }
  }
  if ((*Ite) == NULL) {
    taosHashClear(pStDeleted);
  }
}

static void rebuildTimeWindow(SStreamSessionAggOperatorInfo* pInfo, SArray* pWinArray, int32_t groupId,
                              int32_t numOfOutput, SOperatorInfo* pOperator) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int32_t size = taosArrayGetSize(pWinArray);
  ASSERT(pInfo->pChildren);

  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo* pParentWin = taosArrayGet(pWinArray, i);
    SResultRow*        pCurResult = NULL;
    setWindowOutputBuf(pParentWin, &pCurResult, pSup->pCtx, groupId, numOfOutput, pSup->rowEntryInfoOffset,
                       &pInfo->streamAggSup, pTaskInfo);
    int32_t numOfChildren = taosArrayGetSize(pInfo->pChildren);
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
          SResultRow* pChResult = NULL;
          setWindowOutputBuf(pChWin, &pChResult, pChild->exprSupp.pCtx, groupId, numOfOutput,
                             pChild->exprSupp.rowEntryInfoOffset, &pChInfo->streamAggSup, pTaskInfo);
          compactFunctions(pSup->pCtx, pChild->exprSupp.pCtx, numOfOutput, pTaskInfo);
          SFilePage* bufPage = getBufPage(pChInfo->streamAggSup.pResultBuf, pChWin->pos.pageId);
          releaseBufPage(pChInfo->streamAggSup.pResultBuf, bufPage);
          continue;
        } else if (!pChWin->isClosed) {
          break;
        }
      }
    }
    SFilePage* bufPage = getBufPage(pInfo->streamAggSup.pResultBuf, pParentWin->pos.pageId);
    ASSERT(size > 0);
    setBufPageDirty(bufPage, true);
    releaseBufPage(pInfo->streamAggSup.pResultBuf, bufPage);
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
    taosHashPut(pStDeleted, &pWinInfo->pos, sizeof(SResultRowPosition), &pWinInfo->win.skey, sizeof(TSKEY));
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
  SArray*        pUpdated = taosArrayInit(16, POINTER_BYTES);
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, IS_FINAL_OP(pInfo) ? "final session recv" : "single session recv");

    if (pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doClearSessionWindows(&pInfo->streamAggSup, &pOperator->exprSupp, pBlock, 0, pOperator->exprSupp.numOfExprs, 0,
                            pWins);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                        childIndex = getChildIndex(pBlock);
        SOperatorInfo*                 pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        doClearSessionWindows(&pChildInfo->streamAggSup, &pChildOp->exprSupp, pBlock, 0, pChildOp->exprSupp.numOfExprs,
                              0, NULL);
        rebuildTimeWindow(pInfo, pWins, pBlock->info.groupId, pOperator->exprSupp.numOfExprs, pOperator);
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
        rebuildTimeWindow(pInfo, pWins, pBlock->info.groupId, pOperator->exprSupp.numOfExprs, pOperator);
      }
      copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
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
          longjmp(pOperator->pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
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
  cleanupResultRowInfo(&pInfo->binfo.resultRowInfo);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
}

static void removeSessionResults(SHashObj* pHashMap, SArray* pWins) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo* pWin = taosArrayGet(pWins, i);
    taosHashRemove(pHashMap, &pWin->pos, sizeof(SResultRowPosition));
  }
}

static SSDataBlock* doStreamSessionSemiAgg(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  TSKEY                          maxTs = INT64_MIN;
  SExprSupp*                     pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
    if (pBInfo->pRes->info.rows > 0) {
      printDataBlock(pBInfo->pRes, "sems session");
      return pBInfo->pRes;
    }

    // doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0 && !pInfo->returnDelete) {
      pInfo->returnDelete = true;
      printDataBlock(pInfo->pDelRes, "sems session");
      return pInfo->pDelRes;
    }

    if (pInfo->pUpdateRes->info.rows > 0) {
      // process the rest of the data
      pOperator->status = OP_OPENED;
      printDataBlock(pInfo->pUpdateRes, "sems session");
      return pInfo->pUpdateRes;
    }
    // semi interval operator clear disk buffer
    clearStreamSessionOperator(pInfo);
    pOperator->status = OP_EXEC_DONE;
    return NULL;
  }

  _hash_fn_t     hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*      pStUpdated = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SArray*        pUpdated = taosArrayInit(16, POINTER_BYTES);
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      clearSpecialDataBlock(pInfo->pUpdateRes);
      break;
    }

    if (pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doClearSessionWindows(&pInfo->streamAggSup, pSup, pBlock, 0, pSup->numOfExprs, 0, pWins);
      removeSessionResults(pStUpdated, pWins);
      taosArrayDestroy(pWins);
      copyUpdateDataBlock(pInfo->pUpdateRes, pBlock, pInfo->primaryTsIndex);
      break;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT) {
      // gap must be 0
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, 0, NULL, NULL);
      copyDataBlock(pInfo->pDelRes, pBlock);
      pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;
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
    doStreamSessionAggImpl(pOperator, pBlock, pStUpdated, pInfo->pStDeleted, false);
    maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pBInfo->pRes->info.watermark = pInfo->twAggSup.maxTs;
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  // semi operator
  // closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pUpdated,
  //                    getResWinForSession);
  copyUpdateResult(pStUpdated, pUpdated);
  taosHashCleanup(pStUpdated);

  finalizeUpdatedResult(pOperator->exprSupp.numOfExprs, pInfo->streamAggSup.pResultBuf, pUpdated,
                        pSup->rowEntryInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);

  doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->streamAggSup.pResultBuf);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, "sems session");
    return pBInfo->pRes;
  }

  // doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0 && !pInfo->returnDelete) {
    pInfo->returnDelete = true;
    printDataBlock(pInfo->pDelRes, "sems session");
    return pInfo->pDelRes;
  }

  if (pInfo->pUpdateRes->info.rows > 0) {
    // process the rest of the data
    pOperator->status = OP_OPENED;
    printDataBlock(pInfo->pUpdateRes, "sems session");
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
    pInfo->pUpdateRes = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
    pInfo->pUpdateRes->info.type = STREAM_CLEAR;
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
    destroyStreamSessionAggOperatorInfo(pInfo, pOperator->exprSupp.numOfExprs);
  }

  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyStreamStateOperatorInfo(void* param, int32_t numOfOutput) {
  SStreamStateAggOperatorInfo* pInfo = (SStreamStateAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStateStreamAggSupporter(&pInfo->streamAggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, i);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      destroyStreamSessionAggOperatorInfo(pChInfo, numOfOutput);
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

int32_t updateStateWindowInfo(SArray* pWinInfos, int32_t winIndex, TSKEY* pTs, SColumnInfoData* pKeyCol, int32_t rows,
                              int32_t start, bool* allEqual, SHashObj* pSeDelete) {
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
      if (pSeDelete && pWinInfo->winInfo.isOutput) {
        taosHashPut(pSeDelete, &pWinInfo->winInfo.pos, sizeof(SResultRowPosition), &pWinInfo->winInfo.win.skey,
                    sizeof(TSKEY));
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

static void doClearStateWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, int32_t tsIndex, SColumn* pCol,
                                int32_t keyIndex, SHashObj* pSeUpdated, SHashObj* pSeDeleted) {
  SColumnInfoData* pTsColInfo = taosArrayGet(pBlock->pDataBlock, tsIndex);
  SColumnInfoData* pKeyColInfo = taosArrayGet(pBlock->pDataBlock, keyIndex);
  TSKEY*           tsCol = (TSKEY*)pTsColInfo->pData;
  bool             allEqual = false;
  int32_t          step = 1;
  for (int32_t i = 0; i < pBlock->info.rows; i += step) {
    char*             pKeyData = colDataGetData(pKeyColInfo, i);
    int32_t           winIndex = 0;
    SStateWindowInfo* pCurWin = getStateWindowByTs(pAggSup, tsCol[i], pBlock->info.groupId, &winIndex);
    if (!pCurWin) {
      continue;
    }
    step = updateStateWindowInfo(pAggSup->pCurWins, winIndex, tsCol, pKeyColInfo, pBlock->info.rows, i, &allEqual,
                                 pSeDeleted);
    ASSERT(isTsInWindow(pCurWin, tsCol[i]) || isEqualStateKey(pCurWin, pKeyData));
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
    SStateWindowInfo* pCurWin =
        getStateWindow(pAggSup, tsCols[i], pSDataBlock->info.groupId, pKeyData, &pInfo->stateCol, &winIndex);
    winRows = updateStateWindowInfo(pAggSup->pCurWins, winIndex, tsCols, pKeyColInfo, pSDataBlock->info.rows, i,
                                    &allEqual, pInfo->pSeDeleted);
    if (!allEqual) {
      appendOneRow(pAggSup->pScanBlock, &pCurWin->winInfo.win.skey, &pCurWin->winInfo.win.ekey,
                   &pSDataBlock->info.groupId);
      taosHashRemove(pSeUpdated, &pCurWin->winInfo.pos, sizeof(SResultRowPosition));
      deleteWindow(pAggSup->pCurWins, winIndex, destroyStateWinInfo);
      continue;
    }
    code = doOneStateWindowAgg(pInfo, pSDataBlock, &pCurWin->winInfo, &pResult, i, winRows, numOfOutput, pOperator);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    pCurWin->winInfo.isClosed = false;
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      SWinRes value = {.ts = pCurWin->winInfo.win.skey, .groupId = groupId};
      code = taosHashPut(pSeUpdated, &pCurWin->winInfo.pos, sizeof(SResultRowPosition), &value, sizeof(SWinRes));
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
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
      doClearStateWindows(&pInfo->streamAggSup, pBlock, pInfo->primaryTsIndex, &pInfo->stateCol, pInfo->stateCol.slotId,
                          pSeUpdated, pInfo->pSeDeleted);
      continue;
    } else if (pBlock->info.type == STREAM_DELETE_DATA) {
      SArray* pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, 0, pWins, destroyStateWinInfo);
      copyDeleteWindowInfo(pWins, pInfo->pSeDeleted);
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
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
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
  // pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pDelRes = createOneDataBlock(pInfo->binfo.pRes, false);  // todo(liuyao) for delete
  pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;               // todo(liuyao) for delete
  pInfo->pChildren = NULL;
  pInfo->ignoreExpiredData = pStateNode->window.igExpired;

  pOperator->name = "StreamStateAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->info = pInfo;
  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doStreamStateAgg, NULL, NULL,
                                         destroyStreamStateOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);
  initDownStream(downstream, &pInfo->streamAggSup, 0, pInfo->twAggSup.waterMark, pOperator->operatorType);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  destroyStreamStateOperatorInfo(pInfo, numOfCols);
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroyMergeAlignedIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = (SMergeAlignedIntervalAggOperatorInfo*)param;
  destroyIntervalOperatorInfo(miaInfo->intervalAggOperatorInfo, numOfOutput);

  taosMemoryFreeClear(param);
}

static int32_t outputMergeAlignedIntervalResult(SOperatorInfo* pOperatorInfo, uint64_t tableGroupId,
                                                SSDataBlock* pResultBlock, TSKEY wstartTs) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;

  SIntervalAggOperatorInfo* iaInfo = miaInfo->intervalAggOperatorInfo;
  SExecTaskInfo*            pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*                pSup = &pOperatorInfo->exprSupp;

  SET_RES_WINDOW_KEY(iaInfo->aggSup.keyBuf, &wstartTs, TSDB_KEYSIZE, tableGroupId);
  SResultRowPosition* p1 = (SResultRowPosition*)taosHashGet(iaInfo->aggSup.pResultRowHashTable, iaInfo->aggSup.keyBuf,
                                                            GET_RES_WINDOW_KEY_LEN(TSDB_KEYSIZE));
  ASSERT(p1 != NULL);

  finalizeResultRowIntoResultDataBlock(iaInfo->aggSup.pResultBuf, p1, pSup->pCtx, pSup->pExprInfo, pSup->numOfExprs,
                                       pSup->rowEntryInfoOffset, pResultBlock, pTaskInfo);
  taosHashRemove(iaInfo->aggSup.pResultRowHashTable, iaInfo->aggSup.keyBuf, GET_RES_WINDOW_KEY_LEN(TSDB_KEYSIZE));
  ASSERT(taosHashGetSize(iaInfo->aggSup.pResultRowHashTable) == 0);

  return TSDB_CODE_SUCCESS;
}

static void doMergeAlignedIntervalAggImpl(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo,
                                          SSDataBlock* pBlock, int32_t scanFlag, SSDataBlock* pResultBlock) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*             iaInfo = miaInfo->intervalAggOperatorInfo;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;

  int32_t     startPos = 0;
  int32_t     numOfOutput = pSup->numOfExprs;
  int64_t*    tsCols = extractTsCol(pBlock, iaInfo);
  uint64_t    tableGroupId = pBlock->info.groupId;
  TSKEY       currTs = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow* pResult = NULL;

  // there is an result exists
  if (miaInfo->curTs != INT64_MIN) {
    ASSERT(taosHashGetSize(iaInfo->aggSup.pResultRowHashTable) == 1);
    if (currTs != miaInfo->curTs) {
      outputMergeAlignedIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, miaInfo->curTs);
      miaInfo->curTs = INT64_MIN;
    }
  }

  STimeWindow win = {0};
  win.skey = currTs;
  win.ekey =
      taosTimeAdd(win.skey, iaInfo->interval.interval, iaInfo->interval.intervalUnit, iaInfo->interval.precision) - 1;

  // TODO: remove the hash table (groupid + winkey => result row position)
  int32_t ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  miaInfo->curTs = win.skey;
  int32_t currPos = startPos;

  STimeWindow currWin = win;
  while (++currPos < pBlock->info.rows) {
    if (tsCols[currPos] == currTs) {
      continue;
    }

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, true);
    doApplyFunctions(pTaskInfo, pSup->pCtx, &currWin, &iaInfo->twAggSup.timeWindowData, startPos, currPos - startPos,
                     tsCols, pBlock->info.rows, numOfOutput, iaInfo->inputOrder);

    outputMergeAlignedIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, currTs);
    miaInfo->curTs = INT64_MIN;

    currTs = tsCols[currPos];
    currWin.skey = currTs;
    currWin.ekey = taosTimeAdd(currWin.skey, iaInfo->interval.interval, iaInfo->interval.intervalUnit,
                               iaInfo->interval.precision) -
                   1;

    startPos = currPos;
    ret = setTimeWindowOutputBuf(pResultRowInfo, &currWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                 numOfOutput, pSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    miaInfo->curTs = currWin.skey;
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, true);
  doApplyFunctions(pTaskInfo, pSup->pCtx, &currWin, &iaInfo->twAggSup.timeWindowData, startPos, currPos - startPos,
                   tsCols, pBlock->info.rows, numOfOutput, iaInfo->inputOrder);

  if (currPos >= pBlock->info.rows) {
    // we need to see next block if exists
  } else {
    ASSERT(0);
    outputMergeAlignedIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, currTs);
    miaInfo->curTs = INT64_MIN;
  }
}

static SSDataBlock* doMergeAlignedIntervalAgg(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SMergeAlignedIntervalAggOperatorInfo* miaInfo = pOperator->info;
  SIntervalAggOperatorInfo*             iaInfo = miaInfo->intervalAggOperatorInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExprSupp*   pSup = &pOperator->exprSupp;
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
        // close last unfinalized time window
        if (miaInfo->curTs != INT64_MIN) {
          ASSERT(taosHashGetSize(iaInfo->aggSup.pResultRowHashTable) == 1);
          outputMergeAlignedIntervalResult(pOperator, miaInfo->groupId, pRes, miaInfo->curTs);
          miaInfo->curTs = INT64_MIN;
        }

        doSetOperatorCompleted(pOperator);
        miaInfo->inputBlocksFinished = true;
        break;
      }

      if (!miaInfo->hasGroupId) {
        miaInfo->hasGroupId = true;
        miaInfo->groupId = pBlock->info.groupId;
      } else if (miaInfo->groupId != pBlock->info.groupId) {
        // if there are unclosed time window, close it firstly.
        ASSERT(miaInfo->curTs != INT64_MIN);
        outputMergeAlignedIntervalResult(pOperator, miaInfo->groupId, pRes, miaInfo->curTs);
        miaInfo->prefetchedBlock = pBlock;
        miaInfo->curTs = INT64_MIN;
        break;
      }

      getTableScanInfo(pOperator, &iaInfo->inputOrder, &scanFlag);
      setInputDataBlock(pOperator, pSup->pCtx, pBlock, iaInfo->inputOrder, scanFlag, true);
      doMergeAlignedIntervalAggImpl(pOperator, &iaInfo->binfo.resultRowInfo, pBlock, scanFlag, pRes);
      doFilter(miaInfo->pCondition, pRes, NULL);
      if (pRes->info.rows >= pOperator->resultInfo.capacity) {
        break;
      }
    }

    pRes->info.groupId = miaInfo->groupId;
  }

  miaInfo->hasGroupId = false;

  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;
  return (rows == 0) ? NULL : pRes;
}

SOperatorInfo* createMergeAlignedIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo,
                                                      int32_t numOfCols, SSDataBlock* pResBlock, SInterval* pInterval,
                                                      int32_t primaryTsSlotId, SNode* pCondition,
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

  SIntervalAggOperatorInfo* iaInfo = miaInfo->intervalAggOperatorInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;

  miaInfo->pCondition = pCondition;
  miaInfo->curTs = INT64_MIN;

  iaInfo->win = pTaskInfo->window;
  iaInfo->inputOrder = TSDB_ORDER_ASC;
  iaInfo->interval = *pInterval;
  iaInfo->execModel = pTaskInfo->execModel;
  iaInfo->primaryTsIndex = primaryTsSlotId;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t code =
      initAggInfo(&pOperator->exprSupp, &iaInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  initBasicInfo(&iaInfo->binfo, pResBlock);

  initExecTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &iaInfo->win);

  iaInfo->timeWindowInterpo = timeWindowinterpNeeded(pSup->pCtx, numOfCols, iaInfo);
  if (iaInfo->timeWindowInterpo) {
    iaInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SResultRowPosition));
  }

  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&iaInfo->binfo.resultRowInfo);

  pOperator->name = "TimeMergeAlignedIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = miaInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doMergeAlignedIntervalAgg, NULL, NULL,
                                         destroyMergeAlignedIntervalOperatorInfo, NULL, NULL, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyMergeAlignedIntervalOperatorInfo(miaInfo, numOfCols);
  taosMemoryFreeClear(miaInfo);
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

void destroyMergeIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  SMergeIntervalAggOperatorInfo* miaInfo = (SMergeIntervalAggOperatorInfo*)param;
  tdListFree(miaInfo->groupIntervals);
  destroyIntervalOperatorInfo(&miaInfo->intervalAggOperatorInfo, numOfOutput);

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
  SResultRowPosition* p1 = (SResultRowPosition*)taosHashGet(iaInfo->aggSup.pResultRowHashTable, iaInfo->aggSup.keyBuf,
                                                            GET_RES_WINDOW_KEY_LEN(TSDB_KEYSIZE));
  ASSERT(p1 != NULL);
  finalizeResultRowIntoResultDataBlock(iaInfo->aggSup.pResultBuf, p1, pExprSup->pCtx, pExprSup->pExprInfo,
                                       pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset, pResultBlock, pTaskInfo);
  taosHashRemove(iaInfo->aggSup.pResultRowHashTable, iaInfo->aggSup.keyBuf, GET_RES_WINDOW_KEY_LEN(TSDB_KEYSIZE));
  return TSDB_CODE_SUCCESS;
}

static int32_t outputPrevIntervalResult(SOperatorInfo* pOperatorInfo, uint64_t tableGroupId, SSDataBlock* pResultBlock,
                                        STimeWindow* newWin) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  SExecTaskInfo*                 pTaskInfo = pOperatorInfo->pTaskInfo;
  bool                           ascScan = (iaInfo->inputOrder == TSDB_ORDER_ASC);
  SExprSupp*                     pExprSup = &pOperatorInfo->exprSupp;

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
      finalizeWindowResult(pOperatorInfo, tableGroupId, prevWin, pResultBlock);
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
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, iaInfo->inputOrder);
  ASSERT(forwardRows > 0);

  // prev time window not interpolation yet.
  if (iaInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                                 numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    // window start key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &win, startPos, forwardRows, pExprSup);
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &win, true);
  doApplyFunctions(pTaskInfo, pExprSup->pCtx, &win, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows, tsCols,
                   pBlock->info.rows, numOfOutput, iaInfo->inputOrder);
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
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, iaInfo->inputOrder);

    // window start(end) key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pExprSup);

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pExprSup->pCtx, &nextWin, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows,
                     tsCols, pBlock->info.rows, numOfOutput, iaInfo->inputOrder);
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
      finalizeWindowResult(pOperator, grpWin->groupId, &grpWin->window, pRes);
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

SOperatorInfo* createMergeIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                               SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                               SExecTaskInfo* pTaskInfo) {
  SMergeIntervalAggOperatorInfo* miaInfo = taosMemoryCalloc(1, sizeof(SMergeIntervalAggOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (miaInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  miaInfo->groupIntervals = tdListNew(sizeof(SGroupTimeWindow));

  SIntervalAggOperatorInfo* iaInfo = &miaInfo->intervalAggOperatorInfo;
  iaInfo->win = pTaskInfo->window;
  iaInfo->inputOrder = TSDB_ORDER_ASC;
  iaInfo->interval = *pInterval;
  iaInfo->execModel = pTaskInfo->execModel;

  iaInfo->primaryTsIndex = primaryTsSlotId;

  SExprSupp* pExprSupp = &pOperator->exprSupp;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t code = initAggInfo(pExprSupp, &iaInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  initBasicInfo(&iaInfo->binfo, pResBlock);

  initExecTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &iaInfo->win);

  iaInfo->timeWindowInterpo = timeWindowinterpNeeded(pExprSupp->pCtx, numOfCols, iaInfo);
  if (iaInfo->timeWindowInterpo) {
    iaInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SResultRowPosition));
    if (iaInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }

  initResultRowInfo(&iaInfo->binfo.resultRowInfo);

  pOperator->name = "TimeMergeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->info = miaInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doMergeIntervalAgg, NULL, NULL,
                                         destroyMergeIntervalOperatorInfo, NULL, NULL, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyMergeIntervalOperatorInfo(miaInfo, numOfCols);
  taosMemoryFreeClear(miaInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}
