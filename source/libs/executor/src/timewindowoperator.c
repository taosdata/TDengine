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
#include "tchecksum.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

typedef struct SSessionAggOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSupp;         // supporter for perform scalar function
  SGroupResInfo      groupResInfo;
  SWindowRowsSup     winSup;
  bool               reptScan;  // next round scan
  int64_t            gap;       // session window gap
  int32_t            tsSlotId;  // primary timestamp slot id
  STimeWindowAggSupp twAggSup;
} SSessionAggOperatorInfo;

typedef struct SStateWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  SGroupResInfo      groupResInfo;
  SWindowRowsSup     winSup;
  SColumn            stateCol;  // start row index
  bool               hasKey;
  SStateKeys         stateKey;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
} SStateWindowOperatorInfo;

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP = 2,
} SResultTsInterpType;

typedef struct SOpenWindowInfo {
  SResultRowPosition pos;
  uint64_t           groupId;
} SOpenWindowInfo;

static int64_t* extractTsCol(SSDataBlock* pBlock, const SIntervalAggOperatorInfo* pInfo);

static SResultRowPosition addToOpenWindowList(SResultRowInfo* pResultRowInfo, const SResultRow* pResult,
                                              uint64_t groupId);
static void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult);

static int32_t setTimeWindowOutputBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, bool masterscan,
                                      SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                      int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup,
                                      SExecTaskInfo* pTaskInfo) {
  SResultRow* pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                                  masterscan, tableGroupId, pTaskInfo, true, pAggSup, true);

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

FORCE_INLINE int32_t getForwardStepsInBlock(int32_t numOfRows, __block_search_fn_t searchFn, TSKEY ekey, int32_t pos,
                                            int32_t order, int64_t* pData) {
  int32_t forwardRows = 0;

  if (order == TSDB_ORDER_ASC) {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardRows = end;

      while (pData[end + pos] == ekey) {
        forwardRows += 1;
        ++pos;
      }
    }
  } else {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardRows = end;

      while (pData[end + pos] == ekey) {
        forwardRows += 1;
        ++pos;
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

  ASSERT(forwardRows >= 0);
  return forwardRows;
}

int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order) {
  int32_t midPos = -1;
  int32_t numOfRows;

  if (num <= 0) {
    return -1;
  }

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
  ASSERT(startPos >= 0 && startPos < pDataBlockInfo->rows);

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

  return num;
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

        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_VARBINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR ||
            pColInfo->info.type == TSDB_DATA_TYPE_GEOMETRY) {
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
  bool ascQuery = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);

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
  int32_t order = pInfo->binfo.inputTsOrder;

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
  ASSERT(nextRowIndex >= 0);

  TSKEY nextKey = tsCols[nextRowIndex];
  doTimeWindowInterpolation(pInfo->pPrevValues, pDataBlock, actualEndKey, endRowIndex, nextKey, nextRowIndex, key,
                            RESULT_ROW_END_INTERP, pSup);
  return true;
}

bool inCalSlidingWindow(SInterval* pInterval, STimeWindow* pWin, TSKEY calStart, TSKEY calEnd, EStreamType blockType) {
  if (pInterval->interval != pInterval->sliding &&
      ((pWin->ekey < calStart || pWin->skey > calEnd) || (blockType == STREAM_PULL_DATA && pWin->skey < calStart))) {
    return false;
  }

  return true;
}

bool inSlidingWindow(SInterval* pInterval, STimeWindow* pWin, SDataBlockInfo* pBlockInfo) {
  return inCalSlidingWindow(pInterval, pWin, pBlockInfo->calWin.skey, pBlockInfo->calWin.ekey, pBlockInfo->type);
}

int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                               TSKEY* primaryKeys, int32_t prevPosition, int32_t order) {
  bool ascQuery = (order == TSDB_ORDER_ASC);

  int32_t precision = pInterval->precision;
  getNextTimeWindow(pInterval, pNext, order);

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
      ASSERT(pDataBlockInfo->window.skey <= pNext->ekey);
    } else {
      ASSERT(pDataBlockInfo->window.ekey >= pNext->skey);
    }
  } else {
    if (ascQuery && primaryKeys[startPos] > pNext->ekey) {
      TSKEY next = primaryKeys[startPos];
      if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, pInterval);
        pNext->ekey = taosTimeGetIntervalEnd(pNext->skey, pInterval);
      } else {
        pNext->ekey += ((next - pNext->ekey + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
        pNext->skey = pNext->ekey - pInterval->interval + 1;
      }
    } else if ((!ascQuery) && primaryKeys[startPos] < pNext->skey) {
      TSKEY next = primaryKeys[startPos];
      if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, pInterval);
        pNext->ekey = taosTimeGetIntervalEnd(pNext->skey, pInterval);
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

    TSKEY endKey = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC) ? pBlock->info.window.ekey : pBlock->info.window.skey;
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
    if (NULL == pr) {
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

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
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    ASSERT(!isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

    SGroupKeys* pTsKey = taosArrayGet(pInfo->pPrevValues, 0);
    int64_t     prevTs = *(int64_t*)pTsKey->pData;
    if (groupId == pBlock->info.id.groupId) {
      doTimeWindowInterpolation(pInfo->pPrevValues, pBlock->pDataBlock, prevTs, -1, tsCols[startPos], startPos, w.ekey,
                                RESULT_ROW_END_INTERP, pSup);
    }

    setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    setNotInterpoWindowKey(pSup->pCtx, numOfExprs, RESULT_ROW_START_INTERP);

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &w, 1);
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, 0,
                                    pBlock->info.rows, numOfExprs);

    if (isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
      closeResultRow(pr);
      SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
      taosMemoryFree(pNode);
    } else {  // the remains are can not be closed yet.
      break;
    }
  }
}

static bool tsKeyCompFn(void* l, void* r, void* param) {
  TSKEY*                    lTS = (TSKEY*)l;
  TSKEY*                    rTS = (TSKEY*)r;
  SIntervalAggOperatorInfo* pInfo = param;
  return pInfo->binfo.outputTsOrder == ORDER_ASC ? *lTS < *rTS : *lTS > *rTS;
}

static bool isCalculatedWin(SIntervalAggOperatorInfo* pInfo, const STimeWindow* win, uint64_t tableGroupId) {
  char keyBuf[sizeof(TSKEY) + sizeof(uint64_t)] = {0};
  SET_RES_WINDOW_KEY(keyBuf, (char*)&win->skey, sizeof(TSKEY), tableGroupId);
  return tSimpleHashGet(pInfo->aggSup.pResultRowHashTable, keyBuf, GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY))) != NULL;
}

/**
 * @brief check if cur window should be filtered out by limit info
 * @retval true if should be filtered out
 * @retval false if not filtering out
 * @note If no limit info, we skip filtering.
 *       If input/output ts order mismatch, we skip filtering too.
 *       eg. input ts order: desc, and output ts order: asc, limit: 10
 *       IntervalOperator should output the first 10 windows, however, we can't find the first 10 windows until we scan
 *       every tuple in every block.
 *       And the boundedQueue keeps refreshing all records with smaller ts key.
 */
static bool filterWindowWithLimit(SIntervalAggOperatorInfo* pOperatorInfo, STimeWindow* win, uint64_t groupId) {
  if (!pOperatorInfo->limited  // if no limit info, no filter will be applied
      || pOperatorInfo->binfo.inputTsOrder !=
             pOperatorInfo->binfo.outputTsOrder  // if input/output ts order mismatch, no filter
  ) {
    return false;
  }
  if (pOperatorInfo->limit == 0) return true;

  if (pOperatorInfo->pBQ == NULL) {
    pOperatorInfo->pBQ = createBoundedQueue(pOperatorInfo->limit - 1, tsKeyCompFn, taosMemoryFree, pOperatorInfo);
  }

  bool shouldFilter = false;
  // if BQ has been full, compare it with top of BQ
  if (taosBQSize(pOperatorInfo->pBQ) == taosBQMaxSize(pOperatorInfo->pBQ) + 1) {
    PriorityQueueNode* top = taosBQTop(pOperatorInfo->pBQ);
    shouldFilter = tsKeyCompFn(top->data, &win->skey, pOperatorInfo);
  }
  if (shouldFilter) {
    return true;
  } else if (isCalculatedWin(pOperatorInfo, win, groupId)) {
    return false;
  }

  // cur win not been filtered out and not been pushed into BQ yet, push it into BQ
  PriorityQueueNode node = {.data = taosMemoryMalloc(sizeof(TSKEY))};
  *((TSKEY*)node.data) = win->skey;

  if (NULL == taosBQPush(pOperatorInfo->pBQ, &node)) {
    taosMemoryFree(node.data);
    return true;
  }
  return false;
}

static bool hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                            int32_t scanFlag) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*     pSup = &pOperatorInfo->exprSupp;

  int32_t     startPos = 0;
  int32_t     numOfOutput = pSup->numOfExprs;
  int64_t*    tsCols = extractTsCol(pBlock, pInfo);
  uint64_t    tableGroupId = pBlock->info.id.groupId;
  bool        ascScan = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);
  TSKEY       ts = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow* pResult = NULL;

  if (tableGroupId != pInfo->curGroupId) {
    pInfo->handledGroupNum += 1;
    if (pInfo->slimited && pInfo->handledGroupNum > pInfo->slimit) {
      return true;
    } else {
      pInfo->curGroupId = tableGroupId;
      destroyBoundedQueue(pInfo->pBQ);
      pInfo->pBQ = NULL;
    }
  }

  STimeWindow win =
      getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, pInfo->binfo.inputTsOrder);
  if (filterWindowWithLimit(pInfo, &win, tableGroupId)) return false;

  int32_t ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                                 pInfo->binfo.inputTsOrder);

  // prev time window not interpolation yet.
  if (pInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult, tableGroupId);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pSup->pCtx,
                                 numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    // window start key interpolation
    doWindowBorderInterpolation(pInfo, pBlock, pResult, &win, startPos, forwardRows, pSup);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, 1);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                  pBlock->info.rows, numOfOutput);

  doCloseWindow(pResultRowInfo, pInfo, pResult);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos,
                                      pInfo->binfo.inputTsOrder);
    if (startPos < 0 || filterWindowWithLimit(pInfo, &nextWin, tableGroupId)) {
      break;
    }
    // null data, failed to allocate more memory buffer
    int32_t code = setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                          pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                           pInfo->binfo.inputTsOrder);
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
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, 1);
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                    pBlock->info.rows, numOfOutput);
    doCloseWindow(pResultRowInfo, pInfo, pResult);
  }

  if (pInfo->timeWindowInterpo) {
    saveDataBlockLastRow(pInfo->pPrevValues, pBlock, pInfo->pInterpCols);
  }
  return false;
}

void doCloseWindow(SResultRowInfo* pResultRowInfo, const SIntervalAggOperatorInfo* pInfo, SResultRow* pResult) {
  // current result is done in computing final results.
  if (pInfo->timeWindowInterpo && isResultRowInterpolated(pResult, RESULT_ROW_END_INTERP)) {
    closeResultRow(pResult);
    SListNode* pNode = tdListPopHead(pResultRowInfo->openWindow);
    taosMemoryFree(pNode);
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

  if (pBlock->pDataBlock != NULL && pBlock->info.dataLoad) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
    ASSERT(tsCols[0] != 0);

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

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExprSupp*                pSup = &pOperator->exprSupp;

  int32_t scanFlag = MAIN_SCAN;
  int64_t st = taosGetTimestampUs();

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, pInfo->binfo.inputTsOrder, scanFlag, true);
    if (hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, scanFlag)) break;
  }

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, pInfo->binfo.outputTsOrder);
  OPTR_SET_OPENED(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return TSDB_CODE_SUCCESS;
}

static void doStateWindowAggImpl(SOperatorInfo* pOperator, SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*     pSup = &pOperator->exprSupp;

  SColumnInfoData* pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->stateCol.slotId);
  int64_t          gid = pBlock->info.id.groupId;

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->exprSupp.numOfExprs;
  int32_t bytes = pStateColInfoData->info.bytes;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;
  pRowSup->startRowIndex = 0;

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
    } else {  // a new state window started
      SResultRow* pResult = NULL;

      // keep the time window for the closed time window.
      STimeWindow window = pRowSup->win;

      pRowSup->win.ekey = pRowSup->win.skey;
      int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pSup->pCtx,
                                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, 0);
      applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
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
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                  pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
}

static int32_t openStateWindowAggOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SStateWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  int32_t    order = pInfo->binfo.inputTsOrder;
  int64_t    st = taosGetTimestampUs();

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
    setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
    }

    doStateWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  pOperator->status = OP_RES_TO_RETURN;

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doStateWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SStateWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*           pBInfo = &pInfo->binfo;

  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    setOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      setOperatorCompleted(pOperator);
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
  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      setOperatorCompleted(pOperator);
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

static void doClearWindowImpl(SResultRowPosition* p1, SDiskbasedBuf* pResultBuf, SExprSupp* pSup, int32_t numOfOutput) {
  SResultRow* pResult = getResultRowByPos(pResultBuf, p1, false);
  if (NULL == pResult) {
    return;
  }

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
  if (NULL == bufPage) {
    return;
  }
  setBufPageDirty(bufPage, true);
  releaseBufPage(pResultBuf, bufPage);
}

static void destroyStateWindowOperatorInfo(void* param) {
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->stateKey.pData);
  cleanupExprSupp(&pInfo->scalarSup);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  cleanupAggSup(&pInfo->aggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);

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

  pInfo->pInterpCols = taosArrayDestroy(pInfo->pInterpCols);
  taosArrayDestroyEx(pInfo->pPrevValues, freeItem);

  pInfo->pPrevValues = NULL;

  cleanupGroupResInfo(&pInfo->groupResInfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  destroyBoundedQueue(pInfo->pBQ);
  taosMemoryFreeClear(param);
}

static bool timeWindowinterpNeeded(SqlFunctionCtx* pCtx, int32_t numOfCols, SIntervalAggOperatorInfo* pInfo) {
  // the primary timestamp column
  bool needed = false;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = pCtx[i].pExpr;
    if (fmIsIntervalInterpoFunc(pCtx[i].functionId)) {
      needed = true;
      break;
    }
  }

  if (needed) {
    pInfo->pInterpCols = taosArrayInit(4, sizeof(SColumn));
    pInfo->pPrevValues = taosArrayInit(4, sizeof(SGroupKeys));

    {  // ts column
      SColumn c = {0};
      c.colId = 1;
      c.slotId = pInfo->primaryTsIndex;
      c.type = TSDB_DATA_TYPE_TIMESTAMP;
      c.bytes = sizeof(int64_t);
      taosArrayPush(pInfo->pInterpCols, &c);

      SGroupKeys key;
      key.bytes = c.bytes;
      key.type = c.type;
      key.isNull = true;  // to denote no value is assigned yet
      key.pData = taosMemoryCalloc(1, c.bytes);
      taosArrayPush(pInfo->pPrevValues, &key);
    }
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = pCtx[i].pExpr;

    if (fmIsIntervalInterpoFunc(pCtx[i].functionId)) {
      SFunctParam* pParam = &pExpr->base.pParam[0];

      SColumn c = *pParam->pCol;
      taosArrayPush(pInfo->pInterpCols, &c);

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

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SIntervalPhysiNode* pPhyNode,
                                          SExecTaskInfo* pTaskInfo) {
  SIntervalAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->window.node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  SExprSupp* pSup = &pOperator->exprSupp;
  pInfo->primaryTsIndex = ((SColumnNode*)pPhyNode->window.pTspk)->slotId;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 512);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->window.pFuncs, NULL, &num);
  int32_t    code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                               pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SInterval interval = {.interval = pPhyNode->interval,
                        .sliding = pPhyNode->sliding,
                        .intervalUnit = pPhyNode->intervalUnit,
                        .slidingUnit = pPhyNode->slidingUnit,
                        .offset = pPhyNode->offset,
                        .precision = ((SColumnNode*)pPhyNode->window.pTspk)->node.resType.precision};

  STimeWindowAggSupp as = {
      .waterMark = pPhyNode->window.watermark,
      .calTrigger = pPhyNode->window.triggerType,
      .maxTs = INT64_MIN,
  };

  pInfo->win = pTaskInfo->window;
  pInfo->binfo.inputTsOrder = pPhyNode->window.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pPhyNode->window.node.outputTsOrder;
  pInfo->interval = interval;
  pInfo->twAggSup = as;
  pInfo->binfo.mergeResultBlock = pPhyNode->window.mergeDataBlock;
  if (pPhyNode->window.node.pLimit) {
    SLimitNode* pLimit = (SLimitNode*)pPhyNode->window.node.pLimit;
    pInfo->limited = true;
    pInfo->limit = pLimit->limit + pLimit->offset;
  }
  if (pPhyNode->window.node.pSlimit) {
    SLimitNode* pLimit = (SLimitNode*)pPhyNode->window.node.pSlimit;
    pInfo->slimited = true;
    pInfo->slimit = pLimit->limit + pLimit->offset;
    pInfo->curGroupId = UINT64_MAX;
  }

  if (pPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pPhyNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = filterInitFromNode((SNode*)pPhyNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);
  pInfo->timeWindowInterpo = timeWindowinterpNeeded(pSup->pCtx, num, pInfo);
  if (pInfo->timeWindowInterpo) {
    pInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
    if (pInfo->binfo.resultRowInfo.openWindow == NULL) {
      goto _error;
    }
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "TimeIntervalAggOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(doOpenIntervalAgg, doBuildIntervalResult, NULL, destroyIntervalOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyIntervalOperatorInfo(pInfo);
  }
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
  int64_t gid = pBlock->info.id.groupId;

  int64_t gap = pInfo->gap;

  if (!pInfo->reptScan) {
    pInfo->reptScan = true;
    pInfo->winSup.prevTs = INT64_MIN;
  }

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;
  pRowSup->startRowIndex = 0;

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
    } else {  // start a new session window
      SResultRow* pResult = NULL;

      // keep the time window for the closed time window.
      STimeWindow window = pRowSup->win;

      pRowSup->win.ekey = pRowSup->win.skey;
      int32_t ret = setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pSup->pCtx,
                                           numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
      }

      // pInfo->numOfRows data belong to the current session window
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, 0);
      applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
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
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, 0);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                                  pRowSup->numOfRows, pBlock->info.rows, numOfOutput);
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
      doFilter(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

      bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
      if (!hasRemain) {
        setOperatorCompleted(pOperator);
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
  int32_t order = pInfo->binfo.inputTsOrder;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pBInfo->pRes->info.scanFlag = pBlock->info.scanFlag;
    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }    
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
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
    doFilter(pBInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

    bool hasRemain = hasRemainResults(&pInfo->groupResInfo);
    if (!hasRemain) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pBInfo->pRes->info.rows > 0) {
      break;
    }
  }
  pOperator->resultInfo.totalRows += pBInfo->pRes->info.rows;
  return (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
}

// todo make this as an non-blocking operator
SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SStateWinodwPhysiNode* pStateNode,
                                             SExecTaskInfo* pTaskInfo) {
  SStateWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStateWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;
  SColumnNode* pColNode = (SColumnNode*)(pStateNode->pStateKey);

  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pStateNode->window.pExprs, NULL, &numOfScalarExpr);
    int32_t    code =
        initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  pInfo->stateKey.type = pInfo->stateCol.type;
  pInfo->stateKey.bytes = pInfo->stateCol.bytes;
  pInfo->stateKey.pData = taosMemoryCalloc(1, pInfo->stateCol.bytes);
  if (pInfo->stateKey.pData == NULL) {
    goto _error;
  }
  pInfo->binfo.inputTsOrder = pStateNode->window.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pStateNode->window.node.outputTsOrder;

  int32_t code = filterInitFromNode((SNode*)pStateNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &num);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pStateNode->window.node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->twAggSup =
      (STimeWindowAggSupp){.waterMark = pStateNode->window.watermark, .calTrigger = pStateNode->window.triggerType};

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = tsSlotId;

  setOperatorInfo(pOperator, "StateWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(openStateWindowAggOptr, doStateWindowAgg, NULL, destroyStateWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyStateWindowOperatorInfo(pInfo);
  }

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
  cleanupExprSupp(&pInfo->scalarSupp);
  
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
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pSessionNode->window.node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  int32_t code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                            pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->twAggSup.waterMark = pSessionNode->window.watermark;
  pInfo->twAggSup.calTrigger = pSessionNode->window.triggerType;
  pInfo->gap = pSessionNode->gap;

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  pInfo->binfo.pRes = pResBlock;
  pInfo->winSup.prevTs = INT64_MIN;
  pInfo->reptScan = false;
  pInfo->binfo.inputTsOrder = pSessionNode->window.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pSessionNode->window.node.outputTsOrder;

  if (pSessionNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pSessionNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = filterInitFromNode((SNode*)pSessionNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "SessionWindowAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doSessionWindowAgg, NULL, destroySWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
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

void destroyMAIOperatorInfo(void* param) {
  SMergeAlignedIntervalAggOperatorInfo* miaInfo = (SMergeAlignedIntervalAggOperatorInfo*)param;
  destroyIntervalOperatorInfo(miaInfo->intervalAggOperatorInfo);
  taosMemoryFreeClear(param);
}

static SResultRow* doSetSingleOutputTupleBuf(SResultRowInfo* pResultRowInfo, SAggSupporter* pSup) {
  SResultRow* pResult = getNewResultRow(pSup->pResultBuf, &pSup->currentPageId, pSup->resultRowSize);
  if (NULL == pResult) {
    return pResult;
  }
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

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, 1);
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos,
                                    currPos - startPos, pBlock->info.rows, pSup->numOfExprs);

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

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, 1);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, currPos - startPos,
                                  pBlock->info.rows, pSup->numOfExprs);
}

static void cleanupAfterGroupResultGen(SMergeAlignedIntervalAggOperatorInfo* pMiaInfo, SSDataBlock* pRes) {
  pRes->info.id.groupId = pMiaInfo->groupId;
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

  while (1) {
    SSDataBlock* pBlock = NULL;
    if (pMiaInfo->prefetchedBlock == NULL) {
      pBlock = getNextBlockFromDownstream(pOperator, 0);
    } else {
      pBlock = pMiaInfo->prefetchedBlock;
      pMiaInfo->prefetchedBlock = NULL;

      pMiaInfo->groupId = pBlock->info.id.groupId;
    }

    // no data exists, all query processing is done
    if (pBlock == NULL) {
      // close last unclosed time window
      if (pMiaInfo->curTs != INT64_MIN) {
        finalizeResultRows(pIaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMiaInfo->pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));
        cleanupAfterGroupResultGen(pMiaInfo, pRes);
        doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
      }

      setOperatorCompleted(pOperator);
      break;
    }

    if (pMiaInfo->groupId == 0) {
      if (pMiaInfo->groupId != pBlock->info.id.groupId) {
        pMiaInfo->groupId = pBlock->info.id.groupId;
        pRes->info.id.groupId = pMiaInfo->groupId;
      }
    } else {
      if (pMiaInfo->groupId != pBlock->info.id.groupId) {
        // if there are unclosed time window, close it firstly.
        ASSERT(pMiaInfo->curTs != INT64_MIN);
        finalizeResultRows(pIaInfo->aggSup.pResultBuf, &pResultRowInfo->cur, pSup, pRes, pTaskInfo);
        resetResultRow(pMiaInfo->pResultRow, pIaInfo->aggSup.resultRowSize - sizeof(SResultRow));

        pMiaInfo->prefetchedBlock = pBlock;
        cleanupAfterGroupResultGen(pMiaInfo, pRes);
        doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
        break;
      } else {
        // continue
        pRes->info.id.groupId = pMiaInfo->groupId;
      }
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
    setInputDataBlock(pSup, pBlock, pIaInfo->binfo.inputTsOrder, pBlock->info.scanFlag, true);
    doMergeAlignedIntervalAggImpl(pOperator, &pIaInfo->binfo.resultRowInfo, pBlock, pRes);

    doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
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

  SInterval interval = {.interval = pNode->interval,
                        .sliding = pNode->sliding,
                        .intervalUnit = pNode->intervalUnit,
                        .slidingUnit = pNode->slidingUnit,
                        .offset = pNode->offset,
                        .precision = ((SColumnNode*)pNode->window.pTspk)->node.resType.precision};

  SIntervalAggOperatorInfo* iaInfo = miaInfo->intervalAggOperatorInfo;
  SExprSupp*                pSup = &pOperator->exprSupp;

  int32_t code = filterInitFromNode((SNode*)pNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  miaInfo->curTs = INT64_MIN;
  iaInfo->win = pTaskInfo->window;
  iaInfo->binfo.inputTsOrder = pNode->window.node.inputTsOrder;
  iaInfo->binfo.outputTsOrder = pNode->window.node.outputTsOrder;
  iaInfo->interval = interval;
  iaInfo->primaryTsIndex = ((SColumnNode*)pNode->window.pTspk)->slotId;
  iaInfo->binfo.mergeResultBlock = pNode->window.mergeDataBlock;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 512);

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pNode->window.pFuncs, NULL, &num);

  code = initAggSup(&pOperator->exprSupp, &iaInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pNode->window.node.pOutputDataBlockDesc);
  initBasicInfo(&iaInfo->binfo, pResBlock);
  initExecTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &iaInfo->win);

  iaInfo->timeWindowInterpo = timeWindowinterpNeeded(pSup->pCtx, num, iaInfo);
  if (iaInfo->timeWindowInterpo) {
    iaInfo->binfo.resultRowInfo.openWindow = tdListNew(sizeof(SOpenWindowInfo));
  }

  initResultRowInfo(&iaInfo->binfo.resultRowInfo);
  blockDataEnsureCapacity(iaInfo->binfo.pRes, pOperator->resultInfo.capacity);
  setOperatorInfo(pOperator, "TimeMergeAlignedIntervalAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL,
                  false, OP_NOT_OPENED, miaInfo, pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mergeAlignedIntervalAgg, NULL, destroyMAIOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

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

static int32_t outputPrevIntervalResult(SOperatorInfo* pOperatorInfo, uint64_t tableGroupId, SSDataBlock* pResultBlock,
                                        STimeWindow* newWin) {
  SMergeIntervalAggOperatorInfo* miaInfo = pOperatorInfo->info;
  SIntervalAggOperatorInfo*      iaInfo = &miaInfo->intervalAggOperatorInfo;
  bool                           ascScan = (iaInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);

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
  uint64_t    tableGroupId = pBlock->info.id.groupId;
  bool        ascScan = (iaInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);
  TSKEY       blockStartTs = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow* pResult = NULL;

  STimeWindow win = getActiveTimeWindow(iaInfo->aggSup.pResultBuf, pResultRowInfo, blockStartTs, &iaInfo->interval,
                                        iaInfo->binfo.inputTsOrder);

  int32_t ret =
      setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                             numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                                 iaInfo->binfo.inputTsOrder);
  ASSERT(forwardRows > 0);

  // prev time window not interpolation yet.
  if (iaInfo->timeWindowInterpo) {
    SResultRowPosition pos = addToOpenWindowList(pResultRowInfo, pResult, tableGroupId);
    doInterpUnclosedTimeWindow(pOperatorInfo, numOfOutput, pResultRowInfo, pBlock, scanFlag, tsCols, &pos);

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId, pExprSup->pCtx,
                                 numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    // window start key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &win, startPos, forwardRows, pExprSup);
  }

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &win, 1);
  applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                  pBlock->info.rows, numOfOutput);
  doCloseWindow(pResultRowInfo, iaInfo, pResult);

  // output previous interval results after this interval (&win) is closed
  outputPrevIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, &win);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&iaInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos,
                                      iaInfo->binfo.inputTsOrder);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    int32_t code =
        setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                               pExprSup->pCtx, numOfOutput, pExprSup->rowEntryInfoOffset, &iaInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL,
                                           iaInfo->binfo.inputTsOrder);

    // window start(end) key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pExprSup);

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &nextWin, 1);
    applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows,
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
    while (1) {
      SSDataBlock* pBlock = NULL;
      if (miaInfo->prefetchedBlock == NULL) {
        pBlock = getNextBlockFromDownstream(pOperator, 0);
      } else {
        pBlock = miaInfo->prefetchedBlock;
        miaInfo->groupId = pBlock->info.id.groupId;
        miaInfo->prefetchedBlock = NULL;
      }

      if (pBlock == NULL) {
        tdListInitIter(miaInfo->groupIntervals, &miaInfo->groupIntervalsIter, TD_LIST_FORWARD);
        miaInfo->inputBlocksFinished = true;
        break;
      }

      if (!miaInfo->hasGroupId) {
        miaInfo->hasGroupId = true;
        miaInfo->groupId = pBlock->info.id.groupId;
      } else if (miaInfo->groupId != pBlock->info.id.groupId) {
        miaInfo->prefetchedBlock = pBlock;
        break;
      }

      pRes->info.scanFlag = pBlock->info.scanFlag;
      setInputDataBlock(pExpSupp, pBlock, iaInfo->binfo.inputTsOrder, pBlock->info.scanFlag, true);
      doMergeIntervalAggImpl(pOperator, &iaInfo->binfo.resultRowInfo, pBlock, pBlock->info.scanFlag, pRes);

      if (pRes->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }
    }

    pRes->info.id.groupId = miaInfo->groupId;
  }

  if (miaInfo->inputBlocksFinished) {
    SListNode* listNode = tdListNext(&miaInfo->groupIntervalsIter);

    if (listNode != NULL) {
      SGroupTimeWindow* grpWin = (SGroupTimeWindow*)(listNode->data);
      pRes->info.id.groupId = grpWin->groupId;
    }
  }

  if (pRes->info.rows == 0) {
    setOperatorCompleted(pOperator);
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

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &num);

  SInterval interval = {.interval = pIntervalPhyNode->interval,
                        .sliding = pIntervalPhyNode->sliding,
                        .intervalUnit = pIntervalPhyNode->intervalUnit,
                        .slidingUnit = pIntervalPhyNode->slidingUnit,
                        .offset = pIntervalPhyNode->offset,
                        .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};

  pMergeIntervalInfo->groupIntervals = tdListNew(sizeof(SGroupTimeWindow));

  SIntervalAggOperatorInfo* pIntervalInfo = &pMergeIntervalInfo->intervalAggOperatorInfo;
  pIntervalInfo->win = pTaskInfo->window;
  pIntervalInfo->binfo.inputTsOrder = pIntervalPhyNode->window.node.inputTsOrder;
  pIntervalInfo->interval = interval;
  pIntervalInfo->binfo.mergeResultBlock = pIntervalPhyNode->window.mergeDataBlock;
  pIntervalInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  pIntervalInfo->binfo.outputTsOrder = pIntervalPhyNode->window.node.outputTsOrder;

  SExprSupp* pExprSupp = &pOperator->exprSupp;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t code = initAggSup(pExprSupp, &pIntervalInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                            pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pIntervalPhyNode->window.node.pOutputDataBlockDesc);
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
  setOperatorInfo(pOperator, "TimeMergeIntervalAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL, false,
                  OP_NOT_OPENED, pMergeIntervalInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doMergeIntervalAgg, NULL, destroyMergeIntervalOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pMergeIntervalInfo != NULL) {
    destroyMergeIntervalOperatorInfo(pMergeIntervalInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}
