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
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

#define IS_FINAL_OP(op)    ((op)->isFinal)
#define DEAULT_DELETE_MARK (1000LL * 60LL * 60LL * 24LL * 365LL * 10LL);

typedef struct SStateWindowInfo {
  SResultWindowInfo winInfo;
  SStateKeys*       pStateKey;
} SStateWindowInfo;

typedef struct SSessionAggOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
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

typedef struct SPullWindowInfo {
  STimeWindow window;
  uint64_t    groupId;
  STimeWindow calWin;
} SPullWindowInfo;

typedef struct SOpenWindowInfo {
  SResultRowPosition pos;
  uint64_t           groupId;
} SOpenWindowInfo;

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

        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR ||
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
      ((pWin->ekey < calStart || pWin->skey > calEnd) || (blockType == STREAM_PULL_DATA && pWin->skey < calStart) )) {
    return false;
  }

  return true;
}

bool inSlidingWindow(SInterval* pInterval, STimeWindow* pWin, SDataBlockInfo* pBlockInfo) {
  return inCalSlidingWindow(pInterval, pWin, pBlockInfo->calWin.skey, pBlockInfo->calWin.ekey, pBlockInfo->type);
}

static int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
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
        pNext->ekey = taosTimeAdd(pNext->skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
      } else {
        pNext->ekey += ((next - pNext->ekey + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
        pNext->skey = pNext->ekey - pInterval->interval + 1;
      }
    } else if ((!ascQuery) && primaryKeys[startPos] < pNext->skey) {
      TSKEY next = primaryKeys[startPos];
      if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, pInterval);
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

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &w, true);
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
  if (pData->groupId > pos->groupId) {
    return 1;
  } else if (pData->groupId < pos->groupId) {
    return -1;
  }

  if (pData->window.skey > pos->window.ekey) {
    return 1;
  } else if (pData->window.ekey < pos->window.skey) {
    return -1;
  }
  return 0;
}

static int32_t savePullWindow(SPullWindowInfo* pPullInfo, SArray* pPullWins) {
  int32_t size = taosArrayGetSize(pPullWins);
  int32_t index = binarySearchCom(pPullWins, size, pPullInfo, TSDB_ORDER_DESC, comparePullWinKey);
  if (index == -1) {
    index = 0;
  } else {
    int32_t code = comparePullWinKey(pPullInfo, pPullWins, index);
    if (code == 0) {
      SPullWindowInfo* pos = taosArrayGet(pPullWins, index);
      pos->window.skey = TMIN(pos->window.skey, pPullInfo->window.skey);
      pos->window.ekey = TMAX(pos->window.ekey, pPullInfo->window.ekey);
      pos->calWin.skey = TMIN(pos->calWin.skey, pPullInfo->calWin.skey);
      pos->calWin.ekey = TMAX(pos->calWin.ekey, pPullInfo->calWin.ekey);
      return TSDB_CODE_SUCCESS;
    } else if (code > 0) {
      index++;
    }
  }
  if (taosArrayInsert(pPullWins, index, pPullInfo) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t saveResult(SResultWindowInfo winInfo, SSHashObj* pStUpdated) {
  winInfo.sessionWin.win.ekey = winInfo.sessionWin.win.skey;
  return tSimpleHashPut(pStUpdated, &winInfo.sessionWin, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
}

static int32_t saveWinResult(SWinKey* pKey, SRowBuffPos* pPos, SSHashObj* pUpdatedMap) {
  tSimpleHashPut(pUpdatedMap, pKey, sizeof(SWinKey), &pPos, POINTER_BYTES);
  return TSDB_CODE_SUCCESS;
}

static int32_t saveWinResultInfo(TSKEY ts, uint64_t groupId, SRowBuffPos* pPos, SSHashObj* pUpdatedMap) {
  SWinKey key = {.ts = ts, .groupId = groupId};
  saveWinResult(&key, pPos, pUpdatedMap);
  return TSDB_CODE_SUCCESS;
}

static void removeResults(SArray* pWins, SSHashObj* pUpdatedMap) {
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey* pW = taosArrayGet(pWins, i);
    void*    tmp = tSimpleHashGet(pUpdatedMap, pW, sizeof(SWinKey));
    if (tmp) {
      void* value = *(void**)tmp;
      taosMemoryFree(value);
      tSimpleHashRemove(pUpdatedMap, pW, sizeof(SWinKey));
    }
  }
}

int32_t compareWinKey(void* pKey, void* data, int32_t index) {
  void* pDataPos = taosArrayGet((SArray*)data, index);
  return winKeyCmprImpl(pKey, pDataPos);
}

static void removeDeleteResults(SSHashObj* pUpdatedMap, SArray* pDelWins) {
  taosArraySort(pDelWins, winKeyCmprImpl);
  taosArrayRemoveDuplicate(pDelWins, winKeyCmprImpl, NULL);
  int32_t delSize = taosArrayGetSize(pDelWins);
  if (tSimpleHashGetSize(pUpdatedMap) == 0 || delSize == 0) {
    return;
  }
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pUpdatedMap, pIte, &iter)) != NULL) {
    SWinKey* pResKey = tSimpleHashGetKey(pIte, NULL);
    int32_t  index = binarySearchCom(pDelWins, delSize, pResKey, TSDB_ORDER_DESC, compareWinKey);
    if (index >= 0 && 0 == compareWinKey(pResKey, pDelWins, index)) {
      taosArrayRemove(pDelWins, index);
      delSize = taosArrayGetSize(pDelWins);
    }
  }
}

bool isOverdue(TSKEY ekey, STimeWindowAggSupp* pTwSup) {
  ASSERTS(pTwSup->maxTs == INT64_MIN || pTwSup->maxTs > 0, "maxts should greater than 0");
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
  uint64_t    tableGroupId = pBlock->info.id.groupId;
  bool        ascScan = (pInfo->binfo.inputTsOrder == TSDB_ORDER_ASC);
  TSKEY       ts = getStartTsKey(&pBlock->info.window, tsCols);
  SResultRow* pResult = NULL;

  STimeWindow win =
      getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, pInfo->binfo.inputTsOrder);
  int32_t ret = setTimeWindowOutputBuf(pResultRowInfo, &win, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                       pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
  }

  TSKEY   ekey = ascScan ? win.ekey : win.skey;
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->binfo.inputTsOrder);

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

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, true);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                  pBlock->info.rows, numOfOutput);

  doCloseWindow(pResultRowInfo, pInfo, pResult);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, pInfo->binfo.inputTsOrder);
    if (startPos < 0) {
      break;
    }
    // null data, failed to allocate more memory buffer
    int32_t code = setTimeWindowOutputBuf(pResultRowInfo, &nextWin, (scanFlag == MAIN_SCAN), &pResult, tableGroupId,
                                          pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    ekey = ascScan ? nextWin.ekey : nextWin.skey;
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->binfo.inputTsOrder);
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
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                    pBlock->info.rows, numOfOutput);
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
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
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
    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, scanFlag);
  }

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, pInfo->binfo.outputTsOrder);
  OPTR_SET_OPENED(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return TSDB_CODE_SUCCESS;
}

static bool compareVal(const char* v, const SStateKeys* pKey) {
  if (IS_VAR_DATA_TYPE(pKey->type)) {
    if (varDataLen(v) != varDataLen(pKey->pData)) {
      return false;
    } else {
      return memcmp(varDataVal(v), varDataVal(pKey->pData), varDataLen(v)) == 0;
    }
  } else {
    return memcmp(pKey->pData, v, pKey->bytes) == 0;
  }
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
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
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

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
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
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
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

static bool doDeleteWindow(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId) {
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SWinKey                      key = {.ts = ts, .groupId = groupId};
  tSimpleHashRemove(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey));
  pAPI->stateStore.streamStateDel(pInfo->pState, &key);
  return true;
}

static int32_t getChildIndex(SSDataBlock* pBlock) { return pBlock->info.childId; }

static void doDeleteWindows(SOperatorInfo* pOperator, SInterval* pInterval, SSDataBlock* pBlock, SArray* pUpWins,
                            SSHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SColumnInfoData*             pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*                       startTsCols = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData*             pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*                       endTsCols = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData*             pCalStTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*                       calStTsCols = (TSKEY*)pCalStTsCol->pData;
  SColumnInfoData*             pCalEnTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*                       calEnTsCols = (TSKEY*)pCalEnTsCol->pData;
  SColumnInfoData*             pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*                    pGpDatas = (uint64_t*)pGpCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    SResultRowInfo dumyInfo = {0};
    dumyInfo.cur.pageId = -1;

    STimeWindow win = {0};
    if (IS_FINAL_OP(pInfo)) {
      win.skey = startTsCols[i];
      win.ekey = endTsCols[i];
    } else {
      win = getActiveTimeWindow(NULL, &dumyInfo, startTsCols[i], pInterval, TSDB_ORDER_ASC);
    }

    do {
      if (!inCalSlidingWindow(pInterval, &win, calStTsCols[i], calEnTsCols[i], pBlock->info.type)) {
        getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
        continue;
      }
      uint64_t winGpId = pGpDatas[i];
      SWinKey  winRes = {.ts = win.skey, .groupId = winGpId};
      void*    chIds = taosHashGet(pInfo->pPullDataMap, &winRes, sizeof(SWinKey));
      if (chIds) {
        int32_t childId = getChildIndex(pBlock);
        SArray* chArray = *(void**)chIds;
        int32_t index = taosArraySearchIdx(chArray, &childId, compareInt32Val, TD_EQ);
        if (index != -1) {
          qDebug("===stream===try push delete window%" PRId64 "chId:%d ,continue", win.skey, childId);
          getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
          continue;
        }
      }
      bool res = doDeleteWindow(pOperator, win.skey, winGpId);
      if (pUpWins && res) {
        taosArrayPush(pUpWins, &winRes);
      }
      if (pUpdatedMap) {
        tSimpleHashRemove(pUpdatedMap, &winRes, sizeof(SWinKey));
      }
      getNextTimeWindow(pInterval, &win, TSDB_ORDER_ASC);
    } while (win.ekey <= endTsCols[i]);
  }
}

static int32_t getAllIntervalWindow(SSHashObj* pHashMap, SSHashObj* resWins) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SWinKey* pKey = tSimpleHashGetKey(pIte, NULL);
    uint64_t groupId = pKey->groupId;
    TSKEY    ts = pKey->ts;
    int32_t  code = saveWinResultInfo(ts, groupId, *(SRowBuffPos**)pIte, resWins);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t closeStreamIntervalWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SInterval* pInterval,
                                         SHashObj* pPullDataMap, SSHashObj* closeWins, SArray* pDelWins,
                                         SOperatorInfo* pOperator) {
  qDebug("===stream===close interval window");
  void*                        pIte = NULL;
  int32_t                      iter = 0;
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  int32_t                      delSize = taosArrayGetSize(pDelWins);
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    void*    key = tSimpleHashGetKey(pIte, NULL);
    SWinKey* pWinKey = (SWinKey*)key;
    if (delSize > 0) {
      int32_t index = binarySearchCom(pDelWins, delSize, pWinKey, TSDB_ORDER_DESC, compareWinKey);
      if (index >= 0 && 0 == compareWinKey(pWinKey, pDelWins, index)) {
        taosArrayRemove(pDelWins, index);
        delSize = taosArrayGetSize(pDelWins);
      }
    }

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
        int32_t code = saveWinResult(pWinKey, *(SRowBuffPos**)pIte, closeWins);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      tSimpleHashIterateRemove(pHashMap, pWinKey, sizeof(SWinKey), &pIte, &iter);
    }
  }
  return TSDB_CODE_SUCCESS;
}

STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {.skey = ts, .ekey = INT64_MAX};
  w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  return w;
}

static void doBuildDeleteResult(SStreamIntervalOperatorInfo* pInfo, SArray* pWins, int32_t* index,
                                SSDataBlock* pBlock) {
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
    void*    tbname = NULL;
    pInfo->statestore.streamStateGetParName(pInfo->pState, pWin->groupId, &tbname);
    if (tbname == NULL) {
      appendOneRowToStreamSpecialBlock(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId, NULL);
    } else {
      char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
      STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
      appendOneRowToStreamSpecialBlock(pBlock, &pWin->ts, &pWin->ts, &uid, &pWin->groupId, parTbName);
    }
    pInfo->statestore.streamStateFreeVal(tbname);
    (*index)++;
  }
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
  taosMemoryFreeClear(param);
}

void destroyStreamFinalIntervalOperatorInfo(void* param) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  // it should be empty.
  void* pIte = NULL;
  while ((pIte = taosHashIterate(pInfo->pPullDataMap, pIte)) != NULL) {
    taosArrayDestroy(*(void**)pIte);
  }
  taosHashCleanup(pInfo->pPullDataMap);
  taosHashCleanup(pInfo->pFinalPullDataMap);
  taosArrayDestroy(pInfo->pPullWins);
  blockDataDestroy(pInfo->pPullDataRes);
  taosArrayDestroy(pInfo->pDelWins);
  blockDataDestroy(pInfo->pDelRes);
  pInfo->statestore.streamFileStateDestroy(pInfo->pState->pFileState);
  taosMemoryFreeClear(pInfo->pState);

  nodesDestroyNode((SNode*)pInfo->pPhyNode);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  pInfo->groupResInfo.pRows = taosArrayDestroy(pInfo->groupResInfo.pRows);
  cleanupExprSupp(&pInfo->scalarSupp);

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

void initIntervalDownStream(SOperatorInfo* downstream, uint16_t type, SStreamIntervalOperatorInfo* pInfo) {
  SStateStore* pAPI = &downstream->pTaskInfo->storageAPI.stateStore;

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    initIntervalDownStream(downstream->pDownstream[0], type, pInfo);
    return;
  }

  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup.parentType = type;
  pScanInfo->windowSup.pIntervalAggSup = &pInfo->aggSup;
  if (!pScanInfo->pUpdateInfo) {
    pScanInfo->pUpdateInfo = pAPI->updateInfoInitP(&pInfo->interval, pInfo->twAggSup.waterMark);
  }

  pScanInfo->interval = pInfo->interval;
  pScanInfo->twAggSup = pInfo->twAggSup;
  pScanInfo->pState = pInfo->pState;
}

void initStreamFunciton(SqlFunctionCtx* pCtx, int32_t numOfExpr) {
  for (int32_t i = 0; i < numOfExpr; i++) {
    //    pCtx[i].isStream = true;
  }
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
  int32_t    code =
      initAggSup(pSup, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str, pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
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
                                         optrDefaultBufFn, NULL);

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
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
      }

      // pInfo->numOfRows data belong to the current session window
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
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

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
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
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    pBInfo->pRes->info.scanFlag = pBlock->info.scanFlag;
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
  SColumnNode* pColNode = (SColumnNode*)((STargetNode*)pStateNode->pStateKey)->pExpr;

  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pStateNode->window.pExprs, NULL, &numOfScalarExpr);
    int32_t    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
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
                                         optrDefaultBufFn, NULL);

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
  code = filterInitFromNode((SNode*)pSessionNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "SessionWindowAggOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doSessionWindowAgg, NULL, destroySWindowOperatorInfo,
                                         optrDefaultBufFn, NULL);
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
        qError("%s apply combine functions error, code: %s", GET_TASKID(pTaskInfo), tstrerror(code));
      }
    } else if (pDestCtx[k].fpSet.combine == NULL) {
      char* funName = fmGetFuncName(pDestCtx[k].functionId);
      qError("%s error, combine funcion for %s is not implemented", GET_TASKID(pTaskInfo), funName);
      taosMemoryFreeClear(funName);
    }
  }
}

bool hasIntervalWindow(void* pState, SWinKey* pKey, SStateStore* pStore) { return pStore->streamStateCheck(pState, pKey); }

int32_t setIntervalOutputBuf(void* pState, STimeWindow* win, SRowBuffPos** pResult, int64_t groupId,
                             SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset,
                             SAggSupporter* pAggSup, SStateStore* pStore) {

  SWinKey key = { .ts = win->skey, .groupId = groupId };
  char*   value = NULL;
  int32_t size = pAggSup->resultRowSize;

  if (pStore->streamStateAddIfNotExist(pState, &key, (void**)&value, &size) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pResult = (SRowBuffPos*)value;
  SResultRow* res = (SResultRow*)((*pResult)->pRowBuff);

  // set time window for current result
  res->win = (*win);
  setResultRowInitCtx(res, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

bool isDeletedStreamWindow(STimeWindow* pWin, uint64_t groupId, void* pState, STimeWindowAggSupp* pTwSup, SStateStore* pStore) {
  if (pTwSup->maxTs != INT64_MIN && pWin->ekey < pTwSup->maxTs - pTwSup->deleteMark) {
    SWinKey key = {.ts = pWin->skey, .groupId = groupId};
    if (!hasIntervalWindow(pState, &key, pStore)) {
      return true;
    }
    return false;
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

static void clearStreamIntervalOperator(SStreamIntervalOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->aggSup.pResultRowHashTable);
  clearDiskbasedBuf(pInfo->aggSup.pResultBuf);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->aggSup.currentPageId = -1;
  pInfo->statestore.streamStateClear(pInfo->pState);
}

static void clearSpecialDataBlock(SSDataBlock* pBlock) {
  if (pBlock->info.rows <= 0) {
    return;
  }
  blockDataCleanup(pBlock);
}

static void doBuildPullDataBlock(SArray* array, int32_t* pIndex, SSDataBlock* pBlock) {
  clearSpecialDataBlock(pBlock);
  int32_t size = taosArrayGetSize(array);
  if (size - (*pIndex) == 0) {
    return;
  }
  blockDataEnsureCapacity(pBlock, size - (*pIndex));
  SColumnInfoData* pStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGroupId = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndTs = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  for (; (*pIndex) < size; (*pIndex)++) {
    SPullWindowInfo* pWin = taosArrayGet(array, (*pIndex));
    colDataSetVal(pStartTs, pBlock->info.rows, (const char*)&pWin->window.skey, false);
    colDataSetVal(pEndTs, pBlock->info.rows, (const char*)&pWin->window.ekey, false);
    colDataSetVal(pGroupId, pBlock->info.rows, (const char*)&pWin->groupId, false);
    colDataSetVal(pCalStartTs, pBlock->info.rows, (const char*)&pWin->calWin.skey, false);
    colDataSetVal(pCalEndTs, pBlock->info.rows, (const char*)&pWin->calWin.ekey, false);
    pBlock->info.rows++;
  }
  if ((*pIndex) == size) {
    *pIndex = 0;
    taosArrayClear(array);
  }
  blockDataUpdateTsWindow(pBlock, 0);
}

void processPullOver(SSDataBlock* pBlock, SHashObj* pMap, SHashObj* pFinalMap, SInterval* pInterval, SArray* pPullWins, int32_t numOfCh, SOperatorInfo* pOperator) {
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*           tsData = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*           tsEndData = (TSKEY*)pEndCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIdData = (uint64_t*)pGroupCol->pData;
  int32_t          chId = getChildIndex(pBlock);
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    TSKEY winTs = tsData[i];
    while (winTs <= tsEndData[i]) {
      SWinKey winRes = {.ts = winTs, .groupId = groupIdData[i]};
      void*   chIds = taosHashGet(pMap, &winRes, sizeof(SWinKey));
      if (chIds) {
        SArray* chArray = *(SArray**)chIds;
        int32_t index = taosArraySearchIdx(chArray, &chId, compareInt32Val, TD_EQ);
        if (index != -1) {
          qDebug("===stream===retrive window %" PRId64 " delete child id %d", winRes.ts, chId);
          taosArrayRemove(chArray, index);
          if (taosArrayGetSize(chArray) == 0) {
            // pull data is over
            taosArrayDestroy(chArray);
            taosHashRemove(pMap, &winRes, sizeof(SWinKey));
            qDebug("===stream===retrive pull data over.window %" PRId64 , winRes.ts);

            void* pFinalCh = taosHashGet(pFinalMap, &winRes, sizeof(SWinKey));
            if (pFinalCh) {
              taosHashRemove(pFinalMap, &winRes, sizeof(SWinKey));
              doDeleteWindow(pOperator, winRes.ts, winRes.groupId);
              STimeWindow nextWin = getFinalTimeWindow(winRes.ts, pInterval);
              SPullWindowInfo pull = {.window = nextWin,
                                      .groupId = winRes.groupId,
                                      .calWin.skey = nextWin.skey,
                                      .calWin.ekey = nextWin.skey};
              // add pull data request
              if (savePullWindow(&pull, pPullWins) == TSDB_CODE_SUCCESS) {
                addPullWindow(pMap, &winRes, numOfCh);
                qDebug("===stream===prepare final retrive for delete %" PRId64 ", size:%d", winRes.ts, numOfCh);
              }
            }
          }
        }
      }
      winTs = taosTimeAdd(winTs, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
    }
  }
}

static void addRetriveWindow(SArray* wins, SStreamIntervalOperatorInfo* pInfo, int32_t childId) {
  int32_t size = taosArrayGetSize(wins);
  for (int32_t i = 0; i < size; i++) {
    SWinKey*    winKey = taosArrayGet(wins, i);
    STimeWindow nextWin = getFinalTimeWindow(winKey->ts, &pInfo->interval);
    if (isOverdue(nextWin.ekey, &pInfo->twAggSup) && pInfo->ignoreExpiredData) {
      continue;
    }
    void* chIds = taosHashGet(pInfo->pPullDataMap, winKey, sizeof(SWinKey));
    if (!chIds) {
      SPullWindowInfo pull = {
          .window = nextWin, .groupId = winKey->groupId, .calWin.skey = nextWin.skey, .calWin.ekey = nextWin.skey};
      // add pull data request
      if (savePullWindow(&pull, pInfo->pPullWins) == TSDB_CODE_SUCCESS) {
        addPullWindow(pInfo->pPullDataMap, winKey, pInfo->numOfChild);
        qDebug("===stream===prepare retrive for delete %" PRId64 ", size:%d", winKey->ts, pInfo->numOfChild);
      }
    } else {
      SArray* chArray = *(void**)chIds;
      int32_t index = taosArraySearchIdx(chArray, &childId, compareInt32Val, TD_EQ);
      qDebug("===stream===check final retrive %" PRId64",chid:%d", winKey->ts, index);
      if (index == -1) {
        qDebug("===stream===add final retrive %" PRId64, winKey->ts);
        taosHashPut(pInfo->pFinalPullDataMap, winKey, sizeof(SWinKey), NULL, 0);
      }
    }
  }
}

static void clearFunctionContext(SExprSupp* pSup) {
  for (int32_t i = 0; i < pSup->numOfExprs; i++) {
    pSup->pCtx[i].saveHandle.currentPage = -1;
  }
}

int32_t getOutputBuf(void* pState, SRowBuffPos* pPos, SResultRow** pResult, SStateStore* pStore) {
  return pStore->streamStateGetByPos(pState, pPos, (void**)pResult);
}

int32_t buildDataBlockFromGroupRes(SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock, SExprSupp* pSup,
                                   SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SRowBuffPos* pPos = *(SRowBuffPos**)taosArrayGet(pGroupResInfo->pRows, i);
    SResultRow*  pRow = NULL;
    int32_t      code = getOutputBuf(pState, pPos, &pRow, &pAPI->stateStore);
    uint64_t     groupId = ((SWinKey*)pPos->pKey)->groupId;
    ASSERT(code == 0);
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }
    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = groupId;
      void* tbname = NULL;
      if (pAPI->stateStore.streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
        pBlock->info.parTbName[0] = 0;
      } else {
        memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      }
      pAPI->stateStore.streamStateFreeVal(tbname);
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.id.groupId != groupId) {
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      break;
    }
    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      SResultRowEntryInfo* pEnryInfo = pCtx[j].resultInfo;

      if (pCtx[j].fpSet.finalize) {
        int32_t code1 = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(code1)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code1));
          T_LONG_JMP(pTaskInfo->env, code1);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    pBlock->info.rows += pRow->numOfRows;
  }

  pBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pBlock, 0);
  return TSDB_CODE_SUCCESS;
}

void doBuildStreamIntervalResult(SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock,
                                 SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  buildDataBlockFromGroupRes(pOperator, pState, pBlock, &pOperator->exprSupp, pGroupResInfo);
}

static int32_t getNextQualifiedFinalWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                           TSKEY* primaryKeys, int32_t prevPosition) {
  int32_t startPos = prevPosition + 1;
  if (startPos == pDataBlockInfo->rows) {
    startPos = -1;
  } else {
    *pNext = getFinalTimeWindow(primaryKeys[startPos], pInterval);
  }
  return startPos;
}

static void setStreamDataVersion(SExecTaskInfo* pTaskInfo, int64_t version, int64_t ckId) {
  pTaskInfo->streamInfo.dataVersion = version;
  pTaskInfo->streamInfo.checkPointId = ckId;
}

static void doStreamIntervalAggImpl(SOperatorInfo* pOperatorInfo, SSDataBlock* pSDataBlock, uint64_t groupId,
                                    SSHashObj* pUpdatedMap) {
  SStreamIntervalOperatorInfo* pInfo = (SStreamIntervalOperatorInfo*)pOperatorInfo->info;
  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);

  SResultRowInfo* pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo*  pTaskInfo = pOperatorInfo->pTaskInfo;
  SExprSupp*      pSup = &pOperatorInfo->exprSupp;
  int32_t         numOfOutput = pSup->numOfExprs;
  int32_t         step = 1;
  TSKEY*          tsCols = NULL;
  SRowBuffPos*    pResPos = NULL;
  SResultRow*     pResult = NULL;
  int32_t         forwardRows = 0;

  SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  tsCols = (int64_t*)pColDataInfo->pData;

  int32_t     startPos = 0;
  TSKEY       ts = getStartTsKey(&pSDataBlock->info.window, tsCols);
  STimeWindow nextWin = {0};
  if (IS_FINAL_OP(pInfo)) {
    nextWin = getFinalTimeWindow(ts, &pInfo->interval);
  } else {
    nextWin = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, TSDB_ORDER_ASC);
  }
  while (1) {
    bool isClosed = isCloseWindow(&nextWin, &pInfo->twAggSup);
    if ((pInfo->ignoreExpiredData && isClosed && !IS_FINAL_OP(pInfo)) || !inSlidingWindow(&pInfo->interval, &nextWin, &pSDataBlock->info)) {
      startPos = getNexWindowPos(&pInfo->interval, &pSDataBlock->info, tsCols, startPos, nextWin.ekey, &nextWin);
      if (startPos < 0) {
        break;
      }
      continue;
    }

    if (IS_FINAL_OP(pInfo) && pInfo->numOfChild > 0) {
      bool    ignore = true;
      SWinKey winRes = {
          .ts = nextWin.skey,
          .groupId = groupId,
      };
      void* chIds = taosHashGet(pInfo->pPullDataMap, &winRes, sizeof(SWinKey));
      if (isDeletedStreamWindow(&nextWin, groupId, pInfo->pState, &pInfo->twAggSup, &pInfo->statestore) && isClosed && !chIds) {
        SPullWindowInfo pull = {
            .window = nextWin, .groupId = groupId, .calWin.skey = nextWin.skey, .calWin.ekey = nextWin.skey};
        // add pull data request
        if (savePullWindow(&pull, pInfo->pPullWins) == TSDB_CODE_SUCCESS) {
          addPullWindow(pInfo->pPullDataMap, &winRes, pInfo->numOfChild);
        }
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
        startPos = getNextQualifiedFinalWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, startPos);
        if (startPos < 0) {
          break;
        }
        continue;
      }
    }

    int32_t code = setIntervalOutputBuf(pInfo->pState, &nextWin, &pResPos, groupId, pSup->pCtx, numOfOutput,
                                        pSup->rowEntryInfoOffset, &pInfo->aggSup, &pInfo->statestore);
    pResult = (SResultRow*)pResPos->pRowBuff;
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    if (IS_FINAL_OP(pInfo)) {
      forwardRows = 1;
    } else {
      forwardRows = getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, nextWin.ekey, binarySearchForKey,
                                             NULL, TSDB_ORDER_ASC);
    }

    SWinKey key = {
        .ts = pResult->win.skey,
        .groupId = groupId,
    };
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pUpdatedMap) {
      saveWinResult(&key, pResPos, pUpdatedMap);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      tSimpleHashPut(pInfo->aggSup.pResultRowHashTable, &key, sizeof(SWinKey), &pResPos, POINTER_BYTES);
    }

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, &pInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                    pSDataBlock->info.rows, numOfOutput);
    key.ts = nextWin.skey;

    if (pInfo->delKey.ts > key.ts) {
      pInfo->delKey = key;
    }
    int32_t prevEndPos = (forwardRows - 1) * step + startPos;
    if (pSDataBlock->info.window.skey <= 0 || pSDataBlock->info.window.ekey <= 0) {
      qError("table uid %" PRIu64 " data block timestamp range may not be calculated! minKey %" PRId64
             ",maxKey %" PRId64,
             pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
      blockDataUpdateTsWindow(pSDataBlock, 0);

      // timestamp of the data is incorrect
      if (pSDataBlock->info.window.skey <= 0 || pSDataBlock->info.window.ekey <= 0) {
        qError("table uid %" PRIu64 " data block timestamp is out of range! minKey %" PRId64 ",maxKey %" PRId64,
               pSDataBlock->info.id.uid, pSDataBlock->info.window.skey, pSDataBlock->info.window.ekey);
      }
    }

    if (IS_FINAL_OP(pInfo)) {
      startPos = getNextQualifiedFinalWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos);
    } else {
      startPos =
          getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, TSDB_ORDER_ASC);
    }
    if (startPos < 0) {
      break;
    }
  }
}

static inline int winPosCmprImpl(const void* pKey1, const void* pKey2) {
  SRowBuffPos* pos1 = *(SRowBuffPos**)pKey1;
  SRowBuffPos* pos2 = *(SRowBuffPos**)pKey2;
  SWinKey*     pWin1 = (SWinKey*)pos1->pKey;
  SWinKey*     pWin2 = (SWinKey*)pos2->pKey;

  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->ts > pWin2->ts) {
    return 1;
  } else if (pWin1->ts < pWin2->ts) {
    return -1;
  }

  return 0;
}

static void resetUnCloseWinInfo(SSHashObj* winMap) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(winMap, pIte, &iter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pIte;
    pPos->beUsed = true;
  }
}

static SSDataBlock* doStreamFinalIntervalAgg(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SOperatorInfo*               downstream = pOperator->pDownstream[0];
  SExprSupp*                   pSup = &pOperator->exprSupp;

  qDebug("interval status %d %s", pOperator->status, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildPullDataBlock(pInfo->pPullWins, &pInfo->pullIndex, pInfo->pPullDataRes);
    if (pInfo->pPullDataRes->info.rows != 0) {
      // process the rest of the data
      printDataBlock(pInfo->pPullDataRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      return pInfo->pPullDataRes;
    }

    doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
    if (pInfo->pDelRes->info.rows != 0) {
      // process the rest of the data
      printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      return pInfo->pDelRes;
    }

    doBuildStreamIntervalResult(pOperator, pInfo->pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
    if (pInfo->binfo.pRes->info.rows != 0) {
      printDataBlock(pInfo->binfo.pRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
      return pInfo->binfo.pRes;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseWinInfo(pInfo->aggSup.pResultRowHashTable);
    }

    setOperatorCompleted(pOperator);
    if (!IS_FINAL_OP(pInfo)) {
      clearFunctionContext(&pOperator->exprSupp);
      // semi interval operator clear disk buffer
      clearStreamIntervalOperator(pInfo);
      setStreamDataVersion(pTaskInfo, pInfo->dataVersion, pInfo->pState->checkPointId);
      qDebug("===stream===clear semi operator");
    } else {
      if (pInfo->twAggSup.maxTs > 0 &&
          pInfo->twAggSup.maxTs - pInfo->twAggSup.checkPointInterval > pInfo->twAggSup.checkPointTs) {
        pAPI->stateStore.streamStateCommit(pInfo->pState);
        pAPI->stateStore.streamStateDeleteCheckPoint(pInfo->pState, pInfo->twAggSup.maxTs - pInfo->twAggSup.deleteMark);
        pInfo->twAggSup.checkPointTs = pInfo->twAggSup.maxTs;
      }
      qDebug("===stream===interval final close");
    }
    return NULL;
  } else {
    if (!IS_FINAL_OP(pInfo)) {
      doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
      if (pInfo->pDelRes->info.rows != 0) {
        // process the rest of the data
        printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
        return pInfo->pDelRes;
      }
    }
  }

  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
  }
  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
  }

  while (1) {
    if (isTaskKilled(pTaskInfo)) {
      if (pInfo->pUpdated != NULL) {
        pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
      }

      if (pInfo->pUpdatedMap != NULL) {
        tSimpleHashCleanup(pInfo->pUpdatedMap);
        pInfo->pUpdatedMap = NULL;
      }

      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===return data:%s. recv datablock num:%" PRIu64,
             IS_FINAL_OP(pInfo) ? "interval final" : "interval semi", pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }
    pInfo->numOfDatapack++;
    printDataBlock(pBlock, IS_FINAL_OP(pInfo) ? "interval final recv" : "interval semi recv");

    if (pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_PULL_DATA) {
      pInfo->binfo.pRes->info.type = pBlock->info.type;
    } else if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
               pBlock->info.type == STREAM_CLEAR) {
      SArray* delWins = taosArrayInit(8, sizeof(SWinKey));
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, delWins, pInfo->pUpdatedMap);
      if (IS_FINAL_OP(pInfo)) {
        int32_t chId = getChildIndex(pBlock);
        addRetriveWindow(delWins, pInfo, chId);
        if (pBlock->info.type != STREAM_CLEAR) {
          taosArrayAddAll(pInfo->pDelWins, delWins);
        }
        taosArrayDestroy(delWins);
        continue;
      }
      removeResults(delWins, pInfo->pUpdatedMap);
      taosArrayAddAll(pInfo->pDelWins, delWins);
      taosArrayDestroy(delWins);

      doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
      if (pInfo->pDelRes->info.rows != 0) {
        // process the rest of the data
        printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
        if (pBlock->info.type == STREAM_CLEAR) {
          pInfo->pDelRes->info.type = STREAM_CLEAR;
        } else {
          pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;
        }
        return pInfo->pDelRes;
      }

      break;
    } else if (pBlock->info.type == STREAM_GET_ALL && IS_FINAL_OP(pInfo)) {
      pInfo->recvGetAll = true;
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pInfo->pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_RETRIEVE && !IS_FINAL_OP(pInfo)) {
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, NULL, pInfo->pUpdatedMap);
      if (taosArrayGetSize(pInfo->pUpdated) > 0) {
        break;
      }
      continue;
    } else if (pBlock->info.type == STREAM_PULL_OVER && IS_FINAL_OP(pInfo)) {
      processPullOver(pBlock, pInfo->pPullDataMap, pInfo->pFinalPullDataMap, &pInfo->interval, pInfo->pPullWins, pInfo->numOfChild, pOperator);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else {
      ASSERTS(pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamIntervalAggImpl(pOperator, pBlock, pBlock->info.id.groupId, pInfo->pUpdatedMap);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }

  removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  if (IS_FINAL_OP(pInfo)) {
    closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval,
                              pInfo->pPullDataMap, pInfo->pUpdatedMap, pInfo->pDelWins, pOperator);
  }
  pInfo->binfo.pRes->info.watermark = pInfo->twAggSup.maxTs;

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->pUpdatedMap, pIte, &iter)) != NULL) {
    taosArrayPush(pInfo->pUpdated, pIte);
  }

  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  doBuildPullDataBlock(pInfo->pPullWins, &pInfo->pullIndex, pInfo->pPullDataRes);
  if (pInfo->pPullDataRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pPullDataRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->pPullDataRes;
  }

  doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows != 0) {
    // process the rest of the data
    printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->pDelRes;
  }

  doBuildStreamIntervalResult(pOperator, pInfo->pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows != 0) {
    printDataBlock(pInfo->binfo.pRes, IS_FINAL_OP(pInfo) ? "interval final" : "interval semi");
    return pInfo->binfo.pRes;
  }

  return NULL;
}

int64_t getDeleteMark(SIntervalPhysiNode* pIntervalPhyNode) {
  if (pIntervalPhyNode->window.deleteMark <= 0) {
    return DEAULT_DELETE_MARK;
  }
  int64_t deleteMark = TMAX(pIntervalPhyNode->window.deleteMark, pIntervalPhyNode->window.watermark);
  deleteMark = TMAX(deleteMark, pIntervalPhyNode->interval);
  return deleteMark;
}

TSKEY compareTs(void* pKey) {
  SWinKey* pWinKey = (SWinKey*)pKey;
  return pWinKey->ts;
}

int32_t getSelectivityBufSize(SqlFunctionCtx* pCtx) {
  if (pCtx->subsidiaries.rowLen == 0) {
    int32_t rowLen = 0;
    for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
      rowLen += pc->pExpr->base.resSchema.bytes;
    }

    return rowLen + pCtx->subsidiaries.num * sizeof(bool);
  } else {
    return pCtx->subsidiaries.rowLen;
  }
}

int32_t getMaxFunResSize(SExprSupp* pSup, int32_t numOfCols) {
  int32_t size = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
      int32_t resSize = getSelectivityBufSize(pSup->pCtx + i);
      size = TMAX(size, resSize);
  }
  return size;
}

SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                     SExecTaskInfo* pTaskInfo, int32_t numOfChild) {
  SIntervalPhysiNode*          pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;
  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

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
      .deleteMark = getDeleteMark(pIntervalPhyNode),
      .deleteMarkSaved = 0,
      .calTriggerSaved = 0,
      .checkPointTs = 0,
      .checkPointInterval =
          convertTimePrecision(tsCheckpointInterval, TSDB_TIME_PRECISION_MILLI, pInfo->interval.precision),
  };
  ASSERTS(pInfo->twAggSup.calTrigger != STREAM_TRIGGER_MAX_DELAY, "trigger type should not be max delay");
  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &numOfScalar);
    int32_t    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  pInfo->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pInfo->pState) = *(pTaskInfo->streamInfo.pState);

  pAPI->stateStore.streamStateSetNumber(pInfo->pState, -1);
  int32_t code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                            pInfo->pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initStreamFunciton(pOperator->exprSupp.pCtx, pOperator->exprSupp.numOfExprs);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->numOfChild = numOfChild;

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
  pInfo->pFinalPullDataMap = taosHashInit(64, hashFn, false, HASH_NO_LOCK);
  pInfo->pPullDataRes = createSpecialDataBlock(STREAM_RETRIEVE);
  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->delIndex = 0;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->delKey.ts = INT64_MAX;
  pInfo->delKey.groupId = 0;
  pInfo->numOfDatapack = 0;
  pInfo->pUpdated = NULL;
  pInfo->pUpdatedMap = NULL;
  int32_t funResSize= getMaxFunResSize(&pOperator->exprSupp, numOfCols);
  pInfo->pState->pFileState = pAPI->stateStore.streamFileStateInit(tsStreamBufferSize, sizeof(SWinKey), pInfo->aggSup.resultRowSize, funResSize,
                                                  compareTs, pInfo->pState, pInfo->twAggSup.deleteMark);
  pInfo->dataVersion = 0;
  pInfo->statestore = pTaskInfo->storageAPI.stateStore;
  pInfo->recvGetAll = false;

  pOperator->operatorType = pPhyNode->type;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(NULL, doStreamFinalIntervalAgg, NULL, destroyStreamFinalIntervalOperatorInfo,
                                         optrDefaultBufFn, NULL);
  if (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL) {
    initIntervalDownStream(downstream, pPhyNode->type, pInfo);
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
  tSimpleHashCleanup(pSup->pResultRows);
  destroyDiskbasedBuf(pSup->pResultBuf);
  blockDataDestroy(pSup->pScanBlock);
  taosMemoryFreeClear(pSup->pState);
  taosMemoryFreeClear(pSup->pDummyCtx);
}

void destroyStreamSessionAggOperatorInfo(void* param) {
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);

  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo* pChild = taosArrayGetP(pInfo->pChildren, i);
      destroyOperator(pChild);
    }
    taosArrayDestroy(pInfo->pChildren);
  }
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  blockDataDestroy(pInfo->pWinBlock);
  blockDataDestroy(pInfo->pUpdateRes);
  tSimpleHashCleanup(pInfo->pStDeleted);

  taosMemoryFreeClear(param);
}

int32_t initBasicInfoEx(SOptrBasicInfo* pBasicInfo, SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfCols,
                        SSDataBlock* pResultBlock, SFunctionStateStore* pStore) {
  initBasicInfo(pBasicInfo, pResultBlock);
  int32_t code = initExprSupp(pSup, pExprInfo, numOfCols, pStore);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  initStreamFunciton(pSup->pCtx, pSup->numOfExprs);
  for (int32_t i = 0; i < numOfCols; ++i) {
    pSup->pCtx[i].saveHandle.pBuf = NULL;
  }

  ASSERT(numOfCols > 0);
  return TSDB_CODE_SUCCESS;
}

void initDummyFunction(SqlFunctionCtx* pDummy, SqlFunctionCtx* pCtx, int32_t nums) {
  for (int i = 0; i < nums; i++) {
    pDummy[i].functionId = pCtx[i].functionId;
    pDummy[i].isNotNullFunc = pCtx[i].isNotNullFunc;
    pDummy[i].isPseudoFunc = pCtx[i].isPseudoFunc;
  }
}

void initDownStream(SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, uint16_t type, int32_t tsColIndex,
                    STimeWindowAggSupp* pTwSup) {
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION) {
    SStreamPartitionOperatorInfo* pScanInfo = downstream->info;
    pScanInfo->tsColIndex = tsColIndex;
  }

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    initDownStream(downstream->pDownstream[0], pAggSup, type, tsColIndex, pTwSup);
    return;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->windowSup = (SWindowSupporter){.pStreamAggSup = pAggSup, .gap = pAggSup->gap, .parentType = type};
  pScanInfo->pState = pAggSup->pState;
  if (!pScanInfo->pUpdateInfo) {
    pScanInfo->pUpdateInfo = pAggSup->stateStore.updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, pTwSup->waterMark);
  }
  pScanInfo->twAggSup = *pTwSup;
}

int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, int64_t gap,
                               SStreamState* pState, int32_t keySize, int16_t keyType, SStateStore* pStore) {
  pSup->resultRowSize = keySize + getResultRowSize(pCtx, numOfOutput);
  pSup->pScanBlock = createSpecialDataBlock(STREAM_CLEAR);
  pSup->gap = gap;
  pSup->stateKeySize = keySize;
  pSup->stateKeyType = keyType;
  pSup->pDummyCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pSup->pDummyCtx == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSup->stateStore = *pStore;

  initDummyFunction(pSup->pDummyCtx, pCtx, numOfOutput);
  pSup->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pSup->pState) = *pState;
  pSup->stateStore.streamStateSetNumber(pSup->pState, -1);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pSup->pResultRows = tSimpleHashInit(32, hashFn);

  int32_t pageSize = 4096;
  while (pageSize < pSup->resultRowSize * 4) {
    pageSize <<= 1u;
  }
  // at least four pages need to be in buffer
  int32_t bufSize = 4096 * 256;
  if (bufSize <= pageSize) {
    bufSize = pageSize * 4;
  }

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    qError("Init stream agg supporter failed since %s, tempDir:%s", terrstr(), tsTempDir);
    return terrno;
  }

  int32_t code = createDiskbasedBuf(&pSup->pResultBuf, pageSize, bufSize, "function", tsTempDir);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].saveHandle.pBuf = pSup->pResultBuf;
  }

  return TSDB_CODE_SUCCESS;
}

bool isInTimeWindow(STimeWindow* pWin, TSKEY ts, int64_t gap) {
  if (ts + gap >= pWin->skey && ts - gap <= pWin->ekey) {
    return true;
  }
  return false;
}

bool isInWindow(SResultWindowInfo* pWinInfo, TSKEY ts, int64_t gap) {
  return isInTimeWindow(&pWinInfo->sessionWin.win, ts, gap);
}

void getCurSessionWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                         SSessionKey* pKey) {
  pKey->win.skey = startTs;
  pKey->win.ekey = endTs;
  pKey->groupId = groupId;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKeyByRange(pAggSup->pState, pKey, pKey);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_KEY_INVALID(pKey);
  }
}

bool isInvalidSessionWin(SResultWindowInfo* pWinInfo) { return pWinInfo->sessionWin.win.skey == 0; }

void setSessionOutputBuf(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                         SResultWindowInfo* pCurWin) {
  pCurWin->sessionWin.groupId = groupId;
  pCurWin->sessionWin.win.skey = startTs;
  pCurWin->sessionWin.win.ekey = endTs;
  int32_t size = pAggSup->resultRowSize;
  int32_t code = pAggSup->stateStore.streamStateSessionAddIfNotExist(pAggSup->pState, &pCurWin->sessionWin,
                                                                     pAggSup->gap, &pCurWin->pOutputBuf, &size);
  if (code == TSDB_CODE_SUCCESS) {
    pCurWin->isOutput = true;
  } else {
    pCurWin->sessionWin.win.skey = startTs;
    pCurWin->sessionWin.win.ekey = endTs;
  }
}

int32_t getSessionWinBuf(SStreamAggSupporter* pAggSup, SStreamStateCur* pCur, SResultWindowInfo* pWinInfo) {
  int32_t size = 0;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pWinInfo->sessionWin, &pWinInfo->pOutputBuf, &size);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pAggSup->stateStore.streamStateCurNext(pAggSup->pState, pCur);
  return TSDB_CODE_SUCCESS;
}
void saveDeleteInfo(SArray* pWins, SSessionKey key) {
  // key.win.ekey = key.win.skey;
  taosArrayPush(pWins, &key);
}

void saveDeleteRes(SSHashObj* pStDelete, SSessionKey key) {
  key.win.ekey = key.win.skey;
  tSimpleHashPut(pStDelete, &key, sizeof(SSessionKey), NULL, 0);
}

static void removeSessionResult(SSHashObj* pHashMap, SSHashObj* pResMap, SSessionKey key) {
  key.win.ekey = key.win.skey;
  tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
  tSimpleHashRemove(pResMap, &key, sizeof(SSessionKey));
}

static void getSessionHashKey(const SSessionKey* pKey, SSessionKey* pHashKey) {
  *pHashKey = *pKey;
  pHashKey->win.ekey = pKey->win.skey;
}

static void removeSessionResults(SSHashObj* pHashMap, SArray* pWins) {
  if (tSimpleHashGetSize(pHashMap) == 0) {
    return;
  }
  int32_t size = taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SSessionKey* pWin = taosArrayGet(pWins, i);
    if (!pWin) continue;
    SSessionKey key = {0};
    getSessionHashKey(pWin, &key);
    tSimpleHashRemove(pHashMap, &key, sizeof(SSessionKey));
  }
}

int32_t updateSessionWindowInfo(SResultWindowInfo* pWinInfo, TSKEY* pStartTs, TSKEY* pEndTs, uint64_t groupId,
                                int32_t rows, int32_t start, int64_t gap, SSHashObj* pResultRows, SSHashObj* pStUpdated,
                                SSHashObj* pStDeleted) {
  for (int32_t i = start; i < rows; ++i) {
    if (!isInWindow(pWinInfo, pStartTs[i], gap) && (!pEndTs || !isInWindow(pWinInfo, pEndTs[i], gap))) {
      return i - start;
    }
    if (pWinInfo->sessionWin.win.skey > pStartTs[i]) {
      if (pStDeleted && pWinInfo->isOutput) {
        saveDeleteRes(pStDeleted, pWinInfo->sessionWin);
      }
      removeSessionResult(pStUpdated, pResultRows, pWinInfo->sessionWin);
      pWinInfo->sessionWin.win.skey = pStartTs[i];
    }
    pWinInfo->sessionWin.win.ekey = TMAX(pWinInfo->sessionWin.win.ekey, pStartTs[i]);
    if (pEndTs) {
      pWinInfo->sessionWin.win.ekey = TMAX(pWinInfo->sessionWin.win.ekey, pEndTs[i]);
    }
  }
  return rows - start;
}

static int32_t initSessionOutputBuf(SResultWindowInfo* pWinInfo, SResultRow** pResult, SqlFunctionCtx* pCtx,
                                    int32_t numOfOutput, int32_t* rowEntryInfoOffset) {
  ASSERT(pWinInfo->sessionWin.win.skey <= pWinInfo->sessionWin.win.ekey);
  *pResult = (SResultRow*)pWinInfo->pOutputBuf;
  // set time window for current result
  (*pResult)->win = pWinInfo->sessionWin.win;
  setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static int32_t doOneWindowAggImpl(SColumnInfoData* pTimeWindowData, SResultWindowInfo* pCurWin, SResultRow** pResult,
                                  int32_t startIndex, int32_t winRows, int32_t rows, int32_t numOutput,
                                  SOperatorInfo* pOperator) {
  SExprSupp*     pSup = &pOperator->exprSupp;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t        code = initSessionOutputBuf(pCurWin, pResult, pSup->pCtx, numOutput, pSup->rowEntryInfoOffset);
  if (code != TSDB_CODE_SUCCESS || (*pResult) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  updateTimeWindowInfo(pTimeWindowData, &pCurWin->sessionWin.win, false);
  applyAggFunctionOnPartialTuples(pTaskInfo, pSup->pCtx, pTimeWindowData, startIndex, winRows, rows, numOutput);
  return TSDB_CODE_SUCCESS;
}

static bool doDeleteSessionWindow(SStreamAggSupporter* pAggSup, SSessionKey* pKey) {
  pAggSup->stateStore.streamStateSessionDel(pAggSup->pState, pKey);
  SSessionKey hashKey = {0};
  getSessionHashKey(pKey, &hashKey);
  tSimpleHashRemove(pAggSup->pResultRows, &hashKey, sizeof(SSessionKey));
  return true;
}

static int32_t setSessionWinOutputInfo(SSHashObj* pStUpdated, SResultWindowInfo* pWinInfo) {
  void* pVal = tSimpleHashGet(pStUpdated, &pWinInfo->sessionWin, sizeof(SSessionKey));
  if (pVal) {
    SResultWindowInfo* pWin = pVal;
    pWinInfo->isOutput = pWin->isOutput;
  }
  return TSDB_CODE_SUCCESS;
}

SStreamStateCur* getNextSessionWinInfo(SStreamAggSupporter* pAggSup, SSHashObj* pStUpdated, SResultWindowInfo* pCurWin,
                                       SResultWindowInfo* pNextWin) {
  SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pCurWin->sessionWin);
  pNextWin->isOutput = true;
  setSessionWinOutputInfo(pStUpdated, pNextWin);
  int32_t size = 0;
  pNextWin->sessionWin = pCurWin->sessionWin;
  int32_t code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->sessionWin, &pNextWin->pOutputBuf, &size);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(pNextWin->pOutputBuf);
    SET_SESSION_WIN_INVALID(*pNextWin);
  }
  return pCur;
}

static void compactSessionWindow(SOperatorInfo* pOperator, SResultWindowInfo* pCurWin, SSHashObj* pStUpdated,
                                 SSHashObj* pStDeleted) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SResultRow*                    pCurResult = NULL;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  initSessionOutputBuf(pCurWin, &pCurResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
  // Just look for the window behind StartIndex
  while (1) {
    SResultWindowInfo winInfo = {0};
    SStreamStateCur*  pCur = getNextSessionWinInfo(pAggSup, pStUpdated, pCurWin, &winInfo);
    if (!IS_VALID_SESSION_WIN(winInfo) || !isInWindow(pCurWin, winInfo.sessionWin.win.skey, pAggSup->gap)) {
      taosMemoryFree(winInfo.pOutputBuf);
      pAPI->stateStore.streamStateFreeCur(pCur);
      break;
    }
    SResultRow* pWinResult = NULL;
    initSessionOutputBuf(&winInfo, &pWinResult, pAggSup->pDummyCtx, numOfOutput, pSup->rowEntryInfoOffset);
    pCurWin->sessionWin.win.ekey = TMAX(pCurWin->sessionWin.win.ekey, winInfo.sessionWin.win.ekey);
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pCurWin->sessionWin.win, true);
    compactFunctions(pSup->pCtx, pAggSup->pDummyCtx, numOfOutput, pTaskInfo, &pInfo->twAggSup.timeWindowData);
    tSimpleHashRemove(pStUpdated, &winInfo.sessionWin, sizeof(SSessionKey));
    if (winInfo.isOutput && pStDeleted) {
      saveDeleteRes(pStDeleted, winInfo.sessionWin);
    }
    removeSessionResult(pStUpdated, pAggSup->pResultRows, winInfo.sessionWin);
    doDeleteSessionWindow(pAggSup, &winInfo.sessionWin);
    pAPI->stateStore.streamStateFreeCur(pCur);
    taosMemoryFree(winInfo.pOutputBuf);
  }
}

int32_t saveSessionOutputBuf(SStreamAggSupporter* pAggSup, SResultWindowInfo* pWinInfo) {
  saveSessionDiscBuf(pAggSup->pState, &pWinInfo->sessionWin, pWinInfo->pOutputBuf, pAggSup->resultRowSize, &pAggSup->stateStore);
  return TSDB_CODE_SUCCESS;
}

static void doStreamSessionAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pStUpdated,
                                   SSHashObj* pStDeleted, bool hasEndTs) {
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  int32_t                        numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                       groupId = pSDataBlock->info.id.groupId;
  int64_t                        code = TSDB_CODE_SUCCESS;
  SResultRow*                    pResult = NULL;
  int32_t                        rows = pSDataBlock->info.rows;
  int32_t                        winRows = 0;

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);

  SColumnInfoData* pStartTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY*           startTsCols = (int64_t*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = NULL;
  if (hasEndTs) {
    pEndTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->endTsIndex);
  } else {
    pEndTsCol = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  }

  TSKEY*               endTsCols = (int64_t*)pEndTsCol->pData;
  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;
  for (int32_t i = 0; i < rows;) {
    if (pInfo->ignoreExpiredData && isOverdue(endTsCols[i], &pInfo->twAggSup)) {
      i++;
      continue;
    }
    SResultWindowInfo winInfo = {0};
    setSessionOutputBuf(pAggSup, startTsCols[i], endTsCols[i], groupId, &winInfo);
    setSessionWinOutputInfo(pStUpdated, &winInfo);
    winRows = updateSessionWindowInfo(&winInfo, startTsCols, endTsCols, groupId, rows, i, pAggSup->gap,
                                      pAggSup->pResultRows, pStUpdated, pStDeleted);
    // coverity scan error
    if (!winInfo.pOutputBuf) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }

    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    compactSessionWindow(pOperator, &winInfo, pStUpdated, pStDeleted);
    saveSessionOutputBuf(pAggSup, &winInfo);

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE && pStUpdated) {
      code = saveResult(winInfo, pStUpdated);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      SSessionKey key = {0};
      getSessionHashKey(&winInfo.sessionWin, &key);
      tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &winInfo, sizeof(SResultWindowInfo));
    }

    i += winRows;
  }
}

static void doDeleteTimeWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SArray* result) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startDatas = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endDatas = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpDatas = (uint64_t*)pGroupCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    while (1) {
      SSessionKey curWin = {0};
      getCurSessionWindow(pAggSup, startDatas[i], endDatas[i], gpDatas[i], &curWin);
      if (IS_INVALID_SESSION_WIN_KEY(curWin)) {
        break;
      }
      doDeleteSessionWindow(pAggSup, &curWin);
      if (result) {
        saveDeleteInfo(result, curWin);
      }
    }
  }
}

static inline int32_t sessionKeyCompareAsc(const void* pKey1, const void* pKey2) {
  SSessionKey* pWin1 = (SSessionKey*)pKey1;
  SSessionKey* pWin2 = (SSessionKey*)pKey2;

  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->win.skey > pWin2->win.skey) {
    return 1;
  } else if (pWin1->win.skey < pWin2->win.skey) {
    return -1;
  }

  return 0;
}

static int32_t copyUpdateResult(SSHashObj* pStUpdated, SArray* pUpdated) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pStUpdated, pIte, &iter)) != NULL) {
    void* key = tSimpleHashGetKey(pIte, NULL);
    taosArrayPush(pUpdated, key);
  }
  taosArraySort(pUpdated, sessionKeyCompareAsc);
  return TSDB_CODE_SUCCESS;
}

void doBuildDeleteDataBlock(SOperatorInfo* pOp, SSHashObj* pStDeleted, SSDataBlock* pBlock, void** Ite) {
  SStorageAPI* pAPI = &pOp->pTaskInfo->storageAPI;

  blockDataCleanup(pBlock);
  int32_t size = tSimpleHashGetSize(pStDeleted);
  if (size == 0) {
    return;
  }
  blockDataEnsureCapacity(pBlock, size);
  int32_t iter = 0;
  while (((*Ite) = tSimpleHashIterate(pStDeleted, *Ite, &iter)) != NULL) {
    if (pBlock->info.rows + 1 > pBlock->info.capacity) {
      break;
    }
    SSessionKey*     res = tSimpleHashGetKey(*Ite, NULL);
    SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
    colDataSetVal(pStartTsCol, pBlock->info.rows, (const char*)&res->win.skey, false);
    SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
    colDataSetVal(pEndTsCol, pBlock->info.rows, (const char*)&res->win.skey, false);
    SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
    colDataSetNULL(pUidCol, pBlock->info.rows);
    SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
    colDataSetVal(pGpCol, pBlock->info.rows, (const char*)&res->groupId, false);
    SColumnInfoData* pCalStCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
    colDataSetNULL(pCalStCol, pBlock->info.rows);
    SColumnInfoData* pCalEdCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
    colDataSetNULL(pCalEdCol, pBlock->info.rows);

    SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

    void* tbname = NULL;
    pAPI->stateStore.streamStateGetParName(pOp->pTaskInfo->streamInfo.pState, res->groupId, &tbname);
    if (tbname == NULL) {
      colDataSetNULL(pTableCol, pBlock->info.rows);
    } else {
      char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
      STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
      colDataSetVal(pTableCol, pBlock->info.rows, (const char*)parTbName, false);
      pAPI->stateStore.streamStateFreeVal(tbname);
    }
    pBlock->info.rows += 1;
  }
  if ((*Ite) == NULL) {
    tSimpleHashClear(pStDeleted);
  }
}

static void rebuildSessionWindow(SOperatorInfo* pOperator, SArray* pWinArray, SSHashObj* pStUpdated) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  int32_t                        size = taosArrayGetSize(pWinArray);
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  int32_t                        numOfOutput = pSup->numOfExprs;
  int32_t                        numOfChild = taosArrayGetSize(pInfo->pChildren);

  for (int32_t i = 0; i < size; i++) {
    SSessionKey*      pWinKey = taosArrayGet(pWinArray, i);
    int32_t           num = 0;
    SResultWindowInfo parentWin = {0};
    for (int32_t j = 0; j < numOfChild; j++) {
      SOperatorInfo*                 pChild = taosArrayGetP(pInfo->pChildren, j);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      SStreamAggSupporter*           pChAggSup = &pChInfo->streamAggSup;
      SSessionKey                    chWinKey = {0};
      getSessionHashKey(pWinKey, &chWinKey);
      SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyCurrentNext(pChAggSup->pState, &chWinKey);
      SResultRow*      pResult = NULL;
      SResultRow*      pChResult = NULL;
      while (1) {
        SResultWindowInfo childWin = {0};
        childWin.sessionWin = *pWinKey;
        int32_t code = getSessionWinBuf(pChAggSup, pCur, &childWin);
        if (code == TSDB_CODE_SUCCESS && pWinKey->win.skey <= childWin.sessionWin.win.skey &&
            childWin.sessionWin.win.ekey <= pWinKey->win.ekey) {
          if (num == 0) {
            setSessionOutputBuf(pAggSup, pWinKey->win.skey, pWinKey->win.ekey, pWinKey->groupId, &parentWin);
            code = initSessionOutputBuf(&parentWin, &pResult, pSup->pCtx, numOfOutput, pSup->rowEntryInfoOffset);
            if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
              break;
            }
          }
          num++;
          updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &parentWin.sessionWin.win, true);
          initSessionOutputBuf(&childWin, &pChResult, pChild->exprSupp.pCtx, numOfOutput,
                               pChild->exprSupp.rowEntryInfoOffset);
          compactFunctions(pSup->pCtx, pChild->exprSupp.pCtx, numOfOutput, pTaskInfo, &pInfo->twAggSup.timeWindowData);
          compactSessionWindow(pOperator, &parentWin, pStUpdated, NULL);
          saveResult(parentWin, pStUpdated);
        } else {
          break;
        }
      }
      pAPI->stateStore.streamStateFreeCur(pCur);
    }
    if (num > 0) {
      saveSessionOutputBuf(pAggSup, &parentWin);
    }
  }
}

int32_t closeSessionWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SSHashObj* pClosed) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pWinInfo = pIte;
    if (isCloseWindow(&pWinInfo->sessionWin.win, pTwSup)) {
      if (pTwSup->calTrigger == STREAM_TRIGGER_WINDOW_CLOSE && pClosed) {
        int32_t code = saveResult(*pWinInfo, pClosed);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      SSessionKey* pKey = tSimpleHashGetKey(pIte, NULL);
      tSimpleHashIterateRemove(pHashMap, pKey, sizeof(SSessionKey), &pIte, &iter);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void closeChildSessionWindow(SArray* pChildren, TSKEY maxTs) {
  int32_t size = taosArrayGetSize(pChildren);
  for (int32_t i = 0; i < size; i++) {
    SOperatorInfo*                 pChildOp = taosArrayGetP(pChildren, i);
    SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
    pChInfo->twAggSup.maxTs = TMAX(pChInfo->twAggSup.maxTs, maxTs);
    closeSessionWindow(pChInfo->streamAggSup.pResultRows, &pChInfo->twAggSup, NULL);
  }
}

int32_t getAllSessionWindow(SSHashObj* pHashMap, SSHashObj* pStUpdated) {
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHashMap, pIte, &iter)) != NULL) {
    SResultWindowInfo* pWinInfo = pIte;
    saveResult(*pWinInfo, pStUpdated);
  }
  return TSDB_CODE_SUCCESS;
}

static void copyDeleteWindowInfo(SArray* pResWins, SSHashObj* pStDeleted) {
  int32_t size = taosArrayGetSize(pResWins);
  for (int32_t i = 0; i < size; i++) {
    SSessionKey* pWinKey = taosArrayGet(pResWins, i);
    if (!pWinKey) continue;
    SSessionKey winInfo = {0};
    getSessionHashKey(pWinKey, &winInfo);
    tSimpleHashPut(pStDeleted, &winInfo, sizeof(SSessionKey), NULL, 0);
  }
}

// the allocated memory comes from outer function.
void initGroupResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList) {
  pGroupResInfo->pRows = pArrayList;
  pGroupResInfo->index = 0;
  pGroupResInfo->pBuf = NULL;
}

void doBuildSessionResult(SOperatorInfo* pOperator, void* pState, SGroupResInfo* pGroupResInfo,
                          SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    cleanupGroupResInfo(pGroupResInfo);
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  buildSessionResultDataBlock(pOperator, pState, pBlock, &pOperator->exprSupp, pGroupResInfo);
}

static SSDataBlock* doStreamSessionAgg(SOperatorInfo* pOperator) {
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteDataBlock(pOperator, pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
      return pInfo->pDelRes;
    }
    doBuildSessionResult(pOperator, pAggSup->pState, &pInfo->groupResInfo, pBInfo->pRes);
    if (pBInfo->pRes->info.rows > 0) {
      printDataBlock(pBInfo->pRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
      return pBInfo->pRes;
    }

    setOperatorCompleted(pOperator);
    return NULL;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SSessionKey));
  }
  if (!pInfo->pStUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
  }
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, IS_FINAL_OP(pInfo) ? "final session recv" : "single session recv");

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
      // gap must be 0
      doDeleteTimeWindows(pAggSup, pBlock, pWins);
      removeSessionResults(pInfo->pStUpdated, pWins);
      if (IS_FINAL_OP(pInfo)) {
        int32_t                        childIndex = getChildIndex(pBlock);
        SOperatorInfo*                 pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        // gap must be 0
        doDeleteTimeWindows(&pChildInfo->streamAggSup, pBlock, NULL);
        rebuildSessionWindow(pOperator, pWins, pInfo->pStUpdated);
      }
      copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pAggSup->pResultRows, pInfo->pStUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamSessionAggImpl(pOperator, pBlock, pInfo->pStUpdated, pInfo->pStDeleted, IS_FINAL_OP(pInfo));
    if (IS_FINAL_OP(pInfo)) {
      int32_t chIndex = getChildIndex(pBlock);
      int32_t size = taosArrayGetSize(pInfo->pChildren);
      // if chIndex + 1 - size > 0, add new child
      for (int32_t i = 0; i < chIndex + 1 - size; i++) {
        SOperatorInfo* pChildOp =
            createStreamFinalSessionAggOperatorInfo(NULL, pInfo->pPhyNode, pOperator->pTaskInfo, 0);
        if (!pChildOp) {
          T_LONG_JMP(pOperator->pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
        }
        taosArrayPush(pInfo->pChildren, &pChildOp);
      }
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      setInputDataBlock(&pChildOp->exprSupp, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
      doStreamSessionAggImpl(pChildOp, pBlock, NULL, NULL, true);
    }
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.watermark);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pAggSup->pResultRows, &pInfo->twAggSup, pInfo->pStUpdated);
  closeChildSessionWindow(pInfo->pChildren, pInfo->twAggSup.maxTs);
  copyUpdateResult(pInfo->pStUpdated, pInfo->pUpdated);
  removeSessionResults(pInfo->pStDeleted, pInfo->pUpdated);
  tSimpleHashCleanup(pInfo->pStUpdated);
  pInfo->pStUpdated = NULL;
  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

#if 0
  char* pBuf = streamStateSessionDump(pAggSup->pState);
  qDebug("===stream===final session%s", pBuf);
  taosMemoryFree(pBuf);
#endif

  doBuildDeleteDataBlock(pOperator, pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
    return pInfo->pDelRes;
  }

  doBuildSessionResult(pOperator, pAggSup->pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, IS_FINAL_OP(pInfo) ? "final session" : "single session");
    return pBInfo->pRes;
  }

  setOperatorCompleted(pOperator);
  return NULL;
}

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                  SExecTaskInfo* pTaskInfo) {
  SSessionWinodwPhysiNode*       pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;
  int32_t                        numOfCols = 0;
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
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  SExprSupp* pSup = &pOperator->exprSupp;

  SExprInfo*   pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  code = initBasicInfoEx(&pInfo->binfo, pSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initStreamAggSupporter(&pInfo->streamAggSup, pSup->pCtx, numOfCols, pSessionNode->gap,
                                pTaskInfo->streamInfo.pState, 0, 0, &pTaskInfo->storageAPI.stateStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pSessionNode->window.watermark,
      .calTrigger = pSessionNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
  };

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;
  if (pSessionNode->window.pTsEnd) {
    pInfo->endTsIndex = ((SColumnNode*)pSessionNode->window.pTsEnd)->slotId;
  }
  pInfo->binfo.pRes = pResBlock;
  pInfo->order = TSDB_ORDER_ASC;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pStDeleted = tSimpleHashInit(64, hashFn);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pChildren = NULL;
  pInfo->isFinal = false;
  pInfo->pPhyNode = pPhyNode;
  pInfo->ignoreExpiredData = pSessionNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pStUpdated = NULL;
  pInfo->dataVersion = 0;

  setOperatorInfo(pOperator, "StreamSessionWindowAggOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION, true,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamSessionAgg, NULL, destroyStreamSessionAggOperatorInfo,
                                         optrDefaultBufFn, NULL);

  if (downstream) {
    initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup);
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

static void clearStreamSessionOperator(SStreamSessionAggOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->streamAggSup.pResultRows);
  pInfo->streamAggSup.stateStore.streamStateSessionClear(pInfo->streamAggSup.pState);
}

static SSDataBlock* doStreamSessionSemiAgg(SOperatorInfo* pOperator) {
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*                pBInfo = &pInfo->binfo;
  TSKEY                          maxTs = INT64_MIN;
  SExprSupp*                     pSup = &pOperator->exprSupp;
  SStreamAggSupporter*           pAggSup = &pInfo->streamAggSup;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  {
    doBuildSessionResult(pOperator, pAggSup->pState, &pInfo->groupResInfo, pBInfo->pRes);
    if (pBInfo->pRes->info.rows > 0) {
      printDataBlock(pBInfo->pRes, "semi session");
      return pBInfo->pRes;
    }

    doBuildDeleteDataBlock(pOperator, pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, "semi session delete");
      return pInfo->pDelRes;
    }

    if (pOperator->status == OP_RES_TO_RETURN) {
      clearFunctionContext(&pOperator->exprSupp);
      // semi interval operator clear disk buffer
      clearStreamSessionOperator(pInfo);
      setOperatorCompleted(pOperator);
      return NULL;
    }
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SSessionKey));
  }
  if (!pInfo->pStUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pStUpdated = tSimpleHashInit(64, hashFn);
  }
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      clearSpecialDataBlock(pInfo->pUpdateRes);
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }
    printDataBlock(pBlock, "semi session recv");

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      // gap must be 0
      SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, pWins);
      removeSessionResults(pInfo->pStUpdated, pWins);
      copyDeleteWindowInfo(pWins, pInfo->pStDeleted);
      taosArrayDestroy(pWins);
      break;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pStUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamSessionAggImpl(pOperator, pBlock, pInfo->pStUpdated, NULL, false);
    maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }

  pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
  pBInfo->pRes->info.watermark = pInfo->twAggSup.maxTs;

  copyUpdateResult(pInfo->pStUpdated, pInfo->pUpdated);
  removeSessionResults(pInfo->pStDeleted, pInfo->pUpdated);
  tSimpleHashCleanup(pInfo->pStUpdated);
  pInfo->pStUpdated = NULL;

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);

#if 0
  char* pBuf = streamStateSessionDump(pAggSup->pState);
  qDebug("===stream===semi session%s", pBuf);
  taosMemoryFree(pBuf);
#endif

  doBuildSessionResult(pOperator, pAggSup->pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, "semi session");
    return pBInfo->pRes;
  }

  doBuildDeleteDataBlock(pOperator, pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, "semi session delete");
    return pInfo->pDelRes;
  }

  clearFunctionContext(&pOperator->exprSupp);
  // semi interval operator clear disk buffer
  clearStreamSessionOperator(pInfo);
  setOperatorCompleted(pOperator);
  return NULL;
}

SOperatorInfo* createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                       SExecTaskInfo* pTaskInfo, int32_t numOfChild) {
  int32_t        code = TSDB_CODE_OUT_OF_MEMORY;
  SOperatorInfo* pOperator = createStreamSessionAggOperatorInfo(downstream, pPhyNode, pTaskInfo);
  if (pOperator == NULL) {
    goto _error;
  }

  SStorageAPI* pAPI = &pTaskInfo->storageAPI;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;

  pInfo->isFinal = (pPhyNode->type == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION);
  char* name = (pInfo->isFinal) ? "StreamSessionFinalAggOperator" : "StreamSessionSemiAggOperator";

  if (pPhyNode->type != QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
    pInfo->pUpdateRes = createSpecialDataBlock(STREAM_CLEAR);
    blockDataEnsureCapacity(pInfo->pUpdateRes, 128);
    pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamSessionSemiAgg, NULL,
                                           destroyStreamSessionAggOperatorInfo, optrDefaultBufFn, NULL);
  }

  setOperatorInfo(pOperator, name, pPhyNode->type, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  pOperator->operatorType = pPhyNode->type;
  if (numOfChild > 0) {
    pInfo->pChildren = taosArrayInit(numOfChild, sizeof(void*));
    for (int32_t i = 0; i < numOfChild; i++) {
      SOperatorInfo* pChildOp = createStreamFinalSessionAggOperatorInfo(NULL, pPhyNode, pTaskInfo, 0);
      if (pChildOp == NULL) {
        goto _error;
      }
      SStreamSessionAggOperatorInfo* pChInfo = pChildOp->info;
      pChInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
      pAPI->stateStore.streamStateSetNumber(pChInfo->streamAggSup.pState, i);
      taosArrayPush(pInfo->pChildren, &pChildOp);
    }
  }

  if (!IS_FINAL_OP(pInfo) || numOfChild == 0) {
    pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
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
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo* pChild = taosArrayGetP(pInfo->pChildren, i);
      destroyOperator(pChild);
    }
    taosArrayDestroy(pInfo->pChildren);
  }
  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  tSimpleHashCleanup(pInfo->pSeDeleted);
  taosMemoryFreeClear(param);
}

bool isTsInWindow(SStateWindowInfo* pWin, TSKEY ts) {
  if (pWin->winInfo.sessionWin.win.skey <= ts && ts <= pWin->winInfo.sessionWin.win.ekey) {
    return true;
  }
  return false;
}

bool isEqualStateKey(SStateWindowInfo* pWin, char* pKeyData) {
  return pKeyData && compareVal(pKeyData, pWin->pStateKey);
}

bool compareStateKey(void* data, void* key) {
  SStateKeys* stateKey = (SStateKeys*)key;
  stateKey->pData = (char*)key + sizeof(SStateKeys);
  return compareVal(data, stateKey);
}

void setStateOutputBuf(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, char* pKeyData,
                       SStateWindowInfo* pCurWin, SStateWindowInfo* pNextWin) {
  int32_t size = pAggSup->resultRowSize;
  pCurWin->winInfo.sessionWin.groupId = groupId;
  pCurWin->winInfo.sessionWin.win.skey = ts;
  pCurWin->winInfo.sessionWin.win.ekey = ts;
  int32_t code =
      pAggSup->stateStore.streamStateStateAddIfNotExist(pAggSup->pState, &pCurWin->winInfo.sessionWin, pKeyData, pAggSup->stateKeySize,
                                    compareStateKey, &pCurWin->winInfo.pOutputBuf, &size);
  pCurWin->pStateKey =
      (SStateKeys*)((char*)pCurWin->winInfo.pOutputBuf + (pAggSup->resultRowSize - pAggSup->stateKeySize));
  pCurWin->pStateKey->bytes = pAggSup->stateKeySize - sizeof(SStateKeys);
  pCurWin->pStateKey->type = pAggSup->stateKeyType;
  pCurWin->pStateKey->pData = (char*)pCurWin->pStateKey + sizeof(SStateKeys);
  pCurWin->pStateKey->isNull = false;

  if (code == TSDB_CODE_SUCCESS) {
    pCurWin->winInfo.isOutput = true;
  } else {
    if (IS_VAR_DATA_TYPE(pAggSup->stateKeyType)) {
      varDataCopy(pCurWin->pStateKey->pData, pKeyData);
    } else {
      memcpy(pCurWin->pStateKey->pData, pKeyData, pCurWin->pStateKey->bytes);
    }
  }

  pNextWin->winInfo.sessionWin = pCurWin->winInfo.sessionWin;
  pNextWin->winInfo.pOutputBuf = NULL;
  SStreamStateCur* pCur = pAggSup->stateStore.streamStateSessionSeekKeyNext(pAggSup->pState, &pCurWin->winInfo.sessionWin);
  code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, &pNextWin->winInfo.sessionWin, NULL, 0);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_INVALID(pNextWin->winInfo);
  }
  pAggSup->stateStore.streamStateFreeCur(pCur);
}

int32_t updateStateWindowInfo(SStateWindowInfo* pWinInfo, SStateWindowInfo* pNextWin, TSKEY* pTs, uint64_t groupId,
                              SColumnInfoData* pKeyCol, int32_t rows, int32_t start, bool* allEqual,
                              SSHashObj* pResultRows, SSHashObj* pSeUpdated, SSHashObj* pSeDeleted) {
  *allEqual = true;
  for (int32_t i = start; i < rows; ++i) {
    char* pKeyData = colDataGetData(pKeyCol, i);
    if (!isTsInWindow(pWinInfo, pTs[i])) {
      if (isEqualStateKey(pWinInfo, pKeyData)) {
        if (IS_VALID_SESSION_WIN(pNextWin->winInfo)) {
          // ts belongs to the next window
          if (pTs[i] >= pNextWin->winInfo.sessionWin.win.skey) {
            return i - start;
          }
        }
      } else {
        return i - start;
      }
    }

    if (pWinInfo->winInfo.sessionWin.win.skey > pTs[i]) {
      if (pSeDeleted && pWinInfo->winInfo.isOutput) {
        saveDeleteRes(pSeDeleted, pWinInfo->winInfo.sessionWin);
      }
      removeSessionResult(pSeUpdated, pResultRows, pWinInfo->winInfo.sessionWin);
      pWinInfo->winInfo.sessionWin.win.skey = pTs[i];
    }
    pWinInfo->winInfo.sessionWin.win.ekey = TMAX(pWinInfo->winInfo.sessionWin.win.ekey, pTs[i]);
    if (!isEqualStateKey(pWinInfo, pKeyData)) {
      *allEqual = false;
    }
  }
  return rows - start;
}

static void doStreamStateAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pSeUpdated,
                                 SSHashObj* pStDeleted) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;

  SStreamStateAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                     groupId = pSDataBlock->info.id.groupId;
  int64_t                      code = TSDB_CODE_SUCCESS;
  TSKEY*                       tsCols = NULL;
  SResultRow*                  pResult = NULL;
  int32_t                      winRows = 0;

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);

  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return;
  }

  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;
  int32_t              rows = pSDataBlock->info.rows;
  blockDataEnsureCapacity(pAggSup->pScanBlock, rows);
  SColumnInfoData* pKeyColInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->stateCol.slotId);
  for (int32_t i = 0; i < rows; i += winRows) {
    if (pInfo->ignoreExpiredData && isOverdue(tsCols[i], &pInfo->twAggSup) || colDataIsNull_s(pKeyColInfo, i)) {
      i++;
      continue;
    }
    char*            pKeyData = colDataGetData(pKeyColInfo, i);
    int32_t          winIndex = 0;
    bool             allEqual = true;
    SStateWindowInfo curWin = {0};
    SStateWindowInfo nextWin = {0};
    setStateOutputBuf(pAggSup, tsCols[i], groupId, pKeyData, &curWin, &nextWin);
    setSessionWinOutputInfo(pSeUpdated, &curWin.winInfo);
    winRows = updateStateWindowInfo(&curWin, &nextWin, tsCols, groupId, pKeyColInfo, rows, i, &allEqual,
                                    pAggSup->pResultRows, pSeUpdated, pStDeleted);
    if (!allEqual) {
      uint64_t uid = 0;
      appendOneRowToStreamSpecialBlock(pAggSup->pScanBlock, &curWin.winInfo.sessionWin.win.skey,
                                       &curWin.winInfo.sessionWin.win.ekey, &uid, &groupId, NULL);
      tSimpleHashRemove(pSeUpdated, &curWin.winInfo.sessionWin, sizeof(SSessionKey));
      doDeleteSessionWindow(pAggSup, &curWin.winInfo.sessionWin);
      releaseOutputBuf(pAggSup->pState, NULL, (SResultRow*)curWin.winInfo.pOutputBuf, &pAPI->stateStore);
      continue;
    }
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    saveSessionOutputBuf(pAggSup, &curWin.winInfo);

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = saveResult(curWin.winInfo, pSeUpdated);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
      }
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      SSessionKey key = {0};
      getSessionHashKey(&curWin.winInfo.sessionWin, &key);
      tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curWin.winInfo, sizeof(SResultWindowInfo));
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
    doBuildDeleteDataBlock(pOperator, pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, "single state delete");
      return pInfo->pDelRes;
    }

    doBuildSessionResult(pOperator, pInfo->streamAggSup.pState, &pInfo->groupResInfo, pBInfo->pRes);
    if (pBInfo->pRes->info.rows > 0) {
      printDataBlock(pBInfo->pRes, "single state");
      return pBInfo->pRes;
    }

    setOperatorCompleted(pOperator);
    return NULL;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SSessionKey));
  }
  if (!pInfo->pSeUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeUpdated = tSimpleHashInit(64, hashFn);
  }
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printDataBlock(pBlock, "single state recv");

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      SArray* pWins = taosArrayInit(16, sizeof(SSessionKey));
      doDeleteTimeWindows(&pInfo->streamAggSup, pBlock, pWins);
      removeSessionResults(pInfo->pSeUpdated, pWins);
      copyDeleteWindowInfo(pWins, pInfo->pSeDeleted);
      taosArrayDestroy(pWins);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pSeUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    doStreamStateAggImpl(pOperator, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pInfo->pSeUpdated);
  copyUpdateResult(pInfo->pSeUpdated, pInfo->pUpdated);
  removeSessionResults(pInfo->pSeDeleted, pInfo->pUpdated);
  tSimpleHashCleanup(pInfo->pSeUpdated);
  pInfo->pSeUpdated = NULL;

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

#if 0
  char* pBuf = streamStateSessionDump(pInfo->streamAggSup.pState);
  qDebug("===stream===final session%s", pBuf);
  taosMemoryFree(pBuf);
#endif

  doBuildDeleteDataBlock(pOperator, pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, "single state delete");
    return pInfo->pDelRes;
  }

  doBuildSessionResult(pOperator, pInfo->streamAggSup.pState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, "single state");
    return pBInfo->pRes;
  }
  setOperatorCompleted(pOperator);
  return NULL;
}

SOperatorInfo* createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo) {
  SStreamStateWinodwPhysiNode* pStateNode = (SStreamStateWinodwPhysiNode*)pPhyNode;
  int32_t                      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;
  SColumnNode*                 pColNode = (SColumnNode*)((STargetNode*)pStateNode->pStateKey)->pExpr;
  int32_t                      code = TSDB_CODE_SUCCESS;

  SStreamStateAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamStateAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->stateCol = extractColumnFromColumnNode(pColNode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pStateNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pStateNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pStateNode->window.watermark,
      .calTrigger = pStateNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
  };

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  SExprSupp*   pSup = &pOperator->exprSupp;
  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  code = initBasicInfoEx(&pInfo->binfo, pSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  int32_t keySize = sizeof(SStateKeys) + pColNode->node.resType.bytes;
  int16_t type = pColNode->node.resType.type;
  code = initStreamAggSupporter(&pInfo->streamAggSup, pSup->pCtx, numOfCols, 0, pTaskInfo->streamInfo.pState, keySize,
                                type, &pTaskInfo->storageAPI.stateStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->primaryTsIndex = tsSlotId;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->pChildren = NULL;
  pInfo->ignoreExpiredData = pStateNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pSeUpdated = NULL;
  pInfo->dataVersion = 0;

  setOperatorInfo(pOperator, "StreamStateAggOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamStateAgg, NULL, destroyStreamStateOperatorInfo,
                                         optrDefaultBufFn, NULL);
  initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup);
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

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, true);
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

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &currWin, true);
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
      pBlock = downstream->fpSet.getNextFn(downstream);
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
                                         optrDefaultBufFn, NULL);

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
  int32_t forwardRows =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, iaInfo->binfo.inputTsOrder);
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

  updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &win, true);
  applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &iaInfo->twAggSup.timeWindowData, startPos, forwardRows,
                                  pBlock->info.rows, numOfOutput);
  doCloseWindow(pResultRowInfo, iaInfo, pResult);

  // output previous interval results after this interval (&win) is closed
  outputPrevIntervalResult(pOperatorInfo, tableGroupId, pResultBlock, &win);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = forwardRows - 1 + startPos;
    startPos =
        getNextQualifiedWindow(&iaInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, iaInfo->binfo.inputTsOrder);
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
    forwardRows =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, iaInfo->binfo.inputTsOrder);

    // window start(end) key interpolation
    doWindowBorderInterpolation(iaInfo, pBlock, pResult, &nextWin, startPos, forwardRows, pExprSup);

    updateTimeWindowInfo(&iaInfo->twAggSup.timeWindowData, &nextWin, true);
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
        pBlock = downstream->fpSet.getNextFn(downstream);
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
                                         optrDefaultBufFn, NULL);

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

static SSDataBlock* doStreamIntervalAgg(SOperatorInfo* pOperator) {
  SStreamIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  SExprSupp* pSup = &pOperator->exprSupp;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
    if (pInfo->pDelRes->info.rows > 0) {
      printDataBlock(pInfo->pDelRes, "single interval delete");
      return pInfo->pDelRes;
    }

    doBuildStreamIntervalResult(pOperator, pInfo->pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
    if (pInfo->binfo.pRes->info.rows > 0) {
      printDataBlock(pInfo->binfo.pRes, "single interval");
      return pInfo->binfo.pRes;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseWinInfo(pInfo->aggSup.pResultRowHashTable);
    }

    setOperatorCompleted(pOperator);
    if (pInfo->twAggSup.maxTs > 0 &&
        pInfo->twAggSup.maxTs - pInfo->twAggSup.checkPointInterval > pInfo->twAggSup.checkPointTs) {
      pAPI->stateStore.streamStateCommit(pInfo->pState);
      pAPI->stateStore.streamStateDeleteCheckPoint(pInfo->pState, pInfo->twAggSup.maxTs - pInfo->twAggSup.deleteMark);
      setStreamDataVersion(pTaskInfo, pInfo->dataVersion, pInfo->pState->checkPointId);
      pInfo->twAggSup.checkPointTs = pInfo->twAggSup.maxTs;
    }
    return NULL;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(4096, POINTER_BYTES);
  }

  if (!pInfo->pUpdatedMap) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pUpdatedMap = tSimpleHashInit(4096, hashFn);
  }

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      qDebug("===stream===return data:single interval. recv datablock num:%" PRIu64, pInfo->numOfDatapack);
      pInfo->numOfDatapack = 0;
      break;
    }

    pInfo->numOfDatapack++;
    printDataBlock(pBlock, "single interval recv");

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      doDeleteWindows(pOperator, &pInfo->interval, pBlock, pInfo->pDelWins, pInfo->pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      qDebug("===stream===single interval recv|block type STREAM_GET_ALL");
      pInfo->recvGetAll = true;
      getAllIntervalWindow(pInfo->aggSup.pResultRowHashTable, pInfo->pUpdatedMap);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      printDataBlock(pBlock, "single interval");
      return pBlock;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
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
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    if (pInfo->invertible) {
      setInverFunction(pSup->pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.type);
    }

    doStreamIntervalAggImpl(pOperator, pBlock, pBlock->info.id.groupId, pInfo->pUpdatedMap);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
    pInfo->twAggSup.minTs = TMIN(pInfo->twAggSup.minTs, pBlock->info.window.skey);
  }
  pOperator->status = OP_RES_TO_RETURN;
  removeDeleteResults(pInfo->pUpdatedMap, pInfo->pDelWins);
  closeStreamIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup, &pInfo->interval, NULL,
                            pInfo->pUpdatedMap, pInfo->pDelWins, pOperator);

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->pUpdatedMap, pIte, &iter)) != NULL) {
    taosArrayPush(pInfo->pUpdated, pIte);
  }
  taosArraySort(pInfo->pUpdated, winPosCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  tSimpleHashCleanup(pInfo->pUpdatedMap);
  pInfo->pUpdatedMap = NULL;

#if 0
  char* pBuf = streamStateIntervalDump(pInfo->pState);
  qDebug("===stream===interval state%s", pBuf);
  taosMemoryFree(pBuf);
#endif

  doBuildDeleteResult(pInfo, pInfo->pDelWins, &pInfo->delIndex, pInfo->pDelRes);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, "single interval delete");
    return pInfo->pDelRes;
  }

  doBuildStreamIntervalResult(pOperator, pInfo->pState, pInfo->binfo.pRes, &pInfo->groupResInfo);
  if (pInfo->binfo.pRes->info.rows > 0) {
    printDataBlock(pInfo->binfo.pRes, "single interval");
    return pInfo->binfo.pRes;
  }

  return NULL;
}

SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo) {
  SStreamIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamIntervalOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }
  SStreamIntervalPhysiNode* pIntervalPhyNode = (SStreamIntervalPhysiNode*)pPhyNode;

  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &numOfCols);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  pInfo->interval = (SInterval){
      .interval = pIntervalPhyNode->interval,
      .sliding = pIntervalPhyNode->sliding,
      .intervalUnit = pIntervalPhyNode->intervalUnit,
      .slidingUnit = pIntervalPhyNode->slidingUnit,
      .offset = pIntervalPhyNode->offset,
      .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision,
  };

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pIntervalPhyNode->window.watermark,
      .calTrigger = pIntervalPhyNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(pIntervalPhyNode),
      .checkPointTs = 0,
      .checkPointInterval =
          convertTimePrecision(tsCheckpointInterval, TSDB_TIME_PRECISION_MILLI, pInfo->interval.precision),
  };

  ASSERTS(pInfo->twAggSup.calTrigger != STREAM_TRIGGER_MAX_DELAY, "trigger type should not be max delay");

  pOperator->pTaskInfo = pTaskInfo;
  SStorageAPI* pAPI = &pOperator->pTaskInfo->storageAPI;

  pInfo->ignoreExpiredData = pIntervalPhyNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->isFinal = false;

  SExprSupp* pSup = &pOperator->exprSupp;
  initBasicInfo(&pInfo->binfo, pResBlock);
  initStreamFunciton(pSup->pCtx, pSup->numOfExprs);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pInfo->pState) = *(pTaskInfo->streamInfo.pState);
  pAPI->stateStore.streamStateSetNumber(pInfo->pState, -1);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                    pInfo->pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pIntervalPhyNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pIntervalPhyNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->invertible = allInvertible(pSup->pCtx, numOfCols);
  pInfo->invertible = false;
  pInfo->pDelWins = taosArrayInit(4, sizeof(SWinKey));
  pInfo->delIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pInfo->pPhyNode = NULL;  // create new child
  pInfo->pPullDataMap = NULL;
  pInfo->pFinalPullDataMap = NULL;
  pInfo->pPullWins = NULL;  // SPullWindowInfo
  pInfo->pullIndex = 0;
  pInfo->pPullDataRes = NULL;
  pInfo->isFinal = false;
  pInfo->numOfChild = 0;
  pInfo->delKey.ts = INT64_MAX;
  pInfo->delKey.groupId = 0;
  pInfo->numOfDatapack = 0;
  pInfo->pUpdated = NULL;
  pInfo->pUpdatedMap = NULL;
  int32_t funResSize= getMaxFunResSize(pSup, numOfCols);
  pInfo->pState->pFileState = pTaskInfo->storageAPI.stateStore.streamFileStateInit(tsStreamBufferSize, sizeof(SWinKey), pInfo->aggSup.resultRowSize, funResSize,
                                                  compareTs, pInfo->pState, pInfo->twAggSup.deleteMark);

  setOperatorInfo(pOperator, "StreamIntervalOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamIntervalAgg, NULL,
                                         destroyStreamFinalIntervalOperatorInfo, optrDefaultBufFn, NULL);

  pInfo->statestore = pTaskInfo->storageAPI.stateStore;
  pInfo->recvGetAll = false;

  initIntervalDownStream(downstream, pPhyNode->type, pInfo);
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
