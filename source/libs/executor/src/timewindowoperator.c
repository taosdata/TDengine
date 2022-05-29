#include "executorimpl.h"
#include "functionMgt.h"
#include "tdatablock.h"
#include "ttime.h"

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP = 2,
} SResultTsInterpType;

static SSDataBlock* doStreamFinalIntervalAgg(SOperatorInfo* pOperator);
static SSDataBlock* doStreamSessionWindowAgg(SOperatorInfo* pOperator);

/*
 * There are two cases to handle:
 *
 * 1. Query range is not set yet (queryRangeSet = 0). we need to set the query range info, including
 * pQueryAttr->lastKey, pQueryAttr->window.skey, and pQueryAttr->eKey.
 * 2. Query range is set and query is in progress. There may be another result with the same query ranges to be
 *    merged during merge stage. In this case, we need the pTableQueryInfo->lastResRows to decide if there
 *    is a previous result generated or not.
 */
static void setIntervalQueryRange(STableQueryInfo* pTableQueryInfo, TSKEY key, STimeWindow* pQRange) {
  //  SResultRowInfo*  pResultRowInfo = &pTableQueryInfo->resInfo;
  //  if (pResultRowInfo->curPos != -1) {
  //    return;
  //  }

  //  pTableQueryInfo->win.skey = key;
  //  STimeWindow win = {.skey = key, .ekey = pQRange->ekey};

  /**
   * In handling the both ascending and descending order super table query, we need to find the first qualified
   * timestamp of this table, and then set the first qualified start timestamp.
   * In ascending query, the key is the first qualified timestamp. However, in the descending order query, additional
   * operations involve.
   */
  //  STimeWindow w = TSWINDOW_INITIALIZER;
  //
  //  TSKEY sk = TMIN(win.skey, win.ekey);
  //  TSKEY ek = TMAX(win.skey, win.ekey);
  //  getAlignQueryTimeWindow(pQueryAttr, win.skey, sk, ek, &w);

  //  if (pResultRowInfo->prevSKey == TSKEY_INITIAL_VAL) {
  //    if (!QUERY_IS_ASC_QUERY(pQueryAttr)) {
  //      assert(win.ekey == pQueryAttr->window.ekey);
  //    }
  //
  //    pResultRowInfo->prevSKey = w.skey;
  //  }

  //  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;
}

static TSKEY getStartTsKey(STimeWindow* win, const TSKEY* tsCols, int32_t rows, bool ascQuery) {
  TSKEY ts = TSKEY_INITIAL_VAL;
  if (tsCols == NULL) {
    ts = ascQuery ? win->skey : win->ekey;
  } else {
//    int32_t offset = ascQuery ? 0 : rows - 1;
    ts = tsCols[0];
  }

  return ts;
}

static void getInitialStartTimeWindow(SInterval* pInterval, int32_t precision, TSKEY ts, STimeWindow* w,
                                      bool ascQuery) {
  if (ascQuery) {
    getAlignQueryTimeWindow(pInterval, precision, ts, w);
  } else {
    // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
    getAlignQueryTimeWindow(pInterval, precision, ts, w);

    int64_t key = w->skey;
    while (key < ts) {  // moving towards end
      key = taosTimeAdd(key, pInterval->sliding, pInterval->slidingUnit, precision);
      if (key >= ts) {
        break;
      }

      w->skey = key;
    }
  }
}

// get the correct time window according to the handled timestamp
STimeWindow getActiveTimeWindow(SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int64_t ts,
                                       SInterval* pInterval, int32_t precision, STimeWindow* win) {
  STimeWindow w = {0};

  if (pResultRowInfo->cur.pageId == -1) {  // the first window, from the previous stored value
    getInitialStartTimeWindow(pInterval, precision, ts, &w, true);
    w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
  } else {
    w = getResultRowByPos(pBuf, &pResultRowInfo->cur)->win;
  }

  if (w.skey > ts || w.ekey < ts) {
    if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
      w.skey = taosTimeTruncate(ts, pInterval, precision);
      w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
    } else {
      int64_t st = w.skey;

      if (st > ts) {
        st -= ((st - ts + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
      }

      int64_t et = st + pInterval->interval - 1;
      if (et < ts) {
        st += ((ts - et + pInterval->sliding - 1) / pInterval->sliding) * pInterval->sliding;
      }

      w.skey = st;
      w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
    }
  }
  return w;
}

static int32_t setTimeWindowOutputBuf(SResultRowInfo* pResultRowInfo, STimeWindow* win, bool masterscan,
                                      SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                      int32_t numOfOutput, int32_t* rowCellInfoOffset, SAggSupporter* pAggSup,
                                      SExecTaskInfo* pTaskInfo) {
  assert(win->skey <= win->ekey);
  SResultRow* pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE,
                                                  masterscan, tableGroupId, pTaskInfo, true, pAggSup);

  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // set time window for current result
  pResultRow->win = (*win);
  *pResult = pResultRow;
  setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowCellInfoOffset);
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
  int32_t forwardStep = 0;

  if (order == TSDB_ORDER_ASC) {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardStep = end;

      if (pData[end + pos] == ekey) {
        forwardStep += 1;
      }
    }
  } else {
    int32_t end = searchFn((char*)&pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardStep = end;

      if (pData[end + pos] == ekey) {
        forwardStep += 1;
      }
    }
//    int32_t end = searchFn((char*)pData, pos + 1, ekey, order);
//    if (end >= 0) {
//      forwardStep = pos - end;
//
//      if (pData[end] == ekey) {
//        forwardStep += 1;
//      }
//    }
  }

  assert(forwardStep >= 0);
  return forwardStep;
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

int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos,
                                        TSKEY ekey, __block_search_fn_t searchFn, STableQueryInfo* item,
                                        int32_t order) {
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
  tw->skey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000L, TSDB_TIME_PRECISION_MILLI, precision);

  mon = (int)(mon + interval);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->ekey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000L, TSDB_TIME_PRECISION_MILLI, precision);

  tw->ekey -= 1;
}

void doTimeWindowInterpolation(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SArray* pDataBlock, TSKEY prevTs,
                               int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey, int32_t type) {
  SExprInfo* pExpr = pOperator->pExpr;

  SqlFunctionCtx* pCtx = pInfo->pCtx;

  for (int32_t k = 0; k < pOperator->numOfExprs; ++k) {
    int32_t functionId = pCtx[k].functionId;
    if (functionId != FUNCTION_TWA && functionId != FUNCTION_INTERP) {
      pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SColIndex*       pColIndex = NULL /*&pExpr[k].base.colInfo*/;
    int16_t          index = pColIndex->colIndex;
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, index);

    //    assert(pColInfo->info.colId == pColIndex->info.colId && curTs != windowKey);
    double v1 = 0, v2 = 0, v = 0;

    if (prevRowIndex == -1) {
      //      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char*)pRuntimeEnv->prevRow[index]);
    } else {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char*)pColInfo->pData + prevRowIndex * pColInfo->info.bytes);
    }

    GET_TYPED_DATA(v2, double, pColInfo->info.type, (char*)pColInfo->pData + curRowIndex * pColInfo->info.bytes);

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
    }
  }
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

static bool setTimeWindowInterpolationStartTs(SOperatorInfo* pOperatorInfo, SqlFunctionCtx* pCtx, int32_t pos,
                                              int32_t numOfRows, SArray* pDataBlock, const TSKEY* tsCols,
                                              STimeWindow* win) {
  bool  ascQuery = true;
  TSKEY curTs = tsCols[pos];
  TSKEY lastTs = 0;  //*(TSKEY*)pRuntimeEnv->prevRow[0];

  // lastTs == INT64_MIN and pos == 0 means this is the first time window, interpolation is not needed.
  // start exactly from this point, no need to do interpolation
  TSKEY key = ascQuery ? win->skey : win->ekey;
  if (key == curTs) {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfExprs, RESULT_ROW_START_INTERP);
    return true;
  }

  if (lastTs == INT64_MIN && ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery))) {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfExprs, RESULT_ROW_START_INTERP);
    return true;
  }

  int32_t step = 1;  // GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
  TSKEY   prevTs = ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery)) ? lastTs : tsCols[pos - step];

  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, prevTs, pos - step, curTs, pos, key,
                            RESULT_ROW_START_INTERP);
  return true;
}

static bool setTimeWindowInterpolationEndTs(SOperatorInfo* pOperatorInfo, SqlFunctionCtx* pCtx, int32_t endRowIndex,
                                            SArray* pDataBlock, const TSKEY* tsCols, TSKEY blockEkey,
                                            STimeWindow* win) {
  int32_t order = TSDB_ORDER_ASC;
  int32_t numOfOutput = pOperatorInfo->numOfExprs;

  TSKEY actualEndKey = tsCols[endRowIndex];
  TSKEY key = order ? win->ekey : win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey /*&& QUERY_IS_ASC_QUERY(pQueryAttr)*/) ||
      (key < blockEkey /*&& !QUERY_IS_ASC_QUERY(pQueryAttr)*/)) {
    setNotInterpoWindowKey(pCtx, numOfOutput, RESULT_ROW_END_INTERP);
    return false;
  }

  // there is actual end point of current time window, no interpolation need
  if (key == actualEndKey) {
    setNotInterpoWindowKey(pCtx, numOfOutput, RESULT_ROW_END_INTERP);
    return true;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);
  int32_t nextRowIndex = endRowIndex + step;
  assert(nextRowIndex >= 0);

  TSKEY nextKey = tsCols[nextRowIndex];
  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, actualEndKey, endRowIndex, nextKey,
                            nextRowIndex, key, RESULT_ROW_END_INTERP);
  return true;
}

static int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                      TSKEY* primaryKeys, int32_t prevPosition, int32_t order) {
  bool    ascQuery = (order == TSDB_ORDER_ASC);

  int32_t precision = pInterval->precision;
  getNextTimeWindow(pInterval, precision, order, pNext);

  // next time window is not in current block
  if ((pNext->skey > pDataBlockInfo->window.ekey && order == TSDB_ORDER_ASC) ||
      (pNext->ekey < pDataBlockInfo->window.skey && order == TSDB_ORDER_DESC)) {
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

static bool resultRowInterpolated(SResultRow* pResult, SResultTsInterpType type) {
  assert(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
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

static void doWindowBorderInterpolation(SOperatorInfo* pOperatorInfo, SSDataBlock* pBlock, SqlFunctionCtx* pCtx,
                                        SResultRow* pResult, STimeWindow* win, int32_t startPos, int32_t forwardStep,
                                        int32_t order, bool timeWindowInterpo) {
  if (!timeWindowInterpo) {
    return;
  }

  assert(pBlock != NULL);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (pBlock->pDataBlock == NULL) {
    //    tscError("pBlock->pDataBlock == NULL");
    return;
  }

  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 0);

  TSKEY* tsCols = (TSKEY*)(pColInfo->pData);
  bool   done = resultRowInterpolated(pResult, RESULT_ROW_START_INTERP);
  if (!done) {  // it is not interpolated, now start to generated the interpolated value
    int32_t startRowIndex = startPos;
    bool    interp = setTimeWindowInterpolationStartTs(pOperatorInfo, pCtx, startRowIndex, pBlock->info.rows,
                                                       pBlock->pDataBlock, tsCols, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfExprs, RESULT_ROW_START_INTERP);
  }

  // point interpolation does not require the end key time window interpolation.
  //  if (pointInterpQuery) {
  //    return;
  //  }

  // interpolation query does not generate the time window end interpolation
  done = resultRowInterpolated(pResult, RESULT_ROW_END_INTERP);
  if (!done) {
    int32_t endRowIndex = startPos + (forwardStep - 1) * step;

    TSKEY endKey = (order == TSDB_ORDER_ASC) ? pBlock->info.window.ekey : pBlock->info.window.skey;
    bool  interp =
        setTimeWindowInterpolationEndTs(pOperatorInfo, pCtx, endRowIndex, pBlock->pDataBlock, tsCols, endKey, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfExprs, RESULT_ROW_END_INTERP);
  }
}

static void saveDataBlockLastRow(char** pRow, SArray* pDataBlock, int32_t rowIndex, int32_t numOfCols) {
  if (pDataBlock == NULL) {
    return;
  }

  for (int32_t k = 0; k < numOfCols; ++k) {
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, k);
    memcpy(pRow[k], ((char*)pColInfo->pData) + (pColInfo->info.bytes * rowIndex), pColInfo->info.bytes);
  }
}

typedef int64_t (*__get_value_fn_t)(void* data, int32_t index);

int32_t binarySearch(void* keyList, int num, TSKEY key, int order,
    __get_value_fn_t getValuefn) {
  int    firstPos = 0, lastPos = num - 1, midPos = -1;
  int    numOfRows = 0;

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

int64_t getReskey(void* data, int32_t index) {
  SArray* res = (SArray*) data;
  SResKeyPos* pos = taosArrayGetP(res, index);
  return *(int64_t*)pos->key;
}

static int32_t saveResult(SResultRow* result, uint64_t groupId, SArray* pUpdated) {
  int32_t size = taosArrayGetSize(pUpdated);
  int32_t index = binarySearch(pUpdated, size, result->win.skey, TSDB_ORDER_DESC, getReskey);
  if (index == -1) {
    index = 0;
  } else {
    TSKEY resTs = getReskey(pUpdated, index);
    if (resTs < result->win.skey) {
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
  newPos->pos = (SResultRowPosition){.pageId = result->pageId, .offset = result->offset};
  *(int64_t*)newPos->key = result->win.skey;
  if (taosArrayInsert(pUpdated, index, &newPos) == NULL ){
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static void hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pBlock,
                               uint64_t tableGroupId, SArray* pUpdated) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  int32_t        numOfOutput = pOperatorInfo->numOfExprs;

  int32_t step = 1;
  bool    ascScan = (pInfo->order == TSDB_ORDER_ASC);

  //  int32_t prevIndex = pResultRowInfo->curPos;

  TSKEY* tsCols = NULL;
  if (pBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;

    if (tsCols != NULL) {
      blockDataUpdateTsWindow(pBlock, pInfo->primaryTsIndex);
    }
  }

  int32_t startPos = 0;
  TSKEY   ts = getStartTsKey(&pBlock->info.window, tsCols, pBlock->info.rows, ascScan);

  STimeWindow win = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval,
                                        pInfo->interval.precision, &pInfo->win);
  bool        masterScan = true;
  SResultRow* pResult = NULL;

  int32_t ret = setTimeWindowOutputBuf(pResultRowInfo, &win, masterScan, &pResult, tableGroupId, pInfo->binfo.pCtx,
                                       numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM &&
        (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE ||
         pInfo->twAggSup.calTrigger == 0) ) {
    saveResult(pResult, tableGroupId, pUpdated);
  }

  int32_t forwardStep = 0;
  TSKEY   ekey = ascScan? win.ekey:win.skey;
  forwardStep =
      getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->order);
  ASSERT(forwardStep > 0);

  // prev time window not interpolation yet.
  //  int32_t curIndex = pResultRowInfo->curPos;

#if 0
  if (prevIndex != -1 && prevIndex < curIndex && pInfo->timeWindowInterpo) {
    for (int32_t j = prevIndex; j < curIndex; ++j) {  // previous time window may be all closed already.
      SResultRow* pRes = getResultRow(pResultRowInfo, j);
      if (pRes->closed) {
        assert(resultRowInterpolated(pRes, RESULT_ROW_START_INTERP) && resultRowInterpolated(pRes, RESULT_ROW_END_INTERP));
        continue;
      }

      STimeWindow w = pRes->win;
      ret = setTimeWindowOutputBuf(pResultRowInfo, pBlock->info.uid, &w, masterScan, &pResult, tableGroupId,
                                       pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup,
                                       pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      assert(!resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));
      doTimeWindowInterpolation(pOperatorInfo, &pInfo->binfo, pBlock->pDataBlock, *(TSKEY*)pInfo->pRow[0], -1,
                                tsCols[startPos], startPos, w.ekey, RESULT_ROW_END_INTERP);

      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
      setNotInterpoWindowKey(pInfo->binfo.pCtx, pOperatorInfo->numOfExprs, RESULT_ROW_START_INTERP);

      doApplyFunctions(pInfo->binfo.pCtx, &w, &pInfo->timeWindowData, startPos, 0, tsCols, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
    }

    // restore current time window
    ret = setTimeWindowOutputBuf(pResultRowInfo, pBlock->info.uid, &win, masterScan, &pResult, tableGroupId,
                                     pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup,
                                     pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }
#endif

  // window start key interpolation
  doWindowBorderInterpolation(pOperatorInfo, pBlock, pInfo->binfo.pCtx, pResult, &win, startPos, forwardStep,
                              pInfo->order, false);

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &win, true);
  doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &win, &pInfo->twAggSup.timeWindowData, startPos, forwardStep, tsCols,
                   pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pBlock->info, tsCols, prevEndPos, pInfo->order);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    int32_t code =
        setTimeWindowOutputBuf(pResultRowInfo, &nextWin, masterScan, &pResult, tableGroupId, pInfo->binfo.pCtx,
                               numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM &&
          (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE ||
           pInfo->twAggSup.calTrigger == 0) ) {
      saveResult(pResult, tableGroupId, pUpdated);
    }

    ekey = ascScan? nextWin.ekey:nextWin.skey;
    forwardStep =
        getNumOfRowsInTimeWindow(&pBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, pInfo->order);

    // window start(end) key interpolation
    doWindowBorderInterpolation(pOperatorInfo, pBlock, pInfo->binfo.pCtx, pResult, &nextWin, startPos, forwardStep,
                                pInfo->order, false);

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &nextWin, &pInfo->twAggSup.timeWindowData, startPos, forwardStep, tsCols,
                     pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
  }

  if (pInfo->timeWindowInterpo) {
    int32_t rowIndex = ascScan ? (pBlock->info.rows - 1) : 0;
    saveDataBlockLastRow(pInfo->pRow, pBlock->pDataBlock, rowIndex, pBlock->info.numOfCols);
  }

  //  updateResultRowInfoActiveIndex(pResultRowInfo, &pInfo->win, pRuntimeEnv->current->lastKey, true, false);
}

static int32_t doOpenIntervalAgg(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SIntervalAggOperatorInfo* pInfo = pOperator->info;

  int32_t scanFlag = MAIN_SCAN;

  int64_t st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    getTableScanInfo(pOperator, &pInfo->order, &scanFlag);

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, pInfo->order, scanFlag, true);
    STableQueryInfo* pTableQueryInfo = pInfo->pCurrent;

    setIntervalQueryRange(pTableQueryInfo, pBlock->info.window.skey, &pTaskInfo->window);
    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, pBlock->info.groupId, NULL);

#if 0  // test for encode/decode result info
    if(pOperator->encodeResultRow){
      char *result = NULL;
      int32_t length = 0;
      SAggSupporter   *pSup = &pInfo->aggSup;
      pOperator->encodeResultRow(pOperator, pSup, &pInfo->binfo, &result, &length);
      taosHashClear(pSup->pResultRowHashTable);
      pInfo->binfo.resultRowInfo.size = 0;
      pOperator->decodeResultRow(pOperator, pSup, &pInfo->binfo, result, length);
      if(result){
        taosMemoryFree(result);
      }
    }
#endif
  }

  closeAllResultRows(&pInfo->binfo.resultRowInfo);
  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, pInfo->order);
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

  SColumnInfoData* pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->stateCol.slotId);
  int64_t          gid = pBlock->info.groupId;

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->numOfExprs;
  int16_t bytes = pStateColInfoData->info.bytes;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;

  struct SColumnDataAgg* pAgg = NULL;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    pAgg = (pBlock->pBlockAgg != NULL)? pBlock->pBlockAgg[pInfo->stateCol.slotId]: NULL;
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
      int32_t ret =
          setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pInfo->binfo.pCtx,
                                 numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &window, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
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
  int32_t ret =
      setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid, pInfo->binfo.pCtx,
                             numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &pRowSup->win, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                   pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
}

static SSDataBlock* doStateWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SStateWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*           pBInfo = &pInfo->binfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pBInfo->pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    return pBInfo->pRes;
  }

  int32_t order = TSDB_ORDER_ASC;
  int64_t st = taosGetTimestampUs();

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    doStateWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st)/1000.0;

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pBInfo->resultRowInfo);

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
  if (pBInfo->pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  size_t rows = pBInfo->pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;

  return (rows == 0)? NULL : pBInfo->pRes;
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
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);

    if (pBlock->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }

    size_t rows = pBlock->info.rows;
    pOperator->resultInfo.totalRows += rows;

    return (rows == 0)? NULL:pBlock;
  }
}

// todo merged with the build group result.
static void finalizeUpdatedResult(int32_t numOfOutput, SDiskbasedBuf* pBuf, SArray* pUpdateList,
                                  int32_t* rowCellInfoOffset) {
  size_t num = taosArrayGetSize(pUpdateList);

  for (int32_t i = 0; i < num; ++i) {
    SResKeyPos* pPos = taosArrayGetP(pUpdateList, i);

    SFilePage*  bufPage = getBufPage(pBuf, pPos->pos.pageId);
    SResultRow* pRow = (SResultRow*)((char*)bufPage + pPos->pos.offset);

    for (int32_t j = 0; j < numOfOutput; ++j) {
      SResultRowEntryInfo* pEntry = getResultCell(pRow, j, rowCellInfoOffset);
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

void doClearWindowImpl(SResultRowPosition* p1, SDiskbasedBuf* pResultBuf,
    SOptrBasicInfo* pBinfo, int32_t numOfOutput) {
  SResultRow* pResult = getResultRowByPos(pResultBuf, p1);
  SqlFunctionCtx* pCtx = pBinfo->pCtx;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultCell(pResult, i, pBinfo->rowCellInfoOffset);
    struct SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (fmIsWindowPseudoColumnFunc(pCtx[i].functionId)) {
      continue;
    }
    pResInfo->initialized = false;
    if (pCtx[i].functionId != -1) {
      pCtx[i].fpSet.init(&pCtx[i], pResInfo);
    }
  }
}

void doClearWindow(SAggSupporter* pSup, SOptrBasicInfo* pBinfo, char* pData,
    int16_t bytes, uint64_t groupId, int32_t numOfOutput) {
  SET_RES_WINDOW_KEY(pSup->keyBuf, pData, bytes, groupId);
  SResultRowPosition* p1 =
      (SResultRowPosition*)taosHashGet(pSup->pResultRowHashTable, pSup->keyBuf,
          GET_RES_WINDOW_KEY_LEN(bytes));
  doClearWindowImpl(p1, pSup->pResultBuf, pBinfo, numOfOutput);
}

static void doClearWindows(SAggSupporter* pSup, SOptrBasicInfo* pBinfo,
    SInterval* pInterval, int32_t tsIndex, int32_t numOfOutput, SSDataBlock* pBlock,
    SArray* pUpWins) {
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, tsIndex);
  TSKEY *tsCols = (TSKEY*)pColDataInfo->pData;
  int32_t step = 0;
  for (int32_t i = 0; i < pBlock->info.rows; i += step) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, tsCols[i], pInterval,
                                        pInterval->precision, NULL);
    step = getNumOfRowsInTimeWindow(&pBlock->info, tsCols, i,
        win.ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
    doClearWindow(pSup, pBinfo, (char*)&win.skey, sizeof(TKEY), pBlock->info.groupId, numOfOutput);
    if (pUpWins) {
      taosArrayPush(pUpWins, &win);
    }
  }
}

static int32_t closeIntervalWindow(SHashObj *pHashMap, STimeWindowAggSupp *pSup,
    SInterval* pInterval, SArray* closeWins) {
  void *pIte = NULL;
  size_t keyLen = 0;
  while((pIte = taosHashIterate(pHashMap, pIte)) != NULL) {
    void* key = taosHashGetKey(pIte, &keyLen);
    uint64_t groupId = *(uint64_t*) key;
    ASSERT(keyLen == GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY)));
    TSKEY ts = *(uint64_t*) ((char*)key + sizeof(uint64_t));
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, ts, pInterval,
        pInterval->precision, NULL);
    if (win.ekey < pSup->maxTs - pSup->waterMark) {
      char keyBuf[GET_RES_WINDOW_KEY_LEN(sizeof(TSKEY))];
      SET_RES_WINDOW_KEY(keyBuf, &ts, sizeof(TSKEY), groupId);
      taosHashRemove(pHashMap, keyBuf, keyLen);
      SResKeyPos* pos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
      if (pos == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pos->groupId = groupId;
      pos->pos = *(SResultRowPosition*) pIte;
      *(int64_t*)pos->key = ts;
      if (!taosArrayPush(closeWins, &pos)) {
        taosMemoryFree(pos);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doStreamIntervalAgg(SOperatorInfo* pOperator) {
  SIntervalAggOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  pInfo->order = TSDB_ORDER_ASC;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pInfo->binfo.pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }
    return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  SArray* pUpdated = taosArrayInit(4, POINTER_BYTES);
  SArray* pClosed = taosArrayInit(4, POINTER_BYTES);

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    // The timewindows that overlaps the timestamps of the input pBlock need to be recalculated and return to the
    // caller. Note that all the time window are not close till now.
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, pInfo->order, MAIN_SCAN, true);
    if (pInfo->invertible) {
      setInverFunction(pInfo->binfo.pCtx, pOperator->numOfExprs, pBlock->info.type);
    }

    if (pBlock->info.type == STREAM_REPROCESS) {
      doClearWindows(&pInfo->aggSup, &pInfo->binfo, &pInfo->interval, 0,
          pOperator->numOfExprs, pBlock, NULL);
      qDebug("%s clear existed time window results for updates checked", GET_TASKID(pTaskInfo));
      continue;
    }

    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, pBlock->info.groupId, pUpdated);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
  closeIntervalWindow(pInfo->aggSup.pResultRowHashTable, &pInfo->twAggSup,
      &pInfo->interval, pClosed);
  finalizeUpdatedResult(pOperator->numOfExprs, pInfo->aggSup.pResultBuf, pClosed,
      pInfo->binfo.rowCellInfoOffset);
  if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER__WINDOW_CLOSE) {
    taosArrayAddAll(pUpdated, pClosed);
  }
  taosArrayDestroy(pClosed);
  finalizeUpdatedResult(pOperator->numOfExprs, pInfo->aggSup.pResultBuf, pUpdated,
      pInfo->binfo.rowCellInfoOffset);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);

  pOperator->status = OP_RES_TO_RETURN;

  return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
}

static void destroyStateWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  taosMemoryFreeClear(pInfo->stateKey.pData);
}

void destroyIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  SIntervalAggOperatorInfo* pInfo = (SIntervalAggOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  cleanupAggSup(&pInfo->aggSup);
}

void destroyStreamFinalIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  SStreamFinalIntervalOperatorInfo* pInfo = (SStreamFinalIntervalOperatorInfo *)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  cleanupAggSup(&pInfo->aggSup);
  if (pInfo->pChildren) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, i);
      destroyIntervalOperatorInfo(pChildOp->info, numOfOutput);
      taosMemoryFreeClear(pChildOp->info);
      taosMemoryFreeClear(pChildOp);
    }
  }
}

bool allInvertible(SqlFunctionCtx* pFCtx, int32_t numOfCols) {
  for (int32_t i = 0; i < numOfCols; i++) {
    if (!fmIsInvertible(pFCtx[i].functionId)) {
      return false;
    }
  }
  return true;
}

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  SIntervalAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->order = TSDB_ORDER_ASC;
  pInfo->interval = *pInterval;
  pInfo->execModel = pTaskInfo->execModel;
  pInfo->win = pTaskInfo->window;
  pInfo->twAggSup = *pTwAggSupp;
  pInfo->primaryTsIndex = primaryTsSlotId;

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(pOperator, 4096);

  int32_t code =
      initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResBlock, keyBufSize, pTaskInfo->id.str);

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);
  pInfo->invertible = allInvertible(pInfo->binfo.pCtx, numOfCols);
  pInfo->invertible = false; // Todo(liuyao): Dependent TSDB API

  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)1);

  pOperator->name = "TimeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExprInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(doOpenIntervalAgg, doBuildIntervalResult, doStreamIntervalAgg, NULL,
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

SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  SStreamFinalIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamFinalIntervalOperatorInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }
  pInfo->order = TSDB_ORDER_ASC;
  pInfo->interval = *pInterval;
  pInfo->twAggSup = *pTwAggSupp;
  pInfo->primaryTsIndex = primaryTsSlotId;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(pOperator, 4096);
  int32_t code =
      initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResBlock,
          keyBufSize, pTaskInfo->id.str);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)1);
  int32_t numOfChild = 8;// Todo(liuyao) get it from phy plan
  pInfo->pChildren = taosArrayInit(numOfChild, sizeof(SOperatorInfo));
  for (int32_t i = 0; i < numOfChild; i++) {
    SSDataBlock* chRes = createOneDataBlock(pResBlock, false);
    SOperatorInfo* pChildOp = createIntervalOperatorInfo(NULL, pExprInfo, numOfCols,
        chRes, pInterval, primaryTsSlotId, pTwAggSupp, pTaskInfo);
    if (pChildOp && chRes) {
      taosArrayPush(pInfo->pChildren, &pChildOp);
      continue;
    }
    goto _error;
  }

  pOperator->name = "StreamFinalIntervalOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExprInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet = createOperatorFpSet(NULL, doStreamFinalIntervalAgg, NULL, NULL,
      destroyStreamFinalIntervalOperatorInfo, aggEncodeResultRow, aggDecodeResultRow,
      NULL);

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

SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                                SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                                STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  SIntervalAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIntervalAggOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->order = TSDB_ORDER_ASC;
  pInfo->interval = *pInterval;
  pInfo->execModel = OPTR_EXEC_MODEL_STREAM;
  pInfo->win = pTaskInfo->window;
  pInfo->twAggSup = *pTwAggSupp;
  pInfo->primaryTsIndex = primaryTsSlotId;

  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(pOperator, numOfRows);
  int32_t code =
      initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResBlock, keyBufSize, pTaskInfo->id.str);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->win);

  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)1);

  pOperator->name = "StreamTimeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_INTERVAL;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExprInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->numOfExprs = numOfCols;
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

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->numOfExprs;
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
      int32_t ret =
          setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &window, masterScan, &pResult, gid, pInfo->binfo.pCtx,
                                 numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      // pInfo->numOfRows data belong to the current session window
      updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &window, false);
      doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &window, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                       pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret =
      setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, &pRowSup->win, masterScan, &pResult, gid, pInfo->binfo.pCtx,
                             numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &pRowSup->win, &pInfo->twAggSup.timeWindowData, pRowSup->startRowIndex,
                   pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
}

static SSDataBlock* doSessionWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*          pBInfo = &pInfo->binfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pBInfo->pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    return pBInfo->pRes;
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
    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, order, MAIN_SCAN, true);
    blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);

    doSessionWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pBInfo->resultRowInfo);

  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, TSDB_ORDER_ASC);
  blockDataEnsureCapacity(pBInfo->pRes, pOperator->resultInfo.capacity);
  doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
  if (pBInfo->pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  size_t rows = pBInfo->pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;

  return (rows == 0)? NULL : pBInfo->pRes;
}

static SSDataBlock* doAllIntervalAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    //    doBuildResultDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);
    if (pSliceInfo->binfo.pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pSliceInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }

    return pSliceInfo->binfo.pRes;
  }

  int32_t        order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSliceInfo->binfo.pCtx, pBlock, order, MAIN_SCAN, true);
    //    hashAllIntervalAgg(pOperator, &pSliceInfo->binfo.resultRowInfo, pBlock, 0);
  }

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pSliceInfo->binfo.resultRowInfo);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  //  finalizeQueryResult(pSliceInfo->binfo.pCtx, pOperator->numOfExprs);

  //  initGroupedResultInfo(&pSliceInfo->groupResInfo, &pSliceInfo->binfo.resultRowInfo);
  //  doBuildResultDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pSliceInfo->pRes);

  if (pSliceInfo->binfo.pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pSliceInfo->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pSliceInfo->binfo.pRes->info.rows == 0 ? NULL : pSliceInfo->binfo.pRes;
}

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, SExecTaskInfo* pTaskInfo) {
  STimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  pOperator->name = "TimeSliceOperator";
  //  pOperator->operatorType = OP_AllTimeWindow;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExprInfo;
  pOperator->numOfExprs = numOfCols;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doAllIntervalAgg, NULL, NULL, destroyBasicOperatorInfo,
                                         NULL, NULL, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                             SSDataBlock* pResBlock, STimeWindowAggSupp* pTwAggSup, int32_t tsSlotId,
                                             SColumn* pStateKeyCol, SExecTaskInfo* pTaskInfo) {
  SStateWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStateWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->stateCol = *pStateKeyCol;
  pInfo->stateKey.type = pInfo->stateCol.type;
  pInfo->stateKey.bytes = pInfo->stateCol.bytes;
  pInfo->stateKey.pData = taosMemoryCalloc(1, pInfo->stateCol.bytes);
  if (pInfo->stateKey.pData == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(pOperator, 4096);
  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExpr, numOfCols, pResBlock, keyBufSize, pTaskInfo->id.str);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  pInfo->twAggSup = *pTwAggSup;
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId       = tsSlotId;
  pOperator->name       = "StateWindowOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STATE_WINDOW;
  pOperator->blocking   = true;
  pOperator->status     = OP_NOT_OPENED;
  pOperator->pExpr      = pExpr;
  pOperator->numOfExprs = numOfCols;
  pOperator->pTaskInfo  = pTaskInfo;
  pOperator->info       = pInfo;

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
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                            SSDataBlock* pResBlock, int64_t gap, int32_t tsSlotId,
                                            STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  SSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSessionAggOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(pOperator, 4096);

  int32_t code =
      initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResBlock, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->twAggSup = *pTwAggSupp;
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->tsSlotId = tsSlotId;
  pInfo->gap = gap;
  pInfo->binfo.pRes = pResBlock;
  pInfo->winSup.prevTs = INT64_MIN;
  pInfo->reptScan = false;
  pOperator->name = "SessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExprInfo;
  pOperator->numOfExprs = numOfCols;
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

static SArray* doHashInterval(SOperatorInfo* pOperatorInfo, SSDataBlock* pSDataBlock,
                               int32_t tableGroupId) {
  SStreamFinalIntervalOperatorInfo* pInfo = (SStreamFinalIntervalOperatorInfo*)pOperatorInfo->info;
  SResultRowInfo* pResultRowInfo = &(pInfo->binfo.resultRowInfo);
  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  int32_t numOfOutput = pOperatorInfo->numOfExprs;
  SArray* pUpdated = taosArrayInit(4, POINTER_BYTES);
  int32_t step = 1;
  bool    ascScan = true;
  TSKEY* tsCols = NULL;
  SResultRow* pResult = NULL;
  int32_t forwardStep = 0;

  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return pUpdated;
  }

  int32_t startPos = ascScan ? 0 : (pSDataBlock->info.rows - 1);
  TSKEY ts = getStartTsKey(&pSDataBlock->info.window, tsCols, pSDataBlock->info.rows, ascScan);
  STimeWindow nextWin = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts,
      &pInfo->interval, pInfo->interval.precision, NULL);
  while (1) {
    int32_t code =
        setTimeWindowOutputBuf(pResultRowInfo, &nextWin, true, &pResult, tableGroupId, pInfo->binfo.pCtx,
                               numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    SResKeyPos* pos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
    pos->groupId = tableGroupId;
    pos->pos = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};
    *(int64_t*)pos->key = pResult->win.skey;
    taosArrayPush(pUpdated, &pos);
    forwardStep =
        getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, nextWin.ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);
    // window start(end) key interpolation
    doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &nextWin, startPos, forwardStep,
                                pInfo->order, false);
    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &nextWin, true);
    doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &nextWin, &pInfo->twAggSup.timeWindowData, startPos, forwardStep, tsCols,
                     pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, pInfo->order);
    if (startPos < 0) {
      break;
    }
  }
  return pUpdated;
}

bool isFinalInterval(SStreamFinalIntervalOperatorInfo* pInfo) {
  return pInfo->pChildren != NULL;
}

void compactFunctions(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx,
    int32_t numOfOutput, SExecTaskInfo* pTaskInfo) {
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

static void rebuildIntervalWindow(SStreamFinalIntervalOperatorInfo* pInfo, SArray *pWinArray,
    int32_t groupId, int32_t numOfOutput, SExecTaskInfo* pTaskInfo) {
  int32_t size = taosArrayGetSize(pWinArray);
  ASSERT(pInfo->pChildren);
  for (int32_t i = 0; i < size; i++) {
    STimeWindow* pParentWin = taosArrayGet(pWinArray, i);
    SResultRow* pCurResult = NULL;
    setTimeWindowOutputBuf(&pInfo->binfo.resultRowInfo, pParentWin, true, &pCurResult, 0,
        pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup,
        pTaskInfo);
    int32_t numOfChildren = taosArrayGetSize(pInfo->pChildren);
    for (int32_t j = 0; j < numOfChildren; j++) {
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, j);
      SIntervalAggOperatorInfo* pChInfo = pChildOp->info;
      SResultRow* pChResult = NULL;
      setTimeWindowOutputBuf(&pChInfo->binfo.resultRowInfo, pParentWin, true, &pChResult,
          0, pChInfo->binfo.pCtx, pChildOp->numOfExprs, pChInfo->binfo.rowCellInfoOffset,
          &pChInfo->aggSup, pTaskInfo);
      compactFunctions(pInfo->binfo.pCtx, pChInfo->binfo.pCtx, numOfOutput, pTaskInfo);
    }
  }
}

static SSDataBlock* doStreamFinalIntervalAgg(SOperatorInfo* pOperator) {
  SStreamFinalIntervalOperatorInfo* pInfo = pOperator->info;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SArray* pUpdated = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  } else if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    if (pInfo->binfo.pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }
    return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
  }

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, pInfo->order, MAIN_SCAN, true);
    if (pBlock->info.type == STREAM_REPROCESS) {
      SArray *pUpWins = taosArrayInit(8, sizeof(STimeWindow));
      doClearWindows(&pInfo->aggSup, &pInfo->binfo, &pInfo->interval,
          pInfo->primaryTsIndex, pOperator->numOfExprs, pBlock, pUpWins);
      if (isFinalInterval(pInfo)) {
        int32_t childIndex = 0; //Todo(liuyao) get child id from SSDataBlock
        SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SIntervalAggOperatorInfo* pChildInfo = pChildOp->info;
        doClearWindows(&pChildInfo->aggSup, &pChildInfo->binfo, &pChildInfo->interval,
           pChildInfo->primaryTsIndex, pChildOp->numOfExprs, pBlock, NULL);
        rebuildIntervalWindow(pInfo, pUpWins, pInfo->binfo.pRes->info.groupId,
            pOperator->numOfExprs, pOperator->pTaskInfo);
      }
      taosArrayDestroy(pUpWins);
      continue;
    }
    if (isFinalInterval(pInfo)) {
      int32_t chIndex = 1; //Todo(liuyao) get it from SSDataBlock
      SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, chIndex);
      doStreamIntervalAgg(pChildOp);
    }
    pUpdated = doHashInterval(pOperator, pBlock, 0);
  }

  finalizeUpdatedResult(pOperator->numOfExprs, pInfo->aggSup.pResultBuf, pUpdated, pInfo->binfo.rowCellInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
  pOperator->status = OP_RES_TO_RETURN;
  return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
}

void destroyStreamAggSupporter(SStreamAggSupporter* pSup) {
  taosArrayDestroy(pSup->pResultRows);
  taosMemoryFreeClear(pSup->pKeyBuf);
  destroyDiskbasedBuf(pSup->pResultBuf);
}

void destroyStreamSessionAggOperatorInfo(void* param, int32_t numOfOutput) {
  SStreamSessionAggOperatorInfo* pInfo = (SStreamSessionAggOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  if (pInfo->pChildren != NULL) {
    int32_t size = taosArrayGetSize(pInfo->pChildren);
    for (int32_t i = 0; i < size; i++) {
      SOperatorInfo *pChild = taosArrayGetP(pInfo->pChildren, i);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      destroyStreamSessionAggOperatorInfo(pChInfo, numOfOutput);
      taosMemoryFreeClear(pChild);
      taosMemoryFreeClear(pChInfo);
    }
  }
}

int32_t initBiasicInfo(SOptrBasicInfo* pBasicInfo, SExprInfo* pExprInfo,
    int32_t numOfCols, SSDataBlock* pResultBlock, SDiskbasedBuf* pResultBuf) {
  pBasicInfo->pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pBasicInfo->rowCellInfoOffset);
  pBasicInfo->pRes = pResultBlock;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pBasicInfo->pCtx[i].pBuf = pResultBuf;
  }
  return TSDB_CODE_SUCCESS;
}

void initDummyFunction(SqlFunctionCtx* pDummy, SqlFunctionCtx* pCtx, int32_t nums) {
  for (int i = 0; i < nums; i++) {
    pDummy[i].functionId = pCtx[i].functionId;
  }
}
void initDownStream(SOperatorInfo* downstream, SStreamSessionAggOperatorInfo* pInfo) {
  ASSERT(downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
  SStreamBlockScanInfo* pScanInfo = downstream->info;
  pScanInfo->sessionSup =
      (SessionWindowSupporter){.pStreamAggSup = &pInfo->streamAggSup, .gap = pInfo->gap};
  pScanInfo->pUpdateInfo = updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, 60000 * 60 * 6);
}

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream,
    SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, int64_t gap,
    int32_t tsSlotId, STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_OUT_OF_MEMORY;
  SStreamSessionAggOperatorInfo* pInfo = 
      taosMemoryCalloc(1, sizeof(SStreamSessionAggOperatorInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  initResultSizeInfo(pOperator, 4096);

  code = initStreamAggSupporter(&pInfo->streamAggSup, "StreamSessionAggOperatorInfo");
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initBiasicInfo(&pInfo->binfo, pExprInfo, numOfCols, pResBlock,
      pInfo->streamAggSup.pResultBuf);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pInfo->streamAggSup.resultRowSize = getResultRowSize(pInfo->binfo.pCtx, numOfCols);
  
  pInfo->pDummyCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfCols, sizeof(SqlFunctionCtx));
  if (pInfo->pDummyCtx == NULL) {
    goto _error;
  }
  initDummyFunction(pInfo->pDummyCtx, pInfo->binfo.pCtx, numOfCols);

  pInfo->twAggSup = *pTwAggSupp;
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);
  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  pInfo->primaryTsIndex = tsSlotId;
  pInfo->gap = gap;
  pInfo->binfo.pRes = pResBlock;
  pInfo->order = TSDB_ORDER_ASC;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pStDeleted = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createOneDataBlock(pResBlock, false);
  blockDataEnsureCapacity(pInfo->pDelRes, 64);
  pInfo->pChildren = NULL;

  pOperator->name = "StreamSessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION_WINDOW;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExprInfo;
  pOperator->numOfExprs = numOfCols;
  pOperator->info = pInfo;
  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doStreamSessionWindowAgg,
      NULL, NULL, destroyStreamSessionAggOperatorInfo, aggEncodeResultRow,
      aggDecodeResultRow, NULL);
  pOperator->pTaskInfo = pTaskInfo;
  initDownStream(downstream, pInfo);
  code = appendDownstream(pOperator, &downstream, 1);
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
  SArray* pWinInfos = (SArray*) data;
  SResultWindowInfo* pWin = taosArrayGet(pWinInfos, index);
  return pWin->win.ekey;
}
static bool isInWindow(SResultWindowInfo* pWin, TSKEY ts, int64_t gap) {
  int64_t sGap = ts - pWin->win.skey;
  int64_t eGap = pWin->win.ekey - ts;
  if ( (sGap < 0 && sGap >= -gap) || (eGap < 0 && eGap >= -gap) || (sGap >= 0 && eGap >= 0) ) {
    return true;
  }
  return false;
}

static SResultWindowInfo* insertNewSessionWindow(SArray* pWinInfos, TSKEY ts,
    int32_t index) {
  SResultWindowInfo win = 
      {.pos.offset = -1, .pos.pageId = -1, .win.skey = ts, .win.ekey = ts, .isOutput = false};
  return taosArrayInsert(pWinInfos, index, &win);
}

static SResultWindowInfo* addNewSessionWindow(SArray* pWinInfos, TSKEY ts) {
  SResultWindowInfo win = 
      {.pos.offset = -1, .pos.pageId = -1, .win.skey = ts, .win.ekey = ts, .isOutput = false};
  return taosArrayPush(pWinInfos, &win);
}

SResultWindowInfo* getSessionTimeWindow(SArray* pWinInfos, TSKEY ts, int64_t gap,
    int32_t* pIndex) {
  int32_t size = taosArrayGetSize(pWinInfos);
  if (size == 0) {
    return addNewSessionWindow(pWinInfos, ts);
  }
  // find the first position which is smaller than the key
  int32_t index = binarySearch(pWinInfos, size, ts, TSDB_ORDER_DESC,
      getSessionWindowEndkey);
  SResultWindowInfo* pWin = NULL;
  if (index >= 0) {
    pWin = taosArrayGet(pWinInfos, index);
    if (isInWindow(pWin, ts, gap)) {
      *pIndex = index;
      return pWin;
    }
  }

  if (index + 1 < size) {
    pWin = taosArrayGet(pWinInfos, index + 1);
    if (isInWindow(pWin, ts, gap)) {
      *pIndex = index + 1;
      return pWin;
    }
  }

  if (index == size - 1) {
    *pIndex = taosArrayGetSize(pWinInfos);
    return addNewSessionWindow(pWinInfos, ts);
  }
  *pIndex = index;
  return insertNewSessionWindow(pWinInfos, ts, index);
}

int32_t updateSessionWindowInfo(SResultWindowInfo* pWinInfo, TSKEY* pTs, int32_t rows,
    int32_t start, int64_t gap, SHashObj* pStDeleted) {
  for (int32_t i = start; i < rows; ++i) {
    if (!isInWindow(pWinInfo, pTs[i], gap)) {
      return i - start;
    }
    if (pWinInfo->win.skey > pTs[i]) {
      if (pStDeleted && pWinInfo->isOutput) {
        taosHashPut(pStDeleted, &pWinInfo->pos, sizeof(SResultRowPosition), &pWinInfo->win.skey, sizeof(TSKEY));
        pWinInfo->isOutput = false;
      }
      pWinInfo->win.skey = pTs[i];
    }
    pWinInfo->win.ekey = TMAX(pWinInfo->win.ekey, pTs[i]);
  }
  return rows - start;
}

static int32_t setWindowOutputBuf(SResultWindowInfo* pWinInfo, SResultRow** pResult,
    SqlFunctionCtx* pCtx, int32_t groupId, int32_t numOfOutput,
    int32_t* rowCellInfoOffset, SStreamAggSupporter* pAggSup, SExecTaskInfo* pTaskInfo) {
  assert(pWinInfo->win.skey <= pWinInfo->win.ekey);
  // too many time window in query
  int32_t size = taosArrayGetSize(pAggSup->pResultRows);
  if (size > MAX_INTERVAL_TIME_WINDOW) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }
  
  if (pWinInfo->pos.pageId == -1) {
    *pResult = getNewResultRow_rv(pAggSup->pResultBuf, groupId, pAggSup->resultRowSize);
    if (*pResult == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    initResultRow(*pResult);

    // add a new result set for a new group
    pWinInfo->pos.pageId = (*pResult)->pageId;
    pWinInfo->pos.offset = (*pResult)->offset;
  } else {
    *pResult = getResultRowByPos(pAggSup->pResultBuf, &pWinInfo->pos);
    if (!(*pResult)) {
      qError("getResultRowByPos return NULL, TID:%s", GET_TASKID(pTaskInfo));
      return TSDB_CODE_FAILED;
    }
  }

  // set time window for current result
  (*pResult)->win = pWinInfo->win;
  setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowCellInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static int32_t doOneWindowAgg(SStreamSessionAggOperatorInfo* pInfo,
    SSDataBlock* pSDataBlock, SResultWindowInfo* pCurWin, SResultRow** pResult,
    int32_t startIndex, int32_t winRows, int32_t numOutput, SExecTaskInfo* pTaskInfo ) {
  SColumnInfoData* pColDataInfo =
      taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
  TSKEY* tsCols = (int64_t*)pColDataInfo->pData;
  int32_t code = setWindowOutputBuf(pCurWin, pResult, pInfo->binfo.pCtx, pSDataBlock->info.groupId,
      numOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->streamAggSup, pTaskInfo);
  if (code != TSDB_CODE_SUCCESS || (*pResult) == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pCurWin->win, true);
  doApplyFunctions(pTaskInfo, pInfo->binfo.pCtx, &pCurWin->win,
      &pInfo->twAggSup.timeWindowData, startIndex, winRows, tsCols, pSDataBlock->info.rows,
      numOutput, TSDB_ORDER_ASC);
  return TSDB_CODE_SUCCESS;
}

int32_t copyWinInfoToDataBlock(SSDataBlock* pBlock, SStreamAggSupporter* pAggSup,
     int32_t start, int32_t num, int32_t numOfExprs, SOptrBasicInfo* pBinfo) {
  for (int32_t i = start; i < num; i += 1) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pAggSup->pResultRows, start);
    SFilePage*  bufPage = getBufPage(pAggSup->pResultBuf, pWinInfo->pos.pageId);
    SResultRow* pRow = (SResultRow*)((char*)bufPage + pWinInfo->pos.offset);
    for (int32_t j = 0; j < numOfExprs; ++j) {
      SResultRowEntryInfo* pResultInfo = getResultCell(pRow, j, pBinfo->rowCellInfoOffset);
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, j);
      char* in = GET_ROWCELL_INTERBUF(pBinfo->pCtx[j].resultInfo);
      colDataAppend(pColInfoData, pBlock->info.rows, in, pResultInfo->isNullRes);
    }
    pBlock->info.rows += pRow->numOfRows;
    releaseBufPage(pAggSup->pResultBuf, bufPage);
  }
  blockDataUpdateTsWindow(pBlock, -1);
  return TSDB_CODE_SUCCESS;
}

int32_t getNumCompactWindow(SArray* pWinInfos, int32_t startIndex, int64_t gap) {
  SResultWindowInfo* pCurWin = taosArrayGet(pWinInfos, startIndex);
  int32_t size = taosArrayGetSize(pWinInfos);
  // Just look for the window behind StartIndex
  for (int32_t i = startIndex + 1; i < size; i++) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pWinInfos, i);
    if (!isInWindow(pCurWin, pWinInfo->win.skey, gap)) {
      return i - startIndex - 1;
    }
  }

  return size - startIndex - 1;
}

void compactTimeWindow(SStreamSessionAggOperatorInfo* pInfo, int32_t startIndex, int32_t num,
    int32_t groupId, int32_t numOfOutput, SExecTaskInfo* pTaskInfo, SHashObj* pStUpdated, SHashObj* pStDeleted) {
  SResultWindowInfo* pCurWin = taosArrayGet(pInfo->streamAggSup.pResultRows, startIndex);
  SResultRow* pCurResult = NULL;
  setWindowOutputBuf(pCurWin, &pCurResult, pInfo->binfo.pCtx, groupId,
      numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->streamAggSup, pTaskInfo);
  num += startIndex + 1;
  ASSERT(num <= taosArrayGetSize(pInfo->streamAggSup.pResultRows));
  // Just look for the window behind StartIndex
  for (int32_t i = startIndex + 1; i < num; i++) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pInfo->streamAggSup.pResultRows, i);
    SResultRow* pWinResult = NULL;
    setWindowOutputBuf(pWinInfo, &pWinResult, pInfo->pDummyCtx, groupId,
        numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->streamAggSup, pTaskInfo);
    pCurWin->win.ekey = TMAX(pCurWin->win.ekey, pWinInfo->win.ekey);
    compactFunctions(pInfo->binfo.pCtx, pInfo->pDummyCtx, numOfOutput, pTaskInfo);
    taosHashRemove(pStUpdated, &pWinInfo->pos, sizeof(SResultRowPosition));
    if (pWinInfo->isOutput) {
      taosHashPut(pStDeleted, &pWinInfo->pos, sizeof(SResultRowPosition), &pWinInfo->win.skey, sizeof(TSKEY));
      pWinInfo->isOutput = false;
    }
    taosArrayRemove(pInfo->streamAggSup.pResultRows, i);
  }
}

static void doStreamSessionWindowAggImpl(SOperatorInfo* pOperator,
    SSDataBlock* pSDataBlock, SHashObj* pStUpdated, SHashObj* pStDeleted) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  bool masterScan = true;
  int32_t numOfOutput = pOperator->numOfExprs;
  int64_t groupId = pSDataBlock->info.groupId;
  int64_t gap = pInfo->gap;
  int64_t code = TSDB_CODE_SUCCESS;

  int32_t step = 1;
  bool    ascScan = true;
  TSKEY* tsCols = NULL;
  SResultRow* pResult = NULL;
  int32_t winRows = 0;

  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo =
        taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return ;
  }
  
  SStreamAggSupporter* pAggSup = &pInfo->streamAggSup;
  for(int32_t i = 0; i < pSDataBlock->info.rows; ) {
    int32_t winIndex = 0;
    SResultWindowInfo* pCurWin =
        getSessionTimeWindow(pAggSup->pResultRows, tsCols[i], gap, &winIndex);
    winRows =
      updateSessionWindowInfo(pCurWin, tsCols, pSDataBlock->info.rows, i, pInfo->gap, pStDeleted);
    code = doOneWindowAgg(pInfo, pSDataBlock, pCurWin, &pResult, i, winRows, numOfOutput, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    // window start(end) key interpolation
    // doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &nextWin, startPos, forwardStep,
    //                             pInfo->order, false);
    int32_t winNum = getNumCompactWindow(pAggSup->pResultRows, winIndex, gap);
    if (winNum > 0) {
      compactTimeWindow(pInfo, winIndex, winNum, groupId, numOfOutput, pTaskInfo, pStUpdated, pStDeleted);
    }
    pCurWin->isClosed = false;
    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = taosHashPut(pStUpdated, &pCurWin->pos, sizeof(SResultRowPosition), &(pCurWin->win.skey), sizeof(TSKEY));
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      pCurWin->isOutput = true;
    }
    i += winRows;
  }
}

static void doClearSessionWindows(SStreamAggSupporter* pAggSup, SOptrBasicInfo* pBinfo,
    SSDataBlock* pBlock, int32_t tsIndex, int32_t numOfOutput, int64_t gap, SArray* result) {
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, tsIndex);
  TSKEY *tsCols = (TSKEY*)pColDataInfo->pData;
  int32_t step = 0;
  for (int32_t i = 0; i < pBlock->info.rows; i += step) {
    int32_t winIndex = 0;
    SResultWindowInfo* pCurWin =
        getSessionTimeWindow(pAggSup->pResultRows, tsCols[i], gap, &winIndex);
    step = updateSessionWindowInfo(pCurWin, tsCols, pBlock->info.rows, i, gap, NULL);
    ASSERT(isInWindow(pCurWin, tsCols[i], gap));
    doClearWindowImpl(&pCurWin->pos, pAggSup->pResultBuf, pBinfo, numOfOutput);
    if (result) {
      taosArrayPush(result, pCurWin);
    }
  }
}

static int32_t copyUpdateResult(SHashObj* pStUpdated, SArray* pUpdated, int32_t groupId) {
  void* pData = NULL;
  size_t keyLen = 0;
  while((pData = taosHashIterate(pStUpdated, pData)) != NULL) {
    void* key = taosHashGetKey(pData, &keyLen);
    ASSERT(keyLen == sizeof(SResultRowPosition));
    SResKeyPos* pos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
    if (pos == NULL) {
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
    pos->groupId = groupId;
    pos->pos = *(SResultRowPosition*)key;
    *(int64_t*)pos->key = *(uint64_t*)pData;
    taosArrayPush(pUpdated, &pos);
  }
  return TSDB_CODE_SUCCESS;
}

void doBuildDeleteDataBlock(SHashObj* pStDeleted, SSDataBlock* pBlock, void** Ite) {
  blockDataCleanup(pBlock);
  size_t keyLen = 0;
  while(( (*Ite) = taosHashIterate(pStDeleted, *Ite)) != NULL) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, 0);
    colDataAppend(pColInfoData, pBlock->info.rows, *Ite, false);  
    for (int32_t i = 1; i < pBlock->info.numOfCols; i++) {
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

static void rebuildTimeWindow(SStreamSessionAggOperatorInfo* pInfo, SArray *pWinArray,
    int32_t groupId, int32_t numOfOutput, SExecTaskInfo* pTaskInfo) {
  int32_t size = taosArrayGetSize(pWinArray);
  ASSERT(pInfo->pChildren);
  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo* pParentWin = taosArrayGet(pWinArray, i);
    SResultRow* pCurResult = NULL;
    setWindowOutputBuf(pParentWin, &pCurResult, pInfo->binfo.pCtx, groupId,
        numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->streamAggSup, pTaskInfo);
    int32_t numOfChildren = taosArrayGetSize(pInfo->pChildren);
    for (int32_t j = 0; j < numOfChildren; j++) {
      SOperatorInfo* pChild = taosArrayGetP(pInfo->pChildren, j);
      SStreamSessionAggOperatorInfo* pChInfo = pChild->info;
      SArray* pChWins = pChInfo->streamAggSup.pResultRows;
      int32_t chWinSize = taosArrayGetSize(pChWins);
      int32_t index = binarySearch(pChWins, chWinSize, pParentWin->win.skey,
          TSDB_ORDER_DESC, getSessionWindowEndkey);
      for (int32_t k = index; k > 0 && k < chWinSize; k++) {
        SResultWindowInfo* pcw = taosArrayGet(pChWins, k);
        if (pParentWin->win.skey <= pcw->win.skey && pcw->win.ekey <= pParentWin->win.ekey) {
          SResultRow* pChResult = NULL;
          setWindowOutputBuf(pcw, &pChResult, pChInfo->binfo.pCtx, groupId,
              numOfOutput, pChInfo->binfo.rowCellInfoOffset, &pChInfo->streamAggSup, pTaskInfo);
          compactFunctions(pInfo->binfo.pCtx, pChInfo->binfo.pCtx, numOfOutput, pTaskInfo);
          continue;
        }
        break;
      }
    }
  }
}

bool isFinalSession(SStreamSessionAggOperatorInfo* pInfo) {
  return pInfo->pChildren != NULL;
}

int32_t closeSessionWindow(SArray *pWins, STimeWindowAggSupp *pTwSup, SArray *pClosed,
    int8_t calTrigger) {
  // Todo(liuyao) save window to tdb
  int32_t size =  taosArrayGetSize(pWins);
  for (int32_t i = 0; i < size; i++) {
    SResultWindowInfo *pSeWin = taosArrayGet(pWins, i);
    if (pSeWin->win.ekey < pTwSup->maxTs - pTwSup->waterMark) {
      if (!pSeWin->isClosed) {
        SResKeyPos* pos = taosMemoryMalloc(sizeof(SResKeyPos) + sizeof(uint64_t));
        if (pos == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        pos->groupId = 0;
        pos->pos = pSeWin->pos;
        *(int64_t*)pos->key = pSeWin->win.ekey;
        if (!taosArrayPush(pClosed, &pos)) {
          taosMemoryFree(pos);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        pSeWin->isClosed = true;
        if (calTrigger == STREAM_TRIGGER__WINDOW_CLOSE) {
          pSeWin->isOutput = true;
        }
      }
      continue;
    }
    break;
  }
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doStreamSessionWindowAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo* pBInfo = &pInfo->binfo;
  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
    if (pInfo->pDelRes->info.rows > 0) {
      return pInfo->pDelRes;
    }
    doBuildResultDatablock(pOperator, pBInfo, &pInfo->groupResInfo,
        pInfo->streamAggSup.pResultBuf);
    if (pBInfo->pRes->info.rows == 0 ||
        !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }
    return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj* pStUpdated = taosHashInit(64, hashFn, true, HASH_NO_LOCK);
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
     if (pBlock->info.type == STREAM_REPROCESS) {
      SArray *pWins = taosArrayInit(16, sizeof(SResultWindowInfo));
      doClearSessionWindows(&pInfo->streamAggSup, &pInfo->binfo, pBlock, 0,
          pOperator->numOfExprs, pInfo->gap, pWins);
      if (isFinalSession(pInfo)) {
        int32_t childIndex = 0; //Todo(liuyao) get child id from SSDataBlock
        SOperatorInfo* pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
        SStreamSessionAggOperatorInfo* pChildInfo = pChildOp->info;
        doClearSessionWindows(&pChildInfo->streamAggSup, &pChildInfo->binfo, pBlock, 0,
            pChildOp->numOfExprs, pChildInfo->gap, NULL);
        rebuildTimeWindow(pInfo, pWins, pInfo->binfo.pRes->info.groupId, pOperator->numOfExprs, pOperator->pTaskInfo);
      }
      taosArrayDestroy(pWins);
      continue;
    }
    if (isFinalSession(pInfo)) {
      int32_t childIndex = 0; //Todo(liuyao) get child id from SSDataBlock
      SOptrBasicInfo* pChildOp = taosArrayGetP(pInfo->pChildren, childIndex);
      doStreamSessionWindowAggImpl(pOperator, pBlock, NULL, NULL);
    }
    doStreamSessionWindowAggImpl(pOperator, pBlock, pStUpdated, pInfo->pStDeleted);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  
  SArray* pClosed = taosArrayInit(16, POINTER_BYTES);
  closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pClosed,
      pInfo->twAggSup.calTrigger);
  SArray* pUpdated = taosArrayInit(16, POINTER_BYTES);
  copyUpdateResult(pStUpdated, pUpdated, pBInfo->pRes->info.groupId);
  taosHashCleanup(pStUpdated);
  if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER__WINDOW_CLOSE) {
    taosArrayAddAll(pUpdated, pClosed);
  }

  finalizeUpdatedResult(pOperator->numOfExprs, pInfo->streamAggSup.pResultBuf, pUpdated,
      pInfo->binfo.rowCellInfoOffset);
  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  doBuildDeleteDataBlock(pInfo->pStDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    return pInfo->pDelRes;
  }
  doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo,
      pInfo->streamAggSup.pResultBuf);
  return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
}

SOperatorInfo* createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream,
    SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, int64_t gap,
    int32_t tsSlotId, STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_OUT_OF_MEMORY;
  SStreamSessionAggOperatorInfo* pInfo = NULL;
  SOperatorInfo* pOperator = createStreamSessionAggOperatorInfo(downstream, pExprInfo,
      numOfCols, pResBlock, gap, tsSlotId, pTwAggSupp, pTaskInfo);
  if (pOperator == NULL) {
    goto _error;
  }
  pOperator->name = "StreamFinalSessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION_WINDOW;
  int32_t numOfChild = 1; //Todo(liuyao) get it from phy plan
  pInfo = pOperator->info;
  pInfo->pChildren = taosArrayInit(8, sizeof(void *));
  for (int32_t i = 0; i < numOfChild; i++) {
    SOperatorInfo* pChild = createStreamSessionAggOperatorInfo(NULL, pExprInfo,
        numOfCols, NULL, gap, tsSlotId, pTwAggSupp, pTaskInfo);
    if (pChild == NULL) {
      goto _error;
    }
    taosArrayPush(pInfo->pChildren, &pChild);
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
