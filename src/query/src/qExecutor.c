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
#include "os.h"
#include "qFill.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tglobal.h"

#include "exception.h"
#include "hash.h"
#include "qAst.h"
#include "qExecutor.h"
#include "qResultbuf.h"
#include "qUtil.h"
#include "query.h"
#include "queryLog.h"
#include "tlosertree.h"
#include "tscompression.h"
#include "ttime.h"

/**
 * check if the primary column is load by default, otherwise, the program will
 * forced to load primary column explicitly.
 */
#define Q_STATUS_EQUAL(p, s)  (((p) & (s)) != 0)
#define TSDB_COL_IS_TAG(f)    (((f)&TSDB_COL_TAG) != 0)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define IS_MASTER_SCAN(runtime)        ((runtime)->scanFlag == MASTER_SCAN)
#define IS_REVERSE_SCAN(runtime)       ((runtime)->scanFlag == REVERSE_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime)  ((runtime)->scanFlag = MASTER_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define GET_QINFO_ADDR(x) ((SQInfo *)((char *)(x)-offsetof(SQInfo, runtimeEnv)))

#define GET_COL_DATA_POS(query, index, step) ((query)->pos + (index) * (step))
#define SWITCH_ORDER(n) (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

#define SDATA_BLOCK_INITIALIZER (SDataBlockInfo) {{0}, 0}

enum {
  // when query starts to execute, this status will set
  QUERY_NOT_COMPLETED = 0x1u,

  /* result output buffer is full, current query is paused.
   * this status is only exist in group-by clause and diff/add/division/multiply/ query.
   */
  QUERY_RESBUF_FULL = 0x2u,

  /* query is over
   * 1. this status is used in one row result query process, e.g., count/sum/first/last/ avg...etc.
   * 2. when all data within queried time window, it is also denoted as query_completed
   */
  QUERY_COMPLETED = 0x4u,

  /* when the result is not completed return to client, this status will be
   * usually used in case of interval query with interpolation option
   */
  QUERY_OVER = 0x8u,
};

enum {
  TS_JOIN_TS_EQUAL       = 0,
  TS_JOIN_TS_NOT_EQUALS  = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

typedef struct {
  int32_t     status;       // query status
  TSKEY       lastKey;      // the lastKey value before query executed
  STimeWindow w;            // whole query time window
  STimeWindow curWindow;    // current query window
  int32_t     windowIndex;  // index of active time window result for interval query
  STSCursor   cur;
} SQueryStatusInfo;

#if 0
static UNUSED_FUNC void *u_malloc (size_t __size) {
  uint32_t v = rand();
  if (v % 5 <= 1) {
    return NULL;
  } else {
    return malloc(__size);
  }
}

static UNUSED_FUNC void* u_calloc(size_t num, size_t __size) {
  uint32_t v = rand();
  if (v % 5 <= 1) {
    return NULL;
  } else {
    return calloc(num, __size);
  }
}

#define calloc  u_calloc
#define malloc  u_malloc
#endif

#define CLEAR_QUERY_STATUS(q, st)   ((q)->status &= (~(st)))
#define GET_NUM_OF_TABLEGROUP(q)    taosArrayGetSize((q)->tableqinfoGroupInfo.pGroupList)
#define GET_TABLEGROUP(q, _index)   ((SArray*) taosArrayGetP((q)->tableqinfoGroupInfo.pGroupList, (_index)))

static void setQueryStatus(SQuery *pQuery, int8_t status);
static void finalizeQueryResult(SQueryRuntimeEnv *pRuntimeEnv);

#define QUERY_IS_INTERVAL_QUERY(_q) ((_q)->intervalTime > 0)

// previous time window may not be of the same size of pQuery->intervalTime
#define GET_NEXT_TIMEWINDOW(_q, tw)                                   \
  do {                                                                \
    int32_t factor = GET_FORWARD_DIRECTION_FACTOR((_q)->order.order); \
    (tw)->skey += ((_q)->slidingTime * factor);                       \
    (tw)->ekey = (tw)->skey + ((_q)->intervalTime - 1);               \
  } while (0)

// todo move to utility
static int32_t mergeIntoGroupResultImpl(SQInfo *pQInfo, SArray *group);

static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult);
static void setWindowResOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult);
static void resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx, SResultInfo *pResultInfo);
static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void* inputData, TSKEY *tsCol, SDataBlockInfo* pBlockInfo,
                          SDataStatis *pStatis, void *param, int32_t colIndex);

static void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static void destroyTableQueryInfo(STableQueryInfo *pTableQueryInfo);
static void resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static bool hasMainOutput(SQuery *pQuery);
static void buildTagQueryResult(SQInfo *pQInfo);

static int32_t setAdditionalInfo(SQInfo *pQInfo, void *pTable, STableQueryInfo *pTableQueryInfo);
static int32_t flushFromResultBuf(SQInfo *pQInfo);

bool doFilterData(SQuery *pQuery, int32_t elemPos) {
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];

    char *pElem = pFilterInfo->pData + pFilterInfo->info.bytes * elemPos;
    if (isNull(pElem, pFilterInfo->info.type)) {
      return false;
    }

    bool qualified = false;
    for (int32_t j = 0; j < pFilterInfo->numOfFilters; ++j) {
      SColumnFilterElem *pFilterElem = &pFilterInfo->pFilters[j];

      if (pFilterElem->fp(pFilterElem, pElem, pElem)) {
        qualified = true;
        break;
      }
    }

    if (!qualified) {
      return false;
    }
  }

  return true;
}

int64_t getNumOfResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool    hasMainFunction = hasMainOutput(pQuery);

  int64_t maxOutput = 0;
  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].base.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (hasMainFunction &&
        (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ)) {
      continue;
    }

    SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);
    if (pResInfo != NULL && maxOutput < pResInfo->numOfRes) {
      maxOutput = pResInfo->numOfRes;
    }
  }

  assert(maxOutput >= 0);
  return maxOutput;
}

/*
 * the value of number of result needs to be update due to offset value upated.
 */
void updateNumOfResult(SQueryRuntimeEnv *pRuntimeEnv, int32_t numOfRes) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  
  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);
    
    int16_t functionId = pRuntimeEnv->pCtx[j].functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ ||
        functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }
    
    assert(pResInfo->numOfRes > numOfRes);
    pResInfo->numOfRes = numOfRes;
  }
}

static int32_t getGroupResultId(int32_t groupIndex) {
  int32_t base = 200000;
  return base + (groupIndex * 10000);
}

bool isGroupbyNormalCol(SSqlGroupbyExpr *pGroupbyExpr) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndex *pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, i);
    if (pColIndex->flag == TSDB_COL_NORMAL) {
      //make sure the normal column locates at the second position if tbname exists in group by clause
      if (pGroupbyExpr->numOfGroupCols > 1) {
        assert(pColIndex->colIndex > 0);
      }

      return true;
    }
  }

  return false;
}

int16_t getGroupbyColumnType(SQuery *pQuery, SSqlGroupbyExpr *pGroupbyExpr) {
  assert(pGroupbyExpr != NULL);

  int32_t colId = -2;
  int16_t type = TSDB_DATA_TYPE_NULL;

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndex *pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, i);
    if (pColIndex->flag == TSDB_COL_NORMAL) {
      colId = pColIndex->colId;
      break;
    }
  }

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    if (colId == pQuery->colList[i].colId) {
      type = pQuery->colList[i].type;
      break;
    }
  }

  return type;
}

bool isSelectivityWithTagsQuery(SQuery *pQuery) {
  bool    hasTags = false;
  int32_t numOfSelectivity = 0;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functId = pQuery->pSelectExpr[i].base.functionId;
    if (functId == TSDB_FUNC_TAG_DUMMY || functId == TSDB_FUNC_TS_DUMMY) {
      hasTags = true;
      continue;
    }

    if ((aAggs[functId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    }
  }

  if (numOfSelectivity > 0 && hasTags) {
    return true;
  }

  return false;
}

bool isProjQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functId = pQuery->pSelectExpr[i].base.functionId;
    if (functId != TSDB_FUNC_PRJ && functId != TSDB_FUNC_TAGPRJ) {
      return false;
    }
  }

  return true;
}

bool isTSCompQuery(SQuery *pQuery) { return pQuery->pSelectExpr[0].base.functionId == TSDB_FUNC_TS_COMP; }

static bool limitResults(SQueryRuntimeEnv* pRuntimeEnv) {
  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  SQuery* pQuery = pRuntimeEnv->pQuery;
  
  if ((pQuery->limit.limit > 0) && (pQuery->rec.total + pQuery->rec.rows > pQuery->limit.limit)) {
    pQuery->rec.rows = pQuery->limit.limit - pQuery->rec.total;
    
    qDebug("QInfo:%p discard remain data due to result limitation, limit:%"PRId64", current return:%" PRId64 ", total:%"PRId64,
        pQInfo, pQuery->limit.limit, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
    assert(pQuery->rec.rows >= 0);
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return true;
  }

  return false;
}

static bool isTopBottomQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

static bool hasTagValOutput(SQuery* pQuery) {
  SExprInfo *pExprInfo = &pQuery->pSelectExpr[0];
  if (pQuery->numOfOutput == 1 && pExprInfo->base.functionId == TSDB_FUNC_TS_COMP) {
    return true;
  } else {  // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutput; ++idx) {
      SExprInfo *pLocalExprInfo = &pQuery->pSelectExpr[idx];

      // ts_comp column required the tag value for join filter
      if (TSDB_COL_IS_TAG(pLocalExprInfo->base.colInfo.flag)) {
        return true;
      }
    }
  }

  return false;
}

/**
 * @param pQuery
 * @param col
 * @param pDataBlockInfo
 * @param pStatis
 * @param pColStatis
 * @return
 */
static bool hasNullValue(SColIndex* pColIndex, SDataStatis *pStatis, SDataStatis **pColStatis) {
  if (pStatis != NULL && !TSDB_COL_IS_TAG(pColIndex->flag)) {
    *pColStatis = &pStatis[pColIndex->colIndex];
    assert((*pColStatis)->colId == pColIndex->colId);
  } else {
    *pColStatis = NULL;
  }

  if (TSDB_COL_IS_TAG(pColIndex->flag) || pColIndex->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return false;
  }

  if ((*pColStatis) != NULL && (*pColStatis)->numOfNull == 0) {
    return false;
  }

  return true;
}

static SWindowResult *doSetTimeWindowFromKey(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo, char *pData,
                                             int16_t bytes, bool masterscan) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t *p1 = (int32_t *) taosHashGet(pWindowResInfo->hashList, pData, bytes);
  if (p1 != NULL) {
    pWindowResInfo->curIndex = *p1;
  } else {
    if (!masterscan) {  // not master scan, do not add new timewindow
      return NULL;
    }

    // more than the capacity, reallocate the resources
    if (pWindowResInfo->size >= pWindowResInfo->capacity) {
      int64_t newCap = pWindowResInfo->capacity * 1.5;
      char *t = realloc(pWindowResInfo->pResult, newCap * sizeof(SWindowResult));
      if (t == NULL) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      pWindowResInfo->pResult = (SWindowResult *)t;

      int32_t inc = newCap - pWindowResInfo->capacity;
      memset(&pWindowResInfo->pResult[pWindowResInfo->capacity], 0, sizeof(SWindowResult) * inc);

      for (int32_t i = pWindowResInfo->capacity; i < newCap; ++i) {
        createQueryResultInfo(pQuery, &pWindowResInfo->pResult[i], pRuntimeEnv->stableQuery, pRuntimeEnv->interBufSize);
      }

      pWindowResInfo->capacity = newCap;
    }

    // add a new result set for a new group
    pWindowResInfo->curIndex = pWindowResInfo->size++;
    taosHashPut(pWindowResInfo->hashList, pData, bytes, (char *)&pWindowResInfo->curIndex, sizeof(int32_t));
  }

  return getWindowResult(pWindowResInfo, pWindowResInfo->curIndex);
}

// get the correct time window according to the handled timestamp
static STimeWindow getActiveTimeWindow(SWindowResInfo *pWindowResInfo, int64_t ts, SQuery *pQuery) {
  STimeWindow w = {0};

  if (pWindowResInfo->curIndex == -1) {  // the first window, from the previous stored value
    w.skey = pWindowResInfo->prevSKey;
    w.ekey = w.skey + pQuery->intervalTime - 1;
  } else {
    int32_t slot = curTimeWindow(pWindowResInfo);
    w = getWindowResult(pWindowResInfo, slot)->window;
  }

  if (w.skey > ts || w.ekey < ts) {
    int64_t st = w.skey;

    if (st > ts) {
      st -= ((st - ts + pQuery->slidingTime - 1) / pQuery->slidingTime) * pQuery->slidingTime;
    }

    int64_t et = st + pQuery->intervalTime - 1;
    if (et < ts) {
      st += ((ts - et + pQuery->slidingTime - 1) / pQuery->slidingTime) * pQuery->slidingTime;
    }

    w.skey = st;
    w.ekey = w.skey + pQuery->intervalTime - 1;
  }

  /*
   * query border check, skey should not be bounded by the query time range, since the value skey will
   * be used as the time window index value. So we only change ekey of time window accordingly.
   */
  if (w.ekey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) {
    w.ekey = pQuery->window.ekey;
  }

  assert(ts >= w.skey && ts <= w.ekey);

  return w;
}

static int32_t addNewWindowResultBuf(SWindowResult *pWindowRes, SDiskbasedResultBuf *pResultBuf, int32_t sid,
                                     int32_t numOfRowsPerPage) {
  if (pWindowRes->pos.pageId != -1) {
    return 0;
  }

  tFilePage *pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SIDList list = getDataBufPagesIdList(pResultBuf, sid);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewDataBuf(pResultBuf, sid, &pageId);
  } else {
    pageId = getLastPageId(list);
    pData = getResBufPage(pResultBuf, pageId);

    if (pData->num >= numOfRowsPerPage) {
      pData = getNewDataBuf(pResultBuf, sid, &pageId);
      if (pData != NULL) {
        assert(pData->num == 0);  // number of elements must be 0 for new allocated buffer
      }
    }
  }

  if (pData == NULL) {
    return -1;
  }

  // set the number of rows in current disk page
  if (pWindowRes->pos.pageId == -1) {  // not allocated yet, allocate new buffer
    pWindowRes->pos.pageId = pageId;
    pWindowRes->pos.rowId = pData->num++;
  }

  return 0;
}

static int32_t setWindowOutputBufByKey(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo, int32_t sid,
                                       STimeWindow *win, bool masterscan, bool* newWind) {
  assert(win->skey <= win->ekey);
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&win->skey,
      TSDB_KEYSIZE, masterscan);
  if (pWindowRes == NULL) {
    *newWind = false;

    return masterscan? -1:0;
  }

  *newWind = true;

  // not assign result buffer yet, add new result buffer
  if (pWindowRes->pos.pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pWindowRes, pResultBuf, sid, pRuntimeEnv->numOfRowsPerPage);
    if (ret != TSDB_CODE_SUCCESS) {
      return -1;
    }
  }

  // set time window for current result
  pWindowRes->window = *win;

  setWindowResOutputBufInitCtx(pRuntimeEnv, pWindowRes);
  return TSDB_CODE_SUCCESS;
}

static SWindowStatus *getTimeWindowResStatus(SWindowResInfo *pWindowResInfo, int32_t slot) {
  assert(slot >= 0 && slot < pWindowResInfo->size);
  return &pWindowResInfo->pResult[slot].status;
}

static FORCE_INLINE int32_t getForwardStepsInBlock(int32_t numOfRows, __block_search_fn_t searchFn, TSKEY ekey, int16_t pos,
                                      int16_t order, int64_t *pData) {
  int32_t forwardStep = 0;

  if (order == TSDB_ORDER_ASC) {
    int32_t end = searchFn((char*) &pData[pos], numOfRows - pos, ekey, order);
    if (end >= 0) {
      forwardStep = end;

      if (pData[end + pos] == ekey) {
        forwardStep += 1;
      }
    }
  } else {
    int32_t end = searchFn((char *)pData, pos + 1, ekey, order);
    if (end >= 0) {
      forwardStep = pos - end;

      if (pData[end] == ekey) {
        forwardStep += 1;
      }
    }
  }

  assert(forwardStep > 0);
  return forwardStep;
}

/**
 * NOTE: the query status only set for the first scan of master scan.
 */
static int32_t doCheckQueryCompleted(SQueryRuntimeEnv *pRuntimeEnv, TSKEY lastKey, SWindowResInfo *pWindowResInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pRuntimeEnv->scanFlag != MASTER_SCAN || (!QUERY_IS_INTERVAL_QUERY(pQuery))) {
    return pWindowResInfo->size;
  }

  // no qualified results exist, abort check
  int32_t numOfClosed = 0;
  
  if (pWindowResInfo->size == 0) {
    return pWindowResInfo->size;
  }

  // query completed
  if ((lastKey >= pQuery->current->win.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (lastKey <= pQuery->current->win.ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    closeAllTimeWindow(pWindowResInfo);

    pWindowResInfo->curIndex = pWindowResInfo->size - 1;
    setQueryStatus(pQuery, QUERY_COMPLETED | QUERY_RESBUF_FULL);
  } else {  // set the current index to be the last unclosed window
    int32_t i = 0;
    int64_t skey = TSKEY_INITIAL_VAL;

    for (i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *pResult = &pWindowResInfo->pResult[i];
      if (pResult->status.closed) {
        numOfClosed += 1;
        continue;
      }

      if ((pResult->window.ekey <= lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (pResult->window.skey >= lastKey && !QUERY_IS_ASC_QUERY(pQuery))) {
        closeTimeWindow(pWindowResInfo, i);
      } else {
        skey = pResult->window.skey;
        break;
      }
    }

    // all windows are closed, set the last one to be the skey
    if (skey == TSKEY_INITIAL_VAL) {
      assert(i == pWindowResInfo->size);
      pWindowResInfo->curIndex = pWindowResInfo->size - 1;
    } else {
      pWindowResInfo->curIndex = i;
    }

    pWindowResInfo->prevSKey = pWindowResInfo->pResult[pWindowResInfo->curIndex].window.skey;

    // the number of completed slots are larger than the threshold, return current generated results to client.
    if (numOfClosed > pWindowResInfo->threshold) {
      qDebug("QInfo:%p total result window:%d closed:%d, reached the output threshold %d, return",
          GET_QINFO_ADDR(pRuntimeEnv), pWindowResInfo->size, numOfClosed, pQuery->rec.threshold);
      
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
    } else {
      qDebug("QInfo:%p total result window:%d already closed:%d", GET_QINFO_ADDR(pRuntimeEnv), pWindowResInfo->size,
             numOfClosed);
    }
  }
  
  // output has reached the limitation, set query completed
  if (pQuery->limit.limit > 0 && (pQuery->limit.limit + pQuery->limit.offset) <= numOfClosed &&
      pRuntimeEnv->scanFlag == MASTER_SCAN) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }
  
  assert(pWindowResInfo->prevSKey != TSKEY_INITIAL_VAL);
  return numOfClosed;
}

static int32_t getNumOfRowsInTimeWindow(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo, TSKEY *pPrimaryColumn,
                                        int32_t startPos, TSKEY ekey, __block_search_fn_t searchFn, bool updateLastKey) {
  assert(startPos >= 0 && startPos < pDataBlockInfo->rows);

  int32_t num = -1;
  int32_t order = pQuery->order.order;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  STableQueryInfo* item = pQuery->current;
  
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (ekey < pDataBlockInfo->window.ekey) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (updateLastKey) { // update the last key
        item->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
      }
    } else {
      num = pDataBlockInfo->rows - startPos;
      if (updateLastKey) {
        item->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  } else {  // desc
    if (ekey > pDataBlockInfo->window.skey) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (updateLastKey) {  // update the last key
        item->lastKey = pPrimaryColumn[startPos - (num - 1)] + step;
      }
    } else {
      num = startPos + 1;
      if (updateLastKey) {
        item->lastKey = pDataBlockInfo->window.skey + step;
      }
    }
  }

  assert(num > 0);
  return num;
}

static void doBlockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SWindowStatus *pStatus, STimeWindow *pWin,
                                      int32_t offset, int32_t forwardStep, TSKEY *tsBuf, int32_t numOfTotal) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  if (IS_MASTER_SCAN(pRuntimeEnv) || pStatus->closed) {
    for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].base.functionId;

      pCtx[k].nStartQueryTimestamp = pWin->skey;
      pCtx[k].size = forwardStep;
      pCtx[k].startOffset = (QUERY_IS_ASC_QUERY(pQuery)) ? offset : offset - (forwardStep - 1);

      if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
        pCtx[k].ptsList = &tsBuf[offset];
      }

      // not a whole block involved in query processing, statistics data can not be used
      if (forwardStep != numOfTotal) {
        pCtx[k].preAggVals.isSet = false;
      }

      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
  }
}

static void doRowwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SWindowStatus *pStatus, STimeWindow *pWin,
                                    int32_t offset) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  if (IS_MASTER_SCAN(pRuntimeEnv) || pStatus->closed) {
    for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
      pCtx[k].nStartQueryTimestamp = pWin->skey;

      int32_t functionId = pQuery->pSelectExpr[k].base.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunctionF(&pCtx[k], offset);
      }
    }
  }
}

static int32_t getNextQualifiedWindow(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pNext, SDataBlockInfo *pDataBlockInfo,
    TSKEY *primaryKeys, __block_search_fn_t searchFn, int32_t prevPosition) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  GET_NEXT_TIMEWINDOW(pQuery, pNext);

  // next time window is not in current block
  if ((pNext->skey > pDataBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (pNext->ekey < pDataBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
    return -1;
  }

  TSKEY startKey = -1;
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    startKey = pNext->skey;
    if (startKey < pQuery->window.skey) {
      startKey = pQuery->window.skey;
    }
  } else {
    startKey = pNext->ekey;
    if (startKey > pQuery->window.skey) {
      startKey = pQuery->window.skey;
    }
  }

  int32_t startPos = 0;
  // tumbling time window query, a special case of sliding time window query
  if (pQuery->slidingTime == pQuery->intervalTime && prevPosition != -1) {
    int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
    startPos = prevPosition + factor;
  } else {
    startPos = searchFn((char *)primaryKeys, pDataBlockInfo->rows, startKey, pQuery->order.order);
  }

  /*
   * This time window does not cover any data, try next time window,
   * this case may happen when the time window is too small
   */
  if (QUERY_IS_ASC_QUERY(pQuery) && primaryKeys[startPos] > pNext->ekey) {
    TSKEY next = primaryKeys[startPos];

    pNext->ekey += ((next - pNext->ekey + pQuery->slidingTime - 1)/pQuery->slidingTime) * pQuery->slidingTime;
    pNext->skey = pNext->ekey - pQuery->intervalTime + 1;
  } else if ((!QUERY_IS_ASC_QUERY(pQuery)) && primaryKeys[startPos] < pNext->skey) {
    TSKEY next = primaryKeys[startPos];

    pNext->skey -= ((pNext->skey - next + pQuery->slidingTime - 1) / pQuery->slidingTime) * pQuery->slidingTime;
    pNext->ekey = pNext->skey + pQuery->intervalTime - 1;
  }

  return startPos;
}

static FORCE_INLINE TSKEY reviseWindowEkey(SQuery *pQuery, STimeWindow *pWindow) {
  TSKEY ekey = -1;
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    ekey = pWindow->ekey;
    if (ekey > pQuery->window.ekey) {
      ekey = pQuery->window.ekey;
    }
  } else {
    ekey = pWindow->skey;
    if (ekey < pQuery->window.ekey) {
      ekey = pQuery->window.ekey;
    }
  }

  return ekey;
}

//todo binary search
static void* getDataBlockImpl(SArray* pDataBlock, int32_t colId) {
  int32_t numOfCols = taosArrayGetSize(pDataBlock);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData *p = taosArrayGet(pDataBlock, i);
    if (colId == p->info.colId) {
      return p->pData;
    }
  }
  
  return NULL;
}

static char *getDataBlock(SQueryRuntimeEnv *pRuntimeEnv, SArithmeticSupport *sas, int32_t col, int32_t size,
                    SArray *pDataBlock) {
  if (pDataBlock == NULL) {
    return NULL;
  }

  char *dataBlock = NULL;
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  int32_t functionId = pQuery->pSelectExpr[col].base.functionId;
  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pArithExpr = &pQuery->pSelectExpr[col];

    // set the start offset to be the lowest start position, no matter asc/desc query order
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pCtx->startOffset = pQuery->pos;
    } else {
      pCtx->startOffset = pQuery->pos - (size - 1);
    }

    sas->offset  = 0;
    sas->colList = pQuery->colList;
    sas->numOfCols = pQuery->numOfCols;
    sas->data    = calloc(pQuery->numOfCols, POINTER_BYTES);

    if (sas->data == NULL) {
      finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    // here the pQuery->colList and sas->colList are identical
    int32_t numOfCols = taosArrayGetSize(pDataBlock);
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      SColumnInfo *pColMsg = &pQuery->colList[i];

      dataBlock = NULL;
      for (int32_t k = 0; k < numOfCols; ++k) {  //todo refactor
        SColumnInfoData *p = taosArrayGet(pDataBlock, k);
        if (pColMsg->colId == p->info.colId) {
          dataBlock = p->pData;
          break;
        }
      }

      assert(dataBlock != NULL);
      sas->data[i] = dataBlock/* + pQuery->colList[i].bytes*/;  // start from the offset
    }

  } else {  // other type of query function
    SColIndex *pCol = &pQuery->pSelectExpr[col].base.colInfo;
    if (TSDB_COL_IS_TAG(pCol->flag)) {
      dataBlock = NULL;
    } else {
      SColIndex* pColIndex = &pQuery->pSelectExpr[col].base.colInfo;
      SColumnInfoData *p = taosArrayGet(pDataBlock, pColIndex->colIndex);
      assert(p->info.colId == pColIndex->colId);

      dataBlock = p->pData;
    }
  }

  return dataBlock;
}

/**
 * todo set the last value for pQueryTableInfo as in rowwiseapplyfunctions
 * @param pRuntimeEnv
 * @param forwardStep
 * @param tsCols
 * @param pFields
 * @param isDiskFileBlock
 * @return                  the incremental number of output value, so it maybe 0 for fixed number of query,
 *                          such as count/min/max etc.
 */
static void blockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis,
                                       SDataBlockInfo *pDataBlockInfo, SWindowResInfo *pWindowResInfo,
                                       __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  bool masterScan = IS_MASTER_SCAN(pRuntimeEnv);

  SQuery *pQuery = pRuntimeEnv->pQuery;
  TSKEY  *tsCols = NULL;
  if (pDataBlock != NULL) {
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, 0);
    tsCols = (TSKEY *)(pColInfo->pData);
  }

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutput, sizeof(SArithmeticSupport));
  if (sasArray == NULL) {
    finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    char *dataBlock = getDataBlock(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);
    setExecParams(pQuery, &pCtx[k], dataBlock, tsCols, pDataBlockInfo, pStatis, &sasArray[k], k);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (QUERY_IS_INTERVAL_QUERY(pQuery)/* && tsCols != NULL*/) {
    TSKEY ts = TSKEY_INITIAL_VAL;

    if (tsCols == NULL) {
      ts = QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->window.skey:pDataBlockInfo->window.ekey;
    } else {
      int32_t offset = GET_COL_DATA_POS(pQuery, 0, step);
      ts = tsCols[offset];
    }

    bool        hasTimeWindow = false;
    STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);
    if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &win, masterScan, &hasTimeWindow) !=
        TSDB_CODE_SUCCESS) {
      tfree(sasArray);
      return;
    }

    int32_t forwardStep = 0;
    int32_t startPos = pQuery->pos;

    if (hasTimeWindow) {
      TSKEY ekey = reviseWindowEkey(pQuery, &win);
      forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, tsCols, pQuery->pos, ekey, searchFn, true);

      SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doBlockwiseApplyFunctions(pRuntimeEnv, pStatus, &win, startPos, forwardStep, tsCols, pDataBlockInfo->rows);
    }

    int32_t     index = pWindowResInfo->curIndex;
    STimeWindow nextWin = win;

    while (1) {
      int32_t prevEndPos = (forwardStep - 1) * step + startPos;
      startPos = getNextQualifiedWindow(pRuntimeEnv, &nextWin, pDataBlockInfo, tsCols, searchFn, prevEndPos);
      if (startPos < 0) {
        break;
      }

      // null data, failed to allocate more memory buffer
      hasTimeWindow = false;
      if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &nextWin, masterScan,
                                  &hasTimeWindow) != TSDB_CODE_SUCCESS) {
        break;
      }

      if (!hasTimeWindow) {
        continue;
      }

      TSKEY ekey = reviseWindowEkey(pQuery, &nextWin);
      forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, tsCols, startPos, ekey, searchFn, true);

      SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doBlockwiseApplyFunctions(pRuntimeEnv, pStatus, &nextWin, startPos, forwardStep, tsCols, pDataBlockInfo->rows);
    }

    pWindowResInfo->curIndex = index;
  } else {
    /*
     * the sqlfunctionCtx parameters should be set done before all functions are invoked,
     * since the selectivity + tag_prj query needs all parameters been set done.
     * tag_prj function are changed to be TSDB_FUNC_TAG_DUMMY
     */
    for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].base.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
  }

  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    if (pQuery->pSelectExpr[i].base.functionId != TSDB_FUNC_ARITHM) {
      continue;
    }

    tfree(sasArray[i].data);
  }

  tfree(sasArray);
}

static int32_t setGroupResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, char *pData, int16_t type, int16_t bytes) {
  if (isNull(pData, type)) {  // ignore the null value
    return -1;
  }

  int32_t GROUPRESULTID = 1;

  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  int64_t v = -1;
  // not assign result buffer yet, add new result buffer
  switch(type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:  v = GET_INT8_VAL(pData);  break;
    case TSDB_DATA_TYPE_SMALLINT: v = GET_INT16_VAL(pData); break;
    case TSDB_DATA_TYPE_INT:      v = GET_INT32_VAL(pData); break;
    case TSDB_DATA_TYPE_BIGINT:   v = GET_INT64_VAL(pData); break;
  }

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, pData, bytes, true);
  if (pWindowRes == NULL) {
    return -1;
  }

  pWindowRes->window.skey = v;
  pWindowRes->window.ekey = v;

  if (pWindowRes->pos.pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pWindowRes, pResultBuf, GROUPRESULTID, pRuntimeEnv->numOfRowsPerPage);
    if (ret != 0) {
      return -1;
    }
  }

  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);
  return TSDB_CODE_SUCCESS;
}

static char *getGroupbyColumnData(SQuery *pQuery, int16_t *type, int16_t *bytes, SArray* pDataBlock) {
  SSqlGroupbyExpr *pGroupbyExpr = pQuery->pGroupbyExpr;

  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    SColIndex* pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, k);
    if (pColIndex->flag == TSDB_COL_TAG) {
      continue;
    }

    int16_t colIndex = -1;
    int32_t colId = pColIndex->colId;

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      if (pQuery->colList[i].colId == colId) {
        colIndex = i;
        break;
      }
    }

    assert(colIndex >= 0 && colIndex < pQuery->numOfCols);

    *type = pQuery->colList[colIndex].type;
    *bytes = pQuery->colList[colIndex].bytes;
    /*
     *  the colIndex is acquired from the first tables of all qualified tables in this vnode during query prepare
     * stage, the remain tables may not have the required column in cache actually. So, the validation of required
     * column in cache with the corresponding schema is reinforced.
     */
    int32_t numOfCols = taosArrayGetSize(pDataBlock);

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData *p = taosArrayGet(pDataBlock, i);
      if (pColIndex->colId == p->info.colId) {
        return p->pData;
      }
    }
  }

  return NULL;
}

static int32_t doTSJoinFilter(SQueryRuntimeEnv *pRuntimeEnv, int32_t offset) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STSElem         elem = tsBufGetElem(pRuntimeEnv->pTSBuf);
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  // compare tag first
  if (pCtx[0].tag.i64Key != elem.tag) {
    return TS_JOIN_TAG_NOT_EQUALS;
  }

  TSKEY key = *(TSKEY *)(pCtx[0].aInputElemBuf + TSDB_KEYSIZE * offset);

#if defined(_DEBUG_VIEW)
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64 ", tag:%"PRIu64", query order:%d, ts order:%d, traverse:%d, index:%d\n",
         elem.ts, key, elem.tag, pQuery->order.order, pRuntimeEnv->pTSBuf->tsOrder,
         pRuntimeEnv->pTSBuf->cur.order, pRuntimeEnv->pTSBuf->cur.tsIndex);
#endif

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (key < elem.ts) {
      return TS_JOIN_TS_NOT_EQUALS;
    } else if (key > elem.ts) {
      assert(false);
    }
  } else {
    if (key > elem.ts) {
      return TS_JOIN_TS_NOT_EQUALS;
    } else if (key < elem.ts) {
      assert(false);
    }
  }

  return TS_JOIN_TS_EQUAL;
}

static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  SQuery* pQuery = pRuntimeEnv->pQuery;

  // in case of timestamp column, always generated results.
  if (functionId == TSDB_FUNC_TS) {
    return true;
  }
  
  if (pResInfo->complete || functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
    return false;
  }

  if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_FIRST) {
    return QUERY_IS_ASC_QUERY(pQuery);
  }

  // todo add comments
  if ((functionId == TSDB_FUNC_LAST_DST || functionId == TSDB_FUNC_LAST)) {
    return pCtx->param[0].i64Key == pQuery->order.order;
  }

  // in the supplementary scan, only the following functions need to be executed
  if (IS_REVERSE_SCAN(pRuntimeEnv)) {
    return false;
  }

  return true;
}

static void rowwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis, SDataBlockInfo *pDataBlockInfo,
    SWindowResInfo *pWindowResInfo, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  bool masterScan = IS_MASTER_SCAN(pRuntimeEnv);

  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* item = pQuery->current;

  SColumnInfoData* pColumnInfoData = (SColumnInfoData *)taosArrayGet(pDataBlock, 0);

  TSKEY  *tsCols = (pColumnInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP)? (TSKEY*) pColumnInfoData->pData:NULL;
  bool    groupbyColumnValue = pRuntimeEnv->groupbyNormalCol;

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutput, sizeof(SArithmeticSupport));
  if (sasArray == NULL) {
    finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  int16_t type = 0;
  int16_t bytes = 0;

  char *groupbyColumnData = NULL;
  if (groupbyColumnValue) {
    groupbyColumnData = getGroupbyColumnData(pQuery, &type, &bytes, pDataBlock);
  }

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    char *dataBlock = getDataBlock(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);
    setExecParams(pQuery, &pCtx[k], dataBlock, tsCols, pDataBlockInfo, pStatis, &sasArray[k], k);
  }

  // set the input column data
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    pFilterInfo->pData = getDataBlockImpl(pDataBlock, pFilterInfo->info.colId);
    assert(pFilterInfo->pData != NULL);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // from top to bottom in desc
  // from bottom to top in asc order
  if (pRuntimeEnv->pTSBuf != NULL) {
    SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pRuntimeEnv);
    qDebug("QInfo:%p process data rows, numOfRows:%d, query order:%d, ts comp order:%d", pQInfo, pDataBlockInfo->rows,
           pQuery->order.order, pRuntimeEnv->pTSBuf->cur.order);
  }

  int32_t j = 0;
  int32_t offset = -1;

  for (j = 0; j < pDataBlockInfo->rows; ++j) {
    offset = GET_COL_DATA_POS(pQuery, j, step);

    if (pRuntimeEnv->pTSBuf != NULL) {
      int32_t r = doTSJoinFilter(pRuntimeEnv, offset);
      if (r == TS_JOIN_TAG_NOT_EQUALS) {
        break;
      } else if (r == TS_JOIN_TS_NOT_EQUALS) {
        continue;
      } else {
        assert(r == TS_JOIN_TS_EQUAL);
      }
    }

    if (pQuery->numOfFilterCols > 0 && (!doFilterData(pQuery, offset))) {
      continue;
    }

    // interval window query, decide the time window according to the primary timestamp
    if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
      int64_t     ts = tsCols[offset];
      STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

      bool hasTimeWindow = false;
      int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &win, masterScan, &hasTimeWindow);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        continue;
      }

      if (!hasTimeWindow) {
        continue;
      }

      SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doRowwiseApplyFunctions(pRuntimeEnv, pStatus, &win, offset);

      STimeWindow nextWin = win;
      int32_t     index = pWindowResInfo->curIndex;

      while (1) {
        GET_NEXT_TIMEWINDOW(pQuery, &nextWin);
        if ((nextWin.skey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (nextWin.skey < pQuery->window.ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        if (ts < nextWin.skey || ts > nextWin.ekey) {
          break;
        }

        // null data, failed to allocate more memory buffer
        hasTimeWindow = false;
        if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &nextWin, masterScan, &hasTimeWindow) != TSDB_CODE_SUCCESS) {
          break;
        }

        if (hasTimeWindow) {
          pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
          doRowwiseApplyFunctions(pRuntimeEnv, pStatus, &nextWin, offset);
        }
      }

      pWindowResInfo->curIndex = index;
    } else {  // other queries
      // decide which group this rows belongs to according to current state value
      if (groupbyColumnValue) {
        char *val = groupbyColumnData + bytes * offset;

        int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, val, type, bytes);
        if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
          continue;
        }
      }

      for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
        int32_t functionId = pQuery->pSelectExpr[k].base.functionId;
        if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
          aAggs[functionId].xFunctionF(&pCtx[k], offset);
        }
      }
    }

    if (pRuntimeEnv->pTSBuf != NULL) {
      // if timestamp filter list is empty, quit current query
      if (!tsBufNextPos(pRuntimeEnv->pTSBuf)) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
        break;
      }
    }
  }

  assert(offset >= 0);
  if (tsCols != NULL) {
    item->lastKey = tsCols[offset] + step;
  } else {
    item->lastKey = (QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->window.ekey:pDataBlockInfo->window.skey) + step;
  }

  // todo refactor: extract method
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    if (pQuery->pSelectExpr[i].base.functionId != TSDB_FUNC_ARITHM) {
      continue;
    }

    tfree(sasArray[i].data);
  }

  free(sasArray);
}

static int32_t tableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo,
                                          SDataStatis *pStatis, __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  
  STableQueryInfo* pTableQInfo = pQuery->current;
  SWindowResInfo*  pWindowResInfo = &pRuntimeEnv->windowResInfo;
  
  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL || pRuntimeEnv->groupbyNormalCol) {
    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, pDataBlock);
  } else {
    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, searchFn, pDataBlock);
  }

  // update the lastkey of current table
  TSKEY lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pDataBlockInfo->window.ekey : pDataBlockInfo->window.skey;
  pTableQInfo->lastKey = lastKey + GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // interval query with limit applied
  int32_t numOfRes = 0;
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    numOfRes = doCheckQueryCompleted(pRuntimeEnv, lastKey, pWindowResInfo);
  } else {
    numOfRes = getNumOfResult(pRuntimeEnv);

    // update the number of output result
    if (numOfRes > 0 && pQuery->checkBuffer == 1) {
      assert(numOfRes >= pQuery->rec.rows);
      pQuery->rec.rows = numOfRes;

      if (numOfRes >= pQuery->rec.threshold) {
        setQueryStatus(pQuery, QUERY_RESBUF_FULL);
      }

      if ((pQuery->limit.limit >= 0) && (pQuery->limit.limit + pQuery->limit.offset) <= numOfRes) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }
    }
  }

  return numOfRes;
}

void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void* inputData, TSKEY *tsCol, SDataBlockInfo* pBlockInfo,
                   SDataStatis *pStatis, void *param, int32_t colIndex) {
  
  int32_t functionId = pQuery->pSelectExpr[colIndex].base.functionId;
  int32_t colId = pQuery->pSelectExpr[colIndex].base.colInfo.colId;
  
  SDataStatis *tpField = NULL;
  pCtx->hasNull = hasNullValue(&pQuery->pSelectExpr[colIndex].base.colInfo, pStatis, &tpField);
  pCtx->aInputElemBuf = inputData;

  if (tpField != NULL) {
    pCtx->preAggVals.isSet  = true;
    pCtx->preAggVals.statis = *tpField;
    assert(pCtx->preAggVals.statis.numOfNull <= pBlockInfo->rows);
  } else {
    pCtx->preAggVals.isSet = false;
  }

  pCtx->preAggVals.dataBlockLoaded = (inputData != NULL);

  // limit/offset query will affect this value
  pCtx->startOffset = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->pos:0;
  pCtx->size = QUERY_IS_ASC_QUERY(pQuery) ? pBlockInfo->rows - pQuery->pos : pQuery->pos + 1;

  uint32_t status = aAggs[functionId].nStatus;
  if (((status & (TSDB_FUNCSTATE_SELECTIVITY | TSDB_FUNCSTATE_NEED_TS)) != 0) && (tsCol != NULL)) {
    pCtx->ptsList = tsCol;
  }

  if (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) {
    // last_dist or first_dist function
    // store the first&last timestamp into the intermediate buffer [1], the true
    // value may be null but timestamp will never be null
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_TWA ||
             functionId == TSDB_FUNC_DIFF || (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE)) {
    /*
     * least squares function needs two columns of input, currently, the x value of linear equation is set to
     * timestamp column, and the y-value is the column specified in pQuery->pSelectExpr[i].colIdxInBuffer
     *
     * top/bottom function needs timestamp to indicate when the
     * top/bottom values emerge, so does diff function
     */
    if (functionId == TSDB_FUNC_TWA) {
      STwaInfo *pTWAInfo = GET_RES_INFO(pCtx)->interResultBuf;
      pTWAInfo->SKey = pQuery->window.skey;
      pTWAInfo->EKey = pQuery->window.ekey;
    }

  } else if (functionId == TSDB_FUNC_ARITHM) {
    pCtx->param[1].pz = param;
  } else if (functionId == TSDB_FUNC_SPREAD) {  // set the statistics data for primary time stamp column
    if (colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      pCtx->preAggVals.isSet  = true;
      pCtx->preAggVals.statis.min = pBlockInfo->window.skey;
      pCtx->preAggVals.statis.max = pBlockInfo->window.ekey;
    }
  } else if (functionId == TSDB_FUNC_INTERP) {
    SInterpInfoDetail *pInterpInfo = GET_RES_INFO(pCtx)->interResultBuf;
    pInterpInfo->type = pQuery->fillType;
    pInterpInfo->ts = pQuery->window.skey;
    pInterpInfo->primaryCol = (colId == PRIMARYKEY_TIMESTAMP_COL_INDEX);
  
    if (pQuery->fillVal != NULL) {
      if (isNull((const char*) &pQuery->fillVal[colIndex], pCtx->inputType)) {
        pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
      } else { // todo refactor, tVariantCreateFromBinary should handle the NULL value
        tVariantCreateFromBinary(&pCtx->param[1], (char*) &pQuery->fillVal[colIndex], pCtx->inputBytes, pCtx->inputType);
      }
    }
  }

#if defined(_DEBUG_VIEW)
  //  int64_t *tsList = (int64_t *)primaryColumnData;
//  int64_t  s = tsList[0];
//  int64_t  e = tsList[size - 1];

//    if (IS_DATA_BLOCK_LOADED(blockStatus)) {
//        qDebug("QInfo:%p query ts:%lld-%lld, offset:%d, rows:%d, bstatus:%d,
//        functId:%d", GET_QINFO_ADDR(pQuery),
//               s, e, startOffset, size, blockStatus, functionId);
//    } else {
//        qDebug("QInfo:%p block not loaded, bstatus:%d",
//        GET_QINFO_ADDR(pQuery), blockStatus);
//    }
#endif
}

// set the output buffer for the selectivity + tag query
static void setCtxTagColumnInfo(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (isSelectivityWithTagsQuery(pQuery)) {
    int32_t num = 0;
    int16_t tagLen = 0;
    
    SQLFunctionCtx *p = NULL;
    SQLFunctionCtx **pTagCtx = calloc(pQuery->numOfOutput, POINTER_BYTES);

    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SSqlFuncMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].base;
      
      if (pSqlFuncMsg->functionId == TSDB_FUNC_TAG_DUMMY || pSqlFuncMsg->functionId == TSDB_FUNC_TS_DUMMY) {
        tagLen += pCtx[i].outputBytes;
        pTagCtx[num++] = &pCtx[i];
      } else if ((aAggs[pSqlFuncMsg->functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
        p = &pCtx[i];
      } else if (pSqlFuncMsg->functionId == TSDB_FUNC_TS || pSqlFuncMsg->functionId == TSDB_FUNC_TAG) {
        // tag function may be the group by tag column
        // ts may be the required primary timestamp column
        continue;
      } else {
        // the column may be the normal column, group by normal_column, the functionId is TSDB_FUNC_PRJ
      }
    }
    if (p != NULL) {
      p->tagInfo.pTagCtxList = pTagCtx;
      p->tagInfo.numOfTagCols = num;
      p->tagInfo.tagsLen = tagLen;
    } else {
      tfree(pTagCtx); 
    }
  }
}

static FORCE_INLINE void setWindowResultInfo(SResultInfo *pResultInfo, SQuery *pQuery, bool isStableQuery, char* buf) {
  char* p = buf;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t size = pQuery->pSelectExpr[i].interBytes;
    setResultInfoBuf(&pResultInfo[i], size, isStableQuery, p);

    p += size;
  }
}

static int32_t setupQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv, int16_t order) {
  qDebug("QInfo:%p setup runtime env", GET_QINFO_ADDR(pRuntimeEnv));
  SQuery *pQuery = pRuntimeEnv->pQuery;

  size_t size = pRuntimeEnv->interBufSize + pQuery->numOfOutput * sizeof(SResultInfo);

  pRuntimeEnv->resultInfo = calloc(1, size);
  pRuntimeEnv->pCtx = (SQLFunctionCtx *)calloc(pQuery->numOfOutput, sizeof(SQLFunctionCtx));

  if (pRuntimeEnv->resultInfo == NULL || pRuntimeEnv->pCtx == NULL) {
    goto _clean;
  }

  qDebug("QInfo:%p setup runtime env1", GET_QINFO_ADDR(pRuntimeEnv));

  pRuntimeEnv->offset[0] = 0;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SSqlFuncMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].base;

    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    SColIndex* pIndex = &pSqlFuncMsg->colInfo;

    int32_t index = pSqlFuncMsg->colInfo.colIndex;
    if (TSDB_COL_IS_TAG(pIndex->flag)) {
      if (pIndex->colId == TSDB_TBNAME_COLUMN_INDEX) {  // todo refactor
        SSchema s = tGetTableNameColumnSchema();

        pCtx->inputBytes = s.bytes;
        pCtx->inputType = s.type;
      } else {
        pCtx->inputBytes = pQuery->tagColList[index].bytes;
        pCtx->inputType = pQuery->tagColList[index].type;
      }
      
    } else {
      pCtx->inputBytes = pQuery->colList[index].bytes;
      pCtx->inputType = pQuery->colList[index].type;
    }
  
    assert(isValidDataType(pCtx->inputType));
    pCtx->ptsOutputBuf = NULL;

    pCtx->outputBytes = pQuery->pSelectExpr[i].bytes;
    pCtx->outputType = pQuery->pSelectExpr[i].type;

    pCtx->order = pQuery->order.order;
    pCtx->functionId = pSqlFuncMsg->functionId;

    pCtx->numOfParams = pSqlFuncMsg->numOfParams;
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
      int16_t type = pSqlFuncMsg->arg[j].argType;
      int16_t bytes = pSqlFuncMsg->arg[j].argBytes;
      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
        tVariantCreateFromBinary(&pCtx->param[j], pSqlFuncMsg->arg->argValue.pz, bytes, type);
      } else {
        tVariantCreateFromBinary(&pCtx->param[j], (char *)&pSqlFuncMsg->arg[j].argValue.i64, bytes, type);
      }
    }

    qDebug("QInfo:%p setup runtime env2", GET_QINFO_ADDR(pRuntimeEnv));

    // set the order information for top/bottom query
    int32_t functionId = pCtx->functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      int32_t f = pQuery->pSelectExpr[0].base.functionId;
      assert(f == TSDB_FUNC_TS || f == TSDB_FUNC_TS_DUMMY);

      pCtx->param[2].i64Key = order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i64Key = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i64Key = pQuery->order.orderColId;
    }

    if (i > 0) {
      pRuntimeEnv->offset[i] = pRuntimeEnv->offset[i - 1] + pRuntimeEnv->pCtx[i - 1].outputBytes;
    }
  }

  qDebug("QInfo:%p setup runtime env3", GET_QINFO_ADDR(pRuntimeEnv));

  char* buf = (char*) pRuntimeEnv->resultInfo + sizeof(SResultInfo) * pQuery->numOfOutput;

  // set the intermediate result output buffer
  setWindowResultInfo(pRuntimeEnv->resultInfo, pQuery, pRuntimeEnv->stableQuery, buf);

  qDebug("QInfo:%p setup runtime env4", GET_QINFO_ADDR(pRuntimeEnv));

  // if it is group by normal column, do not set output buffer, the output buffer is pResult
  if (!pRuntimeEnv->groupbyNormalCol && !pRuntimeEnv->stableQuery) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

  qDebug("QInfo:%p setup runtime env5", GET_QINFO_ADDR(pRuntimeEnv));

  setCtxTagColumnInfo(pRuntimeEnv, pRuntimeEnv->pCtx);

  qDebug("QInfo:%p init completed", GET_QINFO_ADDR(pRuntimeEnv));
  return TSDB_CODE_SUCCESS;

_clean:
  tfree(pRuntimeEnv->resultInfo);
  tfree(pRuntimeEnv->pCtx);

  return TSDB_CODE_QRY_OUT_OF_MEMORY;
}

static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  if (pRuntimeEnv->pQuery == NULL) {
    return;
  }

  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo* pQInfo = (SQInfo*) GET_QINFO_ADDR(pRuntimeEnv);

  qDebug("QInfo:%p teardown runtime env", pQInfo);
  cleanupTimeWindowInfo(&pRuntimeEnv->windowResInfo);

  if (pRuntimeEnv->pCtx != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

      for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
        tVariantDestroy(&pCtx->param[j]);
      }

      tVariantDestroy(&pCtx->tag);
      tfree(pCtx->tagInfo.pTagCtxList);
    }

    tfree(pRuntimeEnv->resultInfo);
    tfree(pRuntimeEnv->pCtx);
  }

  pRuntimeEnv->pFillInfo = taosDestoryFillInfo(pRuntimeEnv->pFillInfo);

  destroyResultBuf(pRuntimeEnv->pResultBuf, pQInfo);
  tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
  tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);

  pRuntimeEnv->pTSBuf = tsBufDestroy(pRuntimeEnv->pTSBuf);
}

#define IS_QUERY_KILLED(_q) ((_q)->code == TSDB_CODE_TSC_QUERY_CANCELLED)

static void setQueryKilled(SQInfo *pQInfo) { pQInfo->code = TSDB_CODE_TSC_QUERY_CANCELLED;}

static bool isFixedOutputQuery(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    return false;
  }

  // Note:top/bottom query is fixed output query
  if (pRuntimeEnv->topBotQuery || pRuntimeEnv->groupbyNormalCol) {
    return true;
  }

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SSqlFuncMsg *pExprMsg = &pQuery->pSelectExpr[i].base;

    // ignore the ts_comp function
    if (i == 0 && pExprMsg->functionId == TSDB_FUNC_PRJ && pExprMsg->numOfParams == 1 &&
        pExprMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      continue;
    }

    if (pExprMsg->functionId == TSDB_FUNC_TS || pExprMsg->functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if (!IS_MULTIOUTPUT(aAggs[pExprMsg->functionId].nStatus)) {
      return true;
    }
  }

  return false;
}

// todo refactor with isLastRowQuery
static bool isPointInterpoQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].base.functionId;
    if (functionID == TSDB_FUNC_INTERP) {
      return true;
    }
  }

  return false;
}

// TODO REFACTOR:MERGE WITH CLIENT-SIDE FUNCTION
static bool isSumAvgRateQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_SUM_RATE || functionId == TSDB_FUNC_SUM_IRATE || functionId == TSDB_FUNC_AVG_RATE ||
        functionId == TSDB_FUNC_AVG_IRATE) {
      return true;
    }
  }

  return false;
}

static bool isFirstLastRowQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].base.functionId;
    if (functionID == TSDB_FUNC_LAST_ROW) {
      return true;
    }
  }

  return false;
}

static bool needReverseScan(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if ((functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_FIRST_DST) && !QUERY_IS_ASC_QUERY(pQuery)) {
      return true;
    }

    if (functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_LAST_DST) {
      int32_t order = pQuery->pSelectExpr[i].base.arg->argValue.i64;
      return order != pQuery->order.order;
    }
  }

  return false;
}

static bool onlyQueryTags(SQuery* pQuery) {
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SExprInfo* pExprInfo = &pQuery->pSelectExpr[i];

    int32_t functionId = pExprInfo->base.functionId;
    if (functionId != TSDB_FUNC_TAGPRJ && functionId != TSDB_FUNC_TID_TAG &&
        (!(functionId == TSDB_FUNC_COUNT && pExprInfo->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX))) {
      return false;
    }
  }

  return true;
}

/////////////////////////////////////////////////////////////////////////////////////////////

void getAlignQueryTimeWindow(SQuery *pQuery, int64_t key, int64_t keyFirst, int64_t keyLast, STimeWindow *win) {
  assert(key >= keyFirst && key <= keyLast && pQuery->slidingTime <= pQuery->intervalTime);
  win->skey = taosGetIntervalStartTimestamp(key, pQuery->slidingTime, pQuery->intervalTime, pQuery->slidingTimeUnit, pQuery->precision);

  /*
   * if the realSkey > INT64_MAX - pQuery->intervalTime, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  if (keyFirst > (INT64_MAX - pQuery->intervalTime)) {
    assert(keyLast - keyFirst < pQuery->intervalTime);
    win->ekey = INT64_MAX;
    return;
  } else {
    win->ekey = win->skey + pQuery->intervalTime - 1;
  }
}

static void setScanLimitationByResultBuffer(SQuery *pQuery) {
  if (isTopBottomQuery(pQuery)) {
    pQuery->checkBuffer = 0;
  } else if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    pQuery->checkBuffer = 0;
  } else {
    bool hasMultioutput = false;
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SSqlFuncMsg *pExprMsg = &pQuery->pSelectExpr[i].base;
      if (pExprMsg->functionId == TSDB_FUNC_TS || pExprMsg->functionId == TSDB_FUNC_TS_DUMMY) {
        continue;
      }

      hasMultioutput = IS_MULTIOUTPUT(aAggs[pExprMsg->functionId].nStatus);
      if (!hasMultioutput) {
        break;
      }
    }

    pQuery->checkBuffer = hasMultioutput ? 1 : 0;
  }
}

/*
 * todo add more parameters to check soon..
 */
bool colIdCheck(SQuery *pQuery) {
  // load data column information is incorrect
  for (int32_t i = 0; i < pQuery->numOfCols - 1; ++i) {
    if (pQuery->colList[i].colId == pQuery->colList[i + 1].colId) {
      qError("QInfo:%p invalid data load column for query", GET_QINFO_ADDR(pQuery));
      return false;
    }
  }
  
  return true;
}

// todo ignore the avg/sum/min/max/count/stddev/top/bottom functions, of which
// the scan order is not matter
static bool onlyOneQueryType(SQuery *pQuery, int32_t functId, int32_t functIdDst) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;

    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG ||
        functionId == TSDB_FUNC_TAG_DUMMY) {
      continue;
    }

    if (functionId != functId && functionId != functIdDst) {
      return false;
    }
  }

  return true;
}

static bool onlyFirstQuery(SQuery *pQuery) { return onlyOneQueryType(pQuery, TSDB_FUNC_FIRST, TSDB_FUNC_FIRST_DST); }

static bool onlyLastQuery(SQuery *pQuery) { return onlyOneQueryType(pQuery, TSDB_FUNC_LAST, TSDB_FUNC_LAST_DST); }

// todo refactor, add iterator
static void doExchangeTimeWindow(SQInfo* pQInfo) {
  size_t t = GET_NUM_OF_TABLEGROUP(pQInfo);
  for(int32_t i = 0; i < t; ++i) {
    SArray* p1 = GET_TABLEGROUP(pQInfo, i);

    size_t len = taosArrayGetSize(p1);
    for(int32_t j = 0; j < len; ++j) {
      STableQueryInfo* pTableQueryInfo = (STableQueryInfo*) taosArrayGetP(p1, j);
      SWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, TSKEY);
    }
  }
}

static void changeExecuteScanOrder(SQInfo *pQInfo, bool stableQuery) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;

  // in case of point-interpolation query, use asc order scan
  char msg[] = "QInfo:%p scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%" PRId64
               "-%" PRId64 ", new qrange:%" PRId64 "-%" PRId64;

  // todo handle the case the the order irrelevant query type mixed up with order critical query type
  // descending order query for last_row query
  if (isFirstLastRowQuery(pQuery)) {
    qDebug("QInfo:%p scan order changed for last_row query, old:%d, new:%d", GET_QINFO_ADDR(pQuery),
           pQuery->order.order, TSDB_ORDER_DESC);

    pQuery->order.order = TSDB_ORDER_DESC;

    int64_t skey = MIN(pQuery->window.skey, pQuery->window.ekey);
    int64_t ekey = MAX(pQuery->window.skey, pQuery->window.ekey);

    pQuery->window.skey = ekey;
    pQuery->window.ekey = skey;

    return;
  }

  if (isPointInterpoQuery(pQuery) && pQuery->intervalTime == 0) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      qDebug(msg, GET_QINFO_ADDR(pQuery), "interp", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
             pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    pQuery->order.order = TSDB_ORDER_ASC;
    return;
  }

  if (pQuery->intervalTime == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        qDebug(msg, GET_QINFO_ADDR(pQuery), "only-first", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        doExchangeTimeWindow(pQInfo);
      }

      pQuery->order.order = TSDB_ORDER_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        qDebug(msg, GET_QINFO_ADDR(pQuery), "only-last", pQuery->order.order, TSDB_ORDER_DESC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        doExchangeTimeWindow(pQInfo);
      }

      pQuery->order.order = TSDB_ORDER_DESC;
    }

  } else {  // interval query
    if (stableQuery) {
      if (onlyFirstQuery(pQuery)) {
        if (!QUERY_IS_ASC_QUERY(pQuery)) {
          qDebug(msg, GET_QINFO_ADDR(pQuery), "only-first stable", pQuery->order.order, TSDB_ORDER_ASC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        }

        pQuery->order.order = TSDB_ORDER_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          qDebug(msg, GET_QINFO_ADDR(pQuery), "only-last stable", pQuery->order.order, TSDB_ORDER_DESC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        }

        pQuery->order.order = TSDB_ORDER_DESC;
      }
    }
  }
}

static int32_t getInitialPageNum(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  int32_t INITIAL_RESULT_ROWS_VALUE = 16;

  int32_t num = 0;

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    num = 128;
  } else if (QUERY_IS_INTERVAL_QUERY(pQuery)) {  // time window query, allocate one page for each table
    size_t s = pQInfo->tableqinfoGroupInfo.numOfTables;
    num = MAX(s, INITIAL_RESULT_ROWS_VALUE);
  } else {    // for super table query, one page for each subset
    num = 1;  // pQInfo->pSidSet->numOfSubSet;
  }

  assert(num > 0);
  return num;
}

static void getIntermediateBufInfo(SQueryRuntimeEnv* pRuntimeEnv, int32_t* ps, int32_t* rowsize) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  *rowsize = pQuery->rowSize * GET_ROW_PARAM_FOR_MULTIOUTPUT(pQuery, pRuntimeEnv->topBotQuery, pRuntimeEnv->stableQuery);
  int32_t overhead = sizeof(tFilePage);

  // one page contains at least two rows
  *ps = DEFAULT_INTERN_BUF_PAGE_SIZE;
  while(((*rowsize) * 2) > (*ps) - overhead) {
    *ps = (*ps << 1u);
  }

  pRuntimeEnv->numOfRowsPerPage = ((*ps) - sizeof(tFilePage)) / (*rowsize);

}

#define IS_PREFILTER_TYPE(_t) ((_t) != TSDB_DATA_TYPE_BINARY && (_t) != TSDB_DATA_TYPE_NCHAR)

static bool needToLoadDataBlock(SQueryRuntimeEnv* pRuntimeEnv, SDataStatis *pDataStatis, SQLFunctionCtx *pCtx,
    int32_t numOfRows) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (pDataStatis == NULL || (pQuery->numOfFilterCols == 0 && (!pRuntimeEnv->topBotQuery))) {
    return true;
  }

  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];

    int32_t index = -1;
    for(int32_t i = 0; i < pQuery->numOfCols; ++i) {
      if (pDataStatis[i].colId == pFilterInfo->info.colId) {
        index = i;
        break;
      }
    }

    // no statistics data
    if (index == -1) {
      return true;
    }

    // not support pre-filter operation on binary/nchar data type
    if (!IS_PREFILTER_TYPE(pFilterInfo->info.type)) {
      return true;
    }

    // all points in current column are NULL, no need to check its boundary value
    if (pDataStatis[index].numOfNull == numOfRows) {
      continue;
    }

    SDataStatis* pDataBlockst = &pDataStatis[index];

    if (pFilterInfo->info.type == TSDB_DATA_TYPE_FLOAT) {
      float minval = *(double *)(&pDataBlockst->min);
      float maxval = *(double *)(&pDataBlockst->max);

      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&minval, (char *)&maxval)) {
          return true;
        }
      }
    } else {
      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&pDataBlockst->min, (char *)&pDataBlockst->max)) {
          return true;
        }
      }
    }
  }

  if (pRuntimeEnv->topBotQuery) {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
      if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        return topbot_datablock_filter(&pCtx[i], functionId, (char *)&pDataStatis[i].min, (char *)&pDataStatis[i].max);
      }
    }
  }

  return false;
}

#define PT_IN_WINDOW(_p, _w)  ((_p) > (_w).skey && (_p) < (_w).ekey)

static bool overlapWithTimeWindow(SQuery* pQuery, SDataBlockInfo* pBlockInfo) {
  STimeWindow w = {0};

  TSKEY sk = MIN(pQuery->window.skey, pQuery->window.ekey);
  TSKEY ek = MAX(pQuery->window.skey, pQuery->window.ekey);


  if (QUERY_IS_ASC_QUERY(pQuery)) {
    getAlignQueryTimeWindow(pQuery, pBlockInfo->window.skey, sk, ek, &w);

    if (PT_IN_WINDOW(w.ekey, pBlockInfo->window)) {
      return true;
    }

    while(1) {
      GET_NEXT_TIMEWINDOW(pQuery, &w);
      if (w.skey > pBlockInfo->window.skey) {
        break;
      }

      if (PT_IN_WINDOW(w.skey, pBlockInfo->window) || PT_IN_WINDOW(w.ekey, pBlockInfo->window)) {
        return true;
      }
    }
  } else {
    getAlignQueryTimeWindow(pQuery, pBlockInfo->window.ekey, sk, ek, &w);
    if (PT_IN_WINDOW(w.skey, pBlockInfo->window)) {
      return true;
    }

    while(1) {
      GET_NEXT_TIMEWINDOW(pQuery, &w);
      if (w.ekey < pBlockInfo->window.skey) {
        break;
      }

      if (PT_IN_WINDOW(w.skey, pBlockInfo->window) || PT_IN_WINDOW(w.ekey, pBlockInfo->window)) {
        return true;
      }
    }
  }

  return false;
}

int32_t loadDataBlockOnDemand(SQueryRuntimeEnv *pRuntimeEnv, void* pQueryHandle, SDataBlockInfo* pBlockInfo, SDataStatis **pStatis, SArray** pDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  uint32_t status = 0;
  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf > 0) {
    status = BLK_DATA_ALL_NEEDED;
  } else { // check if this data block is required to load

    // Calculate all time windows that are overlapping or contain current data block.
    // If current data block is contained by all possible time window, do not load current data block.
    if (QUERY_IS_INTERVAL_QUERY(pQuery) && overlapWithTimeWindow(pQuery, pBlockInfo)) {
      status = BLK_DATA_ALL_NEEDED;
    }

    if (status != BLK_DATA_ALL_NEEDED) {
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        SSqlFuncMsg* pSqlFunc = &pQuery->pSelectExpr[i].base;

        int32_t functionId = pSqlFunc->functionId;
        int32_t colId = pSqlFunc->colInfo.colId;

        status |= aAggs[functionId].dataReqFunc(&pRuntimeEnv->pCtx[i], pBlockInfo->window.skey, pBlockInfo->window.ekey, colId);
        if ((status & BLK_DATA_ALL_NEEDED) == BLK_DATA_ALL_NEEDED) {
          break;
        }
      }
    }
  }

  if (status == BLK_DATA_NO_NEEDED) {
    qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", GET_QINFO_ADDR(pRuntimeEnv),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pRuntimeEnv->summary.discardBlocks += 1;
  } else if (status == BLK_DATA_STATIS_NEEDED) {
    if (tsdbRetrieveDataBlockStatisInfo(pQueryHandle, pStatis) != TSDB_CODE_SUCCESS) {
      //        return DISK_DATA_LOAD_FAILED;
    }
  
    pRuntimeEnv->summary.loadBlockStatis += 1;
  
    if (*pStatis == NULL) { // data block statistics does not exist, load data block
      *pDataBlock = tsdbRetrieveDataBlock(pQueryHandle, NULL);
      pRuntimeEnv->summary.totalCheckedRows += pBlockInfo->rows;
    }
  } else {
    assert(status == BLK_DATA_ALL_NEEDED);
  
    // load the data block statistics to perform further filter
    pRuntimeEnv->summary.loadBlockStatis += 1;
    if (tsdbRetrieveDataBlockStatisInfo(pQueryHandle, pStatis) != TSDB_CODE_SUCCESS) {
    }
    
    if (!needToLoadDataBlock(pRuntimeEnv, *pStatis, pRuntimeEnv->pCtx, pBlockInfo->rows)) {
#if defined(_DEBUG_VIEW)
      qDebug("QInfo:%p block discarded by per-filter", GET_QINFO_ADDR(pRuntimeEnv));
#endif
      // current block has been discard due to filter applied
      pRuntimeEnv->summary.discardBlocks += 1;
      qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", GET_QINFO_ADDR(pRuntimeEnv),
          pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      return BLK_DATA_DISCARD;
    }
  
    pRuntimeEnv->summary.totalCheckedRows += pBlockInfo->rows;
    pRuntimeEnv->summary.loadBlocks += 1;
    *pDataBlock = tsdbRetrieveDataBlock(pQueryHandle, NULL);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t binarySearchForKey(char *pValue, int num, TSKEY key, int order) {
  int32_t midPos = -1;
  int32_t numOfRows;

  if (num <= 0) {
    return -1;
  }

  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);

  TSKEY * keyList = (TSKEY *)pValue;
  int32_t firstPos = 0;
  int32_t lastPos = num - 1;

  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
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
      midPos = (numOfRows >> 1) + firstPos;

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

static void ensureOutputBufferSimple(SQueryRuntimeEnv* pRuntimeEnv, int32_t capacity) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (capacity < pQuery->rec.capacity) {
    return;
  }

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t bytes = pQuery->pSelectExpr[i].bytes;
    assert(bytes > 0 && capacity > 0);

    char *tmp = realloc(pQuery->sdata[i], bytes * capacity + sizeof(tFilePage));
    if (tmp == NULL) {  // todo handle the oom
      assert(0);
    } else {
      pQuery->sdata[i] = (tFilePage *)tmp;
    }

    // set the pCtx output buffer position
    pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data;
  }

  qDebug("QInfo:%p realloc output buffer to inc output buffer from: %" PRId64 " rows to:%d rows", GET_QINFO_ADDR(pRuntimeEnv),
         pQuery->rec.capacity, capacity);

  pQuery->rec.capacity = capacity;
}

static void ensureOutputBuffer(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pBlockInfo) {
  // in case of prj/diff query, ensure the output buffer is sufficient to accommodate the results of current block
  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (!QUERY_IS_INTERVAL_QUERY(pQuery) && !pRuntimeEnv->groupbyNormalCol && !isFixedOutputQuery(pRuntimeEnv)) {
    SResultRec *pRec = &pQuery->rec;
    
    if (pQuery->rec.capacity - pQuery->rec.rows < pBlockInfo->rows) {
      int32_t remain = pRec->capacity - pRec->rows;
      int32_t newSize = pRec->capacity + (pBlockInfo->rows - remain);
      
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        int32_t bytes = pQuery->pSelectExpr[i].bytes;
        assert(bytes > 0 && newSize > 0);

        char *tmp = realloc(pQuery->sdata[i], bytes * newSize + sizeof(tFilePage));
        if (tmp == NULL) {  // todo handle the oom
          assert(0);
        } else {
          memset(tmp + sizeof(tFilePage) + bytes * pRec->rows, 0, (newSize - pRec->rows) * bytes);
          pQuery->sdata[i] = (tFilePage *)tmp;
        }
        
        // set the pCtx output buffer position
        pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data + pRec->rows * bytes;
        
        int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
        if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
          pRuntimeEnv->pCtx[i].ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
        }
      }
      
      qDebug("QInfo:%p realloc output buffer, new size: %d rows, old:%" PRId64 ", remain:%" PRId64, GET_QINFO_ADDR(pRuntimeEnv),
             newSize, pRec->capacity, newSize - pRec->rows);
      
      pRec->capacity = newSize;
    }
  }
}

static void doSetInitialTimewindow(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pBlockInfo) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (QUERY_IS_INTERVAL_QUERY(pQuery) && pRuntimeEnv->windowResInfo.prevSKey == TSKEY_INITIAL_VAL) {
    STimeWindow w = TSWINDOW_INITIALIZER;
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      getAlignQueryTimeWindow(pQuery, pBlockInfo->window.skey, pBlockInfo->window.skey, pQuery->window.ekey, &w);
      pWindowResInfo->startTime = w.skey;
      pWindowResInfo->prevSKey = w.skey;
    } else {
      // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
      getAlignQueryTimeWindow(pQuery, pBlockInfo->window.ekey, pQuery->window.ekey, pBlockInfo->window.ekey, &w);

      pWindowResInfo->startTime = pQuery->window.skey;
      pWindowResInfo->prevSKey = w.skey;
    }
  }
}

static int64_t doScanAllDataBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;
  SQueryCostInfo*  summary  = &pRuntimeEnv->summary;

  qDebug("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", lastkey:%" PRId64 ", order:%d",
         GET_QINFO_ADDR(pRuntimeEnv), pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, pTableQueryInfo->lastKey,
         pQuery->order.order);

  TsdbQueryHandleT pQueryHandle = IS_MASTER_SCAN(pRuntimeEnv)? pRuntimeEnv->pQueryHandle : pRuntimeEnv->pSecQueryHandle;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
  while (true) {
    if (!tsdbNextDataBlock(pQueryHandle)) {
      if (terrno != TSDB_CODE_SUCCESS) {
        longjmp(pRuntimeEnv->env, terrno);
      }
      break;
    }
    summary->totalBlocks += 1;

    if (IS_QUERY_KILLED(GET_QINFO_ADDR(pRuntimeEnv))) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
    doSetInitialTimewindow(pRuntimeEnv, &blockInfo);

    // in case of prj/diff query, ensure the output buffer is sufficient to accommodate the results of current block
    ensureOutputBuffer(pRuntimeEnv, &blockInfo);

    SDataStatis *pStatis = NULL;
    SArray *pDataBlock   = NULL;
    if (loadDataBlockOnDemand(pRuntimeEnv, pQueryHandle, &blockInfo, &pStatis, &pDataBlock) == BLK_DATA_DISCARD) {
      pQuery->current->lastKey = QUERY_IS_ASC_QUERY(pQuery)? blockInfo.window.ekey + step:blockInfo.window.skey + step;
      continue;
    }

    // query start position can not move into tableApplyFunctionsOnBlock due to limit/offset condition
    pQuery->pos = QUERY_IS_ASC_QUERY(pQuery)? 0 : blockInfo.rows - 1;
    int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, binarySearchForKey, pDataBlock);

    summary->totalRows += blockInfo.rows;
    qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%"PRId64, GET_QINFO_ADDR(pRuntimeEnv),
           blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes, pQuery->current->lastKey);

    // while the output buffer is full or limit/offset is applied, query may be paused here
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL | QUERY_COMPLETED)) {
      break;
    }
  }

  // if the result buffer is not full, set the query complete
  if (!Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  if (QUERY_IS_INTERVAL_QUERY(pQuery) && IS_MASTER_SCAN(pRuntimeEnv)) {
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      closeAllTimeWindow(&pRuntimeEnv->windowResInfo);
      pRuntimeEnv->windowResInfo.curIndex = pRuntimeEnv->windowResInfo.size - 1;  // point to the last time window
    } else {
      assert(Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL));
    }
  }

  return 0;
}

/*
 * set tag value in SQLFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(void *tsdb, void* pTable, int32_t tagColId, tVariant *tag, int16_t type, int16_t bytes) {
  tVariantDestroy(tag);

  if (tagColId == TSDB_TBNAME_COLUMN_INDEX) {
    char* val = tsdbGetTableName(pTable);
    assert(val != NULL);
    
    tVariantCreateFromBinary(tag, varDataVal(val), varDataLen(val), TSDB_DATA_TYPE_BINARY);
  } else {
    char* val = tsdbGetTableTagVal(pTable, tagColId, type, bytes);
    if (val == NULL) {
      tag->nType = TSDB_DATA_TYPE_NULL;
      return;
    }
    
    if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
      if (isNull(val, type)) {
        tag->nType = TSDB_DATA_TYPE_NULL;
        return;
      }

      tVariantCreateFromBinary(tag, varDataVal(val), varDataLen(val), type);
    } else {
      if (isNull(val, type)) {
        tag->nType = TSDB_DATA_TYPE_NULL;
        return;
      }

      tVariantCreateFromBinary(tag, val, bytes, type);
    }
  }
}

static SColumnInfo* doGetTagColumnInfoById(SColumnInfo* pTagColList, int32_t numOfTags, int16_t colId) {
  assert(pTagColList != NULL && numOfTags > 0);

  for(int32_t i = 0; i < numOfTags; ++i) {
    if (pTagColList[i].colId == colId) {
      return &pTagColList[i];
    }
  }

  return NULL;
}

void setTagVal(SQueryRuntimeEnv *pRuntimeEnv, void *pTable, void *tsdb) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);

  SExprInfo *pExprInfo = &pQuery->pSelectExpr[0];
  if (pQuery->numOfOutput == 1 && pExprInfo->base.functionId == TSDB_FUNC_TS_COMP) {
    assert(pExprInfo->base.numOfParams == 1);

    int16_t tagColId = pExprInfo->base.arg->argValue.i64;
    SColumnInfo* pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagColId);

    doSetTagValueInParam(tsdb, pTable, tagColId, &pRuntimeEnv->pCtx[0].tag, pColInfo->type, pColInfo->bytes);
  } else {
    // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutput; ++idx) {
      SExprInfo* pLocalExprInfo = &pQuery->pSelectExpr[idx];
  
      // ts_comp column required the tag value for join filter
      if (!TSDB_COL_IS_TAG(pLocalExprInfo->base.colInfo.flag)) {
        continue;
      }

      // todo use tag column index to optimize performance
      doSetTagValueInParam(tsdb, pTable, pLocalExprInfo->base.colInfo.colId, &pRuntimeEnv->pCtx[idx].tag,
                           pLocalExprInfo->type, pLocalExprInfo->bytes);
    }

    // set the join tag for first column
    SSqlFuncMsg *pFuncMsg = &pExprInfo->base;
    if ((pFuncMsg->functionId == TSDB_FUNC_TS || pFuncMsg->functionId == TSDB_FUNC_PRJ) && pRuntimeEnv->pTSBuf != NULL &&
        pFuncMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      assert(pFuncMsg->numOfParams == 1);

      int16_t tagColId = pExprInfo->base.arg->argValue.i64;
      SColumnInfo* pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagColId);

      doSetTagValueInParam(tsdb, pTable, tagColId, &pRuntimeEnv->pCtx[0].tag, pColInfo->type, pColInfo->bytes);
      qDebug("QInfo:%p set tag value for join comparison, colId:%" PRId64 ", val:%"PRId64, pQInfo, pExprInfo->base.arg->argValue.i64,
          pRuntimeEnv->pCtx[0].tag.i64Key)
    }
  }
}

static void doMerge(SQueryRuntimeEnv *pRuntimeEnv, int64_t timestamp, SWindowResult *pWindowRes, bool mergeFlag) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (!mergeFlag) {
      pCtx[i].aOutputBuf = pCtx[i].aOutputBuf + pCtx[i].outputBytes;
      pCtx[i].currentStage = FIRST_STAGE_MERGE;

      RESET_RESULT_INFO(pCtx[i].resultInfo);
      aAggs[functionId].init(&pCtx[i]);
    }

    pCtx[i].hasNull = true;
    pCtx[i].nStartQueryTimestamp = timestamp;
    pCtx[i].aInputElemBuf = getPosInResultPage(pRuntimeEnv, i, pWindowRes);

    // in case of tag column, the tag information should be extracted from input buffer
    if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TAG) {
      tVariantDestroy(&pCtx[i].tag);
  
      int32_t type = pCtx[i].outputType;
      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
        tVariantCreateFromBinary(&pCtx[i].tag, varDataVal(pCtx[i].aInputElemBuf), varDataLen(pCtx[i].aInputElemBuf), type);
      } else {
        tVariantCreateFromBinary(&pCtx[i].tag, pCtx[i].aInputElemBuf, pCtx[i].inputBytes, pCtx[i].inputType);
      }
      
    }
  }

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TAG_DUMMY) {
      continue;
    }

    aAggs[functionId].distMergeFunc(&pCtx[i]);
  }
}

static UNUSED_FUNC void printBinaryData(int32_t functionId, char *data, int32_t srcDataType) {
  if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST) {
    switch (srcDataType) {
      case TSDB_DATA_TYPE_BINARY:
        printf("%" PRId64 ",%s\t", *(TSKEY *)data, (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%" PRId64 ",%d\t", *(TSKEY *)data, *(int8_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%" PRId64 ",%d\t", *(TSKEY *)data, *(int16_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%" PRId64 ",%" PRId64 "\t", *(TSKEY *)data, *(TSKEY *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%" PRId64 ",%d\t", *(TSKEY *)data, *(int32_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%" PRId64 ",%f\t", *(TSKEY *)data, *(float *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%" PRId64 ",%lf\t", *(TSKEY *)data, *(double *)(data + TSDB_KEYSIZE + 1));
        break;
    }
  } else if (functionId == TSDB_FUNC_AVG) {
    printf("%lf,%d\t", *(double *)data, *(int32_t *)(data + sizeof(double)));
  } else if (functionId == TSDB_FUNC_SPREAD) {
    printf("%lf,%lf\t", *(double *)data, *(double *)(data + sizeof(double)));
  } else if (functionId == TSDB_FUNC_TWA) {
    data += 1;
    printf("%lf,%" PRId64 ",%" PRId64 ",%" PRId64 "\t", *(double *)data, *(int64_t *)(data + 8),
           *(int64_t *)(data + 16), *(int64_t *)(data + 24));
  } else if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX) {
    switch (srcDataType) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%d\t", *(int8_t *)data);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%d\t", *(int16_t *)data);
        break;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%" PRId64 "\t", *(int64_t *)data);
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%d\t", *(int *)data);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%f\t", *(float *)data);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%f\t", *(float *)data);
        break;
    }
  } else if (functionId == TSDB_FUNC_SUM) {
    if (srcDataType == TSDB_DATA_TYPE_FLOAT || srcDataType == TSDB_DATA_TYPE_DOUBLE) {
      printf("%lf\t", *(float *)data);
    } else {
      printf("%" PRId64 "\t", *(int64_t *)data);
    }
  } else {
    printf("%s\t", data);
  }
}

void UNUSED_FUNC displayInterResult(tFilePage **pdata, SQueryRuntimeEnv* pRuntimeEnv, int32_t numOfRows) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t numOfCols = pQuery->numOfOutput;
  printf("super table query intermediate result, total:%d\n", numOfRows);

  for (int32_t j = 0; j < numOfRows; ++j) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      
      switch (pQuery->pSelectExpr[i].type) {
        case TSDB_DATA_TYPE_BINARY: {
          int32_t type = pQuery->pSelectExpr[i].type;
          printBinaryData(pQuery->pSelectExpr[i].base.functionId, pdata[i]->data + pQuery->pSelectExpr[i].bytes * j,
                          type);
          break;
        }
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          printf("%" PRId64 "\t", *(int64_t *)(pdata[i]->data + pQuery->pSelectExpr[i].bytes * j));
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)(pdata[i]->data + pQuery->pSelectExpr[i].bytes * j));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)(pdata[i]->data + pQuery->pSelectExpr[i].bytes * j));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)(pdata[i]->data + pQuery->pSelectExpr[i].bytes * j));
          break;
      }
    }
    printf("\n");
  }
}

typedef struct SCompSupporter {
  STableQueryInfo **pTableQueryInfo;
  int32_t *         position;
  SQInfo *          pQInfo;
} SCompSupporter;

int32_t tableResultComparFn(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(int32_t *)pLeft;
  int32_t right = *(int32_t *)pRight;

  SCompSupporter *  supporter = (SCompSupporter *)param;
  SQueryRuntimeEnv *pRuntimeEnv = &supporter->pQInfo->runtimeEnv;

  int32_t leftPos = supporter->position[left];
  int32_t rightPos = supporter->position[right];

  /* left source is exhausted */
  if (leftPos == -1) {
    return 1;
  }

  /* right source is exhausted*/
  if (rightPos == -1) {
    return -1;
  }

  SWindowResInfo *pWindowResInfo1 = &supporter->pTableQueryInfo[left]->windowResInfo;
  SWindowResult * pWindowRes1 = getWindowResult(pWindowResInfo1, leftPos);

  char *b1 = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes1);
  TSKEY leftTimestamp = GET_INT64_VAL(b1);

  SWindowResInfo *pWindowResInfo2 = &supporter->pTableQueryInfo[right]->windowResInfo;
  SWindowResult * pWindowRes2 = getWindowResult(pWindowResInfo2, rightPos);

  char *b2 = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes2);
  TSKEY rightTimestamp = GET_INT64_VAL(b2);

  if (leftTimestamp == rightTimestamp) {
    return 0;
  }

  return leftTimestamp > rightTimestamp ? 1 : -1;
}

int32_t mergeIntoGroupResult(SQInfo *pQInfo) {
  int64_t st = taosGetTimestampMs();
  int32_t ret = TSDB_CODE_SUCCESS;

  int32_t numOfGroups = GET_NUM_OF_TABLEGROUP(pQInfo);

  while (pQInfo->groupIndex < numOfGroups) {
    SArray *group = GET_TABLEGROUP(pQInfo, pQInfo->groupIndex);
    ret = mergeIntoGroupResultImpl(pQInfo, group);
    if (ret < 0) {  // not enough disk space to save the data into disk
      return -1;
    }

    pQInfo->groupIndex += 1;

    // this group generates at least one result, return results
    if (ret > 0) {
      break;
    }

    assert(pQInfo->numOfGroupResultPages == 0);
    qDebug("QInfo:%p no result in group %d, continue", pQInfo, pQInfo->groupIndex - 1);
  }

  qDebug("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%" PRId64 "ms", pQInfo,
         pQInfo->groupIndex - 1, numOfGroups, taosGetTimestampMs() - st);

  return TSDB_CODE_SUCCESS;
}

void copyResToQueryResultBuf(SQInfo *pQInfo, SQuery *pQuery) {
  if (pQInfo->offset == pQInfo->numOfGroupResultPages) {
    pQInfo->numOfGroupResultPages = 0;

    // current results of group has been sent to client, try next group
    if (mergeIntoGroupResult(pQInfo) != TSDB_CODE_SUCCESS) {
      return;  // failed to save data in the disk
    }

    // check if all results has been sent to client
    int32_t numOfGroup = GET_NUM_OF_TABLEGROUP(pQInfo);
    if (pQInfo->numOfGroupResultPages == 0 && pQInfo->groupIndex == numOfGroup) {
      pQInfo->tableIndex = pQInfo->tableqinfoGroupInfo.numOfTables;  // set query completed
      return;
    }
  }

  SQueryRuntimeEnv *   pRuntimeEnv = &pQInfo->runtimeEnv;
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  int32_t id = getGroupResultId(pQInfo->groupIndex - 1);
  SIDList list = getDataBufPagesIdList(pResultBuf, pQInfo->offset + id);

  int32_t total = 0;
  int32_t size = taosArrayGetSize(list);
  for (int32_t i = 0; i < size; ++i) {
    int32_t* pgId = taosArrayGet(list, i);
    tFilePage *pData = getResBufPage(pResultBuf, *pgId);
    total += pData->num;
  }

  int32_t rows = total;

  int32_t offset = 0;
  for (int32_t j = 0; j < size; ++j) {
    int32_t* pgId = taosArrayGet(list, j);
    tFilePage *pData = getResBufPage(pResultBuf, *pgId);

    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      char *  pDest = pQuery->sdata[i]->data;

      memcpy(pDest + offset * bytes, pData->data + pRuntimeEnv->offset[i] * pData->num,
             bytes * pData->num);
    }

    offset += pData->num;
  }

  assert(pQuery->rec.rows == 0);

  pQuery->rec.rows += rows;
  pQInfo->offset += 1;
}

int64_t getNumOfResultWindowRes(SQuery *pQuery, SWindowResult *pWindowRes) {
  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].base.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    SResultInfo *pResultInfo = &pWindowRes->resultInfo[j];
    assert(pResultInfo != NULL);

    if (pResultInfo->numOfRes > 0) {
      return pResultInfo->numOfRes;
    }
  }

  return 0;
}

int32_t mergeIntoGroupResultImpl(SQInfo *pQInfo, SArray *pGroup) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  size_t size = taosArrayGetSize(pGroup);
  tFilePage **buffer = pQuery->sdata;

  int32_t*   posList = calloc(size, sizeof(int32_t));
  STableQueryInfo **pTableList = malloc(POINTER_BYTES * size);

  if (pTableList == NULL || posList == NULL) {
    tfree(posList);
    tfree(pTableList);

    qError("QInfo:%p failed alloc memory", pQInfo);
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  // todo opt for the case of one table per group
  int32_t numOfTables = 0;
  for (int32_t i = 0; i < size; ++i) {
    STableQueryInfo *item = taosArrayGetP(pGroup, i);

    SIDList list = getDataBufPagesIdList(pRuntimeEnv->pResultBuf, TSDB_TABLEID(item->pTable)->tid);
    if (taosArrayGetSize(list) > 0 && item->windowResInfo.size > 0) {
      pTableList[numOfTables] = item;
      numOfTables += 1;
    }
  }

  if (numOfTables == 0) {
    tfree(posList);
    tfree(pTableList);

    assert(pQInfo->numOfGroupResultPages == 0);
    return 0;
  } else if (numOfTables == 1) { // no need to merge results since only one table in each group

  }

  SCompSupporter cs = {pTableList, posList, pQInfo};

  SLoserTreeInfo *pTree = NULL;
  tLoserTreeCreate(&pTree, numOfTables, &cs, tableResultComparFn);

  SResultInfo *pResultInfo = calloc(pQuery->numOfOutput, sizeof(SResultInfo));
  if (pResultInfo == NULL) {
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  char* buf = calloc(1, pRuntimeEnv->interBufSize);
  setWindowResultInfo(pResultInfo, pQuery, pRuntimeEnv->stableQuery, buf);
  resetMergeResultBuf(pQuery, pRuntimeEnv->pCtx, pResultInfo);

  int64_t lastTimestamp = -1;
  int64_t startt = taosGetTimestampMs();

  while (1) {
    int32_t pos = pTree->pNode[0].index;

    SWindowResInfo *pWindowResInfo = &pTableList[pos]->windowResInfo;
    SWindowResult * pWindowRes = getWindowResult(pWindowResInfo, cs.position[pos]);

    char *b = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes);
    TSKEY ts = GET_INT64_VAL(b);

    assert(ts == pWindowRes->window.skey);
    int64_t num = getNumOfResultWindowRes(pQuery, pWindowRes);
    if (num <= 0) {
      cs.position[pos] += 1;

      if (cs.position[pos] >= pWindowResInfo->size) {
        cs.position[pos] = -1;

        // all input sources are exhausted
        if (--numOfTables == 0) {
          break;
        }
      }
    } else {
      if (ts == lastTimestamp) {  // merge with the last one
        doMerge(pRuntimeEnv, ts, pWindowRes, true);
      } else {  // copy data to disk buffer
        if (buffer[0]->num == pQuery->rec.capacity) {
          if (flushFromResultBuf(pQInfo) != TSDB_CODE_SUCCESS) {
            return -1;
          }

          resetMergeResultBuf(pQuery, pRuntimeEnv->pCtx, pResultInfo);
        }

        doMerge(pRuntimeEnv, ts, pWindowRes, false);
        buffer[0]->num += 1;
      }

      lastTimestamp = ts;

      cs.position[pos] += 1;
      if (cs.position[pos] >= pWindowResInfo->size) {
        cs.position[pos] = -1;

        // all input sources are exhausted
        if (--numOfTables == 0) {
          break;
        }
      }
    }

    tLoserTreeAdjust(pTree, pos + pTree->numOfEntries);
  }

  if (buffer[0]->num != 0) {  // there are data in buffer
    if (flushFromResultBuf(pQInfo) != TSDB_CODE_SUCCESS) {
      qError("QInfo:%p failed to flush data into temp file, abort query", pQInfo);

      tfree(pTree);
      tfree(pTableList);
      tfree(posList);
      tfree(pResultInfo);

      return -1;
    }
  }

  int64_t endt = taosGetTimestampMs();

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pRuntimeEnv, pQuery->sdata[0]->num);
#endif

  qDebug("QInfo:%p result merge completed for group:%d, elapsed time:%" PRId64 " ms", pQInfo, pQInfo->groupIndex, endt - startt);

  tfree(pTableList);
  tfree(posList);
  tfree(pTree);

  pQInfo->offset = 0;

  tfree(pResultInfo);
  tfree(buf);
  return pQInfo->numOfGroupResultPages;
}

int32_t flushFromResultBuf(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  // the base value for group result, since the maximum number of table for each vnode will not exceed 100,000.
  int32_t pageId = -1;
  int32_t capacity = pResultBuf->numOfRowsPerPage;

  int32_t remain = pQuery->sdata[0]->num;
  int32_t offset = 0;

  while (remain > 0) {
    int32_t r = remain;
    if (r > capacity) {
      r = capacity;
    }

    int32_t    id = getGroupResultId(pQInfo->groupIndex) + pQInfo->numOfGroupResultPages;
    tFilePage *buf = getNewDataBuf(pResultBuf, id, &pageId);

    // pagewise copy to dest buffer
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      buf->num = r;

      memcpy(buf->data + pRuntimeEnv->offset[i] * buf->num, ((char *)pQuery->sdata[i]->data) + offset * bytes,
             buf->num * bytes);
    }

    offset += r;
    remain -= r;
  }

  pQInfo->numOfGroupResultPages += 1;
  return TSDB_CODE_SUCCESS;
}

void resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx, SResultInfo *pResultInfo) {
  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    pCtx[k].aOutputBuf = pQuery->sdata[k]->data - pCtx[k].outputBytes;
    pCtx[k].size = 1;
    pCtx[k].startOffset = 0;
    pCtx[k].resultInfo = &pResultInfo[k];

    pQuery->sdata[k]->num = 0;
  }
}

static void updateTableQueryInfoForReverseScan(SQuery *pQuery, STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }
  
  // order has change already!
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  
  // TODO validate the assertion
//  if (!QUERY_IS_ASC_QUERY(pQuery)) {
//    assert(pTableQueryInfo->win.ekey >= pTableQueryInfo->lastKey + step);
//  } else {
//    assert(pTableQueryInfo->win.ekey <= pTableQueryInfo->lastKey + step);
//  }
  
  pTableQueryInfo->win.ekey = pTableQueryInfo->lastKey + step;
  
  SWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, TSKEY);
  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;
  
  SWITCH_ORDER(pTableQueryInfo->cur.order);
  pTableQueryInfo->cur.vgroupIndex = -1;

  // set the index at the end of time window
  pTableQueryInfo->windowResInfo.curIndex = pTableQueryInfo->windowResInfo.size - 1;
}

static void disableFuncInReverseScanImpl(SQInfo* pQInfo, SWindowResInfo *pWindowResInfo, int32_t order) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  
  for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
    SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, i);
    if (!pStatus->closed) {
      continue;
    }

    SWindowResult *buf = getWindowResult(pWindowResInfo, i);

    // open/close the specified query for each group result
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t functId = pQuery->pSelectExpr[j].base.functionId;

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_ASC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_DESC)) {
        buf->resultInfo[j].complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        buf->resultInfo[j].complete = true;
      }
    }
  }
}

void disableFuncInReverseScan(SQInfo *pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  // group by normal columns and interval query on normal table
  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    disableFuncInReverseScanImpl(pQInfo, pWindowResInfo, order);
  } else {  // for simple result of table query,
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {  // todo refactor
      int32_t functId = pQuery->pSelectExpr[j].base.functionId;

      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[j];
      if (pCtx->resultInfo == NULL) {
        continue; // resultInfo is NULL, means no data checked in previous scan
      }

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_ASC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_DESC)) {
        pCtx->resultInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pCtx->resultInfo->complete = true;
      }
    }
  }
  
  int32_t numOfGroups = GET_NUM_OF_TABLEGROUP(pQInfo);
  
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray *group = GET_TABLEGROUP(pQInfo, i);
    
    size_t t = taosArrayGetSize(group);
    for (int32_t j = 0; j < t; ++j) {
      STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);
      updateTableQueryInfoForReverseScan(pQuery, pCheckInfo);
    }
  }
}

void switchCtxOrder(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SWITCH_ORDER(pRuntimeEnv->pCtx[i].order);
  }
}

int32_t createQueryResultInfo(SQuery *pQuery, SWindowResult *pResultRow, bool isSTableQuery, size_t interBufSize) {
  int32_t numOfCols = pQuery->numOfOutput;

  size_t size = numOfCols * sizeof(SResultInfo) + interBufSize;
  pResultRow->resultInfo = calloc(1, size);
  if (pResultRow->resultInfo == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  pResultRow->pos = (SPosInfo) {-1, -1};

  char* buf = (char*) pResultRow->resultInfo + numOfCols * sizeof(SResultInfo);

  // set the intermediate result output buffer
  setWindowResultInfo(pResultRow->resultInfo, pQuery, isSTableQuery, buf);
  return TSDB_CODE_SUCCESS;
}

void resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = pQuery->sdata[i]->data;

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT/TAGPRJ/PRJ/TAG etc.
     */
    RESET_RESULT_INFO(&pRuntimeEnv->resultInfo[i]);
    pCtx->resultInfo = &pRuntimeEnv->resultInfo[i];

    // set the timestamp output buffer for top/bottom/diff query
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    memset(pQuery->sdata[i]->data, 0, (size_t)pQuery->pSelectExpr[i].bytes * pQuery->rec.capacity);
  }

  initCtxOutputBuf(pRuntimeEnv);
}

void forwardCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, int64_t output) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // reset the execution contexts
  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].base.functionId;
    assert(functionId != TSDB_FUNC_DIFF);

    // set next output position
    if (IS_OUTER_FORWARD(aAggs[functionId].nStatus)) {
      pRuntimeEnv->pCtx[j].aOutputBuf += pRuntimeEnv->pCtx[j].outputBytes * output;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      /*
       * NOTE: for top/bottom query, the value of first column of output (timestamp) are assigned
       * in the procedure of top/bottom routine
       * the output buffer in top/bottom routine is ptsOutputBuf, so we need to forward the output buffer
       *
       * diff function is handled in multi-output function
       */
      pRuntimeEnv->pCtx[j].ptsOutputBuf += TSDB_KEYSIZE * output;
    }

    RESET_RESULT_INFO(pRuntimeEnv->pCtx[j].resultInfo);
  }
}

void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].base.functionId;
    pRuntimeEnv->pCtx[j].currentStage = 0;

    SResultInfo* pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);
    if (pResInfo->initialized) {
      continue;
    }

    aAggs[functionId].init(&pRuntimeEnv->pCtx[j]);
  }
}

void skipResults(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pQuery->rec.rows == 0 || pQuery->limit.offset == 0) {
    return;
  }

  if (pQuery->rec.rows <= pQuery->limit.offset) {
    qDebug("QInfo:%p skip rows:%" PRId64 ", new offset:%" PRIu64, GET_QINFO_ADDR(pRuntimeEnv), pQuery->rec.rows,
        pQuery->limit.offset - pQuery->rec.rows);
    
    pQuery->limit.offset -= pQuery->rec.rows;
    pQuery->rec.rows = 0;

    resetCtxOutputBuf(pRuntimeEnv);

    // clear the buffer full flag if exists
    CLEAR_QUERY_STATUS(pQuery, QUERY_RESBUF_FULL);
  } else {
    int64_t numOfSkip = pQuery->limit.offset;
    pQuery->rec.rows -= numOfSkip;
    pQuery->limit.offset = 0;
  
    qDebug("QInfo:%p skip row:%"PRId64", new offset:%d, numOfRows remain:%" PRIu64, GET_QINFO_ADDR(pRuntimeEnv), numOfSkip,
           0, pQuery->rec.rows);
    
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      
      memmove(pQuery->sdata[i]->data, (char*) pQuery->sdata[i]->data + bytes * numOfSkip, pQuery->rec.rows * bytes);
      pRuntimeEnv->pCtx[i].aOutputBuf = ((char*) pQuery->sdata[i]->data) + pQuery->rec.rows * bytes;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
      }
    }

    updateNumOfResult(pRuntimeEnv, pQuery->rec.rows);
  }
}

void setQueryStatus(SQuery *pQuery, int8_t status) {
  if (status == QUERY_NOT_COMPLETED) {
    pQuery->status = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    CLEAR_QUERY_STATUS(pQuery, QUERY_NOT_COMPLETED);
    pQuery->status |= status;
  }
}

bool needScanDataBlocksAgain(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  bool toContinue = false;
  if (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    // for each group result, call the finalize function for each column
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *pResult = getWindowResult(pWindowResInfo, i);
      if (!pResult->status.closed) {
        continue;
      }

      setWindowResOutputBuf(pRuntimeEnv, pResult);

      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        int16_t functId = pQuery->pSelectExpr[j].base.functionId;
        if (functId == TSDB_FUNC_TS) {
          continue;
        }

        aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
        SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

        toContinue |= (!pResInfo->complete);
      }
    }
  } else {
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int16_t functId = pQuery->pSelectExpr[j].base.functionId;
      if (functId == TSDB_FUNC_TS) {
        continue;
      }

      aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
      SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

      toContinue |= (!pResInfo->complete);
    }
  }

  return toContinue;
}

static SQueryStatusInfo getQueryStatusInfo(SQueryRuntimeEnv *pRuntimeEnv, TSKEY start) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;
  
  assert((start <= pTableQueryInfo->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (start >= pTableQueryInfo->lastKey && !QUERY_IS_ASC_QUERY(pQuery)));
  
  SQueryStatusInfo info = {
      .status      = pQuery->status,
      .windowIndex = pRuntimeEnv->windowResInfo.curIndex,
      .lastKey     = start,
      .w           = pQuery->window,
      .curWindow   = {.skey = start, .ekey = pTableQueryInfo->win.ekey},
  };

  return info;
}

static void setEnvBeforeReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatusInfo *pStatus) {
  SQInfo *pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pStatus->cur = tsBufGetCursor(pRuntimeEnv->pTSBuf);  // save the cursor
  if (pRuntimeEnv->pTSBuf) {
    SWITCH_ORDER(pRuntimeEnv->pTSBuf->cur.order);
    tsBufNextPos(pRuntimeEnv->pTSBuf);
  }

  // reverse order time range
  pQuery->window = pStatus->curWindow;
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);

  SWITCH_ORDER(pQuery->order.order);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    assert(pQuery->window.skey <= pQuery->window.ekey);
  } else {
    assert(pQuery->window.skey >= pQuery->window.ekey);
  }

  SET_REVERSE_SCAN_FLAG(pRuntimeEnv);

  STsdbQueryCond cond = {
      .twindow = pQuery->window,
      .order   = pQuery->order.order,
      .colList = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
  };

  // clean unused handle
  if (pRuntimeEnv->pSecQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
  }

  // add ref for table
  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo);
  if (pRuntimeEnv->pSecQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  switchCtxOrder(pRuntimeEnv);
  disableFuncInReverseScan(pQInfo);
}

static void clearEnvAfterReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatusInfo *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;

  SWITCH_ORDER(pQuery->order.order);
  switchCtxOrder(pRuntimeEnv);

  tsBufSetCursor(pRuntimeEnv->pTSBuf, &pStatus->cur);
  if (pRuntimeEnv->pTSBuf) {
    pRuntimeEnv->pTSBuf->cur.order = pQuery->order.order;
  }

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);

  // update the pQuery->window.skey and pQuery->window.ekey to limit the scan scope of sliding query during reverse scan
  pTableQueryInfo->lastKey = pStatus->lastKey;
  pQuery->status = pStatus->status;
  
  pTableQueryInfo->win = pStatus->w;
  pQuery->window = pTableQueryInfo->win;
}

void scanOneTableDataBlocks(SQueryRuntimeEnv *pRuntimeEnv, TSKEY start) {
  SQInfo *pQInfo = (SQInfo *) GET_QINFO_ADDR(pRuntimeEnv);
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo *pTableQueryInfo = pQuery->current;
  
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  // store the start query position
  SQueryStatusInfo qstatus = getQueryStatusInfo(pRuntimeEnv, start);

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  while (1) {
    doScanAllDataBlocks(pRuntimeEnv);

    if (pRuntimeEnv->scanFlag == MASTER_SCAN) {
      qstatus.status = pQuery->status;

      // do nothing if no data blocks are found qualified during scan
      if (qstatus.lastKey != pTableQueryInfo->lastKey) {
        qstatus.curWindow.ekey = pTableQueryInfo->lastKey - step;
      }

      qstatus.lastKey = pTableQueryInfo->lastKey;
    }

    if (!needScanDataBlocksAgain(pRuntimeEnv)) {
      // restore the status code and jump out of loop
      if (pRuntimeEnv->scanFlag == REPEAT_SCAN) {
        pQuery->status = qstatus.status;
      }

      break;
    }

    STsdbQueryCond cond = {
        .twindow = qstatus.curWindow,
        .order   = pQuery->order.order,
        .colList = pQuery->colList,
        .numOfCols = pQuery->numOfCols,
    };

    if (pRuntimeEnv->pSecQueryHandle != NULL) {
      tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
    }

    pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo);
    if (pRuntimeEnv->pSecQueryHandle == NULL) {
      longjmp(pRuntimeEnv->env, terrno);
    }

    pRuntimeEnv->windowResInfo.curIndex = qstatus.windowIndex;
    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;
    
    qDebug("QInfo:%p start to repeat scan data blocks due to query func required, qrange:%"PRId64"-%"PRId64, pQInfo,
        cond.twindow.skey, cond.twindow.ekey);

    // check if query is killed or not
    if (IS_QUERY_KILLED(pQInfo)) {
      finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }
  }

  if (!needReverseScan(pQuery)) {
    return;
  }

  setEnvBeforeReverseScan(pRuntimeEnv, &qstatus);

  // reverse scan from current position
  qDebug("QInfo:%p start to reverse scan", pQInfo);
  doScanAllDataBlocks(pRuntimeEnv);

  clearEnvAfterReverseScan(pRuntimeEnv, &qstatus);
}

void finalizeQueryResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    // for each group result, call the finalize function for each column
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    if (pRuntimeEnv->groupbyNormalCol) {
      closeAllTimeWindow(pWindowResInfo);
    }

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *buf = &pWindowResInfo->pResult[i];
      if (!isWindowResClosed(pWindowResInfo, i)) {
        continue;
      }

      setWindowResOutputBuf(pRuntimeEnv, buf);

      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        aAggs[pQuery->pSelectExpr[j].base.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
      }

      /*
       * set the number of output results for group by normal columns, the number of output rows usually is 1 except
       * the top and bottom query
       */
      buf->numOfRows = getNumOfResult(pRuntimeEnv);
    }

  } else {
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      aAggs[pQuery->pSelectExpr[j].base.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
    }
  }
}

static bool hasMainOutput(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;

    if (functionId != TSDB_FUNC_TS && functionId != TSDB_FUNC_TAG && functionId != TSDB_FUNC_TAGPRJ) {
      return true;
    }
  }

  return false;
}

static STableQueryInfo *createTableQueryInfo(SQueryRuntimeEnv *pRuntimeEnv, void* pTable, STimeWindow win, void* buf) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STableQueryInfo *pTableQueryInfo = buf;

  pTableQueryInfo->win = win;
  pTableQueryInfo->lastKey = win.skey;

  pTableQueryInfo->pTable = pTable;
  pTableQueryInfo->cur.vgroupIndex = -1;

  // set more initial size of interval/groupby query
  if (QUERY_IS_INTERVAL_QUERY(pQuery) || pRuntimeEnv->groupbyNormalCol) {
    int32_t initialSize = 16;
    int32_t initialThreshold = 100;
    int32_t code = initWindowResInfo(&pTableQueryInfo->windowResInfo, pRuntimeEnv, initialSize, initialThreshold, TSDB_DATA_TYPE_INT);
    if (code != TSDB_CODE_SUCCESS) {
      return NULL;
    }
  } else { // in other aggregate query, do not initialize the windowResInfo
  }

  return pTableQueryInfo;
}

void destroyTableQueryInfo(STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  cleanupTimeWindowInfo(&pTableQueryInfo->windowResInfo);
}

#define CHECK_QUERY_TIME_RANGE(_q, _tableInfo)                                              \
  do {                                                                                      \
    assert((((_tableInfo)->lastKey >= (_tableInfo)->win.skey) && QUERY_IS_ASC_QUERY(_q)) || \
           (((_tableInfo)->lastKey <= (_tableInfo)->win.skey) && !QUERY_IS_ASC_QUERY(_q))); \
  } while (0)

/**
 * set output buffer for different group
 * @param pRuntimeEnv
 * @param pDataBlockInfo
 */
void setExecutionContext(SQInfo *pQInfo, int32_t groupIndex, TSKEY nextKey) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  STableQueryInfo  *pTableQueryInfo = pRuntimeEnv->pQuery->current;
  SWindowResInfo   *pWindowResInfo = &pRuntimeEnv->windowResInfo;

  // lastKey needs to be updated
  pTableQueryInfo->lastKey = nextKey;

  if (pRuntimeEnv->hasTagResults || pRuntimeEnv->pTSBuf != NULL) {
    setAdditionalInfo(pQInfo, pTableQueryInfo->pTable, pTableQueryInfo);
  }

  if (pRuntimeEnv->prevGroupId != INT32_MIN && pRuntimeEnv->prevGroupId == groupIndex) {
    return;
  }

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&groupIndex,
      sizeof(groupIndex), true);
  if (pWindowRes == NULL) {
    return;
  }

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pWindowRes->pos.pageId == -1) {
    if (addNewWindowResultBuf(pWindowRes, pRuntimeEnv->pResultBuf, groupIndex, pRuntimeEnv->numOfRowsPerPage) !=
        TSDB_CODE_SUCCESS) {
      return;
    }
  }

  // record the current active group id
  pRuntimeEnv->prevGroupId = groupIndex;
  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);
}

void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = getPosInResultPage(pRuntimeEnv, i, pResult);

    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx->resultInfo = &pResult->resultInfo[i];

    // set super table query flag
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->superTableQ = pRuntimeEnv->stableQuery;
  }
}

void setWindowResOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

    pCtx->resultInfo = &pResult->resultInfo[i];
    if (pCtx->resultInfo->initialized && pCtx->resultInfo->complete) {
      continue;
    }

    pCtx->aOutputBuf = getPosInResultPage(pRuntimeEnv, i, pResult);
    pCtx->currentStage = 0;

    int32_t functionId = pCtx->functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx->resultInfo->superTableQ = pRuntimeEnv->stableQuery;     // set super table query flag

    if (!pCtx->resultInfo->initialized) {
      aAggs[functionId].init(pCtx);
    }
  }
}

int32_t setAdditionalInfo(SQInfo *pQInfo, void* pTable, STableQueryInfo *pTableQueryInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  setTagVal(pRuntimeEnv, pTable, pQInfo->tsdb);

  // both the master and supplement scan needs to set the correct ts comp start position
  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pTableQueryInfo->cur.vgroupIndex == -1) {
      pTableQueryInfo->tag = pRuntimeEnv->pCtx[0].tag.i64Key;

      tsBufGetElemStartPos(pRuntimeEnv->pTSBuf, 0, pTableQueryInfo->tag);

      // keep the cursor info of current meter
      pTableQueryInfo->cur = pRuntimeEnv->pTSBuf->cur;
    } else {
      tsBufSetCursor(pRuntimeEnv->pTSBuf, &pTableQueryInfo->cur);
    }
  }

  return 0;
}

/*
 * There are two cases to handle:
 *
 * 1. Query range is not set yet (queryRangeSet = 0). we need to set the query range info, including pQuery->lastKey,
 *    pQuery->window.skey, and pQuery->eKey.
 * 2. Query range is set and query is in progress. There may be another result with the same query ranges to be
 *    merged during merge stage. In this case, we need the pTableQueryInfo->lastResRows to decide if there
 *    is a previous result generated or not.
 */
void setIntervalQueryRange(SQInfo *pQInfo, TSKEY key) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo *pTableQueryInfo = pQuery->current;
  
  if (pTableQueryInfo->queryRangeSet) {
    pTableQueryInfo->lastKey = key;
  } else {
    pTableQueryInfo->win.skey = key;
    STimeWindow win = {.skey = key, .ekey = pQuery->window.ekey};

    // for too small query range, no data in this interval.
    if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.ekey < pQuery->window.skey)) ||
        (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.skey < pQuery->window.ekey))) {
      return;
    }

    /**
     * In handling the both ascending and descending order super table query, we need to find the first qualified
     * timestamp of this table, and then set the first qualified start timestamp.
     * In ascending query, key is the first qualified timestamp. However, in the descending order query, additional
     * operations involve.
     */
    STimeWindow     w = TSWINDOW_INITIALIZER;
    SWindowResInfo *pWindowResInfo = &pTableQueryInfo->windowResInfo;

    TSKEY sk = MIN(win.skey, win.ekey);
    TSKEY ek = MAX(win.skey, win.ekey);
    getAlignQueryTimeWindow(pQuery, win.skey, sk, ek, &w);
    pWindowResInfo->startTime = pTableQueryInfo->win.skey;  // windowSKey may be 0 in case of 1970 timestamp

    if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        assert(win.ekey == pQuery->window.ekey);
      }
      
      pWindowResInfo->prevSKey = w.skey;
    }

    pTableQueryInfo->queryRangeSet = 1;
    pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;
  }
}

bool requireTimestamp(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; i++) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_NEED_TS) != 0) {
      return true;
    }
  }
  return false;
}

bool needPrimaryTimestampCol(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo) {
  /*
   * 1. if skey or ekey locates in this block, we need to load the timestamp column to decide the precise position
   * 2. if there are top/bottom, first_dst/last_dst functions, we need to load timestamp column in any cases;
   */
  STimeWindow *w = &pDataBlockInfo->window;
  STableQueryInfo* pTableQueryInfo = pQuery->current;
  
  bool loadPrimaryTS = (pTableQueryInfo->lastKey >= w->skey && pTableQueryInfo->lastKey <= w->ekey) ||
                       (pQuery->window.ekey >= w->skey && pQuery->window.ekey <= w->ekey) || requireTimestamp(pQuery);

  return loadPrimaryTS;
}

static int32_t doCopyToSData(SQInfo *pQInfo, SWindowResInfo *pResultInfo, int32_t orderType) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfResult = 0;
  int32_t startIdx = 0;
  int32_t step = -1;

  qDebug("QInfo:%p start to copy data from windowResInfo to query buf", pQInfo);
  int32_t totalSet = numOfClosedTimeWindow(pResultInfo);
  SWindowResult* result = pResultInfo->pResult;

  if (orderType == TSDB_ORDER_ASC) {
    startIdx = pQInfo->groupIndex;
    step = 1;
  } else {  // desc order copy all data
    startIdx = totalSet - pQInfo->groupIndex - 1;
    step = -1;
  }

  for (int32_t i = startIdx; (i < totalSet) && (i >= 0); i += step) {
    if (result[i].numOfRows == 0) {
      pQInfo->offset = 0;
      pQInfo->groupIndex += 1;
      continue;
    }

    assert(pQInfo->offset <= 1);

    int32_t numOfRowsToCopy = result[i].numOfRows - pQInfo->offset;
    int32_t oldOffset = pQInfo->offset;

    /*
     * current output space is not enough to keep all the result data of this group, only copy partial results
     * to SQuery object's result buffer
     */
    if (numOfRowsToCopy > pQuery->rec.capacity - numOfResult) {
      numOfRowsToCopy = pQuery->rec.capacity - numOfResult;
      pQInfo->offset += numOfRowsToCopy;
    } else {
      pQInfo->offset = 0;
      pQInfo->groupIndex += 1;
    }

    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t size = pRuntimeEnv->pCtx[j].outputBytes;

      char *out = pQuery->sdata[j]->data + numOfResult * size;
      char *in = getPosInResultPage(pRuntimeEnv, j, &result[i]);
      memcpy(out, in + oldOffset * size, size * numOfRowsToCopy);
    }

    numOfResult += numOfRowsToCopy;
    if (numOfResult == pQuery->rec.capacity) {
      break;
    }
  }

  qDebug("QInfo:%p copy data to query buf completed", pQInfo);

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pRuntimeEnv, numOfResult);
#endif
  return numOfResult;
}

/**
 * copyFromWindowResToSData support copy data in ascending/descending order
 * For interval query of both super table and table, copy the data in ascending order, since the output results are
 * ordered in SWindowResutl already. While handling the group by query for both table and super table,
 * all group result are completed already.
 *
 * @param pQInfo
 * @param result
 */
void copyFromWindowResToSData(SQInfo *pQInfo, SWindowResInfo *pResultInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSDB_ORDER_ASC;
  int32_t numOfResult = doCopyToSData(pQInfo, pResultInfo, orderType);

  pQuery->rec.rows += numOfResult;

  assert(pQuery->rec.rows <= pQuery->rec.capacity);
}

static void updateWindowResNumOfRes(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // update the number of result for each, only update the number of rows for the corresponding window result.
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    return;
  }

  for (int32_t i = 0; i < pRuntimeEnv->windowResInfo.size; ++i) {
    SWindowResult *pResult = &pRuntimeEnv->windowResInfo.pResult[i];

    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t functionId = pRuntimeEnv->pCtx[j].functionId;
      if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
        continue;
      }

      pResult->numOfRows = MAX(pResult->numOfRows, pResult->resultInfo[j].numOfRes);
    }
  }
}

static void stableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo, SDataStatis *pStatis,
    SArray *pDataBlock, __block_search_fn_t searchFn) {
  SQuery *         pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;
  
  SWindowResInfo * pWindowResInfo = &pTableQueryInfo->windowResInfo;
  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery)? 0 : pDataBlockInfo->rows - 1;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL || pRuntimeEnv->groupbyNormalCol) {
    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, pDataBlock);
  } else {
    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, searchFn, pDataBlock);
  }
}

bool queryHasRemainResults(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SFillInfo *pFillInfo = pRuntimeEnv->pFillInfo;

  if (pQuery->limit.limit > 0 && pQuery->rec.total >= pQuery->limit.limit) {
    return false;
  }

  if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
    // There are results not returned to client yet, so filling operation applied to the remain result is required
    // in the first place.
    int32_t remain = taosNumOfRemainRows(pFillInfo);
    if (remain > 0) {
      return true;
    }

    /*
     * While the code reaches here, there are no results remains now.
     * If query is not completed yet, the gaps between two results blocks need to be handled after next data block
     * is retrieved from TSDB.
     *
     * NOTE: If the result set is not the first block, the gap in front of the result set will be filled. If the result
     * set is the FIRST result block, the gap between the start time of query time window and the timestamp of the
     * first result row in the actual result set will fill nothing.
     */
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      int32_t numOfTotal = getFilledNumOfRes(pFillInfo, pQuery->window.ekey, pQuery->rec.capacity);
      return numOfTotal > 0;
    }

  } else {
    // there are results waiting for returned to client.
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED) &&
        (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery)) &&
        (pRuntimeEnv->windowResInfo.size > 0)) {
      return true;
    }
  }

  return false;
}

static void doCopyQueryResultToMsg(SQInfo *pQInfo, int32_t numOfRows, char *data) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
    int32_t bytes = pQuery->pSelectExpr[col].bytes;

    memmove(data, pQuery->sdata[col]->data, bytes * numOfRows);
    data += bytes * numOfRows;
  }

  int32_t numOfTables = (int32_t)taosArrayGetSize(pQInfo->arrTableIdInfo);
  *(int32_t*)data = htonl(numOfTables);
  data += sizeof(int32_t);
  for(int32_t i = 0; i < numOfTables; i++) {
    STableIdInfo* pSrc = taosArrayGet(pQInfo->arrTableIdInfo, i);
    STableIdInfo* pDst = (STableIdInfo*)data;
    pDst->uid = htobe64(pSrc->uid);
    pDst->tid = htonl(pSrc->tid);
    pDst->key = htobe64(pSrc->key);
    data += sizeof(STableIdInfo);
  }

  // all data returned, set query over
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    if (pQInfo->runtimeEnv.stableQuery) {
      if (pQInfo->tableIndex >= pQInfo->tableqinfoGroupInfo.numOfTables) {
        setQueryStatus(pQuery, QUERY_OVER);
      }
    } else {
      if (!queryHasRemainResults(&pQInfo->runtimeEnv)) {
        setQueryStatus(pQuery, QUERY_OVER);
      }
    }
  }
}

int32_t doFillGapsInResults(SQueryRuntimeEnv* pRuntimeEnv, tFilePage **pDst, int32_t *numOfFilled) {
  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SFillInfo* pFillInfo = pRuntimeEnv->pFillInfo;
  
  while (1) {
    int32_t ret = taosGenerateDataBlock(pFillInfo, (tFilePage**) pQuery->sdata, pQuery->rec.capacity);
    
    // todo apply limit output function
    /* reached the start position of according to offset value, return immediately */
    if (pQuery->limit.offset == 0) {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows", pQInfo, pFillInfo->numOfRows, ret);
      return ret;
    }

    if (pQuery->limit.offset < ret) {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows, offset:%" PRId64 ". Discard due to offset, remain:%" PRId64 ", new offset:%d",
             pQInfo, pFillInfo->numOfRows, ret, pQuery->limit.offset, ret - pQuery->limit.offset, 0);
      
      ret -= pQuery->limit.offset;
      // todo !!!!there exactly number of interpo is not valid.
      // todo refactor move to the beginning of buffer
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        memmove(pDst[i]->data, pDst[i]->data + pQuery->pSelectExpr[i].bytes * pQuery->limit.offset,
                ret * pQuery->pSelectExpr[i].bytes);
      }
      
      pQuery->limit.offset = 0;
      return ret;
    } else {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows, offset:%" PRId64 ". Discard due to offset, "
             "remain:%d, new offset:%" PRId64, pQInfo, pFillInfo->numOfRows, ret, pQuery->limit.offset, 0,
          pQuery->limit.offset - ret);
      
      pQuery->limit.offset -= ret;
      pQuery->rec.rows = 0;
      ret = 0;
    }

    if (!queryHasRemainResults(pRuntimeEnv)) {
      return ret;
    }
  }
}

static void queryCostStatis(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQueryCostInfo *pSummary = &pRuntimeEnv->summary;

  qDebug("QInfo:%p :cost summary: elapsed time:%"PRId64" us, total blocks:%d, load block statis:%d,"
         " load data block:%d, total rows:%"PRId64 ", check rows:%"PRId64,
         pQInfo, pSummary->elapsedTime, pSummary->totalBlocks, pSummary->loadBlockStatis,
         pSummary->loadBlocks, pSummary->totalRows, pSummary->totalCheckedRows);
}

static void updateOffsetVal(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pBlockInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;
  
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  if (pQuery->limit.offset == pBlockInfo->rows) {  // current block will ignore completed
    pTableQueryInfo->lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pBlockInfo->window.ekey + step : pBlockInfo->window.skey + step;
    pQuery->limit.offset = 0;
    return;
  }

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    pQuery->pos = pQuery->limit.offset;
  } else {
    pQuery->pos = pBlockInfo->rows - pQuery->limit.offset - 1;
  }

  assert(pQuery->pos >= 0 && pQuery->pos <= pBlockInfo->rows - 1);

  SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
  SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);

  // update the pQuery->limit.offset value, and pQuery->pos value
  TSKEY *keys = (TSKEY *) pColInfoData->pData;

  // update the offset value
  pTableQueryInfo->lastKey = keys[pQuery->pos];
  pQuery->limit.offset = 0;

  int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);

  qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%"PRId64, GET_QINFO_ADDR(pRuntimeEnv),
         pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes, pQuery->current->lastKey);
}

void skipBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0) {
    return;
  }

  pQuery->pos = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  STableQueryInfo* pTableQueryInfo = pQuery->current;
  TsdbQueryHandleT pQueryHandle = pRuntimeEnv->pQueryHandle;

  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
  while (true) {
    if (!tsdbNextDataBlock(pQueryHandle)) {
      if (terrno != TSDB_CODE_SUCCESS) {
        longjmp(pRuntimeEnv->env, terrno);
      }
      break;
    }

    if (IS_QUERY_KILLED(GET_QINFO_ADDR(pRuntimeEnv))) {
      finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);

    if (pQuery->limit.offset > blockInfo.rows) {
      pQuery->limit.offset -= blockInfo.rows;
      pTableQueryInfo->lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.window.ekey : blockInfo.window.skey;
      pTableQueryInfo->lastKey += step;

      qDebug("QInfo:%p skip rows:%d, offset:%" PRId64, GET_QINFO_ADDR(pRuntimeEnv), blockInfo.rows,
             pQuery->limit.offset);
    } else {  // find the appropriated start position in current block
      updateOffsetVal(pRuntimeEnv, &blockInfo);
      break;
    }
  }
}

static bool skipTimeInterval(SQueryRuntimeEnv *pRuntimeEnv, TSKEY* start) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  *start = pQuery->current->lastKey;

  // if queried with value filter, do NOT forward query start position
  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL || pRuntimeEnv->pFillInfo != NULL) {
    return true;
  }

  /*
   * 1. for interval without interpolation query we forward pQuery->intervalTime at a time for
   *    pQuery->limit.offset times. Since hole exists, pQuery->intervalTime*pQuery->limit.offset value is
   *    not valid. otherwise, we only forward pQuery->limit.offset number of points
   */
  assert(pRuntimeEnv->windowResInfo.prevSKey == TSKEY_INITIAL_VAL);

  STimeWindow w = TSWINDOW_INITIALIZER;
  
  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  STableQueryInfo *pTableQueryInfo = pQuery->current;

  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
  while (true) {
    if (!tsdbNextDataBlock(pRuntimeEnv->pQueryHandle)) {
      if (terrno != TSDB_CODE_SUCCESS) {
        longjmp(pRuntimeEnv->env, terrno);
      }
      break;
    }

    tsdbRetrieveDataBlockInfo(pRuntimeEnv->pQueryHandle, &blockInfo);

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
        getAlignQueryTimeWindow(pQuery, blockInfo.window.skey, blockInfo.window.skey, pQuery->window.ekey, &w);
        pWindowResInfo->startTime = w.skey;
        pWindowResInfo->prevSKey = w.skey;
      }
    } else {
      getAlignQueryTimeWindow(pQuery, blockInfo.window.ekey, pQuery->window.ekey, blockInfo.window.ekey, &w);

      pWindowResInfo->startTime = pQuery->window.skey;
      pWindowResInfo->prevSKey = w.skey;
    }

    // the first time window
    STimeWindow win = getActiveTimeWindow(pWindowResInfo, pWindowResInfo->prevSKey, pQuery);

    while (pQuery->limit.offset > 0) {
      if ((win.ekey <= blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (win.ekey >= blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
        pQuery->limit.offset -= 1;
        pWindowResInfo->prevSKey = win.skey;
      }

      STimeWindow tw = win;
      GET_NEXT_TIMEWINDOW(pQuery, &tw);

      if (pQuery->limit.offset == 0) {
        if ((tw.skey <= blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (tw.ekey >= blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
          // load the data block and check data remaining in current data block
          // TODO optimize performance
          SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
          SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);

          tw = win;
          int32_t startPos =
              getNextQualifiedWindow(pRuntimeEnv, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey, -1);
          assert(startPos >= 0);

          // set the abort info
          pQuery->pos = startPos;
          
          // reset the query start timestamp
          pTableQueryInfo->win.skey = ((TSKEY *)pColInfoData->pData)[startPos];
          pQuery->window.skey = pTableQueryInfo->win.skey;
          *start = pTableQueryInfo->win.skey;
          
          pWindowResInfo->prevSKey = tw.skey;
          int32_t index = pRuntimeEnv->windowResInfo.curIndex;
          
          int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, NULL, binarySearchForKey, pDataBlock);
          pRuntimeEnv->windowResInfo.curIndex = index;  // restore the window index
          
          qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%"PRId64,
                 GET_QINFO_ADDR(pRuntimeEnv), blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes, pQuery->current->lastKey);
          
          return true;
        } else { // do nothing
          *start = tw.skey;
          pQuery->window.skey = tw.skey;
          pWindowResInfo->prevSKey = tw.skey;
          return true;
        }
      }

      /*
       * If the next time window still starts from current data block,
       * load the primary timestamp column first, and then find the start position for the next queried time window.
       * Note that only the primary timestamp column is required.
       * TODO: Optimize for this cases. All data blocks are not needed to be loaded, only if the first actually required
       * time window resides in current data block.
       */
      if ((tw.skey <= blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (tw.ekey >= blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
        SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
        SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);

        tw = win;
        int32_t startPos =
            getNextQualifiedWindow(pRuntimeEnv, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey, -1);
        assert(startPos >= 0);

        // set the abort info
        pQuery->pos = startPos;
        pTableQueryInfo->lastKey = ((TSKEY *)pColInfoData->pData)[startPos];
        pWindowResInfo->prevSKey = tw.skey;
        win = tw;
      } else {
        break;  // offset is not 0, and next time window begins or ends in the next block.
      }
    }
  }

  return true;
}

static int32_t setupQueryHandle(void* tsdb, SQInfo* pQInfo, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if (onlyQueryTags(pQuery)) {
    return TSDB_CODE_SUCCESS;
  }

  if (isSTableQuery && (!QUERY_IS_INTERVAL_QUERY(pQuery)) && (!isFixedOutputQuery(pRuntimeEnv))) {
    return TSDB_CODE_SUCCESS;
  }

  STsdbQueryCond cond = {
    .twindow = pQuery->window,
    .order   = pQuery->order.order,
    .colList = pQuery->colList,
    .numOfCols = pQuery->numOfCols,
  };

  if (!isSTableQuery
    && (pQInfo->tableqinfoGroupInfo.numOfTables == 1)
    && (cond.order == TSDB_ORDER_ASC) 
    && (!QUERY_IS_INTERVAL_QUERY(pQuery))
    && (!isGroupbyNormalCol(pQuery->pGroupbyExpr))
    && (!isFixedOutputQuery(pRuntimeEnv))
  ) {
    SArray* pa = GET_TABLEGROUP(pQInfo, 0);
    STableQueryInfo* pCheckInfo = taosArrayGetP(pa, 0);
    cond.twindow = pCheckInfo->win;
  }

  terrno = TSDB_CODE_SUCCESS;
  if (isFirstLastRowQuery(pQuery)) {
    pRuntimeEnv->pQueryHandle = tsdbQueryLastRow(tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo);
  } else if (isPointInterpoQuery(pQuery)) {
    pRuntimeEnv->pQueryHandle = tsdbQueryRowsInExternalWindow(tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo);
  } else {
    pRuntimeEnv->pQueryHandle = tsdbQueryTables(tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo);
  }

  return terrno;
}

static SFillColInfo* taosCreateFillColInfo(SQuery* pQuery) {
  int32_t numOfCols = pQuery->numOfOutput;
  int32_t offset = 0;
  
  SFillColInfo* pFillCol = calloc(numOfCols, sizeof(SFillColInfo));
  for(int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExprInfo = &pQuery->pSelectExpr[i];
    
    pFillCol[i].col.bytes  = pExprInfo->bytes;
    pFillCol[i].col.type   = pExprInfo->type;
    pFillCol[i].col.offset = offset;
    pFillCol[i].flag       = TSDB_COL_NORMAL;    // always be ta normal column for table query
    pFillCol[i].functionId = pExprInfo->base.functionId;
    pFillCol[i].fillVal.i = pQuery->fillVal[i];
    
    offset += pExprInfo->bytes;
  }
  
  return pFillCol;
}

int32_t doInitQInfo(SQInfo *pQInfo, STSBuf *pTsBuf, void *tsdb, int32_t vgId, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  int32_t code = TSDB_CODE_SUCCESS;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  pQuery->precision = tsdbGetCfg(tsdb)->precision;
  pRuntimeEnv->topBotQuery = isTopBottomQuery(pQuery);
  pRuntimeEnv->hasTagResults = hasTagValOutput(pQuery);

  setScanLimitationByResultBuffer(pQuery);
  changeExecuteScanOrder(pQInfo, false);

  code = setupQueryHandle(tsdb, pQInfo, isSTableQuery);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  
  pQInfo->tsdb = tsdb;
  pQInfo->vgId = vgId;

  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pTSBuf = pTsBuf;
  pRuntimeEnv->cur.vgroupIndex = -1;
  pRuntimeEnv->stableQuery = isSTableQuery;
  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->groupbyNormalCol = isGroupbyNormalCol(pQuery->pGroupbyExpr);

  if (pTsBuf != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTSBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTSBuf, order);
  }

  // create runtime environment
  code = setupQueryRuntimeEnv(pRuntimeEnv, pQuery->order.order);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t ps = DEFAULT_PAGE_SIZE;
  int32_t rowsize = 0;
  getIntermediateBufInfo(pRuntimeEnv, &ps, &rowsize);

  if (isSTableQuery && !onlyQueryTags(pRuntimeEnv->pQuery)) {
    int32_t numOfPages = getInitialPageNum(pQInfo);
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, numOfPages, rowsize, ps, numOfPages, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (!QUERY_IS_INTERVAL_QUERY(pQuery)) {
      int16_t type = TSDB_DATA_TYPE_NULL;
      int32_t threshold = 0;

      if (pRuntimeEnv->groupbyNormalCol) {  // group by columns not tags;
        type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
        threshold = 4000;
      } else {
        type = TSDB_DATA_TYPE_INT;  // group id
        threshold = GET_NUM_OF_TABLEGROUP(pQInfo);
        if (threshold < 8) {
          threshold = 8;
        }
      }

      code = initWindowResInfo(&pRuntimeEnv->windowResInfo, pRuntimeEnv, 8, threshold, type);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else if (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    int32_t numOfResultRows = getInitialPageNum(pQInfo);
    getIntermediateBufInfo(pRuntimeEnv, &ps, &rowsize);

    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, numOfResultRows, rowsize, ps, numOfResultRows, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    int16_t type = TSDB_DATA_TYPE_NULL;
    if (pRuntimeEnv->groupbyNormalCol) {
      type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
    } else {
      type = TSDB_DATA_TYPE_TIMESTAMP;
    }

    code = initWindowResInfo(&pRuntimeEnv->windowResInfo, pRuntimeEnv, numOfResultRows, 4096, type);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
    SFillColInfo* pColInfo = taosCreateFillColInfo(pQuery);
    STimeWindow w = TSWINDOW_INITIALIZER;

    TSKEY sk = MIN(pQuery->window.skey, pQuery->window.ekey);
    TSKEY ek = MAX(pQuery->window.skey, pQuery->window.ekey);
    getAlignQueryTimeWindow(pQuery, pQuery->window.skey, sk, ek, &w);

    pRuntimeEnv->pFillInfo = taosInitFillInfo(pQuery->order.order, w.skey, 0, pQuery->rec.capacity, pQuery->numOfOutput,
                                              pQuery->slidingTime, pQuery->slidingTimeUnit, pQuery->precision,
                                              pQuery->fillType, pColInfo);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  return TSDB_CODE_SUCCESS;
}

static void enableExecutionForNextTable(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[i]);
    if (pResInfo != NULL) {
      pResInfo->complete = false;
    }
  }
}

static FORCE_INLINE void setEnvForEachBlock(SQInfo* pQInfo, STableQueryInfo* pTableQueryInfo, SDataBlockInfo* pBlockInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  if (!QUERY_IS_INTERVAL_QUERY(pQuery)) {
    setExecutionContext(pQInfo, pTableQueryInfo->groupIndex, pBlockInfo->window.ekey + step);
  } else {  // interval query
    TSKEY nextKey = pBlockInfo->window.skey;
    setIntervalQueryRange(pQInfo, nextKey);

    if (pRuntimeEnv->hasTagResults || pRuntimeEnv->pTSBuf != NULL) {
      setAdditionalInfo(pQInfo, pTableQueryInfo->pTable, pTableQueryInfo);
    }
  }
}

static int64_t scanMultiTableDataBlocks(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery*           pQuery = pRuntimeEnv->pQuery;
  SQueryCostInfo*   summary  = &pRuntimeEnv->summary;
  
  int64_t st = taosGetTimestampMs();

  TsdbQueryHandleT pQueryHandle = IS_MASTER_SCAN(pRuntimeEnv)? pRuntimeEnv->pQueryHandle : pRuntimeEnv->pSecQueryHandle;
  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  while (true) {
    if (!tsdbNextDataBlock(pQueryHandle)) {
      if (terrno != TSDB_CODE_SUCCESS) {
        longjmp(pRuntimeEnv->env, terrno);
      }
      break;
    }

    summary->totalBlocks += 1;
    
    if (IS_QUERY_KILLED(pQInfo)) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
    STableQueryInfo **pTableQueryInfo = (STableQueryInfo**) taosHashGet(pQInfo->tableqinfoGroupInfo.map, &blockInfo.tid, sizeof(blockInfo.tid));
    if(pTableQueryInfo == NULL) {
      break;
    }

    pQuery->current = *pTableQueryInfo;
    CHECK_QUERY_TIME_RANGE(pQuery, *pTableQueryInfo);

    if (!pRuntimeEnv->groupbyNormalCol) {
      setEnvForEachBlock(pQInfo, *pTableQueryInfo, &blockInfo);
    }

    SDataStatis *pStatis = NULL;
    SArray *pDataBlock = NULL;
    if (loadDataBlockOnDemand(pRuntimeEnv, pQueryHandle, &blockInfo, &pStatis, &pDataBlock) == BLK_DATA_DISCARD) {
      pQuery->current->lastKey = QUERY_IS_ASC_QUERY(pQuery)? blockInfo.window.ekey + step:blockInfo.window.skey + step;
      continue;
    }

    summary->totalRows += blockInfo.rows;
    stableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, pDataBlock, binarySearchForKey);
  
    qDebug("QInfo:%p check data block, uid:%"PRId64", tid:%d, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, lastKey:%" PRId64,
           pQInfo, blockInfo.uid, blockInfo.tid, blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, pQuery->current->lastKey);
  }

  updateWindowResNumOfRes(pRuntimeEnv);

  int64_t et = taosGetTimestampMs();
  return et - st;
}

static bool multiTableMultioutputHelper(SQInfo *pQInfo, int32_t index) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  SArray *group = GET_TABLEGROUP(pQInfo, 0);
  STableQueryInfo* pCheckInfo = taosArrayGetP(group, index);

  if (pRuntimeEnv->hasTagResults || pRuntimeEnv->pTSBuf != NULL) {
    setTagVal(pRuntimeEnv, pCheckInfo->pTable, pQInfo->tsdb);
  }

  STableId* id = TSDB_TABLEID(pCheckInfo->pTable);
  qDebug("QInfo:%p query on (%d): uid:%" PRIu64 ", tid:%d, qrange:%" PRId64 "-%" PRId64, pQInfo, index,
         id->uid, id->tid, pCheckInfo->lastKey, pCheckInfo->win.ekey);

  STsdbQueryCond cond = {
      .twindow   = {pCheckInfo->lastKey, pCheckInfo->win.ekey},
      .order     = pQuery->order.order,
      .colList   = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
  };

  // todo refactor
  SArray *g1 = taosArrayInit(1, POINTER_BYTES);
  SArray *tx = taosArrayInit(1, POINTER_BYTES);

  taosArrayPush(tx, &pCheckInfo->pTable);
  taosArrayPush(g1, &tx);
  STableGroupInfo gp = {.numOfTables = 1, .pGroupList = g1};

  // include only current table
  if (pRuntimeEnv->pQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
    pRuntimeEnv->pQueryHandle = NULL;
  }

  pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &gp, pQInfo);
  taosArrayDestroy(tx);
  taosArrayDestroy(g1);
  if (pRuntimeEnv->pQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pRuntimeEnv->cur.vgroupIndex == -1) {
      int64_t tag = pRuntimeEnv->pCtx[0].tag.i64Key;
      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTSBuf, 0, tag);

      // failed to find data with the specified tag value
      if (elem.vnode < 0) {
        return false;
      }
    } else {
      tsBufSetCursor(pRuntimeEnv->pTSBuf, &pRuntimeEnv->cur);
    }
  }

  initCtxOutputBuf(pRuntimeEnv);
  return true;
}

/**
 * super table query handler
 * 1. super table projection query, group-by on normal columns query, ts-comp query
 * 2. point interpolation query, last row query
 *
 * @param pQInfo
 */
static void sequentialTableProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_COMPLETED);

  size_t numOfGroups = GET_NUM_OF_TABLEGROUP(pQInfo);

  if (isPointInterpoQuery(pQuery) || isFirstLastRowQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pQInfo->groupIndex < numOfGroups) {
      SArray* group = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, pQInfo->groupIndex);

      qDebug("QInfo:%p last_row query on group:%d, total group:%zu, current group:%p", pQInfo, pQInfo->groupIndex,
             numOfGroups, group);

      STsdbQueryCond cond = {
          .twindow = pQuery->window,
          .colList = pQuery->colList,
          .order   = pQuery->order.order,
          .numOfCols = pQuery->numOfCols,
      };

      SArray *g1 = taosArrayInit(1, POINTER_BYTES);
      SArray *tx = taosArrayClone(group);
      taosArrayPush(g1, &tx);
      
      STableGroupInfo gp = {.numOfTables = taosArrayGetSize(tx), .pGroupList = g1};

      // include only current table
      if (pRuntimeEnv->pQueryHandle != NULL) {
        tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
        pRuntimeEnv->pQueryHandle = NULL;
      }
      
      if (isFirstLastRowQuery(pQuery)) {
        pRuntimeEnv->pQueryHandle = tsdbQueryLastRow(pQInfo->tsdb, &cond, &gp, pQInfo);
      } else {
        pRuntimeEnv->pQueryHandle = tsdbQueryRowsInExternalWindow(pQInfo->tsdb, &cond, &gp, pQInfo);
      }

      taosArrayDestroy(tx);
      taosArrayDestroy(g1);
      if (pRuntimeEnv->pQueryHandle == NULL) {
        longjmp(pRuntimeEnv->env, terrno);
      }

      initCtxOutputBuf(pRuntimeEnv);
      
      SArray* s = tsdbGetQueriedTableList(pRuntimeEnv->pQueryHandle);
      assert(taosArrayGetSize(s) >= 1);
      
      setTagVal(pRuntimeEnv, taosArrayGetP(s, 0), pQInfo->tsdb);
      if (isFirstLastRowQuery(pQuery)) {
        assert(taosArrayGetSize(s) == 1);
      }

      taosArrayDestroy(s);

      // here we simply set the first table as current table
      SArray* first = GET_TABLEGROUP(pQInfo, pQInfo->groupIndex);
      pQuery->current = taosArrayGetP(first, 0);

      scanOneTableDataBlocks(pRuntimeEnv, pQuery->current->lastKey);
      
      int64_t numOfRes = getNumOfResult(pRuntimeEnv);
      if (numOfRes > 0) {
        pQuery->rec.rows += numOfRes;
        forwardCtxOutputBuf(pRuntimeEnv, numOfRes);
      }
      
      skipResults(pRuntimeEnv);
      pQInfo->groupIndex += 1;

      // enable execution for next table, when handling the projection query
      enableExecutionForNextTable(pRuntimeEnv);

      if (pQuery->rec.rows >= pQuery->rec.capacity) {
        setQueryStatus(pQuery, QUERY_RESBUF_FULL);
        break;
      }
    }
  } else if (pRuntimeEnv->groupbyNormalCol) { // group-by on normal columns query
    while (pQInfo->groupIndex < numOfGroups) {
      SArray* group = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, pQInfo->groupIndex);

      qDebug("QInfo:%p group by normal columns group:%d, total group:%zu", pQInfo, pQInfo->groupIndex, numOfGroups);

      STsdbQueryCond cond = {
          .twindow = pQuery->window,
          .colList = pQuery->colList,
          .order   = pQuery->order.order,
          .numOfCols = pQuery->numOfCols,
      };

      SArray *g1 = taosArrayInit(1, POINTER_BYTES);
      SArray *tx = taosArrayClone(group);
      taosArrayPush(g1, &tx);

      STableGroupInfo gp = {.numOfTables = taosArrayGetSize(tx), .pGroupList = g1};

      // include only current table
      if (pRuntimeEnv->pQueryHandle != NULL) {
        tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
        pRuntimeEnv->pQueryHandle = NULL;
      }

      pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &gp, pQInfo);
      taosArrayDestroy(g1);
      taosArrayDestroy(tx);
      if (pRuntimeEnv->pQueryHandle == NULL) {
        longjmp(pRuntimeEnv->env, terrno);
      }

      SArray* s = tsdbGetQueriedTableList(pRuntimeEnv->pQueryHandle);
      assert(taosArrayGetSize(s) >= 1);

      setTagVal(pRuntimeEnv, taosArrayGetP(s, 0), pQInfo->tsdb);

      // here we simply set the first table as current table
      scanMultiTableDataBlocks(pQInfo);
      pQInfo->groupIndex += 1;

      SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

        // no results generated for current group, continue to try the next group
      taosArrayDestroy(s); 
      if (pWindowResInfo->size <= 0) {
        continue;
      }

      for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
        SWindowStatus *pStatus = &pWindowResInfo->pResult[i].status;
        pStatus->closed = true;  // enable return all results for group by normal columns

        SWindowResult *pResult = &pWindowResInfo->pResult[i];
        for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
          pResult->numOfRows = MAX(pResult->numOfRows, pResult->resultInfo[j].numOfRes);
        }
      }

      qDebug("QInfo:%p generated groupby columns results %d rows for group %d completed", pQInfo, pWindowResInfo->size,
          pQInfo->groupIndex);
      int32_t currentGroupIndex = pQInfo->groupIndex;

      pQuery->rec.rows = 0;
      pQInfo->groupIndex = 0;

      ensureOutputBufferSimple(pRuntimeEnv, pWindowResInfo->size);
      copyFromWindowResToSData(pQInfo, pWindowResInfo);

      pQInfo->groupIndex = currentGroupIndex;  //restore the group index
      assert(pQuery->rec.rows == pWindowResInfo->size);

      clearClosedTimeWindow(pRuntimeEnv);
      break;
    }
  } else {
    /*
     * 1. super table projection query, 2. ts-comp query
     * if the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (pQInfo->groupIndex > 0) {
      copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
      pQuery->rec.total += pQuery->rec.rows;

      if (pQuery->rec.rows > 0) {
        return;
      }
    }

    // all data have returned already
    if (pQInfo->tableIndex >= pQInfo->tableqinfoGroupInfo.numOfTables) {
      return;
    }

    resetCtxOutputBuf(pRuntimeEnv);
    resetTimeWindowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);

    SArray *group = GET_TABLEGROUP(pQInfo, 0);
    assert(taosArrayGetSize(group) == pQInfo->tableqinfoGroupInfo.numOfTables &&
           1 == taosArrayGetSize(pQInfo->tableqinfoGroupInfo.pGroupList));

    while (pQInfo->tableIndex < pQInfo->tableqinfoGroupInfo.numOfTables) {
      if (IS_QUERY_KILLED(pQInfo)) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
      }

      pQuery->current = taosArrayGetP(group, pQInfo->tableIndex);
      if (!multiTableMultioutputHelper(pQInfo, pQInfo->tableIndex)) {
        pQInfo->tableIndex++;
        continue;
      }

      // TODO handle the limit offset problem
      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
          pQInfo->tableIndex++;
          continue;
        }
      }

      scanOneTableDataBlocks(pRuntimeEnv, pQuery->current->lastKey);
      skipResults(pRuntimeEnv);

      // the limitation of output result is reached, set the query completed
      if (limitResults(pRuntimeEnv)) {
        pQInfo->tableIndex = pQInfo->tableqinfoGroupInfo.numOfTables;
        break;
      }

      // enable execution for next table, when handling the projection query
      enableExecutionForNextTable(pRuntimeEnv);

      if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
        /*
         * query range is identical in terms of all meters involved in query,
         * so we need to restore them at the *beginning* of query on each meter,
         * not the consecutive query on meter on which is aborted due to buffer limitation
         * to ensure that, we can reset the query range once query on a meter is completed.
         */
        pQInfo->tableIndex++;

        STableIdInfo tidInfo = {0};

        STableId* id = TSDB_TABLEID(pQuery->current->pTable);
        tidInfo.uid = id->uid;
        tidInfo.tid = id->tid;
        tidInfo.key = pQuery->current->lastKey;
        taosArrayPush(pQInfo->arrTableIdInfo, &tidInfo);

        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL) /*||
            isGroupbyEachTable(pQuery->pGroupbyExpr, pSupporter->pSidSet)*/) {
          break;
        }

      } else {
        // all data in the result buffer are skipped due to the offset, continue to retrieve data from current meter
        if (pQuery->rec.rows == 0) {
          assert(!Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL));
          continue;
        } else {
          // buffer is full, wait for the next round to retrieve data from current meter
          assert(Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL));
          break;
        }
      }
    }

    if (pQInfo->tableIndex >= pQInfo->tableqinfoGroupInfo.numOfTables) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
    }
  }

  /*
   * 1. super table projection query, group-by on normal columns query, ts-comp query
   * 2. point interpolation query, last row query
   *
   * group-by on normal columns query and last_row query do NOT invoke the finalizer here,
   * since the finalize stage will be done at the client side.
   *
   * projection query, point interpolation query do not need the finalizer.
   *
   * Only the ts-comp query requires the finalizer function to be executed here.
   */
  if (isTSCompQuery(pQuery)) {
    finalizeQueryResult(pRuntimeEnv);
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->cur = pRuntimeEnv->pTSBuf->cur;
  }

  qDebug(
      "QInfo %p numOfTables:%"PRIu64", index:%d, numOfGroups:%zu, %"PRId64" points returned, total:%"PRId64", offset:%" PRId64,
      pQInfo, pQInfo->tableqinfoGroupInfo.numOfTables, pQInfo->tableIndex, numOfGroups, pQuery->rec.rows, pQuery->rec.total,
      pQuery->limit.offset);
}

static void doSaveContext(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SET_REVERSE_SCAN_FLAG(pRuntimeEnv);
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  SWITCH_ORDER(pQuery->order.order);
  
  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pQuery->order.order;
  }
  
  STsdbQueryCond cond = {
      .twindow = pQuery->window,
      .order   = pQuery->order.order,
      .colList = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
  };
  
  // clean unused handle
  if (pRuntimeEnv->pSecQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
  }

  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo);
  if (pRuntimeEnv->pSecQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  switchCtxOrder(pRuntimeEnv);
  disableFuncInReverseScan(pQInfo);
}

static void doRestoreContext(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);

  if (pRuntimeEnv->pTSBuf != NULL) {
    SWITCH_ORDER(pRuntimeEnv->pTSBuf->cur.order);
  }

  switchCtxOrder(pRuntimeEnv);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
}

static void doCloseAllTimeWindowAfterScan(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pQInfo);
    for (int32_t i = 0; i < numOfGroup; ++i) {
      SArray *group = GET_TABLEGROUP(pQInfo, i);

      size_t num = taosArrayGetSize(group);
      for (int32_t j = 0; j < num; ++j) {
        STableQueryInfo* item = taosArrayGetP(group, j);
        closeAllTimeWindow(&item->windowResInfo);
      }
    }
  } else {  // close results for group result
    closeAllTimeWindow(&pQInfo->runtimeEnv.windowResInfo);
  }
}

static void multiTableQueryProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (pQInfo->groupIndex > 0) {
    /*
     * if the groupIndex > 0, the query process must be completed yet, we only need to
     * copy the data into output buffer
     */
    if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
      copyResToQueryResultBuf(pQInfo, pQuery);
#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pRuntimeEnv, pQuery->sdata[0]->num);
#endif
    } else {
      copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
    }

    qDebug("QInfo:%p current:%"PRId64", total:%"PRId64"", pQInfo, pQuery->rec.rows, pQuery->rec.total);
    return;
  }

  qDebug("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", order:%d, forward scan start", pQInfo,
         pQuery->window.skey, pQuery->window.ekey, pQuery->order.order);

  // do check all qualified data blocks
  int64_t el = scanMultiTableDataBlocks(pQInfo);
  qDebug("QInfo:%p master scan completed, elapsed time: %" PRId64 "ms, reverse scan start", pQInfo, el);

  // query error occurred or query is killed, abort current execution
  if (pQInfo->code != TSDB_CODE_SUCCESS || IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:%p query killed or error occurred, code:%s, abort", pQInfo, tstrerror(pQInfo->code));
    finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
    longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  // close all time window results
  doCloseAllTimeWindowAfterScan(pQInfo);

  if (needReverseScan(pQuery)) {
    doSaveContext(pQInfo);

    el = scanMultiTableDataBlocks(pQInfo);
    qDebug("QInfo:%p reversed scan completed, elapsed time: %" PRId64 "ms", pQInfo, el);

//    doCloseAllTimeWindowAfterScan(pQInfo);
    doRestoreContext(pQInfo);
  } else {
    qDebug("QInfo:%p no need to do reversed scan, query completed", pQInfo);
  }

  setQueryStatus(pQuery, QUERY_COMPLETED);

  if (pQInfo->code != TSDB_CODE_SUCCESS || IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:%p query killed or error occurred, code:%s, abort", pQInfo, tstrerror(pQInfo->code));
    finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
    longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  if (QUERY_IS_INTERVAL_QUERY(pQuery) || isSumAvgRateQuery(pQuery)) {
    if (mergeIntoGroupResult(pQInfo) == TSDB_CODE_SUCCESS) {
      copyResToQueryResultBuf(pQInfo, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pRuntimeEnv, pQuery->sdata[0]->num);
#endif
    }
  } else {  // not a interval query
    copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
  }

  // handle the limitation of output buffer
  qDebug("QInfo:%p points returned:%" PRId64 ", total:%" PRId64, pQInfo, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
}

/*
 * in each query, this function will be called only once, no retry for further result.
 *
 * select count(*)/top(field,k)/avg(field name) from table_name [where ts>now-1a];
 * select count(*) from table_name group by status_column;
 */
static void tableFixedOutputProcess(SQInfo *pQInfo, STableQueryInfo* pTableInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (!pRuntimeEnv->topBotQuery && pQuery->limit.offset > 0) {  // no need to execute, since the output will be ignore.
    return;
  }
  
  pQuery->current = pTableInfo;  // set current query table info
  
  scanOneTableDataBlocks(pRuntimeEnv, pTableInfo->lastKey);
  finalizeQueryResult(pRuntimeEnv);

  if (IS_QUERY_KILLED(pQInfo)) {
    finalizeQueryResult(pRuntimeEnv); // clean up allocated resource during query
    longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  // since the numOfRows must be identical for all sql functions that are allowed to be executed simutaneously.
  pQuery->rec.rows = getNumOfResult(pRuntimeEnv);

  skipResults(pRuntimeEnv);
  limitResults(pRuntimeEnv);
}

static void tableMultiOutputProcess(SQInfo *pQInfo, STableQueryInfo* pTableInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  
  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->current = pTableInfo;
  
  // for ts_comp query, re-initialized is not allowed
  if (!isTSCompQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

  // skip blocks without load the actual data block from file if no filter condition present
  skipBlocks(&pQInfo->runtimeEnv);
  if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return;
  }

  while (1) {
    scanOneTableDataBlocks(pRuntimeEnv, pQuery->current->lastKey);
    finalizeQueryResult(pRuntimeEnv);

    pQuery->rec.rows = getNumOfResult(pRuntimeEnv);
    if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols > 0 && pQuery->rec.rows > 0) {
      skipResults(pRuntimeEnv);
    }

    /*
     * 1. if pQuery->size == 0, pQuery->limit.offset >= 0, still need to check data
     * 2. if pQuery->size > 0, pQuery->limit.offset must be 0
     */
    if (pQuery->rec.rows > 0 || Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      break;
    }

    qDebug("QInfo:%p skip current result, offset:%" PRId64 ", next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pQuery->limit.offset, pQuery->current->lastKey, pQuery->current->win.ekey);

    resetCtxOutputBuf(pRuntimeEnv);
  }

  limitResults(pRuntimeEnv);
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    qDebug("QInfo:%p query paused due to output limitation, next qrange:%" PRId64 "-%" PRId64, pQInfo,
        pQuery->current->lastKey, pQuery->window.ekey);
  } else if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    STableIdInfo tidInfo;
    STableId* id = TSDB_TABLEID(pQuery->current->pTable);

    tidInfo.uid = id->uid;
    tidInfo.tid = id->tid;
    tidInfo.key = pQuery->current->lastKey;
    taosArrayPush(pQInfo->arrTableIdInfo, &tidInfo);
  }

  if (!isTSCompQuery(pQuery)) {
    assert(pQuery->rec.rows <= pQuery->rec.capacity);
  }
}

static void tableIntervalProcessImpl(SQueryRuntimeEnv *pRuntimeEnv, TSKEY start) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    scanOneTableDataBlocks(pRuntimeEnv, start);

    assert(!Q_STATUS_EQUAL(pQuery->status, QUERY_NOT_COMPLETED));
    finalizeQueryResult(pRuntimeEnv);

    // here we can ignore the records in case of no interpolation
    // todo handle offset, in case of top/bottom interval query
    if ((pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) && pQuery->limit.offset > 0 &&
        pQuery->fillType == TSDB_FILL_NONE) {
      // maxOutput <= 0, means current query does not generate any results
      int32_t numOfClosed = numOfClosedTimeWindow(&pRuntimeEnv->windowResInfo);

      int32_t c = MIN(numOfClosed, pQuery->limit.offset);
      clearFirstNTimeWindow(pRuntimeEnv, c);
      pQuery->limit.offset -= c;
    }

    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED | QUERY_RESBUF_FULL)) {
      break;
    }
  }
}

// handle time interval query on table
static void tableIntervalProcess(SQInfo *pQInfo, STableQueryInfo* pTableInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &(pQInfo->runtimeEnv);

  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->current = pTableInfo;

  int32_t numOfFilled = 0;
  TSKEY newStartKey = TSKEY_INITIAL_VAL;
  
  // skip blocks without load the actual data block from file if no filter condition present
  skipTimeInterval(pRuntimeEnv, &newStartKey);
  if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0 && pRuntimeEnv->pFillInfo == NULL) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return;
  }

  while (1) {
    tableIntervalProcessImpl(pRuntimeEnv, newStartKey);

    if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
      pQInfo->groupIndex = 0;  // always start from 0
      pQuery->rec.rows = 0;
      copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);

      clearFirstNTimeWindow(pRuntimeEnv, pQInfo->groupIndex);
    }

    // the offset is handled at prepare stage if no interpolation involved
    if (pQuery->fillType == TSDB_FILL_NONE || pQuery->rec.rows == 0) {
      limitResults(pRuntimeEnv);
      break;
    } else {
      taosFillSetStartInfo(pRuntimeEnv->pFillInfo, pQuery->rec.rows, pQuery->window.ekey);
      taosFillCopyInputDataFromFilePage(pRuntimeEnv->pFillInfo, (tFilePage**) pQuery->sdata);
      numOfFilled = 0;
      
      pQuery->rec.rows = doFillGapsInResults(pRuntimeEnv, (tFilePage **)pQuery->sdata, &numOfFilled);
      if (pQuery->rec.rows > 0 || Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
        limitResults(pRuntimeEnv);
        break;
      }

      // no result generated yet, continue retrieve data
      pQuery->rec.rows = 0;
    }
  }

  // all data scanned, the group by normal column can return
  if (pRuntimeEnv->groupbyNormalCol) {  // todo refactor with merge interval time result
    pQInfo->groupIndex = 0;
    pQuery->rec.rows = 0;
    copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
    clearFirstNTimeWindow(pRuntimeEnv, pQInfo->groupIndex);
  }

  pQInfo->pointsInterpo += numOfFilled;
}

static void tableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (queryHasRemainResults(pRuntimeEnv)) {

    if (pQuery->fillType != TSDB_FILL_NONE) {
      /*
       * There are remain results that are not returned due to result interpolation
       * So, we do keep in this procedure instead of launching retrieve procedure for next results.
       */
      int32_t numOfFilled = 0;
      pQuery->rec.rows = doFillGapsInResults(pRuntimeEnv, (tFilePage **)pQuery->sdata, &numOfFilled);

      if (pQuery->rec.rows > 0) {
        limitResults(pRuntimeEnv);
      }

      qDebug("QInfo:%p current:%" PRId64 " returned, total:%" PRId64, pQInfo, pQuery->rec.rows, pQuery->rec.total);
      return;
    } else {
      pQuery->rec.rows = 0;
      pQInfo->groupIndex = 0;  // always start from 0

      if (pRuntimeEnv->windowResInfo.size > 0) {
        copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
        clearFirstNTimeWindow(pRuntimeEnv, pQInfo->groupIndex);

        if (pQuery->rec.rows > 0) {
          qDebug("QInfo:%p %"PRId64" rows returned from group results, total:%"PRId64"", pQInfo, pQuery->rec.rows, pQuery->rec.total);

          // there are not data remains
          if (pRuntimeEnv->windowResInfo.size <= 0) {
            qDebug("QInfo:%p query over, %"PRId64" rows are returned", pQInfo, pQuery->rec.total);
          }

          return;
        }
      }
    }
  }

  // number of points returned during this query
  pQuery->rec.rows = 0;
  int64_t st = taosGetTimestampUs();
  
  assert(pQInfo->tableqinfoGroupInfo.numOfTables == 1);
  SArray* g = GET_TABLEGROUP(pQInfo, 0);
  STableQueryInfo* item = taosArrayGetP(g, 0);
  
  // group by normal column, sliding window query, interval query are handled by interval query processor
  if (QUERY_IS_INTERVAL_QUERY(pQuery) || pRuntimeEnv->groupbyNormalCol) {  // interval (down sampling operation)
    tableIntervalProcess(pQInfo, item);
  } else if (isFixedOutputQuery(pRuntimeEnv)) {
    tableFixedOutputProcess(pQInfo, item);
  } else {  // diff/add/multiply/subtract/division
    assert(pQuery->checkBuffer == 1);
    tableMultiOutputProcess(pQInfo, item);
  }

  // record the total elapsed time
  pRuntimeEnv->summary.elapsedTime += (taosGetTimestampUs() - st);
  assert(pQInfo->tableqinfoGroupInfo.numOfTables == 1);
}

static void stableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->rec.rows = 0;

  int64_t st = taosGetTimestampUs();

  if (QUERY_IS_INTERVAL_QUERY(pQuery) ||
      (isFixedOutputQuery(pRuntimeEnv) && (!isPointInterpoQuery(pQuery)) && !pRuntimeEnv->groupbyNormalCol &&
      !isFirstLastRowQuery(pQuery))) {
    multiTableQueryProcess(pQInfo);
  } else {
    assert((pQuery->checkBuffer == 1 && pQuery->intervalTime == 0) || isPointInterpoQuery(pQuery) ||
            isFirstLastRowQuery(pQuery) || pRuntimeEnv->groupbyNormalCol);

    sequentialTableProcess(pQInfo);

  }

  // record the total elapsed time
  pQInfo->runtimeEnv.summary.elapsedTime += (taosGetTimestampUs() - st);
}

static int32_t getColumnIndexInSource(SQueryTableMsg *pQueryMsg, SSqlFuncMsg *pExprMsg, SColumnInfo* pTagCols) {
  int32_t j = 0;

  if (TSDB_COL_IS_TAG(pExprMsg->colInfo.flag)) {
    if (pExprMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
      return -1;
    }

    while(j < pQueryMsg->numOfTags) {
      if (pExprMsg->colInfo.colId == pTagCols[j].colId) {
        return j;
      }

      j += 1;
    }

  } else {
    while (j < pQueryMsg->numOfCols) {
      if (pExprMsg->colInfo.colId == pQueryMsg->colList[j].colId) {
        return j;
      }

      j += 1;
    }
  }

  assert(0);
}

bool validateExprColumnInfo(SQueryTableMsg *pQueryMsg, SSqlFuncMsg *pExprMsg, SColumnInfo* pTagCols) {
  int32_t j = getColumnIndexInSource(pQueryMsg, pExprMsg, pTagCols);
  return j < pQueryMsg->numOfCols || j < pQueryMsg->numOfTags;
}

static bool validateQueryMsg(SQueryTableMsg *pQueryMsg) {
  if (pQueryMsg->intervalTime < 0) {
    qError("qmsg:%p illegal value of interval time %" PRId64, pQueryMsg, pQueryMsg->intervalTime);
    return false;
  }

  if (pQueryMsg->numOfTables <= 0) {
    qError("qmsg:%p illegal value of numOfTables %d", pQueryMsg, pQueryMsg->numOfTables);
    return false;
  }

  if (pQueryMsg->numOfGroupCols < 0) {
    qError("qmsg:%p illegal value of numOfGroupbyCols %d", pQueryMsg, pQueryMsg->numOfGroupCols);
    return false;
  }

  if (pQueryMsg->numOfOutput > TSDB_MAX_COLUMNS || pQueryMsg->numOfOutput <= 0) {
    qError("qmsg:%p illegal value of output columns %d", pQueryMsg, pQueryMsg->numOfOutput);
    return false;
  }

  return true;
}

static bool validateQuerySourceCols(SQueryTableMsg *pQueryMsg, SSqlFuncMsg** pExprMsg) {
  int32_t numOfTotal = pQueryMsg->numOfCols + pQueryMsg->numOfTags;
  if (pQueryMsg->numOfCols < 0 || pQueryMsg->numOfTags < 0 || numOfTotal > TSDB_MAX_COLUMNS) {
    qError("qmsg:%p illegal value of numOfCols %d numOfTags:%d", pQueryMsg, pQueryMsg->numOfCols, pQueryMsg->numOfTags);
    return false;
  } else if (numOfTotal == 0) {
    for(int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
      SSqlFuncMsg* pFuncMsg = pExprMsg[i];

      if ((pFuncMsg->functionId == TSDB_FUNC_TAGPRJ) ||
          (pFuncMsg->functionId == TSDB_FUNC_TID_TAG && pFuncMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) ||
          (pFuncMsg->functionId == TSDB_FUNC_COUNT && pFuncMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX)) {
        continue;
      }

      return false;
    }
  }

  return true;
}

static char *createTableIdList(SQueryTableMsg *pQueryMsg, char *pMsg, SArray **pTableIdList) {
  assert(pQueryMsg->numOfTables > 0);

  *pTableIdList = taosArrayInit(pQueryMsg->numOfTables, sizeof(STableIdInfo));

  for (int32_t j = 0; j < pQueryMsg->numOfTables; ++j) {
    STableIdInfo* pTableIdInfo = (STableIdInfo *)pMsg;

    pTableIdInfo->tid = htonl(pTableIdInfo->tid);
    pTableIdInfo->uid = htobe64(pTableIdInfo->uid);
    pTableIdInfo->key = htobe64(pTableIdInfo->key);

    taosArrayPush(*pTableIdList, pTableIdInfo);
    pMsg += sizeof(STableIdInfo);
  }

  return pMsg;
}

/**
 * pQueryMsg->head has been converted before this function is called.
 *
 * @param pQueryMsg
 * @param pTableIdList
 * @param pExpr
 * @return
 */
static int32_t convertQueryMsg(SQueryTableMsg *pQueryMsg, SArray **pTableIdList, SSqlFuncMsg ***pExpr,
                               char **tagCond, char** tbnameCond, SColIndex **groupbyCols, SColumnInfo** tagCols) {
  int32_t code = TSDB_CODE_SUCCESS;

  pQueryMsg->numOfTables = htonl(pQueryMsg->numOfTables);

  pQueryMsg->window.skey = htobe64(pQueryMsg->window.skey);
  pQueryMsg->window.ekey = htobe64(pQueryMsg->window.ekey);
  pQueryMsg->intervalTime = htobe64(pQueryMsg->intervalTime);
  pQueryMsg->slidingTime = htobe64(pQueryMsg->slidingTime);
  pQueryMsg->limit = htobe64(pQueryMsg->limit);
  pQueryMsg->offset = htobe64(pQueryMsg->offset);

  pQueryMsg->order = htons(pQueryMsg->order);
  pQueryMsg->orderColId = htons(pQueryMsg->orderColId);
  pQueryMsg->queryType = htonl(pQueryMsg->queryType);
  pQueryMsg->tagNameRelType = htons(pQueryMsg->tagNameRelType);

  pQueryMsg->numOfCols = htons(pQueryMsg->numOfCols);
  pQueryMsg->numOfOutput = htons(pQueryMsg->numOfOutput);
  pQueryMsg->numOfGroupCols = htons(pQueryMsg->numOfGroupCols);
  pQueryMsg->tagCondLen = htons(pQueryMsg->tagCondLen);
  pQueryMsg->tsOffset = htonl(pQueryMsg->tsOffset);
  pQueryMsg->tsLen = htonl(pQueryMsg->tsLen);
  pQueryMsg->tsNumOfBlocks = htonl(pQueryMsg->tsNumOfBlocks);
  pQueryMsg->tsOrder = htonl(pQueryMsg->tsOrder);
  pQueryMsg->numOfTags = htonl(pQueryMsg->numOfTags);

  // query msg safety check
  if (!validateQueryMsg(pQueryMsg)) {
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _cleanup;
  }

  char *pMsg = (char *)(pQueryMsg->colList) + sizeof(SColumnInfo) * pQueryMsg->numOfCols;
  for (int32_t col = 0; col < pQueryMsg->numOfCols; ++col) {
    SColumnInfo *pColInfo = &pQueryMsg->colList[col];

    pColInfo->colId = htons(pColInfo->colId);
    pColInfo->type = htons(pColInfo->type);
    pColInfo->bytes = htons(pColInfo->bytes);
    pColInfo->numOfFilters = htons(pColInfo->numOfFilters);

    assert(pColInfo->type >= TSDB_DATA_TYPE_BOOL && pColInfo->type <= TSDB_DATA_TYPE_NCHAR);

    int32_t numOfFilters = pColInfo->numOfFilters;
    if (numOfFilters > 0) {
      pColInfo->filters = calloc(numOfFilters, sizeof(SColumnFilterInfo));
    }

    for (int32_t f = 0; f < numOfFilters; ++f) {
      SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)pMsg;
      
      SColumnFilterInfo *pColFilter = &pColInfo->filters[f];
      pColFilter->filterstr = htons(pFilterMsg->filterstr);

      pMsg += sizeof(SColumnFilterInfo);

      if (pColFilter->filterstr) {
        pColFilter->len = htobe64(pFilterMsg->len);

        pColFilter->pz = (int64_t) calloc(1, pColFilter->len + 1 * TSDB_NCHAR_SIZE); // note: null-terminator
        memcpy((void *)pColFilter->pz, pMsg, pColFilter->len);
        pMsg += (pColFilter->len + 1);
      } else {
        pColFilter->lowerBndi = htobe64(pFilterMsg->lowerBndi);
        pColFilter->upperBndi = htobe64(pFilterMsg->upperBndi);
      }

      pColFilter->lowerRelOptr = htons(pFilterMsg->lowerRelOptr);
      pColFilter->upperRelOptr = htons(pFilterMsg->upperRelOptr);
    }
  }

  *pExpr = calloc(pQueryMsg->numOfOutput, POINTER_BYTES);
  SSqlFuncMsg *pExprMsg = (SSqlFuncMsg *)pMsg;

  for (int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
    (*pExpr)[i] = pExprMsg;

    pExprMsg->colInfo.colIndex = htons(pExprMsg->colInfo.colIndex);
    pExprMsg->colInfo.colId = htons(pExprMsg->colInfo.colId);
    pExprMsg->colInfo.flag = htons(pExprMsg->colInfo.flag);
    pExprMsg->functionId = htons(pExprMsg->functionId);
    pExprMsg->numOfParams = htons(pExprMsg->numOfParams);

    pMsg += sizeof(SSqlFuncMsg);

    for (int32_t j = 0; j < pExprMsg->numOfParams; ++j) {
      pExprMsg->arg[j].argType = htons(pExprMsg->arg[j].argType);
      pExprMsg->arg[j].argBytes = htons(pExprMsg->arg[j].argBytes);

      if (pExprMsg->arg[j].argType == TSDB_DATA_TYPE_BINARY) {
        pExprMsg->arg[j].argValue.pz = pMsg;
        pMsg += pExprMsg->arg[j].argBytes;  // one more for the string terminated char.
      } else {
        pExprMsg->arg[j].argValue.i64 = htobe64(pExprMsg->arg[j].argValue.i64);
      }
    }

    int16_t functionId = pExprMsg->functionId;
    if (functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_TAG_DUMMY) {
      if (pExprMsg->colInfo.flag != TSDB_COL_TAG) {  // ignore the column  index check for arithmetic expression.
        code = TSDB_CODE_QRY_INVALID_MSG;
        goto _cleanup;
      }
    } else {
//      if (!validateExprColumnInfo(pQueryMsg, pExprMsg)) {
//        return TSDB_CODE_QRY_INVALID_MSG;
//      }
    }

    pExprMsg = (SSqlFuncMsg *)pMsg;
  }

  if (!validateQuerySourceCols(pQueryMsg, *pExpr)) {
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _cleanup;
  }

  pMsg = createTableIdList(pQueryMsg, pMsg, pTableIdList);

  if (pQueryMsg->numOfGroupCols > 0) {  // group by tag columns
    *groupbyCols = malloc(pQueryMsg->numOfGroupCols * sizeof(SColIndex));
    if (*groupbyCols == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    for (int32_t i = 0; i < pQueryMsg->numOfGroupCols; ++i) {
      (*groupbyCols)[i].colId = *(int16_t *)pMsg;
      pMsg += sizeof((*groupbyCols)[i].colId);

      (*groupbyCols)[i].colIndex = *(int16_t *)pMsg;
      pMsg += sizeof((*groupbyCols)[i].colIndex);

      (*groupbyCols)[i].flag = *(int16_t *)pMsg;
      pMsg += sizeof((*groupbyCols)[i].flag);

      memcpy((*groupbyCols)[i].name, pMsg, tListLen(groupbyCols[i]->name));
      pMsg += tListLen((*groupbyCols)[i].name);
    }

    pQueryMsg->orderByIdx = htons(pQueryMsg->orderByIdx);
    pQueryMsg->orderType = htons(pQueryMsg->orderType);
  }

  pQueryMsg->fillType = htons(pQueryMsg->fillType);
  if (pQueryMsg->fillType != TSDB_FILL_NONE) {
    pQueryMsg->fillVal = (uint64_t)(pMsg);

    int64_t *v = (int64_t *)pMsg;
    for (int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
      v[i] = htobe64(v[i]);
    }

    pMsg += sizeof(int64_t) * pQueryMsg->numOfOutput;
  }

  if (pQueryMsg->numOfTags > 0) {
    (*tagCols) = calloc(1, sizeof(SColumnInfo) * pQueryMsg->numOfTags);
    for (int32_t i = 0; i < pQueryMsg->numOfTags; ++i) {
      SColumnInfo* pTagCol = (SColumnInfo*) pMsg;

      pTagCol->colId = htons(pTagCol->colId);
      pTagCol->bytes = htons(pTagCol->bytes);
      pTagCol->type  = htons(pTagCol->type);
      pTagCol->numOfFilters = 0;

      (*tagCols)[i] = *pTagCol;
      pMsg += sizeof(SColumnInfo);
    }
  }

  // the tag query condition expression string is located at the end of query msg
  if (pQueryMsg->tagCondLen > 0) {
    *tagCond = calloc(1, pQueryMsg->tagCondLen);
    memcpy(*tagCond, pMsg, pQueryMsg->tagCondLen);
    pMsg += pQueryMsg->tagCondLen;
  }

  if (*pMsg != 0) {
    size_t len = strlen(pMsg) + 1;

    *tbnameCond = malloc(len);
    if (*tbnameCond == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    strcpy(*tbnameCond, pMsg);
    pMsg += len;
  }

  qDebug("qmsg:%p query %d tables, type:%d, qrange:%" PRId64 "-%" PRId64 ", numOfGroupbyTagCols:%d, order:%d, "
         "outputCols:%d, numOfCols:%d, interval:%" PRId64 ", fillType:%d, comptsLen:%d, compNumOfBlocks:%d, limit:%" PRId64 ", offset:%" PRId64,
         pQueryMsg, pQueryMsg->numOfTables, pQueryMsg->queryType, pQueryMsg->window.skey, pQueryMsg->window.ekey, pQueryMsg->numOfGroupCols,
         pQueryMsg->order, pQueryMsg->numOfOutput, pQueryMsg->numOfCols, pQueryMsg->intervalTime,
         pQueryMsg->fillType, pQueryMsg->tsLen, pQueryMsg->tsNumOfBlocks, pQueryMsg->limit, pQueryMsg->offset);

  return TSDB_CODE_SUCCESS;

_cleanup:
  tfree(*pExpr);
  taosArrayDestroy(*pTableIdList);
  *pTableIdList = NULL;
  tfree(*tbnameCond);
  tfree(*groupbyCols);
  tfree(*tagCols);
  tfree(*tagCond);

  return code;
}

static int32_t buildAirthmeticExprFromMsg(SExprInfo *pArithExprInfo, SQueryTableMsg *pQueryMsg) {
  qDebug("qmsg:%p create arithmetic expr from binary string: %s", pQueryMsg, pArithExprInfo->base.arg[0].argValue.pz);

  tExprNode* pExprNode = NULL;
  TRY(TSDB_MAX_TAGS) {
    pExprNode = exprTreeFromBinary(pArithExprInfo->base.arg[0].argValue.pz, pArithExprInfo->base.arg[0].argBytes);
  } CATCH( code ) {
    CLEANUP_EXECUTE();
    qError("qmsg:%p failed to create arithmetic expression string from:%s, reason: %s", pQueryMsg, pArithExprInfo->base.arg[0].argValue.pz, tstrerror(code));
    return code;
  } END_TRY

  if (pExprNode == NULL) {
    qError("qmsg:%p failed to create arithmetic expression string from:%s", pQueryMsg, pArithExprInfo->base.arg[0].argValue.pz);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  pArithExprInfo->pExpr = pExprNode;
  return TSDB_CODE_SUCCESS;
}

static int32_t createQFunctionExprFromMsg(SQueryTableMsg *pQueryMsg, SExprInfo **pExprInfo, SSqlFuncMsg **pExprMsg,
    SColumnInfo* pTagCols) {
  *pExprInfo = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SExprInfo *pExprs = (SExprInfo *)calloc(pQueryMsg->numOfOutput, sizeof(SExprInfo));
  if (pExprs == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  bool    isSuperTable = QUERY_IS_STABLE_QUERY(pQueryMsg->queryType);
  int16_t tagLen = 0;

  for (int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
    pExprs[i].base = *pExprMsg[i];
    pExprs[i].bytes = 0;

    int16_t type = 0;
    int16_t bytes = 0;

    // parse the arithmetic expression
    if (pExprs[i].base.functionId == TSDB_FUNC_ARITHM) {
      code = buildAirthmeticExprFromMsg(&pExprs[i], pQueryMsg);

      if (code != TSDB_CODE_SUCCESS) {
        tfree(pExprs);
        return code;
      }

      type  = TSDB_DATA_TYPE_DOUBLE;
      bytes = tDataTypeDesc[type].nSize;
    } else if (pExprs[i].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX && pExprs[i].base.functionId == TSDB_FUNC_TAGPRJ) {  // parse the normal column
      SSchema s = tGetTableNameColumnSchema();
      type  = s.type;
      bytes = s.bytes;
    } else{
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].base, pTagCols);
      assert(j < pQueryMsg->numOfCols || j < pQueryMsg->numOfTags);

      if (pExprs[i].base.colInfo.colId != TSDB_TBNAME_COLUMN_INDEX && j >= 0) {
        SColumnInfo* pCol = (TSDB_COL_IS_TAG(pExprs[i].base.colInfo.flag))? &pTagCols[j]:&pQueryMsg->colList[j];
        type = pCol->type;
        bytes = pCol->bytes;
      } else {
        SSchema s = tGetTableNameColumnSchema();

        type  = s.type;
        bytes = s.bytes;
      }
    }

    int32_t param = pExprs[i].base.arg[0].argValue.i64;
    if (getResultDataInfo(type, bytes, pExprs[i].base.functionId, param, &pExprs[i].type, &pExprs[i].bytes,
                          &pExprs[i].interBytes, 0, isSuperTable) != TSDB_CODE_SUCCESS) {
      tfree(pExprs);
      return TSDB_CODE_QRY_INVALID_MSG;
    }

    if (pExprs[i].base.functionId == TSDB_FUNC_TAG_DUMMY || pExprs[i].base.functionId == TSDB_FUNC_TS_DUMMY) {
      tagLen += pExprs[i].bytes;
    }
    assert(isValidDataType(pExprs[i].type));
  }

  // TODO refactor
  for (int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
    pExprs[i].base = *pExprMsg[i];
    int16_t functId = pExprs[i].base.functionId;

    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].base, pTagCols);
      assert(j < pQueryMsg->numOfCols);

      SColumnInfo *pCol = &pQueryMsg->colList[j];

      int32_t ret =
          getResultDataInfo(pCol->type, pCol->bytes, functId, pExprs[i].base.arg[0].argValue.i64,
                            &pExprs[i].type, &pExprs[i].bytes, &pExprs[i].interBytes, tagLen, isSuperTable);
      assert(ret == TSDB_CODE_SUCCESS);
    }
  }
  *pExprInfo = pExprs;

  return TSDB_CODE_SUCCESS;
}

static SSqlGroupbyExpr *createGroupbyExprFromMsg(SQueryTableMsg *pQueryMsg, SColIndex *pColIndex, int32_t *code) {
  if (pQueryMsg->numOfGroupCols == 0) {
    return NULL;
  }

  // using group by tag columns
  SSqlGroupbyExpr *pGroupbyExpr = (SSqlGroupbyExpr *)calloc(1, sizeof(SSqlGroupbyExpr));
  if (pGroupbyExpr == NULL) {
    *code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pGroupbyExpr->numOfGroupCols = pQueryMsg->numOfGroupCols;
  pGroupbyExpr->orderType = pQueryMsg->orderType;
  pGroupbyExpr->orderIndex = pQueryMsg->orderByIdx;

  pGroupbyExpr->columnInfo = taosArrayInit(pQueryMsg->numOfGroupCols, sizeof(SColIndex));
  for(int32_t i = 0; i < pQueryMsg->numOfGroupCols; ++i) {
    taosArrayPush(pGroupbyExpr->columnInfo, &pColIndex[i]);
  }

  return pGroupbyExpr;
}

static int32_t createFilterInfo(void *pQInfo, SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].numOfFilters > 0) {
      pQuery->numOfFilterCols++;
    }
  }

  if (pQuery->numOfFilterCols == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pQuery->pFilterInfo = calloc(1, sizeof(SSingleColumnFilterInfo) * pQuery->numOfFilterCols);

  for (int32_t i = 0, j = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].numOfFilters > 0) {
      SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[j];

      memcpy(&pFilterInfo->info, &pQuery->colList[i], sizeof(SColumnInfo));
      pFilterInfo->info = pQuery->colList[i];

      pFilterInfo->numOfFilters = pQuery->colList[i].numOfFilters;
      pFilterInfo->pFilters = calloc(pFilterInfo->numOfFilters, sizeof(SColumnFilterElem));

      for (int32_t f = 0; f < pFilterInfo->numOfFilters; ++f) {
        SColumnFilterElem *pSingleColFilter = &pFilterInfo->pFilters[f];
        pSingleColFilter->filterInfo = pQuery->colList[i].filters[f];

        int32_t lower = pSingleColFilter->filterInfo.lowerRelOptr;
        int32_t upper = pSingleColFilter->filterInfo.upperRelOptr;

        if (lower == TSDB_RELATION_INVALID && upper == TSDB_RELATION_INVALID) {
          qError("QInfo:%p invalid filter info", pQInfo);
          return TSDB_CODE_QRY_INVALID_MSG;
        }

        int16_t type  = pQuery->colList[i].type;
        int16_t bytes = pQuery->colList[i].bytes;

        // todo refactor
        __filter_func_t *rangeFilterArray = getRangeFilterFuncArray(type);
        __filter_func_t *filterArray = getValueFilterFuncArray(type);

        if (rangeFilterArray == NULL && filterArray == NULL) {
          qError("QInfo:%p failed to get filter function, invalid data type:%d", pQInfo, type);
          return TSDB_CODE_QRY_INVALID_MSG;
        }

        if ((lower == TSDB_RELATION_GREATER_EQUAL || lower == TSDB_RELATION_GREATER) &&
            (upper == TSDB_RELATION_LESS_EQUAL || upper == TSDB_RELATION_LESS)) {
          assert(rangeFilterArray != NULL);
          if (lower == TSDB_RELATION_GREATER_EQUAL) {
            if (upper == TSDB_RELATION_LESS_EQUAL) {
              pSingleColFilter->fp = rangeFilterArray[4];
            } else {
              pSingleColFilter->fp = rangeFilterArray[2];
            }
          } else {
            if (upper == TSDB_RELATION_LESS_EQUAL) {
              pSingleColFilter->fp = rangeFilterArray[3];
            } else {
              pSingleColFilter->fp = rangeFilterArray[1];
            }
          }
        } else {  // set callback filter function
          assert(filterArray != NULL);
          if (lower != TSDB_RELATION_INVALID) {
            pSingleColFilter->fp = filterArray[lower];

            if (upper != TSDB_RELATION_INVALID) {
              qError("pQInfo:%p failed to get filter function, invalid filter condition: %d", pQInfo, type);
              return TSDB_CODE_QRY_INVALID_MSG;
            }
          } else {
            pSingleColFilter->fp = filterArray[upper];
          }
        }
        assert(pSingleColFilter->fp != NULL);
        pSingleColFilter->bytes = bytes;
      }

      j++;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void doUpdateExprColumnIndex(SQuery *pQuery) {
  assert(pQuery->pSelectExpr != NULL && pQuery != NULL);

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    SSqlFuncMsg *pSqlExprMsg = &pQuery->pSelectExpr[k].base;
    if (pSqlExprMsg->functionId == TSDB_FUNC_ARITHM) {
      continue;
    }

    // todo opt performance
    SColIndex *pColIndex = &pSqlExprMsg->colInfo;
    if (!TSDB_COL_IS_TAG(pColIndex->flag)) {
      int32_t f = 0;
      for (f = 0; f < pQuery->numOfCols; ++f) {
        if (pColIndex->colId == pQuery->colList[f].colId) {
          pColIndex->colIndex = f;
          break;
        }
      }
      
      assert (f < pQuery->numOfCols);
    } else {
      int32_t f = 0;
      for (f = 0; f < pQuery->numOfTags; ++f) {
        if (pColIndex->colId == pQuery->tagColList[f].colId) {
          pColIndex->colIndex = f;
          break;
        }
      }
      
      assert(f < pQuery->numOfTags || pColIndex->colId == TSDB_TBNAME_COLUMN_INDEX);
    }
  }
}

static int compareTableIdInfo(const void* a, const void* b) {
  const STableIdInfo* x = (const STableIdInfo*)a;
  const STableIdInfo* y = (const STableIdInfo*)b;
  if (x->uid > y->uid) return 1;
  if (x->uid < y->uid) return -1;
  return 0;
}

static void freeQInfo(SQInfo *pQInfo);

static void calResultBufSize(SQuery* pQuery) {
  const int32_t RESULT_MSG_MIN_SIZE  = 1024 * (1024 + 512);  // bytes
  const int32_t RESULT_MSG_MIN_ROWS  = 8192;
  const float RESULT_THRESHOLD_RATIO = 0.85;

  if (isProjQuery(pQuery)) {
    int32_t numOfRes = RESULT_MSG_MIN_SIZE / pQuery->rowSize;
    if (numOfRes < RESULT_MSG_MIN_ROWS) {
      numOfRes = RESULT_MSG_MIN_ROWS;
    }

    pQuery->rec.capacity  = numOfRes;
    pQuery->rec.threshold = numOfRes * RESULT_THRESHOLD_RATIO;
  } else {  // in case of non-prj query, a smaller output buffer will be used.
    pQuery->rec.capacity = 4096;
    pQuery->rec.threshold = pQuery->rec.capacity * RESULT_THRESHOLD_RATIO;
  }
}

static SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SArray* pTableIdList, SSqlGroupbyExpr *pGroupbyExpr, SExprInfo *pExprs,
                               STableGroupInfo *pTableGroupInfo, SColumnInfo* pTagCols) {
  int16_t numOfCols = pQueryMsg->numOfCols;
  int16_t numOfOutput = pQueryMsg->numOfOutput;

  SQInfo *pQInfo = (SQInfo *)calloc(1, sizeof(SQInfo));
  if (pQInfo == NULL) {
    goto _cleanup_qinfo;
  }

  // to make sure third party won't overwrite this structure
  pQInfo->signature = pQInfo;
  pQInfo->tableGroupInfo = *pTableGroupInfo;

  SQuery *pQuery = calloc(1, sizeof(SQuery));
  if (pQuery == NULL) {
    goto _cleanup_query;
  }
  pQInfo->runtimeEnv.pQuery = pQuery;

  pQuery->numOfCols       = numOfCols;
  pQuery->numOfOutput     = numOfOutput;
  pQuery->limit.limit     = pQueryMsg->limit;
  pQuery->limit.offset    = pQueryMsg->offset;
  pQuery->order.order     = pQueryMsg->order;
  pQuery->order.orderColId = pQueryMsg->orderColId;
  pQuery->pSelectExpr     = pExprs;
  pQuery->pGroupbyExpr    = pGroupbyExpr;
  pQuery->intervalTime    = pQueryMsg->intervalTime;
  pQuery->slidingTime     = pQueryMsg->slidingTime;
  pQuery->slidingTimeUnit = pQueryMsg->slidingTimeUnit;
  pQuery->fillType        = pQueryMsg->fillType;
  pQuery->numOfTags       = pQueryMsg->numOfTags;
  pQuery->tagColList      = pTagCols;

  pQuery->colList = calloc(numOfCols, sizeof(SSingleColumnFilterInfo));
  if (pQuery->colList == NULL) {
    goto _cleanup;
  }

  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i] = pQueryMsg->colList[i];
    pQuery->colList[i].filters = tscFilterInfoClone(pQueryMsg->colList[i].filters, pQuery->colList[i].numOfFilters);
  }

  // calculate the result row size
  for (int16_t col = 0; col < numOfOutput; ++col) {
    assert(pExprs[col].bytes > 0);
    pQuery->rowSize += pExprs[col].bytes;
  }

  doUpdateExprColumnIndex(pQuery);

  int32_t ret = createFilterInfo(pQInfo, pQuery);
  if (ret != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  // prepare the result buffer
  pQuery->sdata = (tFilePage **)calloc(pQuery->numOfOutput, POINTER_BYTES);
  if (pQuery->sdata == NULL) {
    goto _cleanup;
  }

  calResultBufSize(pQuery);

  for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
    assert(pExprs[col].interBytes >= pExprs[col].bytes);

    // allocate additional memory for interResults that are usually larger then final results
    size_t size = (pQuery->rec.capacity + 1) * pExprs[col].bytes + pExprs[col].interBytes + sizeof(tFilePage);
    pQuery->sdata[col] = (tFilePage *)calloc(1, size);
    if (pQuery->sdata[col] == NULL) {
      goto _cleanup;
    }
  }

  if (pQuery->fillType != TSDB_FILL_NONE) {
    pQuery->fillVal = malloc(sizeof(int64_t) * pQuery->numOfOutput);
    if (pQuery->fillVal == NULL) {
      goto _cleanup;
    }

    // the first column is the timestamp
    memcpy(pQuery->fillVal, (char *)pQueryMsg->fillVal, pQuery->numOfOutput * sizeof(int64_t));
  }

  size_t numOfGroups = 0;
  if (pTableGroupInfo->pGroupList != NULL) {
    numOfGroups = taosArrayGetSize(pTableGroupInfo->pGroupList);

    pQInfo->tableqinfoGroupInfo.pGroupList = taosArrayInit(numOfGroups, POINTER_BYTES);
    pQInfo->tableqinfoGroupInfo.numOfTables = pTableGroupInfo->numOfTables;
    pQInfo->tableqinfoGroupInfo.map = taosHashInit(pTableGroupInfo->numOfTables,
                                                   taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  }

  int tableIndex = 0;
  STimeWindow window = pQueryMsg->window;
  taosArraySort(pTableIdList, compareTableIdInfo);

  pQInfo->runtimeEnv.interBufSize = getOutputInterResultBufSize(pQuery);
  pQInfo->pBuf = calloc(pTableGroupInfo->numOfTables, sizeof(STableQueryInfo));
  int32_t index = 0;

  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray* pa = taosArrayGetP(pTableGroupInfo->pGroupList, i);

    size_t s = taosArrayGetSize(pa);
    SArray* p1 = taosArrayInit(s, POINTER_BYTES);
    if (p1 == NULL) {
      goto _cleanup;
    }

    for(int32_t j = 0; j < s; ++j) {
      void* pTable = taosArrayGetP(pa, j);
      STableId* id = TSDB_TABLEID(pTable);

      STableIdInfo* pTableId = taosArraySearch(pTableIdList, id, compareTableIdInfo);
      if (pTableId != NULL ) {
        window.skey = pTableId->key;
      } else {
        window.skey = pQueryMsg->window.skey;
      }

      void* buf = pQInfo->pBuf + index * sizeof(STableQueryInfo);
      STableQueryInfo* item = createTableQueryInfo(&pQInfo->runtimeEnv, pTable, window, buf);
      if (item == NULL) {
        goto _cleanup;
      }
      item->groupIndex = i;
      taosArrayPush(p1, &item);
      taosHashPut(pQInfo->tableqinfoGroupInfo.map, &id->tid, sizeof(id->tid), &item, POINTER_BYTES);
      index += 1;
    }

    taosArrayPush(pQInfo->tableqinfoGroupInfo.pGroupList, &p1);
  }

  pQInfo->arrTableIdInfo = taosArrayInit(tableIndex, sizeof(STableIdInfo));

  pQuery->pos = -1;
  pQuery->window = pQueryMsg->window;

  if (sem_init(&pQInfo->dataReady, 0, 0) != 0) {
    int32_t code = TAOS_SYSTEM_ERROR(errno);
    qError("QInfo:%p init dataReady sem failed, reason:%s", pQInfo, tstrerror(code));
    goto _cleanup;
  }

  colIdCheck(pQuery);

  qDebug("qmsg:%p QInfo:%p created", pQueryMsg, pQInfo);
  return pQInfo;

_cleanup_qinfo:
  tsdbDestroyTableGroup(pTableGroupInfo);

_cleanup_query:
  if (pGroupbyExpr != NULL) {
    taosArrayDestroy(pGroupbyExpr->columnInfo);
    free(pGroupbyExpr);
  }
  tfree(pTagCols);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo = &pExprs[i];
    if (pExprInfo->pExpr != NULL) {
      tExprTreeDestroy(&pExprInfo->pExpr, NULL);
    }
  }
  tfree(pExprs);

_cleanup:
  freeQInfo(pQInfo);
  return NULL;
}

static bool isValidQInfo(void *param) {
  SQInfo *pQInfo = (SQInfo *)param;
  if (pQInfo == NULL) {
    return false;
  }

  /*
   * pQInfo->signature may be changed by another thread, so we assign value of signature
   * into local variable, then compare by using local variable
   */
  uint64_t sig = (uint64_t)pQInfo->signature;
  return (sig == (uint64_t)pQInfo);
}

static int32_t initQInfo(SQueryTableMsg *pQueryMsg, void *tsdb, int32_t vgId, SQInfo *pQInfo, bool isSTable, void* param) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  STSBuf *pTSBuf = NULL;
  if (pQueryMsg->tsLen > 0) {  // open new file to save the result
    char *tsBlock = (char *) pQueryMsg + pQueryMsg->tsOffset;
    pTSBuf = tsBufCreateFromCompBlocks(tsBlock, pQueryMsg->tsNumOfBlocks, pQueryMsg->tsLen, pQueryMsg->tsOrder);

    tsBufResetPos(pTSBuf);
    bool ret = tsBufNextPos(pTSBuf);
    UNUSED(ret);
  }

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.skey > pQuery->window.ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.ekey > pQuery->window.skey))) {
    qDebug("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->window.skey,
           pQuery->window.ekey, pQuery->order.order);
    setQueryStatus(pQuery, QUERY_COMPLETED);
    pQInfo->tableqinfoGroupInfo.numOfTables = 0;

    sem_post(&pQInfo->dataReady);
    return TSDB_CODE_SUCCESS;
  }

  pQInfo->param = param;

  if (pQInfo->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:%p no table qualified for tag filter, abort query", pQInfo);
    setQueryStatus(pQuery, QUERY_COMPLETED);
  
    sem_post(&pQInfo->dataReady);
    return TSDB_CODE_SUCCESS;
  }

  // filter the qualified
  if ((code = doInitQInfo(pQInfo, pTSBuf, tsdb, vgId, isSTable)) != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  
  return code;

_error:
  // table query ref will be decrease during error handling
  freeQInfo(pQInfo);
  return code;
}

static void freeColumnFilterInfo(SColumnFilterInfo* pFilter, int32_t numOfFilters) {
    if (pFilter == NULL) {
      return;
    }
    for (int32_t i = 0; i < numOfFilters; i++) {
      if (pFilter[i].filterstr) {
        free((void*)(pFilter[i].pz));
      }
    }
    free(pFilter);
}

static void freeQInfo(SQInfo *pQInfo) {
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  setQueryKilled(pQInfo);

  qDebug("QInfo:%p start to free QInfo", pQInfo);
  for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
    tfree(pQuery->sdata[col]);
  }

  sem_destroy(&(pQInfo->dataReady));
  teardownQueryRuntimeEnv(&pQInfo->runtimeEnv);

  for (int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
    SSingleColumnFilterInfo *pColFilter = &pQuery->pFilterInfo[i];
    if (pColFilter->numOfFilters > 0) {
      tfree(pColFilter->pFilters);
    }
  }

  if (pQuery->pSelectExpr != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SExprInfo* pExprInfo = &pQuery->pSelectExpr[i];

      if (pExprInfo->pExpr != NULL) {
        tExprTreeDestroy(&pExprInfo->pExpr, NULL);
      }
    }

    tfree(pQuery->pSelectExpr);
  }

  if (pQuery->fillVal != NULL) {
    tfree(pQuery->fillVal);
  }

  // todo refactor, extract method to destroytableDataInfo
  if (pQInfo->tableqinfoGroupInfo.pGroupList != NULL) {
    int32_t numOfGroups = GET_NUM_OF_TABLEGROUP(pQInfo);
    for (int32_t i = 0; i < numOfGroups; ++i) {
      SArray *p = GET_TABLEGROUP(pQInfo, i);

      size_t num = taosArrayGetSize(p);
      for(int32_t j = 0; j < num; ++j) {
        STableQueryInfo* item = taosArrayGetP(p, j);
        destroyTableQueryInfo(item);
      }

      taosArrayDestroy(p);
    }
  }

  tfree(pQInfo->pBuf);
  taosArrayDestroy(pQInfo->tableqinfoGroupInfo.pGroupList);
  taosHashCleanup(pQInfo->tableqinfoGroupInfo.map);
  tsdbDestroyTableGroup(&pQInfo->tableGroupInfo);
  taosArrayDestroy(pQInfo->arrTableIdInfo);
  
  if (pQuery->pGroupbyExpr != NULL) {
    taosArrayDestroy(pQuery->pGroupbyExpr->columnInfo);
    tfree(pQuery->pGroupbyExpr);
  }

  tfree(pQuery->tagColList);
  tfree(pQuery->pFilterInfo);

  if (pQuery->colList != NULL) {
    for (int32_t i = 0; i < pQuery->numOfCols; i++) {
      SColumnInfo* column = pQuery->colList + i;
      freeColumnFilterInfo(column->filters, column->numOfFilters);
    }
    tfree(pQuery->colList);
  }

  tfree(pQuery->sdata);
  tfree(pQuery);
  pQInfo->signature = 0;

  qDebug("QInfo:%p QInfo is freed", pQInfo);

  tfree(pQInfo);
}

static size_t getResultSize(SQInfo *pQInfo, int64_t *numOfRows) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  /*
   * get the file size and set the numOfRows to be the file size, since for tsComp query,
   * the returned row size is equalled to 1
   * TODO handle the case that the file is too large to send back one time
   */
  if (isTSCompQuery(pQuery) && (*numOfRows) > 0) {
    struct stat fstat;
    if (stat(pQuery->sdata[0]->data, &fstat) == 0) {
      *numOfRows = fstat.st_size;
      return fstat.st_size;
    } else {
      qError("QInfo:%p failed to get file info, path:%s, reason:%s", pQInfo, pQuery->sdata[0]->data, strerror(errno));
      return 0;
    }
  } else {
    return pQuery->rowSize * (*numOfRows);
  }
}

static int32_t doDumpQueryResult(SQInfo *pQInfo, char *data) {
  // the remained number of retrieved rows, not the interpolated result
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  // load data from file to msg buffer
  if (isTSCompQuery(pQuery)) {
    int32_t fd = open(pQuery->sdata[0]->data, O_RDONLY, 0666);

    // make sure file exist
    if (FD_VALID(fd)) {
      int32_t s = lseek(fd, 0, SEEK_END);
      UNUSED(s);
      qDebug("QInfo:%p ts comp data return, file:%s, size:%d", pQInfo, pQuery->sdata[0]->data, s);
      if (lseek(fd, 0, SEEK_SET) >= 0) {
        size_t sz = read(fd, data, s);
        UNUSED(sz);
      } else {
        // todo handle error
      }

      close(fd);
      unlink(pQuery->sdata[0]->data);
    } else {
      // todo return the error code to client and handle invalid fd
      qError("QInfo:%p failed to open tmp file to send ts-comp data to client, path:%s, reason:%s", pQInfo,
             pQuery->sdata[0]->data, strerror(errno));
      if (fd != -1) {
        close(fd); 
      }
    }

    // all data returned, set query over
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      setQueryStatus(pQuery, QUERY_OVER);
    }
  } else {
    doCopyQueryResultToMsg(pQInfo, pQuery->rec.rows, data);
  }

  pQuery->rec.total += pQuery->rec.rows;
  qDebug("QInfo:%p current numOfRes rows:%" PRId64 ", total:%" PRId64, pQInfo, pQuery->rec.rows, pQuery->rec.total);

  if (pQuery->limit.limit > 0 && pQuery->limit.limit == pQuery->rec.total) {
    qDebug("QInfo:%p results limitation reached, limitation:%"PRId64, pQInfo, pQuery->limit.limit);
    setQueryStatus(pQuery, QUERY_OVER);
  }
  
  return TSDB_CODE_SUCCESS;
}

typedef struct SQueryMgmt {
  SCacheObj      *qinfoPool;      // query handle pool
  int32_t         vgId;
  bool            closed;
  pthread_mutex_t lock;
} SQueryMgmt;

int32_t qCreateQueryInfo(void* tsdb, int32_t vgId, SQueryTableMsg* pQueryMsg, void* param, qinfo_t* pQInfo) {
  assert(pQueryMsg != NULL && tsdb != NULL);

  int32_t code = TSDB_CODE_SUCCESS;

  char            *tagCond  = NULL;
  char            *tbnameCond = NULL;
  SArray          *pTableIdList = NULL;
  SSqlFuncMsg    **pExprMsg = NULL;
  SExprInfo       *pExprs   = NULL;
  SColIndex       *pGroupColIndex = NULL;
  SColumnInfo     *pTagColumnInfo = NULL;
  SSqlGroupbyExpr *pGroupbyExpr   = NULL;

  code = convertQueryMsg(pQueryMsg, &pTableIdList, &pExprMsg, &tagCond, &tbnameCond, &pGroupColIndex, &pTagColumnInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  if (pQueryMsg->numOfTables <= 0) {
    qError("Invalid number of tables to query, numOfTables:%d", pQueryMsg->numOfTables);
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _over;
  }

  if (pTableIdList == NULL || taosArrayGetSize(pTableIdList) == 0) {
    qError("qmsg:%p, SQueryTableMsg wrong format", pQueryMsg);
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _over;
  }

  if ((code = createQFunctionExprFromMsg(pQueryMsg, &pExprs, pExprMsg, pTagColumnInfo)) != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  pGroupbyExpr = createGroupbyExprFromMsg(pQueryMsg, pGroupColIndex, &code);
  if ((pGroupbyExpr == NULL && pQueryMsg->numOfGroupCols != 0) || code != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  bool isSTableQuery = false;
  STableGroupInfo tableGroupInfo = {0};
  int64_t st = taosGetTimestampUs();

  if (TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_TABLE_QUERY)) {
    STableIdInfo *id = taosArrayGet(pTableIdList, 0);

    qDebug("qmsg:%p query normal table, uid:%"PRId64", tid:%d", pQueryMsg, id->uid, id->tid);
    if ((code = tsdbGetOneTableGroup(tsdb, id->uid, &tableGroupInfo)) != TSDB_CODE_SUCCESS) {
      goto _over;
    }
  } else if (TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_MULTITABLE_QUERY|TSDB_QUERY_TYPE_STABLE_QUERY)) {
    isSTableQuery = true;

    // also note there's possibility that only one table in the super table
    if (!TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_MULTITABLE_QUERY)) {
      STableIdInfo *id = taosArrayGet(pTableIdList, 0);

      // group by normal column, do not pass the group by condition to tsdb to group table into different group
      int32_t numOfGroupByCols = pQueryMsg->numOfGroupCols;
      if (pQueryMsg->numOfGroupCols == 1 && !TSDB_COL_IS_TAG(pGroupColIndex->flag)) {
        numOfGroupByCols = 0;
      }

      qDebug("qmsg:%p query stable, uid:%"PRId64", tid:%d", pQueryMsg, id->uid, id->tid);
      code = tsdbQuerySTableByTagCond(tsdb, id->uid, tagCond, pQueryMsg->tagCondLen, pQueryMsg->tagNameRelType, tbnameCond, &tableGroupInfo, pGroupColIndex,
                                          numOfGroupByCols);
      if (code != TSDB_CODE_SUCCESS) {
        qError("qmsg:%p failed to query stable, reason: %s", pQueryMsg, tstrerror(code));
        goto _over;
      }
    } else {
      code = tsdbGetTableGroupFromIdList(tsdb, pTableIdList, &tableGroupInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _over;
      }

      qDebug("qmsg:%p query on %zu tables in one group from client", pQueryMsg, tableGroupInfo.numOfTables);
    }

    int64_t el = taosGetTimestampUs() - st;
    qDebug("qmsg:%p tag filter completed, numOfTables:%zu, elapsed time:%"PRId64"us", pQueryMsg, tableGroupInfo.numOfTables, el);
  } else {
    assert(0);
  }

  (*pQInfo) = createQInfoImpl(pQueryMsg, pTableIdList, pGroupbyExpr, pExprs, &tableGroupInfo, pTagColumnInfo);
  pExprs = NULL;
  pGroupbyExpr = NULL;
  pTagColumnInfo = NULL;
  
  if ((*pQInfo) == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _over;
  }

  code = initQInfo(pQueryMsg, tsdb, vgId, *pQInfo, isSTableQuery, param);

_over:
  free(tagCond);
  free(tbnameCond);
  free(pGroupColIndex);
  if (pGroupbyExpr != NULL) {
    taosArrayDestroy(pGroupbyExpr->columnInfo);
    free(pGroupbyExpr);
  } 
  free(pTagColumnInfo);
  free(pExprs);
  free(pExprMsg);
  taosArrayDestroy(pTableIdList);

  for (int32_t i = 0; i < pQueryMsg->numOfCols; i++) {
    SColumnInfo* column = pQueryMsg->colList + i;
    freeColumnFilterInfo(column->filters, column->numOfFilters);
  }

  //pQInfo already freed in initQInfo, but *pQInfo may not pointer to null;
  if (code != TSDB_CODE_SUCCESS) {
    *pQInfo = NULL;
  }

  // if failed to add ref for all tables in this query, abort current query
  return code;
}

void qDestroyQueryInfo(qinfo_t qHandle) {
  SQInfo* pQInfo = (SQInfo*) qHandle;
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  qDebug("QInfo:%p query completed", pQInfo);
  queryCostStatis(pQInfo);   // print the query cost summary
  freeQInfo(pQInfo);
}

void qTableQuery(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || pQInfo->signature != pQInfo) {
    qDebug("QInfo:%p has been freed, no need to execute", pQInfo);
    return;
  }

  if (IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:%p it is already killed, abort", pQInfo);
    sem_post(&pQInfo->dataReady);
    return;
  }

  if (pQInfo->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:%p no table exists for query, abort", pQInfo);
    sem_post(&pQInfo->dataReady);
    return;
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pQInfo->runtimeEnv.env);
  if (ret != TSDB_CODE_SUCCESS) {
    pQInfo->code = ret;
    qDebug("QInfo:%p query abort due to error/cancel occurs, code:%s", pQInfo, tstrerror(pQInfo->code));
    sem_post(&pQInfo->dataReady);
    return;
  }

  qDebug("QInfo:%p query task is launched", pQInfo);

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  if (onlyQueryTags(pQInfo->runtimeEnv.pQuery)) {
    assert(pQInfo->runtimeEnv.pQueryHandle == NULL);
    buildTagQueryResult(pQInfo);
  } else if (pQInfo->runtimeEnv.stableQuery) {
    stableQueryImpl(pQInfo);
  } else {
    tableQueryImpl(pQInfo);
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:%p query is killed", pQInfo);
  } else if (pQuery->rec.rows == 0) {
    qDebug("QInfo:%p over, %zu tables queried, %"PRId64" rows are returned", pQInfo, pQInfo->tableqinfoGroupInfo.numOfTables, pQuery->rec.total);
  } else {
    qDebug("QInfo:%p query paused, %" PRId64 " rows returned, numOfTotal:%" PRId64 " rows",
           pQInfo, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
  }

  sem_post(&pQInfo->dataReady);
}

int32_t qRetrieveQueryResultInfo(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  if (IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:%p query is killed, code:%d", pQInfo, pQInfo->code);
    return pQInfo->code;
  }

  sem_wait(&pQInfo->dataReady);
  qDebug("QInfo:%p retrieve result info, rowsize:%d, rows:%"PRId64", code:%d", pQInfo, pQuery->rowSize, pQuery->rec.rows,
         pQInfo->code);

  return pQInfo->code;
}

bool qHasMoreResultsToRetrieve(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (!isValidQInfo(pQInfo) || pQInfo->code != TSDB_CODE_SUCCESS) {
    qDebug("QInfo:%p invalid qhandle or error occurs, abort query, code:%x", pQInfo, pQInfo->code);
    return false;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  bool ret = false;
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_OVER)) {
    ret = false;
  } else if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    ret = true;
  } else if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    ret = true;
  } else {
    assert(0);
  }

  if (ret) {
    qDebug("QInfo:%p has more results waits for client retrieve", pQInfo);
  }

  return ret;
}

int32_t qDumpRetrieveResult(qinfo_t qinfo, SRetrieveTableRsp **pRsp, int32_t *contLen) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  size_t  size = getResultSize(pQInfo, &pQuery->rec.rows);
  size += sizeof(int32_t);
  size += sizeof(STableIdInfo) * taosArrayGetSize(pQInfo->arrTableIdInfo);
  *contLen = size + sizeof(SRetrieveTableRsp);

  // todo proper handle failed to allocate memory,
  // current solution only avoid crash, but cannot return error code to client
  *pRsp = (SRetrieveTableRsp *)rpcMallocCont(*contLen);
  if (*pRsp == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  (*pRsp)->numOfRows = htonl(pQuery->rec.rows);

  int32_t code = pQInfo->code;
  if (code == TSDB_CODE_SUCCESS) {
    (*pRsp)->offset = htobe64(pQuery->limit.offset);
    (*pRsp)->useconds = htobe64(pRuntimeEnv->summary.elapsedTime);
  } else {
    (*pRsp)->offset = 0;
    (*pRsp)->useconds = 0;
  }
  
  (*pRsp)->precision = htons(pQuery->precision);
  if (pQuery->rec.rows > 0 && code == TSDB_CODE_SUCCESS) {
    code = doDumpQueryResult(pQInfo, (*pRsp)->data);
  } else {
    setQueryStatus(pQuery, QUERY_OVER);
    code = pQInfo->code;
  }

  if (IS_QUERY_KILLED(pQInfo) || Q_STATUS_EQUAL(pQuery->status, QUERY_OVER)) {
    (*pRsp)->completed = 1;  // notify no more result to client
  }

  return code;
}

int32_t qKillQuery(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  sem_post(&pQInfo->dataReady);
  setQueryKilled(pQInfo);
  return TSDB_CODE_SUCCESS;
}

static void doSetTagValueToResultBuf(char* output, const char* val, int16_t type, int16_t bytes) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    if (val == NULL) {
      setVardataNull(output, type);
    } else {
      memcpy(output, val, varDataTLen(val));
    }
  } else {
    if (val == NULL) {
      setNull(output, type, bytes);
    } else {  // todo here stop will cause client crash
      memcpy(output, val, bytes);
    }
  }
}

static void buildTagQueryResult(SQInfo* pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pQInfo);
  assert(numOfGroup == 0 || numOfGroup == 1);

  if (numOfGroup == 0) {
    return;
  }
  
  SArray* pa = GET_TABLEGROUP(pQInfo, 0);

  size_t num = taosArrayGetSize(pa);
  assert(num == pQInfo->tableqinfoGroupInfo.numOfTables);

  int32_t count = 0;
  int32_t functionId = pQuery->pSelectExpr[0].base.functionId;
  if (functionId == TSDB_FUNC_TID_TAG) { // return the tags & table Id
    assert(pQuery->numOfOutput == 1);

    SExprInfo* pExprInfo = &pQuery->pSelectExpr[0];
    int32_t rsize = pExprInfo->bytes;
    count = 0;

    int16_t bytes = pExprInfo->bytes;
    int16_t type = pExprInfo->type;

    for(int32_t i = 0; i < pQuery->numOfTags; ++i) {
      if (pQuery->tagColList[i].colId == pExprInfo->base.colInfo.colId) {
        bytes = pQuery->tagColList[i].bytes;
        type = pQuery->tagColList[i].type;
        break;
      }
    }

    while(pQInfo->tableIndex < num && count < pQuery->rec.capacity) {
      int32_t i = pQInfo->tableIndex++;
      STableQueryInfo *item = taosArrayGetP(pa, i);

      char *output = pQuery->sdata[0]->data + i * rsize;
      varDataSetLen(output, rsize - VARSTR_HEADER_SIZE);

      output = varDataVal(output);
      STableId* id = TSDB_TABLEID(item->pTable);

      *(int64_t *)output = id->uid;  // memory align problem, todo serialize
      output += sizeof(id->uid);

      *(int32_t *)output = id->tid;
      output += sizeof(id->tid);

      *(int32_t *)output = pQInfo->vgId;
      output += sizeof(pQInfo->vgId);

      if (pExprInfo->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
        char* data = tsdbGetTableName(item->pTable);
        memcpy(output, data, varDataTLen(data));
      } else {
        char* data = tsdbGetTableTagVal(item->pTable, pExprInfo->base.colInfo.colId, type, bytes);
        doSetTagValueToResultBuf(output, data, type, bytes);
      }

      count += 1;
    }

    qDebug("QInfo:%p create (tableId, tag) info completed, rows:%d", pQInfo, count);

  } else if (functionId == TSDB_FUNC_COUNT) {// handle the "count(tbname)" query
    *(int64_t*) pQuery->sdata[0]->data = num;

    count = 1;
    pQInfo->tableIndex = num;  //set query completed
    qDebug("QInfo:%p create count(tbname) query, res:%d rows:1", pQInfo, count);
  } else {  // return only the tags|table name etc.
    count = 0;
    SSchema tbnameSchema = tGetTableNameColumnSchema();

    int32_t maxNumOfTables = pQuery->rec.capacity;
    if (pQuery->limit.limit >= 0 && pQuery->limit.limit < pQuery->rec.capacity) {
      maxNumOfTables = pQuery->limit.limit;
    }

    while(pQInfo->tableIndex < num && count < maxNumOfTables) {
      int32_t i = pQInfo->tableIndex++;

      // discard current result due to offset
      if (pQuery->limit.offset > 0) {
        pQuery->limit.offset -= 1;
        continue;
      }

      SExprInfo* pExprInfo = pQuery->pSelectExpr;
      STableQueryInfo* item = taosArrayGetP(pa, i);

      char *data = NULL, *dst = NULL;
      int16_t type = 0, bytes = 0;
      for(int32_t j = 0; j < pQuery->numOfOutput; ++j) {

        if (pExprInfo[j].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
          bytes = tbnameSchema.bytes;
          type = tbnameSchema.type;

          data = tsdbGetTableName(item->pTable);
          dst = pQuery->sdata[j]->data + count * tbnameSchema.bytes;
        } else {
          type = pExprInfo[j].type;
          bytes = pExprInfo[j].bytes;
          
          data = tsdbGetTableTagVal(item->pTable, pExprInfo[j].base.colInfo.colId, type, bytes);
          dst = pQuery->sdata[j]->data + count * pExprInfo[j].bytes;

        }

        doSetTagValueToResultBuf(dst, data, type, bytes);
      }
      count += 1;
    }

    qDebug("QInfo:%p create tag values results completed, rows:%d", pQInfo, count);
  }

  pQuery->rec.rows = count;
  setQueryStatus(pQuery, QUERY_COMPLETED);
}

void freeqinfoFn(void *qhandle) {
  void** handle = qhandle;
  if (handle == NULL || *handle == NULL) {
    return;
  }

  qKillQuery(*handle);
  qDestroyQueryInfo(*handle);
}

void* qOpenQueryMgmt(int32_t vgId) {
  const int32_t REFRESH_HANDLE_INTERVAL = 2; // every 2 seconds, refresh handle pool

  char cacheName[128] = {0};
  sprintf(cacheName, "qhandle_%d", vgId);

  SQueryMgmt* pQueryHandle = calloc(1, sizeof(SQueryMgmt));

  pQueryHandle->qinfoPool = taosCacheInit(TSDB_DATA_TYPE_BIGINT, REFRESH_HANDLE_INTERVAL, true, freeqinfoFn, cacheName);
  pQueryHandle->closed    = false;
  pthread_mutex_init(&pQueryHandle->lock, NULL);

  qDebug("vgId:%d, open querymgmt success", vgId);
  return pQueryHandle;
}

static void queryMgmtKillQueryFn(void* handle) {
  void** fp = (void**)handle;
  qKillQuery(*fp);
}

void qQueryMgmtNotifyClosed(void* pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  SQueryMgmt* pQueryMgmt = pQMgmt;
  qDebug("vgId:%d, set querymgmt closed, wait for all queries cancelled", pQueryMgmt->vgId);

  pthread_mutex_lock(&pQueryMgmt->lock);
  pQueryMgmt->closed = true;
  pthread_mutex_unlock(&pQueryMgmt->lock);

  taosCacheRefresh(pQueryMgmt->qinfoPool, queryMgmtKillQueryFn);
}

void qCleanupQueryMgmt(void* pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  SQueryMgmt* pQueryMgmt = pQMgmt;
  int32_t vgId = pQueryMgmt->vgId;

  assert(pQueryMgmt->closed);

  SCacheObj* pqinfoPool = pQueryMgmt->qinfoPool;
  pQueryMgmt->qinfoPool = NULL;

  taosCacheCleanup(pqinfoPool);
  pthread_mutex_destroy(&pQueryMgmt->lock);
  tfree(pQueryMgmt);

  qDebug("vgId:%d querymgmt cleanup completed", vgId);
}

void** qRegisterQInfo(void* pMgmt, uint64_t qInfo) {
  if (pMgmt == NULL) {
    return NULL;
  }

  const int32_t DEFAULT_QHANDLE_LIFE_SPAN = tsShellActivityTimer * 2;

  SQueryMgmt *pQueryMgmt = pMgmt;
  if (pQueryMgmt->qinfoPool == NULL) {
    qError("QInfo:%p failed to add qhandle into qMgmt, since qMgmt is closed", (void *)qInfo);
    return NULL;
  }

  pthread_mutex_lock(&pQueryMgmt->lock);
  if (pQueryMgmt->closed) {
    pthread_mutex_unlock(&pQueryMgmt->lock);
    qError("QInfo:%p failed to add qhandle into cache, since qMgmt is colsing", (void *)qInfo);
    return NULL;
  } else {
    uint64_t handleVal = (uint64_t) qInfo;

    void** handle = taosCachePut(pQueryMgmt->qinfoPool, &handleVal, sizeof(int64_t), &qInfo, POINTER_BYTES, DEFAULT_QHANDLE_LIFE_SPAN);
    pthread_mutex_unlock(&pQueryMgmt->lock);

    return handle;
  }
}

void** qAcquireQInfo(void* pMgmt, uint64_t key) {
  SQueryMgmt *pQueryMgmt = pMgmt;

  if (pQueryMgmt->qinfoPool == NULL || pQueryMgmt->closed) {
    return NULL;
  }

  void** handle = taosCacheAcquireByKey(pQueryMgmt->qinfoPool, &key, sizeof(uint64_t));
  if (handle == NULL || *handle == NULL) {
    return NULL;
  } else {
    return handle;
  }
}

void** qReleaseQInfo(void* pMgmt, void* pQInfo, bool needFree) {
  SQueryMgmt *pQueryMgmt = pMgmt;

  if (pQueryMgmt->qinfoPool == NULL) {
    return NULL;
  }

  taosCacheRelease(pQueryMgmt->qinfoPool, pQInfo, needFree);
  return 0;
}

