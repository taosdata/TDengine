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

#include "hash.h"
#include "hashfunc.h"
#include "qast.h"
#include "qresultBuf.h"
#include "query.h"
#include "queryExecutor.h"
#include "queryLog.h"
#include "queryUtil.h"
#include "taosmsg.h"
#include "tlosertree.h"
#include "tscompression.h"
#include "ttime.h"
#include "tscUtil.h"   // todo move the function to common module
#include "tdataformat.h"

#define DEFAULT_INTERN_BUF_SIZE 16384L

/**
 * check if the primary column is load by default, otherwise, the program will
 * forced to load primary column explicitly.
 */
#define Q_STATUS_EQUAL(p, s) (((p) & (s)) != 0)
#define TSDB_COL_IS_TAG(f) (((f)&TSDB_COL_TAG) != 0)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define IS_MASTER_SCAN(runtime) (((runtime)->scanFlag & 1u) == MASTER_SCAN)
#define IS_SUPPLEMENT_SCAN(runtime) ((runtime)->scanFlag == SUPPLEMENTARY_SCAN)
#define SET_SUPPLEMENT_SCAN_FLAG(runtime) ((runtime)->scanFlag = SUPPLEMENTARY_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime) ((runtime)->scanFlag = MASTER_SCAN)

#define GET_QINFO_ADDR(x) ((void *)((char *)(x)-offsetof(SQInfo, runtimeEnv)))

#define GET_COL_DATA_POS(query, index, step) ((query)->pos + (index) * (step))
#define SWITCH_ORDER(n) (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

/* get the qinfo struct address from the query struct address */
#define GET_COLUMN_BYTES(query, colidx) \
  ((query)->colList[(query)->pSelectExpr[colidx].base.colInfo.colIndex].bytes)
#define GET_COLUMN_TYPE(query, colidx) ((query)->colList[(query)->pSelectExpr[colidx].base.colInfo.colIndex].type)

typedef struct SPointInterpoSupporter {
  int32_t numOfCols;
  SArray* prev;
  SArray* next;
} SPointInterpoSupporter;

typedef enum {
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
} vnodeQueryStatus;

enum {
  TS_JOIN_TS_EQUAL = 0,
  TS_JOIN_TS_NOT_EQUALS = 1,
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

typedef struct SGroupItem {
  STableId id;
  STableQueryInfo* info;
} SGroupItem;

static void setQueryStatus(SQuery *pQuery, int8_t status);
static bool isIntervalQuery(SQuery *pQuery) { return pQuery->intervalTime > 0; }

// todo move to utility
static int32_t mergeIntoGroupResultImpl(SQInfo *pQInfo, SArray *group);

static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult);
static void resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx, SResultInfo *pResultInfo);
static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);
static void getNextTimeWindow(SQuery *pQuery, STimeWindow *pTimeWindow);

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void *inputData, TSKEY *tsCol, int32_t size,
                          int32_t functionId, SDataStatis *pStatis, bool hasNull, void *param, int32_t scanFlag);
static void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static void destroyTableQueryInfo(STableQueryInfo *pTableQueryInfo, int32_t numOfCols);
static void resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static bool hasMainOutput(SQuery *pQuery);
static void createTableQueryInfo(SQInfo *pQInfo);
static void buildTagQueryResult(SQInfo *pQInfo);

static int32_t setAdditionalInfo(SQInfo *pQInfo, STableId *pTaleId, STableQueryInfo *pTableQueryInfo);
static int32_t flushFromResultBuf(SQInfo *pQInfo);

bool getNeighborPoints(SQInfo *pQInfo, void *pMeterObj, SPointInterpoSupporter *pPointInterpSupporter) {
#if 0
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (!isPointInterpoQuery(pQuery)) {
    return false;
  }

  /*
   * for interpolate point query, points that are directly before/after the specified point are required
   */
  if (isFirstLastRowQuery(pQuery)) {
    assert(!QUERY_IS_ASC_QUERY(pQuery));
  } else {
    assert(QUERY_IS_ASC_QUERY(pQuery));
  }
  assert(pPointInterpSupporter != NULL && pQuery->skey == pQuery->ekey);

  SCacheBlock *pBlock = NULL;

  qTrace("QInfo:%p get next data point, fileId:%d, slot:%d, pos:%d", GET_QINFO_ADDR(pQuery), pQuery->fileId,
         pQuery->slot, pQuery->pos);

  // save the point that is directly after or equals to the specified point
  getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pNextPoint, pQuery->pos);

  /*
   * 1. for last_row query, return immediately.
   * 2. the specified timestamp equals to the required key, interpolation according to neighbor points is not necessary
   *    for interp query.
   */
  TSKEY actualKey = *(TSKEY *)pPointInterpSupporter->pNextPoint[0];
  if (isFirstLastRowQuery(pQuery) || actualKey == pQuery->skey) {
    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

    /*
     * the retrieved ts may not equals to pMeterObj->lastKey due to cache re-allocation
     * set the pQuery->ekey/pQuery->skey/pQuery->lastKey to be the new value.
     */
    if (pQuery->ekey != actualKey) {
      pQuery->skey = actualKey;
      pQuery->ekey = actualKey;
      pQuery->lastKey = actualKey;
      pSupporter->rawSKey = actualKey;
      pSupporter->rawEKey = actualKey;
    }
    return true;
  }

  /* the qualified point is not the first point in data block */
  if (pQuery->pos > 0) {
    int32_t prevPos = pQuery->pos - 1;

    /* save the point that is directly after the specified point */
    getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, prevPos);
  } else {
    __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

//    savePointPosition(&pRuntimeEnv->startPos, pQuery->fileId, pQuery->slot, pQuery->pos);

    // backwards movement would not set the pQuery->pos correct. We need to set it manually later.
    moveToNextBlock(pRuntimeEnv, QUERY_DESC_FORWARD_STEP, searchFn, true);

    /*
     * no previous data exists.
     * reset the status and load the data block that contains the qualified point
     */
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
      qTrace("QInfo:%p no previous data block, start fileId:%d, slot:%d, pos:%d, qrange:%" PRId64 "-%" PRId64
             ", out of range",
             GET_QINFO_ADDR(pQuery), pRuntimeEnv->startPos.fileId, pRuntimeEnv->startPos.slot,
             pRuntimeEnv->startPos.pos, pQuery->skey, pQuery->ekey);

      // no result, return immediately
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return false;
    } else {  // prev has been located
      if (pQuery->fileId >= 0) {
        pQuery->pos = pQuery->pBlock[pQuery->slot].numOfPoints - 1;
        getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, pQuery->pos);

        qTrace("QInfo:%p get prev data point, fileId:%d, slot:%d, pos:%d, pQuery->pos:%d", GET_QINFO_ADDR(pQuery),
               pQuery->fileId, pQuery->slot, pQuery->pos, pQuery->pos);
      } else {
        // moveToNextBlock make sure there is a available cache block, if exists
        assert(vnodeIsDatablockLoaded(pRuntimeEnv, pMeterObj, -1, true) == DISK_BLOCK_NO_NEED_TO_LOAD);
        pBlock = &pRuntimeEnv->cacheBlock;

        pQuery->pos = pBlock->numOfPoints - 1;
        getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, pQuery->pos);

        qTrace("QInfo:%p get prev data point, fileId:%d, slot:%d, pos:%d, pQuery->pos:%d", GET_QINFO_ADDR(pQuery),
               pQuery->fileId, pQuery->slot, pBlock->numOfPoints - 1, pQuery->pos);
      }
    }
  }

  pQuery->skey = *(TSKEY *)pPointInterpSupporter->pPrevPoint[0];
  pQuery->ekey = *(TSKEY *)pPointInterpSupporter->pNextPoint[0];
  pQuery->lastKey = pQuery->skey;
#endif
  return true;
}

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
      /*
       * make sure the normal column locates at the second position if tbname exists in group by clause
       */
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

bool isTSCompQuery(SQuery *pQuery) { return pQuery->pSelectExpr[0].base.functionId == TSDB_FUNC_TS_COMP; }

static bool limitResults(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if ((pQuery->limit.limit > 0) && (pQuery->rec.total + pQuery->rec.rows > pQuery->limit.limit)) {
    pQuery->rec.rows = pQuery->limit.limit - pQuery->rec.total;
    assert(pQuery->rec.rows > 0);

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

static SDataStatis *getStatisInfo(SQuery *pQuery, SDataStatis *pStatis, SDataBlockInfo *pDataBlockInfo, int32_t index) {
  // for a tag column, no corresponding field info
  SColIndex *pColIndexEx = &pQuery->pSelectExpr[index].base.colInfo;
  if (TSDB_COL_IS_TAG(pColIndexEx->flag)) {
    return NULL;
  }

  /*
   * Choose the right column field info by field id, since the file block may be out of date,
   * which means the newest table schema is not equalled to the schema of this block.
   */
  for (int32_t i = 0; i < pDataBlockInfo->numOfCols; ++i) {
    if (pColIndexEx->colId == pStatis[i].colId) {
      return &pStatis[i];
    }
  }

  return NULL;
}

/**
 * @param pQuery
 * @param col
 * @param pDataBlockInfo
 * @param pStatis
 * @param pColStatis
 * @return
 */
static bool hasNullValue(SQuery *pQuery, int32_t col, SDataBlockInfo *pDataBlockInfo, SDataStatis *pStatis,
                         SDataStatis **pColStatis) {
  SColIndex *pColIndex = &pQuery->pSelectExpr[col].base.colInfo;
  if (TSDB_COL_IS_TAG(pColIndex->flag)) {
    return false;
  }

  // query on primary timestamp column, not null value at all
  if (pColIndex->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return false;
  }

  *pColStatis = NULL;
  if (pStatis != NULL) {
    *pColStatis = getStatisInfo(pQuery, pStatis, pDataBlockInfo, col);
  }

  if ((*pColStatis) != NULL && (*pColStatis)->numOfNull == 0) {
    return false;
  }

  return true;
}

static SWindowResult *doSetTimeWindowFromKey(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo, char *pData,
                                             int16_t bytes) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t *p1 = (int32_t *)taosHashGet(pWindowResInfo->hashList, pData, bytes);
  if (p1 != NULL) {
    pWindowResInfo->curIndex = *p1;
  } else {  // more than the capacity, reallocate the resources
    if (pWindowResInfo->size >= pWindowResInfo->capacity) {
      int64_t newCap = pWindowResInfo->capacity * 2;

      char *t = realloc(pWindowResInfo->pResult, newCap * sizeof(SWindowResult));
      if (t != NULL) {
        pWindowResInfo->pResult = (SWindowResult *)t;
        memset(&pWindowResInfo->pResult[pWindowResInfo->capacity], 0, sizeof(SWindowResult) * pWindowResInfo->capacity);
      } else {
        // todo
      }

      for (int32_t i = pWindowResInfo->capacity; i < newCap; ++i) {
        SPosInfo pos = {-1, -1};
        createQueryResultInfo(pQuery, &pWindowResInfo->pResult[i], pRuntimeEnv->stableQuery, &pos);
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

  assert(ts >= w.skey && ts <= w.ekey && w.skey != 0);

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

  if (list.size == 0) {
    pData = getNewDataBuf(pResultBuf, sid, &pageId);
  } else {
    pageId = getLastPageId(&list);
    pData = getResultBufferPageById(pResultBuf, pageId);

    if (pData->numOfElems >= numOfRowsPerPage) {
      pData = getNewDataBuf(pResultBuf, sid, &pageId);
      if (pData != NULL) {
        assert(pData->numOfElems == 0);  // number of elements must be 0 for new allocated buffer
      }
    }
  }

  if (pData == NULL) {
    return -1;
  }

  // set the number of rows in current disk page
  if (pWindowRes->pos.pageId == -1) {  // not allocated yet, allocate new buffer
    pWindowRes->pos.pageId = pageId;
    pWindowRes->pos.rowId = pData->numOfElems++;
  }

  return 0;
}

static int32_t setWindowOutputBufByKey(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo, int32_t sid,
                                       STimeWindow *win) {
  assert(win->skey <= win->ekey);
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&win->skey, TSDB_KEYSIZE);
  if (pWindowRes == NULL) {
    return -1;
  }

  // not assign result buffer yet, add new result buffer
  if (pWindowRes->pos.pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pWindowRes, pResultBuf, sid, pRuntimeEnv->numOfRowsPerPage);
    if (ret != 0) {
      return -1;
    }
  }

  // set time window for current result
  pWindowRes->window = *win;

  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);

  return TSDB_CODE_SUCCESS;
}

static SWindowStatus *getTimeWindowResStatus(SWindowResInfo *pWindowResInfo, int32_t slot) {
  assert(slot >= 0 && slot < pWindowResInfo->size);
  return &pWindowResInfo->pResult[slot].status;
}

static int32_t getForwardStepsInBlock(int32_t numOfPoints, __block_search_fn_t searchFn, TSKEY ekey, int16_t pos,
                                      int16_t order, int64_t *pData) {
  int32_t endPos = searchFn((char *)pData, numOfPoints, ekey, order);
  int32_t forwardStep = 0;

  if (endPos >= 0) {
    forwardStep = (order == TSDB_ORDER_ASC) ? (endPos - pos) : (pos - endPos);
    assert(forwardStep >= 0);

    // endPos data is equalled to the key so, we do need to read the element in endPos
    if (pData[endPos] == ekey) {
      forwardStep += 1;
    }
  }

  return forwardStep;
}

/**
 * NOTE: the query status only set for the first scan of master scan.
 */
static void doCheckQueryCompleted(SQueryRuntimeEnv *pRuntimeEnv, TSKEY lastKey, SWindowResInfo *pWindowResInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pRuntimeEnv->scanFlag != MASTER_SCAN || (!isIntervalQuery(pQuery))) {
    return;
  }

  // no qualified results exist, abort check
  if (pWindowResInfo->size == 0) {
    return;
  }

  // query completed
  if ((lastKey >= pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (lastKey <= pQuery->window.ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    closeAllTimeWindow(pWindowResInfo);

    pWindowResInfo->curIndex = pWindowResInfo->size - 1;
    setQueryStatus(pQuery, QUERY_COMPLETED | QUERY_RESBUF_FULL);
  } else {  // set the current index to be the last unclosed window
    int32_t i = 0;
    int64_t skey = 0;

    for (i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *pResult = &pWindowResInfo->pResult[i];
      if (pResult->status.closed) {
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
    if (skey == 0) {
      assert(i == pWindowResInfo->size);
      pWindowResInfo->curIndex = pWindowResInfo->size - 1;
    } else {
      pWindowResInfo->curIndex = i;
    }

    pWindowResInfo->prevSKey = pWindowResInfo->pResult[pWindowResInfo->curIndex].window.skey;

    // the number of completed slots are larger than the threshold, dump to client immediately.
    int32_t n = numOfClosedTimeWindow(pWindowResInfo);
    if (n > pWindowResInfo->threshold) {
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
    }

    qTrace("QInfo:%p total window:%d, closed:%d", GET_QINFO_ADDR(pQuery), pWindowResInfo->size, n);
  }

  assert(pWindowResInfo->prevSKey != 0);
}

static int32_t getNumOfRowsInTimeWindow(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo, TSKEY *pPrimaryColumn,
                                        int32_t startPos, TSKEY ekey, __block_search_fn_t searchFn,
                                        bool updateLastKey) {
  assert(startPos >= 0 && startPos < pDataBlockInfo->rows);

  int32_t num = -1;
  int32_t order = pQuery->order.order;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (ekey < pDataBlockInfo->window.ekey) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (num == 0) {  // no qualified data in current block, do not update the lastKey value
        assert(ekey < pPrimaryColumn[startPos]);
      } else {
        if (updateLastKey) {
          pQuery->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
        }
      }
    } else {
      num = pDataBlockInfo->rows - startPos;
      if (updateLastKey) {
        pQuery->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  } else {  // desc
    if (ekey > pDataBlockInfo->window.skey) {
      num = getForwardStepsInBlock(pDataBlockInfo->rows, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (num == 0) {  // no qualified data in current block, do not update the lastKey value
        assert(ekey > pPrimaryColumn[startPos]);
      } else {
        if (updateLastKey) {
          pQuery->lastKey = pPrimaryColumn[startPos - (num - 1)] + step;
        }
      }
    } else {
      num = startPos + 1;
      if (updateLastKey) {
        pQuery->lastKey = pDataBlockInfo->window.skey + step;
      }
    }
  }

  assert(num >= 0);
  return num;
}

static void doBlockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SWindowStatus *pStatus, STimeWindow *pWin,
                                      int32_t startPos, int32_t forwardStep, TSKEY *tsBuf) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  if (IS_MASTER_SCAN(pRuntimeEnv) || pStatus->closed) {
    for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].base.functionId;

      pCtx[k].nStartQueryTimestamp = pWin->skey;
      pCtx[k].size = forwardStep;
      pCtx[k].startOffset = (QUERY_IS_ASC_QUERY(pQuery)) ? startPos : startPos - (forwardStep - 1);

      if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
        pCtx[k].ptsList = &tsBuf[pCtx[k].startOffset];
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

static int32_t getNextQualifiedWindow(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pNextWin,
                                      SDataBlockInfo *pDataBlockInfo, TSKEY *primaryKeys,
                                      __block_search_fn_t searchFn) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    if ((pNextWin->ekey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pNextWin->skey < pQuery->window.ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
      return -1;
    }

    getNextTimeWindow(pQuery, pNextWin);

    // next time window is not in current block
    if ((pNextWin->skey > pDataBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pNextWin->ekey < pDataBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
      return -1;
    }

    TSKEY startKey = -1;
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      startKey = pNextWin->skey;
      if (startKey < pQuery->window.skey) {
        startKey = pQuery->window.skey;
      }
    } else {
      startKey = pNextWin->ekey;
      if (startKey > pQuery->window.skey) {
        startKey = pQuery->window.skey;
      }
    }

    int32_t startPos = searchFn((char *)primaryKeys, pDataBlockInfo->rows, startKey, pQuery->order.order);

    /*
     * This time window does not cover any data, try next time window,
     * this case may happen when the time window is too small
     */
    if ((primaryKeys[startPos] > pNextWin->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (primaryKeys[startPos] < pNextWin->skey && !QUERY_IS_ASC_QUERY(pQuery))) {
      continue;
    }

    return startPos;
  }
}

static TSKEY reviseWindowEkey(SQuery *pQuery, STimeWindow *pWindow) {
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

char *getDataBlocks(SQueryRuntimeEnv *pRuntimeEnv, SArithmeticSupport *sas, int32_t col, int32_t size,
                    SArray *pDataBlock) {
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
    
    // here the pQuery->colList and sas->colList are identical
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      SColumnInfo *pColMsg = &pQuery->colList[i];
  
      int32_t numOfCols = taosArrayGetSize(pDataBlock);
      
      dataBlock = NULL;
      for (int32_t k = 0; k < numOfCols; ++k) {  //todo refactor
        SColumnInfoData *p = taosArrayGet(pDataBlock, k);
        if (pColMsg->colId == p->info.colId) {
          dataBlock = p->pData;
          break;
        }
      }
      
      assert(dataBlock != NULL);
      sas->data[i] = dataBlock + pCtx->startOffset * pQuery->colList[i].bytes;  // start from the offset
    }

  } else {  // other type of query function
    SColIndex *pCol = &pQuery->pSelectExpr[col].base.colInfo;
    if (TSDB_COL_IS_TAG(pCol->flag) || pDataBlock == NULL) {
      dataBlock = NULL;
    } else {
      /*
       *  the colIndex is acquired from the first meter of all qualified meters in this vnode during query prepare
       * stage, the remain meter may not have the required column in cache actually. So, the validation of required
       * column in cache with the corresponding meter schema is reinforced.
       */
      int32_t numOfCols = taosArrayGetSize(pDataBlock);
      
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData *p = taosArrayGet(pDataBlock, i);
        if (pCol->colId == p->info.colId) {
          dataBlock = p->pData;
          break;
        }
      }
    }
  }

  return dataBlock;
}

/**
 *
 * @param pRuntimeEnv
 * @param forwardStep
 * @param primaryKeyCol
 * @param pFields
 * @param isDiskFileBlock
 * @return                  the incremental number of output value, so it maybe 0 for fixed number of query,
 *                          such as count/min/max etc.
 */
static void blockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis,
                                       SDataBlockInfo *pDataBlockInfo, SWindowResInfo *pWindowResInfo,
                                       __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;

  SColumnInfoData *pColInfo = NULL;
  TSKEY *          primaryKeyCol = NULL;

  if (pDataBlock != NULL) {
    pColInfo = taosArrayGet(pDataBlock, 0);
    primaryKeyCol = (TSKEY *)(pColInfo->pData);
  }

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutput, sizeof(SArithmeticSupport));
  
  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].base.functionId;

    SDataStatis *tpField = NULL;

    bool  hasNull = hasNullValue(pQuery, k, pDataBlockInfo, pStatis, &tpField);
    char *dataBlock = getDataBlocks(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);

    setExecParams(pQuery, &pCtx[k], dataBlock, primaryKeyCol, pDataBlockInfo->rows, functionId, tpField, hasNull,
                  &sasArray[k], pRuntimeEnv->scanFlag);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (isIntervalQuery(pQuery)) {
    int32_t offset = GET_COL_DATA_POS(pQuery, 0, step);
    TSKEY   ts = primaryKeyCol[offset];

    STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);
    if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &win) != TSDB_CODE_SUCCESS) {
      return;
    }

    TSKEY   ekey = reviseWindowEkey(pQuery, &win);
    int32_t forwardStep =
        getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, primaryKeyCol, pQuery->pos, ekey, searchFn, true);

    SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
    doBlockwiseApplyFunctions(pRuntimeEnv, pStatus, &win, pQuery->pos, forwardStep, primaryKeyCol);

    int32_t     index = pWindowResInfo->curIndex;
    STimeWindow nextWin = win;

    while (1) {
      int32_t startPos = getNextQualifiedWindow(pRuntimeEnv, &nextWin, pDataBlockInfo, primaryKeyCol, searchFn);
      if (startPos < 0) {
        break;
      }

      // null data, failed to allocate more memory buffer
      if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &nextWin) != TSDB_CODE_SUCCESS) {
        break;
      }

      ekey = reviseWindowEkey(pQuery, &nextWin);
      forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, primaryKeyCol, startPos, ekey, searchFn, true);

      pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doBlockwiseApplyFunctions(pRuntimeEnv, pStatus, &nextWin, startPos, forwardStep, primaryKeyCol);
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

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, pData, bytes);
  if (pWindowRes == NULL) {
    return -1;
  }

  // not assign result buffer yet, add new result buffer
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
     *  the colIndex is acquired from the first meter of all qualified meters in this vnode during query prepare
     * stage, the remain meter may not have the required column in cache actually. So, the validation of required
     * column in cache with the corresponding meter schema is reinforced.
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

  if (pResInfo->complete || functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
    return false;
  }

  // in the supplementary scan, only the following functions need to be executed
  if (IS_SUPPLEMENT_SCAN(pRuntimeEnv) &&
      !(functionId == TSDB_FUNC_LAST_DST || functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_FIRST ||
        functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS)) {
    return false;
  }

  return true;
}

static void rowwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis, SDataBlockInfo *pDataBlockInfo,
    SWindowResInfo *pWindowResInfo, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  
  SQuery *pQuery = pRuntimeEnv->pQuery;
  TSKEY  *primaryKeyCol = (TSKEY*) ((SColumnInfoData *)taosArrayGet(pDataBlock, 0))->pData;

  bool groupbyStateValue = isGroupbyNormalCol(pQuery->pGroupbyExpr);
  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutput, sizeof(SArithmeticSupport));

  int16_t type = 0;
  int16_t bytes = 0;

  char *groupbyColumnData = NULL;
  if (groupbyStateValue) {
    groupbyColumnData = getGroupbyColumnData(pQuery, &type, &bytes, pDataBlock);
  }

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].base.functionId;

    SDataStatis *pColStatis = NULL;

    bool  hasNull = hasNullValue(pQuery, k, pDataBlockInfo, pStatis, &pColStatis);
    char *dataBlock = getDataBlocks(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);

    setExecParams(pQuery, &pCtx[k], dataBlock, primaryKeyCol, pDataBlockInfo->rows, functionId, pColStatis, hasNull,
                  &sasArray[k], pRuntimeEnv->scanFlag);
  }

  // set the input column data
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    pFilterInfo->pData = getDataBlocks(pRuntimeEnv, &sasArray[k], pFilterInfo->info.colId, pDataBlockInfo->rows, pDataBlock);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // from top to bottom in desc
  // from bottom to top in asc order
  if (pRuntimeEnv->pTSBuf != NULL) {
    SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
    qTrace("QInfo:%p process data rows, numOfRows:%d, query order:%d, ts comp order:%d", pQInfo, pDataBlockInfo->rows,
           pQuery->order.order, pRuntimeEnv->pTSBuf->cur.order);
  }

  int32_t j = 0;
  TSKEY   lastKey = -1;

  for (j = 0; j < pDataBlockInfo->rows; ++j) {
    int32_t offset = GET_COL_DATA_POS(pQuery, j, step);

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

    // interval window query
    if (isIntervalQuery(pQuery)) {
      // decide the time window according to the primary timestamp
      int64_t     ts = primaryKeyCol[offset];
      STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

      int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &win);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        continue;
      }

      // all startOffset are identical
      offset -= pCtx[0].startOffset;

      SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doRowwiseApplyFunctions(pRuntimeEnv, pStatus, &win, offset);

      lastKey = ts;
      STimeWindow nextWin = win;
      int32_t     index = pWindowResInfo->curIndex;

      while (1) {
        getNextTimeWindow(pQuery, &nextWin);
        if (pWindowResInfo->startTime > nextWin.skey ||
            (nextWin.skey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (nextWin.skey > pQuery->window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        if (ts < nextWin.skey || ts > nextWin.ekey) {
          break;
        }

        // null data, failed to allocate more memory buffer
        if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->tid, &nextWin) != TSDB_CODE_SUCCESS) {
          break;
        }

        pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
        doRowwiseApplyFunctions(pRuntimeEnv, pStatus, &nextWin, offset);
      }

      pWindowResInfo->curIndex = index;
    } else {  // other queries
      // decide which group this rows belongs to according to current state value
      if (groupbyStateValue) {
        char *stateVal = groupbyColumnData + bytes * offset;

        int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, stateVal, type, bytes);
        if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
          continue;
        }
      }

      // update the lastKey
      lastKey = primaryKeyCol[offset];

      // all startOffset are identical
      offset -= pCtx[0].startOffset;

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
  
  pQuery->lastKey = lastKey + step;
  
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
                                          SDataStatis *pStatis, __block_search_fn_t searchFn,
                                          SWindowResInfo *pWindowResInfo, SArray *pDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, pDataBlock);
  } else {
    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, searchFn, pDataBlock);
  }

  TSKEY lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pDataBlockInfo->window.ekey : pDataBlockInfo->window.skey;
  pQuery->lastKey = lastKey + GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  doCheckQueryCompleted(pRuntimeEnv, lastKey, pWindowResInfo);

  // interval query with limit applied
  if (isIntervalQuery(pQuery) && pQuery->limit.limit > 0 &&
      (pQuery->limit.limit + pQuery->limit.offset) <= numOfClosedTimeWindow(pWindowResInfo) &&
      pRuntimeEnv->scanFlag == MASTER_SCAN) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  int32_t numOfRes = getNumOfResult(pRuntimeEnv);

  // update the number of output result
  if (numOfRes > 0 && pQuery->checkBuffer == 1) {
    assert(numOfRes >= pQuery->rec.rows);
    pQuery->rec.rows = numOfRes;

    if (numOfRes >= pQuery->rec.threshold) {
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
    }
  }

  return numOfRes;
}

void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void *inputData, TSKEY *tsCol, int32_t size,
                   int32_t functionId, SDataStatis *pStatis, bool hasNull, void *param, int32_t scanFlag) {
  pCtx->scanFlag = scanFlag;

  pCtx->aInputElemBuf = inputData;
  pCtx->hasNull = hasNull;

  if (pStatis != NULL) {
    pCtx->preAggVals.isSet = true;
    pCtx->preAggVals.size = size;
    pCtx->preAggVals.statis = *pStatis;
  } else {
    pCtx->preAggVals.isSet = false;
  }

  pCtx->startOffset = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->pos : 0;
  pCtx->size = QUERY_IS_ASC_QUERY(pQuery) ? size - pQuery->pos : pQuery->pos + 1;

  uint32_t status = aAggs[functionId].nStatus;
  if (((status & (TSDB_FUNCSTATE_SELECTIVITY | TSDB_FUNCSTATE_NEED_TS)) != 0) && (tsCol != NULL)) {
    pCtx->ptsList = &tsCol[pCtx->startOffset];
  }

  if (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) {
    // last_dist or first_dist function
    // store the first&last timestamp into the intermediate buffer [1], the true
    // value may be null but timestamp will never be null
    //    pCtx->ptsList = tsCol;
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_TWA ||
             functionId == TSDB_FUNC_DIFF || (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE)) {
    /*
     * leastsquares function needs two columns of input, currently, the x value of linear equation is set to
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
  }

#if defined(_DEBUG_VIEW)
  //  int64_t *tsList = (int64_t *)primaryColumnData;
//  int64_t  s = tsList[0];
//  int64_t  e = tsList[size - 1];

//    if (IS_DATA_BLOCK_LOADED(blockStatus)) {
//        qTrace("QInfo:%p query ts:%lld-%lld, offset:%d, rows:%d, bstatus:%d,
//        functId:%d", GET_QINFO_ADDR(pQuery),
//               s, e, startOffset, size, blockStatus, functionId);
//    } else {
//        qTrace("QInfo:%p block not loaded, bstatus:%d",
//        GET_QINFO_ADDR(pQuery), blockStatus);
//    }
#endif
}

// set the output buffer for the selectivity + tag query
static void setCtxTagColumnInfo(SQuery *pQuery, SQLFunctionCtx *pCtx) {
  if (isSelectivityWithTagsQuery(pQuery)) {
    int32_t         num = 0;
    SQLFunctionCtx *p = NULL;

    int16_t tagLen = 0;

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

    p->tagInfo.pTagCtxList = pTagCtx;
    p->tagInfo.numOfTagCols = num;
    p->tagInfo.tagsLen = tagLen;
  }
}

static void setWindowResultInfo(SResultInfo *pResultInfo, SQuery *pQuery, bool isStableQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    setResultInfoBuf(&pResultInfo[i], pQuery->pSelectExpr[i].interResBytes, isStableQuery);
  }
}

static int32_t setupQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv, int16_t order) {
  qTrace("QInfo:%p setup runtime env", GET_QINFO_ADDR(pRuntimeEnv));
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pRuntimeEnv->resultInfo = calloc(pQuery->numOfOutput, sizeof(SResultInfo));
  pRuntimeEnv->pCtx = (SQLFunctionCtx *)calloc(pQuery->numOfOutput, sizeof(SQLFunctionCtx));

  if (pRuntimeEnv->resultInfo == NULL || pRuntimeEnv->pCtx == NULL) {
    goto _error_clean;
  }

  pRuntimeEnv->offset[0] = 0;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SSqlFuncMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].base;

    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    SColIndex* pIndex = &pSqlFuncMsg->colInfo;
    
    int32_t index = pSqlFuncMsg->colInfo.colIndex;
    if (TSDB_COL_IS_TAG(pIndex->flag)) {
      if (pIndex->colId == TSDB_TBNAME_COLUMN_INDEX) {
        pCtx->inputBytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
        pCtx->inputType = TSDB_DATA_TYPE_BINARY;
      } else {
        pCtx->inputBytes = pQuery->tagColList[index].bytes;
        pCtx->inputType = pQuery->tagColList[index].type;
      }
    } else {
      pCtx->inputBytes = pQuery->colList[index].bytes;
      pCtx->inputType = pQuery->colList[index].type;
    }
    
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

  // set the intermediate result output buffer
  setWindowResultInfo(pRuntimeEnv->resultInfo, pQuery, pRuntimeEnv->stableQuery);

  // if it is group by normal column, do not set output buffer, the output buffer is pResult
  if (!isGroupbyNormalCol(pQuery->pGroupbyExpr) && !pRuntimeEnv->stableQuery) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

  setCtxTagColumnInfo(pQuery, pRuntimeEnv->pCtx);
  return TSDB_CODE_SUCCESS;

_error_clean:
  tfree(pRuntimeEnv->resultInfo);
  tfree(pRuntimeEnv->pCtx);

  return TSDB_CODE_SERV_OUT_OF_MEMORY;
}

static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  if (pRuntimeEnv->pQuery == NULL) {
    return;
  }

  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo* pQInfo = (SQInfo*) GET_QINFO_ADDR(pRuntimeEnv);
  
  qTrace("QInfo:%p teardown runtime env", pQInfo);
  cleanupTimeWindowInfo(&pRuntimeEnv->windowResInfo, pQuery->numOfOutput);

  if (pRuntimeEnv->pCtx != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

      for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
        tVariantDestroy(&pCtx->param[j]);
      }

      tVariantDestroy(&pCtx->tag);
      tfree(pCtx->tagInfo.pTagCtxList);
      tfree(pRuntimeEnv->resultInfo[i].interResultBuf);
    }

    tfree(pRuntimeEnv->resultInfo);
    tfree(pRuntimeEnv->pCtx);
  }

  taosDestoryInterpoInfo(&pRuntimeEnv->interpoInfo);

  if (pRuntimeEnv->pInterpoBuf != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      tfree(pRuntimeEnv->pInterpoBuf[i]);
    }

    tfree(pRuntimeEnv->pInterpoBuf);
  }

  destroyResultBuf(pRuntimeEnv->pResultBuf, pQInfo);
  tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
  tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);

  pRuntimeEnv->pTSBuf = tsBufDestory(pRuntimeEnv->pTSBuf);
}

static bool isQueryKilled(SQInfo *pQInfo) {
  return (pQInfo->code == TSDB_CODE_QUERY_CANCELLED);
#if 0
  /*
   * check if the queried meter is going to be deleted.
   * if it will be deleted soon, stop current query ASAP.
   */
  SMeterObj *pMeterObj = pQInfo->pObj;
  if (vnodeIsMeterState(pMeterObj, TSDB_METER_STATE_DROPPING)) {
    pQInfo->killed = 1;
    return true;
  }
  
  return (pQInfo->killed == 1);
#endif
}

static void setQueryKilled(SQInfo *pQInfo) { pQInfo->code = TSDB_CODE_QUERY_CANCELLED; }

static bool isFixedOutputQuery(SQuery *pQuery) {
  if (pQuery->intervalTime != 0) {
    return false;
  }

  // Note:top/bottom query is fixed output query
  if (isTopBottomQuery(pQuery) || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
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

static bool isPointInterpoQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].base.functionId;
    if (functionID == TSDB_FUNC_INTERP || functionID == TSDB_FUNC_LAST_ROW) {
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

static UNUSED_FUNC bool notHasQueryTimeRange(SQuery *pQuery) {
  return (pQuery->window.skey == 0 && pQuery->window.ekey == INT64_MAX && QUERY_IS_ASC_QUERY(pQuery)) ||
         (pQuery->window.skey == INT64_MAX && pQuery->window.ekey == 0 && (!QUERY_IS_ASC_QUERY(pQuery)));
}

static bool needReverseScan(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (((functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_LAST_DST) && QUERY_IS_ASC_QUERY(pQuery)) ||
        ((functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_FIRST_DST) && !QUERY_IS_ASC_QUERY(pQuery))) {
      return true;
    }
  }

  return false;
}

static bool onlyQueryTags(SQuery* pQuery) {
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
    if (functionId != TSDB_FUNC_TAGPRJ && functionId != TSDB_FUNC_TID_TAG) {
      return false;
    }
  }
  
  return true;
}

/////////////////////////////////////////////////////////////////////////////////////////////

void getAlignQueryTimeWindow(SQuery *pQuery, int64_t key, int64_t keyFirst, int64_t keyLast, int64_t *realSkey,
                             int64_t *realEkey, STimeWindow *win) {
  assert(key >= keyFirst && key <= keyLast && pQuery->slidingTime <= pQuery->intervalTime);

  win->skey = taosGetIntervalStartTimestamp(key, pQuery->slidingTime, pQuery->slidingTimeUnit, pQuery->precision);

  if (keyFirst > (INT64_MAX - pQuery->intervalTime)) {
    /*
     * if the realSkey > INT64_MAX - pQuery->intervalTime, the query duration between
     * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
     */
    assert(keyLast - keyFirst < pQuery->intervalTime);

    *realSkey = keyFirst;
    *realEkey = keyLast;

    win->ekey = INT64_MAX;
    return;
  }

  win->ekey = win->skey + pQuery->intervalTime - 1;

  if (win->skey < keyFirst) {
    *realSkey = keyFirst;
  } else {
    *realSkey = win->skey;
  }

  if (win->ekey < keyLast) {
    *realEkey = win->ekey;
  } else {
    *realEkey = keyLast;
  }
}

static UNUSED_FUNC bool doGetQueryPos(TSKEY key, SQInfo *pQInfo, SPointInterpoSupporter *pPointInterpSupporter) {
#if 0
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  SMeterObj *       pMeterObj = pRuntimeEnv->pTabObj;
  
  /* key in query range. If not, no qualified in disk file */
  if (key != -1 && key <= pQuery->window.ekey) {
    if (isPointInterpoQuery(pQuery)) { /* no qualified data in this query range */
      return getNeighborPoints(pQInfo, pMeterObj, pPointInterpSupporter);
    } else {
      return true;
    }
  } else {  // key > pQuery->window.ekey, abort for normal query, continue for interp query
    if (isPointInterpoQuery(pQuery)) {
      return getNeighborPoints(pQInfo, pMeterObj, pPointInterpSupporter);
    } else {
      return false;
    }
  }
#endif
  return true;
}

static UNUSED_FUNC bool doSetDataInfo(SQInfo *pQInfo, SPointInterpoSupporter *pPointInterpSupporter, void *pMeterObj,
                                      TSKEY nextKey) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (isFirstLastRowQuery(pQuery)) {
    /*
     * if the pQuery->window.skey != pQuery->window.ekey for last_row query,
     * the query range is existed, so set them both the value of nextKey
     */
    if (pQuery->window.skey != pQuery->window.ekey) {
      assert(pQuery->window.skey >= pQuery->window.ekey && !QUERY_IS_ASC_QUERY(pQuery) &&
             nextKey >= pQuery->window.ekey && nextKey <= pQuery->window.skey);

      pQuery->window.skey = nextKey;
      pQuery->window.ekey = nextKey;
    }

    return getNeighborPoints(pQInfo, pMeterObj, pPointInterpSupporter);
  } else {
    return true;
  }
}

// TODO refactor code, the best way to implement the last_row is utilizing the iterator
bool normalizeUnBoundLastRowQuery(SQInfo *pQInfo, SPointInterpoSupporter *pPointInterpSupporter) {
#if 0
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pTabObj;

  assert(!QUERY_IS_ASC_QUERY(pQuery) && notHasQueryTimeRange(pQuery));
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  TSKEY lastKey = -1;

  pQuery->fileId = -1;
  vnodeFreeFieldsEx(pRuntimeEnv);

  // keep in-memory cache status in local variables in case that it may be changed by write operation
  getBasicCacheInfoSnapshot(pQuery, pMeterObj->pCache, pMeterObj->vnode);

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo != NULL && pCacheInfo->cacheBlocks != NULL && pQuery->numOfBlocks > 0) {
    pQuery->fileId = -1;
    TSKEY key = pMeterObj->lastKey;

    pQuery->window.skey = key;
    pQuery->window.ekey = key;
    pQuery->lastKey = pQuery->window.skey;

    /*
     * cache block may have been flushed to disk, and no data in cache anymore.
     * So, copy cache block to local buffer is required.
     */
    lastKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
    if (lastKey < 0) {  // data has been flushed to disk, try again search in file
      lastKey = getQueryPositionForCacheInvalid(pRuntimeEnv, searchFn);

      if (Q_STATUS_EQUAL(pQuery->status, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
        return false;
      }
    }
  } else {  // no data in cache, try file
    TSKEY key = pMeterObj->lastKeyOnFile;

    pQuery->window.skey = key;
    pQuery->window.ekey = key;
    pQuery->lastKey = pQuery->window.skey;

    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn);
    if (!ret) {  // no data in file, return false;
      return false;
    }

    lastKey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
  }

  assert(lastKey <= pQuery->window.skey);

  pQuery->window.skey = lastKey;
  pQuery->window.ekey = lastKey;
  pQuery->lastKey = pQuery->window.skey;

  return getNeighborPoints(pQInfo, pMeterObj, pPointInterpSupporter);
#endif

  return true;
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
bool vnodeParametersSafetyCheck(SQuery *pQuery) {
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

static void changeExecuteScanOrder(SQuery *pQuery, bool metricQuery) {
  // in case of point-interpolation query, use asc order scan
  char msg[] = "QInfo:%p scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%" PRId64
               "-%" PRId64 ", new qrange:%" PRId64 "-%" PRId64;

  // todo handle the case the the order irrelevant query type mixed up with order critical query type
  // descending order query for last_row query
  if (isFirstLastRowQuery(pQuery)) {
    qTrace("QInfo:%p scan order changed for last_row query, old:%d, new:%d", GET_QINFO_ADDR(pQuery),
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
      qTrace(msg, GET_QINFO_ADDR(pQuery), "interp", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
             pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    pQuery->order.order = TSDB_ORDER_ASC;
    return;
  }

  if (pQuery->intervalTime == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        qTrace(msg, GET_QINFO_ADDR(pQuery), "only-first", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
      }

      pQuery->order.order = TSDB_ORDER_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        qTrace(msg, GET_QINFO_ADDR(pQuery), "only-last", pQuery->order.order, TSDB_ORDER_DESC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
      }

      pQuery->order.order = TSDB_ORDER_DESC;
    }

  } else {  // interval query
    if (metricQuery) {
      if (onlyFirstQuery(pQuery)) {
        if (!QUERY_IS_ASC_QUERY(pQuery)) {
          qTrace(msg, GET_QINFO_ADDR(pQuery), "only-first stable", pQuery->order.order, TSDB_ORDER_ASC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        }

        pQuery->order.order = TSDB_ORDER_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          qTrace(msg, GET_QINFO_ADDR(pQuery), "only-last stable", pQuery->order.order, TSDB_ORDER_DESC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        }

        pQuery->order.order = TSDB_ORDER_DESC;
      }
    }
  }
}

static UNUSED_FUNC void doSetInterpVal(SQLFunctionCtx *pCtx, TSKEY ts, int16_t type, int32_t index, char *data) {
  assert(pCtx->param[index].pz == NULL);

  int32_t len = 0;
  size_t  t = 0;

  if (type == TSDB_DATA_TYPE_BINARY) {
    t = strlen(data);

    len = t + 1 + TSDB_KEYSIZE;
    pCtx->param[index].pz = calloc(1, len);
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    t = wcslen((const wchar_t *)data);

    len = (t + 1) * TSDB_NCHAR_SIZE + TSDB_KEYSIZE;
    pCtx->param[index].pz = calloc(1, len);
  } else {
    len = TSDB_KEYSIZE * 2;
    pCtx->param[index].pz = malloc(len);
  }

  pCtx->param[index].nType = TSDB_DATA_TYPE_BINARY;

  char *z = pCtx->param[index].pz;
  *(TSKEY *)z = ts;
  z += TSDB_KEYSIZE;

  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:
      *(double *)z = GET_FLOAT_VAL(data);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *(double *)z = GET_DOUBLE_VAL(data);
      break;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      *(int64_t *)z = GET_INT64_VAL(data);
      break;
    case TSDB_DATA_TYPE_BINARY:
      strncpy(z, data, t);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      wcsncpy((wchar_t *)z, (const wchar_t *)data, t);
    } break;
    default:
      assert(0);
  }

  pCtx->param[index].nLen = len;
}

/**
 * param[1]: default value/previous value of specified timestamp
 * param[2]: next value of specified timestamp
 * param[3]: denotes if the result is a precious result or interpolation results
 *
 * @param pQInfo
 * @param pQInfo
 * @param pInterpoRaw
 */
void pointInterpSupporterSetData(SQInfo *pQInfo, SPointInterpoSupporter *pPointInterpSupport) {
#if 0
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  // not point interpolation query, abort
  if (!isPointInterpoQuery(pQuery)) {
    return;
  }

  int32_t count = 1;
  TSKEY key = *(TSKEY *)pPointInterpSupport->next[0];

  if (key == pQuery->window.skey) {
    // the queried timestamp has value, return it directly without interpolation
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      tVariantCreateFromBinary(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

      pRuntimeEnv->pCtx[i].param[0].i64Key = key;
      pRuntimeEnv->pCtx[i].param[0].nType = TSDB_DATA_TYPE_BIGINT;
    }
  } else {
    // set the direct previous(next) point for process
    count = 2;

    if (pQuery->interpoType == TSDB_INTERPO_SET_VALUE) {
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

        // only the function of interp needs the corresponding information
        if (pCtx->functionId != TSDB_FUNC_INTERP) {
          continue;
        }

        pCtx->numOfParams = 4;

        SInterpInfo *pInterpInfo = (SInterpInfo *)pRuntimeEnv->pCtx[i].aOutputBuf;
        pInterpInfo->pInterpDetail = calloc(1, sizeof(SInterpInfoDetail));

        SInterpInfoDetail *pInterpDetail = pInterpInfo->pInterpDetail;

        // for primary timestamp column, set the flag
        if (pQuery->pSelectExpr[i].base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          pInterpDetail->primaryCol = 1;
        }

        tVariantCreateFromBinary(&pCtx->param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

        if (isNull((char *)&pQuery->defaultVal[i], pCtx->inputType)) {
          pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
        } else {
          tVariantCreateFromBinary(&pCtx->param[1], (char *)&pQuery->defaultVal[i], pCtx->inputBytes, pCtx->inputType);
        }

        pInterpDetail->ts = pQuery->window.skey;
        pInterpDetail->type = pQuery->interpoType;
      }
    } else {
      TSKEY prevKey = *(TSKEY *)pPointInterpSupport->pPrevPoint[0];
      TSKEY nextKey = *(TSKEY *)pPointInterpSupport->pNextPoint[0];

      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

        // tag column does not need the interp environment
        if (pQuery->pSelectExpr[i].base.functionId == TSDB_FUNC_TAG) {
          continue;
        }

        int32_t      colInBuf = 0;  // pQuery->pSelectExpr[i].base.colInfo.colIdxInBuf;
        SInterpInfo *pInterpInfo = (SInterpInfo *)pRuntimeEnv->pCtx[i].aOutputBuf;

        pInterpInfo->pInterpDetail = calloc(1, sizeof(SInterpInfoDetail));
        SInterpInfoDetail *pInterpDetail = pInterpInfo->pInterpDetail;

        //        int32_t type = GET_COLUMN_TYPE(pQuery, i);
        int32_t type = 0;
        assert(0);

        // for primary timestamp column, set the flag
        if (pQuery->pSelectExpr[i].base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          pInterpDetail->primaryCol = 1;
        } else {
          doSetInterpVal(pCtx, prevKey, type, 1, pPointInterpSupport->pPrevPoint[colInBuf]);
          doSetInterpVal(pCtx, nextKey, type, 2, pPointInterpSupport->pNextPoint[colInBuf]);
        }

        tVariantCreateFromBinary(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

        pInterpDetail->ts = pQInfo->runtimeEnv.pQuery->window.skey;
        pInterpDetail->type = pQuery->interpoType;
      }
    }
  }
#endif
}

void pointInterpSupporterInit(SQuery *pQuery, SPointInterpoSupporter *pInterpoSupport) {
#if 0
  if (isPointInterpoQuery(pQuery)) {
    pInterpoSupport->pPrevPoint = malloc(pQuery->numOfCols * POINTER_BYTES);
    pInterpoSupport->pNextPoint = malloc(pQuery->numOfCols * POINTER_BYTES);

    pInterpoSupport->numOfCols = pQuery->numOfCols;

    /* get appropriated size for one row data source*/
    int32_t len = 0;
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      len += pQuery->colList[i].bytes;
    }

    //    assert(PRIMARY_TSCOL_LOADED(pQuery));

    void *prev = calloc(1, len);
    void *next = calloc(1, len);

    int32_t offset = 0;

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      pInterpoSupport->pPrevPoint[i] = prev + offset;
      pInterpoSupport->pNextPoint[i] = next + offset;

      offset += pQuery->colList[i].bytes;
    }
  }
#endif
}

void pointInterpSupporterDestroy(SPointInterpoSupporter *pPointInterpSupport) {
#if 0
  if (pPointInterpSupport->numOfCols <= 0 || pPointInterpSupport->pPrevPoint == NULL) {
    return;
  }

  tfree(pPointInterpSupport->pPrevPoint[0]);
  tfree(pPointInterpSupport->pNextPoint[0]);

  tfree(pPointInterpSupport->pPrevPoint);
  tfree(pPointInterpSupport->pNextPoint);

  pPointInterpSupport->numOfCols = 0;
#endif
}

static UNUSED_FUNC void allocMemForInterpo(SQInfo *pQInfo, SQuery *pQuery, void *pMeterObj) {
#if 0
  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    assert(isIntervalQuery(pQuery) || (pQuery->intervalTime == 0 && isPointInterpoQuery(pQuery)));
    
    if (isIntervalQuery(pQuery)) {
      pQInfo->runtimeEnv.pInterpoBuf = malloc(POINTER_BYTES * pQuery->numOfOutput);
      
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        pQInfo->runtimeEnv.pInterpoBuf[i] =
            calloc(1, sizeof(tFilePage) + pQuery->pSelectExpr[i].bytes * pMeterObj->pointsPerFileBlock);
      }
    }
  }
#endif
}

static int32_t getInitialPageNum(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  int32_t INITIAL_RESULT_ROWS_VALUE = 16;

  int32_t num = 0;

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    num = 128;
  } else if (isIntervalQuery(pQuery)) {  // time window query, allocate one page for each table
    size_t s = pQInfo->groupInfo.numOfTables;
    num = MAX(s, INITIAL_RESULT_ROWS_VALUE);
  } else {    // for super table query, one page for each subset
    num = 1;  // pQInfo->pSidSet->numOfSubSet;
  }

  assert(num > 0);
  return num;
}

static int32_t getRowParamForMultiRowsOutput(SQuery *pQuery, bool isSTableQuery) {
  int32_t rowparam = 1;

  if (isTopBottomQuery(pQuery) && (!isSTableQuery)) {
    rowparam = pQuery->pSelectExpr[1].base.arg->argValue.i64;
  }

  return rowparam;
}

static int32_t getNumOfRowsInResultPage(SQuery *pQuery, bool isSTableQuery) {
  int32_t rowSize = pQuery->rowSize * getRowParamForMultiRowsOutput(pQuery, isSTableQuery);
  return (DEFAULT_INTERN_BUF_SIZE - sizeof(tFilePage)) / rowSize;
}

char *getPosInResultPage(SQueryRuntimeEnv *pRuntimeEnv, int32_t columnIndex, SWindowResult *pResult) {
  assert(pResult != NULL && pRuntimeEnv != NULL);

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  tFilePage *page = getResultBufferPageById(pRuntimeEnv->pResultBuf, pResult->pos.pageId);

  int32_t numOfRows = getNumOfRowsInResultPage(pQuery, pRuntimeEnv->stableQuery);
  int32_t realRowId = pResult->pos.rowId * getRowParamForMultiRowsOutput(pQuery, pRuntimeEnv->stableQuery);

  return ((char *)page->data) + pRuntimeEnv->offset[columnIndex] * numOfRows +
         pQuery->pSelectExpr[columnIndex].bytes * realRowId;
}

/**
 * decrease the refcount for each table involved in this query
 * @param pQInfo
 */
UNUSED_FUNC void vnodeDecMeterRefcnt(SQInfo *pQInfo) {
  if (pQInfo != NULL) {
    //    assert(taosHashGetSize(pQInfo->groupInfo) >= 1);
  }

#if 0
  if (pQInfo == NULL || pQInfo->groupInfo.numOfTables == 1) {
    atomic_fetch_sub_32(&pQInfo->pObj->numOfQueries, 1);
    qTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pQInfo->pObj->vnode,
           pQInfo->pObj->sid, pQInfo->pObj->meterId, pQInfo->pObj->numOfQueries);
  } else {
    int32_t num = 0;
    for (int32_t i = 0; i < pQInfo->groupInfo.numOfTables; ++i) {
      SMeterObj *pMeter = getMeterObj(pQInfo->groupInfo, pQInfo->pSidSet->pTableIdList[i]->sid);
      atomic_fetch_sub_32(&(pMeter->numOfQueries), 1);
      
      if (pMeter->numOfQueries > 0) {
        qTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pMeter->vnode, pMeter->sid,
               pMeter->meterId, pMeter->numOfQueries);
        num++;
      }
    }
    
    /*
     * in order to reduce log output, for all meters of which numOfQueries count are 0,
     * we do not output corresponding information
     */
    num = pQInfo->groupInfo.numOfTables - num;
    qTrace("QInfo:%p metric query is over, dec query ref for %d meters, numOfQueries on %d meters are 0", pQInfo,
           pQInfo->groupInfo.numOfTables, num);
  }
#endif
}

static bool needToLoadDataBlock(SQuery *pQuery, SDataStatis *pDataStatis, SQLFunctionCtx *pCtx,
                                int32_t numOfTotalPoints) {
  if (pDataStatis == NULL) {
    return true;
  }

#if 0
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    int32_t                  colIndex = pFilterInfo->info.colIndex;
    
    // this column not valid in current data block
    if (colIndex < 0 || pDataStatis[colIndex].colId != pFilterInfo->info.data.colId) {
      continue;
    }
    
    // not support pre-filter operation on binary/nchar data type
    if (!vnodeSupportPrefilter(pFilterInfo->info.data.type)) {
      continue;
    }
    
    // all points in current column are NULL, no need to check its boundary value
    if (pDataStatis[colIndex].numOfNull == numOfTotalPoints) {
      continue;
    }
    
    if (pFilterInfo->info.info.type == TSDB_DATA_TYPE_FLOAT) {
      float minval = *(double *)(&pDataStatis[colIndex].min);
      float maxval = *(double *)(&pDataStatis[colIndex].max);
      
      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&minval, (char *)&maxval)) {
          return true;
        }
      }
    } else {
      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&pDataStatis[colIndex].min,
                                        (char *)&pDataStatis[colIndex].max)) {
          return true;
        }
      }
    }
  }
  
  // todo disable this opt code block temporarily
  //  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
  //    int32_t functId = pQuery->pSelectExpr[i].base.functionId;
  //    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
  //      return top_bot_datablock_filter(&pCtx[i], functId, (char *)&pField[i].min, (char *)&pField[i].max);
  //    }
  //  }

#endif
  return true;
}

// previous time window may not be of the same size of pQuery->intervalTime
static void getNextTimeWindow(SQuery *pQuery, STimeWindow *pTimeWindow) {
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  pTimeWindow->skey += (pQuery->slidingTime * factor);
  pTimeWindow->ekey = pTimeWindow->skey + (pQuery->intervalTime - 1);
}

SArray *loadDataBlockOnDemand(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pBlockInfo, SDataStatis **pStatis) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  uint32_t r = 0;
  SArray * pDataBlock = NULL;

  if (pQuery->numOfFilterCols > 0) {
    r = BLK_DATA_ALL_NEEDED;
  } else {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
      int32_t colId = pQuery->pSelectExpr[i].base.colInfo.colId;
      r |= aAggs[functionId].dataReqFunc(&pRuntimeEnv->pCtx[i], pQuery->window.skey, pQuery->window.ekey, colId);
    }

    if (pRuntimeEnv->pTSBuf > 0 || isIntervalQuery(pQuery)) {
      r |= BLK_DATA_ALL_NEEDED;
    }
  }

  if (r == BLK_DATA_NO_NEEDED) {
    qTrace("QInfo:%p data block ignored, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_QINFO_ADDR(pRuntimeEnv),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
  } else if (r == BLK_DATA_FILEDS_NEEDED) {
    if (tsdbRetrieveDataBlockStatisInfo(pRuntimeEnv->pQueryHandle, pStatis) != TSDB_CODE_SUCCESS) {
      //        return DISK_DATA_LOAD_FAILED;
    }

    if (*pStatis == NULL) {
      pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
    }
  } else {
    assert(r == BLK_DATA_ALL_NEEDED);
    if (tsdbRetrieveDataBlockStatisInfo(pRuntimeEnv->pQueryHandle, pStatis) != TSDB_CODE_SUCCESS) {
      //        return DISK_DATA_LOAD_FAILED;
    }

    /*
     * if this block is completed included in the query range, do more filter operation
     * filter the data block according to the value filter condition.
     * no need to load the data block, continue for next block
     */
    if (!needToLoadDataBlock(pQuery, *pStatis, pRuntimeEnv->pCtx, pBlockInfo->rows)) {
#if defined(_DEBUG_VIEW)
      qTrace("QInfo:%p block discarded by per-filter", GET_QINFO_ADDR(pRuntimeEnv));
#endif
      //        return DISK_DATA_DISCARDED;
    }

    pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
  }

  return pDataBlock;
}

int32_t binarySearchForKey(char *pValue, int num, TSKEY key, int order) {
  int32_t midPos = -1;
  int32_t numOfPoints;

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

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

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

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

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

static int64_t doScanAllDataBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  qTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", lastkey:%" PRId64 ", order:%d",
         GET_QINFO_ADDR(pRuntimeEnv), pQuery->window.skey, pQuery->window.ekey, pQuery->lastKey, pQuery->order.order);

  TsdbQueryHandleT pQueryHandle =
      pRuntimeEnv->scanFlag == MASTER_SCAN ? pRuntimeEnv->pQueryHandle : pRuntimeEnv->pSecQueryHandle;
  while (tsdbNextDataBlock(pQueryHandle)) {
    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      return 0;
    }

    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);

    // todo extract methods
    if (isIntervalQuery(pQuery) && pRuntimeEnv->windowResInfo.prevSKey == 0) {
      TSKEY           skey1, ekey1;
      STimeWindow     w = {0};
      SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

      if (QUERY_IS_ASC_QUERY(pQuery)) {
        getAlignQueryTimeWindow(pQuery, blockInfo.window.skey, blockInfo.window.skey, pQuery->window.ekey, &skey1,
                                &ekey1, &w);
        pWindowResInfo->startTime = w.skey;
        pWindowResInfo->prevSKey = w.skey;
      } else {
        // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
        getAlignQueryTimeWindow(pQuery, blockInfo.window.ekey, pQuery->window.ekey, blockInfo.window.ekey, &skey1,
                                &ekey1, &w);

        pWindowResInfo->startTime = pQuery->window.skey;
        pWindowResInfo->prevSKey = w.skey;
      }
    }

    // in case of prj/diff query, ensure the output buffer is sufficient to accomodate the results of current block
    if (!isIntervalQuery(pQuery) && !isGroupbyNormalCol(pQuery->pGroupbyExpr) && !isFixedOutputQuery(pQuery)) {
      SResultRec *pRec = &pQuery->rec;

      if (pQuery->rec.capacity - pQuery->rec.rows < blockInfo.rows) {
        int32_t remain = pRec->capacity - pRec->rows;
        int32_t newSize = pRec->capacity + (blockInfo.rows - remain);

        for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
          int32_t bytes = pQuery->pSelectExpr[i].bytes;

          char *tmp = realloc(pQuery->sdata[i], bytes * newSize + sizeof(SData));
          if (tmp == NULL) {  // todo handle the oom
          } else {
            pQuery->sdata[i] = (SData *)tmp;
          }

          // set the pCtx output buffer position
          pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data + pRec->rows * bytes;
        }

        pRec->capacity = newSize;
      }
    }

    SDataStatis *pStatis = NULL;
    SArray *     pDataBlock = loadDataBlockOnDemand(pRuntimeEnv, &blockInfo, &pStatis);

    pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : blockInfo.rows - 1;
    int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, binarySearchForKey,
                                                  &pRuntimeEnv->windowResInfo, pDataBlock);

    qTrace("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", rows:%d, res:%d", GET_QINFO_ADDR(pRuntimeEnv),
           blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes);

    // save last access position
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
      break;
    }
  }

  // if the result buffer is not full, set the query completed flag
  if (!Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  if (isIntervalQuery(pQuery) && IS_MASTER_SCAN(pRuntimeEnv)) {
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      int32_t step = QUERY_IS_ASC_QUERY(pQuery) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP;

      closeAllTimeWindow(&pRuntimeEnv->windowResInfo);
      removeRedundantWindow(&pRuntimeEnv->windowResInfo, pQuery->lastKey - step, step);
      pRuntimeEnv->windowResInfo.curIndex = pRuntimeEnv->windowResInfo.size - 1;
    } else {
      assert(Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL));
    }
  }

  return 0;
}

static void updatelastkey(SQuery *pQuery, STableQueryInfo *pTableQInfo) { pTableQInfo->lastKey = pQuery->lastKey; }

/*
 * set tag value in SQLFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(void *tsdb, STableId* pTableId, int32_t tagColId, tVariant *param) {
  tVariantDestroy(param);

  char *  val = NULL;
  int16_t bytes = 0;
  int16_t type = 0;

  if (tagColId == TSDB_TBNAME_COLUMN_INDEX) {
    tsdbGetTableName(tsdb, pTableId, &val);
    bytes = strnlen(val, TSDB_TABLE_NAME_LEN);
    type = TSDB_DATA_TYPE_BINARY;
  } else {
    tsdbGetTableTagVal(tsdb, pTableId, tagColId, &type, &bytes, &val);
  }
  
  tVariantCreateFromBinary(param, val, bytes, type);
  
  if (tagColId == TSDB_TBNAME_COLUMN_INDEX) {
    tfree(val);
  }
}

void setTagVal(SQueryRuntimeEnv *pRuntimeEnv, STableId* pTableId, void *tsdb) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  SSqlFuncMsg *pFuncMsg = &pQuery->pSelectExpr[0].base;
  if (pQuery->numOfOutput == 1 && pFuncMsg->functionId == TSDB_FUNC_TS_COMP) {
    assert(pFuncMsg->numOfParams == 1);
    doSetTagValueInParam(tsdb, pTableId, pFuncMsg->arg->argValue.i64, &pRuntimeEnv->pCtx[0].tag);
  } else {
    // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutput; ++idx) {
      SColIndex *pCol = &pQuery->pSelectExpr[idx].base.colInfo;

      // ts_comp column required the tag value for join filter
      if (!TSDB_COL_IS_TAG(pCol->flag)) {
        continue;
      }

      // todo use tag column index to optimize performance
      doSetTagValueInParam(tsdb, pTableId, pCol->colId, &pRuntimeEnv->pCtx[idx].tag);
    }

    // set the join tag for first column
    if (pFuncMsg->functionId == TSDB_FUNC_TS && pFuncMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX &&
        pRuntimeEnv->pTSBuf != NULL) {
      assert(pFuncMsg->numOfParams == 1);
      assert(0);  // to do fix me
      //      doSetTagValueInParam(pTagSchema, pFuncMsg->arg->argValue.i64, pMeterSidInfo, &pRuntimeEnv->pCtx[0].tag);
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

      resetResultInfo(pCtx[i].resultInfo);
      aAggs[functionId].init(&pCtx[i]);
    }

    pCtx[i].hasNull = true;
    pCtx[i].nStartQueryTimestamp = timestamp;
    pCtx[i].aInputElemBuf = getPosInResultPage(pRuntimeEnv, i, pWindowRes);
    //    pCtx[i].aInputElemBuf = ((char *)inputSrc->data) +
    //                            ((int32_t)pRuntimeEnv->offset[i] * pRuntimeEnv->numOfRowsPerPage) +
    //                            pCtx[i].outputBytes * inputIdx;

    // in case of tag column, the tag information should be extracted from input buffer
    if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TAG) {
      tVariantDestroy(&pCtx[i].tag);
      tVariantCreateFromBinary(&pCtx[i].tag, pCtx[i].aInputElemBuf, pCtx[i].inputBytes, pCtx[i].inputType);
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

void UNUSED_FUNC displayInterResult(SData **pdata, SQuery *pQuery, int32_t numOfRows) {
#if 0
  int32_t numOfCols = pQuery->numOfOutput;
  printf("super table query intermediate result, total:%d\n", numOfRows);
  
  SQInfo *   pQInfo = (SQInfo *)(GET_QINFO_ADDR(pQuery));
  SMeterObj *pMeterObj = pQInfo->pObj;
  
  for (int32_t j = 0; j < numOfRows; ++j) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      switch (pQuery->pSelectExpr[i].type) {
        case TSDB_DATA_TYPE_BINARY: {
          int32_t colIndex = pQuery->pSelectExpr[i].base.colInfo.colIndex;
          int32_t type = 0;
          
          if (TSDB_COL_IS_TAG(pQuery->pSelectExpr[i].base.colInfo.flag)) {
            type = pQuery->pSelectExpr[i].type;
          } else {
            type = pMeterObj->schema[colIndex].type;
          }
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
#endif
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

  int32_t numOfGroups = taosArrayGetSize(pQInfo->groupInfo.pGroupList);

  while (pQInfo->groupIndex < numOfGroups) {
    SArray *group = taosArrayGetP(pQInfo->groupInfo.pGroupList, pQInfo->groupIndex);
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
    qTrace("QInfo:%p no result in group %d, continue", pQInfo, pQInfo->groupIndex - 1);
  }

  qTrace("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%lldms", pQInfo,
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

    // set current query completed
    //    if (pQInfo->numOfGroupResultPages == 0 && pQInfo->groupIndex == pQInfo->pSidSet->numOfSubSet) {
    //      pQInfo->tableIndex = pQInfo->pSidSet->numOfTables;
    //      return;
    //    }
  }

  SQueryRuntimeEnv *   pRuntimeEnv = &pQInfo->runtimeEnv;
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  int32_t id = getGroupResultId(pQInfo->groupIndex - 1);
  SIDList list = getDataBufPagesIdList(pResultBuf, pQInfo->offset + id);

  int32_t total = 0;
  for (int32_t i = 0; i < list.size; ++i) {
    tFilePage *pData = getResultBufferPageById(pResultBuf, list.pData[i]);
    total += pData->numOfElems;
  }

  int32_t rows = total;

  int32_t offset = 0;
  for (int32_t num = 0; num < list.size; ++num) {
    tFilePage *pData = getResultBufferPageById(pResultBuf, list.pData[num]);

    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      char *  pDest = pQuery->sdata[i]->data;

      memcpy(pDest + offset * bytes, pData->data + pRuntimeEnv->offset[i] * pData->numOfElems,
             bytes * pData->numOfElems);
    }

    offset += pData->numOfElems;
  }

  assert(pQuery->rec.rows == 0);

  pQuery->rec.rows += rows;
  pQInfo->offset += 1;
}

int64_t getNumOfResultWindowRes(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pWindowRes) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int64_t maxOutput = 0;
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
    if (pResultInfo != NULL && maxOutput < pResultInfo->numOfRes) {
      maxOutput = pResultInfo->numOfRes;
    }
  }

  return maxOutput;
}

int32_t mergeIntoGroupResultImpl(SQInfo *pQInfo, SArray *pGroup) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  size_t size = taosArrayGetSize(pGroup);

  tFilePage **buffer = (tFilePage **)pQuery->sdata;
  int32_t *   posList = calloc(size, sizeof(int32_t));

  STableQueryInfo **pTableList = malloc(POINTER_BYTES * size);

  // todo opt for the case of one table per group
  int32_t numOfTables = 0;
  for (int32_t i = 0; i < size; ++i) {
    SGroupItem *item = taosArrayGet(pGroup, i);
    STableQueryInfo *pInfo = item->info;

    SIDList list = getDataBufPagesIdList(pRuntimeEnv->pResultBuf, pInfo->id.tid);
    if (list.size > 0 && pInfo->windowResInfo.size > 0) {
      pTableList[numOfTables] = pInfo;
      numOfTables += 1;
    }
  }

  if (numOfTables == 0) {
    tfree(posList);
    tfree(pTableList);

    assert(pQInfo->numOfGroupResultPages == 0);
    return 0;
  }

  SCompSupporter cs = {pTableList, posList, pQInfo};

  SLoserTreeInfo *pTree = NULL;
  tLoserTreeCreate(&pTree, numOfTables, &cs, tableResultComparFn);

  SResultInfo *pResultInfo = calloc(pQuery->numOfOutput, sizeof(SResultInfo));
  setWindowResultInfo(pResultInfo, pQuery, pRuntimeEnv->stableQuery);
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
    int64_t num = getNumOfResultWindowRes(pRuntimeEnv, pWindowRes);
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
        if (buffer[0]->numOfElems == pQuery->rec.capacity) {
          if (flushFromResultBuf(pQInfo) != TSDB_CODE_SUCCESS) {
            return -1;
          }

          resetMergeResultBuf(pQuery, pRuntimeEnv->pCtx, pResultInfo);
        }

        doMerge(pRuntimeEnv, ts, pWindowRes, false);
        buffer[0]->numOfElems += 1;
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

  if (buffer[0]->numOfElems != 0) {  // there are data in buffer
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
  displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->num);
#endif

  qTrace("QInfo:%p result merge completed, elapsed time:%" PRId64 " ms", GET_QINFO_ADDR(pQuery), endt - startt);
  tfree(pTree);
  tfree(pTableList);
  tfree(posList);

  pQInfo->offset = 0;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    tfree(pResultInfo[i].interResultBuf);
  }

  tfree(pResultInfo);
  return pQInfo->numOfGroupResultPages;
}

int32_t flushFromResultBuf(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;
  int32_t              capacity = (DEFAULT_INTERN_BUF_SIZE - sizeof(tFilePage)) / pQuery->rowSize;

  // the base value for group result, since the maximum number of table for each vnode will not exceed 100,000.
  int32_t pageId = -1;

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
      buf->numOfElems = r;

      memcpy(buf->data + pRuntimeEnv->offset[i] * buf->numOfElems, ((char *)pQuery->sdata[i]->data) + offset * bytes,
             buf->numOfElems * bytes);
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

void setTableDataInfo(STableQueryInfo *pTableQueryInfo, int32_t tableIndex, int32_t groupId) {
  pTableQueryInfo->groupIdx = groupId;
  pTableQueryInfo->tableIndex = tableIndex;
}

static void doDisableFunctsForSupplementaryScan(SQuery *pQuery, SWindowResInfo *pWindowResInfo, int32_t order) {
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

void disableFuncInReverseScan(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  // group by normal columns and interval query on normal table
  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
  } else {  // for simple result of table query,
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t functId = pQuery->pSelectExpr[j].base.functionId;

      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[j];

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_ASC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_DESC)) {
        pCtx->resultInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pCtx->resultInfo->complete = true;
      }
    }
  }
}

void disableFuncForReverseScan(SQInfo *pQInfo, int32_t order) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  if (isIntervalQuery(pQuery)) {
    //    for (int32_t i = 0; i < pQInfo->groupInfo.numOfTables; ++i) {
    //      STableQueryInfo *pTableQueryInfo = pQInfo->pTableQueryInfo[i].pTableQInfo;
    //      SWindowResInfo * pWindowResInfo = &pTableQueryInfo->windowResInfo;
    //
    //      doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
    //    }
  } else {
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
  }

  pQuery->order.order = (pQuery->order.order) ^ 1u;
}

void switchCtxOrder(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SWITCH_ORDER(pRuntimeEnv->pCtx[i]
                     .order);  // = (pRuntimeEnv->pCtx[i].order == TSDB_ORDER_ASC)? TSDB_ORDER_DESC:TSDB_ORDER_ASC;
  }
}

void createQueryResultInfo(SQuery *pQuery, SWindowResult *pResultRow, bool isSTableQuery, SPosInfo *posInfo) {
  int32_t numOfCols = pQuery->numOfOutput;

  pResultRow->resultInfo = calloc((size_t)numOfCols, sizeof(SResultInfo));
  pResultRow->pos = *posInfo;

  // set the intermediate result output buffer
  setWindowResultInfo(pResultRow->resultInfo, pQuery, isSTableQuery);
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
    resetResultInfo(&pRuntimeEnv->resultInfo[i]);
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

    resetResultInfo(pRuntimeEnv->pCtx[j].resultInfo);
  }
}

void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].base.functionId;

    pRuntimeEnv->pCtx[j].currentStage = 0;
    aAggs[functionId].init(&pRuntimeEnv->pCtx[j]);
  }
}

void skipResults(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pQuery->rec.rows == 0 || pQuery->limit.offset == 0) {
    return;
  }

  if (pQuery->rec.rows <= pQuery->limit.offset) {
    pQuery->limit.offset -= pQuery->rec.rows;
    pQuery->rec.rows = 0;

    resetCtxOutputBuf(pRuntimeEnv);

    // clear the buffer is full flag if exists
    pQuery->status &= (~QUERY_RESBUF_FULL);
  } else {
    int32_t numOfSkip = (int32_t) pQuery->limit.offset;
    pQuery->rec.rows -= numOfSkip;

    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].base.functionId;
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      
      memmove(pQuery->sdata[i]->data, pQuery->sdata[i]->data + bytes * numOfSkip, pQuery->rec.rows * bytes);
      pRuntimeEnv->pCtx[i].aOutputBuf += bytes * numOfSkip;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf += TSDB_KEYSIZE * numOfSkip;
      }
    }

    pQuery->limit.offset = 0;
  }
}

void setQueryStatus(SQuery *pQuery, int8_t status) {
  if (status == QUERY_NOT_COMPLETED) {
    pQuery->status = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    pQuery->status &= (~QUERY_NOT_COMPLETED);
    pQuery->status |= status;
  }
}

bool needScanDataBlocksAgain(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  bool toContinue = false;
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
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

static SQueryStatusInfo getQueryStatusInfo(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  SQueryStatusInfo info = {
      .status = pQuery->status,
      .windowIndex = pRuntimeEnv->windowResInfo.curIndex,
      .lastKey = pQuery->lastKey,
      .w = pQuery->window,
      .curWindow = {.skey = pQuery->lastKey, .ekey = pQuery->window.ekey},
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
  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);

  STsdbQueryCond cond = {
      .twindow = pQuery->window,
      .order = pQuery->order.order,
      .colList = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
  };

  // clean unused handle
  if (pRuntimeEnv->pSecQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
  }

  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableIdGroupInfo);

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  switchCtxOrder(pRuntimeEnv);
  disableFuncInReverseScan(pRuntimeEnv);
}

static void clearEnvAfterReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatusInfo *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  SWITCH_ORDER(pQuery->order.order);
  switchCtxOrder(pRuntimeEnv);

  tsBufSetCursor(pRuntimeEnv->pTSBuf, &pStatus->cur);
  if (pRuntimeEnv->pTSBuf) {
    pRuntimeEnv->pTSBuf->cur.order = pQuery->order.order;
  }

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);

  // update the pQuery->window.skey and pQuery->window.ekey to limit the scan scope of sliding query
  // during reverse scan
  pQuery->lastKey = pStatus->lastKey;
  pQuery->status = pStatus->status;
  pQuery->window = pStatus->w;
}

void scanAllDataBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  // store the start query position
  SQInfo *         pQInfo = (SQInfo *)GET_QINFO_ADDR(pRuntimeEnv);
  SQueryStatusInfo qstatus = getQueryStatusInfo(pRuntimeEnv);

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  while (1) {
    doScanAllDataBlocks(pRuntimeEnv);

    if (pRuntimeEnv->scanFlag == MASTER_SCAN) {
      qstatus.status = pQuery->status;
      qstatus.curWindow.ekey = pQuery->lastKey - step;
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
        .order = pQuery->order.order,
        .colList = pQuery->colList,
        .numOfCols = pQuery->numOfCols,
    };

    if (pRuntimeEnv->pSecQueryHandle != NULL) {
      tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
    }

    pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableIdGroupInfo);
    pRuntimeEnv->windowResInfo.curIndex = qstatus.windowIndex;

    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    // check if query is killed or not
    if (isQueryKilled(pQInfo)) {
      return;
    }
  }

  if (!needReverseScan(pQuery)) {
    return;
  }

  setEnvBeforeReverseScan(pRuntimeEnv, &qstatus);

  // reverse scan from current position
  qTrace("QInfo:%p start to reverse scan", pQInfo);
  doScanAllDataBlocks(pRuntimeEnv);

  clearEnvAfterReverseScan(pRuntimeEnv, &qstatus);
}

void finalizeQueryResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    // for each group result, call the finalize function for each column
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
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

STableQueryInfo *createTableQueryInfoImpl(SQueryRuntimeEnv *pRuntimeEnv, STableId tableId, STimeWindow win) {
  STableQueryInfo *pTableQueryInfo = calloc(1, sizeof(STableQueryInfo));

  pTableQueryInfo->win = win;
  pTableQueryInfo->lastKey = win.skey;

  pTableQueryInfo->id = tableId;
  pTableQueryInfo->cur.vgroupIndex = -1;

  initWindowResInfo(&pTableQueryInfo->windowResInfo, pRuntimeEnv, 100, 100, TSDB_DATA_TYPE_INT);
  return pTableQueryInfo;
}

void destroyTableQueryInfo(STableQueryInfo *pTableQueryInfo, int32_t numOfCols) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  cleanupTimeWindowInfo(&pTableQueryInfo->windowResInfo, numOfCols);
  free(pTableQueryInfo);
}

void changeMeterQueryInfoForSuppleQuery(SQuery *pQuery, STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  // order has change already!
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (!QUERY_IS_ASC_QUERY(pQuery)) {
    assert(pTableQueryInfo->win.ekey >= pTableQueryInfo->lastKey + step);
  } else {
    assert(pTableQueryInfo->win.ekey <= pTableQueryInfo->lastKey + step);
  }

  pTableQueryInfo->win.ekey = pTableQueryInfo->lastKey + step;

  SWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, TSKEY);
  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;

  pTableQueryInfo->cur.order = pTableQueryInfo->cur.order ^ 1u;
  pTableQueryInfo->cur.vgroupIndex = -1;
}

void restoreIntervalQueryRange(SQueryRuntimeEnv *pRuntimeEnv, STableQueryInfo *pTableQueryInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pQuery->window = pTableQueryInfo->win;
  pQuery->lastKey = pTableQueryInfo->lastKey;

  assert(((pQuery->lastKey >= pQuery->window.skey) && QUERY_IS_ASC_QUERY(pQuery)) ||
         ((pQuery->lastKey <= pQuery->window.skey) && !QUERY_IS_ASC_QUERY(pQuery)));
}

/**
 * set output buffer for different group
 * @param pRuntimeEnv
 * @param pDataBlockInfo
 */
void setExecutionContext(SQInfo *pQInfo, STableQueryInfo *pTableQueryInfo, STableId* pTableId, int32_t groupIdx, TSKEY nextKey) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SWindowResInfo *  pWindowResInfo = &pRuntimeEnv->windowResInfo;
  int32_t           GROUPRESULTID = 1;

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&groupIdx, sizeof(groupIdx));
  if (pWindowRes == NULL) {
    return;
  }

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pWindowRes->pos.pageId == -1) {
    if (addNewWindowResultBuf(pWindowRes, pRuntimeEnv->pResultBuf, GROUPRESULTID, pRuntimeEnv->numOfRowsPerPage) !=
        TSDB_CODE_SUCCESS) {
      return;
    }
  }

  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);

  pTableQueryInfo->lastKey = nextKey;
  setAdditionalInfo(pQInfo, pTableId, pTableQueryInfo);
}

static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->numOfElems == 0, there is only fixed number of results for each group
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

int32_t setAdditionalInfo(SQInfo *pQInfo, STableId* pTableId, STableQueryInfo *pTableQueryInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  assert(pTableQueryInfo->lastKey >= 0);

  setTagVal(pRuntimeEnv, pTableId, pQInfo->tsdb);

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
void setIntervalQueryRange(STableQueryInfo *pTableQueryInfo, SQInfo *pQInfo, TSKEY key) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (pTableQueryInfo->queryRangeSet) {
    pQuery->lastKey = key;
    pTableQueryInfo->lastKey = key;
  } else {
    pQuery->window.skey = key;
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
    TSKEY           skey1, ekey1;
    STimeWindow     w = {0};
    SWindowResInfo *pWindowResInfo = &pTableQueryInfo->windowResInfo;

    getAlignQueryTimeWindow(pQuery, win.skey, win.skey, win.ekey, &skey1, &ekey1, &w);
    pWindowResInfo->startTime = pQuery->window.skey;  // windowSKey may be 0 in case of 1970 timestamp

    if (pWindowResInfo->prevSKey == 0) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        pWindowResInfo->prevSKey = w.skey;
      } else {
        assert(win.ekey == pQuery->window.skey);
        pWindowResInfo->prevSKey = w.skey;
      }
    }

    pTableQueryInfo->queryRangeSet = 1;
    pTableQueryInfo->lastKey = pQuery->window.skey;
    pTableQueryInfo->win.skey = pQuery->window.skey;

    pQuery->lastKey = pQuery->window.skey;
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
  bool         loadPrimaryTS = (pQuery->lastKey >= w->skey && pQuery->lastKey <= w->ekey) ||
                       (pQuery->window.ekey >= w->skey && pQuery->window.ekey <= w->ekey) || requireTimestamp(pQuery);

  return loadPrimaryTS;
}

bool onDemandLoadDatablock(SQuery *pQuery, int16_t queryRangeSet) {
  return (pQuery->intervalTime == 0) || ((queryRangeSet == 1) && (isIntervalQuery(pQuery)));
}

static int32_t getNumOfSubset(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  int32_t totalSubset = 0;
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || (isIntervalQuery(pQuery))) {
    totalSubset = numOfClosedTimeWindow(&pQInfo->runtimeEnv.windowResInfo);
  } else {
    totalSubset = taosArrayGetSize(pQInfo->groupInfo.pGroupList);
  }

  return totalSubset;
}

static int32_t doCopyToSData(SQInfo *pQInfo, SWindowResult *result, int32_t orderType) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfResult = 0;
  int32_t startIdx = 0;
  int32_t step = -1;

  qTrace("QInfo:%p start to copy data from windowResInfo to query buf", GET_QINFO_ADDR(pQuery));
  int32_t totalSubset = getNumOfSubset(pQInfo);

  if (orderType == TSDB_ORDER_ASC) {
    startIdx = pQInfo->groupIndex;
    step = 1;
  } else {  // desc order copy all data
    startIdx = totalSubset - pQInfo->groupIndex - 1;
    step = -1;
  }

  for (int32_t i = startIdx; (i < totalSubset) && (i >= 0); i += step) {
    if (result[i].numOfRows == 0) {
      pQInfo->offset = 0;
      pQInfo->groupIndex += 1;
      continue;
    }

    assert(result[i].numOfRows >= 0 && pQInfo->offset <= 1);

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

  qTrace("QInfo:%p copy data to query buf completed", pQInfo);

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pQuery, numOfResult);
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
void copyFromWindowResToSData(SQInfo *pQInfo, SWindowResult *result) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSDB_ORDER_ASC;
  int32_t numOfResult = doCopyToSData(pQInfo, result, orderType);

  pQuery->rec.rows += numOfResult;

  assert(pQuery->rec.rows <= pQuery->rec.capacity);
}

static void updateWindowResNumOfRes(SQueryRuntimeEnv *pRuntimeEnv, STableQueryInfo *pTableQueryInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // update the number of result for each, only update the number of rows for the corresponding window result.
  if (pQuery->intervalTime == 0) {
    int32_t g = pTableQueryInfo->groupIdx;
    assert(pRuntimeEnv->windowResInfo.size > 0);

    SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, (char *)&g, sizeof(g));
    if (pWindowRes->numOfRows == 0) {
      pWindowRes->numOfRows = getNumOfResult(pRuntimeEnv);
    }
  }
}

void stableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, STableQueryInfo *pTableQueryInfo,
                                 SDataBlockInfo *pDataBlockInfo, SDataStatis *pStatis, SArray *pDataBlock,
                                 __block_search_fn_t searchFn) {
  SQuery *         pQuery = pRuntimeEnv->pQuery;
  SWindowResInfo * pWindowResInfo = &pTableQueryInfo->windowResInfo;
  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery)? 0 : pDataBlockInfo->rows - 1;
  
  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, pDataBlock);
  } else {
    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, searchFn, pDataBlock);
  }

  updateWindowResNumOfRes(pRuntimeEnv, pTableQueryInfo);
  updatelastkey(pQuery, pTableQueryInfo);
}

bool vnodeHasRemainResults(void *handle) {
  SQInfo *pQInfo = (SQInfo *)handle;

  if (pQInfo == NULL || pQInfo->runtimeEnv.pQuery->interpoType == TSDB_INTERPO_NONE) {
    return false;
  }

  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SInterpolationInfo *pInterpoInfo = &pRuntimeEnv->interpoInfo;
  if (pQuery->limit.limit > 0 && pQuery->rec.rows >= pQuery->limit.limit) {
    return false;
  }

  int32_t remain = taosNumOfRemainPoints(pInterpoInfo);
  if (remain > 0) {
    return true;
  } else {
    if (pRuntimeEnv->pInterpoBuf == NULL) {
      return false;
    }

    // query has completed
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      /*TSKEY ekey =*/taosGetRevisedEndKey(pQuery->window.ekey, pQuery->order.order, pQuery->intervalTime,
                                           pQuery->slidingTimeUnit, pQuery->precision);
      //      int32_t numOfTotal = taosGetNumOfResultWithInterpo(pInterpoInfo, (TSKEY
      //      *)pRuntimeEnv->pInterpoBuf[0]->data,
      //                                                         remain, pQuery->intervalTime, ekey,
      //                                                         pQuery->pointsToRead);
      //      return numOfTotal > 0;
      assert(0);
      return false;
    }

    return false;
  }
}

static UNUSED_FUNC int32_t resultInterpolate(SQInfo *pQInfo, tFilePage **data, tFilePage **pDataSrc, int32_t numOfRows,
                                             int32_t outputRows) {
#if 0
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = &pRuntimeEnv->pQuery;
  
  assert(pRuntimeEnv->pCtx[0].outputBytes == TSDB_KEYSIZE);
  
  // build support structure for performing interpolation
  SSchema *pSchema = calloc(1, sizeof(SSchema) * pQuery->numOfOutput);
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    pSchema[i].bytes = pRuntimeEnv->pCtx[i].outputBytes;
    pSchema[i].type = pQuery->pSelectExpr[i].type;
  }
  
//  SColumnModel *pModel = createColumnModel(pSchema, pQuery->numOfOutput, pQuery->pointsToRead);
  
  char *  srcData[TSDB_MAX_COLUMNS] = {0};
  int32_t functions[TSDB_MAX_COLUMNS] = {0};
  
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    srcData[i] = pDataSrc[i]->data;
    functions[i] = pQuery->pSelectExpr[i].base.functionId;
  }
  
  assert(0);
//  int32_t numOfRes = taosDoInterpoResult(&pRuntimeEnv->interpoInfo, pQuery->interpoType, data, numOfRows, outputRows,
//                                         pQuery->intervalTime, (int64_t *)pDataSrc[0]->data, pModel, srcData,
//                                         pQuery->defaultVal, functions, pRuntimeEnv->pTabObj->pointsPerFileBlock);
  
  destroyColumnModel(pModel);
  free(pSchema);
#endif
  return 0;
}

static void doCopyQueryResultToMsg(SQInfo *pQInfo, int32_t numOfRows, char *data) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
    int32_t bytes = pQuery->pSelectExpr[col].bytes;

    memmove(data, pQuery->sdata[col]->data, bytes * numOfRows);
    data += bytes * numOfRows;
  }

  // all data returned, set query over
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    setQueryStatus(pQuery, QUERY_OVER);
  }
}

int32_t vnodeQueryResultInterpolate(SQInfo *pQInfo, tFilePage **pDst, tFilePage **pDataSrc, int32_t numOfRows,
                                    int32_t *numOfInterpo) {
//  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
//  SQuery *          pQuery = pRuntimeEnv->pQuery;
#if 0
  while (1) {
    numOfRows = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);
    
    TSKEY   ekey = taosGetRevisedEndKey(pQuery->window.skey, pQuery->order.order, pQuery->intervalTime,
                                        pQuery->slidingTimeUnit, pQuery->precision);
    int32_t numOfFinalRows = taosGetNumOfResultWithInterpo(&pRuntimeEnv->interpoInfo, (TSKEY *)pDataSrc[0]->data,
                                                           numOfRows, pQuery->intervalTime, ekey, pQuery->pointsToRead);
    
    int32_t ret = resultInterpolate(pQInfo, pDst, pDataSrc, numOfRows, numOfFinalRows);
    assert(ret == numOfFinalRows);
    
    /* reached the start position of according to offset value, return immediately */
    if (pQuery->limit.offset == 0) {
      return ret;
    }
    
    if (pQuery->limit.offset < ret) {
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
      pQuery->limit.offset -= ret;
      ret = 0;
    }
    
    if (!vnodeHasRemainResults(pQInfo)) {
      return ret;
    }
  }
#endif

  return 0;
}

void vnodePrintQueryStatistics(SQInfo *pQInfo) {
#if 0
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pRuntimeEnv->pQuery;

  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;
  if (pRuntimeEnv->pResultBuf == NULL) {
    pSummary->tmpBufferInDisk = 0;
  } else {
    pSummary->tmpBufferInDisk = getResBufSize(pRuntimeEnv->pResultBuf);
  }
  
  qTrace("QInfo:%p statis: comp blocks:%d, size:%d Bytes, elapsed time:%.2f ms", pQInfo, pSummary->readCompInfo,
         pSummary->totalCompInfoSize, pSummary->loadCompInfoUs / 1000.0);
  
  qTrace("QInfo:%p statis: field info: %d, size:%d Bytes, avg size:%.2f Bytes, elapsed time:%.2f ms", pQInfo,
         pSummary->readField, pSummary->totalFieldSize, (double)pSummary->totalFieldSize / pSummary->readField,
         pSummary->loadFieldUs / 1000.0);
  
  qTrace(
      "QInfo:%p statis: file blocks:%d, size:%d Bytes, elapsed time:%.2f ms, skipped:%d, in-memory gen null:%d Bytes",
      pQInfo, pSummary->readDiskBlocks, pSummary->totalBlockSize, pSummary->loadBlocksUs / 1000.0,
      pSummary->skippedFileBlocks, pSummary->totalGenData);
  
  qTrace("QInfo:%p statis: cache blocks:%d", pQInfo, pSummary->blocksInCache, 0);
  qTrace("QInfo:%p statis: temp file:%d Bytes", pQInfo, pSummary->tmpBufferInDisk);
  
  qTrace("QInfo:%p statis: file:%d, table:%d", pQInfo, pSummary->numOfFiles, pSummary->numOfTables);
  qTrace("QInfo:%p statis: seek ops:%d", pQInfo, pSummary->numOfSeek);
  
  double total = pSummary->fileTimeUs + pSummary->cacheTimeUs;
  double io = pSummary->loadCompInfoUs + pSummary->loadBlocksUs + pSummary->loadFieldUs;
  
  // todo add the intermediate result save cost!!
  double computing = total - io;
  
  qTrace(
      "QInfo:%p statis: total elapsed time:%.2f ms, file:%.2f ms(%.2f%), cache:%.2f ms(%.2f%). io:%.2f ms(%.2f%),"
      "comput:%.2fms(%.2f%)",
      pQInfo, total / 1000.0, pSummary->fileTimeUs / 1000.0, pSummary->fileTimeUs * 100 / total,
      pSummary->cacheTimeUs / 1000.0, pSummary->cacheTimeUs * 100 / total, io / 1000.0, io * 100 / total,
      computing / 1000.0, computing * 100 / total);
#endif
}

static void updateOffsetVal(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pBlockInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  if (pQuery->limit.offset == pBlockInfo->rows) {  // current block will ignore completed
    pQuery->lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pBlockInfo->window.ekey + step : pBlockInfo->window.skey + step;
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
  TSKEY *keys = (TSKEY *)pColInfoData->pData;

  // update the offset value
  pQuery->lastKey = keys[pQuery->pos];
  pQuery->limit.offset = 0;

  int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey,
                                                &pRuntimeEnv->windowResInfo, pDataBlock);

  qTrace("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", rows:%d, res:%d", GET_QINFO_ADDR(pRuntimeEnv),
         pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes);
}

void skipBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0) {
    return;
  }

  pQuery->pos = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  TsdbQueryHandleT pQueryHandle = pRuntimeEnv->pQueryHandle;

  while (tsdbNextDataBlock(pQueryHandle)) {
    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      return;
    }

    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);

    if (pQuery->limit.offset > blockInfo.rows) {
      pQuery->limit.offset -= blockInfo.rows;
      pQuery->lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.window.ekey : blockInfo.window.skey;
      pQuery->lastKey += step;

      qTrace("QInfo:%p skip rows:%d, offset:%" PRId64 "", GET_QINFO_ADDR(pRuntimeEnv), blockInfo.rows,
             pQuery->limit.offset);
    } else {  // find the appropriated start position in current block
      updateOffsetVal(pRuntimeEnv, &blockInfo);
      break;
    }
  }
}

static bool skipTimeInterval(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // if queried with value filter, do NOT forward query start position
  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    return true;
  }

  /*
   * 1. for interval without interpolation query we forward pQuery->intervalTime at a time for
   *    pQuery->limit.offset times. Since hole exists, pQuery->intervalTime*pQuery->limit.offset value is
   *    not valid. otherwise, we only forward pQuery->limit.offset number of points
   */
  assert(pRuntimeEnv->windowResInfo.prevSKey == 0);

  TSKEY           skey1, ekey1;
  STimeWindow     w = {0};
  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

  while (tsdbNextDataBlock(pRuntimeEnv->pQueryHandle)) {
    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pRuntimeEnv->pQueryHandle);

    if (QUERY_IS_ASC_QUERY(pQuery) && pWindowResInfo->prevSKey == 0) {
      getAlignQueryTimeWindow(pQuery, blockInfo.window.skey, blockInfo.window.skey, pQuery->window.ekey, &skey1, &ekey1,
                              &w);
      pWindowResInfo->startTime = w.skey;
      pWindowResInfo->prevSKey = w.skey;
    } else {
      // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
      getAlignQueryTimeWindow(pQuery, blockInfo.window.ekey, pQuery->window.ekey, blockInfo.window.ekey, &skey1, &ekey1,
                              &w);

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
      getNextTimeWindow(pQuery, &tw);

      if (pQuery->limit.offset == 0) {
        if ((tw.skey <= blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (tw.ekey >= blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
          // load the data block
          SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
          SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);

          tw = win;
          int32_t startPos =
              getNextQualifiedWindow(pRuntimeEnv, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey);
          assert(startPos >= 0);

          // set the abort info
          pQuery->pos = startPos;
          pQuery->lastKey = ((TSKEY *)pColInfoData->pData)[startPos];
          pWindowResInfo->prevSKey = tw.skey;

          int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, NULL, binarySearchForKey,
                                                        &pRuntimeEnv->windowResInfo, pDataBlock);

          qTrace("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", rows:%d, res:%d",
                 GET_QINFO_ADDR(pRuntimeEnv), blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes);
          return true;
        } else {
          // do nothing,
          return true;
        }
      }

      // next time window starts from current data block
      if ((tw.skey <= blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (tw.ekey >= blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
        // load the data block, note that only the primary timestamp column is required
        SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
        SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);

        tw = win;
        int32_t startPos =
            getNextQualifiedWindow(pRuntimeEnv, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey);
        assert(startPos >= 0);

        // set the abort info
        pQuery->pos = startPos;
        pQuery->lastKey = ((TSKEY *)pColInfoData->pData)[startPos];
        pWindowResInfo->prevSKey = tw.skey;
        win = tw;
      } else {
        break;  // offset is not 0, and next time window locates in the next block.
      }
    }
  }

  return true;
}

int32_t doInitQInfo(SQInfo *pQInfo, void *param, void *tsdb, int32_t vgId, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  int32_t code = TSDB_CODE_SUCCESS;

  setScanLimitationByResultBuffer(pQuery);
  changeExecuteScanOrder(pQuery, false);

  // dataInCache requires lastKey value
  pQuery->lastKey = pQuery->window.skey;

  STsdbQueryCond cond = {
      .twindow = pQuery->window,
      .order = pQuery->order.order,
      .colList = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
  };
  
  
  // normal query setup the queryhandle here
  if (isFirstLastRowQuery(pQuery) && !isSTableQuery) {  // in case of last_row query, invoke a different API.
    pRuntimeEnv->pQueryHandle = tsdbQueryLastRow(tsdb, &cond, &pQInfo->tableIdGroupInfo);
  } else if (!isSTableQuery || isIntervalQuery(pQuery) || isFixedOutputQuery(pQuery)) {
    pRuntimeEnv->pQueryHandle = tsdbQueryTables(tsdb, &cond, &pQInfo->tableIdGroupInfo);
  }
  
  pQInfo->tsdb = tsdb;
  pQInfo->vgId = vgId;

  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pTSBuf = param;
  pRuntimeEnv->cur.vgroupIndex = -1;
  pRuntimeEnv->stableQuery = isSTableQuery;

  if (param != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTSBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTSBuf, order);
  }

  // create runtime environment
  code = setupQueryRuntimeEnv(pRuntimeEnv, pQuery->order.order);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pRuntimeEnv->numOfRowsPerPage = getNumOfRowsInResultPage(pQuery, isSTableQuery);

  if (isSTableQuery) {
    int32_t rows = getInitialPageNum(pQInfo);
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rows, pQuery->rowSize, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pQuery->intervalTime == 0) {
      int16_t type = TSDB_DATA_TYPE_NULL;

      if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // group by columns not tags;
        type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
      } else {
        type = TSDB_DATA_TYPE_INT;  // group id
      }

      initWindowResInfo(&pRuntimeEnv->windowResInfo, pRuntimeEnv, 512, 4096, type);
    }

  } else if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    int32_t rows = getInitialPageNum(pQInfo);
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rows, pQuery->rowSize, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    int16_t type = TSDB_DATA_TYPE_NULL;
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
      type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
    } else {
      type = TSDB_DATA_TYPE_TIMESTAMP;
    }

    initWindowResInfo(&pRuntimeEnv->windowResInfo, pRuntimeEnv, rows, 4096, type);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

//  SPointInterpoSupporter interpInfo = {0};
//  pointInterpSupporterInit(pQuery, &interpInfo);

  /*
   * in case of last_row query without query range, we set the query timestamp to be
   * STable->lastKey. Otherwise, keep the initial query time range unchanged.
   */
//  if (isFirstLastRowQuery(pQuery)) {
//    if (!normalizeUnBoundLastRowQuery(pQInfo, &interpInfo)) {
//      sem_post(&pQInfo->dataReady);
//      pointInterpSupporterDestroy(&interpInfo);
//      return TSDB_CODE_SUCCESS;
//    }
//  }

  /*
   * here we set the value for before and after the specified time into the
   * parameter for interpolation query
   */
//  pointInterpSupporterSetData(pQInfo, &interpInfo);
//  pointInterpSupporterDestroy(&interpInfo);

//  int64_t rs = taosGetIntervalStartTimestamp(pQuery->window.skey, pQuery->intervalTime, pQuery->slidingTimeUnit,
//                                             pQuery->precision);
//  taosInitInterpoInfo(&pRuntimeEnv->interpoInfo, pQuery->order.order, rs, 0, 0);
  //  allocMemForInterpo(pQInfo, pQuery, pMeterObj);

//  if (!isPointInterpoQuery(pQuery)) {
    //    assert(pQuery->pos >= 0 && pQuery->slot >= 0);
//  }

  // the pQuery->window.skey is changed during normalizedFirstQueryRange, so set the newest lastkey value
  pQuery->lastKey = pQuery->window.skey;
  return TSDB_CODE_SUCCESS;
}

static UNUSED_FUNC bool isGroupbyEachTable(SSqlGroupbyExpr *pGroupbyExpr, STableGroupInfo *pSidset) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndex* pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, i);
    if (pColIndex->flag == TSDB_COL_TAG) {
      return true;
    }
  }

  return false;
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

static int64_t queryOnDataBlocks(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int64_t st = taosGetTimestampMs();

  TsdbQueryHandleT *pQueryHandle = pRuntimeEnv->pQueryHandle;
  while (tsdbNextDataBlock(pQueryHandle)) {
    if (isQueryKilled(pQInfo)) {
      break;
    }

    SDataBlockInfo  blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);
    STableQueryInfo *pTableQueryInfo = NULL;

    // todo opt performance using hash table
    size_t numOfGroup = taosArrayGetSize(pQInfo->groupInfo.pGroupList);
    for (int32_t i = 0; i < numOfGroup; ++i) {
      SArray *group = taosArrayGetP(pQInfo->groupInfo.pGroupList, i);

      size_t num = taosArrayGetSize(group);
      for (int32_t j = 0; j < num; ++j) {
        SGroupItem *item = taosArrayGet(group, j);
        STableQueryInfo *pInfo = item->info;

        if (pInfo->id.tid == blockInfo.tid) {
          assert(pInfo->id.uid == blockInfo.uid);
          pTableQueryInfo = item->info;
          
          break;
        }
      }
      
      if (pTableQueryInfo != NULL) {
        break;
      }
    }

    assert(pTableQueryInfo != NULL && pTableQueryInfo != NULL);
    restoreIntervalQueryRange(pRuntimeEnv, pTableQueryInfo);

    SDataStatis *pStatis = NULL;
    SArray *     pDataBlock = loadDataBlockOnDemand(pRuntimeEnv, &blockInfo, &pStatis);

    TSKEY nextKey = blockInfo.window.skey;
    if (!isIntervalQuery(pQuery)) {
      setExecutionContext(pQInfo, pTableQueryInfo, &pTableQueryInfo->id, pTableQueryInfo->groupIdx, nextKey);
    } else {  // interval query
      setIntervalQueryRange(pTableQueryInfo, pQInfo, nextKey);
      int32_t ret = setAdditionalInfo(pQInfo, &pTableQueryInfo->id, pTableQueryInfo);

      if (ret != TSDB_CODE_SUCCESS) {
        pQInfo->code = ret;
        return taosGetTimestampMs() - st;
      }
    }

    stableApplyFunctionsOnBlock(pRuntimeEnv, pTableQueryInfo, &blockInfo, pStatis, pDataBlock, binarySearchForKey);
  }

  int64_t et = taosGetTimestampMs();
  return et - st;
}

static bool multiTableMultioutputHelper(SQInfo *pQInfo, int32_t index) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  SArray *group = taosArrayGetP(pQInfo->groupInfo.pGroupList, 0);
  SGroupItem* item = taosArrayGet(group, index);

  setTagVal(pRuntimeEnv, &item->id, pQInfo->tsdb);

  qTrace("QInfo:%p query on (%d): uid:%" PRIu64 ", tid:%d, qrange:%" PRId64 "-%" PRId64, pQInfo, index,
         item->id.uid, item->id.tid, item->info->lastKey, item->info->win.ekey);

  STsdbQueryCond cond = {
      .twindow   = {item->info->lastKey, item->info->win.ekey},
      .order     = pQuery->order.order,
      .colList   = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
  };

  SArray *g1 = taosArrayInit(1, POINTER_BYTES);
  SArray *tx = taosArrayInit(1, sizeof(STableId));

  taosArrayPush(tx, &item->info->id);
  taosArrayPush(g1, &tx);
  STableGroupInfo gp = {.numOfTables = 1, .pGroupList = g1};

  // include only current table
  if (pRuntimeEnv->pQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
    pRuntimeEnv->pQueryHandle = NULL;
  }
  
  pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &gp);
  taosArrayDestroy(tx);
  taosArrayDestroy(g1);
  
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

static UNUSED_FUNC int64_t doCheckTables(SQInfo *pQInfo, SArray* pTableList) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (!multiTableMultioutputHelper(pQInfo, 0)) {
    return 0;
  }

  SPointInterpoSupporter pointInterpSupporter = {0};
  pointInterpSupporterInit(pQuery, &pointInterpSupporter);

  /*
   * here we set the value for before and after the specified time into the
   * parameter for interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &pointInterpSupporter);
  pointInterpSupporterDestroy(&pointInterpSupporter);

  scanAllDataBlocks(pRuntimeEnv);

  // first/last_row query, do not invoke the finalize for super table query
  finalizeQueryResult(pRuntimeEnv);

  int64_t numOfRes = getNumOfResult(pRuntimeEnv);
  assert(numOfRes == 1 || numOfRes == 0);

  // accumulate the point interpolation result
  if (numOfRes > 0) {
    pQuery->rec.rows += numOfRes;
    forwardCtxOutputBuf(pRuntimeEnv, numOfRes);
  }

  return numOfRes;
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

  size_t numOfGroups = taosArrayGetSize(pQInfo->groupInfo.pGroupList);

  if (isPointInterpoQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pQInfo->groupIndex < numOfGroups) {
      SArray* group = taosArrayGetP(pQInfo->groupInfo.pGroupList, pQInfo->groupIndex);

      if (isFirstLastRowQuery(pQuery)) {
        qTrace("QInfo:%p last_row query on group:%d, total group:%d, current group:%d", pQInfo, pQInfo->groupIndex,
               numOfGroups);
  
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
        
        pRuntimeEnv->pQueryHandle = tsdbQueryLastRow(pQInfo->tsdb, &cond, &gp);
        
        initCtxOutputBuf(pRuntimeEnv);
        setTagVal(pRuntimeEnv, (STableId*) taosArrayGet(tx, 0), pQInfo->tsdb);
        scanAllDataBlocks(pRuntimeEnv);
        
        int64_t numOfRes = getNumOfResult(pRuntimeEnv);
        if (numOfRes > 0) {
          pQuery->rec.rows += numOfRes;
          forwardCtxOutputBuf(pRuntimeEnv, numOfRes);
        }
        
        skipResults(pRuntimeEnv);
        pQInfo->groupIndex += 1;
  
        // enable execution for next table, when handling the projection query
        enableExecutionForNextTable(pRuntimeEnv);
      }
    }
  } else {
    createTableQueryInfo(pQInfo);

    /*
     * 1. super table projection query, 2. group-by on normal columns query, 3. ts-comp query
     * if the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (pQInfo->groupIndex > 0) {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
      pQuery->rec.total += pQuery->rec.rows;

      if (pQuery->rec.rows > 0) {
        return;
      }
    }

    // all data have returned already
    if (pQInfo->tableIndex >= pQInfo->groupInfo.numOfTables) {
      return;
    }

    resetCtxOutputBuf(pRuntimeEnv);
    resetTimeWindowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);

    SArray *group = taosArrayGetP(pQInfo->groupInfo.pGroupList, 0);
    assert(taosArrayGetSize(group) == pQInfo->groupInfo.numOfTables &&
           1 == taosArrayGetSize(pQInfo->groupInfo.pGroupList));

    while (pQInfo->tableIndex < pQInfo->groupInfo.numOfTables) {
      if (isQueryKilled(pQInfo)) {
        return;
      }

      SGroupItem *item = taosArrayGet(group, pQInfo->tableIndex);
      
      STableQueryInfo *pInfo = item->info;
      if (pInfo->lastKey > 0) {
        pQuery->window.skey = pInfo->lastKey;
      }

      if (!multiTableMultioutputHelper(pQInfo, pQInfo->tableIndex)) {
        pQInfo->tableIndex++;
        continue;
      }

      //      SPointInterpoSupporter pointInterpSupporter = {0};

      // TODO handle the limit problem
      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        //        skipBlocks(pRuntimeEnv);

        if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
          pQInfo->tableIndex++;
          continue;
        }
      }

      scanAllDataBlocks(pRuntimeEnv);

      pQuery->rec.rows = getNumOfResult(pRuntimeEnv);
      skipResults(pRuntimeEnv);

      // the limitation of output result is reached, set the query completed
      if (limitResults(pQInfo)) {
        pQInfo->tableIndex = pQInfo->groupInfo.numOfTables;
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
        pInfo->lastKey = pQuery->lastKey;

        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL) /*||
            isGroupbyEachTable(pQuery->pGroupbyExpr, pSupporter->pSidSet)*/) {
          break;
        }

      } else {  // forward query range
        pQuery->window.skey = pQuery->lastKey;

        // all data in the result buffer are skipped due to the offset, continue to retrieve data from current meter
        if (pQuery->rec.rows == 0) {
          assert(!Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL));
          continue;
        } else {
          //          pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;
          //          // buffer is full, wait for the next round to retrieve data from current meter
          //          assert(Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          //          break;
        }
      }
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

  // todo refactor
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowStatus *pStatus = &pWindowResInfo->pResult[i].status;
      pStatus->closed = true;  // enable return all results for group by normal columns

      SWindowResult *pResult = &pWindowResInfo->pResult[i];
      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        pResult->numOfRows = MAX(pResult->numOfRows, pResult->resultInfo[j].numOfRes);
      }
    }

    pQInfo->groupIndex = 0;
    pQuery->rec.rows = 0;
    copyFromWindowResToSData(pQInfo, pWindowResInfo->pResult);
  }

  pQuery->rec.total += pQuery->rec.rows;

  qTrace(
      "QInfo %p, numOfTables:%d, index:%d, numOfGroups:%d, %d points returned, total:%"PRId64", offset:%" PRId64,
      pQInfo, pQInfo->groupInfo.numOfTables, pQInfo->tableIndex, numOfGroups, pQuery->rec.rows, pQuery->rec.total,
      pQuery->limit.offset);
}

static void createTableQueryInfo(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  // todo make sure the table are added the reference count to gauranteed that all involved tables are valid
  size_t numOfGroups = taosArrayGetSize(pQInfo->groupInfo.pGroupList);

  int32_t index = 0;
  for (int32_t i = 0; i < numOfGroups; ++i) {  // load all meter meta info
    SArray *group = *(SArray **)taosArrayGet(pQInfo->groupInfo.pGroupList, i);

    size_t s = taosArrayGetSize(group);
    for (int32_t j = 0; j < s; ++j) {
      SGroupItem* item = (SGroupItem *)taosArrayGet(group, j);

      // STableQueryInfo has been created for each table
      if (item->info != NULL) {
        return;
      }

      STableQueryInfo* pInfo = createTableQueryInfoImpl(&pQInfo->runtimeEnv, item->id, pQuery->window);
      pInfo->groupIdx = i;
      pInfo->tableIndex = index;
      
      item->info = pInfo;
      index += 1;
    }
  }
}

static void prepareQueryInfoForReverseScan(SQInfo *pQInfo) {
  //  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  //  for (int32_t i = 0; i < pQInfo->groupInfo.numOfTables; ++i) {
  //    STableQueryInfo *pTableQueryInfo = pQInfo->pTableQueryInfo[i].pTableQInfo;
  //    changeMeterQueryInfoForSuppleQuery(pQuery, pTableQueryInfo);
  //  }
}

static void doSaveContext(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);
  disableFuncForReverseScan(pQInfo, pQuery->order.order);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1u;
  }

  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  prepareQueryInfoForReverseScan(pQInfo);
}

static void doRestoreContext(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1;
  }

  switchCtxOrder(pRuntimeEnv);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
}

static void doCloseAllTimeWindowAfterScan(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if (isIntervalQuery(pQuery)) {
    size_t numOfGroup = taosArrayGetSize(pQInfo->groupInfo.pGroupList);
    for (int32_t i = 0; i < numOfGroup; ++i) {
      SArray *group = taosArrayGetP(pQInfo->groupInfo.pGroupList, i);

      size_t num = taosArrayGetSize(group);
      for (int32_t j = 0; j < num; ++j) {
        SGroupItem* item = taosArrayGet(group, j);
        closeAllTimeWindow(&item->info->windowResInfo);
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
    if (isIntervalQuery(pQuery)) {
      copyResToQueryResultBuf(pQInfo, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->num);
#endif
    } else {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    }

    pQuery->rec.rows += pQuery->rec.rows;

    if (pQuery->rec.rows == 0) {
      //      vnodePrintQueryStatistics(pSupporter);
    }

    qTrace("QInfo:%p current:%lld, total:%lld", pQInfo, pQuery->rec.rows, pQuery->rec.total);
    return;
  }

  qTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", order:%d, forward scan start", pQInfo,
         pQuery->window.skey, pQuery->window.ekey, pQuery->order.order);

  // create the query support structures
  createTableQueryInfo(pQInfo);

  // do check all qualified data blocks
  int64_t el = queryOnDataBlocks(pQInfo);
  qTrace("QInfo:%p forward scan completed, elapsed time: %lldms, reversed scan start", pQInfo, el);

  // query error occurred or query is killed, abort current execution
  if (pQInfo->code != TSDB_CODE_SUCCESS || isQueryKilled(pQInfo)) {
    qTrace("QInfo:%p query killed or error occurred, code:%d, abort", pQInfo, pQInfo->code);
    return;
  }

  // close all time window results
  doCloseAllTimeWindowAfterScan(pQInfo);

  if (needReverseScan(pQuery)) {
    doSaveContext(pQInfo);

    el = queryOnDataBlocks(pQInfo);
    qTrace("QInfo:%p reversed scan completed, elapsed time: %lldms", pQInfo, el);

    doRestoreContext(pQInfo);
  } else {
    qTrace("QInfo:%p no need to do reversed scan, query completed", pQInfo);
  }

  setQueryStatus(pQuery, QUERY_COMPLETED);

  if (pQInfo->code != TSDB_CODE_SUCCESS || isQueryKilled(pQInfo)) {
    qTrace("QInfo:%p query killed or error occurred, code:%d, abort", pQInfo, pQInfo->code);
    return;
  }

  if (isIntervalQuery(pQuery) || isSumAvgRateQuery(pQuery)) {
    if (mergeIntoGroupResult(pQInfo) == TSDB_CODE_SUCCESS) {
      copyResToQueryResultBuf(pQInfo, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->num);
#endif
    }
  } else {  // not a interval query
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
  }

  // handle the limitation of output buffer
  qTrace("QInfo:%p points returned:%d, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
}

/*
 * in each query, this function will be called only once, no retry for further result.
 *
 * select count(*)/top(field,k)/avg(field name) from table_name [where ts>now-1a];
 * select count(*) from table_name group by status_column;
 */
static void tableFixedOutputProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  scanAllDataBlocks(pRuntimeEnv);
  finalizeQueryResult(pRuntimeEnv);

  if (isQueryKilled(pQInfo)) {
    return;
  }

  // since the numOfOutputElems must be identical for all sql functions that are allowed to be executed simutanelously.
  pQuery->rec.rows = getNumOfResult(pRuntimeEnv);

  // must be top/bottom query if offset > 0
  if (pQuery->limit.offset > 0) {
    assert(isTopBottomQuery(pQuery));
  }

  skipResults(pRuntimeEnv);
  limitResults(pQInfo);
}

static void tableMultiOutputProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

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
    scanAllDataBlocks(pRuntimeEnv);
    finalizeQueryResult(pRuntimeEnv);

    if (isQueryKilled(pQInfo)) {
      return;
    }

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

    qTrace("QInfo:%p vid:%d sid:%d id:%s, skip current result, offset:%" PRId64 ", next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pQuery->limit.offset, pQuery->lastKey);

    resetCtxOutputBuf(pRuntimeEnv);
  }

  limitResults(pQInfo);
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    qTrace("QInfo:%p query paused due to output limitation, next qrange:%" PRId64 "-%" PRId64, pQInfo, pQuery->lastKey,
           pQuery->window.ekey);
  }

  if (!isTSCompQuery(pQuery)) {
    assert(pQuery->rec.rows <= pQuery->rec.capacity);
  }
}

static void tableIntervalProcessImpl(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    scanAllDataBlocks(pRuntimeEnv);

    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      return;
    }

    assert(!Q_STATUS_EQUAL(pQuery->status, QUERY_NOT_COMPLETED));
    finalizeQueryResult(pRuntimeEnv);

    // here we can ignore the records in case of no interpolation
    // todo handle offset, in case of top/bottom interval query
    if ((pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) && pQuery->limit.offset > 0 &&
        pQuery->interpoType == TSDB_INTERPO_NONE) {
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
static void tableIntervalProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &(pQInfo->runtimeEnv);
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfInterpo = 0;

  // skip blocks without load the actual data block from file if no filter condition present
  skipTimeInterval(pRuntimeEnv);
  if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return;
  }

  while (1) {
    tableIntervalProcessImpl(pRuntimeEnv);

    if (isIntervalQuery(pQuery)) {
      pQInfo->groupIndex = 0;  // always start from 0
      pQuery->rec.rows = 0;
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);

      clearFirstNTimeWindow(pRuntimeEnv, pQInfo->groupIndex);
    }

    // the offset is handled at prepare stage if no interpolation involved
    if (pQuery->interpoType == TSDB_INTERPO_NONE) {
      limitResults(pQInfo);
      break;
    } else {
      taosInterpoSetStartInfo(&pRuntimeEnv->interpoInfo, pQuery->rec.rows, pQuery->interpoType);
      SData **pInterpoBuf = pRuntimeEnv->pInterpoBuf;

      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        memcpy(pInterpoBuf[i]->data, pQuery->sdata[i]->data, pQuery->rec.rows * pQuery->pSelectExpr[i].bytes);
      }

      numOfInterpo = 0;
      pQuery->rec.rows = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata, (tFilePage **)pInterpoBuf,
                                                     pQuery->rec.rows, &numOfInterpo);

      qTrace("QInfo: %p interpo completed, final:%d", pQInfo, pQuery->rec.rows);
      if (pQuery->rec.rows > 0 || Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
        limitResults(pQInfo);
        break;
      }

      // no result generated yet, continue retrieve data
      pQuery->rec.rows = 0;
    }
  }

  // all data scanned, the group by normal column can return
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // todo refactor with merge interval time result
    pQInfo->groupIndex = 0;
    pQuery->rec.rows = 0;
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    clearFirstNTimeWindow(pRuntimeEnv, pQInfo->groupIndex);
  }

  pQInfo->pointsInterpo += numOfInterpo;
}

static void tableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (vnodeHasRemainResults(pQInfo)) {
    /*
     * There are remain results that are not returned due to result interpolation
     * So, we do keep in this procedure instead of launching retrieve procedure for next results.
     */
    int32_t numOfInterpo = 0;
    int32_t remain = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);
    pQuery->rec.rows = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata,
                                                   (tFilePage **)pRuntimeEnv->pInterpoBuf, remain, &numOfInterpo);

    limitResults(pQInfo);

    pQInfo->pointsInterpo += numOfInterpo;
    qTrace("QInfo:%p current:%d returned, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total);
    return;
  }

  // here we have scan all qualified data in both data file and cache
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    // continue to get push data from the group result
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr) ||
        ((isIntervalQuery(pQuery) && pQuery->rec.total < pQuery->limit.limit))) {
      // todo limit the output for interval query?
      pQuery->rec.rows = 0;
      pQInfo->groupIndex = 0;  // always start from 0

      if (pRuntimeEnv->windowResInfo.size > 0) {
        copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
        pQuery->rec.rows += pQuery->rec.rows;

        clearFirstNTimeWindow(pRuntimeEnv, pQInfo->groupIndex);

        if (pQuery->rec.rows > 0) {
          qTrace("QInfo:%p %d rows returned from group results, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total);
          return;
        }
      }
    }

    qTrace("QInfo:%p query over, %d rows are returned", pQInfo, pQuery->rec.total);
    //    vnodePrintQueryStatistics(pSupporter);
    return;
  }

  // number of points returned during this query
  pQuery->rec.rows = 0;
  int64_t st = taosGetTimestampUs();

  // group by normal column, sliding window query, interval query are handled by interval query processor
  if (isIntervalQuery(pQuery) || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // interval (down sampling operation)
    tableIntervalProcess(pQInfo);
  } else if (isFixedOutputQuery(pQuery)) {
    tableFixedOutputProcess(pQInfo);
  } else {  // diff/add/multiply/subtract/division
    assert(pQuery->checkBuffer == 1);
    tableMultiOutputProcess(pQInfo);
  }

  // record the total elapsed time
  pQInfo->elapsedTime += (taosGetTimestampUs() - st);
  assert(pQInfo->groupInfo.numOfTables == 1);

  /* check if query is killed or not */
  if (isQueryKilled(pQInfo)) {
    qTrace("QInfo:%p query is killed", pQInfo);
  } else {// todo set the table uid and tid in log
//    SArray* p = taosArrayGetP(pQInfo->groupInfo.pGroupList, 0);
//    SPair* pair = taosArrayGet(p, 0);
    qTrace("QInfo:%p query paused, %" PRId64 " rows returned, numOfTotal:%" PRId64 " rows",
        pQInfo, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
  }
}

static void stableQueryImpl(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  pQuery->rec.rows = 0;

  int64_t st = taosGetTimestampUs();

  if (isIntervalQuery(pQuery) ||
      (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)) && !isGroupbyNormalCol(pQuery->pGroupbyExpr))) {
    multiTableQueryProcess(pQInfo);
  } else {
    assert((pQuery->checkBuffer == 1 && pQuery->intervalTime == 0) || isPointInterpoQuery(pQuery) ||
           isGroupbyNormalCol(pQuery->pGroupbyExpr));

    sequentialTableProcess(pQInfo);
  }

  // record the total elapsed time
  pQInfo->elapsedTime += (taosGetTimestampUs() - st);
  //  taosInterpoSetStartInfo(&pQInfo->runtimeEnv.interpoInfo, pQuery->size, pQInfo->query.interpoType);

  if (pQuery->rec.rows == 0) {
    qTrace("QInfo:%p over, %d tables queried, %d points are returned", pQInfo, pQInfo->groupInfo.numOfTables,
           pQuery->rec.total);
    //    vnodePrintQueryStatistics(pSupporter);
  }
}

static int32_t getColumnIndexInSource(SQueryTableMsg *pQueryMsg, SSqlFuncMsg *pExprMsg, SColumnInfo* pTagCols) {
  int32_t j = 0;
  
  if (TSDB_COL_IS_TAG(pExprMsg->colInfo.flag)) {
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
    qError("qmsg:%p illegal value of interval time %" PRId64 "", pQueryMsg, pQueryMsg->intervalTime);
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
      if (pExprMsg[i]->functionId != TSDB_FUNC_TAGPRJ) {
        return false;
      }
    }
  }
  
  return true;
}

static char *createTableIdList(SQueryTableMsg *pQueryMsg, char *pMsg, SArray **pTableIdList) {
  assert(pQueryMsg->numOfTables > 0);

  *pTableIdList = taosArrayInit(pQueryMsg->numOfTables, sizeof(STableId));

  STableIdInfo *pTableIdInfo = (STableIdInfo *)pMsg;
  pTableIdInfo->tid = htonl(pTableIdInfo->tid);
  pTableIdInfo->uid = htobe64(pTableIdInfo->uid);
  pTableIdInfo->key = htobe64(pTableIdInfo->key);

  STableId id = {.uid = pTableIdInfo->uid, .tid = pTableIdInfo->tid};
  taosArrayPush(*pTableIdList, &id);

  pMsg += sizeof(STableIdInfo);

  for (int32_t j = 1; j < pQueryMsg->numOfTables; ++j) {
    pTableIdInfo = (STableIdInfo *)pMsg;

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
  pQueryMsg->numOfTables = htonl(pQueryMsg->numOfTables);

  pQueryMsg->window.skey = htobe64(pQueryMsg->window.skey);
  pQueryMsg->window.ekey = htobe64(pQueryMsg->window.ekey);
  pQueryMsg->intervalTime = htobe64(pQueryMsg->intervalTime);
  pQueryMsg->slidingTime = htobe64(pQueryMsg->slidingTime);
  pQueryMsg->limit = htobe64(pQueryMsg->limit);
  pQueryMsg->offset = htobe64(pQueryMsg->offset);

  pQueryMsg->order = htons(pQueryMsg->order);
  pQueryMsg->orderColId = htons(pQueryMsg->orderColId);
  pQueryMsg->queryType = htons(pQueryMsg->queryType);
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
    return TSDB_CODE_INVALID_QUERY_MSG;
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
      SColumnFilterInfo *pFilterInfo = (SColumnFilterInfo *)pMsg;
      SColumnFilterInfo *pDestFilterInfo = &pColInfo->filters[f];

      pDestFilterInfo->filterstr = htons(pFilterInfo->filterstr);

      pMsg += sizeof(SColumnFilterInfo);

      if (pDestFilterInfo->filterstr) {
        pDestFilterInfo->len = htobe64(pFilterInfo->len);

        pDestFilterInfo->pz = (int64_t) calloc(1, pDestFilterInfo->len);
        memcpy((void *)pDestFilterInfo->pz, pMsg, pDestFilterInfo->len);
        pMsg += (pDestFilterInfo->len);
      } else {
        pDestFilterInfo->lowerBndi = htobe64(pFilterInfo->lowerBndi);
        pDestFilterInfo->upperBndi = htobe64(pFilterInfo->upperBndi);
      }

      pDestFilterInfo->lowerRelOptr = htons(pFilterInfo->lowerRelOptr);
      pDestFilterInfo->upperRelOptr = htons(pFilterInfo->upperRelOptr);
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

    if (pExprMsg->functionId == TSDB_FUNC_TAG || pExprMsg->functionId == TSDB_FUNC_TAGPRJ ||
               pExprMsg->functionId == TSDB_FUNC_TAG_DUMMY) {
      if (pExprMsg->colInfo.flag != TSDB_COL_TAG) {  // ignore the column  index check for arithmetic expression.
        return TSDB_CODE_INVALID_QUERY_MSG;
      }
    } else {
//      if (!validateExprColumnInfo(pQueryMsg, pExprMsg)) {
//        return TSDB_CODE_INVALID_QUERY_MSG;
//      }
    }

    pExprMsg = (SSqlFuncMsg *)pMsg;
  }
  
  if (!validateQuerySourceCols(pQueryMsg, *pExpr)) {
    tfree(*pExpr);
    
    return TSDB_CODE_INVALID_QUERY_MSG;
  }

  pMsg = createTableIdList(pQueryMsg, pMsg, pTableIdList);

  if (pQueryMsg->numOfGroupCols > 0) {  // group by tag columns
    *groupbyCols = malloc(pQueryMsg->numOfGroupCols * sizeof(SColIndex));

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

  pQueryMsg->interpoType = htons(pQueryMsg->interpoType);
  if (pQueryMsg->interpoType != TSDB_INTERPO_NONE) {
    pQueryMsg->defaultVal = (uint64_t)(pMsg);

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
    strcpy(*tbnameCond, pMsg);
    pMsg += len;
  }
  
  qTrace("qmsg:%p query %d tables, qrange:%" PRId64 "-%" PRId64 ", numOfGroupbyTagCols:%d, order:%d, "
         "outputCols:%d, numOfCols:%d, interval:%" PRId64 ", fillType:%d, comptsLen:%d, limit:%" PRId64 ", offset:%" PRId64,
         pQueryMsg, pQueryMsg->numOfTables, pQueryMsg->window.skey, pQueryMsg->window.ekey, pQueryMsg->numOfGroupCols,
         pQueryMsg->order, pQueryMsg->numOfOutput, pQueryMsg->numOfCols, pQueryMsg->intervalTime,
         pQueryMsg->interpoType, pQueryMsg->tsLen, pQueryMsg->limit, pQueryMsg->offset);

  return 0;
}

static int32_t buildAirthmeticExprFromMsg(SExprInfo *pArithExprInfo, SQueryTableMsg *pQueryMsg) {
  qTrace("qmsg:%p create arithmetic expr from binary string", pQueryMsg, pArithExprInfo->base.arg[0].argValue.pz);

  tExprNode* pExprNode = NULL;
  TRY(32) {
    pExprNode = exprTreeFromBinary(pArithExprInfo->base.arg[0].argValue.pz, pArithExprInfo->base.arg[0].argBytes);
  } CATCH( code ) {
    CLEANUP_EXECUTE();
    return code;
  } END_TRY

  if (pExprNode == NULL) {
    qError("qmsg:%p failed to create arithmetic expression string from:%s", pQueryMsg, pArithExprInfo->base.arg[0].argValue.pz);
    return TSDB_CODE_APP_ERROR;
  }
  
  pArithExprInfo->pExpr = pExprNode;
  return TSDB_CODE_SUCCESS;
}

static int32_t createSqlFunctionExprFromMsg(SQueryTableMsg *pQueryMsg, SExprInfo **pSqlFuncExpr,
                                            SSqlFuncMsg **pExprMsg, SColumnInfo* pTagCols) {
  *pSqlFuncExpr = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SExprInfo *pExprs = (SExprInfo *)calloc(1, sizeof(SExprInfo) * pQueryMsg->numOfOutput);
  if (pExprs == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
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
    } else if (pExprs[i].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {  // parse the normal column
      type  = TSDB_DATA_TYPE_BINARY;
      bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
    } else{
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].base, pTagCols);
      assert(j < pQueryMsg->numOfCols || j < pQueryMsg->numOfTags);

      SColumnInfo* pCol = (TSDB_COL_IS_TAG(pExprs[i].base.colInfo.flag))? &pTagCols[j]:&pQueryMsg->colList[j];
      type = pCol->type;
      bytes = pCol->bytes;
    }

    int32_t param = pExprs[i].base.arg[0].argValue.i64;
    if (getResultDataInfo(type, bytes, pExprs[i].base.functionId, param, &pExprs[i].type, &pExprs[i].bytes,
                          &pExprs[i].interResBytes, 0, isSuperTable) != TSDB_CODE_SUCCESS) {
      tfree(pExprs);
      return TSDB_CODE_INVALID_QUERY_MSG;
    }

    if (pExprs[i].base.functionId == TSDB_FUNC_TAG_DUMMY || pExprs[i].base.functionId == TSDB_FUNC_TS_DUMMY) {
      tagLen += pExprs[i].bytes;
    }
    assert(isValidDataType(pExprs[i].type, pExprs[i].bytes));
  }

  // get the correct result size for top/bottom query, according to the number of tags columns in selection clause

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
                            &pExprs[i].type, &pExprs[i].bytes, &pExprs[i].interResBytes, tagLen, isSuperTable);
      assert(ret == TSDB_CODE_SUCCESS);
    }
  }

  tfree(pExprMsg);
  *pSqlFuncExpr = pExprs;

  return TSDB_CODE_SUCCESS;
}

static SSqlGroupbyExpr *createGroupbyExprFromMsg(SQueryTableMsg *pQueryMsg, SColIndex *pColIndex, int32_t *code) {
  if (pQueryMsg->numOfGroupCols == 0) {
    return NULL;
  }

  // using group by tag columns
  SSqlGroupbyExpr *pGroupbyExpr = (SSqlGroupbyExpr *)calloc(1, sizeof(SSqlGroupbyExpr));
  if (pGroupbyExpr == NULL) {
    *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
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

      memcpy(&pFilterInfo->info, &pQuery->colList[i], sizeof(SColumnInfoData));
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
          return TSDB_CODE_INVALID_QUERY_MSG;
        }

        int16_t type  = pQuery->colList[i].type;
        int16_t bytes = pQuery->colList[i].bytes;

        // todo refactor
        __filter_func_t *rangeFilterArray = getRangeFilterFuncArray(type);
        __filter_func_t *filterArray = getValueFilterFuncArray(type);

        if (rangeFilterArray == NULL && filterArray == NULL) {
          qError("QInfo:%p failed to get filter function, invalid data type:%d", pQInfo, type);
          return TSDB_CODE_INVALID_QUERY_MSG;
        }

        if ((lower == TSDB_RELATION_GREATER_EQUAL || lower == TSDB_RELATION_GREATER) &&
            (upper == TSDB_RELATION_LESS_EQUAL || upper == TSDB_RELATION_LESS)) {
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
          if (lower != TSDB_RELATION_INVALID) {
            pSingleColFilter->fp = filterArray[lower];

            if (upper != TSDB_RELATION_INVALID) {
              qError("pQInfo:%p failed to get filter function, invalid filter condition", pQInfo, type);
              return TSDB_CODE_INVALID_QUERY_MSG;
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
    if (pSqlExprMsg->functionId == TSDB_FUNC_ARITHM || pSqlExprMsg->colInfo.flag == TSDB_COL_TAG) {
      continue;
    }

    SColIndex *pColIndexEx = &pSqlExprMsg->colInfo;
    if (!TSDB_COL_IS_TAG(pColIndexEx->flag)) {
      for (int32_t f = 0; f < pQuery->numOfCols; ++f) {
        if (pColIndexEx->colId == pQuery->colList[f].colId) {
          pColIndexEx->colIndex = f;
          break;
        }
      }
    } else {
      for (int32_t f = 0; f < pQuery->numOfTags; ++f) {
        if (pColIndexEx->colId == pQuery->tagColList[f].colId) {
          pColIndexEx->colIndex = f;
          break;
        }
      }
    }
  }
}

static SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SSqlGroupbyExpr *pGroupbyExpr, SExprInfo *pExprs,
                               STableGroupInfo *groupInfo, SColumnInfo* pTagCols) {
  SQInfo *pQInfo = (SQInfo *)calloc(1, sizeof(SQInfo));
  if (pQInfo == NULL) {
    return NULL;
  }

  SQuery *pQuery = calloc(1, sizeof(SQuery));
  pQInfo->runtimeEnv.pQuery = pQuery;

  int16_t numOfCols = pQueryMsg->numOfCols;
  int16_t numOfOutput = pQueryMsg->numOfOutput;

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
  pQuery->interpoType     = pQueryMsg->interpoType;
  pQuery->numOfTags       = pQueryMsg->numOfTags;

  // todo do not allocate ??
  pQuery->colList = calloc(numOfCols, sizeof(SSingleColumnFilterInfo));
  if (pQuery->colList == NULL) {
    goto _cleanup;
  }

  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i] = pQueryMsg->colList[i];

    SColumnInfo *pColInfo = &pQuery->colList[i];
    pColInfo->filters = tscFilterInfoClone(pQueryMsg->colList[i].filters, pColInfo->numOfFilters);
  }
  
  pQuery->tagColList = pTagCols;
  
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
  pQuery->sdata = (SData **)calloc(pQuery->numOfOutput, POINTER_BYTES);
  if (pQuery->sdata == NULL) {
    goto _cleanup;
  }

  // set the output buffer capacity
  pQuery->rec.capacity = 4096;
  pQuery->rec.threshold = 4000;

  for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
    assert(pExprs[col].interResBytes >= pExprs[col].bytes);

    // allocate additional memory for interResults that are usually larger then final results
    size_t size = (pQuery->rec.capacity + 1) * pExprs[col].bytes + pExprs[col].interResBytes + sizeof(SData);
    pQuery->sdata[col] = (SData *)calloc(1, size);
    if (pQuery->sdata[col] == NULL) {
      goto _cleanup;
    }
  }

  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    pQuery->defaultVal = malloc(sizeof(int64_t) * pQuery->numOfOutput);
    if (pQuery->defaultVal == NULL) {
      goto _cleanup;
    }

    // the first column is the timestamp
    memcpy(pQuery->defaultVal, (char *)pQueryMsg->defaultVal, pQuery->numOfOutput * sizeof(int64_t));
  }

  // to make sure third party won't overwrite this structure
  pQInfo->signature = pQInfo;
  
  pQInfo->tableIdGroupInfo = *groupInfo;
  size_t numOfGroups = taosArrayGetSize(groupInfo->pGroupList);
  
  pQInfo->groupInfo.pGroupList = taosArrayInit(numOfGroups, POINTER_BYTES);
  pQInfo->groupInfo.numOfTables = groupInfo->numOfTables;
  
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray* pa = taosArrayGetP(groupInfo->pGroupList, i);
    size_t s = taosArrayGetSize(pa);
    
    SArray* p1 = taosArrayInit(s, sizeof(SGroupItem));
    
    for(int32_t j = 0; j < s; ++j) {
      SGroupItem item = { .id = *(STableId*) taosArrayGet(pa, j), .info = NULL, };
      taosArrayPush(p1, &item);
    }
    
    taosArrayPush(pQInfo->groupInfo.pGroupList, &p1);
  }

  pQuery->pos = -1;

  pQuery->window = pQueryMsg->window;
  pQuery->lastKey = pQuery->window.skey;

  if (sem_init(&pQInfo->dataReady, 0, 0) != 0) {
    qError("QInfo:%p init dataReady sem failed, reason:%s", pQInfo, strerror(errno));
    goto _cleanup;
  }

  vnodeParametersSafetyCheck(pQuery);

  qTrace("qmsg:%p QInfo:%p created", pQueryMsg, pQInfo);
  return pQInfo;

_cleanup:
  tfree(pQuery->defaultVal);

  if (pQuery->sdata != NULL) {
    for (int16_t col = 0; col < pQuery->numOfOutput; ++col) {
      tfree(pQuery->sdata[col]);
    }
  }

  tfree(pQuery->sdata);
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);

  tfree(pExprs);
  tfree(pGroupbyExpr);

  tfree(pQInfo);

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

static void freeQInfo(SQInfo *pQInfo);

static int32_t initQInfo(SQueryTableMsg *pQueryMsg, void *tsdb, int32_t vgId, SQInfo *pQInfo, bool isSTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  STSBuf *pTSBuf = NULL;
  if (pQueryMsg->tsLen > 0) {  // open new file to save the result
    char *tsBlock = (char *)pQueryMsg + pQueryMsg->tsOffset;
    pTSBuf = tsBufCreateFromCompBlocks(tsBlock, pQueryMsg->tsNumOfBlocks, pQueryMsg->tsLen, pQueryMsg->tsOrder);

    tsBufResetPos(pTSBuf);
    tsBufNextPos(pTSBuf);
  }

  // only the successful complete requries the sem_post/over = 1 operations.
  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.skey > pQuery->window.ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.ekey > pQuery->window.skey))) {
    qTrace("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->window.skey,
           pQuery->window.ekey, pQuery->order.order);
    setQueryStatus(pQuery, QUERY_COMPLETED);

    sem_post(&pQInfo->dataReady);
    return TSDB_CODE_SUCCESS;
  }

  // filter the qualified
  if ((code = doInitQInfo(pQInfo, pTSBuf, tsdb, vgId, isSTable)) != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  // qTrace("QInfo:%p set query flag and prepare runtime environment completed, ref:%d, wait for schedule", pQInfo,
  //       pQInfo->refCount);
  return code;

_error:
  // table query ref will be decrease during error handling
  freeQInfo(pQInfo);
  return code;
}

static void freeQInfo(SQInfo *pQInfo) {
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  setQueryKilled(pQInfo);

  qTrace("QInfo:%p start to free QInfo", pQInfo);
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

  if (pQuery->defaultVal != NULL) {
    tfree(pQuery->defaultVal);
  }

  // todo refactor, extract method to destroytableDataInfo
  int32_t numOfGroups = taosArrayGetSize(pQInfo->groupInfo.pGroupList);
  for (int32_t i = 0; i < numOfGroups; ++i) {
    SArray *p = taosArrayGetP(pQInfo->groupInfo.pGroupList, i);
    
    size_t num = taosArrayGetSize(p);
    for(int32_t j = 0; j < num; ++j) {
      SGroupItem* item = taosArrayGet(p, j);
      if (item->info != NULL) {
        destroyTableQueryInfo(item->info, pQuery->numOfOutput);
      }
    }
    
    taosArrayDestroy(p);
  }
  
  taosArrayDestroy(pQInfo->groupInfo.pGroupList);
  
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray* p = taosArrayGetP(pQInfo->tableIdGroupInfo.pGroupList, i);
    taosArrayDestroy(p);
  }
  
  taosArrayDestroy(pQInfo->tableIdGroupInfo.pGroupList);
  
  if (pQuery->pGroupbyExpr != NULL) {
    taosArrayDestroy(pQuery->pGroupbyExpr->columnInfo);
    tfree(pQuery->pGroupbyExpr);
  }
  
  tfree(pQuery->tagColList);
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);
  tfree(pQuery->sdata);
  
  tfree(pQuery);
  
  qTrace("QInfo:%p QInfo is freed", pQInfo);

  // destroy signature, in order to avoid the query process pass the object safety check
  memset(pQInfo, 0, sizeof(SQInfo));
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
      size_t s = lseek(fd, 0, SEEK_END);
      qTrace("QInfo:%p ts comp data return, file:%s, size:%zu", pQInfo, pQuery->sdata[0]->data, s);

      lseek(fd, 0, SEEK_SET);
      read(fd, data, s);
      close(fd);

      unlink(pQuery->sdata[0]->data);
    } else {
      // todo return the error code to client
      qError("QInfo:%p failed to open tmp file to send ts-comp data to client, path:%s, reason:%s", pQInfo,
             pQuery->sdata[0]->data, strerror(errno));
    }
  
    // all data returned, set query over
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      setQueryStatus(pQuery, QUERY_OVER);
    }
  } else {
    doCopyQueryResultToMsg(pQInfo, pQuery->rec.rows, data);
  }

  pQuery->rec.total += pQuery->rec.rows;
  qTrace("QInfo:%p current:%d, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total);

  return TSDB_CODE_SUCCESS;

  // todo if interpolation exists, the result may be dump to client by several rounds
}

int32_t qCreateQueryInfo(void *tsdb, int32_t vgId, SQueryTableMsg *pQueryMsg, qinfo_t *pQInfo) {
  assert(pQueryMsg != NULL);

  int32_t code = TSDB_CODE_SUCCESS;

  char *        tagCond = NULL, *tbnameCond = NULL;
  SArray *      pTableIdList = NULL;
  SSqlFuncMsg **pExprMsg = NULL;
  SColIndex *   pGroupColIndex = NULL;
  SColumnInfo*  pTagColumnInfo = NULL;

  if ((code = convertQueryMsg(pQueryMsg, &pTableIdList, &pExprMsg, &tagCond, &tbnameCond, &pGroupColIndex, &pTagColumnInfo)) !=
         TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pQueryMsg->numOfTables <= 0) {
    qError("Invalid number of tables to query, numOfTables:%d", pQueryMsg->numOfTables);
    code = TSDB_CODE_INVALID_QUERY_MSG;
    goto _query_over;
  }

  if (pTableIdList == NULL || taosArrayGetSize(pTableIdList) == 0) {
    qError("qmsg:%p, SQueryTableMsg wrong format", pQueryMsg);
    code = TSDB_CODE_INVALID_QUERY_MSG;
    goto _query_over;
  }

  SExprInfo *pExprs = NULL;
  if ((code = createSqlFunctionExprFromMsg(pQueryMsg, &pExprs, pExprMsg, pTagColumnInfo)) != TSDB_CODE_SUCCESS) {
    goto _query_over;
  }

  SSqlGroupbyExpr *pGroupbyExpr = createGroupbyExprFromMsg(pQueryMsg, pGroupColIndex, &code);
  if ((pGroupbyExpr == NULL && pQueryMsg->numOfGroupCols != 0) || code != TSDB_CODE_SUCCESS) {
    goto _query_over;
  }

  bool isSTableQuery = false;
  STableGroupInfo groupInfo = {0};
  
  if (TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_MULTITABLE_QUERY|TSDB_QUERY_TYPE_TABLE_QUERY)) {
    isSTableQuery = TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_MULTITABLE_QUERY);
    
    STableId *id = taosArrayGet(pTableIdList, 0);
    if ((code = tsdbGetOneTableGroup(tsdb, id->uid, &groupInfo)) != TSDB_CODE_SUCCESS) {
      goto _query_over;
    }
  } else if (TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_STABLE_QUERY)) {
    isSTableQuery = true;
    STableId *id = taosArrayGet(pTableIdList, 0);
    
    // group by normal column, do not pass the group by condition to tsdb to group table into different group
    int32_t numOfGroupByCols = pQueryMsg->numOfGroupCols;
    if (pQueryMsg->numOfGroupCols == 1 && !TSDB_COL_IS_TAG(pGroupColIndex->flag)) {
      numOfGroupByCols = 0;
    }
    
    // todo handle the error
    /*int32_t ret =*/tsdbQuerySTableByTagCond(tsdb, id->uid, tagCond, pQueryMsg->tagCondLen, pQueryMsg->tagNameRelType, tbnameCond, &groupInfo, pGroupColIndex,
                                         numOfGroupByCols);
    if (groupInfo.numOfTables == 0) {  // no qualified tables no need to do query
      code = TSDB_CODE_SUCCESS;
      goto _query_over;
    }
  } else {
    assert(0);
  }

  (*pQInfo) = createQInfoImpl(pQueryMsg, pGroupbyExpr, pExprs, &groupInfo, pTagColumnInfo);
  if ((*pQInfo) == NULL) {
    code = TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  code = initQInfo(pQueryMsg, tsdb, vgId, *pQInfo, isSTableQuery);

_query_over:
  tfree(tagCond);
  tfree(tbnameCond);
  taosArrayDestroy(pTableIdList);
  
  // if failed to add ref for all meters in this query, abort current query
  //  atomic_fetch_add_32(&vnodeSelectReqNum, 1);
  return code;
}

void qDestroyQueryInfo(qinfo_t pQInfo) {
  qTrace("QInfo:%p query completed", pQInfo);
  freeQInfo(pQInfo);
}

void qTableQuery(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || pQInfo->signature != pQInfo) {
    qTrace("%p freed abort query", pQInfo);
    return;
  }

  if (isQueryKilled(pQInfo)) {
    qTrace("QInfo:%p it is already killed, abort", pQInfo);
    return;
  }

  qTrace("QInfo:%p query task is launched", pQInfo);
  
  if (onlyQueryTags(pQInfo->runtimeEnv.pQuery)) {
    buildTagQueryResult(pQInfo);   // todo support the limit/offset
  } else if (pQInfo->runtimeEnv.stableQuery) {
    stableQueryImpl(pQInfo);
  } else {
    tableQueryImpl(pQInfo);
  }
  
  sem_post(&pQInfo->dataReady);
  //  vnodeDecRefCount(pQInfo);
}

int32_t qRetrieveQueryResultInfo(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_INVALID_QHANDLE;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  if (isQueryKilled(pQInfo)) {
    qTrace("QInfo:%p query is killed, code:%d", pQInfo, pQInfo->code);
    return pQInfo->code;
  }

  sem_wait(&pQInfo->dataReady);
  qTrace("QInfo:%p retrieve result info, rowsize:%d, rows:%d, code:%d", pQInfo, pQuery->rowSize, pQuery->rec.rows,
         pQInfo->code);

  return pQInfo->code;
}

bool qHasMoreResultsToRetrieve(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || pQInfo->signature != pQInfo || pQInfo->code != TSDB_CODE_SUCCESS) {
    return false;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_OVER)) {
    return false;
  } else if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    return true;
  } else if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    return true;
  } else {
    assert(0);
  }
}

int32_t qDumpRetrieveResult(qinfo_t qinfo, SRetrieveTableRsp **pRsp, int32_t *contLen) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_INVALID_QHANDLE;
  }

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  size_t  size = getResultSize(pQInfo, &pQuery->rec.rows);
  *contLen = size + sizeof(SRetrieveTableRsp);

  // todo handle failed to allocate memory
  *pRsp = (SRetrieveTableRsp *)rpcMallocCont(*contLen);
  (*pRsp)->numOfRows = htonl(pQuery->rec.rows);

  int32_t code = pQInfo->code;
  if (code == TSDB_CODE_SUCCESS) {
    (*pRsp)->offset = htobe64(pQuery->limit.offset);
    (*pRsp)->useconds = htobe64(pQInfo->elapsedTime);
  } else {
    (*pRsp)->offset = 0;
    (*pRsp)->useconds = 0;
  }

  if (pQuery->rec.rows > 0 && code == TSDB_CODE_SUCCESS) {
    code = doDumpQueryResult(pQInfo, (*pRsp)->data);
  } else {
    setQueryStatus(pQuery, QUERY_OVER);
    code = pQInfo->code;
  }

  if (isQueryKilled(pQInfo) || Q_STATUS_EQUAL(pQuery->status, QUERY_OVER)) {
    (*pRsp)->completed = 1;  // notify no more result to client
  }

  return code;

  //  if (numOfRows == 0 && (pRetrieve->qhandle == (uint64_t)pObj->qhandle) && (code != TSDB_CODE_ACTION_IN_PROGRESS)) {
  //    qTrace("QInfo:%p %s free qhandle code:%d", pObj->qhandle, __FUNCTION__, code);
  //    vnodeDecRefCount(pObj->qhandle);
  //    pObj->qhandle = NULL;
  //  }
}

static void buildTagQueryResult(SQInfo* pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  
  size_t num = taosArrayGetSize(pQInfo->groupInfo.pGroupList);
  assert(num == 1);   // only one group
  
  SArray* pa = taosArrayGetP(pQInfo->groupInfo.pGroupList, 0);
  num = taosArrayGetSize(pa);
  
  assert(num == pQInfo->groupInfo.numOfTables);
  int16_t type, bytes;
  
  int32_t functionId = pQuery->pSelectExpr[0].base.functionId;
  if (functionId == TSDB_FUNC_TID_TAG) { // return the tags & table Id
    assert(pQuery->numOfOutput == 1);
    SExprInfo* pExprInfo = &pQuery->pSelectExpr[0];
  
    int32_t rsize = pExprInfo->bytes;
    char* data = NULL;
    
    for(int32_t i = 0; i < num; ++i) {
      SGroupItem* item = taosArrayGet(pa, i);
    
      char* output = pQuery->sdata[0]->data + i * rsize;
      *(int64_t*) output = item->id.uid;  // memory align problem
      output += sizeof(item->id.uid);
      
      *(int32_t*) output = item->id.tid;
      output += sizeof(item->id.tid);
      
      *(int32_t*) output = pQInfo->vgId;
      output += sizeof(pQInfo->vgId);
      
      tsdbGetTableTagVal(pQInfo->tsdb, &item->id, pExprInfo->base.colInfo.colId, &type, &bytes, &data);
      memcpy(output, data, bytes);
    }
  
    qTrace("QInfo:%p create (tableId, tag) info completed, rows:%d", pQInfo, num);
  } else {  // return only the tags|table name etc.
    for(int32_t i = 0; i < num; ++i) {
      SExprInfo* pExprInfo = pQuery->pSelectExpr;
      SGroupItem* item = taosArrayGet(pa, i);
    
      char* data = NULL;
      for(int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        // todo check the return value, refactor codes
        if (pExprInfo[j].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
          tsdbGetTableName(pQInfo->tsdb, &item->id, &data);
          
          char* dst = pQuery->sdata[j]->data + i * (TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE);
          STR_WITH_MAXSIZE_TO_VARSTR(dst, data, TSDB_TABLE_NAME_LEN);
          tfree(data);
        
        } else {// todo refactor, return the true length of binary|nchar data
          tsdbGetTableTagVal(pQInfo->tsdb, &item->id, pExprInfo[j].base.colInfo.colId, &type, &bytes, &data);
          assert(bytes == pExprInfo[j].bytes && type == pExprInfo[j].type);
          
          char* dst = pQuery->sdata[j]->data + i * bytes;
          if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
            memcpy(dst, data, varDataTLen(data));
          } else {
            memcpy(dst, data, bytes);
          }
        }
      
      }
    }
  
    qTrace("QInfo:%p create tag values results completed, rows:%d", pQInfo, num);
  }
  
  pQuery->rec.rows = num;
  setQueryStatus(pQuery, QUERY_COMPLETED);
}

