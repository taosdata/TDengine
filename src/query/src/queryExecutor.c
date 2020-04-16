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
#include "taosmsg.h"
#include "tlog.h"
#include "tlosertree.h"
#include "tscompression.h"
#include "ttime.h"

#include "qast.h"

#include "qresultBuf.h"
#include "queryExecutor.h"
#include "queryUtil.h"
#include "tsdb.h"

#define DEFAULT_INTERN_BUF_SIZE 16384L

/**
 * check if the primary column is load by default, otherwise, the program will
 * forced to load primary column explicitly.
 */
#define PRIMARY_TSCOL_LOADED(query) ((query)->colList[0].data.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)

#define Q_STATUS_EQUAL(p, s) (((p) & (s)) != 0)
#define TSDB_COL_IS_TAG(f) (((f)&TSDB_COL_TAG) != 0)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define IS_MASTER_SCAN(runtime) (((runtime)->scanFlag & 1u) == MASTER_SCAN)
#define IS_SUPPLEMENT_SCAN(runtime) ((runtime)->scanFlag == SUPPLEMENTARY_SCAN)
#define SET_SUPPLEMENT_SCAN_FLAG(runtime) ((runtime)->scanFlag = SUPPLEMENTARY_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime) ((runtime)->scanFlag = MASTER_SCAN)

#define GET_QINFO_ADDR(x) ((void*)((char *)(x)-offsetof(SQInfo, runtimeEnv)))

#define GET_COL_DATA_POS(query, index, step) ((query)->pos + (index) * (step))

/* get the qinfo struct address from the query struct address */
#define GET_COLUMN_BYTES(query, colidx) \
  ((query)->colList[(query)->pSelectExpr[colidx].pBase.colInfo.colIndex].info.bytes)
#define GET_COLUMN_TYPE(query, colidx) \
  ((query)->colList[(query)->pSelectExpr[colidx].pBase.colInfo.colIndex].info.type)

typedef struct SPointInterpoSupporter {
  int32_t numOfCols;
  char ** pPrevPoint;
  char ** pNextPoint;
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
  QUERY_OVER      = 0x8u,
} vnodeQueryStatus;

static void setQueryStatus(SQuery *pQuery, int8_t status);
bool        isIntervalQuery(SQuery *pQuery) { return pQuery->intervalTime > 0; }

enum {
  TS_JOIN_TS_EQUAL = 0,
  TS_JOIN_TS_NOT_EQUALS = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

static int32_t mergeIntoGroupResultImpl(SQInfo *pQInfo, STableDataInfo *pTableDataInfo, int32_t start, int32_t end);
static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult);

static void    resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx, SResultInfo *pResultInfo);
static int32_t flushFromResultBuf(SQInfo *pQInfo);
static bool    functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);
static void    getNextTimeWindow(SQuery *pQuery, STimeWindow *pTimeWindow);

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void *inputData, TSKEY *tsCol, int32_t size,
                          int32_t functionId, SDataStatis *pStatis, bool hasNull, void *param, int32_t scanFlag);
static void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static void destroyMeterQueryInfo(STableQueryInfo *pTableQueryInfo, int32_t numOfCols);
static int32_t setAdditionalInfo(SQInfo *pQInfo, int32_t meterIdx, STableQueryInfo *pTableQueryInfo);
static void    resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static bool    hasMainOutput(SQuery *pQuery);

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
      dTrace("QInfo:%p no previous data block, start fileId:%d, slot:%d, pos:%d, qrange:%" PRId64 "-%" PRId64
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

bool vnodeDoFilterData(SQuery *pQuery, int32_t elemPos) {
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    char *                   pElem = pFilterInfo->pData + pFilterInfo->info.info.bytes * elemPos;

    if (isNull(pElem, pFilterInfo->info.info.type)) {
      return false;
    }

    int32_t num = pFilterInfo->numOfFilters;
    bool    qualified = false;
    for (int32_t j = 0; j < num; ++j) {
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

bool vnodeFilterData(SQuery *pQuery, int32_t *numOfActualRead, int32_t index) {
  (*numOfActualRead)++;
  if (!vnodeDoFilterData(pQuery, index)) {
    return false;
  }

  if (pQuery->limit.offset > 0) {
    pQuery->limit.offset--;  // ignore this qualified row
    return false;
  }

  return true;
}

int64_t getNumOfResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool    hasMainFunction = hasMainOutput(pQuery);

  int64_t maxOutput = 0;
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;

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
    SColIndex *pColIndex = &pGroupbyExpr->columnInfo[i];
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
    SColIndex *pColIndex = &pGroupbyExpr->columnInfo[i];
    if (pColIndex->flag == TSDB_COL_NORMAL) {
      colId = pColIndex->colId;
      break;
    }
  }

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    if (colId == pQuery->colList[i].info.colId) {
      type = pQuery->colList[i].info.type;
      break;
    }
  }

  return type;
}

bool isSelectivityWithTagsQuery(SQuery *pQuery) {
  bool    hasTags = false;
  int32_t numOfSelectivity = 0;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functId = pQuery->pSelectExpr[i].pBase.functionId;
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

bool isTSCompQuery(SQuery *pQuery) { return pQuery->pSelectExpr[0].pBase.functionId == TSDB_FUNC_TS_COMP; }

bool doRevisedResultsByLimit(SQInfo *pQInfo) {
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
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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
  SColIndex *pColIndexEx = &pQuery->pSelectExpr[index].pBase.colInfo;
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
  SColIndex* pColIndex = &pQuery->pSelectExpr[col].pBase.colInfo;
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
    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

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
    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      pCtx[k].nStartQueryTimestamp = pWin->skey;

      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunctionF(&pCtx[k], offset);
      }
    }
  }
}

static int32_t getNextQualifiedWindow(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pNextWin,
                                      SWindowResInfo *pWindowResInfo, SDataBlockInfo *pDataBlockInfo,
                                      TSKEY *primaryKeys, __block_search_fn_t searchFn) {
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
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  char *dataBlock = NULL;

  int32_t functionId = pQuery->pSelectExpr[col].pBase.functionId;

  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pExpr = &pQuery->pSelectExpr[col];

    // set the start offset to be the lowest start position, no matter asc/desc query order
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pCtx->startOffset = pQuery->pos;
    } else {
      pCtx->startOffset = pQuery->pos - (size - 1);
    }

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      SColumnInfo *pColMsg = &pQuery->colList[i].info;
      assert(0);
      //      char *       pData = doGetDataBlocks(pQuery, pRuntimeEnv->colDataBuffer, pQuery->colList[i].colIdxInBuf);

      sas->elemSize[i] = pColMsg->bytes;
      //      sas->data[i] = pData + pCtx->startOffset * sas->elemSize[i];  // start from the offset
    }

    sas->numOfCols = pQuery->numOfCols;
    sas->offset = 0;
  } else {  // other type of query function
    SColIndex *pCol = &pQuery->pSelectExpr[col].pBase.colInfo;
    if (TSDB_COL_IS_TAG(pCol->flag)) {
      dataBlock = NULL;
    } else {
      /*
       *  the colIndex is acquired from the first meter of all qualified meters in this vnode during query prepare stage,
       *  the remain meter may not have the required column in cache actually.
       *  So, the validation of required column in cache with the corresponding meter schema is reinforced.
       */
      if (pDataBlock == NULL) {
        return NULL;
      }

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
static void blockwiseApplyAllFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis,
                                          SDataBlockInfo *pDataBlockInfo, SWindowResInfo *pWindowResInfo,
                                          __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;

  SColumnInfoData *pColInfo = NULL;
  TSKEY *        primaryKeyCol = NULL;

  if (pDataBlock != NULL) {
    pColInfo = taosArrayGet(pDataBlock, 0);
    primaryKeyCol = (TSKEY *)(pColInfo->pData);
  }

  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pDataBlockInfo->rows - 1;
  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutputCols, sizeof(SArithmeticSupport));

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

    SDataStatis *tpField = NULL;
    
    bool hasNull = hasNullValue(pQuery, k, pDataBlockInfo, pStatis, &tpField);
    char *dataBlock = getDataBlocks(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);

    setExecParams(pQuery, &pCtx[k], dataBlock, primaryKeyCol, pDataBlockInfo->rows, functionId, tpField,
                  hasNull, &sasArray[k], pRuntimeEnv->scanFlag);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (isIntervalQuery(pQuery)) {
    int32_t offset = GET_COL_DATA_POS(pQuery, 0, step);
    TSKEY   ts = primaryKeyCol[offset];

    STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);
    if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->sid, &win) != TSDB_CODE_SUCCESS) {
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
      int32_t startPos =
          getNextQualifiedWindow(pRuntimeEnv, &nextWin, pWindowResInfo, pDataBlockInfo, primaryKeyCol, searchFn);
      if (startPos < 0) {
        break;
      }

      // null data, failed to allocate more memory buffer
      if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pDataBlockInfo->sid, &nextWin) != TSDB_CODE_SUCCESS) {
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
    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
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

static UNUSED_FUNC char *getGroupbyColumnData(SQuery *pQuery, SData **data, int16_t *type, int16_t *bytes) {
  char *groupbyColumnData = NULL;

  SSqlGroupbyExpr *pGroupbyExpr = pQuery->pGroupbyExpr;

  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    if (pGroupbyExpr->columnInfo[k].flag == TSDB_COL_TAG) {
      continue;
    }

    int16_t colIndex = -1;
    int32_t colId = pGroupbyExpr->columnInfo[k].colId;

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      if (pQuery->colList[i].info.colId == colId) {
        colIndex = i;
        break;
      }
    }

    assert(colIndex >= 0 && colIndex < pQuery->numOfCols);

    *type = pQuery->colList[colIndex].info.type;
    *bytes = pQuery->colList[colIndex].info.bytes;

    //    groupbyColumnData = doGetDataBlocks(pQuery, data, pQuery->colList[colIndex].inf);
    break;
  }

  return groupbyColumnData;
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
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64
         ", tag:%d, id:%s, query order:%d, ts order:%d, traverse:%d, index:%d\n",
         elem.ts, key, elem.tag, pRuntimeEnv->pTabObj->meterId, pQuery->order.order, pRuntimeEnv->pTSBuf->tsOrder,
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

static int32_t rowwiseApplyAllFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis,
                                        SDataBlockInfo *pDataBlockInfo, SWindowResInfo *pWindowResInfo,
                                        SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  TSKEY *         primaryKeyCol = (TSKEY *)taosArrayGet(pDataBlock, 0);

  //  SData **data = pRuntimeEnv->colDataBuffer;

  int64_t prevNumOfRes = 0;
  bool    groupbyStateValue = isGroupbyNormalCol(pQuery->pGroupbyExpr);

  if (!groupbyStateValue) {
    prevNumOfRes = getNumOfResult(pRuntimeEnv);
  }

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutputCols, sizeof(SArithmeticSupport));

  int16_t type = 0;
  int16_t bytes = 0;

  char *groupbyColumnData = NULL;
  if (groupbyStateValue) {
    assert(0);
    //    groupbyColumnData = getGroupbyColumnData(pQuery, data, &type, &bytes);
  }

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

    SDataStatis *pColStatis = NULL;

    bool  hasNull = hasNullValue(pQuery, k, pDataBlockInfo, pStatis, &pColStatis);
    char *dataBlock = getDataBlocks(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);

    setExecParams(pQuery, &pCtx[k], dataBlock, primaryKeyCol, pDataBlockInfo->rows, functionId, pColStatis,
                  hasNull, &sasArray[k], pRuntimeEnv->scanFlag);
  }

  // set the input column data
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
//    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    assert(0);
    /*
     * NOTE: here the tbname/tags column cannot reach here, since it will never be a filter column,
     * so we do NOT check if is a tag or not
     */
    //    pFilterInfo->pData = doGetDataBlocks(pQuery, data, pFilterInfo->info.colIdxInBuf);
  }

  int32_t numOfRes = 0;
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

    if (pQuery->numOfFilterCols > 0 && (!vnodeDoFilterData(pQuery, offset))) {
      continue;
    }

    // interval window query
    if (isIntervalQuery(pQuery)) {
      // decide the time window according to the primary timestamp
      int64_t     ts = primaryKeyCol[offset];
      STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

      assert(0);
      int32_t ret = 0;
      //      int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pRuntimeEnv->pTabObj->sid, &win);
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
      assert(0);
      int32_t sid = 0;  // pRuntimeEnv->pTabObj->sid;

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
        if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, sid, &nextWin) != TSDB_CODE_SUCCESS) {
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

      for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
        int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;
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

    /*
     * pointsOffset is the maximum available space in result buffer update the actual forward step for query that
     * requires checking buffer during loop
     */
    if ((pQuery->checkBuffer == 1) && (++numOfRes) >= pQuery->pointsOffset) {
      pQuery->lastKey = lastKey + step;
      assert(0);
      //      *forwardStep = j + 1;
      break;
    }
  }

  free(sasArray);

  /*
   * No need to calculate the number of output results for group-by normal columns, interval query
   * because the results of group by normal column is put into intermediate buffer.
   */
  int32_t num = 0;
  if (!groupbyStateValue && !isIntervalQuery(pQuery)) {
    num = getNumOfResult(pRuntimeEnv) - prevNumOfRes;
  }

  return num;
}

static UNUSED_FUNC int32_t reviseForwardSteps(SQueryRuntimeEnv *pRuntimeEnv, int32_t forwardStep) {
  /*
   * 1. If value filter exists, we try all data in current block, and do not set the QUERY_RESBUF_FULL flag.
   *
   * 2. In case of top/bottom/ts_comp query, the checkBuffer == 1 and pQuery->numOfFilterCols
   * may be 0 or not. We do not check the capacity of output buffer, since the filter function will do it.
   *
   * 3. In handling the query of secondary query of join, tsBuf servers as a ts filter.
   */
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (isTopBottomQuery(pQuery) || isTSCompQuery(pQuery) || pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    return forwardStep;
  }

  // current buffer does not have enough space, try in the next loop
  if ((pQuery->checkBuffer == 1) && (pQuery->pointsOffset <= forwardStep)) {
    forwardStep = pQuery->pointsOffset;
  }

  return forwardStep;
}

static int32_t tableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo,
                                          SDataStatis *pStatis, __block_search_fn_t searchFn, SWindowResInfo *pWindowResInfo, SArray *pDataBlock) {

  SQuery *pQuery = pRuntimeEnv->pQuery;
  
  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    /*numOfRes = */rowwiseApplyAllFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, pDataBlock);
  } else {
    blockwiseApplyAllFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, searchFn, pDataBlock);
  }
  
  TSKEY lastKey = QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->window.ekey : pDataBlockInfo->window.skey;
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

  if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0 && (tsCol != NULL)) {
    pCtx->ptsList = tsCol;
  }

  if (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) {
    // last_dist or first_dist function
    // store the first&last timestamp into the intermediate buffer [1], the true
    // value may be null but timestamp will never be null
    pCtx->ptsList = tsCol;
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

    pCtx->ptsList = tsCol;

  } else if (functionId == TSDB_FUNC_ARITHM) {
    pCtx->param[1].pz = param;
  }

  pCtx->startOffset = 0;
  pCtx->size = size;

#if defined(_DEBUG_VIEW)
  //  int64_t *tsList = (int64_t *)primaryColumnData;
//  int64_t  s = tsList[0];
//  int64_t  e = tsList[size - 1];

//    if (IS_DATA_BLOCK_LOADED(blockStatus)) {
//        dTrace("QInfo:%p query ts:%lld-%lld, offset:%d, rows:%d, bstatus:%d,
//        functId:%d", GET_QINFO_ADDR(pQuery),
//               s, e, startOffset, size, blockStatus, functionId);
//    } else {
//        dTrace("QInfo:%p block not loaded, bstatus:%d",
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

    SQLFunctionCtx **pTagCtx = calloc(pQuery->numOfOutputCols, POINTER_BYTES);
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlFuncExprMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].pBase;
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
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    setResultInfoBuf(&pResultInfo[i], pQuery->pSelectExpr[i].interResBytes, isStableQuery);
  }
}

static int32_t setupQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv, SColumnModel *pTagsSchema, int16_t order) {
  dTrace("QInfo:%p setup runtime env", GET_QINFO_ADDR(pRuntimeEnv));
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pRuntimeEnv->resultInfo = calloc(pQuery->numOfOutputCols, sizeof(SResultInfo));
  pRuntimeEnv->pCtx = (SQLFunctionCtx *)calloc(pQuery->numOfOutputCols, sizeof(SQLFunctionCtx));

  if (pRuntimeEnv->resultInfo == NULL || pRuntimeEnv->pCtx == NULL) {
    goto _error_clean;
  }

  pRuntimeEnv->offset[0] = 0;
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SSqlFuncExprMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].pBase;

    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->inputType = GET_COLUMN_TYPE(pQuery, i);
    pCtx->inputBytes = GET_COLUMN_BYTES(pQuery, i);

    pCtx->ptsOutputBuf = NULL;

    pCtx->outputBytes = pQuery->pSelectExpr[i].resBytes;
    pCtx->outputType = pQuery->pSelectExpr[i].resType;

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
      int32_t f = pQuery->pSelectExpr[0].pBase.functionId;
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

  dTrace("QInfo:%p teardown runtime env", GET_QINFO_ADDR(pQuery));
  cleanupTimeWindowInfo(&pRuntimeEnv->windowResInfo, pQuery->numOfOutputCols);

  if (pRuntimeEnv->pCtx != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
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
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      tfree(pRuntimeEnv->pInterpoBuf[i]);
    }

    tfree(pRuntimeEnv->pInterpoBuf);
  }

  destroyResultBuf(pRuntimeEnv->pResultBuf);
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

static void setQueryKilled(SQInfo* pQInfo) {
  pQInfo->code = TSDB_CODE_QUERY_CANCELLED;
}

bool isFixedOutputQuery(SQuery *pQuery) {
  if (pQuery->intervalTime != 0) {
    return false;
  }

  // Note:top/bottom query is fixed output query
  if (isTopBottomQuery(pQuery) || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    return true;
  }

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SSqlFuncExprMsg *pExprMsg = &pQuery->pSelectExpr[i].pBase;

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

bool isPointInterpoQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionID == TSDB_FUNC_INTERP || functionID == TSDB_FUNC_LAST_ROW) {
      return true;
    }
  }

  return false;
}

// TODO REFACTOR:MERGE WITH CLIENT-SIDE FUNCTION
bool isSumAvgRateQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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

bool isFirstLastRowQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionID == TSDB_FUNC_LAST_ROW) {
      return true;
    }
  }

  return false;
}

bool notHasQueryTimeRange(SQuery *pQuery) {
  return (pQuery->window.skey == 0 && pQuery->window.ekey == INT64_MAX && QUERY_IS_ASC_QUERY(pQuery)) ||
         (pQuery->window.skey == INT64_MAX && pQuery->window.ekey == 0 && (!QUERY_IS_ASC_QUERY(pQuery)));
}

static bool needReverseScan(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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
/////////////////////////////////////////////////////////////////////////////////////////////

void getAlignQueryTimeWindow(SQuery *pQuery, int64_t key, int64_t keyFirst, int64_t keyLast,
                                        int64_t *realSkey, int64_t *realEkey, STimeWindow *win) {
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
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlFuncExprMsg *pExprMsg = &pQuery->pSelectExpr[i].pBase;
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
    if (pQuery->colList[i].info.colId == pQuery->colList[i + 1].info.colId) {
      dError("QInfo:%p invalid data load column for query", GET_QINFO_ADDR(pQuery));
      return false;
    }
  }
  return true;
}

// todo ignore the avg/sum/min/max/count/stddev/top/bottom functions, of which
// the scan order is not matter
static bool onlyOneQueryType(SQuery *pQuery, int32_t functId, int32_t functIdDst) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

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
    dTrace("QInfo:%p scan order changed for last_row query, old:%d, new:%d", GET_QINFO_ADDR(pQuery),
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
      dTrace(msg, GET_QINFO_ADDR(pQuery), "interp", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
             pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    pQuery->order.order = TSDB_ORDER_ASC;
    return;
  }

  if (pQuery->intervalTime == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        dTrace(msg, GET_QINFO_ADDR(pQuery), "only-first", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
      }

      pQuery->order.order = TSDB_ORDER_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        dTrace(msg, GET_QINFO_ADDR(pQuery), "only-last", pQuery->order.order, TSDB_ORDER_DESC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
      }

      pQuery->order.order = TSDB_ORDER_DESC;
    }

  } else {  // interval query
    if (metricQuery) {
      if (onlyFirstQuery(pQuery)) {
        if (!QUERY_IS_ASC_QUERY(pQuery)) {
          dTrace(msg, GET_QINFO_ADDR(pQuery), "only-first stable", pQuery->order.order, TSDB_ORDER_ASC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        }

        pQuery->order.order = TSDB_ORDER_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          dTrace(msg, GET_QINFO_ADDR(pQuery), "only-last stable", pQuery->order.order, TSDB_ORDER_DESC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        }

        pQuery->order.order = TSDB_ORDER_DESC;
      }
    }
  }
}

static void doSetInterpVal(SQLFunctionCtx *pCtx, TSKEY ts, int16_t type, int32_t index, char *data) {
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
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  // not point interpolation query, abort
  if (!isPointInterpoQuery(pQuery)) {
    return;
  }

  int32_t count = 1;
  TSKEY   key = *(TSKEY *)pPointInterpSupport->pNextPoint[0];

  if (key == pQuery->window.skey) {
    // the queried timestamp has value, return it directly without interpolation
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      tVariantCreateFromBinary(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

      pRuntimeEnv->pCtx[i].param[0].i64Key = key;
      pRuntimeEnv->pCtx[i].param[0].nType = TSDB_DATA_TYPE_BIGINT;
    }
  } else {
    // set the direct previous(next) point for process
    count = 2;

    if (pQuery->interpoType == TSDB_INTERPO_SET_VALUE) {
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
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
        if (pQuery->pSelectExpr[i].pBase.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
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

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

        // tag column does not need the interp environment
        if (pQuery->pSelectExpr[i].pBase.functionId == TSDB_FUNC_TAG) {
          continue;
        }

        int32_t colInBuf = 0;//pQuery->pSelectExpr[i].pBase.colInfo.colIdxInBuf;
        SInterpInfo *pInterpInfo = (SInterpInfo *)pRuntimeEnv->pCtx[i].aOutputBuf;

        pInterpInfo->pInterpDetail = calloc(1, sizeof(SInterpInfoDetail));
        SInterpInfoDetail *pInterpDetail = pInterpInfo->pInterpDetail;

        //        int32_t type = GET_COLUMN_TYPE(pQuery, i);
        int32_t type = 0;
        assert(0);

        // for primary timestamp column, set the flag
        if (pQuery->pSelectExpr[i].pBase.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
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
}

void pointInterpSupporterInit(SQuery *pQuery, SPointInterpoSupporter *pInterpoSupport) {
  if (isPointInterpoQuery(pQuery)) {
    pInterpoSupport->pPrevPoint = malloc(pQuery->numOfCols * POINTER_BYTES);
    pInterpoSupport->pNextPoint = malloc(pQuery->numOfCols * POINTER_BYTES);

    pInterpoSupport->numOfCols = pQuery->numOfCols;

    /* get appropriated size for one row data source*/
    int32_t len = 0;
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      len += pQuery->colList[i].info.bytes;
    }

    //    assert(PRIMARY_TSCOL_LOADED(pQuery));

    void *prev = calloc(1, len);
    void *next = calloc(1, len);

    int32_t offset = 0;

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      pInterpoSupport->pPrevPoint[i] = prev + offset;
      pInterpoSupport->pNextPoint[i] = next + offset;

      offset += pQuery->colList[i].info.bytes;
    }
  }
}

void pointInterpSupporterDestroy(SPointInterpoSupporter *pPointInterpSupport) {
  if (pPointInterpSupport->numOfCols <= 0 || pPointInterpSupport->pPrevPoint == NULL) {
    return;
  }

  tfree(pPointInterpSupport->pPrevPoint[0]);
  tfree(pPointInterpSupport->pNextPoint[0]);

  tfree(pPointInterpSupport->pPrevPoint);
  tfree(pPointInterpSupport->pNextPoint);

  pPointInterpSupport->numOfCols = 0;
}

static UNUSED_FUNC void allocMemForInterpo(SQInfo *pQInfo, SQuery *pQuery, void *pMeterObj) {
#if 0
  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    assert(isIntervalQuery(pQuery) || (pQuery->intervalTime == 0 && isPointInterpoQuery(pQuery)));
    
    if (isIntervalQuery(pQuery)) {
      pQInfo->runtimeEnv.pInterpoBuf = malloc(POINTER_BYTES * pQuery->numOfOutputCols);
      
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        pQInfo->runtimeEnv.pInterpoBuf[i] =
            calloc(1, sizeof(tFilePage) + pQuery->pSelectExpr[i].resBytes * pMeterObj->pointsPerFileBlock);
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
    size_t s = taosArrayGetSize(pQInfo->pTableList);
    num = MAX(s, INITIAL_RESULT_ROWS_VALUE);
  } else {  // for super table query, one page for each subset
    num = 1;//pQInfo->pSidSet->numOfSubSet;
  }

  assert(num > 0);
  return num;
}

static int32_t getRowParamForMultiRowsOutput(SQuery *pQuery, bool isSTableQuery) {
  int32_t rowparam = 1;

  if (isTopBottomQuery(pQuery) && (!isSTableQuery)) {
    rowparam = pQuery->pSelectExpr[1].pBase.arg->argValue.i64;
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
         pQuery->pSelectExpr[columnIndex].resBytes * realRowId;
}

int32_t UNUSED_FUNC vnodeSTableQueryPrepare(SQInfo *pQInfo, SQuery *pQuery, void *param) {
  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.skey > pQuery->window.ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.ekey > pQuery->window.skey))) {
    dTrace("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->window.skey,
           pQuery->window.ekey, pQuery->order.order);

    sem_post(&pQInfo->dataReady);
    return TSDB_CODE_SUCCESS;
  }

  pQuery->status = 0;
  pQuery->rec = (SResultRec){0};

  changeExecuteScanOrder(pQuery, true);
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  /*
   * since we employ the output control mechanism in main loop.
   * so, disable it during data block scan procedure.
   */
  setScanLimitationByResultBuffer(pQuery);

  // save raw query range for applying to each subgroup
  pQuery->lastKey = pQuery->window.skey;

  // create runtime environment
  //  SColumnModel *pTagSchemaInfo = pQInfo->pSidSet->pColumnModel;

  // get one queried meter
  assert(0);
  //  SMeterObj *pMeter = getMeterObj(pQInfo->pTableList, pQInfo->pSidSet->pTableIdList[0]->sid);

  pRuntimeEnv->pTSBuf = param;
  pRuntimeEnv->cur.vnodeIndex = -1;

  // set the ts-comp file traverse order
  if (param != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTSBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTSBuf, order);
  }

  assert(0);
  //  int32_t ret = setupQueryRuntimeEnv(pMeter, pQuery, &pQInfo->runtimeEnv, pTagSchemaInfo, TSDB_ORDER_ASC, true);
  //  if (ret != TSDB_CODE_SUCCESS) {
  //    return ret;
  //  }

  //  createTableGroup(pQInfo->pSidSet);

  int32_t size = getInitialPageNum(pQInfo);
  int32_t ret = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, size, pQuery->rowSize);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
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

  pRuntimeEnv->numOfRowsPerPage = getNumOfRowsInResultPage(pQuery, true);

  STsdbQueryCond cond = {
    .twindow = (STimeWindow) {.skey = pQuery->window.skey, .ekey = pQuery->window.ekey},
    .order = pQuery->order.order,
    .colList = pQuery->colList,
  };
  
  SArray *sa = taosArrayInit(1, POINTER_BYTES);

  //  for(int32_t i = 0; i < pQInfo->pSidSet->numOfTables; ++i) {
  //    SMeterObj *p1 = getMeterObj(pQInfo->pTableList, pQInfo->pSidSet->pTableIdList[i]->sid);
  //    taosArrayPush(sa, &p1);
  //  }

  SArray *cols = taosArrayInit(pQuery->numOfCols, sizeof(pQuery->colList[0]));
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    taosArrayPush(cols, &pQuery->colList[i]);
  }

  pRuntimeEnv->pQueryHandle = tsdbQueryTables(NULL, &cond, sa, cols);

  // metric query do not invoke interpolation, it will be done at the second-stage merge
  if (!isPointInterpoQuery(pQuery)) {
    pQuery->interpoType = TSDB_INTERPO_NONE;
  }

  TSKEY revisedStime = taosGetIntervalStartTimestamp(pQuery->window.skey, pQuery->intervalTime, pQuery->slidingTimeUnit,
                                                     pQuery->precision);
  taosInitInterpoInfo(&pRuntimeEnv->interpoInfo, pQuery->order.order, revisedStime, 0, 0);
  pRuntimeEnv->stableQuery = true;

  return TSDB_CODE_SUCCESS;
}

/**
 * decrease the refcount for each table involved in this query
 * @param pQInfo
 */
void vnodeDecMeterRefcnt(SQInfo *pQInfo) {
  if (pQInfo != NULL) {
    //    assert(taosHashGetSize(pQInfo->pTableList) >= 1);
  }

#if 0
  if (pQInfo == NULL || pQInfo->numOfTables == 1) {
    atomic_fetch_sub_32(&pQInfo->pObj->numOfQueries, 1);
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pQInfo->pObj->vnode,
           pQInfo->pObj->sid, pQInfo->pObj->meterId, pQInfo->pObj->numOfQueries);
  } else {
    int32_t num = 0;
    for (int32_t i = 0; i < pQInfo->numOfTables; ++i) {
      SMeterObj *pMeter = getMeterObj(pQInfo->pTableList, pQInfo->pSidSet->pTableIdList[i]->sid);
      atomic_fetch_sub_32(&(pMeter->numOfQueries), 1);
      
      if (pMeter->numOfQueries > 0) {
        dTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pMeter->vnode, pMeter->sid,
               pMeter->meterId, pMeter->numOfQueries);
        num++;
      }
    }
    
    /*
     * in order to reduce log output, for all meters of which numOfQueries count are 0,
     * we do not output corresponding information
     */
    num = pQInfo->numOfTables - num;
    dTrace("QInfo:%p metric query is over, dec query ref for %d meters, numOfQueries on %d meters are 0", pQInfo,
           pQInfo->numOfTables, num);
  }
#endif
}

void setTimestampRange(SQueryRuntimeEnv *pRuntimeEnv, int64_t stime, int64_t etime) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

    if (functionId == TSDB_FUNC_SPREAD) {
      pRuntimeEnv->pCtx[i].param[1].dKey = stime;
      pRuntimeEnv->pCtx[i].param[2].dKey = etime;

      pRuntimeEnv->pCtx[i].param[1].nType = TSDB_DATA_TYPE_DOUBLE;
      pRuntimeEnv->pCtx[i].param[2].nType = TSDB_DATA_TYPE_DOUBLE;
    }
  }
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
  //  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
  //    int32_t functId = pQuery->pSelectExpr[i].pBase.functionId;
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
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
      int32_t colId = pQuery->pSelectExpr[i].pBase.colInfo.colId;
      r |= aAggs[functionId].dataReqFunc(&pRuntimeEnv->pCtx[i], pQuery->window.skey, pQuery->window.ekey, colId);
    }

    if (pRuntimeEnv->pTSBuf > 0 || isIntervalQuery(pQuery)) {
      r |= BLK_DATA_ALL_NEEDED;
    }
  }

  if (r == BLK_DATA_NO_NEEDED) {
    qTrace("QInfo:%p slot:%d, data block ignored, brange:%" PRId64 "-%" PRId64 ", rows:%d",
          GET_QINFO_ADDR(pRuntimeEnv), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
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
      dTrace("QInfo:%p fileId:%d, slot:%d, block discarded by per-filter", GET_QINFO_ADDR(pQuery), pQuery->fileId,
             pQuery->slot);
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
  
  TSKEY* keyList   = (TSKEY *)pValue;
  int32_t firstPos = 0;
  int32_t lastPos  = num - 1;

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
  dTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", lastkey:%" PRId64 ", order:%d",
         GET_QINFO_ADDR(pRuntimeEnv), pQuery->window.skey, pQuery->window.ekey, pQuery->lastKey, pQuery->order.order);

  tsdb_query_handle_t pQueryHandle = pRuntimeEnv->scanFlag == MASTER_SCAN? pRuntimeEnv->pQueryHandle:pRuntimeEnv->pSecQueryHandle;
  while (tsdbNextDataBlock(pQueryHandle)) {
    
    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      return 0;
    }

    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);

    // todo extract methods
    if (isIntervalQuery(pQuery) && pRuntimeEnv->windowResInfo.prevSKey == 0) {
      TSKEY skey1, ekey1;
      STimeWindow w = {0};
      SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

      if (QUERY_IS_ASC_QUERY(pQuery)) {
        getAlignQueryTimeWindow(pQuery, blockInfo.window.skey, blockInfo.window.skey, pQuery->window.ekey,
                                           &skey1, &ekey1, &w);
        pWindowResInfo->startTime = w.skey;
        pWindowResInfo->prevSKey = w.skey;
      } else {
        // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
        TSKEY start = blockInfo.window.ekey - pQuery->intervalTime;
        getAlignQueryTimeWindow(pQuery, start, pQuery->window.ekey, blockInfo.window.ekey, &skey1, &ekey1, &w);

        pWindowResInfo->startTime = pQuery->window.skey;
        pWindowResInfo->prevSKey = w.skey;
      }
    }
    
    // in case of prj/diff query, ensure the output buffer is sufficient to accomodate the results of current block
    if (!isIntervalQuery(pQuery) && !isGroupbyNormalCol(pQuery->pGroupbyExpr) && !isFixedOutputQuery(pQuery)) {
      SResultRec* pRec = &pQuery->rec;
      
      if (pQuery->rec.capacity - pQuery->rec.rows < blockInfo.rows) {
        int32_t remain = pRec->capacity - pRec->rows;
        int32_t newSize = pRec->capacity + (blockInfo.rows - remain);
        
        for(int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
          int32_t bytes = pQuery->pSelectExpr[i].resBytes;
          
          char* tmp = realloc(pQuery->sdata[i], bytes * newSize + sizeof(SData));
          if (tmp == NULL) { // todo handle the oom
          } else {
            pQuery->sdata[i] = (SData*) tmp;
          }
          
          // set the pCtx output buffer position
          pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data + pRec->rows*bytes;
        }
        
        pRec->capacity = newSize;
      }
    }

    SDataStatis *pStatis = NULL;
    SArray *pDataBlock = loadDataBlockOnDemand(pRuntimeEnv, &blockInfo, &pStatis);
    int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, binarySearchForKey,
                                                     &pRuntimeEnv->windowResInfo, pDataBlock);

    dTrace("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", rows:%d, res:%d",
               GET_QINFO_ADDR(pRuntimeEnv), blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes);

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
static void doSetTagValueInParam(SColumnModel *pTagSchema, int32_t tagColIdx, void *pMeterSidInfo, tVariant *param) {
  assert(tagColIdx >= 0);
#if 0
  int16_t offset = getColumnModelOffset(pTagSchema, tagColIdx);
  
  void *   pStr = (char *)pMeterSidInfo->tags + offset;
  SSchema *pCol = getColumnModelSchema(pTagSchema, tagColIdx);
  
  tVariantDestroy(param);
  
  if (isNull(pStr, pCol->type)) {
    param->nType = TSDB_DATA_TYPE_NULL;
  } else {
    tVariantCreateFromBinary(param, pStr, pCol->bytes, pCol->type);
  }
#endif
}

void vnodeSetTagValueInParam(STableGroupList *pSidSet, SQueryRuntimeEnv *pRuntimeEnv, void *pMeterSidInfo) {
  SQuery *      pQuery = pRuntimeEnv->pQuery;
  SColumnModel *pTagSchema = NULL;//pSidSet->pColumnModel;

  SSqlFuncExprMsg *pFuncMsg = &pQuery->pSelectExpr[0].pBase;
  if (pQuery->numOfOutputCols == 1 && pFuncMsg->functionId == TSDB_FUNC_TS_COMP) {
    assert(pFuncMsg->numOfParams == 1);
    doSetTagValueInParam(pTagSchema, pFuncMsg->arg->argValue.i64, pMeterSidInfo, &pRuntimeEnv->pCtx[0].tag);
  } else {
    // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutputCols; ++idx) {
      SColIndex *pColEx = &pQuery->pSelectExpr[idx].pBase.colInfo;

      // ts_comp column required the tag value for join filter
      if (!TSDB_COL_IS_TAG(pColEx->flag)) {
        continue;
      }

      doSetTagValueInParam(pTagSchema, pColEx->colIndex, pMeterSidInfo, &pRuntimeEnv->pCtx[idx].tag);
    }

    // set the join tag for first column
    if (pFuncMsg->functionId == TSDB_FUNC_TS && pFuncMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX &&
        pRuntimeEnv->pTSBuf != NULL) {
      assert(pFuncMsg->numOfParams == 1);
      doSetTagValueInParam(pTagSchema, pFuncMsg->arg->argValue.i64, pMeterSidInfo, &pRuntimeEnv->pCtx[0].tag);
    }
  }
}

static void doMerge(SQueryRuntimeEnv *pRuntimeEnv, int64_t timestamp, SWindowResult *pWindowRes, bool mergeFlag) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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
  int32_t numOfCols = pQuery->numOfOutputCols;
  printf("super table query intermediate result, total:%d\n", numOfRows);
  
  SQInfo *   pQInfo = (SQInfo *)(GET_QINFO_ADDR(pQuery));
  SMeterObj *pMeterObj = pQInfo->pObj;
  
  for (int32_t j = 0; j < numOfRows; ++j) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      switch (pQuery->pSelectExpr[i].resType) {
        case TSDB_DATA_TYPE_BINARY: {
          int32_t colIndex = pQuery->pSelectExpr[i].pBase.colInfo.colIndex;
          int32_t type = 0;
          
          if (TSDB_COL_IS_TAG(pQuery->pSelectExpr[i].pBase.colInfo.flag)) {
            type = pQuery->pSelectExpr[i].resType;
          } else {
            type = pMeterObj->schema[colIndex].type;
          }
          printBinaryData(pQuery->pSelectExpr[i].pBase.functionId, pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j,
                          type);
          break;
        }
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          printf("%" PRId64 "\t", *(int64_t *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
      }
    }
    printf("\n");
  }
#endif
}

typedef struct SCompSupporter {
  STableDataInfo **pTableDataInfo;
  int32_t *        position;
  SQInfo *         pQInfo;
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

  SWindowResInfo *pWindowResInfo1 = &supporter->pTableDataInfo[left]->pTableQInfo->windowResInfo;
  SWindowResult * pWindowRes1 = getWindowResult(pWindowResInfo1, leftPos);

  char *b1 = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes1);
  TSKEY leftTimestamp = GET_INT64_VAL(b1);

  SWindowResInfo *pWindowResInfo2 = &supporter->pTableDataInfo[right]->pTableQInfo->windowResInfo;
  SWindowResult * pWindowRes2 = getWindowResult(pWindowResInfo2, rightPos);

  char *b2 = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes2);
  TSKEY rightTimestamp = GET_INT64_VAL(b2);

  if (leftTimestamp == rightTimestamp) {
    return 0;
  }

  return leftTimestamp > rightTimestamp ? 1 : -1;
}

int32_t mergeIntoGroupResult(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

//  int64_t st = taosGetTimestampMs();
  int32_t ret = TSDB_CODE_SUCCESS;

//  while (pQInfo->subgroupIdx < pQInfo->pSidSet->numOfSubSet) {
    int32_t start = 0;//pQInfo->pSidSet->starterPos[pQInfo->subgroupIdx];
    int32_t end = taosArrayGetSize(pQInfo->pTableList) - 1;//pQInfo->pSidSet->starterPos[pQInfo->subgroupIdx + 1];

    ret = mergeIntoGroupResultImpl(pQInfo, pQInfo->pTableDataInfo, start, end);
    if (ret < 0) {  // not enough disk space to save the data into disk
      return -1;
    }

    pQInfo->subgroupIdx += 1;

    // this group generates at least one result, return results
//    if (ret > 0) {
//      break;
//    }

    assert(pQInfo->numOfGroupResultPages == 0);
    dTrace("QInfo:%p no result in group %d, continue", GET_QINFO_ADDR(pQuery), pQInfo->subgroupIdx - 1);
//  }

//  dTrace("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%lldms",
//  GET_QINFO_ADDR(pQuery), pQInfo->subgroupIdx - 1, pQInfo->pSidSet->numOfSubSet, taosGetTimestampMs() - st);

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
    //    if (pQInfo->numOfGroupResultPages == 0 && pQInfo->subgroupIdx == pQInfo->pSidSet->numOfSubSet) {
    //      pQInfo->tableIndex = pQInfo->pSidSet->numOfTables;
    //      return;
    //    }
  }

  SQueryRuntimeEnv *   pRuntimeEnv = &pQInfo->runtimeEnv;
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  int32_t id = getGroupResultId(pQInfo->subgroupIdx - 1);
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

    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
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
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;

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

int32_t mergeIntoGroupResultImpl(SQInfo *pQInfo, STableDataInfo *pTableDataInfo, int32_t start, int32_t end) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;

  tFilePage **     buffer = (tFilePage **)pQuery->sdata;
  int32_t *        posList = calloc((end - start), sizeof(int32_t));
  STableDataInfo **pTableList = malloc(POINTER_BYTES * (end - start));

  // todo opt for the case of one table per group
  int32_t numOfTables = 0;
  for (int32_t i = start; i < end; ++i) {
    int32_t tid = pTableDataInfo[i].pTableQInfo->tid;

    SIDList list = getDataBufPagesIdList(pRuntimeEnv->pResultBuf, tid);
    if (list.size > 0 && pTableDataInfo[i].pTableQInfo->windowResInfo.size > 0) {
      pTableList[numOfTables] = &pTableDataInfo[i];
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

  SResultInfo *pResultInfo = calloc(pQuery->numOfOutputCols, sizeof(SResultInfo));
  setWindowResultInfo(pResultInfo, pQuery, pRuntimeEnv->stableQuery);
  resetMergeResultBuf(pQuery, pRuntimeEnv->pCtx, pResultInfo);
  
  int64_t lastTimestamp = -1;
  int64_t startt = taosGetTimestampMs();

  while (1) {
    int32_t pos = pTree->pNode[0].index;

    SWindowResInfo *pWindowResInfo = &pTableList[pos]->pTableQInfo->windowResInfo;
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
      dError("QInfo:%p failed to flush data into temp file, abort query", pQInfo);
      
      tfree(pTree);
      tfree(pTableList);
      tfree(posList);
      tfree(pResultInfo);

      return -1;
    }
  }

  int64_t endt = taosGetTimestampMs();

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif

  dTrace("QInfo:%p result merge completed, elapsed time:%" PRId64 " ms", GET_QINFO_ADDR(pQuery), endt - startt);
  tfree(pTree);
  tfree(pTableList);
  tfree(posList);

  pQInfo->offset = 0;
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
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

  assert(0);
  int32_t remain = 0;//pQuery->sdata[0]->num;
  int32_t offset = 0;

  while (remain > 0) {
    int32_t r = remain;
    if (r > capacity) {
      r = capacity;
    }

    int32_t    id = getGroupResultId(pQInfo->subgroupIdx) + pQInfo->numOfGroupResultPages;
    tFilePage *buf = getNewDataBuf(pResultBuf, id, &pageId);

    // pagewise copy to dest buffer
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
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
  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    pCtx[k].aOutputBuf = pQuery->sdata[k]->data - pCtx[k].outputBytes;
    pCtx[k].size = 1;
    pCtx[k].startOffset = 0;
    pCtx[k].resultInfo = &pResultInfo[k];

    pQuery->sdata[k] = 0;
  }
}

void setTableDataInfo(STableDataInfo *pTableDataInfo, int32_t tableIndex, int32_t groupId) {
  pTableDataInfo->groupIdx = groupId;
  pTableDataInfo->tableIndex = tableIndex;
}

static void doDisableFunctsForSupplementaryScan(SQuery *pQuery, SWindowResInfo *pWindowResInfo, int32_t order) {
  for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
    SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, i);
    if (!pStatus->closed) {
      continue;
    }

    SWindowResult *buf = getWindowResult(pWindowResInfo, i);

    // open/close the specified query for each group result
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int32_t functId = pQuery->pSelectExpr[j].pBase.functionId;

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_DESC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_ASC)) {
        buf->resultInfo[j].complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        buf->resultInfo[j].complete = true;
      }
    }
  }
}

void disableFunctForTableSuppleScan(SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // group by normal columns and interval query on normal table
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
  } else {  // for simple result of table query,
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int32_t         functId = pQuery->pSelectExpr[j].pBase.functionId;
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[j];

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_DESC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_ASC)) {
        pCtx->resultInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pCtx->resultInfo->complete = true;
      }
    }
  }

  pQuery->order.order = pQuery->order.order ^ 1u;
}

void disableFuncForReverseScan(SQInfo *pQInfo, int32_t order) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  if (isIntervalQuery(pQuery)) {
    size_t numOfTables = taosArrayGetSize(pQInfo->pTableList);

    for (int32_t i = 0; i < numOfTables; ++i) {
      STableQueryInfo *pTableQueryInfo = pQInfo->pTableDataInfo[i].pTableQInfo;
      SWindowResInfo * pWindowResInfo = &pTableQueryInfo->windowResInfo;

      doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
    }
  } else {
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
  }

  pQuery->order.order = (pQuery->order.order) ^ 1u;
}

void enableFuncForForwardScan(SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  pQuery->order.order = (pQuery->order.order) ^ 1u;
}

void createQueryResultInfo(SQuery *pQuery, SWindowResult *pResultRow, bool isSTableQuery, SPosInfo *posInfo) {
  int32_t numOfCols = pQuery->numOfOutputCols;

  pResultRow->resultInfo = calloc((size_t)numOfCols, sizeof(SResultInfo));
  pResultRow->pos = *posInfo;

  // set the intermediate result output buffer
  setWindowResultInfo(pResultRow->resultInfo, pQuery, isSTableQuery);
}

void resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = pQuery->sdata[i]->data;

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT/TAGPRJ/PRJ/TAG etc.
     */
    resetResultInfo(&pRuntimeEnv->resultInfo[i]);
    pCtx->resultInfo = &pRuntimeEnv->resultInfo[i];

    // set the timestamp output buffer for top/bottom/diff query
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    memset(pQuery->sdata[i]->data, 0, (size_t) pQuery->pSelectExpr[i].resBytes * pQuery->rec.capacity);
  }

  initCtxOutputBuf(pRuntimeEnv);
}

void forwardCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, int64_t output) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // reset the execution contexts
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;
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

  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;
    
    pRuntimeEnv->pCtx[j].currentStage = 0;
    aAggs[functionId].init(&pRuntimeEnv->pCtx[j]);
  }
}

void doSkipResults(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pQuery->rec.rows == 0 || pQuery->limit.offset == 0) {
    return;
  }

  if (pQuery->rec.rows <= pQuery->limit.offset) {
    pQuery->limit.offset -= pQuery->rec.rows;

    pQuery->rec.rows = 0;
    //    pQuery->pointsOffset = pQuery->rec.pointsToRead;  // clear all data in result buffer

    resetCtxOutputBuf(pRuntimeEnv);

    // clear the buffer is full flag if exists
    pQuery->status &= (~QUERY_RESBUF_FULL);
  } else {
    int32_t numOfSkip = (int32_t)pQuery->limit.offset;
    pQuery->rec.rows -= numOfSkip;

    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      assert(0);
      //      memmove(pQuery->sdata[i]->data, pQuery->sdata[i]->data + bytes * numOfSkip, pQuery->size * bytes);
      pRuntimeEnv->pCtx[i].aOutputBuf += bytes * numOfSkip;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf += TSDB_KEYSIZE * numOfSkip;
      }
    }

    pQuery->limit.offset = 0;
  }
}

typedef struct SQueryStatus {
  int8_t    overStatus;
  TSKEY     lastKey;
  STSCursor cur;
} SQueryStatus;

// todo refactor
static void queryStatusSave(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatus *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pStatus->overStatus = pQuery->status;
  pStatus->lastKey = pQuery->lastKey;

  pStatus->cur = tsBufGetCursor(pRuntimeEnv->pTSBuf);  // save the cursor

  if (pRuntimeEnv->pTSBuf) {
    pRuntimeEnv->pTSBuf->cur.order ^= 1u;
    tsBufNextPos(pRuntimeEnv->pTSBuf);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  pQuery->lastKey = pQuery->window.skey;
}

static void queryStatusRestore(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatus *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);

  pQuery->lastKey = pStatus->lastKey;
  pQuery->status = pStatus->overStatus;

  tsBufSetCursor(pRuntimeEnv->pTSBuf, &pStatus->cur);
}

static void doSingleMeterSupplementScan(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *     pQuery = pRuntimeEnv->pQuery;
  SQueryStatus qStatus = {0};

  if (!needReverseScan(pQuery)) {
    return;
  }

  dTrace("QInfo:%p start to supp scan", GET_QINFO_ADDR(pQuery));
  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);

  // close necessary function execution during supplementary scan
  disableFunctForTableSuppleScan(pRuntimeEnv, pQuery->order.order);
  queryStatusSave(pRuntimeEnv, &qStatus);

  STimeWindow w = {.skey = pQuery->window.skey, .ekey = pQuery->window.ekey};

  // reverse scan from current position
  tsdbpos_t current = tsdbDataBlockTell(pRuntimeEnv->pQueryHandle);
  tsdbResetQuery(pRuntimeEnv->pQueryHandle, &w, current, pQuery->order.order);

  doScanAllDataBlocks(pRuntimeEnv);

  queryStatusRestore(pRuntimeEnv, &qStatus);
  enableFuncForForwardScan(pRuntimeEnv, pQuery->order.order);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
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

      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        int16_t functId = pQuery->pSelectExpr[j].pBase.functionId;
        if (functId == TSDB_FUNC_TS) {
          continue;
        }

        aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
        SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

        toContinue |= (!pResInfo->complete);
      }
    }
  } else {
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int16_t functId = pQuery->pSelectExpr[j].pBase.functionId;
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

void scanAllDataBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  // store the start query position
//  void *pos = tsdbDataBlockTell(pRuntimeEnv->pQueryHandle);
  SQInfo* pQInfo = (SQInfo*) GET_QINFO_ADDR(pRuntimeEnv);
  
  int64_t skey = pQuery->lastKey;
  int32_t status = pQuery->status;
  int32_t activeSlot = pRuntimeEnv->windowResInfo.curIndex;

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  while (1) {
    doScanAllDataBlocks(pRuntimeEnv);

    if (!needScanDataBlocksAgain(pRuntimeEnv)) {
      
      // restore the status
      if (pRuntimeEnv->scanFlag == REPEAT_SCAN) {
        pQuery->status = status;
      }

      break;
    }

    // set the correct start position, and load the corresponding block in buffer for next round scan all data blocks.
//    /*int32_t ret =*/ tsdbDataBlockSeek(pRuntimeEnv->pQueryHandle, pos);
  
    STsdbQueryCond cond = {
        .twindow = {pQuery->window.skey, pQuery->lastKey},
        .order   = pQuery->order.order,
        .colList = pQuery->colList,
    };
  
    SArray *cols = taosArrayInit(pQuery->numOfCols, sizeof(pQuery->colList[0]));
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      taosArrayPush(cols, &pQuery->colList[i]);
    }
  
    if (pRuntimeEnv->pSecQueryHandle != NULL) {
      pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, pQInfo->pTableList, cols);
    }
    
    taosArrayDestroy(cols);
  
    status = pQuery->status;
    pRuntimeEnv->windowResInfo.curIndex = activeSlot;

    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    // check if query is killed or not
    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      return;
    }
  }

  // no need to set the end key
  TSKEY lkey = pQuery->lastKey;
  TSKEY ekey = pQuery->window.ekey;

  pQuery->window.skey = skey;
  pQuery->window.ekey = pQuery->lastKey - step;
  /*tsdbpos_t current =*/ tsdbDataBlockTell(pRuntimeEnv->pQueryHandle);

  doSingleMeterSupplementScan(pRuntimeEnv);

  // update the pQuery->window.skey and pQuery->window.ekey to limit the scan scope of sliding query during reverse scan
  pQuery->lastKey = lkey;
  pQuery->window.ekey = ekey;

//  STimeWindow win = {.skey = pQuery->window.skey, .ekey = pQuery->window.ekey};
//  tsdbResetQuery(pRuntimeEnv->pQueryHandle, &win, current, pQuery->order.order);
//  tsdbNextDataBlock(pRuntimeEnv->pQueryHandle);
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

      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        aAggs[pQuery->pSelectExpr[j].pBase.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
      }

      /*
       * set the number of output results for group by normal columns, the number of output rows usually is 1 except
       * the top and bottom query
       */
      buf->numOfRows = getNumOfResult(pRuntimeEnv);
    }

  } else {
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      aAggs[pQuery->pSelectExpr[j].pBase.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
    }
  }
}

static bool hasMainOutput(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

    if (functionId != TSDB_FUNC_TS && functionId != TSDB_FUNC_TAG && functionId != TSDB_FUNC_TAGPRJ) {
      return true;
    }
  }

  return false;
}

STableQueryInfo *createTableQueryInfo(SQueryRuntimeEnv *pRuntimeEnv, int32_t tid, STimeWindow win) {
  STableQueryInfo *pTableQueryInfo = calloc(1, sizeof(STableQueryInfo));

  pTableQueryInfo->win = win;
  pTableQueryInfo->lastKey = win.skey;

  pTableQueryInfo->tid = tid;
  pTableQueryInfo->cur.vnodeIndex = -1;

  initWindowResInfo(&pTableQueryInfo->windowResInfo, pRuntimeEnv, 100, 100, TSDB_DATA_TYPE_INT);
  return pTableQueryInfo;
}

void destroyMeterQueryInfo(STableQueryInfo *pTableQueryInfo, int32_t numOfCols) {
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
  pTableQueryInfo->cur.vnodeIndex = -1;
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
void setExecutionContext(SQInfo *pQInfo, STableQueryInfo *pTableQueryInfo, int32_t meterIdx, int32_t groupIdx,
                         TSKEY nextKey) {
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
  setAdditionalInfo(pQInfo, meterIdx, pTableQueryInfo);
}

static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->numOfElems == 0, there is only fixed number of results for each group
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = getPosInResultPage(pRuntimeEnv, i, pResult);

    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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

int32_t setAdditionalInfo(SQInfo *pQInfo, int32_t meterIdx, STableQueryInfo *pTableQueryInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  assert(pTableQueryInfo->lastKey > 0);

  //  vnodeSetTagValueInParam(pQInfo->pSidSet, pRuntimeEnv, pQInfo->pMeterSidExtInfo[meterIdx]);

  // both the master and supplement scan needs to set the correct ts comp start position
  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pTableQueryInfo->cur.vnodeIndex == -1) {
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
  for (int32_t i = 0; i < pQuery->numOfOutputCols; i++) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
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
    totalSubset = 1;//pQInfo->pSidSet->numOfSubSet;
  }

  return totalSubset;
}

static int32_t doCopyToSData(SQInfo *pQInfo, SWindowResult *result, int32_t orderType) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfResult = 0;
  int32_t startIdx = 0;
  int32_t step = -1;

  dTrace("QInfo:%p start to copy data from windowResInfo to query buf", GET_QINFO_ADDR(pQuery));
  int32_t totalSubset = getNumOfSubset(pQInfo);

  if (orderType == TSDB_ORDER_ASC) {
    startIdx = pQInfo->subgroupIdx;
    step = 1;
  } else {  // desc order copy all data
    startIdx = totalSubset - pQInfo->subgroupIdx - 1;
    step = -1;
  }

  for (int32_t i = startIdx; (i < totalSubset) && (i >= 0); i += step) {
    if (result[i].numOfRows == 0) {
      pQInfo->offset = 0;
      pQInfo->subgroupIdx += 1;
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
      pQInfo->subgroupIdx += 1;
    }

    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
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

  dTrace("QInfo:%p copy data to query buf completed", pQInfo);

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

static void updateWindowResNumOfRes(SQueryRuntimeEnv *pRuntimeEnv, STableDataInfo *pTableDataInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // update the number of result for each, only update the number of rows for the corresponding window result.
  if (pQuery->intervalTime == 0) {
    int32_t g = pTableDataInfo->groupIdx;
    assert(pRuntimeEnv->windowResInfo.size > 0);

    SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, (char *)&g, sizeof(g));
    if (pWindowRes->numOfRows == 0) {
      pWindowRes->numOfRows = getNumOfResult(pRuntimeEnv);
    }
  }
}

void stableApplyFunctionsOnBlock(SQueryRuntimeEnv* pRuntimeEnv, STableDataInfo *pTableDataInfo, SDataBlockInfo *pDataBlockInfo,
                                  SDataStatis *pStatis, SArray *pDataBlock, __block_search_fn_t searchFn) {
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo * pTableQueryInfo = pTableDataInfo->pTableQInfo;
  SWindowResInfo *  pWindowResInfo = &pTableQueryInfo->windowResInfo;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    //    numOfRes = rowwiseApplyAllFunctions(pRuntimeEnv, &forwardStep, pFields, pDataBlockInfo, pWindowResInfo);
  } else {
    blockwiseApplyAllFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pWindowResInfo, searchFn, pDataBlock);
  }

  updateWindowResNumOfRes(pRuntimeEnv, pTableDataInfo);
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
      /*TSKEY ekey =*/ taosGetRevisedEndKey(pQuery->window.ekey, pQuery->order.order, pQuery->intervalTime,
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
  SSchema *pSchema = calloc(1, sizeof(SSchema) * pQuery->numOfOutputCols);
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pSchema[i].bytes = pRuntimeEnv->pCtx[i].outputBytes;
    pSchema[i].type = pQuery->pSelectExpr[i].resType;
  }
  
//  SColumnModel *pModel = createColumnModel(pSchema, pQuery->numOfOutputCols, pQuery->pointsToRead);
  
  char *  srcData[TSDB_MAX_COLUMNS] = {0};
  int32_t functions[TSDB_MAX_COLUMNS] = {0};
  
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    srcData[i] = pDataSrc[i]->data;
    functions[i] = pQuery->pSelectExpr[i].pBase.functionId;
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
  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
    int32_t bytes = pQuery->pSelectExpr[col].resBytes;
    
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
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        memmove(pDst[i]->data, pDst[i]->data + pQuery->pSelectExpr[i].resBytes * pQuery->limit.offset,
                ret * pQuery->pSelectExpr[i].resBytes);
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
  
  dTrace("QInfo:%p statis: comp blocks:%d, size:%d Bytes, elapsed time:%.2f ms", pQInfo, pSummary->readCompInfo,
         pSummary->totalCompInfoSize, pSummary->loadCompInfoUs / 1000.0);
  
  dTrace("QInfo:%p statis: field info: %d, size:%d Bytes, avg size:%.2f Bytes, elapsed time:%.2f ms", pQInfo,
         pSummary->readField, pSummary->totalFieldSize, (double)pSummary->totalFieldSize / pSummary->readField,
         pSummary->loadFieldUs / 1000.0);
  
  dTrace(
      "QInfo:%p statis: file blocks:%d, size:%d Bytes, elapsed time:%.2f ms, skipped:%d, in-memory gen null:%d Bytes",
      pQInfo, pSummary->readDiskBlocks, pSummary->totalBlockSize, pSummary->loadBlocksUs / 1000.0,
      pSummary->skippedFileBlocks, pSummary->totalGenData);
  
  dTrace("QInfo:%p statis: cache blocks:%d", pQInfo, pSummary->blocksInCache, 0);
  dTrace("QInfo:%p statis: temp file:%d Bytes", pQInfo, pSummary->tmpBufferInDisk);
  
  dTrace("QInfo:%p statis: file:%d, table:%d", pQInfo, pSummary->numOfFiles, pSummary->numOfTables);
  dTrace("QInfo:%p statis: seek ops:%d", pQInfo, pSummary->numOfSeek);
  
  double total = pSummary->fileTimeUs + pSummary->cacheTimeUs;
  double io = pSummary->loadCompInfoUs + pSummary->loadBlocksUs + pSummary->loadFieldUs;
  
  // todo add the intermediate result save cost!!
  double computing = total - io;
  
  dTrace(
      "QInfo:%p statis: total elapsed time:%.2f ms, file:%.2f ms(%.2f%), cache:%.2f ms(%.2f%). io:%.2f ms(%.2f%),"
      "comput:%.2fms(%.2f%)",
      pQInfo, total / 1000.0, pSummary->fileTimeUs / 1000.0, pSummary->fileTimeUs * 100 / total,
      pSummary->cacheTimeUs / 1000.0, pSummary->cacheTimeUs * 100 / total, io / 1000.0, io * 100 / total,
      computing / 1000.0, computing * 100 / total);
#endif
}

int32_t doInitQInfo(SQInfo *pQInfo, void *param, void* tsdb, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  int32_t code = TSDB_CODE_SUCCESS;

  setScanLimitationByResultBuffer(pQuery);
  changeExecuteScanOrder(pQuery, false);

  // dataInCache requires lastKey value
  pQuery->lastKey = pQuery->window.skey;

  STsdbQueryCond cond = {
    .twindow = pQuery->window,
    .order   = pQuery->order.order,
    .colList = pQuery->colList,
  };

  SArray *cols = taosArrayInit(pQuery->numOfCols, sizeof(pQuery->colList[0]));
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    taosArrayPush(cols, &pQuery->colList[i]);
  }
  
  pRuntimeEnv->pQueryHandle = tsdbQueryTables(tsdb, &cond, pQInfo->pTableList, cols);
  taosArrayDestroy(cols);
  pQInfo->tsdb = tsdb;
  
  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pTSBuf = param;
  pRuntimeEnv->cur.vnodeIndex = -1;
  pRuntimeEnv->stableQuery = isSTableQuery;
  
  if (param != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTSBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTSBuf, order);
  }

  // create runtime environment
  code = setupQueryRuntimeEnv(pRuntimeEnv, NULL, pQuery->order.order);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pRuntimeEnv->numOfRowsPerPage = getNumOfRowsInResultPage(pQuery, isSTableQuery);
  
  if (isSTableQuery) {
    int32_t rows = getInitialPageNum(pQInfo);
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rows, pQuery->rowSize);
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
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rows, pQuery->rowSize);
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

  SPointInterpoSupporter interpInfo = {0};
  pointInterpSupporterInit(pQuery, &interpInfo);

  /*
   * in case of last_row query without query range, we set the query timestamp to
   * pMeterObj->lastKey. Otherwise, keep the initial query time range unchanged.
   */
  if (isFirstLastRowQuery(pQuery) && notHasQueryTimeRange(pQuery)) {
    if (!normalizeUnBoundLastRowQuery(pQInfo, &interpInfo)) {
      sem_post(&pQInfo->dataReady);
      pointInterpSupporterDestroy(&interpInfo);
      return TSDB_CODE_SUCCESS;
    }
  }

  /*
   * here we set the value for before and after the specified time into the
   * parameter for interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &interpInfo);
  pointInterpSupporterDestroy(&interpInfo);

  // todo move to other location
  //  if (!forwardQueryStartPosIfNeeded(pQInfo, pQInfo, dataInDisk, dataInCache)) {
  //    return TSDB_CODE_SUCCESS;
  //  }

  int64_t rs = taosGetIntervalStartTimestamp(pQuery->window.skey, pQuery->intervalTime, pQuery->slidingTimeUnit,
                                             pQuery->precision);
  taosInitInterpoInfo(&pRuntimeEnv->interpoInfo, pQuery->order.order, rs, 0, 0);
  //  allocMemForInterpo(pQInfo, pQuery, pMeterObj);

  if (!isPointInterpoQuery(pQuery)) {
    //    assert(pQuery->pos >= 0 && pQuery->slot >= 0);
  }

  // the pQuery->window.skey is changed during normalizedFirstQueryRange, so set the newest lastkey value
  pQuery->lastKey = pQuery->window.skey;
  return TSDB_CODE_SUCCESS;
}

static UNUSED_FUNC bool isGroupbyEachTable(SSqlGroupbyExpr *pGroupbyExpr, STableGroupList *pSidset) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndex *pColIndex = &pGroupbyExpr->columnInfo[i];
    if (pColIndex->flag == TSDB_COL_TAG) {
      //      assert(pSidset->numOfTables == pSidset->numOfSubSet);
      return true;
    }
  }

  return false;
}

static UNUSED_FUNC bool doCheckWithPrevQueryRange(SQuery *pQuery, TSKEY nextKey) {
  if ((nextKey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextKey < pQuery->window.ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    return false;
  }

  return true;
}

static UNUSED_FUNC void enableExecutionForNextTable(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
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
  size_t numOfTables = taosArrayGetSize(pQInfo->pTableList);
  
  tsdb_query_handle_t *pQueryHandle = pRuntimeEnv->pQueryHandle;
  while (tsdbNextDataBlock(pQueryHandle)) {
    if (isQueryKilled(pQInfo)) {
      break;
    }

    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);
    STableDataInfo* pTableDataInfo = NULL;
    
    // todo opt performance
    for(int32_t i = 0; i < numOfTables; ++i) {
      if (pQInfo->pTableDataInfo[i].pTableQInfo->tid == blockInfo.sid) {
        pTableDataInfo = &pQInfo->pTableDataInfo[i];
        break;
      }
    }

    assert(pTableDataInfo != NULL && pTableDataInfo->pTableQInfo != NULL);
    STableQueryInfo *pTableQueryInfo = pTableDataInfo->pTableQInfo;

    restoreIntervalQueryRange(pRuntimeEnv, pTableQueryInfo);

    SDataStatis *pStatis = NULL;
    SArray *     pDataBlock = loadDataBlockOnDemand(pRuntimeEnv, &blockInfo, &pStatis);

    TSKEY nextKey = blockInfo.window.ekey;
    if (!isIntervalQuery(pQuery)) {
      setExecutionContext(pQInfo, pTableQueryInfo, pTableDataInfo->tableIndex, pTableDataInfo->groupIdx, nextKey);
    } else {  // interval query
      setIntervalQueryRange(pTableQueryInfo, pQInfo, nextKey);
      int32_t ret = setAdditionalInfo(pQInfo, pTableDataInfo->tableIndex, pTableQueryInfo);
      
      if (ret != TSDB_CODE_SUCCESS) {
        pQInfo->code = ret;
        return taosGetTimestampMs() - st;
      }
    }

    stableApplyFunctionsOnBlock(pRuntimeEnv, pTableDataInfo, &blockInfo, pStatis, pDataBlock, binarySearchForKey);
  }
  
  int64_t et = taosGetTimestampMs();
  return et - st;
}

static bool multimeterMultioutputHelper(SQInfo *pQInfo, bool *dataInDisk, bool *dataInCache, int32_t index,
                                        int32_t start) {
  //  STableIdInfo **pMeterSidExtInfo = pQInfo->pMeterSidExtInfo;
//  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
//  SQuery *          pQuery = pRuntimeEnv->pQuery;
#if 0
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  
  SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[index]->sid);
  if (pMeterObj == NULL) {
    dError("QInfo:%p do not find required meter id: %d, all meterObjs id is:", pQInfo, pMeterSidExtInfo[index]->sid);
    return false;
  }
  
  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pMeterSidExtInfo[index]);
  
  dTrace("QInfo:%p query on (%d): vid:%d sid:%d meterId:%s, qrange:%" PRId64 "-%" PRId64, pQInfo, index - start,
         pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey);
  
  pQInfo->pObj = pMeterObj;
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->pMeterObj = pMeterObj;
  
  vnodeUpdateQueryColumnIndex(pQuery, pRuntimeEnv->pMeterObj);
  vnodeUpdateFilterColumnIndex(pQuery);

  vnodeCheckIfDataExists(pRuntimeEnv, pMeterObj, dataInDisk, dataInCache);
  
  // data in file or cache is not qualified for the query. abort
  if (!(dataInCache || dataInDisk)) {
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, qrange:%" PRId64 "-%" PRId64 ", nores, %p", pQInfo, pMeterObj->vnode,
           pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery);
    return false;
  }
  
  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pRuntimeEnv->cur.vnodeIndex == -1) {
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

#endif

//  initCtxOutputBuf(pRuntimeEnv);
  return true;
}

static UNUSED_FUNC int64_t doCheckMetersInGroup(SQInfo *pQInfo, int32_t index, int32_t start) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  bool dataInDisk = true;
  bool dataInCache = true;
  if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, index, start)) {
    return 0;
  }

  SPointInterpoSupporter pointInterpSupporter = {0};
  pointInterpSupporterInit(pQuery, &pointInterpSupporter);
  assert(0);

  //  if (!normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter, NULL)) {
  //    pointInterpSupporterDestroy(&pointInterpSupporter);
  //    return 0;
  //  }

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
static void vnodeSTableSeqProcessor(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_COMPLETED);
  
#if 0
//  STableGroupList *pTableIdList = pSupporter->pSidSet;
  
  int32_t vid = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[0]->sid)->vnode;
  
  if (isPointInterpoQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
    
    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);
    
    while (pSupporter->subgroupIdx < pTableIdList->numOfSubSet) {
      int32_t start = pTableIdList->starterPos[pSupporter->subgroupIdx];
      int32_t end = pTableIdList->starterPos[pSupporter->subgroupIdx + 1] - 1;
      
      if (isFirstLastRowQuery(pQuery)) {
        dTrace("QInfo:%p last_row query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, vid, pTableIdList->numOfSubSet,
               pSupporter->subgroupIdx);
        
        TSKEY   key = -1;
        int32_t index = -1;
        
        // choose the last key for one group
        pSupporter->meterIdx = start;
        
        for (int32_t k = start; k <= end; ++k, pSupporter->meterIdx++) {
          if (isQueryKilled(pQInfo)) {
            setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
            return;
          }
          
          // get the last key of meters that belongs to this group
          SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[k]->sid);
          if (pMeterObj != NULL) {
            if (key < pMeterObj->lastKey) {
              key = pMeterObj->lastKey;
              index = k;
            }
          }
        }
        
        pQuery->skey = key;
        pQuery->ekey = key;
        pSupporter->rawSKey = key;
        pSupporter->rawEKey = key;
        
        int64_t num = doCheckMetersInGroup(pQInfo, index, start);
        assert(num >= 0);
      } else {
        dTrace("QInfo:%p interp query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, vid, pTableIdList->numOfSubSet,
               pSupporter->subgroupIdx);
        
        for (int32_t k = start; k <= end; ++k) {
          if (isQueryKilled(pQInfo)) {
            setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
            return;
          }
          
          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;
          
          int64_t num = doCheckMetersInGroup(pQInfo, k, start);
          if (num == 1) {
            break;
          }
        }
      }
      
      pSupporter->subgroupIdx++;
      
      // output buffer is full, return to client
      if (pQuery->size >= pQuery->pointsToRead) {
        break;
      }
    }
  } else {
    /*
     * 1. super table projection query, 2. group-by on normal columns query, 3. ts-comp query
     */
    assert(pSupporter->meterIdx >= 0);
    
    /*
     * if the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (pSupporter->subgroupIdx > 0) {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
      pQInfo->size += pQuery->size;
      
      if (pQuery->size > 0) {
        return;
      }
    }
    
    if (pSupporter->meterIdx >= pTableIdList->numOfTables) {
      return;
    }
    
    resetCtxOutputBuf(pRuntimeEnv);
    resetTimeWindowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);
    
    while (pSupporter->meterIdx < pSupporter->numOfTables) {
      int32_t k = pSupporter->meterIdx;
      
      if (isQueryKilled(pQInfo)) {
        setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        return;
      }
      
      TSKEY skey = pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key;
      if (skey > 0) {
        pQuery->skey = skey;
      }
      
      bool dataInDisk = true;
      bool dataInCache = true;
      if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, k, 0)) {
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;
        
        pSupporter->meterIdx++;
        continue;
      }

#if DEFAULT_IO_ENGINE == IO_ENGINE_MMAP
      for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
        resetMMapWindow(&pRuntimeEnv->pVnodeFiles[i]);
      }
#endif
      
      SPointInterpoSupporter pointInterpSupporter = {0};
      assert(0);
//      if (normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter, NULL) == false) {
//        pQuery->skey = pSupporter->rawSKey;
//        pQuery->ekey = pSupporter->rawEKey;
//
//        pSupporter->meterIdx++;
//        continue;
//      }
      
      // TODO handle the limit problem
      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        forwardQueryStartPosition(pRuntimeEnv);
        
        if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;
          
          pSupporter->meterIdx++;
          continue;
        }
      }
      
      scanAllDataBlocks(pRuntimeEnv);
      
      pQuery->size = getNumOfResult(pRuntimeEnv);
      doSkipResults(pRuntimeEnv);
      
      // the limitation of output result is reached, set the query completed
      if (doRevisedResultsByLimit(pQInfo)) {
        pSupporter->meterIdx = pSupporter->pSidSet->numOfTables;
        break;
      }
      
      // enable execution for next table, when handling the projection query
      enableExecutionForNextTable(pRuntimeEnv);
      
      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
        /*
         * query range is identical in terms of all meters involved in query,
         * so we need to restore them at the *beginning* of query on each meter,
         * not the consecutive query on meter on which is aborted due to buffer limitation
         * to ensure that, we can reset the query range once query on a meter is completed.
         */
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;
        pSupporter->meterIdx++;
        
        pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;
        
        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL) ||
            isGroupbyEachTable(pQuery->pGroupbyExpr, pSupporter->pSidSet)) {
          break;
        }
        
      } else {  // forward query range
        pQuery->skey = pQuery->lastKey;
        
        // all data in the result buffer are skipped due to the offset, continue to retrieve data from current meter
        if (pQuery->size == 0) {
          assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          continue;
        } else {
          pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;
          // buffer is full, wait for the next round to retrieve data from current meter
          assert(Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          break;
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
      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        pResult->numOfRows = MAX(pResult->numOfRows, pResult->resultInfo[j].numOfRes);
      }
    }
    
    pQInfo->pTableQuerySupporter->subgroupIdx = 0;
    pQuery->size = 0;
    copyFromWindowResToSData(pQInfo, pWindowResInfo->pResult);
  }
  
  pQInfo->size += pQuery->size;
  pQuery->pointsOffset = pQuery->pointsToRead;
  
  dTrace(
      "QInfo %p vid:%d, numOfTables:%d, index:%d, numOfGroups:%d, %d points returned, totalRead:%d totalReturn:%d,"
      "next skey:%" PRId64 ", offset:%" PRId64,
      pQInfo, vid, pTableIdList->numOfTables, pSupporter->meterIdx, pTableIdList->numOfSubSet, pQuery->size, pQInfo->size,
      pQInfo->pointsReturned, pQuery->skey, pQuery->limit.offset);
#endif
}

static void createTableDataInfo(SQInfo* pQInfo) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  
  // todo make sure the table are added the reference count to gauranteed that all involved tables are valid
  int32_t numOfTables = taosArrayGetSize(pQInfo->pTableList);
  
  if (pQInfo->pTableDataInfo == NULL) {
    pQInfo->pTableDataInfo = (STableDataInfo *)calloc(1, sizeof(STableDataInfo) * numOfTables);
    if (pQInfo->pTableDataInfo == NULL) {
      dError("QInfo:%p failed to allocate memory, %s", pQInfo, strerror(errno));
      pQInfo->code = -TSDB_CODE_SERV_OUT_OF_MEMORY;
      return;
    }
    
    int32_t groupId = 0;
    for (int32_t i = 0; i < numOfTables; ++i) {  // load all meter meta info
      STableId *id = taosArrayGet(pQInfo->pTableList, i);
      STableDataInfo *pInfo = &pQInfo->pTableDataInfo[i];
      
      setTableDataInfo(pInfo, i, groupId);
      pInfo->pTableQInfo = createTableQueryInfo(&pQInfo->runtimeEnv, id->tid, pQuery->window);
    }
  }
}

static void prepareQueryInfoForReverseScan(SQInfo *pQInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  size_t numOfTables = taosArrayGetSize(pQInfo->pTableList);
  
  for (int32_t i = 0; i < numOfTables; ++i) {
    STableQueryInfo *pTableQueryInfo = pQInfo->pTableDataInfo[i].pTableQInfo;
    changeMeterQueryInfoForSuppleQuery(pQuery, pTableQueryInfo);
  }
}

static void doSaveContext(SQInfo* pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;
  
  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);
  disableFuncForReverseScan(pQInfo, pQuery->order.order);
  
  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1u;
  }
  
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  prepareQueryInfoForReverseScan(pQInfo);
}

static void doRestoreContext(SQInfo* pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;
  
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  
  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1;
  }
  
  enableFuncForForwardScan(pRuntimeEnv, pQuery->order.order);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
}

static void doCloseAllTimeWindowAfterScan(SQInfo* pQInfo) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  size_t numOfTables = taosArrayGetSize(pQInfo->pTableList);
  
  if (isIntervalQuery(pQuery)) {
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableQueryInfo *pTableQueryInfo = pQInfo->pTableDataInfo[i].pTableQInfo;
      closeAllTimeWindow(&pTableQueryInfo->windowResInfo);
    }
  } else {  // close results for group result
    closeAllTimeWindow(&pQInfo->runtimeEnv.windowResInfo);
  }
}

static void multiTableQueryProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (pQInfo->subgroupIdx > 0) {
    /*
     * if the subgroupIdx > 0, the query process must be completed yet, we only need to
     * copy the data into output buffer
     */
    if (isIntervalQuery(pQuery)) {
      copyResToQueryResultBuf(pQInfo, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    } else {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    }

    pQuery->rec.rows += pQuery->rec.rows;

    if (pQuery->rec.rows == 0) {
      //      vnodePrintQueryStatistics(pSupporter);
    }

    dTrace("QInfo:%p current:%lld, total:%lld", pQInfo, pQuery->rec.rows, pQuery->rec.total);
    return;
  }
  
  dTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", order:%d, forward scan start", pQInfo, pQuery->window.skey,
         pQuery->window.ekey, pQuery->order.order);
  
  // create the query support structures
  createTableDataInfo(pQInfo);
  
  // do check all qualified data blocks
  int64_t el = queryOnDataBlocks(pQInfo);
  dTrace("QInfo:%p forward scan completed, elapsed time: %lldms, reversed scan start, order:%d", pQInfo, el,
         pQuery->order.order ^ 1u);
  
  // query error occurred or query is killed, abort current execution
  if (pQInfo->code != TSDB_CODE_SUCCESS || isQueryKilled(pQInfo)) {
    dTrace("QInfo:%p query killed or error occurred, code:%d, abort", pQInfo, pQInfo->code);
    return;
  }
  
  // close all time window results
  doCloseAllTimeWindowAfterScan(pQInfo);
  
  if (needReverseScan(pQuery)) {
    doSaveContext(pQInfo);
    
    el = queryOnDataBlocks(pQInfo);
    dTrace("QInfo:%p reversed scan completed, elapsed time: %lldms", pQInfo, el);
    
    doRestoreContext(pQInfo);
  } else {
    dTrace("QInfo:%p no need to do reversed scan, query completed", pQInfo);
  }
  
  setQueryStatus(pQuery, QUERY_COMPLETED);
  
  if (pQInfo->code != TSDB_CODE_SUCCESS || isQueryKilled(pQInfo)) {
    dTrace("QInfo:%p query killed or error occurred, code:%d, abort", pQInfo, pQInfo->code);
    return;
  }
  
  if (isIntervalQuery(pQuery) || isSumAvgRateQuery(pQuery)) {
//    assert(pSupporter->subgroupIdx == 0 && pSupporter->numOfGroupResultPages == 0);
    
    if (mergeIntoGroupResult(pQInfo) == TSDB_CODE_SUCCESS) {
      copyResToQueryResultBuf(pQInfo, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    }
  } else {  // not a interval query
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
  }
  
  // handle the limitation of output buffer
  dTrace("QInfo:%p points returned:%d, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
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

  doSkipResults(pRuntimeEnv);
  doRevisedResultsByLimit(pQInfo);
}

static void tableMultiOutputProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // for ts_comp query, re-initialized is not allowed
  if (!isTSCompQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

  while (1) {
    scanAllDataBlocks(pRuntimeEnv);
    finalizeQueryResult(pRuntimeEnv);

    if (isQueryKilled(pQInfo)) {
      return;
    }

    pQuery->rec.rows = getNumOfResult(pRuntimeEnv);
    if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols > 0 && pQuery->rec.rows > 0) {
      doSkipResults(pRuntimeEnv);
    }

    /*
     * 1. if pQuery->size == 0, pQuery->limit.offset >= 0, still need to check data
     * 2. if pQuery->size > 0, pQuery->limit.offset must be 0
     */
    if (pQuery->rec.rows > 0 || Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      break;
    }

    dTrace("QInfo:%p vid:%d sid:%d id:%s, skip current result, offset:%" PRId64 ", next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pQuery->limit.offset, pQuery->lastKey);

    resetCtxOutputBuf(pRuntimeEnv);
  }

  doRevisedResultsByLimit(pQInfo);
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    dTrace("QInfo:%p query paused due to output limitation, next qrange:%" PRId64 "-%" PRId64,
        pQInfo, pQuery->lastKey, pQuery->window.ekey);
  }

//  dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned, totalRead:%d totalReturn:%d", pQInfo, pMeterObj->vnode,
//         pMeterObj->sid, pMeterObj->meterId, pQuery->size, pQInfo->size, pQInfo->pointsReturned);

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

    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED|QUERY_RESBUF_FULL)) {
      break;
    }
  }
}

// handle time interval query on table
static void tableIntervalProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &(pQInfo->runtimeEnv);
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfInterpo = 0;

  while (1) {
    tableIntervalProcessImpl(pRuntimeEnv);

    if (isIntervalQuery(pQuery)) {
      pQInfo->subgroupIdx = 0;  // always start from 0
      pQuery->rec.rows = 0;
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);

      clearFirstNTimeWindow(pRuntimeEnv, pQInfo->subgroupIdx);
    }

    // the offset is handled at prepare stage if no interpolation involved
    if (pQuery->interpoType == TSDB_INTERPO_NONE) {
      doRevisedResultsByLimit(pQInfo);
      break;
    } else {
      taosInterpoSetStartInfo(&pRuntimeEnv->interpoInfo, pQuery->rec.rows, pQuery->interpoType);
      SData **pInterpoBuf = pRuntimeEnv->pInterpoBuf;

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        memcpy(pInterpoBuf[i]->data, pQuery->sdata[i]->data, pQuery->rec.rows * pQuery->pSelectExpr[i].resBytes);
      }

      numOfInterpo = 0;
      pQuery->rec.rows = vnodeQueryResultInterpolate(
          pQInfo, (tFilePage **)pQuery->sdata, (tFilePage **)pInterpoBuf, pQuery->rec.rows, &numOfInterpo);

      dTrace("QInfo: %p interpo completed, final:%d", pQInfo, pQuery->rec.rows);
      if (pQuery->rec.rows > 0 || Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
        doRevisedResultsByLimit(pQInfo);
        break;
      }

      // no result generated yet, continue retrieve data
      pQuery->rec.rows = 0;
    }
  }

  // all data scanned, the group by normal column can return
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // todo refactor with merge interval time result
    pQInfo->subgroupIdx = 0;
    pQuery->rec.rows = 0;
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    clearFirstNTimeWindow(pRuntimeEnv, pQInfo->subgroupIdx);
  }

  pQInfo->pointsInterpo += numOfInterpo;
}

static void tableQueryImpl(SQInfo* pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;
  
  if (vnodeHasRemainResults(pQInfo)) {
    /*
     * There are remain results that are not returned due to result interpolation
     * So, we do keep in this procedure instead of launching retrieve procedure for next results.
     */
    int32_t numOfInterpo = 0;
    int32_t remain = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);
    pQuery->rec.rows = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata,
                                                   (tFilePage **)pRuntimeEnv->pInterpoBuf, remain, &numOfInterpo);
    
    doRevisedResultsByLimit(pQInfo);
    
    pQInfo->pointsInterpo += numOfInterpo;
    dTrace("QInfo:%p current:%d returned, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total);
    sem_post(&pQInfo->dataReady);
    return;
  }
  
  // here we have scan all qualified data in both data file and cache
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    // continue to get push data from the group result
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr) ||
          ((isIntervalQuery(pQuery) && pQuery->rec.total < pQuery->limit.limit))) {
      
      // todo limit the output for interval query?
      pQuery->rec.rows = 0;
      pQInfo->subgroupIdx = 0;  // always start from 0
      
      if (pRuntimeEnv->windowResInfo.size > 0) {
        copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
        pQuery->rec.rows += pQuery->rec.rows;
        
        clearFirstNTimeWindow(pRuntimeEnv, pQInfo->subgroupIdx);
        
        if (pQuery->rec.rows > 0) {
          dTrace("QInfo:%p %d rows returned from group results, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total);
          sem_post(&pQInfo->dataReady);
          return;
        }
      }
    }
    
    dTrace("QInfo:%p query over, %d rows are returned", pQInfo, pQuery->rec.total);
    //    vnodePrintQueryStatistics(pSupporter);
    sem_post(&pQInfo->dataReady);
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
  assert(taosArrayGetSize(pQInfo->pTableList) == 1);
  
  /* check if query is killed or not */
  if (isQueryKilled(pQInfo)) {
    dTrace("QInfo:%p query is killed", pQInfo);
  } else {
    STableId* pTableId = taosArrayGet(pQInfo->pTableList, 0);
    dTrace("QInfo:%p uid:%" PRIu64 " tid:%d, query completed, %" PRId64 " rows returned, numOfTotal:%" PRId64 " rows",
        pQInfo, pTableId->uid, pTableId->tid, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
  }
  
  sem_post(&pQInfo->dataReady);
}

static void stableQueryImpl(SQInfo* pQInfo) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  pQuery->rec.rows = 0;
  
  int64_t st = taosGetTimestampUs();
  
  if (isIntervalQuery(pQuery) ||
      (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)) && !isGroupbyNormalCol(pQuery->pGroupbyExpr))) {
    multiTableQueryProcess(pQInfo);
  } else {
    assert((pQuery->checkBuffer == 1 && pQuery->intervalTime == 0) || isPointInterpoQuery(pQuery) ||
        isGroupbyNormalCol(pQuery->pGroupbyExpr));
    
    vnodeSTableSeqProcessor(pQInfo);
  }
  
  // record the total elapsed time
  pQInfo->elapsedTime += (taosGetTimestampUs() - st);
//  taosInterpoSetStartInfo(&pQInfo->runtimeEnv.interpoInfo, pQuery->size, pQInfo->query.interpoType);
  
  if (pQuery->rec.rows == 0) {
    int32_t numOfTables = taosArrayGetSize(pQInfo->pTableList);
    dTrace("QInfo:%p over, %d tables queried, %d points are returned", pQInfo, numOfTables, pQuery->rec.total);
//    vnodePrintQueryStatistics(pSupporter);
  }
  
  sem_post(&pQInfo->dataReady);
}

static int32_t getColumnIndexInSource(SQueryTableMsg *pQueryMsg, SSqlFuncExprMsg *pExprMsg) {
  int32_t j = 0;

  while (j < pQueryMsg->numOfCols) {
    if (pExprMsg->colInfo.colId == pQueryMsg->colList[j].colId) {
      break;
    }

    j += 1;
  }

  return j;
}

bool vnodeValidateExprColumnInfo(SQueryTableMsg *pQueryMsg, SSqlFuncExprMsg *pExprMsg) {
  int32_t j = getColumnIndexInSource(pQueryMsg, pExprMsg);
  return j < pQueryMsg->numOfCols;
}

static int32_t validateQueryMsg(SQueryTableMsg *pQueryMsg) {
  if (pQueryMsg->intervalTime < 0) {
    dError("qmsg:%p illegal value of aggTimeInterval %" PRId64 "", pQueryMsg, pQueryMsg->intervalTime);
    return -1;
  }

  if (pQueryMsg->numOfCols <= 0 || pQueryMsg->numOfCols > TSDB_MAX_COLUMNS) {
    dError("qmsg:%p illegal value of numOfCols %d", pQueryMsg, pQueryMsg->numOfCols);
    return -1;
  }

  if (pQueryMsg->numOfTables <= 0) {
    dError("qmsg:%p illegal value of numOfTables %d", pQueryMsg, pQueryMsg->numOfTables);
    return -1;
  }

  if (pQueryMsg->numOfGroupCols < 0) {
    dError("qmsg:%p illegal value of numOfGroupbyCols %d", pQueryMsg, pQueryMsg->numOfGroupCols);
    return -1;
  }

  if (pQueryMsg->numOfOutputCols > TSDB_MAX_COLUMNS || pQueryMsg->numOfOutputCols <= 0) {
    dError("qmsg:%p illegal value of output columns %d", pQueryMsg, pQueryMsg->numOfOutputCols);
    return -1;
  }

  return 0;
}

static char* createTableIdList(SQueryTableMsg* pQueryMsg, char* pMsg, SArray** pTableIdList) {
  assert(pQueryMsg->numOfTables > 0);
  
  *pTableIdList = taosArrayInit(pQueryMsg->numOfTables, sizeof(STableId));
  
  STableIdInfo *pTableIdInfo = (STableIdInfo *)pMsg;
  pTableIdInfo->sid = htonl(pTableIdInfo->sid);
  pTableIdInfo->uid = htobe64(pTableIdInfo->uid);
  pTableIdInfo->key = htobe64(pTableIdInfo->key);
  
  STableId id = {.uid = pTableIdInfo->uid, .tid = pTableIdInfo->sid};
  taosArrayPush(*pTableIdList, &id);
  
  pMsg += sizeof(STableIdInfo);
  
  for (int32_t j = 1; j < pQueryMsg->numOfTables; ++j) {
    pTableIdInfo = (STableIdInfo *)pMsg;
    
    pTableIdInfo->sid = htonl(pTableIdInfo->sid);
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
static int32_t convertQueryMsg(SQueryTableMsg *pQueryMsg, SArray **pTableIdList, SSqlFuncExprMsg ***pExpr,
      char** tagCond, SColIndex** groupbyCols) {
  pQueryMsg->numOfTables   = htonl(pQueryMsg->numOfTables);

  pQueryMsg->window.skey   = htobe64(pQueryMsg->window.skey);
  pQueryMsg->window.ekey   = htobe64(pQueryMsg->window.ekey);
  pQueryMsg->intervalTime  = htobe64(pQueryMsg->intervalTime);
  pQueryMsg->slidingTime   = htobe64(pQueryMsg->slidingTime);
  pQueryMsg->limit         = htobe64(pQueryMsg->limit);
  pQueryMsg->offset        = htobe64(pQueryMsg->offset);
  
  pQueryMsg->order         = htons(pQueryMsg->order);
  pQueryMsg->orderColId    = htons(pQueryMsg->orderColId);
  pQueryMsg->queryType     = htons(pQueryMsg->queryType);

  pQueryMsg->numOfCols     = htons(pQueryMsg->numOfCols);
  pQueryMsg->numOfOutputCols = htons(pQueryMsg->numOfOutputCols);
  pQueryMsg->numOfGroupCols = htons(pQueryMsg->numOfGroupCols);
  pQueryMsg->tagCondLen    = htons(pQueryMsg->tagCondLen);
  pQueryMsg->tsOffset      = htonl(pQueryMsg->tsOffset);
  pQueryMsg->tsLen         = htonl(pQueryMsg->tsLen);
  pQueryMsg->tsNumOfBlocks = htonl(pQueryMsg->tsNumOfBlocks);
  pQueryMsg->tsOrder       = htonl(pQueryMsg->tsOrder);

  // query msg safety check
  if (validateQueryMsg(pQueryMsg) != 0) {
    return TSDB_CODE_INVALID_QUERY_MSG;
  }

  char *pMsg = (char *)(pQueryMsg->colList) + sizeof(SColumnInfo) * pQueryMsg->numOfCols;

  for (int32_t col = 0; col < pQueryMsg->numOfCols; ++col) {
    SColumnInfo* pColInfo = &pQueryMsg->colList[col];
    
    pColInfo->colId = htons(pColInfo->colId);
    pColInfo->type  = htons(pColInfo->type);
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

      pDestFilterInfo->filterOnBinary = htons(pFilterInfo->filterOnBinary);

      pMsg += sizeof(SColumnFilterInfo);

      if (pDestFilterInfo->filterOnBinary) {
        pDestFilterInfo->len = htobe64(pFilterInfo->len);

        pDestFilterInfo->pz = (int64_t)calloc(1, pDestFilterInfo->len + 1);
        memcpy((void *)pDestFilterInfo->pz, pMsg, pDestFilterInfo->len + 1);
        pMsg += (pDestFilterInfo->len + 1);
      } else {
        pDestFilterInfo->lowerBndi = htobe64(pFilterInfo->lowerBndi);
        pDestFilterInfo->upperBndi = htobe64(pFilterInfo->upperBndi);
      }

      pDestFilterInfo->lowerRelOptr = htons(pFilterInfo->lowerRelOptr);
      pDestFilterInfo->upperRelOptr = htons(pFilterInfo->upperRelOptr);
    }
  }

  bool hasArithmeticFunction = false;

  *pExpr = calloc(pQueryMsg->numOfOutputCols, POINTER_BYTES);
  SSqlFuncExprMsg *pExprMsg = (SSqlFuncExprMsg *)pMsg;

  for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
    (*pExpr)[i] = pExprMsg;

    pExprMsg->colInfo.colIndex = htons(pExprMsg->colInfo.colIndex);
    pExprMsg->colInfo.colId  = htons(pExprMsg->colInfo.colId);
    pExprMsg->colInfo.flag   = htons(pExprMsg->colInfo.flag);
    pExprMsg->functionId     = htons(pExprMsg->functionId);
    pExprMsg->numOfParams    = htons(pExprMsg->numOfParams);

    pMsg += sizeof(SSqlFuncExprMsg);

    for (int32_t j = 0; j < pExprMsg->numOfParams; ++j) {
      pExprMsg->arg[j].argType  = htons(pExprMsg->arg[j].argType);
      pExprMsg->arg[j].argBytes = htons(pExprMsg->arg[j].argBytes);

      if (pExprMsg->arg[j].argType == TSDB_DATA_TYPE_BINARY) {
        pExprMsg->arg[j].argValue.pz = pMsg;
        pMsg += pExprMsg->arg[j].argBytes + 1;  // one more for the string terminated char.
      } else {
        pExprMsg->arg[j].argValue.i64 = htobe64(pExprMsg->arg[j].argValue.i64);
      }
    }

    if (pExprMsg->functionId == TSDB_FUNC_ARITHM) {
      hasArithmeticFunction = true;
    } else if (pExprMsg->functionId == TSDB_FUNC_TAG || pExprMsg->functionId == TSDB_FUNC_TAGPRJ ||
               pExprMsg->functionId == TSDB_FUNC_TAG_DUMMY) {
      if (pExprMsg->colInfo.flag != TSDB_COL_TAG) {  // ignore the column  index check for arithmetic expression.
        return TSDB_CODE_INVALID_QUERY_MSG;
      }
    } else {
      if (!vnodeValidateExprColumnInfo(pQueryMsg, pExprMsg)) {
        return TSDB_CODE_INVALID_QUERY_MSG;
      }
    }

    pExprMsg = (SSqlFuncExprMsg *)pMsg;
  }

  pQueryMsg->colNameLen = htonl(pQueryMsg->colNameLen);
  if (hasArithmeticFunction) {  // column name array
    assert(pQueryMsg->colNameLen > 0);
    pQueryMsg->colNameList = (int64_t)pMsg;
    pMsg += pQueryMsg->colNameLen;
  }
  
  pMsg = createTableIdList(pQueryMsg, pMsg, pTableIdList);

  if (pQueryMsg->numOfGroupCols > 0) {  // group by tag columns
    *groupbyCols = malloc(pQueryMsg->numOfGroupCols*sizeof(SColIndex));
    
    for(int32_t i = 0; i < pQueryMsg->numOfGroupCols; ++i) {
      (*groupbyCols)[i].colId = *(int16_t*) pMsg;
      pMsg += sizeof((*groupbyCols)[i].colId);
      
      (*groupbyCols)[i].colIndex = *(int16_t*) pMsg;
      pMsg += sizeof((*groupbyCols)[i].colIndex);

      (*groupbyCols)[i].flag = *(int16_t*) pMsg;
      pMsg += sizeof((*groupbyCols)[i].flag);

      memcpy((*groupbyCols)[i].name, pMsg, tListLen(groupbyCols[i]->name));
      pMsg += tListLen((*groupbyCols)[i].name);
    }
    
    pQueryMsg->orderByIdx = htons(pQueryMsg->orderByIdx);
    pQueryMsg->orderType = htons(pQueryMsg->orderType);

    pMsg += sizeof(SColIndex) * pQueryMsg->numOfGroupCols;
  }

  pQueryMsg->interpoType = htons(pQueryMsg->interpoType);
  if (pQueryMsg->interpoType != TSDB_INTERPO_NONE) {
    pQueryMsg->defaultVal = (uint64_t)(pMsg);

    int64_t *v = (int64_t *)pMsg;
    for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
      v[i] = htobe64(v[i]);
    }
    
    pMsg += sizeof(int64_t) * pQueryMsg->numOfOutputCols;
  }
  
  // the tag query condition expression string is located at the end of query msg
  if (pQueryMsg->tagCondLen > 0) {
    *tagCond = calloc(1, pQueryMsg->tagCondLen);
    memcpy(*tagCond, pMsg, pQueryMsg->tagCondLen);
  }
  
  dTrace("qmsg:%p query on %d table(s), qrange:%" PRId64 "-%" PRId64 ", numOfGroupbyTagCols:%d, ts order:%d, "
         "outputCols:%d, numOfCols:%d, interval:%d" PRId64 ", fillType:%d, comptsLen:%d, limit:%" PRId64 ", offset:%" PRId64,
         pQueryMsg, pQueryMsg->numOfTables, pQueryMsg->window.skey, pQueryMsg->window.ekey,
         pQueryMsg->numOfGroupCols, pQueryMsg->order, pQueryMsg->numOfOutputCols,
         pQueryMsg->numOfCols, pQueryMsg->intervalTime, pQueryMsg->interpoType, pQueryMsg->tsLen,
         pQueryMsg->limit, pQueryMsg->offset);

  return 0;
}

static int32_t buildAirthmeticExprFromMsg(SSqlFunctionExpr *pExpr, SQueryTableMsg *pQueryMsg) {
//  SSqlBinaryExprInfo *pBinaryExprInfo = &pExpr->binExprInfo;
//  SColumnInfo *       pColMsg = pQueryMsg->colList;
#if 0
  tExprNode* pBinExpr = NULL;
  SSchema*        pSchema = toSchema(pQueryMsg, pColMsg, pQueryMsg->numOfCols);
  
  dTrace("qmsg:%p create binary expr from string:%s", pQueryMsg, pExpr->pBase.arg[0].argValue.pz);
  tSQLBinaryExprFromString(&pBinExpr, pSchema, pQueryMsg->numOfCols, pExpr->pBase.arg[0].argValue.pz,
                           pExpr->pBase.arg[0].argBytes);
  
  if (pBinExpr == NULL) {
    dError("qmsg:%p failed to create arithmetic expression string from:%s", pQueryMsg, pExpr->pBase.arg[0].argValue.pz);
    return TSDB_CODE_APP_ERROR;
  }
  
  pBinaryExprInfo->pBinExpr = pBinExpr;
  
  int32_t num = 0;
  int16_t ids[TSDB_MAX_COLUMNS] = {0};
  
  tSQLBinaryExprTrv(pBinExpr, &num, ids);
  qsort(ids, num, sizeof(int16_t), id_compar);
  
  int32_t i = 0, j = 0;
  
  while (i < num && j < num) {
    if (ids[i] == ids[j]) {
      j++;
    } else {
      ids[++i] = ids[j++];
    }
  }
  assert(i <= num);
  
  // there may be duplicated referenced columns.
  num = i + 1;
  pBinaryExprInfo->pReqColumns = malloc(sizeof(SColIndex) * num);
  
  for (int32_t k = 0; k < num; ++k) {
    SColIndex* pColIndex = &pBinaryExprInfo->pReqColumns[k];
    pColIndex->colId = ids[k];
  }
  
  pBinaryExprInfo->numOfCols = num;
  free(pSchema);
#endif

  return TSDB_CODE_SUCCESS;
}

static int32_t createSqlFunctionExprFromMsg(SQueryTableMsg *pQueryMsg, SSqlFunctionExpr **pSqlFuncExpr, SSqlFuncExprMsg** pExprMsg) {
  *pSqlFuncExpr = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SSqlFunctionExpr *pExprs = (SSqlFunctionExpr *)calloc(1, sizeof(SSqlFunctionExpr) * pQueryMsg->numOfOutputCols);
  if (pExprs == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  bool    isSuperTable = QUERY_IS_STABLE_QUERY(pQueryMsg->queryType);
  int16_t tagLen = 0;

  for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
    pExprs[i].pBase = *pExprMsg[i];
    pExprs[i].resBytes = 0;

    int16_t type = 0;
    int16_t bytes = 0;

    // parse the arithmetic expression
    if (pExprs[i].pBase.functionId == TSDB_FUNC_ARITHM) {
      code = buildAirthmeticExprFromMsg(&pExprs[i], pQueryMsg);

      if (code != TSDB_CODE_SUCCESS) {
        tfree(pExprs);
        return code;
      }

      type = TSDB_DATA_TYPE_DOUBLE;
      bytes = tDataTypeDesc[type].nSize;
    } else {  // parse the normal column
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].pBase);
      assert(j < pQueryMsg->numOfCols);

      SColumnInfo *pCol = &pQueryMsg->colList[j];
      type = pCol->type;
      bytes = pCol->bytes;
    }

    int32_t param = pExprs[i].pBase.arg[0].argValue.i64;
    if (getResultDataInfo(type, bytes, pExprs[i].pBase.functionId, param, &pExprs[i].resType, &pExprs[i].resBytes,
                          &pExprs[i].interResBytes, 0, isSuperTable) != TSDB_CODE_SUCCESS) {
      tfree(pExprs);
      return TSDB_CODE_INVALID_QUERY_MSG;
    }

    if (pExprs[i].pBase.functionId == TSDB_FUNC_TAG_DUMMY || pExprs[i].pBase.functionId == TSDB_FUNC_TS_DUMMY) {
      tagLen += pExprs[i].resBytes;
    }
    assert(isValidDataType(pExprs[i].resType, pExprs[i].resBytes));
  }

  // get the correct result size for top/bottom query, according to the number of tags columns in selection clause

  // TODO refactor
  for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
    pExprs[i].pBase = *pExprMsg[i];
    int16_t functId = pExprs[i].pBase.functionId;
    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].pBase);
      assert(j < pQueryMsg->numOfCols);

      SColumnInfo *pCol = &pQueryMsg->colList[j];
      int16_t      type = pCol->type;
      int16_t      bytes = pCol->bytes;

      int32_t ret =
          getResultDataInfo(type, bytes, pExprs[i].pBase.functionId, pExprs[i].pBase.arg[0].argValue.i64,
                            &pExprs[i].resType, &pExprs[i].resBytes, &pExprs[i].interResBytes, tagLen, isSuperTable);
      assert(ret == TSDB_CODE_SUCCESS);
    }
  }

  tfree(pExprMsg);
  *pSqlFuncExpr = pExprs;

  return TSDB_CODE_SUCCESS;
}

static SSqlGroupbyExpr *createGroupbyExprFromMsg(SQueryTableMsg *pQueryMsg, SColIndex* pColIndex, int32_t *code) {
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

  pGroupbyExpr->columnInfo = pColIndex;
  return pGroupbyExpr;
}

static int32_t vnodeCreateFilterInfo(void *pQInfo, SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].info.numOfFilters > 0) {
      pQuery->numOfFilterCols++;
    }
  }

  if (pQuery->numOfFilterCols == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pQuery->pFilterInfo = calloc(1, sizeof(SSingleColumnFilterInfo) * pQuery->numOfFilterCols);

  for (int32_t i = 0, j = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].info.numOfFilters > 0) {
      SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[j];

      memcpy(&pFilterInfo->info, &pQuery->colList[i], sizeof(SColumnInfoData));
      pFilterInfo->info.info.filters = NULL;

      pFilterInfo->numOfFilters = pQuery->colList[i].info.numOfFilters;
      pFilterInfo->pFilters = calloc(pFilterInfo->numOfFilters, sizeof(SColumnFilterElem));

      for (int32_t f = 0; f < pFilterInfo->numOfFilters; ++f) {
        SColumnFilterElem *pSingleColFilter = &pFilterInfo->pFilters[f];
        pSingleColFilter->filterInfo = pQuery->colList[i].info.filters[f];

        int32_t lower = pSingleColFilter->filterInfo.lowerRelOptr;
        int32_t upper = pSingleColFilter->filterInfo.upperRelOptr;

        if (lower == TSDB_RELATION_INVALID && upper == TSDB_RELATION_INVALID) {
          dError("QInfo:%p invalid filter info", pQInfo);
          return TSDB_CODE_INVALID_QUERY_MSG;
        }

        int16_t type = pQuery->colList[i].info.type;
        int16_t bytes = pQuery->colList[i].info.bytes;

        __filter_func_t *rangeFilterArray = NULL;  // vnodeGetRangeFilterFuncArray(type);
        __filter_func_t *filterArray = NULL;       // vnodeGetValueFilterFuncArray(type);

        if (rangeFilterArray == NULL && filterArray == NULL) {
          dError("QInfo:%p failed to get filter function, invalid data type:%d", pQInfo, type);
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
              dError("pQInfo:%p failed to get filter function, invalid filter condition", pQInfo, type);
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

static void doUpdateExprColumnIndex(SQuery* pQuery) {
  assert(pQuery->pSelectExpr != NULL && pQuery != NULL);
//  int32_t i = 0, j = 0;
//  while (i < pQuery->numOfCols && j < pMeterObj->numOfColumns) {
//    if (pQuery->colList[i].data.colId == pMeterObj->schema[j].colId) {
//      pQuery->colList[i++].colIndex = (int16_t)j++;
//    } else if (pQuery->colList[i].data.colId < pMeterObj->schema[j].colId) {
//      pQuery->colList[i++].colIndex = -1;
//    } else if (pQuery->colList[i].data.colId > pMeterObj->schema[j].colId) {
//      j++;
//    }
//  }

//  while (i < pQuery->numOfCols) {
//    pQuery->colList[i++].colIndex = -1;  // not such column in current meter
//  }
  
  for(int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    SSqlFuncExprMsg* pSqlExprMsg = &pQuery->pSelectExpr[k].pBase;
    if (pSqlExprMsg->functionId == TSDB_FUNC_ARITHM || pSqlExprMsg->colInfo.flag == TSDB_COL_TAG) {
      continue;
    }
    
    SColIndex* pColIndexEx = &pSqlExprMsg->colInfo;
    for(int32_t f = 0; f < pQuery->numOfCols; ++f) {
      if (pColIndexEx->colId == pQuery->colList[f].info.colId) {
        pColIndexEx->colIndex = f;
        break;
      }
    }
  }
}

static SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *pExprs,
                               SArray *pTableList) {
  SQInfo *pQInfo = (SQInfo *)calloc(1, sizeof(SQInfo));
  if (pQInfo == NULL) {
    goto _clean_pQInfo_memory;
  }

  SQuery *pQuery = calloc(1, sizeof(SQuery));
  pQInfo->runtimeEnv.pQuery = pQuery;

  int16_t numOfCols = pQueryMsg->numOfCols;
  int16_t numOfOutputCols = pQueryMsg->numOfOutputCols;

  pQuery->numOfCols = numOfCols;
  pQuery->numOfOutputCols = numOfOutputCols;

  pQuery->limit.limit = pQueryMsg->limit;
  pQuery->limit.offset = pQueryMsg->offset;

  pQuery->order.order = pQueryMsg->order;
  pQuery->order.orderColId = pQueryMsg->orderColId;

  pQuery->pSelectExpr = pExprs;
  pQuery->pGroupbyExpr = pGroupbyExpr;

  pQuery->intervalTime = pQueryMsg->intervalTime;

  pQuery->slidingTime = pQueryMsg->slidingTime;
  pQuery->slidingTimeUnit = pQueryMsg->slidingTimeUnit;

  pQuery->interpoType = pQueryMsg->interpoType;

  pQuery->colList = calloc(1, sizeof(SSingleColumnFilterInfo) * numOfCols);
  if (pQuery->colList == NULL) {
    goto _clean_memory;
  }

  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i].info = pQueryMsg->colList[i];
    
    SColumnInfo *pColInfo = &pQuery->colList[i].info;
    pColInfo->filters = NULL;
    //    if (colList[i].numOfFilters > 0) {
    //      pColInfo->filters = calloc(1, colList[i].numOfFilters * sizeof(SColumnFilterInfo));
    //
    //      for (int32_t j = 0; j < colList[i].numOfFilters; ++j) {
    //        tscColumnFilterInfoCopy(&pColInfo->filters[j], &colList[i].filters[j]);
    //      }
    //    } else {
    //      pQuery->colList[i].data.filters = NULL;
    //    }
  }

  // calculate the result row size
  for (int16_t col = 0; col < numOfOutputCols; ++col) {
    assert(pExprs[col].resBytes > 0);
    pQuery->rowSize += pExprs[col].resBytes;
  }
  
  doUpdateExprColumnIndex(pQuery);

  int32_t ret = vnodeCreateFilterInfo(pQInfo, pQuery);
  if (ret != TSDB_CODE_SUCCESS) {
    goto _clean_memory;
  }

  // prepare the result buffer
  pQuery->sdata = (SData **)calloc(pQuery->numOfOutputCols, POINTER_BYTES);
  if (pQuery->sdata == NULL) {
    goto _clean_memory;
  }

  // set the output buffer capacity
  pQuery->rec.capacity = 4096;
  pQuery->rec.threshold = 4000;
  
  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
    assert(pExprs[col].interResBytes >= pExprs[col].resBytes);

    // allocate additional memory for interResults that are usually larger then final results
    size_t size = (pQuery->rec.capacity + 1) * pExprs[col].resBytes + pExprs[col].interResBytes + sizeof(SData);
    pQuery->sdata[col] = (SData *)calloc(1, size);
    if (pQuery->sdata[col] == NULL) {
      goto _clean_memory;
    }
  }

  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    pQuery->defaultVal = malloc(sizeof(int64_t) * pQuery->numOfOutputCols);
    if (pQuery->defaultVal == NULL) {
      goto _clean_memory;
    }

    // the first column is the timestamp
    memcpy(pQuery->defaultVal, (char *)pQueryMsg->defaultVal, pQuery->numOfOutputCols * sizeof(int64_t));
  }

  // to make sure third party won't overwrite this structure
  pQInfo->signature = pQInfo;
  pQInfo->pTableList = pTableList;

  pQuery->pos = -1;
  
  pQuery->window.skey = pQueryMsg->window.skey;
  pQuery->window.ekey = pQueryMsg->window.ekey;
  pQuery->lastKey     = pQuery->window.skey;
  
  if (sem_init(&pQInfo->dataReady, 0, 0) != 0) {
    dError("QInfo:%p init dataReady sem failed, reason:%s", pQInfo, strerror(errno));
    goto _clean_memory;
  }
  
  vnodeParametersSafetyCheck(pQuery);
  
  dTrace("qmsg:%p QInfo:%p created", pQueryMsg, pQInfo);
  return pQInfo;

_clean_memory:
  tfree(pQuery->defaultVal);

  if (pQuery->sdata != NULL) {
    for (int16_t col = 0; col < pQuery->numOfOutputCols; ++col) {
      tfree(pQuery->sdata[col]);
    }
  }

  tfree(pQuery->sdata);
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);

  tfree(pExprs);
  tfree(pGroupbyExpr);

_clean_pQInfo_memory:
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
  uint64_t sig = (uint64_t) pQInfo->signature;
  return (sig == (uint64_t)pQInfo);
}

static void freeQInfo(SQInfo *pQInfo);
static int32_t initQInfo(SQueryTableMsg *pQueryMsg, void* tsdb, SQInfo *pQInfo, bool isSTable) {
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
    dTrace("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->window.skey,
           pQuery->window.ekey, pQuery->order.order);
    
    sem_post(&pQInfo->dataReady);
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return TSDB_CODE_SUCCESS;
  }

  // filter the qualified
  if ((code = doInitQInfo(pQInfo, pTSBuf, tsdb, isSTable)) != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  // dTrace("QInfo:%p set query flag and prepare runtime environment completed, ref:%d, wait for schedule", pQInfo,
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
  
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  setQueryKilled(pQInfo);
  
  dTrace("QInfo:%p start to free QInfo", pQInfo);
  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
    tfree(pQuery->sdata[col]);
  }
  
  sem_destroy(&(pQInfo->dataReady));
  teardownQueryRuntimeEnv(&pQInfo->runtimeEnv);
  
  if (pQInfo->pTableDataInfo != NULL) {
    //    size_t num = taosHashGetSize(pQInfo->pTableList);
    for (int32_t j = 0; j < 0; ++j) {
      destroyMeterQueryInfo(pQInfo->pTableDataInfo[j].pTableQInfo, pQuery->numOfOutputCols);
    }
  }
  
  tfree(pQInfo->pTableDataInfo);
  
  for (int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
    SSingleColumnFilterInfo *pColFilter = &pQuery->pFilterInfo[i];
    if (pColFilter->numOfFilters > 0) {
      tfree(pColFilter->pFilters);
    }
  }
  
  tfree(pQuery->pFilterInfo);
  tfree(pQuery->colList);
  tfree(pQuery->sdata);
  
  if (pQuery->pSelectExpr != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlBinaryExprInfo *pBinExprInfo = &pQuery->pSelectExpr[i].binExprInfo;
      
      if (pBinExprInfo->numOfCols > 0) {
        tfree(pBinExprInfo->pReqColumns);
        tExprTreeDestroy(&pBinExprInfo->pBinExpr, NULL);
      }
    }
    
    tfree(pQuery->pSelectExpr);
  }
  
  if (pQuery->defaultVal != NULL) {
    tfree(pQuery->defaultVal);
  }
  
  tfree(pQuery->pGroupbyExpr);
  tfree(pQuery);
  
  taosArrayDestroy(pQInfo->pTableList);
  
  dTrace("QInfo:%p QInfo is freed", pQInfo);
  
  // destroy signature, in order to avoid the query process pass the object safety check
  memset(pQInfo, 0, sizeof(SQInfo));
  tfree(pQInfo);
}

static size_t getResultSize(SQInfo *pQInfo, int64_t *numOfRows) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  
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
      dError("QInfo:%p failed to get file info, path:%s, reason:%s", pQInfo, pQuery->sdata[0]->data, strerror(errno));
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
      dTrace("QInfo:%p ts comp data return, file:%s, size:%zu", pQInfo, pQuery->sdata[0]->data, s);
      
      lseek(fd, 0, SEEK_SET);
      read(fd, data, s);
      close(fd);
      
      unlink(pQuery->sdata[0]->data);
    } else {
      dError("QInfo:%p failed to open tmp file to send ts-comp data to client, path:%s, reason:%s", pQInfo,
             pQuery->sdata[0]->data, strerror(errno));
    }
  } else {
    doCopyQueryResultToMsg(pQInfo, pQuery->rec.rows, data);
  }
  
  pQuery->rec.total += pQuery->rec.rows;
  dTrace("QInfo:%p current:%d, total:%d", pQInfo, pQuery->rec.rows, pQuery->rec.total);
  
  return TSDB_CODE_SUCCESS;
  
  // todo if interpolation exists, the result may be dump to client by several rounds
}

int32_t qCreateQueryInfo(void* tsdb, SQueryTableMsg *pQueryMsg, SQInfo **pQInfo) {
  assert(pQueryMsg != NULL);

  int32_t code = TSDB_CODE_SUCCESS;
  
  char* tagCond = NULL;
  SArray *pTableIdList = NULL;
  SSqlFuncExprMsg** pExprMsg = NULL;
  SColIndex* pGroupColIndex = NULL;
  
  if ((code = convertQueryMsg(pQueryMsg, &pTableIdList, &pExprMsg, &tagCond, &pGroupColIndex)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pQueryMsg->numOfTables <= 0) {
    dError("Invalid number of tables to query, numOfTables:%d", pQueryMsg->numOfTables);
    code = TSDB_CODE_INVALID_QUERY_MSG;
    goto _query_over;
  }

  // todo check vnode status
  if (pTableIdList == NULL || taosArrayGetSize(pTableIdList) == 0) {
    dError("qmsg:%p, SQueryTableMsg wrong format", pQueryMsg);
    code = TSDB_CODE_INVALID_QUERY_MSG;
    goto _query_over;
  }

  SSqlFunctionExpr *pExprs = NULL;
  if ((code = createSqlFunctionExprFromMsg(pQueryMsg, &pExprs, pExprMsg)) != TSDB_CODE_SUCCESS) {
    goto _query_over;
  }

  SSqlGroupbyExpr *pGroupbyExpr = createGroupbyExprFromMsg(pQueryMsg, pGroupColIndex, &code);
  if ((pGroupbyExpr == NULL && pQueryMsg->numOfGroupCols != 0) || code != TSDB_CODE_SUCCESS) {
    goto _query_over;
  }
  
  bool isSTableQuery = false;
  SArray* pGroupList = NULL;
  
  if ((pQueryMsg->queryType & TSDB_QUERY_TYPE_STABLE_QUERY) != 0) {
    isSTableQuery = true;
    
    STableId* id = taosArrayGet(pTableIdList, 0);
    id->uid = -1;  //todo fix me
    
    /*int32_t ret =*/ tsdbQueryTags(tsdb, id->uid, tagCond, pQueryMsg->tagCondLen, &pGroupList, pGroupColIndex, pQueryMsg->numOfGroupCols);
    if (taosArrayGetSize(pGroupList) == 0) { // no qualified tables no need to do query
      code = TSDB_CODE_SUCCESS;
      goto _query_over;
    }
  } else {
    assert(taosArrayGetSize(pTableIdList) == 1);
  
    STableId* id = taosArrayGet(pTableIdList, 0);
    if ((code = tsdbGetOneTableGroup(tsdb, id->uid, &pGroupList)) != TSDB_CODE_SUCCESS) {
      goto _query_over;
    }
  }
  
  (*pQInfo) = createQInfoImpl(pQueryMsg, pGroupbyExpr, pExprs, pGroupList);
  if ((*pQInfo) == NULL) {
    code = TSDB_CODE_SERV_OUT_OF_MEMORY;
  }
  
  code = initQInfo(pQueryMsg, tsdb, *pQInfo, isSTableQuery);
  
_query_over:
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pTableIdList);
  }

  // if failed to add ref for all meters in this query, abort current query
  //  if (code != TSDB_CODE_SUCCESS) {
  //    vnodeDecQueryRefCount(pQueryMsg, pMeterObjList, incNumber);
  //  }
  //
  //  tfree(pQueryMsg->pSqlFuncExprs);
  //  tfree(pMeterObjList);
  //  ret = vnodeSendQueryRspMsg(pObj, code, pObj->qhandle);
  //
  //  tfree(pQueryMsg->pSidExtInfo);
  //  for(int32_t i = 0; i < pQueryMsg->numOfCols; ++i) {
  //    vnodeFreeColumnInfo(&pQueryMsg->colList[i]);
  //  }
  //
  //  atomic_fetch_add_32(&vnodeSelectReqNum, 1);
  return TSDB_CODE_SUCCESS;
}

void qDestroyQueryInfo(SQInfo* pQInfo) {
  dTrace("QInfo:%p query completed", pQInfo);
  freeQInfo(pQInfo);
}

void qTableQuery(SQInfo *pQInfo) {
  if (pQInfo == NULL || pQInfo->signature != pQInfo) {
    dTrace("%p freed abort query", pQInfo);
    return;
  }
  
  if (isQueryKilled(pQInfo)) {
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    return;
  }
  
  dTrace("QInfo:%p query task is launched", pQInfo);
  
  if (pQInfo->runtimeEnv.stableQuery) {
    stableQueryImpl(pQInfo);
  } else {
    tableQueryImpl(pQInfo);
  }
  
  //  vnodeDecRefCount(pQInfo);
}

int32_t qRetrieveQueryResultInfo(SQInfo *pQInfo) {
  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_INVALID_QHANDLE;
  }
  
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  if (isQueryKilled(pQInfo)) {
    dTrace("QInfo:%p query is killed, code:%d", pQInfo, pQInfo->code);
    return pQInfo->code;
  }

  sem_wait(&pQInfo->dataReady);
  dTrace("QInfo:%p retrieve result info, rowsize:%d, rows:%d, code:%d", pQInfo, pQuery->rowSize, pQuery->rec.rows,
      pQInfo->code);
  
  return pQInfo->code;
}

bool qHasMoreResultsToRetrieve(SQInfo* pQInfo) {
  if (pQInfo == NULL || pQInfo->signature != pQInfo || pQInfo->code != TSDB_CODE_SUCCESS) {
    return false;
  }
  
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
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

int32_t qDumpRetrieveResult(SQInfo *pQInfo, SRetrieveTableRsp** pRsp, int32_t* contLen) {
  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_INVALID_QHANDLE;
  }
  
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  size_t size = getResultSize(pQInfo, &pQuery->rec.rows);
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
    (*pRsp)->completed = 1; // notify no more result to client
  }
  
  return code;
  
//  if (numOfRows == 0 && (pRetrieve->qhandle == (uint64_t)pObj->qhandle) && (code != TSDB_CODE_ACTION_IN_PROGRESS)) {
//    dTrace("QInfo:%p %s free qhandle code:%d", pObj->qhandle, __FUNCTION__, code);
//    vnodeDecRefCount(pObj->qhandle);
//    pObj->qhandle = NULL;
//  }
}
