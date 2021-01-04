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
#include "ttype.h"

#define MAX_ROWS_PER_RESBUF_PAGE  ((1u<<12) - 1)

/**
 * check if the primary column is load by default, otherwise, the program will
 * forced to load primary column explicitly.
 */
#define Q_STATUS_EQUAL(p, s)  (((p) & (s)) != 0u)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define IS_MASTER_SCAN(runtime)        ((runtime)->scanFlag == MASTER_SCAN)
#define IS_REVERSE_SCAN(runtime)       ((runtime)->scanFlag == REVERSE_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime)  ((runtime)->scanFlag = MASTER_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define GET_QINFO_ADDR(x) ((SQInfo *)((char *)(x)-offsetof(SQInfo, runtimeEnv)))

#define GET_COL_DATA_POS(query, index, step) ((query)->pos + (index) * (step))
#define SWITCH_ORDER(n) (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

#define SDATA_BLOCK_INITIALIZER (SDataBlockInfo) {{0}, 0}

#define TIME_WINDOW_COPY(_dst, _src)  do {\
   (_dst).skey = (_src).skey;\
   (_dst).ekey = (_src).ekey;\
} while (0)

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

  if (v % 1000 <= 0) {
    return NULL;
  } else {
    return malloc(__size);
  }
}

static UNUSED_FUNC void* u_calloc(size_t num, size_t __size) {
  uint32_t v = rand();
  if (v % 1000 <= 0) {
    return NULL;
  } else {
    return calloc(num, __size);
  }
}

static UNUSED_FUNC void* u_realloc(void* p, size_t __size) {
  uint32_t v = rand();
  if (v % 5 <= 1) {
    return NULL;
  } else {
    return realloc(p, __size);
  }
}

#define calloc  u_calloc
#define malloc  u_malloc
#define realloc u_realloc
#endif

#define CLEAR_QUERY_STATUS(q, st)   ((q)->status &= (~(st)))
#define GET_NUM_OF_TABLEGROUP(q)    taosArrayGetSize((q)->tableqinfoGroupInfo.pGroupList)
#define GET_TABLEGROUP(q, _index)   ((SArray*) taosArrayGetP((q)->tableqinfoGroupInfo.pGroupList, (_index)))
#define QUERY_IS_INTERVAL_QUERY(_q) ((_q)->interval.interval > 0)

static void setQueryStatus(SQuery *pQuery, int8_t status);
static void finalizeQueryResult(SQueryRuntimeEnv *pRuntimeEnv);

static int32_t getMaximumIdleDurationSec() {
  return tsShellActivityTimer * 2;
}

static void getNextTimeWindow(SQuery* pQuery, STimeWindow* tw) {
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (pQuery->interval.intervalUnit != 'n' && pQuery->interval.intervalUnit != 'y') {
    tw->skey += pQuery->interval.sliding * factor;
    tw->ekey = tw->skey + pQuery->interval.interval - 1;
    return;
  }

  int64_t key = tw->skey / 1000, interval = pQuery->interval.interval;
  if (pQuery->precision == TSDB_TIME_PRECISION_MICRO) {
    key /= 1000;
  }
  if (pQuery->interval.intervalUnit == 'y') {
    interval *= 12;
  }

  struct tm tm;
  time_t t = (time_t)key;
  localtime_r(&t, &tm);

  int mon = (int)(tm.tm_year * 12 + tm.tm_mon + interval * factor);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->skey = mktime(&tm) * 1000L;

  mon = (int)(mon + interval);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->ekey = mktime(&tm) * 1000L;

  if (pQuery->precision == TSDB_TIME_PRECISION_MICRO) {
    tw->skey *= 1000L;
    tw->ekey *= 1000L;
  }
  tw->ekey -= 1;
}

#define SET_STABLE_QUERY_OVER(_q) ((_q)->tableIndex = (int32_t)((_q)->tableqinfoGroupInfo.numOfTables))
#define IS_STASBLE_QUERY_OVER(_q) ((_q)->tableIndex >= (int32_t)((_q)->tableqinfoGroupInfo.numOfTables))

// todo move to utility
static int32_t mergeIntoGroupResultImpl(SGroupResInfo* pGroupResInfo, SArray *pTableList, SQInfo* pQInfo);

static void setResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult);
static void setResultRowOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult);
static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void* inputData, TSKEY *tsCol, SDataBlockInfo* pBlockInfo,
                          SDataStatis *pStatis, void *param, int32_t colIndex, int32_t vgId);

static void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static void destroyTableQueryInfoImpl(STableQueryInfo *pTableQueryInfo);
static void resetDefaultResInfoOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static bool hasMainOutput(SQuery *pQuery);
static void buildTagQueryResult(SQInfo *pQInfo);

static int32_t setAdditionalInfo(SQInfo *pQInfo, void *pTable, STableQueryInfo *pTableQueryInfo);
static int32_t checkForQueryBuf(size_t numOfTables);
static void releaseQueryBuf(size_t numOfTables);
static int32_t binarySearchForKey(char *pValue, int num, TSKEY key, int order);
static void doRowwiseTimeWindowInterpolation(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey, int32_t type);
static STsdbQueryCond createTsdbQueryCond(SQuery* pQuery, STimeWindow* win);
static STableIdInfo createTableIdInfo(SQuery* pQuery);

bool doFilterData(SQuery *pQuery, int32_t elemPos) {
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];

    char *pElem = (char*)pFilterInfo->pData + pFilterInfo->info.bytes * elemPos;

    bool qualified = false;
    for (int32_t j = 0; j < pFilterInfo->numOfFilters; ++j) {
      SColumnFilterElem *pFilterElem = &pFilterInfo->pFilters[j];

      bool isnull = isNull(pElem, pFilterInfo->info.type);
      if (isnull) {
        if (pFilterElem->fp == isNull_filter) {
          qualified = true;
          break;
        } else {
          continue;
        }
      } else {
        if (pFilterElem->fp == notNull_filter) {
          qualified = true;
          break;
        } else if (pFilterElem->fp == isNull_filter) {
          continue;
        }
      }

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
    int32_t functionId = pQuery->pExpr1[j].base.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (hasMainFunction &&
        (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ)) {
      continue;
    }

    SResultRowCellInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);
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
    SResultRowCellInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

    int16_t functionId = pRuntimeEnv->pCtx[j].functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ ||
        functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    assert(pResInfo->numOfRes > numOfRes);
    pResInfo->numOfRes = numOfRes;
  }
}

static UNUSED_FUNC int32_t getMergeResultGroupId(int32_t groupIndex) {
  int32_t base = 50000000;
  return base + (groupIndex * 10000);
}

bool isGroupbyNormalCol(SSqlGroupbyExpr *pGroupbyExpr) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndex *pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, i);
    if (TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
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
    if (TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
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
    int32_t functId = pQuery->pExpr1[i].base.functionId;
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
    int32_t functId = pQuery->pExpr1[i].base.functionId;
    if (functId != TSDB_FUNC_PRJ && functId != TSDB_FUNC_TAGPRJ) {
      return false;
    }
  }

  return true;
}

bool isTSCompQuery(SQuery *pQuery) { return pQuery->pExpr1[0].base.functionId == TSDB_FUNC_TS_COMP; }

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
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

static bool timeWindowInterpoRequired(SQuery *pQuery) {
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_TWA) {
      return true;
    }
  }

  return false;
}

static bool hasTagValOutput(SQuery* pQuery) {
  SExprInfo *pExprInfo = &pQuery->pExpr1[0];
  if (pQuery->numOfOutput == 1 && pExprInfo->base.functionId == TSDB_FUNC_TS_COMP) {
    return true;
  } else {  // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutput; ++idx) {
      SExprInfo *pLocalExprInfo = &pQuery->pExpr1[idx];

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
  if (pStatis != NULL && TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
    *pColStatis = &pStatis[pColIndex->colIndex];
    assert((*pColStatis)->colId == pColIndex->colId);
  } else {
    *pColStatis = NULL;
  }

  if (TSDB_COL_IS_TAG(pColIndex->flag) || TSDB_COL_IS_UD_COL(pColIndex->flag) || pColIndex->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return false;
  }

  if ((*pColStatis) != NULL && (*pColStatis)->numOfNull == 0) {
    return false;
  }

  return true;
}

static SResultRow *doPrepareResultRowFromKey(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, char *pData,
                                             int16_t bytes, bool masterscan, uint64_t uid) {
  bool existed = false;
  SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, uid);

  SResultRow **p1 =
      (SResultRow **)taosHashGet(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  // in case of repeat scan/reverse scan, no new time window added.
  if (QUERY_IS_INTERVAL_QUERY(pRuntimeEnv->pQuery)) {
    if (!masterscan) {  // the *p1 may be NULL in case of sliding+offset exists.
      return (p1 != NULL)? *p1:NULL;
    }

    if (p1 != NULL) {
      for(int32_t i = pResultRowInfo->size - 1; i >= 0; --i) {
        if (pResultRowInfo->pResult[i] == (*p1)) {
          pResultRowInfo->curIndex = i;
          existed = true;
          break;
        }
      }
    }
  } else {
    if (p1 != NULL) {  // group by column query
      return *p1;
    }
  }

  if (!existed) {
    // TODO refactor
    // more than the capacity, reallocate the resources
    if (pResultRowInfo->size >= pResultRowInfo->capacity) {
      int64_t newCapacity = 0;
      if (pResultRowInfo->capacity > 10000) {
        newCapacity = (int64_t)(pResultRowInfo->capacity * 1.25);
      } else {
        newCapacity = (int64_t)(pResultRowInfo->capacity * 1.5);
      }

      char *t = realloc(pResultRowInfo->pResult, (size_t)(newCapacity * POINTER_BYTES));
      if (t == NULL) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      pResultRowInfo->pResult = (SResultRow **)t;

      int32_t inc = (int32_t)newCapacity - pResultRowInfo->capacity;
      memset(&pResultRowInfo->pResult[pResultRowInfo->capacity], 0, POINTER_BYTES * inc);

      pResultRowInfo->capacity = (int32_t)newCapacity;
    }

    SResultRow *pResult = NULL;

    if (p1 == NULL) {
      pResult = getNewResultRow(pRuntimeEnv->pool);
      int32_t ret = initResultRow(pResult);
      if (ret != TSDB_CODE_SUCCESS) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      // add a new result set for a new group
      taosHashPut(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pResult, POINTER_BYTES);
    } else {
      pResult = *p1;
    }

    pResultRowInfo->pResult[pResultRowInfo->size] = pResult;
    pResultRowInfo->curIndex = pResultRowInfo->size++;
  }

  // too many time window in query
  if (pResultRowInfo->size > MAX_INTERVAL_TIME_WINDOW) {
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  return getResultRow(pResultRowInfo, pResultRowInfo->curIndex);
}

// get the correct time window according to the handled timestamp
static STimeWindow getActiveTimeWindow(SResultRowInfo *pWindowResInfo, int64_t ts, SQuery *pQuery) {
  STimeWindow w = {0};

 if (pWindowResInfo->curIndex == -1) {  // the first window, from the previous stored value
    w.skey = pWindowResInfo->prevSKey;
    if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
      w.ekey = taosTimeAdd(w.skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision);
    } else {
      w.ekey = w.skey + pQuery->interval.interval - 1;
    }
  } else {
    int32_t slot = curTimeWindowIndex(pWindowResInfo);
    SResultRow* pWindowRes = getResultRow(pWindowResInfo, slot);
    w = pWindowRes->win;
  }

  if (w.skey > ts || w.ekey < ts) {
    if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
      w.skey = taosTimeTruncate(ts, &pQuery->interval, pQuery->precision);
      w.ekey = taosTimeAdd(w.skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision) - 1;
    } else {
      int64_t st = w.skey;

      if (st > ts) {
        st -= ((st - ts + pQuery->interval.sliding - 1) / pQuery->interval.sliding) * pQuery->interval.sliding;
      }

      int64_t et = st + pQuery->interval.interval - 1;
      if (et < ts) {
        st += ((ts - et + pQuery->interval.sliding - 1) / pQuery->interval.sliding) * pQuery->interval.sliding;
      }

      w.skey = st;
      w.ekey = w.skey + pQuery->interval.interval - 1;
    }
  }

  /*
   * query border check, skey should not be bounded by the query time range, since the value skey will
   * be used as the time window index value. So we only change ekey of time window accordingly.
   */
  if (w.ekey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) {
    w.ekey = pQuery->window.ekey;
  }

  return w;
}

static int32_t addNewWindowResultBuf(SResultRow *pWindowRes, SDiskbasedResultBuf *pResultBuf, int32_t tid,
                                     int32_t numOfRowsPerPage) {
  if (pWindowRes->pageId != -1) {
    return 0;
  }

  tFilePage *pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SIDList list = getDataBufPagesIdList(pResultBuf, tid);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewDataBuf(pResultBuf, tid, &pageId);
  } else {
    SPageInfo* pi = getLastPageInfo(list);
    pData = getResBufPage(pResultBuf, pi->pageId);
    pageId = pi->pageId;

    if (pData->num >= numOfRowsPerPage) {
      // release current page first, and prepare the next one
      releaseResBufPageInfo(pResultBuf, pi);
      pData = getNewDataBuf(pResultBuf, tid, &pageId);
      if (pData != NULL) {
        assert(pData->num == 0);  // number of elements must be 0 for new allocated buffer
      }
    }
  }

  if (pData == NULL) {
    return -1;
  }

  // set the number of rows in current disk page
  if (pWindowRes->pageId == -1) {  // not allocated yet, allocate new buffer
    pWindowRes->pageId = pageId;
    pWindowRes->rowId = (int32_t)(pData->num++);

    assert(pWindowRes->pageId >= 0);
  }

  return 0;
}

static int32_t setWindowOutputBufByKey(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, STimeWindow *win,
    bool masterscan, SResultRow** pResult, int64_t groupId) {
  assert(win->skey <= win->ekey);
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SResultRow *pResultRow = doPrepareResultRowFromKey(pRuntimeEnv, pResultRowInfo, (char *)&win->skey, TSDB_KEYSIZE, masterscan, groupId);
  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // not assign result buffer yet, add new result buffer
  if (pResultRow->pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, (int32_t) groupId, pRuntimeEnv->numOfRowsPerPage);
    if (ret != TSDB_CODE_SUCCESS) {
      return -1;
    }
  }

  // set time window for current result
  pResultRow->win = (*win);
  *pResult = pResultRow;
  setResultRowOutputBufInitCtx(pRuntimeEnv, pResultRow);

  return TSDB_CODE_SUCCESS;
}

static bool getResultRowStatus(SResultRowInfo *pWindowResInfo, int32_t slot) {
  assert(slot >= 0 && slot < pWindowResInfo->size);
  return pWindowResInfo->pResult[slot]->closed;
}

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP   = 2,
} SResultTsInterpType;

static void setResultRowInterpo(SResultRow* pResult, SResultTsInterpType type) {
  assert(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
  if (type == RESULT_ROW_START_INTERP) {
    pResult->startInterp = true;
  } else {
    pResult->endInterp   = true;
  }
}

static bool resultRowInterpolated(SResultRow* pResult, SResultTsInterpType type) {
  assert(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
  if (type == RESULT_ROW_START_INTERP) {
    return pResult->startInterp == true;
  } else {
    return pResult->endInterp   == true;
  }
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

static void doUpdateResultRowIndex(SResultRowInfo*pResultRowInfo, TSKEY lastKey, bool ascQuery) {
  int64_t skey = TSKEY_INITIAL_VAL;
  int32_t i = 0;
  for (i = pResultRowInfo->size - 1; i >= 0; --i) {
    SResultRow *pResult = pResultRowInfo->pResult[i];
    if (pResult->closed) {
      break;
    }

    // new closed result rows
    if ((pResult->win.ekey <= lastKey && ascQuery) || (pResult->win.skey >= lastKey && !ascQuery)) {
      closeResultRow(pResultRowInfo, i);
    } else {
      skey = pResult->win.skey;
    }
  }

  // all result rows are closed, set the last one to be the skey
  if (skey == TSKEY_INITIAL_VAL) {
    pResultRowInfo->curIndex = pResultRowInfo->size - 1;
  } else {

    for (i = pResultRowInfo->size - 1; i >= 0; --i) {
      SResultRow *pResult = pResultRowInfo->pResult[i];
      if (pResult->closed) {
        break;
      }
    }

    pResultRowInfo->curIndex = i + 1;  // current not closed result object
    pResultRowInfo->prevSKey = pResultRowInfo->pResult[pResultRowInfo->curIndex]->win.skey;
  }
}

static void updateResultRowIndex(SResultRowInfo* pResultRowInfo, STableQueryInfo* pTableQueryInfo, bool ascQuery) {
  if ((pTableQueryInfo->lastKey > pTableQueryInfo->win.ekey && ascQuery) || (pTableQueryInfo->lastKey < pTableQueryInfo->win.ekey && (!ascQuery))) {
    closeAllResultRows(pResultRowInfo);
    pResultRowInfo->curIndex = pResultRowInfo->size - 1;
  } else {
    int32_t step = ascQuery? 1:-1;
    doUpdateResultRowIndex(pResultRowInfo, pTableQueryInfo->lastKey - step, ascQuery);
  }
}

static int32_t getNumOfRowsInTimeWindow(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo, TSKEY *pPrimaryColumn,
                                        int32_t startPos, TSKEY ekey, __block_search_fn_t searchFn, bool updateLastKey) {
  assert(startPos >= 0 && startPos < pDataBlockInfo->rows);

  int32_t num   = -1;
  int32_t order = pQuery->order.order;
  int32_t step  = GET_FORWARD_DIRECTION_FACTOR(order);

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

static void doBlockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pWin, int32_t offset, int32_t forwardStep, TSKEY *tsCol, int32_t numOfTotal) {
  SQuery         *pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  bool hasPrev = pCtx[0].preAggVals.isSet;

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    pCtx[k].nStartQueryTimestamp = pWin->skey;
    pCtx[k].size = forwardStep;
    pCtx[k].startOffset = (QUERY_IS_ASC_QUERY(pQuery)) ? offset : offset - (forwardStep - 1);

    int32_t functionId = pQuery->pExpr1[k].base.functionId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      pCtx[k].ptsList = &tsCol[pCtx[k].startOffset];
    }

    // not a whole block involved in query processing, statistics data can not be used
    // NOTE: the original value of isSet have been changed here
    if (pCtx[k].preAggVals.isSet && forwardStep < numOfTotal) {
      pCtx[k].preAggVals.isSet = false;
    }

    if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
      aAggs[functionId].xFunction(&pCtx[k]);
    }

    // restore it
    pCtx[k].preAggVals.isSet = hasPrev;
  }
}

static void doRowwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pWin, int32_t offset) {
  SQuery         *pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    pCtx[k].nStartQueryTimestamp = pWin->skey;

    int32_t functionId = pQuery->pExpr1[k].base.functionId;
    if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
      aAggs[functionId].xFunctionF(&pCtx[k], offset);
    }
  }
}

static int32_t getNextQualifiedWindow(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pNext, SDataBlockInfo *pDataBlockInfo,
    TSKEY *primaryKeys, __block_search_fn_t searchFn, int32_t prevPosition) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  getNextTimeWindow(pQuery, pNext);

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
  if (pQuery->interval.sliding == pQuery->interval.interval && prevPosition != -1) {
    int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
    startPos = prevPosition + factor;
  } else {
    if (startKey <= pDataBlockInfo->window.skey && QUERY_IS_ASC_QUERY(pQuery)) {
      startPos = 0;
    } else if (startKey >= pDataBlockInfo->window.ekey && !QUERY_IS_ASC_QUERY(pQuery)) {
      startPos = pDataBlockInfo->rows - 1;
    } else {
      startPos = searchFn((char *)primaryKeys, pDataBlockInfo->rows, startKey, pQuery->order.order);
    }
  }

  /*
   * This time window does not cover any data, try next time window,
   * this case may happen when the time window is too small
   */
  if (primaryKeys == NULL) {
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      assert(pDataBlockInfo->window.skey <= pNext->ekey);
    } else {
      assert(pDataBlockInfo->window.ekey >= pNext->skey);
    }
  } else {
    if (QUERY_IS_ASC_QUERY(pQuery) && primaryKeys[startPos] > pNext->ekey) {
      TSKEY next = primaryKeys[startPos];
      if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, &pQuery->interval, pQuery->precision);
        pNext->ekey = taosTimeAdd(pNext->skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision) - 1;
      } else {
        pNext->ekey += ((next - pNext->ekey + pQuery->interval.sliding - 1)/pQuery->interval.sliding) * pQuery->interval.sliding;
        pNext->skey = pNext->ekey - pQuery->interval.interval + 1;
      }
    } else if ((!QUERY_IS_ASC_QUERY(pQuery)) && primaryKeys[startPos] < pNext->skey) {
      TSKEY next = primaryKeys[startPos];
      if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
        pNext->skey = taosTimeTruncate(next, &pQuery->interval, pQuery->precision);
        pNext->ekey = taosTimeAdd(pNext->skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision) - 1;
      } else {
        pNext->skey -= ((pNext->skey - next + pQuery->interval.sliding - 1) / pQuery->interval.sliding) * pQuery->interval.sliding;
        pNext->ekey = pNext->skey + pQuery->interval.interval - 1;
      }
    }
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
  int32_t numOfCols = (int32_t)taosArrayGetSize(pDataBlock);

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

  int32_t functionId = pQuery->pExpr1[col].base.functionId;
  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pArithExpr = &pQuery->pExpr1[col];

    sas->offset    = (QUERY_IS_ASC_QUERY(pQuery))? pQuery->pos : pQuery->pos - (size - 1);
    sas->colList   = pQuery->colList;
    sas->numOfCols = pQuery->numOfCols;
    sas->data      = calloc(pQuery->numOfCols, POINTER_BYTES);

    if (sas->data == NULL) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    // here the pQuery->colList and sas->colList are identical
    int32_t numOfCols = (int32_t)taosArrayGetSize(pDataBlock);
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
      sas->data[i] = dataBlock;  // start from the offset
    }

  } else {  // other type of query function
    SColIndex *pCol = &pQuery->pExpr1[col].base.colInfo;
    if (TSDB_COL_IS_NORMAL_COL(pCol->flag)) {
      SColIndex* pColIndex = &pQuery->pExpr1[col].base.colInfo;
      SColumnInfoData *p = taosArrayGet(pDataBlock, pColIndex->colIndex);
      assert(p->info.colId == pColIndex->colId);

      dataBlock = p->pData;
    } else {
      dataBlock = NULL;
    }
  }

  return dataBlock;
}

static void setNotInterpoWindowKey(SQLFunctionCtx* pCtx, int32_t numOfOutput, int32_t type) {
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

// window start key interpolation
static bool setTimeWindowInterpolationStartTs(SQueryRuntimeEnv* pRuntimeEnv, int32_t pos, int32_t numOfRows, SArray* pDataBlock, TSKEY* tsCols, STimeWindow* win) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  TSKEY curTs  = tsCols[pos];
  TSKEY lastTs = *(TSKEY *) pRuntimeEnv->prevRow[0];

  // lastTs == INT64_MIN and pos == 0 means this is the first time window, interpolation is not needed.
  // start exactly from this point, no need to do interpolation
  TSKEY key = QUERY_IS_ASC_QUERY(pQuery)? win->skey:win->ekey;
  if (key == curTs) {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
    return true;
  }

  if (lastTs == INT64_MIN && ((pos == 0 && QUERY_IS_ASC_QUERY(pQuery)) || (pos == (numOfRows - 1) && !QUERY_IS_ASC_QUERY(pQuery)))) {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
    return true;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  TSKEY   prevTs = ((pos == 0 && QUERY_IS_ASC_QUERY(pQuery)) || (pos == (numOfRows - 1) && !QUERY_IS_ASC_QUERY(pQuery)))?
      lastTs:tsCols[pos - step];

  doRowwiseTimeWindowInterpolation(pRuntimeEnv, pDataBlock, prevTs, pos - step, curTs, pos, key, RESULT_ROW_START_INTERP);
  return true;
}

static bool setTimeWindowInterpolationEndTs(SQueryRuntimeEnv* pRuntimeEnv, int32_t endRowIndex, SArray* pDataBlock, TSKEY* tsCols, TSKEY blockEkey, STimeWindow* win) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  TSKEY   actualEndKey = tsCols[endRowIndex];

  TSKEY key = QUERY_IS_ASC_QUERY(pQuery)? win->ekey:win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey && QUERY_IS_ASC_QUERY(pQuery)) || (key < blockEkey && !QUERY_IS_ASC_QUERY(pQuery))) {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_END_INTERP);
    return false;
  }

  // there is actual end point of current time window, no interpolation need
  if (key == actualEndKey) {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_END_INTERP);
    return true;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  int32_t nextRowIndex = endRowIndex + step;
  assert(nextRowIndex >= 0);

  TSKEY nextKey = tsCols[nextRowIndex];
  doRowwiseTimeWindowInterpolation(pRuntimeEnv, pDataBlock, actualEndKey, endRowIndex, nextKey, nextRowIndex, key, RESULT_ROW_END_INTERP);
  return true;
}

static void saveDataBlockLastRow(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pDataBlockInfo, SArray* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t rowIndex = QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->rows-1:0;
  for (int32_t k = 0; k < pQuery->numOfCols; ++k) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, k);
    memcpy(pRuntimeEnv->prevRow[k], ((char*)pColInfo->pData) + (pColInfo->info.bytes * rowIndex), pColInfo->info.bytes);
  }
}

static TSKEY getStartTsKey(SQuery* pQuery, SDataBlockInfo* pDataBlockInfo, TSKEY* tsCols, int32_t step) {
  TSKEY ts = TSKEY_INITIAL_VAL;

  if (tsCols == NULL) {
    ts = QUERY_IS_ASC_QUERY(pQuery) ? pDataBlockInfo->window.skey : pDataBlockInfo->window.ekey;
  } else {
    int32_t offset = GET_COL_DATA_POS(pQuery, 0, step);
    ts = tsCols[offset];
  }

  return ts;
}

static void doWindowBorderInterpolation(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pDataBlockInfo, SArray *pDataBlock,
    SResultRow* pResult, STimeWindow* win, int32_t startPos, int32_t forwardStep) {
  if (!pRuntimeEnv->timeWindowInterpo) {
    return;
  }

  assert(pDataBlock != NULL);

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, 0);

  TSKEY  *tsCols = (TSKEY *)(pColInfo->pData);
  bool done = resultRowInterpolated(pResult, RESULT_ROW_START_INTERP);
  if (!done) {
    int32_t startRowIndex = startPos;
    bool    interp = setTimeWindowInterpolationStartTs(pRuntimeEnv, startRowIndex, pDataBlockInfo->rows, pDataBlock, tsCols, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
  }

  done = resultRowInterpolated(pResult, RESULT_ROW_END_INTERP);
  if (!done) {
    int32_t endRowIndex = startPos + (forwardStep - 1) * step;

    TSKEY endKey = QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->window.ekey:pDataBlockInfo->window.skey;
    bool  interp = setTimeWindowInterpolationEndTs(pRuntimeEnv, endRowIndex, pDataBlock, tsCols, endKey, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_END_INTERP);
  }
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
static void blockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis, SDataBlockInfo *pDataBlockInfo,
                                    SResultRowInfo *pWindowResInfo, __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  bool            masterScan = IS_MASTER_SCAN(pRuntimeEnv);

  SQuery *pQuery  = pRuntimeEnv->pQuery;
  int64_t groupId = pQuery->current->groupIndex;

  TSKEY  *tsCols = NULL;
  if (pDataBlock != NULL) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, 0);
    tsCols = (TSKEY *)(pColInfo->pData);
  }

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutput, sizeof(SArithmeticSupport));
  if (sasArray == NULL) {
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SQInfo *pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    char *dataBlock = getDataBlock(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);
    setExecParams(pQuery, &pCtx[k], dataBlock, tsCols, pDataBlockInfo, pStatis, &sasArray[k], k, pQInfo->vgId);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    int32_t prevIndex = curTimeWindowIndex(pWindowResInfo);

    TSKEY ts = getStartTsKey(pQuery, pDataBlockInfo, tsCols, step);
    STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

    SResultRow* pResult = NULL;
    int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId);
    if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
      goto _end;
    }

    int32_t forwardStep = 0;
    int32_t startPos = pQuery->pos;

    TSKEY ekey = reviseWindowEkey(pQuery, &win);
    forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, tsCols, pQuery->pos, ekey, searchFn, true);

    // prev time window not interpolation yet.
    int32_t curIndex = curTimeWindowIndex(pWindowResInfo);
    if (prevIndex != -1 && prevIndex < curIndex && pRuntimeEnv->timeWindowInterpo) {
      for(int32_t j = prevIndex; j < curIndex; ++j) { // previous time window may be all closed already.
        SResultRow *pRes = pWindowResInfo->pResult[j];
        if (pRes->closed) {
          assert(resultRowInterpolated(pRes, RESULT_ROW_START_INTERP) && resultRowInterpolated(pRes, RESULT_ROW_END_INTERP));
          continue;
        }

        STimeWindow w = pRes->win;
        ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &w, masterScan, &pResult, groupId);
        assert(ret == TSDB_CODE_SUCCESS && !resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

        int32_t p = QUERY_IS_ASC_QUERY(pQuery)? 0:pDataBlockInfo->rows-1;
        doRowwiseTimeWindowInterpolation(pRuntimeEnv, pDataBlock, *(TSKEY*) pRuntimeEnv->prevRow[0], -1,  tsCols[0], p, w.ekey, RESULT_ROW_END_INTERP);
        setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
        setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);

        doBlockwiseApplyFunctions(pRuntimeEnv, &w, startPos, 0, tsCols, pDataBlockInfo->rows);
      }

      // restore current time window
      ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId);
      assert (ret == TSDB_CODE_SUCCESS);
    }

    // window start key interpolation
    doWindowBorderInterpolation(pRuntimeEnv, pDataBlockInfo, pDataBlock, pResult, &win, pQuery->pos, forwardStep);
    doBlockwiseApplyFunctions(pRuntimeEnv, &win, startPos, forwardStep, tsCols, pDataBlockInfo->rows);

    STimeWindow nextWin = win;
    while (1) {
      int32_t prevEndPos = (forwardStep - 1) * step + startPos;
      startPos = getNextQualifiedWindow(pRuntimeEnv, &nextWin, pDataBlockInfo, tsCols, searchFn, prevEndPos);
      if (startPos < 0) {
        break;
      }

      // null data, failed to allocate more memory buffer
      int32_t code = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &nextWin, masterScan, &pResult, groupId);
      if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
        break;
      }

      ekey = reviseWindowEkey(pQuery, &nextWin);
      forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, tsCols, startPos, ekey, searchFn, true);

      // window start(end) key interpolation
      doWindowBorderInterpolation(pRuntimeEnv, pDataBlockInfo, pDataBlock, pResult, &nextWin, startPos, forwardStep);
      doBlockwiseApplyFunctions(pRuntimeEnv, &nextWin, startPos, forwardStep, tsCols, pDataBlockInfo->rows);
    }

  } else {
    /*
     * the sqlfunctionCtx parameters should be set done before all functions are invoked,
     * since the selectivity + tag_prj query needs all parameters been set done.
     * tag_prj function are changed to be TSDB_FUNC_TAG_DUMMY
     */
    for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
      int32_t functionId = pQuery->pExpr1[k].base.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        pCtx[k].nStartQueryTimestamp = pDataBlockInfo->window.skey;
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
  }

  _end:
  if (pRuntimeEnv->timeWindowInterpo) {
    saveDataBlockLastRow(pRuntimeEnv, pDataBlockInfo, pDataBlock);
  }

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    if (pQuery->pExpr1[i].base.functionId != TSDB_FUNC_ARITHM) {
      continue;
    }

    tfree(sasArray[i].data);
  }

  tfree(sasArray);
}

static int32_t setGroupResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, char *pData, int16_t type, int16_t bytes, int32_t groupIndex) {
  if (isNull(pData, type)) {  // ignore the null value
    return -1;
  }

  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  // not assign result buffer yet, add new result buffer, TODO remove it
  char* d = pData;
  int16_t len = bytes;
  if (type == TSDB_DATA_TYPE_BINARY||type == TSDB_DATA_TYPE_NCHAR) {
    d = varDataVal(pData);
    len = varDataLen(pData);
  } else if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
    SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
    qError("QInfo:%p group by not supported on double/float columns, abort", pQInfo);

    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
  }

  SResultRow *pResultRow = doPrepareResultRowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, d, len, true, groupIndex);
  assert (pResultRow != NULL);

  int64_t v = -1;
  GET_TYPED_DATA(v, int64_t, type, pData);
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    if (pResultRow->key == NULL) {
      pResultRow->key = malloc(varDataTLen(pData));
      varDataCopy(pResultRow->key, pData);
    } else {
      assert(memcmp(pResultRow->key, pData, varDataTLen(pData)) == 0);
    }
  } else {
    pResultRow->win.skey = v;
    pResultRow->win.ekey = v;
  }

  if (pResultRow->pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, groupIndex, pRuntimeEnv->numOfRowsPerPage);
    if (ret != 0) {
      return -1;
    }
  }

  setResultOutputBuf(pRuntimeEnv, pResultRow);
  initCtxOutputBuf(pRuntimeEnv);
  return TSDB_CODE_SUCCESS;
}

static char *getGroupbyColumnData(SQuery *pQuery, int16_t *type, int16_t *bytes, SArray* pDataBlock) {
  SSqlGroupbyExpr *pGroupbyExpr = pQuery->pGroupbyExpr;

  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    SColIndex* pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, k);
    if (TSDB_COL_IS_TAG(pColIndex->flag)) {
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
    int32_t numOfCols = (int32_t)taosArrayGetSize(pDataBlock);

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

  STSElem         elem = tsBufGetElem(pRuntimeEnv->pTsBuf);
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  // compare tag first
  if (tVariantCompare(&pCtx[0].tag, elem.tag) != 0) {
    return TS_JOIN_TAG_NOT_EQUALS;
  }

  TSKEY key = *(TSKEY *)((char*)pCtx[0].aInputElemBuf + TSDB_KEYSIZE * offset);

#if defined(_DEBUG_VIEW)
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64 ", tag:%"PRIu64", query order:%d, ts order:%d, traverse:%d, index:%d\n",
         elem.ts, key, elem.tag.i64Key, pQuery->order.order, pRuntimeEnv->pTsBuf->tsOrder,
         pRuntimeEnv->pTsBuf->cur.order, pRuntimeEnv->pTsBuf->cur.tsIndex);
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
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
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

  // denote the order type
  if ((functionId == TSDB_FUNC_LAST_DST || functionId == TSDB_FUNC_LAST)) {
    return pCtx->param[0].i64Key == pQuery->order.order;
  }

  // in the supplementary scan, only the following functions need to be executed
  if (IS_REVERSE_SCAN(pRuntimeEnv)) {
    return false;
  }

  return true;
}

void doRowwiseTimeWindowInterpolation(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey,  int32_t type) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    int32_t functionId = pQuery->pExpr1[k].base.functionId;
    if (functionId != TSDB_FUNC_TWA) {
      pRuntimeEnv->pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SColIndex* pColIndex = &pQuery->pExpr1[k].base.colInfo;
    int16_t index = pColIndex->colIndex;
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, index);

    assert(pColInfo->info.colId == pColIndex->colId && curTs != windowKey);
    double v1 = 0, v2 = 0, v = 0;

    if (prevRowIndex == -1) {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char *)pRuntimeEnv->prevRow[index]);
    } else {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char *)pColInfo->pData + prevRowIndex * pColInfo->info.bytes);
    }

    GET_TYPED_DATA(v2, double, pColInfo->info.type, (char *)pColInfo->pData + curRowIndex * pColInfo->info.bytes);

    SPoint point1 = (SPoint){.key = prevTs, .val = &v1};
    SPoint point2 = (SPoint){.key = curTs, .val = &v2};
    SPoint point  = (SPoint){.key = windowKey, .val = &v};
    taosGetLinearInterpolationVal(TSDB_DATA_TYPE_DOUBLE, &point1, &point2, &point);

    if (type == RESULT_ROW_START_INTERP) {
      pRuntimeEnv->pCtx[k].start.key = point.key;
      pRuntimeEnv->pCtx[k].start.val = v;
    } else {
      pRuntimeEnv->pCtx[k].end.key = point.key;
      pRuntimeEnv->pCtx[k].end.val = v;
    }
  }
}

static void setTimeWindowSKeyInterp(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY ts, int32_t offset, SResultRow* pResult, STimeWindow* win) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  bool done = resultRowInterpolated(pResult, RESULT_ROW_START_INTERP);
  if (!done) {
    TSKEY key = QUERY_IS_ASC_QUERY(pQuery)? win->skey:win->ekey;
    if (key == ts) {
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    } else if (prevTs != INT64_MIN && ((QUERY_IS_ASC_QUERY(pQuery) && prevTs < key) || (!QUERY_IS_ASC_QUERY(pQuery) && prevTs > key))) {
      doRowwiseTimeWindowInterpolation(pRuntimeEnv, pDataBlock, prevTs, prevRowIndex, ts, offset, key, RESULT_ROW_START_INTERP);
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    } else {
      setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
    }

    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_END_INTERP);
    for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
      pRuntimeEnv->pCtx[k].size = 1;
    }
  } else {
    setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
  }
}

static void setTimeWindowEKeyInterp(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY ts, int32_t offset, SResultRow* pResult, STimeWindow* win) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  TSKEY key = QUERY_IS_ASC_QUERY(pQuery)? win->ekey:win->skey;
  doRowwiseTimeWindowInterpolation(pRuntimeEnv, pDataBlock, prevTs, prevRowIndex, ts, offset, key, RESULT_ROW_END_INTERP);
  setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);

  setNotInterpoWindowKey(pRuntimeEnv->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    pRuntimeEnv->pCtx[i].size = 0;
  }
}

static void rowwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SDataStatis *pStatis, SDataBlockInfo *pDataBlockInfo,
    SResultRowInfo *pWindowResInfo, SArray *pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  bool masterScan = IS_MASTER_SCAN(pRuntimeEnv);

  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* item = pQuery->current;

  int64_t groupId = item->groupIndex;

  SColumnInfoData* pColumnInfoData = (SColumnInfoData *)taosArrayGet(pDataBlock, 0);

  TSKEY  *tsCols = (pColumnInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP)? (TSKEY*) pColumnInfoData->pData:NULL;
  bool    groupbyColumnValue = pRuntimeEnv->groupbyNormalCol;

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutput, sizeof(SArithmeticSupport));
  if (sasArray == NULL) {
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  int16_t type = 0;
  int16_t bytes = 0;

  char *groupbyColumnData = NULL;
  if (groupbyColumnValue) {
    groupbyColumnData = getGroupbyColumnData(pQuery, &type, &bytes, pDataBlock);
  }

  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    char *dataBlock = getDataBlock(pRuntimeEnv, &sasArray[k], k, pDataBlockInfo->rows, pDataBlock);
    setExecParams(pQuery, &pCtx[k], dataBlock, tsCols, pDataBlockInfo, pStatis, &sasArray[k], k, pQInfo->vgId);
    pCtx[k].size = 1;
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
  if (pRuntimeEnv->pTsBuf != NULL) {
    qDebug("QInfo:%p process data rows, numOfRows:%d, query order:%d, ts comp order:%d", pQInfo, pDataBlockInfo->rows,
           pQuery->order.order, pRuntimeEnv->pTsBuf->cur.order);
  }

  int32_t offset = -1;
  TSKEY   prevTs = *(TSKEY*) pRuntimeEnv->prevRow[0];
  int32_t prevRowIndex = -1;

  for (int32_t j = 0; j < pDataBlockInfo->rows; ++j) {
    offset = GET_COL_DATA_POS(pQuery, j, step);

    if (pRuntimeEnv->pTsBuf != NULL) {
      int32_t ret = doTSJoinFilter(pRuntimeEnv, offset);
      if (ret == TS_JOIN_TAG_NOT_EQUALS) {
        break;
      } else if (ret == TS_JOIN_TS_NOT_EQUALS) {
        continue;
      } else {
        assert(ret == TS_JOIN_TS_EQUAL);
      }
    }

    if (pQuery->numOfFilterCols > 0 && (!doFilterData(pQuery, offset))) {
      continue;
    }

    // interval window query, decide the time window according to the primary timestamp
    if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
      int32_t prevWindowIndex = curTimeWindowIndex(pWindowResInfo);
      int64_t ts  = tsCols[offset];

      STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

      SResultRow* pResult = NULL;
      int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId);
      if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {  // null data, too many state code
        goto _end;
      }

      // window start key interpolation
      if (pRuntimeEnv->timeWindowInterpo) {
        // check for the time window end time interpolation
        int32_t curIndex = curTimeWindowIndex(pWindowResInfo);
        if (prevWindowIndex != -1 && prevWindowIndex < curIndex) {
          for (int32_t k = prevWindowIndex; k < curIndex; ++k) {
            SResultRow *pRes = pWindowResInfo->pResult[k];
            if (pRes->closed) {
              assert(resultRowInterpolated(pResult, RESULT_ROW_START_INTERP) && resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));
              continue;
            }

            ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &pRes->win, masterScan, &pResult, groupId);
            assert(ret == TSDB_CODE_SUCCESS && !resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

            setTimeWindowEKeyInterp(pRuntimeEnv, pDataBlock, prevTs, prevRowIndex, ts, offset, pResult, &pRes->win);
            doRowwiseApplyFunctions(pRuntimeEnv, &pRes->win, offset);
          }

          // restore current time window
          ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId);
          if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
            continue;
          }
        }

        setTimeWindowSKeyInterp(pRuntimeEnv, pDataBlock, prevTs, prevRowIndex, ts, offset, pResult, &win);
      }

      doRowwiseApplyFunctions(pRuntimeEnv, &win, offset);

      STimeWindow nextWin = win;
      int32_t     index = pWindowResInfo->curIndex;

      while (1) {
        getNextTimeWindow(pQuery, &nextWin);
        if ((nextWin.skey > pQuery->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (nextWin.ekey < pQuery->window.ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        if (ts < nextWin.skey || ts > nextWin.ekey) {
          break;
        }

        // null data, failed to allocate more memory buffer
        int32_t code = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &nextWin, masterScan, &pResult, groupId);
        if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
          break;
        }

        setTimeWindowSKeyInterp(pRuntimeEnv, pDataBlock, prevTs, prevRowIndex, ts, offset, pResult, &nextWin);
        doRowwiseApplyFunctions(pRuntimeEnv, &nextWin, offset);
      }

      pWindowResInfo->curIndex = index;
    } else {  // other queries
      // decide which group this rows belongs to according to current state value
      if (groupbyColumnValue) {
        char *val = groupbyColumnData + bytes * offset;

        int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, val, type, bytes, item->groupIndex);
        if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
          continue;
        }
      }

      for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
        int32_t functionId = pQuery->pExpr1[k].base.functionId;
        if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
          aAggs[functionId].xFunctionF(&pCtx[k], offset);
        }
      }
    }

    prevTs = tsCols[offset];
    prevRowIndex = offset;

    if (pRuntimeEnv->pTsBuf != NULL) {
      // if timestamp filter list is empty, quit current query
      if (!tsBufNextPos(pRuntimeEnv->pTsBuf)) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
        break;
      }
    }
  }

  _end:
  assert(offset >= 0);
  if (tsCols != NULL) {
    item->lastKey = tsCols[offset] + step;
  } else {
    item->lastKey = (QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->window.ekey:pDataBlockInfo->window.skey) + step;
  }

  if (pRuntimeEnv->pTsBuf != NULL) {
    item->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
  }

  // todo refactor: extract method
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    if (pQuery->pExpr1[i].base.functionId != TSDB_FUNC_ARITHM) {
      continue;
    }

    tfree(sasArray[i].data);
  }

  free(sasArray);
}

static int32_t tableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo,
                                          SDataStatis *pStatis, __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STableQueryInfo* pTableQueryInfo = pQuery->current;
  SResultRowInfo*  pResultRowInfo = &pRuntimeEnv->windowResInfo;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pRuntimeEnv->groupbyNormalCol) {
    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, pDataBlock);
  } else {
    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, searchFn, pDataBlock);
  }

  // update the lastkey of current table for projection/aggregation query
  TSKEY lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pDataBlockInfo->window.ekey : pDataBlockInfo->window.skey;
  pTableQueryInfo->lastKey = lastKey + GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // interval query with limit applied
  int32_t numOfRes = 0;
  if (QUERY_IS_INTERVAL_QUERY(pQuery) || pRuntimeEnv->groupbyNormalCol) {
    numOfRes = pResultRowInfo->size;
    updateResultRowIndex(pResultRowInfo, pTableQueryInfo, QUERY_IS_ASC_QUERY(pQuery));
  } else { // projection query
    numOfRes = (int32_t) getNumOfResult(pRuntimeEnv);

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

      if (((pTableQueryInfo->lastKey > pTableQueryInfo->win.ekey) && QUERY_IS_ASC_QUERY(pQuery)) ||
          ((pTableQueryInfo->lastKey < pTableQueryInfo->win.ekey) && (!QUERY_IS_ASC_QUERY(pQuery)))) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }
    }
  }

  return numOfRes;
}

void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void* inputData, TSKEY *tsCol, SDataBlockInfo* pBlockInfo,
                   SDataStatis *pStatis, void *param, int32_t colIndex, int32_t vgId) {

  int32_t functionId = pQuery->pExpr1[colIndex].base.functionId;
  int32_t colId = pQuery->pExpr1[colIndex].base.colInfo.colId;

  SDataStatis *tpField = NULL;
  pCtx->hasNull = hasNullValue(&pQuery->pExpr1[colIndex].base.colInfo, pStatis, &tpField);
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
  pCtx->size = QUERY_IS_ASC_QUERY(pQuery) ? pBlockInfo->rows - pQuery->pos : pQuery->pos + 1;

  // minimum value no matter ascending/descending order query
  pCtx->startOffset = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->pos: (pQuery->pos - pCtx->size + 1);
  assert(pCtx->startOffset >= 0);

  uint32_t status = aAggs[functionId].nStatus;
  if (((status & (TSDB_FUNCSTATE_SELECTIVITY | TSDB_FUNCSTATE_NEED_TS)) != 0) && (tsCol != NULL)) {
    pCtx->ptsList = &tsCol[pCtx->startOffset];
  }

  if (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) {
    // last_dist or first_dist function
    // store the first&last timestamp into the intermediate buffer [1], the true
    // value may be null but timestamp will never be null
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_TWA ||
             functionId == TSDB_FUNC_DIFF || (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE)) {
    /*
     * least squares function needs two columns of input, currently, the x value of linear equation is set to
     * timestamp column, and the y-value is the column specified in pQuery->pExpr1[i].colIdxInBuffer
     *
     * top/bottom function needs timestamp to indicate when the
     * top/bottom values emerge, so does diff function
     */
    if (functionId == TSDB_FUNC_TWA) {
       pCtx->param[1].i64Key = pQuery->window.skey;
       pCtx->param[1].nType = TSDB_DATA_TYPE_BIGINT;
       pCtx->param[2].i64Key = pQuery->window.ekey;
       pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
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
    SResultRowCellInfo* pInfo = GET_RES_INFO(pCtx);

    SInterpInfoDetail *pInterpInfo = (SInterpInfoDetail *)GET_ROWCELL_INTERBUF(pInfo);
    pInterpInfo->type = (int8_t)pQuery->fillType;
    pInterpInfo->ts = pQuery->window.skey;
    pInterpInfo->primaryCol = (colId == PRIMARYKEY_TIMESTAMP_COL_INDEX);

    if (pQuery->fillVal != NULL) {
      if (isNull((const char*) &pQuery->fillVal[colIndex], pCtx->inputType)) {
        pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
      } else { // todo refactor, tVariantCreateFromBinary should handle the NULL value
        if (pCtx->inputType != TSDB_DATA_TYPE_BINARY && pCtx->inputType != TSDB_DATA_TYPE_NCHAR) {
          tVariantCreateFromBinary(&pCtx->param[1], (char*) &pQuery->fillVal[colIndex], pCtx->inputBytes, pCtx->inputType);
        }
      }
    }
  } else if (functionId == TSDB_FUNC_TS_COMP) {
    pCtx->param[0].i64Key = vgId;
    pCtx->param[0].nType = TSDB_DATA_TYPE_BIGINT;
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
static int32_t setCtxTagColumnInfo(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (isSelectivityWithTagsQuery(pQuery)) {
    int32_t num = 0;
    int16_t tagLen = 0;

    SQLFunctionCtx *p = NULL;
    SQLFunctionCtx **pTagCtx = calloc(pQuery->numOfOutput, POINTER_BYTES);
    if (pTagCtx == NULL) {
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }

    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SSqlFuncMsg *pSqlFuncMsg = &pQuery->pExpr1[i].base;

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

  return TSDB_CODE_SUCCESS;
}

static int32_t setupQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv, int16_t order) {
  qDebug("QInfo:%p setup runtime env", GET_QINFO_ADDR(pRuntimeEnv));
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pRuntimeEnv->pCtx = (SQLFunctionCtx *)calloc(pQuery->numOfOutput, sizeof(SQLFunctionCtx));
  pRuntimeEnv->offset = calloc(pQuery->numOfOutput, sizeof(int16_t));
  pRuntimeEnv->rowCellInfoOffset = calloc(pQuery->numOfOutput, sizeof(int32_t));

  if (pRuntimeEnv->offset == NULL || pRuntimeEnv->pCtx == NULL || pRuntimeEnv->rowCellInfoOffset == NULL) {
    goto _clean;
  }

  pRuntimeEnv->offset[0] = 0;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SSqlFuncMsg *pSqlFuncMsg = &pQuery->pExpr1[i].base;

    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    SColIndex* pIndex = &pSqlFuncMsg->colInfo;

    if (TSDB_COL_REQ_NULL(pIndex->flag)) {
      pCtx->requireNull = true;
      pIndex->flag &= ~(TSDB_COL_NULL);
    } else {
      pCtx->requireNull = false;
    }

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
    } else if (TSDB_COL_IS_UD_COL(pIndex->flag)) {
      pCtx->inputBytes = pSqlFuncMsg->arg[0].argBytes;
      pCtx->inputType = pSqlFuncMsg->arg[0].argType;
    } else {
      pCtx->inputBytes = pQuery->colList[index].bytes;
      pCtx->inputType = pQuery->colList[index].type;
    }

    assert(isValidDataType(pCtx->inputType));
    pCtx->ptsOutputBuf = NULL;

    pCtx->outputBytes  = pQuery->pExpr1[i].bytes;
    pCtx->outputType   = pQuery->pExpr1[i].type;

    pCtx->order        = pQuery->order.order;
    pCtx->functionId   = pSqlFuncMsg->functionId;
    pCtx->stableQuery  = pRuntimeEnv->stableQuery;
    pCtx->interBufBytes = pQuery->pExpr1[i].interBytes;
    pCtx->start.key    = INT64_MIN;
    pCtx->end.key      = INT64_MIN;

    pCtx->numOfParams  = pSqlFuncMsg->numOfParams;
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
      int16_t type = pSqlFuncMsg->arg[j].argType;
      int16_t bytes = pSqlFuncMsg->arg[j].argBytes;
      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
        tVariantCreateFromBinary(&pCtx->param[j], pSqlFuncMsg->arg[j].argValue.pz, bytes, type);
      } else {
        tVariantCreateFromBinary(&pCtx->param[j], (char *)&pSqlFuncMsg->arg[j].argValue.i64, bytes, type);
      }
    }

    // set the order information for top/bottom query
    int32_t functionId = pCtx->functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      int32_t f = pQuery->pExpr1[0].base.functionId;
      assert(f == TSDB_FUNC_TS || f == TSDB_FUNC_TS_DUMMY);

      pCtx->param[2].i64Key = order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i64Key = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i64Key = pQuery->order.orderColId;
    }

    if (i > 0) {
      pRuntimeEnv->offset[i] = pRuntimeEnv->offset[i - 1] + pRuntimeEnv->pCtx[i - 1].outputBytes;
      pRuntimeEnv->rowCellInfoOffset[i] = pRuntimeEnv->rowCellInfoOffset[i - 1] + sizeof(SResultRowCellInfo) + pQuery->pExpr1[i - 1].interBytes;
    }

  }

  *(int64_t*) pRuntimeEnv->prevRow[0] = INT64_MIN;

  // if it is group by normal column, do not set output buffer, the output buffer is pResult
  // fixed output query/multi-output query for normal table
  if (!pRuntimeEnv->groupbyNormalCol && !pRuntimeEnv->stableQuery && !QUERY_IS_INTERVAL_QUERY(pRuntimeEnv->pQuery)) {
    resetDefaultResInfoOutputBuf(pRuntimeEnv);
  }

  if (setCtxTagColumnInfo(pRuntimeEnv, pRuntimeEnv->pCtx) != TSDB_CODE_SUCCESS) {
    goto _clean;
  }

  qDebug("QInfo:%p init runtime completed", GET_QINFO_ADDR(pRuntimeEnv));
  return TSDB_CODE_SUCCESS;

_clean:
  tfree(pRuntimeEnv->pCtx);
  tfree(pRuntimeEnv->offset);
  tfree(pRuntimeEnv->rowCellInfoOffset);

  return TSDB_CODE_QRY_OUT_OF_MEMORY;
}

static void doFreeQueryHandle(SQInfo* pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;

  tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
  tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);

  pRuntimeEnv->pQueryHandle = NULL;
  pRuntimeEnv->pSecQueryHandle = NULL;

  SMemRef* pMemRef = &pQInfo->memRef;
  assert(pMemRef->ref == 0 && pMemRef->imem == NULL && pMemRef->mem == NULL);
}

static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  if (pRuntimeEnv->pQuery == NULL) {
    return;
  }

  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo* pQInfo = (SQInfo*) GET_QINFO_ADDR(pRuntimeEnv);

  qDebug("QInfo:%p teardown runtime env", pQInfo);
  cleanupResultRowInfo(&pRuntimeEnv->windowResInfo);

  if (pRuntimeEnv->pCtx != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

      for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
        tVariantDestroy(&pCtx->param[j]);
      }

      tVariantDestroy(&pCtx->tag);
      tfree(pCtx->tagInfo.pTagCtxList);
    }

    tfree(pRuntimeEnv->pCtx);
  }

  pRuntimeEnv->pFillInfo = taosDestroyFillInfo(pRuntimeEnv->pFillInfo);

  destroyResultBuf(pRuntimeEnv->pResultBuf);
  doFreeQueryHandle(pQInfo);

  pRuntimeEnv->pTsBuf = tsBufDestroy(pRuntimeEnv->pTsBuf);

  tfree(pRuntimeEnv->offset);
  tfree(pRuntimeEnv->keyBuf);
  tfree(pRuntimeEnv->rowCellInfoOffset);
  tfree(pRuntimeEnv->prevRow);

  taosHashCleanup(pRuntimeEnv->pResultRowHashTable);
  pRuntimeEnv->pResultRowHashTable = NULL;

  pRuntimeEnv->pool = destroyResultRowPool(pRuntimeEnv->pool);
}

static bool needBuildResAfterQueryComplete(SQInfo* pQInfo) {
  return pQInfo->rspContext != NULL;
}

#define IS_QUERY_KILLED(_q) ((_q)->code == TSDB_CODE_TSC_QUERY_CANCELLED)

static bool isQueryKilled(SQInfo *pQInfo) {
  if (IS_QUERY_KILLED(pQInfo)) {
    return true;
  }

  // query has been executed more than tsShellActivityTimer, and the retrieve has not arrived
  // abort current query execution.
  if (pQInfo->owner != 0 && ((taosGetTimestampSec() - pQInfo->startExecTs) > getMaximumIdleDurationSec()) &&
      (!needBuildResAfterQueryComplete(pQInfo))) {

    assert(pQInfo->startExecTs != 0);
    qDebug("QInfo:%p retrieve not arrive beyond %d sec, abort current query execution, start:%"PRId64", current:%d", pQInfo, 1,
           pQInfo->startExecTs, taosGetTimestampSec());
    return true;
  }

  return false;
}

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
    SSqlFuncMsg *pExprMsg = &pQuery->pExpr1[i].base;

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
bool isPointInterpoQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_INTERP) {
      return true;
    }
  }

  return false;
}

// TODO REFACTOR:MERGE WITH CLIENT-SIDE FUNCTION
static bool isSumAvgRateQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
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
    int32_t functionID = pQuery->pExpr1[i].base.functionId;
    if (functionID == TSDB_FUNC_LAST_ROW) {
      return true;
    }
  }

  return false;
}

static bool needReverseScan(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if ((functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_FIRST_DST) && !QUERY_IS_ASC_QUERY(pQuery)) {
      return true;
    }

    if (functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_LAST_DST) {
      // the scan order to acquire the last result of the specified column
      int32_t order = (int32_t)pQuery->pExpr1[i].base.arg->argValue.i64;
      if (order != pQuery->order.order) {
        return true;
      }
    }
  }

  return false;
}

/**
 * The following 4 kinds of query are treated as the tags query
 * tagprj, tid_tag query, count(tbname), 'abc' (user defined constant value column) query
 */
static bool onlyQueryTags(SQuery* pQuery) {
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SExprInfo* pExprInfo = &pQuery->pExpr1[i];

    int32_t functionId = pExprInfo->base.functionId;

    if (functionId != TSDB_FUNC_TAGPRJ &&
        functionId != TSDB_FUNC_TID_TAG &&
        (!(functionId == TSDB_FUNC_COUNT && pExprInfo->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX)) &&
        (!(functionId == TSDB_FUNC_PRJ && TSDB_COL_IS_UD_COL(pExprInfo->base.colInfo.flag)))) {
      return false;
    }
  }

  return true;
}

/////////////////////////////////////////////////////////////////////////////////////////////

void getAlignQueryTimeWindow(SQuery *pQuery, int64_t key, int64_t keyFirst, int64_t keyLast, STimeWindow *win) {
  assert(key >= keyFirst && key <= keyLast && pQuery->interval.sliding <= pQuery->interval.interval);
  win->skey = taosTimeTruncate(key, &pQuery->interval, pQuery->precision);

  /*
   * if the realSkey > INT64_MAX - pQuery->interval.interval, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  if (keyFirst > (INT64_MAX - pQuery->interval.interval)) {
    assert(keyLast - keyFirst < pQuery->interval.interval);
    win->ekey = INT64_MAX;
  } else if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
    win->ekey = taosTimeAdd(win->skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision) - 1;
  } else {
    win->ekey = win->skey + pQuery->interval.interval - 1;
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
      SSqlFuncMsg *pExprMsg = &pQuery->pExpr1[i].base;
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
    int32_t functionId = pQuery->pExpr1[i].base.functionId;

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
static void doExchangeTimeWindow(SQInfo* pQInfo, STimeWindow* win) {
  size_t t = taosArrayGetSize(pQInfo->tableGroupInfo.pGroupList);
  for(int32_t i = 0; i < t; ++i) {
    SArray* p1 = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, i);

    size_t len = taosArrayGetSize(p1);
    for(int32_t j = 0; j < len; ++j) {
      STableKeyInfo* pInfo = taosArrayGet(p1, j);

      // update the new lastkey if it is equalled to the value of the old skey
      if (pInfo->lastKey == win->ekey) {
        pInfo->lastKey = win->skey;
      }
    }
  }
}

static void changeExecuteScanOrder(SQInfo *pQInfo, SQueryTableMsg* pQueryMsg, bool stableQuery) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;

  // in case of point-interpolation query, use asc order scan
  char msg[] = "QInfo:%p scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%" PRId64
               "-%" PRId64 ", new qrange:%" PRId64 "-%" PRId64;

  // todo handle the case the the order irrelevant query type mixed up with order critical query type
  // descending order query for last_row query
  if (isFirstLastRowQuery(pQuery)) {
    qDebug("QInfo:%p scan order changed for last_row query, old:%d, new:%d", pQInfo, pQuery->order.order, TSDB_ORDER_ASC);

    pQuery->order.order = TSDB_ORDER_ASC;
    if (pQuery->window.skey > pQuery->window.ekey) {
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    return;
  }

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) && pQuery->order.order == TSDB_ORDER_DESC) {
    pQuery->order.order = TSDB_ORDER_ASC;
    if (pQuery->window.skey > pQuery->window.ekey) {
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    doExchangeTimeWindow(pQInfo, &pQuery->window);
    return;
  }

  if (isPointInterpoQuery(pQuery) && pQuery->interval.interval == 0) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      qDebug(msg, GET_QINFO_ADDR(pQuery), "interp", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
             pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    pQuery->order.order = TSDB_ORDER_ASC;
    return;
  }

  if (pQuery->interval.interval == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        qDebug(msg, GET_QINFO_ADDR(pQuery), "only-first", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        doExchangeTimeWindow(pQInfo, &pQuery->window);
      }

      pQuery->order.order = TSDB_ORDER_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        qDebug(msg, GET_QINFO_ADDR(pQuery), "only-last", pQuery->order.order, TSDB_ORDER_DESC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        doExchangeTimeWindow(pQInfo, &pQuery->window);
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
          doExchangeTimeWindow(pQInfo, &pQuery->window);
        }

        pQuery->order.order = TSDB_ORDER_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          qDebug(msg, GET_QINFO_ADDR(pQuery), "only-last stable", pQuery->order.order, TSDB_ORDER_DESC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
          doExchangeTimeWindow(pQInfo, &pQuery->window);
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
    num = (int32_t)(MAX(s, INITIAL_RESULT_ROWS_VALUE));
  } else {    // for super table query, one page for each subset
    num = 1;  // pQInfo->pSidSet->numOfSubSet;
  }

  assert(num > 0);
  return num;
}

static void getIntermediateBufInfo(SQueryRuntimeEnv* pRuntimeEnv, int32_t* ps, int32_t* rowsize) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t MIN_ROWS_PER_PAGE = 4;

  *rowsize = (int32_t)(pQuery->rowSize * GET_ROW_PARAM_FOR_MULTIOUTPUT(pQuery, pRuntimeEnv->topBotQuery, pRuntimeEnv->stableQuery));
  int32_t overhead = sizeof(tFilePage);

  // one page contains at least two rows
  *ps = DEFAULT_INTERN_BUF_PAGE_SIZE;
  while(((*rowsize) * MIN_ROWS_PER_PAGE) > (*ps) - overhead) {
    *ps = (*ps << 1u);
  }

  pRuntimeEnv->numOfRowsPerPage = ((*ps) - sizeof(tFilePage)) / (*rowsize);
  assert(pRuntimeEnv->numOfRowsPerPage <= MAX_ROWS_PER_RESBUF_PAGE);
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

    // no statistics data, load the true data block
    if (index == -1) {
      return true;
    }

    // not support pre-filter operation on binary/nchar data type
    if (!IS_PREFILTER_TYPE(pFilterInfo->info.type)) {
      return true;
    }

    // all data in current column are NULL, no need to check its boundary value
    if (pDataStatis[index].numOfNull == numOfRows) {

      // if isNULL query exists, load the null data column
      for (int32_t j = 0; j < pFilterInfo->numOfFilters; ++j) {
        SColumnFilterElem *pFilterElem = &pFilterInfo->pFilters[j];
        if (pFilterElem->fp == isNull_filter) {
          return true;
        }
      }

      continue;
    }

    SDataStatis* pDataBlockst = &pDataStatis[index];

    if (pFilterInfo->info.type == TSDB_DATA_TYPE_FLOAT) {
      float minval = (float)(*(double *)(&pDataBlockst->min));
      float maxval = (float)(*(double *)(&pDataBlockst->max));

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
      int32_t functionId = pQuery->pExpr1[i].base.functionId;
      if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        return topbot_datablock_filter(&pCtx[i], functionId, (char *)&pDataStatis[i].min, (char *)&pDataStatis[i].max);
      }
    }
  }

  return false;
}

static bool overlapWithTimeWindow(SQuery* pQuery, SDataBlockInfo* pBlockInfo) {
  STimeWindow w = {0};

  TSKEY sk = MIN(pQuery->window.skey, pQuery->window.ekey);
  TSKEY ek = MAX(pQuery->window.skey, pQuery->window.ekey);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    getAlignQueryTimeWindow(pQuery, pBlockInfo->window.skey, sk, ek, &w);
    assert(w.ekey >= pBlockInfo->window.skey);

    if (w.ekey < pBlockInfo->window.ekey) {
      return true;
    }

    while(1) {
      getNextTimeWindow(pQuery, &w);
      if (w.skey > pBlockInfo->window.ekey) {
        break;
      }

      assert(w.ekey > pBlockInfo->window.ekey);
      if (w.skey <= pBlockInfo->window.ekey && w.skey > pBlockInfo->window.skey) {
        return true;
      }
    }
  } else {
    getAlignQueryTimeWindow(pQuery, pBlockInfo->window.ekey, sk, ek, &w);
    assert(w.skey <= pBlockInfo->window.ekey);

    if (w.skey > pBlockInfo->window.skey) {
      return true;
    }

    while(1) {
      getNextTimeWindow(pQuery, &w);
      if (w.ekey < pBlockInfo->window.skey) {
        break;
      }

      assert(w.skey < pBlockInfo->window.skey);
      if (w.ekey < pBlockInfo->window.ekey && w.ekey >= pBlockInfo->window.skey) {
        return true;
      }
    }
  }

  return false;
}

int32_t loadDataBlockOnDemand(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo * pWindowResInfo, void* pQueryHandle, SDataBlockInfo* pBlockInfo, SDataStatis **pStatis, SArray** pDataBlock, uint32_t* status) {
  *status = BLK_DATA_NO_NEEDED;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  int64_t groupId = pQuery->current->groupIndex;

  SQueryCostInfo* pCost = &pRuntimeEnv->summary;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf > 0) {
    *status = BLK_DATA_ALL_NEEDED;
  } else { // check if this data block is required to load

    // Calculate all time windows that are overlapping or contain current data block.
    // If current data block is contained by all possible time window, do not load current data block.
    if (QUERY_IS_INTERVAL_QUERY(pQuery) && overlapWithTimeWindow(pQuery, pBlockInfo)) {
      *status = BLK_DATA_ALL_NEEDED;
    }

    if ((*status) != BLK_DATA_ALL_NEEDED) {
      // the pCtx[i] result is belonged to previous time window since the outputBuf has not been set yet,
      // the filter result may be incorrect. So in case of interval query, we need to set the correct time output buffer
      if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
        SResultRow* pResult = NULL;

        bool masterScan = IS_MASTER_SCAN(pRuntimeEnv);

        TSKEY k = QUERY_IS_ASC_QUERY(pQuery)? pBlockInfo->window.skey:pBlockInfo->window.ekey;
        STimeWindow win = getActiveTimeWindow(pWindowResInfo, k, pQuery);
        if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId) != TSDB_CODE_SUCCESS) {
          // todo handle error in set result for timewindow
        }
      }

      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        SSqlFuncMsg* pSqlFunc = &pQuery->pExpr1[i].base;

        int32_t functionId = pSqlFunc->functionId;
        int32_t colId = pSqlFunc->colInfo.colId;
        (*status) |= aAggs[functionId].dataReqFunc(&pRuntimeEnv->pCtx[i], pBlockInfo->window.skey, pBlockInfo->window.ekey, colId);
        if (((*status) & BLK_DATA_ALL_NEEDED) == BLK_DATA_ALL_NEEDED) {
          break;
        }
      }
    }
  }

  if ((*status) == BLK_DATA_NO_NEEDED) {
    qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", GET_QINFO_ADDR(pRuntimeEnv),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->discardBlocks += 1;
  } else if ((*status) == BLK_DATA_STATIS_NEEDED) {

    // this function never returns error?
    tsdbRetrieveDataBlockStatisInfo(pQueryHandle, pStatis);
    pCost->loadBlockStatis += 1;

    if (*pStatis == NULL) { // data block statistics does not exist, load data block
      *pDataBlock = tsdbRetrieveDataBlock(pQueryHandle, NULL);
      pCost->totalCheckedRows += pBlockInfo->rows;
    }
  } else {
    assert((*status) == BLK_DATA_ALL_NEEDED);

    // load the data block statistics to perform further filter
    pCost->loadBlockStatis += 1;
    tsdbRetrieveDataBlockStatisInfo(pQueryHandle, pStatis);

    if (!needToLoadDataBlock(pRuntimeEnv, *pStatis, pRuntimeEnv->pCtx, pBlockInfo->rows)) {
      // current block has been discard due to filter applied
      pCost->discardBlocks += 1;
      qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", GET_QINFO_ADDR(pRuntimeEnv),
          pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      (*status) = BLK_DATA_DISCARD;
    }

    pCost->totalCheckedRows += pBlockInfo->rows;
    pCost->loadBlocks += 1;
    *pDataBlock = tsdbRetrieveDataBlock(pQueryHandle, NULL);
    if (*pDataBlock == NULL) {
      return terrno;
    }
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

static void ensureOutputBufferSimple(SQueryRuntimeEnv* pRuntimeEnv, int32_t capacity) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (capacity < pQuery->rec.capacity) {
    return;
  }

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t bytes = pQuery->pExpr1[i].bytes;
    assert(bytes > 0 && capacity > 0);

    char *tmp = realloc(pQuery->sdata[i], bytes * capacity + sizeof(tFilePage));
    if (tmp == NULL) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
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

// TODO merge with enuserOutputBufferSimple
static void ensureOutputBuffer(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pBlockInfo) {
  // in case of prj/diff query, ensure the output buffer is sufficient to accommodate the results of current block
  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (!QUERY_IS_INTERVAL_QUERY(pQuery) && !pRuntimeEnv->groupbyNormalCol && !isFixedOutputQuery(pRuntimeEnv) && !isTSCompQuery(pQuery)) {
    SResultRec *pRec = &pQuery->rec;

    if (pQuery->rec.capacity - pQuery->rec.rows < pBlockInfo->rows) {
      int32_t remain = (int32_t)(pRec->capacity - pRec->rows);
      int32_t newSize = (int32_t)(pRec->capacity + (pBlockInfo->rows - remain));

      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        int32_t bytes = pQuery->pExpr1[i].bytes;
        assert(bytes > 0 && newSize > 0);

        char *tmp = realloc(pQuery->sdata[i], bytes * newSize + sizeof(tFilePage));
        if (tmp == NULL) {
          longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        } else {
          memset(tmp + sizeof(tFilePage) + bytes * pRec->rows, 0, (size_t)((newSize - pRec->rows) * bytes));
          pQuery->sdata[i] = (tFilePage *)tmp;
        }

        // set the pCtx output buffer position
        pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data + pRec->rows * bytes;

        int32_t functionId = pQuery->pExpr1[i].base.functionId;
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
    SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      getAlignQueryTimeWindow(pQuery, pBlockInfo->window.skey, pBlockInfo->window.skey, pQuery->window.ekey, &w);
      pWindowResInfo->prevSKey = w.skey;
    } else { // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
      getAlignQueryTimeWindow(pQuery, pBlockInfo->window.ekey, pQuery->window.ekey, pBlockInfo->window.ekey, &w);
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
  while (tsdbNextDataBlock(pQueryHandle)) {
    summary->totalBlocks += 1;

    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
    doSetInitialTimewindow(pRuntimeEnv, &blockInfo);

    // in case of prj/diff query, ensure the output buffer is sufficient to accommodate the results of current block
    ensureOutputBuffer(pRuntimeEnv, &blockInfo);

    SDataStatis *pStatis = NULL;
    SArray *     pDataBlock = NULL;
    uint32_t     status = 0;

    int32_t ret = loadDataBlockOnDemand(pRuntimeEnv, &pRuntimeEnv->windowResInfo, pQueryHandle, &blockInfo, &pStatis, &pDataBlock, &status);
    if (ret != TSDB_CODE_SUCCESS) {
      break;
    }

    if (status == BLK_DATA_DISCARD) {
      pQuery->current->lastKey =
              QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.window.ekey + step : blockInfo.window.skey + step;
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

  if (terrno != TSDB_CODE_SUCCESS) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  // if the result buffer is not full, set the query complete
  if (!Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    closeAllResultRows(&pRuntimeEnv->windowResInfo);
    pRuntimeEnv->windowResInfo.curIndex = pRuntimeEnv->windowResInfo.size - 1;  // point to the last time window
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

  SExprInfo *pExprInfo = &pQuery->pExpr1[0];
  if (pQuery->numOfOutput == 1 && pExprInfo->base.functionId == TSDB_FUNC_TS_COMP) {
    assert(pExprInfo->base.numOfParams == 1);

    int16_t tagColId = (int16_t)pExprInfo->base.arg->argValue.i64;
    SColumnInfo* pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagColId);

    doSetTagValueInParam(tsdb, pTable, tagColId, &pRuntimeEnv->pCtx[0].tag, pColInfo->type, pColInfo->bytes);
  } else {
    // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutput; ++idx) {
      SExprInfo* pLocalExprInfo = &pQuery->pExpr1[idx];

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
    if ((pFuncMsg->functionId == TSDB_FUNC_TS || pFuncMsg->functionId == TSDB_FUNC_PRJ) && pRuntimeEnv->pTsBuf != NULL &&
        pFuncMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      assert(pFuncMsg->numOfParams == 1);

      int16_t      tagColId = (int16_t)pExprInfo->base.arg->argValue.i64;
      SColumnInfo *pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagColId);

      doSetTagValueInParam(tsdb, pTable, tagColId, &pRuntimeEnv->pCtx[0].tag, pColInfo->type, pColInfo->bytes);

      int16_t tagType = pRuntimeEnv->pCtx[0].tag.nType;
      if (tagType == TSDB_DATA_TYPE_BINARY || tagType == TSDB_DATA_TYPE_NCHAR) {
        qDebug("QInfo:%p set tag value for join comparison, colId:%" PRId64 ", val:%s", pQInfo,
               pExprInfo->base.arg->argValue.i64, pRuntimeEnv->pCtx[0].tag.pz);
      } else {
        qDebug("QInfo:%p set tag value for join comparison, colId:%" PRId64 ", val:%" PRId64, pQInfo,
               pExprInfo->base.arg->argValue.i64, pRuntimeEnv->pCtx[0].tag.i64Key);
      }
    }
  }
}

static UNUSED_FUNC void doMerge(SQueryRuntimeEnv *pRuntimeEnv, int64_t timestamp, SResultRow *pWindowRes, bool mergeFlag) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pWindowRes->pageId);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (!mergeFlag) {
      pCtx[i].aOutputBuf = pCtx[i].aOutputBuf + pCtx[i].outputBytes;
      pCtx[i].currentStage = FIRST_STAGE_MERGE;

      RESET_RESULT_INFO(pCtx[i].resultInfo);
      aAggs[functionId].init(&pCtx[i]);
    }

    pCtx[i].hasNull = true;
    pCtx[i].nStartQueryTimestamp = timestamp;
    pCtx[i].aInputElemBuf = getPosInResultPage(pRuntimeEnv, i, pWindowRes, page);

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
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
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

      switch (pQuery->pExpr1[i].type) {
        case TSDB_DATA_TYPE_BINARY: {
          int32_t type = pQuery->pExpr1[i].type;
          printBinaryData(pQuery->pExpr1[i].base.functionId, pdata[i]->data + pQuery->pExpr1[i].bytes * j,
                          type);
          break;
        }
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          printf("%" PRId64 "\t", *(int64_t *)(pdata[i]->data + pQuery->pExpr1[i].bytes * j));
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)(pdata[i]->data + pQuery->pExpr1[i].bytes * j));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)(pdata[i]->data + pQuery->pExpr1[i].bytes * j));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)(pdata[i]->data + pQuery->pExpr1[i].bytes * j));
          break;
      }
    }
    printf("\n");
  }
}

typedef struct SCompSupporter {
  STableQueryInfo **pTableQueryInfo;
  int32_t          *rowIndex;
  int32_t           order;
} SCompSupporter;

int32_t tableResultComparFn(const void *pLeft, const void *pRight, void *param) {
  int32_t left  = *(int32_t *)pLeft;
  int32_t right = *(int32_t *)pRight;

  SCompSupporter *  supporter = (SCompSupporter *)param;

  int32_t leftPos  = supporter->rowIndex[left];
  int32_t rightPos = supporter->rowIndex[right];

  /* left source is exhausted */
  if (leftPos == -1) {
    return 1;
  }

  /* right source is exhausted*/
  if (rightPos == -1) {
    return -1;
  }

  STableQueryInfo** pList = supporter->pTableQueryInfo;

  SResultRowInfo *pWindowResInfo1 = &(pList[left]->windowResInfo);
  SResultRow * pWindowRes1 = getResultRow(pWindowResInfo1, leftPos);
  TSKEY leftTimestamp = pWindowRes1->win.skey;

  SResultRowInfo *pWindowResInfo2 = &(pList[right]->windowResInfo);
  SResultRow * pWindowRes2 = getResultRow(pWindowResInfo2, rightPos);
  TSKEY rightTimestamp = pWindowRes2->win.skey;

  if (leftTimestamp == rightTimestamp) {
    return 0;
  }

  if (supporter->order == TSDB_ORDER_ASC) {
    return (leftTimestamp > rightTimestamp)? 1:-1;
  } else {
    return (leftTimestamp < rightTimestamp)? 1:-1;
  }
}

int32_t mergeGroupResult(SQInfo *pQInfo) {
  int64_t st = taosGetTimestampUs();

  SGroupResInfo* pGroupResInfo = &pQInfo->groupResInfo;

  int32_t numOfGroups = (int32_t)(GET_NUM_OF_TABLEGROUP(pQInfo));
  while (pQInfo->groupIndex < numOfGroups) {
    SArray *group = GET_TABLEGROUP(pQInfo, pQInfo->groupIndex);

    int32_t ret = mergeIntoGroupResultImpl(pGroupResInfo, group, pQInfo);
    if (ret < 0) {
      return -1;
    }

    // this group generates at least one result, return results
    pQInfo->groupIndex += 1;
    if (taosArrayGetSize(pGroupResInfo->pRows) > 0) {
      break;
    }

    qDebug("QInfo:%p no result in group %d, continue", pQInfo, pQInfo->groupIndex - 1);
    taosArrayClear(pGroupResInfo->pRows);

    pGroupResInfo->index = 0;
    pGroupResInfo->rowId = 0;
  }

  if (pQInfo->groupIndex == numOfGroups && taosArrayGetSize(pGroupResInfo->pRows) == 0) {
    SET_STABLE_QUERY_OVER(pQInfo);
  }

  int64_t elapsedTime = taosGetTimestampUs() - st;
  qDebug("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%" PRId64 "us", pQInfo,
         pQInfo->groupIndex - 1, numOfGroups, elapsedTime);

  pQInfo->runtimeEnv.summary.firstStageMergeTime += elapsedTime;
  return TSDB_CODE_SUCCESS;
}

static int32_t doCopyToSData(SQInfo *pQInfo, SResultRow **pRows, int32_t numOfRows, int32_t* index, int32_t orderType);

void copyResToQueryResultBuf(SQInfo *pQInfo, SQuery *pQuery) {
  SGroupResInfo* pGroupResInfo = &pQInfo->groupResInfo;

  // all results in current group have been returned to client, try next group
  if (pGroupResInfo->index >= taosArrayGetSize(pGroupResInfo->pRows)) {
    // current results of group has been sent to client, try next group
    if (mergeGroupResult(pQInfo) != TSDB_CODE_SUCCESS) {
      return;  // failed to save data in the disk
    }

    // check if all results has been sent to client
    int32_t numOfGroup = (int32_t)(GET_NUM_OF_TABLEGROUP(pQInfo));
    if (taosArrayGetSize(pGroupResInfo->pRows) == 0 && pQInfo->groupIndex == numOfGroup) {
      SET_STABLE_QUERY_OVER(pQInfo);
      return;
    }
  }

  int32_t size = (int32_t) taosArrayGetSize(pGroupResInfo->pRows);
  pQuery->rec.rows = doCopyToSData(pQInfo, pGroupResInfo->pRows->pData, (int32_t) size, &pGroupResInfo->index, TSDB_ORDER_ASC);
}

int64_t getNumOfResultWindowRes(SQueryRuntimeEnv* pRuntimeEnv, SResultRow *pResultRow) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pExpr1[j].base.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    SResultRowCellInfo *pResultInfo = getResultCell(pRuntimeEnv, pResultRow, j);
    assert(pResultInfo != NULL);

    if (pResultInfo->numOfRes > 0) {
      return pResultInfo->numOfRes;
    }
  }

  return 0;
}

int32_t mergeIntoGroupResultImpl(SGroupResInfo* pGroupResInfo, SArray *pTableList, SQInfo* pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  bool ascQuery = QUERY_IS_ASC_QUERY(pRuntimeEnv->pQuery);

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t *posList = NULL;
  SLoserTreeInfo *pTree = NULL;
  STableQueryInfo **pTableQueryInfoList = NULL;

  size_t size = taosArrayGetSize(pTableList);
  if (pGroupResInfo->pRows == NULL) {
    pGroupResInfo->pRows = taosArrayInit(100, POINTER_BYTES);
  }

  posList = calloc(size, sizeof(int32_t));
  pTableQueryInfoList = malloc(POINTER_BYTES * size);

  if (pTableQueryInfoList == NULL || posList == NULL) {
    qError("QInfo:%p failed alloc memory", pQInfo);
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _end;
  }

  int32_t numOfTables = 0;
  for (int32_t i = 0; i < size; ++i) {
    STableQueryInfo *item = taosArrayGetP(pTableList, i);
    if (item->windowResInfo.size > 0) {
      pTableQueryInfoList[numOfTables++] = item;
    }
  }

  // there is no data in current group
  // no need to merge results since only one table in each group
  if (numOfTables == 0) {
    goto _end;
  }

  SCompSupporter cs = {pTableQueryInfoList, posList, pRuntimeEnv->pQuery->order.order};

  int32_t ret = tLoserTreeCreate(&pTree, numOfTables, &cs, tableResultComparFn);
  if (ret != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _end;
  }

  int64_t lastTimestamp = ascQuery? INT64_MIN:INT64_MAX;
  int64_t startt = taosGetTimestampMs();

  while (1) {
    if (isQueryKilled(pQInfo)) {
      qDebug("QInfo:%p it is already killed, abort", pQInfo);
      code = TSDB_CODE_TSC_QUERY_CANCELLED;
      goto _end;
    }

    int32_t tableIndex = pTree->pNode[0].index;

    SResultRowInfo *pWindowResInfo = &pTableQueryInfoList[tableIndex]->windowResInfo;
    SResultRow  *pWindowRes = getResultRow(pWindowResInfo, cs.rowIndex[tableIndex]);

    int64_t num = getNumOfResultWindowRes(pRuntimeEnv, pWindowRes);
    if (num <= 0) {
      cs.rowIndex[tableIndex] += 1;

      if (cs.rowIndex[tableIndex] >= pWindowResInfo->size) {
        cs.rowIndex[tableIndex] = -1;
        if (--numOfTables == 0) { // all input sources are exhausted
          break;
        }
      }
    } else {
      assert((pWindowRes->win.skey >= lastTimestamp && ascQuery) || (pWindowRes->win.skey <= lastTimestamp && !ascQuery));

      if (pWindowRes->win.skey != lastTimestamp) {
        taosArrayPush(pGroupResInfo->pRows, &pWindowRes);
        pWindowRes->numOfRows = (uint32_t) num;
      }

      lastTimestamp = pWindowRes->win.skey;

      // move to the next row of current entry
      if ((++cs.rowIndex[tableIndex]) >= pWindowResInfo->size) {
        cs.rowIndex[tableIndex] = -1;

        // all input sources are exhausted
        if ((--numOfTables) == 0) {
          break;
        }
      }
    }

    tLoserTreeAdjust(pTree, tableIndex + pTree->numOfEntries);
  }

  int64_t endt = taosGetTimestampMs();

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pRuntimeEnv, pQuery->sdata[0]->num);
#endif

  qDebug("QInfo:%p result merge completed for group:%d, elapsed time:%" PRId64 " ms", pQInfo, pQInfo->groupIndex, endt - startt);

  _end:
  tfree(pTableQueryInfoList);
  tfree(posList);
  tfree(pTree);

  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pRuntimeEnv->env, code);
  }

  return code;
}

static void updateTableQueryInfoForReverseScan(SQuery *pQuery, STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  // order has changed already
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (pTableQueryInfo->lastKey == pTableQueryInfo->win.skey) {
    // do nothing, no results
  } else {// NOTE: even win.skey != lastKey, the results may not generated.
    pTableQueryInfo->win.ekey = pTableQueryInfo->lastKey + step;
  }

  SWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, TSKEY);
  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;

  SWITCH_ORDER(pTableQueryInfo->cur.order);
  pTableQueryInfo->cur.vgroupIndex = -1;

  // set the index at the end of time window
  pTableQueryInfo->windowResInfo.curIndex = pTableQueryInfo->windowResInfo.size - 1;
}

static void disableFuncInReverseScanImpl(SQueryRuntimeEnv* pRuntimeEnv, SResultRowInfo *pWindowResInfo, int32_t order) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
    bool closed = getResultRowStatus(pWindowResInfo, i);
    if (!closed) {
      continue;
    }

    SResultRow *pRow = getResultRow(pWindowResInfo, i);

    // open/close the specified query for each group result
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t functId = pQuery->pExpr1[j].base.functionId;
      SResultRowCellInfo* pInfo = getResultCell(pRuntimeEnv, pRow, j);

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_ASC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_DESC)) {
        pInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pInfo->complete = true;
      }
    }
  }
}

void disableFuncInReverseScan(SQInfo *pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  // group by normal columns and interval query on normal table
  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    disableFuncInReverseScanImpl(pRuntimeEnv, pWindowResInfo, order);
  } else {  // for simple result of table query,
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {  // todo refactor
      int32_t functId = pQuery->pExpr1[j].base.functionId;

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
}

static void setupQueryRangeForReverseScan(SQInfo* pQInfo) {
  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  int32_t numOfGroups = (int32_t)(GET_NUM_OF_TABLEGROUP(pQInfo));

  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray *group = GET_TABLEGROUP(pQInfo, i);
    SArray *tableKeyGroup = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, i);

    size_t t = taosArrayGetSize(group);
    for (int32_t j = 0; j < t; ++j) {
      STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);
      updateTableQueryInfoForReverseScan(pQuery, pCheckInfo);

      // update the last key in tableKeyInfo list, the tableKeyInfo is used to build the tsdbQueryHandle and decide
      // the start check timestamp of tsdbQueryHandle
      STableKeyInfo *pTableKeyInfo = taosArrayGet(tableKeyGroup, j);
      pTableKeyInfo->lastKey = pCheckInfo->lastKey;

      assert(pCheckInfo->pTable == pTableKeyInfo->pTable);
    }
  }
}

void switchCtxOrder(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SWITCH_ORDER(pRuntimeEnv->pCtx[i].order);
  }
}

int32_t initResultRow(SResultRow *pResultRow) {
  pResultRow->pCellInfo = (SResultRowCellInfo*)((char*)pResultRow + sizeof(SResultRow));
  pResultRow->pageId    = -1;
  pResultRow->rowId     = -1;
  return TSDB_CODE_SUCCESS;
}

void resetDefaultResInfoOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t tid = 0;
  int64_t uid = 0;
  SResultRow* pRow = doPrepareResultRowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, (char *)&tid, sizeof(tid), true, uid);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = pQuery->sdata[i]->data;

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT/TAGPRJ/PRJ/TAG etc.
     */
    SResultRowCellInfo* pCellInfo = getResultCell(pRuntimeEnv, pRow, i);
    RESET_RESULT_INFO(pCellInfo);
    pCtx->resultInfo = pCellInfo;

    // set the timestamp output buffer for top/bottom/diff query
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    memset(pQuery->sdata[i]->data, 0, (size_t)(pQuery->pExpr1[i].bytes * pQuery->rec.capacity));
  }

  initCtxOutputBuf(pRuntimeEnv);
}

void forwardCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, int64_t output) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // reset the execution contexts
  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pExpr1[j].base.functionId;
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
      pRuntimeEnv->pCtx[j].ptsOutputBuf = (char*)pRuntimeEnv->pCtx[j].ptsOutputBuf + TSDB_KEYSIZE * output;
    }

    RESET_RESULT_INFO(pRuntimeEnv->pCtx[j].resultInfo);
  }
}

void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pExpr1[j].base.functionId;
    pRuntimeEnv->pCtx[j].currentStage = 0;

    SResultRowCellInfo* pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);
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

    resetDefaultResInfoOutputBuf(pRuntimeEnv);

    // clear the buffer full flag if exists
    CLEAR_QUERY_STATUS(pQuery, QUERY_RESBUF_FULL);
  } else {
    int64_t numOfSkip = pQuery->limit.offset;
    pQuery->rec.rows -= numOfSkip;
    pQuery->limit.offset = 0;

    qDebug("QInfo:%p skip row:%"PRId64", new offset:%d, numOfRows remain:%" PRIu64, GET_QINFO_ADDR(pRuntimeEnv), numOfSkip,
           0, pQuery->rec.rows);

    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      int32_t functionId = pQuery->pExpr1[i].base.functionId;
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;

      memmove(pQuery->sdata[i]->data, (char*)pQuery->sdata[i]->data + bytes * numOfSkip, (size_t)(pQuery->rec.rows * bytes));
      pRuntimeEnv->pCtx[i].aOutputBuf = ((char*) pQuery->sdata[i]->data) + pQuery->rec.rows * bytes;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
      }
    }

    updateNumOfResult(pRuntimeEnv, (int32_t)pQuery->rec.rows);
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
    SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SResultRow *pResult = getResultRow(pWindowResInfo, i);

      setResultOutputBuf(pRuntimeEnv, pResult);
      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        int16_t functId = pQuery->pExpr1[j].base.functionId;
        if (functId == TSDB_FUNC_TS) {
          continue;
        }

        aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
        SResultRowCellInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

        toContinue |= (!pResInfo->complete);
      }
    }
  } else {
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int16_t functId = pQuery->pExpr1[j].base.functionId;
      if (functId == TSDB_FUNC_TS) {
        continue;
      }

      aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
      SResultRowCellInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

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
  };

  TIME_WINDOW_COPY(info.w, pQuery->window);
  TIME_WINDOW_COPY(info.curWindow, pTableQueryInfo->win);

  info.curWindow.skey = start;

  return info;
}

static void setEnvBeforeReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatusInfo *pStatus) {
  SQInfo *pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pStatus->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);  // save the cursor
  if (pRuntimeEnv->pTsBuf) {
    SWITCH_ORDER(pRuntimeEnv->pTsBuf->cur.order);
    bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
    assert(ret);
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
  STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  switchCtxOrder(pRuntimeEnv);
  disableFuncInReverseScan(pQInfo);
  setupQueryRangeForReverseScan(pQInfo);

  // clean unused handle
  if (pRuntimeEnv->pSecQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
  }

  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);
  if (pRuntimeEnv->pSecQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }
}

static void clearEnvAfterReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatusInfo *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;

  SWITCH_ORDER(pQuery->order.order);
  switchCtxOrder(pRuntimeEnv);

  tsBufSetCursor(pRuntimeEnv->pTsBuf, &pStatus->cur);
  if (pRuntimeEnv->pTsBuf) {
    pRuntimeEnv->pTsBuf->cur.order = pQuery->order.order;
  }

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);

  // update the pQuery->window.skey and pQuery->window.ekey to limit the scan scope of sliding query during reverse scan
  pTableQueryInfo->lastKey = pStatus->lastKey;
  pQuery->status = pStatus->status;

  pTableQueryInfo->win = pStatus->w;
  pQuery->window = pTableQueryInfo->win;
}

static void restoreTimeWindow(STableGroupInfo* pTableGroupInfo, STsdbQueryCond* pCond) {
  assert(pTableGroupInfo->numOfTables == 1);
  SArray* pTableKeyGroup = taosArrayGetP(pTableGroupInfo->pGroupList, 0);
  STableKeyInfo* pKeyInfo = taosArrayGet(pTableKeyGroup, 0);
  pKeyInfo->lastKey = pCond->twindow.skey;
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
      } else { // the lastkey does not increase, which means no data checked yet
        qDebug("QInfo:%p no results generated in this scan", pQInfo);
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

    if (pRuntimeEnv->pSecQueryHandle != NULL) {
      tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
    }

    STsdbQueryCond cond = createTsdbQueryCond(pQuery, &qstatus.curWindow);
    restoreTimeWindow(&pQInfo->tableGroupInfo, &cond);
    pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);
    if (pRuntimeEnv->pSecQueryHandle == NULL) {
      longjmp(pRuntimeEnv->env, terrno);
    }

    pRuntimeEnv->windowResInfo.curIndex = qstatus.windowIndex;
    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    qDebug("QInfo:%p start to repeat scan data blocks due to query func required, qrange:%"PRId64"-%"PRId64, pQInfo,
        cond.twindow.skey, cond.twindow.ekey);

    // check if query is killed or not
    if (isQueryKilled(pQInfo)) {
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
    SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    if (pRuntimeEnv->groupbyNormalCol) {
      closeAllResultRows(pWindowResInfo);
    }

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SResultRow *buf = pWindowResInfo->pResult[i];
      if (!isResultRowClosed(pWindowResInfo, i)) {
        continue;
      }

      setResultOutputBuf(pRuntimeEnv, buf);

      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        aAggs[pQuery->pExpr1[j].base.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
      }

      /*
       * set the number of output results for group by normal columns, the number of output rows usually is 1 except
       * the top and bottom query
       */
      buf->numOfRows = (uint16_t)getNumOfResult(pRuntimeEnv);
    }

  } else {
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      aAggs[pQuery->pExpr1[j].base.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
    }
  }
}

static bool hasMainOutput(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;

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
    int32_t initialSize = 128;
    int32_t code = initResultRowInfo(&pTableQueryInfo->windowResInfo, initialSize, TSDB_DATA_TYPE_INT);
    if (code != TSDB_CODE_SUCCESS) {
      return NULL;
    }
  } else { // in other aggregate query, do not initialize the windowResInfo
  }

  return pTableQueryInfo;
}

void destroyTableQueryInfoImpl(STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  tVariantDestroy(&pTableQueryInfo->tag);
  cleanupResultRowInfo(&pTableQueryInfo->windowResInfo);
}

/**
 * set output buffer for different group
 * @param pRuntimeEnv
 * @param pDataBlockInfo
 */
void setExecutionContext(SQInfo *pQInfo, int32_t groupIndex, TSKEY nextKey) {
  SQueryRuntimeEnv *pRuntimeEnv     = &pQInfo->runtimeEnv;
  STableQueryInfo  *pTableQueryInfo = pRuntimeEnv->pQuery->current;
  SResultRowInfo   *pWindowResInfo  = &pRuntimeEnv->windowResInfo;

  // lastKey needs to be updated
  pTableQueryInfo->lastKey = nextKey;

  if (pRuntimeEnv->hasTagResults || pRuntimeEnv->pTsBuf != NULL) {
    setAdditionalInfo(pQInfo, pTableQueryInfo->pTable, pTableQueryInfo);
  }

  if (pRuntimeEnv->prevGroupId != INT32_MIN && pRuntimeEnv->prevGroupId == groupIndex) {
    return;
  }

  int64_t uid = 0;
  SResultRow *pResultRow = doPrepareResultRowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&groupIndex,
      sizeof(groupIndex), true, uid);
  assert (pResultRow != NULL);

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pResultRow->pageId == -1) {
    if (addNewWindowResultBuf(pResultRow, pRuntimeEnv->pResultBuf, groupIndex, pRuntimeEnv->numOfRowsPerPage) !=
        TSDB_CODE_SUCCESS) {
      return;
    }
  }

  // record the current active group id
  pRuntimeEnv->prevGroupId = groupIndex;
  setResultOutputBuf(pRuntimeEnv, pResultRow);
  initCtxOutputBuf(pRuntimeEnv);
}

void setResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = getPosInResultPage(pRuntimeEnv, i, pResult, page);

    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    /*
     * set the output buffer information and intermediate buffer,
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx->resultInfo = getResultCell(pRuntimeEnv, pResult, i);
  }
}

void setResultRowOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  tFilePage* bufPage = getResBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

    pCtx->resultInfo = getResultCell(pRuntimeEnv, pResult, i);
    if (pCtx->resultInfo->initialized && pCtx->resultInfo->complete) {
      continue;
    }

    pCtx->aOutputBuf = getPosInResultPage(pRuntimeEnv, i, pResult, bufPage);
    pCtx->currentStage = 0;

    int32_t functionId = pCtx->functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    if (!pCtx->resultInfo->initialized) {
      aAggs[functionId].init(pCtx);
    }
  }
}

int32_t setAdditionalInfo(SQInfo *pQInfo, void* pTable, STableQueryInfo *pTableQueryInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  setTagVal(pRuntimeEnv, pTable, pQInfo->tsdb);

  // both the master and supplement scan needs to set the correct ts comp start position
  if (pRuntimeEnv->pTsBuf != NULL) {
    tVariant* pTag = &pRuntimeEnv->pCtx[0].tag;

    if (pTableQueryInfo->cur.vgroupIndex == -1) {
      tVariantAssign(&pTableQueryInfo->tag, pTag);

      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQInfo->vgId, &pTableQueryInfo->tag);

      // failed to find data with the specified tag value and vnodeId
      if (!tsBufIsValidElem(&elem)) {
        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qError("QInfo:%p failed to find tag:%s in ts_comp", pQInfo, pTag->pz);
        } else {
          qError("QInfo:%p failed to find tag:%" PRId64 " in ts_comp", pQInfo, pTag->i64Key);
        }

        return false;
      }

      // keep the cursor info of current meter
      pTableQueryInfo->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
      if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
        qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
      } else {
        qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64Key, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
      }

    } else {
      tsBufSetCursor(pRuntimeEnv->pTsBuf, &pTableQueryInfo->cur);

      if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
        qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
      } else {
        qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64Key, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
      }
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
     * In ascending query, the key is the first qualified timestamp. However, in the descending order query, additional
     * operations involve.
     */
    STimeWindow     w = TSWINDOW_INITIALIZER;
    SResultRowInfo *pWindowResInfo = &pTableQueryInfo->windowResInfo;

    TSKEY sk = MIN(win.skey, win.ekey);
    TSKEY ek = MAX(win.skey, win.ekey);
    getAlignQueryTimeWindow(pQuery, win.skey, sk, ek, &w);

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
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
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

static int32_t doCopyToSData(SQInfo *pQInfo, SResultRow **pRows, int32_t numOfRows, int32_t *index, int32_t orderType) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfResult = 0;
  int32_t start = 0;
  int32_t step = -1;

  qDebug("QInfo:%p start to copy data from windowResInfo to query buf", pQInfo);
  if (orderType == TSDB_ORDER_ASC) {
    start = (*index);
    step = 1;
  } else {  // desc order copy all data
    start = numOfRows - (*index) - 1;
    step = -1;
  }

  SGroupResInfo* pGroupResInfo = &pQInfo->groupResInfo;

  for (int32_t i = start; (i < numOfRows) && (i >= 0); i += step) {
    if (pRows[i]->numOfRows == 0) {
      (*index) += 1;
      pGroupResInfo->rowId = 0;
      continue;
    }

    int32_t numOfRowsToCopy = pRows[i]->numOfRows - pGroupResInfo->rowId;
    int32_t oldOffset = pGroupResInfo->rowId;

    /*
     * current output space is not enough to accommodate all data of this page, only partial results
     * will be copied to SQuery object's result buffer
     */
    if (numOfRowsToCopy > pQuery->rec.capacity - numOfResult) {
      numOfRowsToCopy = (int32_t) pQuery->rec.capacity - numOfResult;
      pGroupResInfo->rowId += numOfRowsToCopy;
    } else {
      pGroupResInfo->rowId = 0;
      (*index) += 1;
    }

    tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pRows[i]->pageId);

    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t size = pRuntimeEnv->pCtx[j].outputBytes;

      char *out = pQuery->sdata[j]->data + numOfResult * size;
      char *in  = getPosInResultPage(pRuntimeEnv, j, pRows[i], page);
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
void copyFromWindowResToSData(SQInfo *pQInfo, SResultRowInfo *pResultInfo) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSDB_ORDER_ASC;
  int32_t numOfResult = doCopyToSData(pQInfo, pResultInfo->pResult, pResultInfo->size, &pQInfo->groupIndex, orderType);

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
    SResultRow *pResult = pRuntimeEnv->windowResInfo.pResult[i];

    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      int32_t functionId = pRuntimeEnv->pCtx[j].functionId;
      if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
        continue;
      }

      SResultRowCellInfo* pCell = getResultCell(pRuntimeEnv, pResult, j);
      pResult->numOfRows = (uint16_t)(MAX(pResult->numOfRows, pCell->numOfRes));
    }
  }
}

static void stableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo, SDataStatis *pStatis,
    SArray *pDataBlock, __block_search_fn_t searchFn) {
  SQuery *         pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;

  SResultRowInfo * pResultRowInfo = &pTableQueryInfo->windowResInfo;
  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery)? 0 : pDataBlockInfo->rows - 1;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pRuntimeEnv->groupbyNormalCol) {
    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, pDataBlock);
  } else {
    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, searchFn, pDataBlock);
  }

  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    updateResultRowIndex(pResultRowInfo, pTableQueryInfo, QUERY_IS_ASC_QUERY(pQuery));
  }
}

bool queryHasRemainResForTableQuery(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SFillInfo *pFillInfo = pRuntimeEnv->pFillInfo;

  if (pQuery->limit.limit > 0 && pQuery->rec.total >= pQuery->limit.limit) {
    return false;
  }

  if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
    // There are results not returned to client yet, so filling applied to the remain result is required firstly.
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
      int32_t numOfTotal = (int32_t)getNumOfResWithFill(pFillInfo, pQuery->window.ekey, (int32_t)pQuery->rec.capacity);
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

static int16_t getNumOfFinalResCol(SQuery* pQuery) {
  return pQuery->pExpr2 == NULL? pQuery->numOfOutput:pQuery->numOfExpr2;
}

static void doCopyQueryResultToMsg(SQInfo *pQInfo, int32_t numOfRows, char *data) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if (pQuery->pExpr2 == NULL) {
    for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
      int32_t bytes = pQuery->pExpr1[col].bytes;

      memmove(data, pQuery->sdata[col]->data, bytes * numOfRows);
      data += bytes * numOfRows;
    }
  } else {
    for (int32_t col = 0; col < pQuery->numOfExpr2; ++col) {
      int32_t bytes = pQuery->pExpr2[col].bytes;

      memmove(data, pQuery->sdata[col]->data, bytes * numOfRows);
      data += bytes * numOfRows;
    }
  }

  int32_t numOfTables = (int32_t) taosHashGetSize(pQInfo->arrTableIdInfo);
  *(int32_t*)data = htonl(numOfTables);
  data += sizeof(int32_t);

  int32_t total = 0;
  STableIdInfo* item = taosHashIterate(pQInfo->arrTableIdInfo, NULL);

  while(item) {
    STableIdInfo* pDst = (STableIdInfo*)data;
    pDst->uid = htobe64(item->uid);
    pDst->tid = htonl(item->tid);
    pDst->key = htobe64(item->key);

    data += sizeof(STableIdInfo);
    total++;

    qDebug("QInfo:%p set subscribe info, tid:%d, uid:%"PRIu64", skey:%"PRId64, pQInfo, item->tid, item->uid, item->key);
    item = taosHashIterate(pQInfo->arrTableIdInfo, item);
  }

  qDebug("QInfo:%p set %d subscribe info", pQInfo, total);

  // Check if query is completed or not for stable query or normal table query respectively.
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    if (pQInfo->runtimeEnv.stableQuery) {
      if (IS_STASBLE_QUERY_OVER(pQInfo)) {
        setQueryStatus(pQuery, QUERY_OVER);
      }
    } else {
      if (!queryHasRemainResForTableQuery(&pQInfo->runtimeEnv)) {
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
    int32_t ret = (int32_t)taosFillResultDataBlock(pFillInfo, (tFilePage**)pQuery->sdata, (int32_t)pQuery->rec.capacity);

    // todo apply limit output function
    /* reached the start position of according to offset value, return immediately */
    if (pQuery->limit.offset == 0) {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows", pQInfo, pFillInfo->numOfRows, ret);
      return ret;
    }

    if (pQuery->limit.offset < ret) {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows, offset:%" PRId64 ". Discard due to offset, remain:%" PRId64 ", new offset:%d",
             pQInfo, pFillInfo->numOfRows, ret, pQuery->limit.offset, ret - pQuery->limit.offset, 0);

      ret -= (int32_t)pQuery->limit.offset;
      // todo !!!!there exactly number of interpo is not valid.
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        memmove(pDst[i]->data, pDst[i]->data + pQuery->pExpr1[i].bytes * pQuery->limit.offset,
                ret * pQuery->pExpr1[i].bytes);
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

    if (!queryHasRemainResForTableQuery(pRuntimeEnv)) {
      return ret;
    }
  }
}

static void queryCostStatis(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQueryCostInfo *pSummary = &pRuntimeEnv->summary;

  uint64_t hashSize = taosHashGetMemSize(pQInfo->runtimeEnv.pResultRowHashTable);
  hashSize += taosHashGetMemSize(pQInfo->tableqinfoGroupInfo.map);
  pSummary->hashSize = hashSize;

  // add the merge time
  pSummary->elapsedTime += pSummary->firstStageMergeTime;

  SResultRowPool* p = pQInfo->runtimeEnv.pool;
  pSummary->winInfoSize = getResultRowPoolMemSize(p);
  pSummary->numOfTimeWindows = getNumOfAllocatedResultRows(p);

  qDebug("QInfo:%p :cost summary: elapsed time:%"PRId64" us, first merge:%"PRId64" us, total blocks:%d, "
         "load block statis:%d, load data block:%d, total rows:%"PRId64 ", check rows:%"PRId64,
         pQInfo, pSummary->elapsedTime, pSummary->firstStageMergeTime, pSummary->totalBlocks, pSummary->loadBlockStatis,
         pSummary->loadBlocks, pSummary->totalRows, pSummary->totalCheckedRows);

  qDebug("QInfo:%p :cost summary: winResPool size:%.2f Kb, numOfWin:%"PRId64", tableInfoSize:%.2f Kb, hashTable:%.2f Kb", pQInfo, pSummary->winInfoSize/1024.0,
      pSummary->numOfTimeWindows, pSummary->tableInfoSize/1024.0, pSummary->hashSize/1024.0);
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
    pQuery->pos = (int32_t)pQuery->limit.offset;
  } else {
    pQuery->pos = pBlockInfo->rows - (int32_t)pQuery->limit.offset - 1;
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
  while (tsdbNextDataBlock(pQueryHandle)) {
    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
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

  if (terrno != TSDB_CODE_SUCCESS) {
    longjmp(pRuntimeEnv->env, terrno);
  }
}

static TSKEY doSkipIntervalProcess(SQueryRuntimeEnv* pRuntimeEnv, STimeWindow* win, SDataBlockInfo* pBlockInfo, STableQueryInfo* pTableQueryInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

  assert(pQuery->limit.offset == 0);
  STimeWindow tw = *win;
  getNextTimeWindow(pQuery, &tw);

  if ((tw.skey <= pBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (tw.ekey >= pBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {

    // load the data block and check data remaining in current data block
    // TODO optimize performance
    SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
    SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);

    tw = *win;
    int32_t startPos =
        getNextQualifiedWindow(pRuntimeEnv, &tw, pBlockInfo, pColInfoData->pData, binarySearchForKey, -1);
    assert(startPos >= 0);

    // set the abort info
    pQuery->pos = startPos;

    // reset the query start timestamp
    pTableQueryInfo->win.skey = ((TSKEY *)pColInfoData->pData)[startPos];
    pQuery->window.skey = pTableQueryInfo->win.skey;
    TSKEY key = pTableQueryInfo->win.skey;

    pWindowResInfo->prevSKey = tw.skey;
    int32_t index = pRuntimeEnv->windowResInfo.curIndex;

    int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
    pRuntimeEnv->windowResInfo.curIndex = index;  // restore the window index

    qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%" PRId64,
           GET_QINFO_ADDR(pRuntimeEnv), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes,
           pQuery->current->lastKey);

    return key;
  } else {  // do nothing
    pQuery->window.skey = tw.skey;
    pWindowResInfo->prevSKey = tw.skey;

    return tw.skey;
  }

  return true;
}

static bool skipTimeInterval(SQueryRuntimeEnv *pRuntimeEnv, TSKEY* start) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // get the first unclosed time window
  bool assign = false;
  for(int32_t i = 0; i < pRuntimeEnv->windowResInfo.size; ++i) {
    if (pRuntimeEnv->windowResInfo.pResult[i]->closed) {
      continue;
    }

    assign = true;
    *start = pRuntimeEnv->windowResInfo.pResult[i]->win.skey;
  }

  if (!assign) {
    *start = pQuery->current->lastKey;
  }

  assert(*start <= pQuery->current->lastKey);

  // if queried with value filter, do NOT forward query start position
  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pRuntimeEnv->pFillInfo != NULL) {
    return true;
  }

  /*
   * 1. for interval without interpolation query we forward pQuery->interval.interval at a time for
   *    pQuery->limit.offset times. Since hole exists, pQuery->interval.interval*pQuery->limit.offset value is
   *    not valid. otherwise, we only forward pQuery->limit.offset number of points
   */
  assert(pRuntimeEnv->windowResInfo.prevSKey == TSKEY_INITIAL_VAL);

  STimeWindow w = TSWINDOW_INITIALIZER;

  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  STableQueryInfo *pTableQueryInfo = pQuery->current;

  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
  while (tsdbNextDataBlock(pRuntimeEnv->pQueryHandle)) {
    tsdbRetrieveDataBlockInfo(pRuntimeEnv->pQueryHandle, &blockInfo);

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
        getAlignQueryTimeWindow(pQuery, blockInfo.window.skey, blockInfo.window.skey, pQuery->window.ekey, &w);
        pWindowResInfo->prevSKey = w.skey;
      }
    } else {
      getAlignQueryTimeWindow(pQuery, blockInfo.window.ekey, pQuery->window.ekey, blockInfo.window.ekey, &w);
      pWindowResInfo->prevSKey = w.skey;
    }

    // the first time window
    STimeWindow win = getActiveTimeWindow(pWindowResInfo, pWindowResInfo->prevSKey, pQuery);

    while (pQuery->limit.offset > 0) {
      STimeWindow tw = win;

      if ((win.ekey <= blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (win.ekey >= blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
        pQuery->limit.offset -= 1;
        pWindowResInfo->prevSKey = win.skey;
      }

      // current window does not ended in current data block, try next data block
      getNextTimeWindow(pQuery, &tw);
      if (pQuery->limit.offset == 0) {
        *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
        return true;
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

        if ((win.ekey > blockInfo.window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (win.ekey < blockInfo.window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
          pQuery->limit.offset -= 1;
        }

        if (pQuery->limit.offset == 0) {
          *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
          return true;
        } else {
          tw = win;
          int32_t startPos =
              getNextQualifiedWindow(pRuntimeEnv, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey, -1);
          assert(startPos >= 0);

          // set the abort info
          pQuery->pos = startPos;
          pTableQueryInfo->lastKey = ((TSKEY *)pColInfoData->pData)[startPos];
          pWindowResInfo->prevSKey = tw.skey;
          win = tw;
        }
      } else {
        break;  // offset is not 0, and next time window begins or ends in the next block.
      }
    }
  }

  // check for error
  if (terrno != TSDB_CODE_SUCCESS) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  return true;
}

static void doDestroyTableQueryInfo(STableGroupInfo* pTableqinfoGroupInfo);

static int32_t setupQueryHandle(void* tsdb, SQInfo* pQInfo, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if (onlyQueryTags(pQuery)) {
    return TSDB_CODE_SUCCESS;
  }

  if (isSTableQuery && (!QUERY_IS_INTERVAL_QUERY(pQuery)) && (!isFixedOutputQuery(pRuntimeEnv))) {
    return TSDB_CODE_SUCCESS;
  }

  STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

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
    pRuntimeEnv->pQueryHandle = tsdbQueryLastRow(tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);

    // update the query time window
    pQuery->window = cond.twindow;
    if (pQInfo->tableGroupInfo.numOfTables == 0) {
      pQInfo->tableqinfoGroupInfo.numOfTables = 0;
    } else {
      size_t numOfGroups = GET_NUM_OF_TABLEGROUP(pQInfo);
      for(int32_t i = 0; i < numOfGroups; ++i) {
        SArray *group = GET_TABLEGROUP(pQInfo, i);

        size_t t = taosArrayGetSize(group);
        for (int32_t j = 0; j < t; ++j) {
          STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);

          pCheckInfo->win = pQuery->window;
          pCheckInfo->lastKey = pCheckInfo->win.skey;
        }
      }
    }
  } else if (isPointInterpoQuery(pQuery)) {
    pRuntimeEnv->pQueryHandle = tsdbQueryRowsInExternalWindow(tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);
  } else {
    pRuntimeEnv->pQueryHandle = tsdbQueryTables(tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);
  }

  return terrno;
}

static SFillColInfo* createFillColInfo(SQuery* pQuery) {
  int32_t numOfCols = getNumOfFinalResCol(pQuery);
  int32_t offset = 0;

  SFillColInfo* pFillCol = calloc(numOfCols, sizeof(SFillColInfo));
  if (pFillCol == NULL) {
    return NULL;
  }

  // TODO refactor
  for(int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExprInfo = (pQuery->pExpr2 == NULL)? &pQuery->pExpr1[i]:&pQuery->pExpr2[i];

    pFillCol[i].col.bytes  = pExprInfo->bytes;
    pFillCol[i].col.type   = (int8_t)pExprInfo->type;
    pFillCol[i].col.offset = offset;
    pFillCol[i].tagIndex   = -2;
    pFillCol[i].flag       = TSDB_COL_NORMAL;    // always be ta normal column for table query
    pFillCol[i].functionId = pExprInfo->base.functionId;
    pFillCol[i].fillVal.i = pQuery->fillVal[i];

    offset += pExprInfo->bytes;
  }

  return pFillCol;
}

int32_t doInitQInfo(SQInfo *pQInfo, STSBuf *pTsBuf, void *tsdb, int32_t vgId, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  pRuntimeEnv->topBotQuery = isTopBottomQuery(pQuery);
  pRuntimeEnv->hasTagResults = hasTagValOutput(pQuery);
  pRuntimeEnv->timeWindowInterpo = timeWindowInterpoRequired(pQuery);

  setScanLimitationByResultBuffer(pQuery);

  int32_t code = setupQueryHandle(tsdb, pQInfo, isSTableQuery);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pQInfo->tsdb = tsdb;
  pQInfo->vgId = vgId;

  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pTsBuf = pTsBuf;
  pRuntimeEnv->cur.vgroupIndex = -1;
  pRuntimeEnv->stableQuery = isSTableQuery;
  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->groupbyNormalCol = isGroupbyNormalCol(pQuery->pGroupbyExpr);

  if (pTsBuf != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTsBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTsBuf, order);
  }

  int32_t ps = DEFAULT_PAGE_SIZE;
  int32_t rowsize = 0;
  getIntermediateBufInfo(pRuntimeEnv, &ps, &rowsize);
  int32_t TENMB = 1024*1024*10;

  if (isSTableQuery && !onlyQueryTags(pRuntimeEnv->pQuery)) {
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rowsize, ps, TENMB, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (!QUERY_IS_INTERVAL_QUERY(pQuery)) {
      int16_t type = TSDB_DATA_TYPE_NULL;
      if (pRuntimeEnv->groupbyNormalCol) {  // group by columns not tags;
        type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
      } else {
        type = TSDB_DATA_TYPE_INT;  // group id
      }

      code = initResultRowInfo(&pRuntimeEnv->windowResInfo, 8, type);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else if (pRuntimeEnv->groupbyNormalCol || QUERY_IS_INTERVAL_QUERY(pQuery) || (!isSTableQuery)) {
    int32_t numOfResultRows = getInitialPageNum(pQInfo);
    getIntermediateBufInfo(pRuntimeEnv, &ps, &rowsize);
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rowsize, ps, TENMB, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    int16_t type = TSDB_DATA_TYPE_NULL;
    if (pRuntimeEnv->groupbyNormalCol) {
      type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
    } else {
      type = TSDB_DATA_TYPE_TIMESTAMP;
    }

    code = initResultRowInfo(&pRuntimeEnv->windowResInfo, numOfResultRows, type);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // create runtime environment
  code = setupQueryRuntimeEnv(pRuntimeEnv, pQuery->order.order);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
    SFillColInfo* pColInfo = createFillColInfo(pQuery);
    STimeWindow w = TSWINDOW_INITIALIZER;

    TSKEY sk = MIN(pQuery->window.skey, pQuery->window.ekey);
    TSKEY ek = MAX(pQuery->window.skey, pQuery->window.ekey);
    getAlignQueryTimeWindow(pQuery, pQuery->window.skey, sk, ek, &w);

    int32_t numOfCols = getNumOfFinalResCol(pQuery);
    pRuntimeEnv->pFillInfo = taosInitFillInfo(pQuery->order.order, w.skey, 0, (int32_t)pQuery->rec.capacity, numOfCols,
                                              pQuery->interval.sliding, pQuery->interval.slidingUnit, (int8_t)pQuery->precision,
                                              pQuery->fillType, pColInfo, pQInfo);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  return TSDB_CODE_SUCCESS;
}

static void enableExecutionForNextTable(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[i]);
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

    if (pRuntimeEnv->hasTagResults || pRuntimeEnv->pTsBuf != NULL) {
      setAdditionalInfo(pQInfo, pTableQueryInfo->pTable, pTableQueryInfo);
    }
  }
}

static void doTableQueryInfoTimeWindowCheck(SQuery* pQuery, STableQueryInfo* pTableQueryInfo) {
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    assert(
        (pTableQueryInfo->win.skey <= pTableQueryInfo->win.ekey) &&
        (pTableQueryInfo->lastKey >= pTableQueryInfo->win.skey) &&
        (pTableQueryInfo->win.skey >= pQuery->window.skey && pTableQueryInfo->win.ekey <= pQuery->window.ekey));
  } else {
    assert(
        (pTableQueryInfo->win.skey >= pTableQueryInfo->win.ekey) &&
        (pTableQueryInfo->lastKey <= pTableQueryInfo->win.skey) &&
        (pTableQueryInfo->win.skey <= pQuery->window.skey && pTableQueryInfo->win.ekey >= pQuery->window.ekey));
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

  while (tsdbNextDataBlock(pQueryHandle)) {
    summary->totalBlocks += 1;

    if (isQueryKilled(pQInfo)) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
    STableQueryInfo **pTableQueryInfo = (STableQueryInfo**) taosHashGet(pQInfo->tableqinfoGroupInfo.map, &blockInfo.tid, sizeof(blockInfo.tid));
    if(pTableQueryInfo == NULL) {
      break;
    }

    pQuery->current = *pTableQueryInfo;
    doTableQueryInfoTimeWindowCheck(pQuery, *pTableQueryInfo);

    if (!pRuntimeEnv->groupbyNormalCol) {
      setEnvForEachBlock(pQInfo, *pTableQueryInfo, &blockInfo);
    }

    uint32_t     status = 0;
    SDataStatis *pStatis = NULL;
    SArray      *pDataBlock = NULL;

    int32_t ret = loadDataBlockOnDemand(pRuntimeEnv, &pQuery->current->windowResInfo, pQueryHandle, &blockInfo, &pStatis, &pDataBlock, &status);
    if (ret != TSDB_CODE_SUCCESS) {
      break;
    }

    if (status == BLK_DATA_DISCARD) {
      pQuery->current->lastKey = QUERY_IS_ASC_QUERY(pQuery)? blockInfo.window.ekey + step : blockInfo.window.skey + step;
      continue;
    }

    summary->totalRows += blockInfo.rows;
    stableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, pDataBlock, binarySearchForKey);

    qDebug("QInfo:%p check data block completed, uid:%"PRId64", tid:%d, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, "
           "lastKey:%" PRId64,
           pQInfo, blockInfo.uid, blockInfo.tid, blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows,
           pQuery->current->lastKey);
  }

  if (terrno != TSDB_CODE_SUCCESS) {
    longjmp(pRuntimeEnv->env, terrno);
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

  if (pRuntimeEnv->hasTagResults || pRuntimeEnv->pTsBuf != NULL) {
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
  SArray *tx = taosArrayInit(1, sizeof(STableKeyInfo));

  STableKeyInfo info = {.pTable = pCheckInfo->pTable, .lastKey = pCheckInfo->lastKey};
  taosArrayPush(tx, &info);

  taosArrayPush(g1, &tx);
  STableGroupInfo gp = {.numOfTables = 1, .pGroupList = g1};

  // include only current table
  if (pRuntimeEnv->pQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
    pRuntimeEnv->pQueryHandle = NULL;
  }

  pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &gp, pQInfo, &pQInfo->memRef);
  taosArrayDestroy(tx);
  taosArrayDestroy(g1);
  if (pRuntimeEnv->pQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  if (pRuntimeEnv->pTsBuf != NULL) {
      tVariant* pTag = &pRuntimeEnv->pCtx[0].tag;

    if (pRuntimeEnv->cur.vgroupIndex == -1) {
      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQInfo->vgId, pTag);
      // failed to find data with the specified tag value and vnodeId
      if (!tsBufIsValidElem(&elem)) {
        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qError("QInfo:%p failed to find tag:%s in ts_comp", pQInfo, pTag->pz);
        } else {
          qError("QInfo:%p failed to find tag:%"PRId64" in ts_comp", pQInfo, pTag->i64Key);
        }

        return false;
      } else {
        STSCursor cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);

        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz,
                 cur.blockIndex, cur.tsIndex);
        } else {
          qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64Key,
                 cur.blockIndex, cur.tsIndex);
        }
      }
    } else {
      STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);
      if (tVariantCompare(elem.tag, &pRuntimeEnv->pCtx[0].tag) != 0) {

        STSElem elem1 = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQInfo->vgId, pTag);
        // failed to find data with the specified tag value and vnodeId
        if (!tsBufIsValidElem(&elem1)) {
          if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
            qError("QInfo:%p failed to find tag:%s in ts_comp", pQInfo, pTag->pz);
          } else {
            qError("QInfo:%p failed to find tag:%"PRId64" in ts_comp", pQInfo, pTag->i64Key);
          }

          return false;
        } else {
          STSCursor cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
          if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
            qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz, cur.blockIndex, cur.tsIndex);
          } else {
            qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64Key, cur.blockIndex, cur.tsIndex);
          }
        }

      } else {
        tsBufSetCursor(pRuntimeEnv->pTsBuf, &pRuntimeEnv->cur);
        STSCursor cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qDebug("QInfo:%p continue scan ts_comp file, tag:%s blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz, cur.blockIndex, cur.tsIndex);
        } else {
          qDebug("QInfo:%p continue scan ts_comp file, tag:%"PRId64" blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64Key, cur.blockIndex, cur.tsIndex);
        }
      }
    }
  }

  initCtxOutputBuf(pRuntimeEnv);
  return true;
}

STsdbQueryCond createTsdbQueryCond(SQuery* pQuery, STimeWindow* win) {
  STsdbQueryCond cond = {
      .colList   = pQuery->colList,
      .order     = pQuery->order.order,
      .numOfCols = pQuery->numOfCols,
  };

  TIME_WINDOW_COPY(cond.twindow, *win);
  return cond;
}

static STableIdInfo createTableIdInfo(SQuery* pQuery) {
  assert(pQuery != NULL && pQuery->current != NULL);

  STableIdInfo tidInfo;
  STableId* id = TSDB_TABLEID(pQuery->current->pTable);

  tidInfo.uid = id->uid;
  tidInfo.tid = id->tid;
  tidInfo.key = pQuery->current->lastKey;

  return tidInfo;
}

static void updateTableIdInfo(SQuery* pQuery, SHashObj* pTableIdInfo) {
  STableIdInfo tidInfo = createTableIdInfo(pQuery);
  STableIdInfo* idinfo = taosHashGet(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid));
  if (idinfo != NULL) {
    assert(idinfo->tid == tidInfo.tid && idinfo->uid == tidInfo.uid);
    idinfo->key = tidInfo.key;
  } else {
    taosHashPut(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid), &tidInfo, sizeof(STableIdInfo));
  }
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

  if (isPointInterpoQuery(pQuery)) {
    resetDefaultResInfoOutputBuf(pRuntimeEnv);
    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pQInfo->groupIndex < numOfGroups) {
      SArray *group = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, pQInfo->groupIndex);

      qDebug("QInfo:%p point interpolation query on group:%d, total group:%" PRIzu ", current group:%p", pQInfo,
             pQInfo->groupIndex, numOfGroups, group);
      STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

      SArray *g1 = taosArrayInit(1, POINTER_BYTES);
      SArray *tx = taosArrayClone(group);
      taosArrayPush(g1, &tx);

      STableGroupInfo gp = {.numOfTables = taosArrayGetSize(tx), .pGroupList = g1};

      // include only current table
      if (pRuntimeEnv->pQueryHandle != NULL) {
        tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
        pRuntimeEnv->pQueryHandle = NULL;
      }

      pRuntimeEnv->pQueryHandle = tsdbQueryRowsInExternalWindow(pQInfo->tsdb, &cond, &gp, pQInfo, &pQInfo->memRef);

      taosArrayDestroy(tx);
      taosArrayDestroy(g1);
      if (pRuntimeEnv->pQueryHandle == NULL) {
        longjmp(pRuntimeEnv->env, terrno);
      }

      initCtxOutputBuf(pRuntimeEnv);

      SArray *s = tsdbGetQueriedTableList(pRuntimeEnv->pQueryHandle);
      assert(taosArrayGetSize(s) >= 1);

      setTagVal(pRuntimeEnv, taosArrayGetP(s, 0), pQInfo->tsdb);
      taosArrayDestroy(s);

      // here we simply set the first table as current table
      SArray *first = GET_TABLEGROUP(pQInfo, pQInfo->groupIndex);
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
  } else if (pRuntimeEnv->groupbyNormalCol) {  // group-by on normal columns query
    while (pQInfo->groupIndex < numOfGroups) {
      SArray *group = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, pQInfo->groupIndex);

      qDebug("QInfo:%p group by normal columns group:%d, total group:%" PRIzu "", pQInfo, pQInfo->groupIndex,
             numOfGroups);

      STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

      SArray *g1 = taosArrayInit(1, POINTER_BYTES);
      SArray *tx = taosArrayClone(group);
      taosArrayPush(g1, &tx);

      STableGroupInfo gp = {.numOfTables = taosArrayGetSize(tx), .pGroupList = g1};

      // include only current table
      if (pRuntimeEnv->pQueryHandle != NULL) {
        tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
        pRuntimeEnv->pQueryHandle = NULL;
      }

      // no need to update the lastkey for each table
      pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &gp, pQInfo, &pQInfo->memRef);

      taosArrayDestroy(g1);
      taosArrayDestroy(tx);
      if (pRuntimeEnv->pQueryHandle == NULL) {
        longjmp(pRuntimeEnv->env, terrno);
      }

      SArray *s = tsdbGetQueriedTableList(pRuntimeEnv->pQueryHandle);
      assert(taosArrayGetSize(s) >= 1);

      setTagVal(pRuntimeEnv, taosArrayGetP(s, 0), pQInfo->tsdb);

      // here we simply set the first table as current table
      scanMultiTableDataBlocks(pQInfo);
      pQInfo->groupIndex += 1;

      taosArrayDestroy(s);

      // no results generated for current group, continue to try the next group
      SResultRowInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
      if (pWindowResInfo->size <= 0) {
        continue;
      }

      for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
        pWindowResInfo->pResult[i]->closed = true;  // enable return all results for group by normal columns

        SResultRow *pResult = pWindowResInfo->pResult[i];
        for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
          SResultRowCellInfo *pCell = getResultCell(pRuntimeEnv, pResult, j);
          pResult->numOfRows = (uint16_t)(MAX(pResult->numOfRows, pCell->numOfRes));
        }
      }

      qDebug("QInfo:%p generated groupby columns results %d rows for group %d completed", pQInfo, pWindowResInfo->size,
             pQInfo->groupIndex);
      int32_t currentGroupIndex = pQInfo->groupIndex;

      pQuery->rec.rows = 0;
      pQInfo->groupIndex = 0;

      ensureOutputBufferSimple(pRuntimeEnv, pWindowResInfo->size);
      copyFromWindowResToSData(pQInfo, pWindowResInfo);

      pQInfo->groupIndex = currentGroupIndex;  // restore the group index
      assert(pQuery->rec.rows == pWindowResInfo->size);
      resetResultRowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);
      break;
    }
  } else if (pRuntimeEnv->queryWindowIdentical && pRuntimeEnv->pTsBuf == NULL && !isTSCompQuery(pQuery)) {
    //super table projection query with identical query time range for all tables.
    SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
    resetDefaultResInfoOutputBuf(pRuntimeEnv);

    SArray *group = GET_TABLEGROUP(pQInfo, 0);
    assert(taosArrayGetSize(group) == pQInfo->tableqinfoGroupInfo.numOfTables &&
           1 == taosArrayGetSize(pQInfo->tableqinfoGroupInfo.pGroupList));

    void *pQueryHandle = pRuntimeEnv->pQueryHandle;
    if (pQueryHandle == NULL) {
      STsdbQueryCond con = createTsdbQueryCond(pQuery, &pQuery->window);
      pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQInfo->tsdb, &con, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);
      pQueryHandle = pRuntimeEnv->pQueryHandle;
    }

    // skip blocks without load the actual data block from file if no filter condition present
    //    skipBlocks(&pQInfo->runtimeEnv);
    //    if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0) {
    //      setQueryStatus(pQuery, QUERY_COMPLETED);
    //      return;
    //    }

    if (pQuery->prjInfo.vgroupLimit != -1) {
      assert(pQuery->limit.limit == -1 && pQuery->limit.offset == 0);
    } else if (pQuery->limit.limit != -1) {
      assert(pQuery->prjInfo.vgroupLimit == -1);
    }

    bool hasMoreBlock = true;
    int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
    SQueryCostInfo *summary = &pRuntimeEnv->summary;
    while ((hasMoreBlock = tsdbNextDataBlock(pQueryHandle)) == true) {
      summary->totalBlocks += 1;

      if (isQueryKilled(pQInfo)) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
      }

      tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
      STableQueryInfo **pTableQueryInfo =
          (STableQueryInfo **) taosHashGet(pQInfo->tableqinfoGroupInfo.map, &blockInfo.tid, sizeof(blockInfo.tid));
      if (pTableQueryInfo == NULL) {
        break;
      }

      pQuery->current = *pTableQueryInfo;
      doTableQueryInfoTimeWindowCheck(pQuery, *pTableQueryInfo);

      if (pRuntimeEnv->hasTagResults) {
        setTagVal(pRuntimeEnv, pQuery->current->pTable, pQInfo->tsdb);
      }

      if (pQuery->prjInfo.vgroupLimit > 0 && pQuery->current->windowResInfo.size > pQuery->prjInfo.vgroupLimit) {
        pQuery->current->lastKey =
                QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.window.ekey + step : blockInfo.window.skey + step;
        continue;
      }

      // it is a super table ordered projection query, check for the number of output for each vgroup
      if (pQuery->prjInfo.vgroupLimit > 0 && pQuery->rec.rows >= pQuery->prjInfo.vgroupLimit) {
        if (QUERY_IS_ASC_QUERY(pQuery) && blockInfo.window.skey >= pQuery->prjInfo.ts) {
          pQuery->current->lastKey =
                  QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.window.ekey + step : blockInfo.window.skey + step;
          continue;
        } else if (!QUERY_IS_ASC_QUERY(pQuery) && blockInfo.window.ekey <= pQuery->prjInfo.ts) {
          pQuery->current->lastKey =
                  QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.window.ekey + step : blockInfo.window.skey + step;
          continue;
        }
      }

      uint32_t     status = 0;
      SDataStatis *pStatis = NULL;
      SArray      *pDataBlock = NULL;

      int32_t ret = loadDataBlockOnDemand(pRuntimeEnv, &pQuery->current->windowResInfo, pQueryHandle, &blockInfo,
                                          &pStatis, &pDataBlock, &status);
      if (ret != TSDB_CODE_SUCCESS) {
        break;
      }

      if(status == BLK_DATA_DISCARD) {
        pQuery->current->lastKey =
                QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.window.ekey + step : blockInfo.window.skey + step;
        continue;
      }

      ensureOutputBuffer(pRuntimeEnv, &blockInfo);
      int64_t prev = getNumOfResult(pRuntimeEnv);

      pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : blockInfo.rows - 1;
      int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, binarySearchForKey, pDataBlock);

      summary->totalRows += blockInfo.rows;
      qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%" PRId64,
             GET_QINFO_ADDR(pRuntimeEnv), blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes,
             pQuery->current->lastKey);

      pQuery->rec.rows = getNumOfResult(pRuntimeEnv);

      int64_t inc = pQuery->rec.rows - prev;
      pQuery->current->windowResInfo.size += (int32_t) inc;

      // the flag may be set by tableApplyFunctionsOnBlock, clear it here
      CLEAR_QUERY_STATUS(pQuery, QUERY_COMPLETED);

      updateTableIdInfo(pQuery, pQInfo->arrTableIdInfo);

      if (pQuery->prjInfo.vgroupLimit >= 0) {
        if (((pQuery->rec.rows + pQuery->rec.total) < pQuery->prjInfo.vgroupLimit) || ((pQuery->rec.rows + pQuery->rec.total) > pQuery->prjInfo.vgroupLimit && prev < pQuery->prjInfo.vgroupLimit)) {
          if (QUERY_IS_ASC_QUERY(pQuery) && pQuery->prjInfo.ts < blockInfo.window.ekey) {
            pQuery->prjInfo.ts = blockInfo.window.ekey;
          } else if (!QUERY_IS_ASC_QUERY(pQuery) && pQuery->prjInfo.ts > blockInfo.window.skey) {
            pQuery->prjInfo.ts = blockInfo.window.skey;
          }
        }
      } else {
        // the limitation of output result is reached, set the query completed
        skipResults(pRuntimeEnv);
        if (limitResults(pRuntimeEnv)) {
          setQueryStatus(pQuery, QUERY_COMPLETED);
          SET_STABLE_QUERY_OVER(pQInfo);
          break;
        }
      }

      // while the output buffer is full or limit/offset is applied, query may be paused here
      if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL|QUERY_COMPLETED)) {
        break;
      }
    }

    if (!hasMoreBlock) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
      SET_STABLE_QUERY_OVER(pQInfo);
    }
  } else {
    /*
     * the following two cases handled here.
     * 1. ts-comp query, and 2. the super table projection query with different query time range for each table.
     * If the subgroup index is larger than 0, results generated by group by tbname,k is existed.
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

    resetDefaultResInfoOutputBuf(pRuntimeEnv);
    resetResultRowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);

    SArray *group = GET_TABLEGROUP(pQInfo, 0);
    assert(taosArrayGetSize(group) == pQInfo->tableqinfoGroupInfo.numOfTables &&
           1 == taosArrayGetSize(pQInfo->tableqinfoGroupInfo.pGroupList));

    while (pQInfo->tableIndex < pQInfo->tableqinfoGroupInfo.numOfTables) {
      if (isQueryKilled(pQInfo)) {
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
        SET_STABLE_QUERY_OVER(pQInfo);
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
        updateTableIdInfo(pQuery, pQInfo->arrTableIdInfo);

        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
          break;
        }

        if (pRuntimeEnv->pTsBuf != NULL) {
          pRuntimeEnv->cur = pRuntimeEnv->pTsBuf->cur;
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

    if (pRuntimeEnv->pTsBuf != NULL) {
      pRuntimeEnv->cur = pRuntimeEnv->pTsBuf->cur;
    }

    qDebug("QInfo %p numOfTables:%" PRIu64 ", index:%d, numOfGroups:%" PRIzu ", %" PRId64
           " points returned, total:%" PRId64 ", offset:%" PRId64,
           pQInfo, (uint64_t)pQInfo->tableqinfoGroupInfo.numOfTables, pQInfo->tableIndex, numOfGroups, pQuery->rec.rows,
           pQuery->rec.total, pQuery->limit.offset);
  }
}

static void doSaveContext(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SET_REVERSE_SCAN_FLAG(pRuntimeEnv);
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  SWITCH_ORDER(pQuery->order.order);

  if (pRuntimeEnv->pTsBuf != NULL) {
    SWITCH_ORDER(pRuntimeEnv->pTsBuf->cur.order);
  }

  STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

  // clean unused handle
  if (pRuntimeEnv->pSecQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  switchCtxOrder(pRuntimeEnv);
  disableFuncInReverseScan(pQInfo);
  setupQueryRangeForReverseScan(pQInfo);

  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQInfo->tsdb, &cond, &pQInfo->tableGroupInfo, pQInfo, &pQInfo->memRef);
  if (pRuntimeEnv->pSecQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }
}

static void doRestoreContext(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  SWITCH_ORDER(pQuery->order.order);

  if (pRuntimeEnv->pTsBuf != NULL) {
    SWITCH_ORDER(pRuntimeEnv->pTsBuf->cur.order);
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
        closeAllResultRows(&item->windowResInfo);
      }
    }
  } else {  // close results for group result
    closeAllResultRows(&pQInfo->runtimeEnv.windowResInfo);
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
  if (pQInfo->code != TSDB_CODE_SUCCESS || isQueryKilled(pQInfo)) {
    qDebug("QInfo:%p query killed or error occurred, code:%s, abort", pQInfo, tstrerror(pQInfo->code));
    longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  // close all time window results
  doCloseAllTimeWindowAfterScan(pQInfo);

  if (needReverseScan(pQuery)) {
    doSaveContext(pQInfo);

    el = scanMultiTableDataBlocks(pQInfo);
    qDebug("QInfo:%p reversed scan completed, elapsed time: %" PRId64 "ms", pQInfo, el);

    doRestoreContext(pQInfo);
  } else {
    qDebug("QInfo:%p no need to do reversed scan, query completed", pQInfo);
  }

  setQueryStatus(pQuery, QUERY_COMPLETED);

  if (pQInfo->code != TSDB_CODE_SUCCESS || isQueryKilled(pQInfo)) {
    qDebug("QInfo:%p query killed or error occurred, code:%s, abort", pQInfo, tstrerror(pQInfo->code));
    //TODO finalizeQueryResult may cause SEGSEV, since the memory may not allocated yet, add a cleanup function instead
    longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  if (QUERY_IS_INTERVAL_QUERY(pQuery) || isSumAvgRateQuery(pQuery)) {
    if (mergeGroupResult(pQInfo) == TSDB_CODE_SUCCESS) {
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


static char *getArithemicInputSrc(void *param, const char *name, int32_t colId) {
  SArithmeticSupport *pSupport = (SArithmeticSupport *) param;
  SExprInfo* pExprInfo = (SExprInfo*) pSupport->exprList;

  int32_t index = -1;
  for (int32_t i = 0; i < pSupport->numOfCols; ++i) {
    if (colId == pExprInfo[i].base.resColId) {
      index = i;
      break;
    }
  }

  assert(index >= 0 && index < pSupport->numOfCols);
  return pSupport->data[index] + pSupport->offset * pExprInfo[index].bytes;
}

static void doSecondaryArithmeticProcess(SQuery* pQuery) {
  if (pQuery->numOfExpr2 == 0) {
    return;
  }

  SArithmeticSupport arithSup = {0};
  tFilePage **data = calloc(pQuery->numOfExpr2, POINTER_BYTES);
  for (int32_t i = 0; i < pQuery->numOfExpr2; ++i) {
    int32_t bytes = pQuery->pExpr2[i].bytes;
    data[i] = (tFilePage *)malloc((size_t)(bytes * pQuery->rec.rows) + sizeof(tFilePage));
  }

  arithSup.offset = 0;
  arithSup.numOfCols = (int32_t)pQuery->numOfOutput;
  arithSup.exprList  = pQuery->pExpr1;
  arithSup.data      = calloc(arithSup.numOfCols, POINTER_BYTES);

  for (int32_t k = 0; k < arithSup.numOfCols; ++k) {
    arithSup.data[k] = pQuery->sdata[k]->data;
  }

  for (int i = 0; i < pQuery->numOfExpr2; ++i) {
    SExprInfo *pExpr = &pQuery->pExpr2[i];

    // calculate the result from several other columns
    SSqlFuncMsg* pSqlFunc = &pExpr->base;
    if (pSqlFunc->functionId != TSDB_FUNC_ARITHM) {

      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        if (pSqlFunc->functionId == pQuery->pExpr1[j].base.functionId &&
            pSqlFunc->colInfo.colId == pQuery->pExpr1[j].base.colInfo.colId) {
          memcpy(data[i]->data, pQuery->sdata[j]->data, (size_t)(pQuery->pExpr1[j].bytes * pQuery->rec.rows));
          break;
        }
      }
    } else {
      arithSup.pArithExpr = pExpr;
      tExprTreeCalcTraverse(arithSup.pArithExpr->pExpr, (int32_t)pQuery->rec.rows, data[i]->data, &arithSup, TSDB_ORDER_ASC,
                            getArithemicInputSrc);
    }
  }

  for (int32_t i = 0; i < pQuery->numOfExpr2; ++i) {
    memcpy(pQuery->sdata[i]->data, data[i]->data, (size_t)(pQuery->pExpr2[i].bytes * pQuery->rec.rows));
  }

  for (int32_t i = 0; i < pQuery->numOfExpr2; ++i) {
    tfree(data[i]);
  }

  tfree(data);
  tfree(arithSup.data);
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

  // since the numOfRows must be identical for all sql functions that are allowed to be executed simutaneously.
  pQuery->rec.rows = getNumOfResult(pRuntimeEnv);
  doSecondaryArithmeticProcess(pQuery);

  if (isQueryKilled(pQInfo)) {
    longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  skipResults(pRuntimeEnv);
  limitResults(pRuntimeEnv);
}

static void tableMultiOutputProcess(SQInfo *pQInfo, STableQueryInfo* pTableInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->current = pTableInfo;

  // for ts_comp query, re-initialized is not allowed
  if (!isTSCompQuery(pQuery)) {
    resetDefaultResInfoOutputBuf(pRuntimeEnv);
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

    resetDefaultResInfoOutputBuf(pRuntimeEnv);
  }

  limitResults(pRuntimeEnv);
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_RESBUF_FULL)) {
    qDebug("QInfo:%p query paused due to output limitation, next qrange:%" PRId64 "-%" PRId64, pQInfo,
        pQuery->current->lastKey, pQuery->window.ekey);
  } else if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    STableIdInfo tidInfo = createTableIdInfo(pQuery);
    taosHashPut(pQInfo->arrTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid), &tidInfo, sizeof(STableIdInfo));
  }

  if (!isTSCompQuery(pQuery)) {
    assert(pQuery->rec.rows <= pQuery->rec.capacity);
  }
}

// handle time interval query on table
static void tableIntervalProcess(SQInfo *pQInfo, STableQueryInfo* pTableInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &(pQInfo->runtimeEnv);

  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->current = pTableInfo;

  TSKEY newStartKey = TSKEY_INITIAL_VAL;

  // skip blocks without load the actual data block from file if no filter condition present
  if (!pRuntimeEnv->groupbyNormalCol) {
    skipTimeInterval(pRuntimeEnv, &newStartKey);
    if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0 && pRuntimeEnv->pFillInfo == NULL) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return;
    }
  }

  scanOneTableDataBlocks(pRuntimeEnv, newStartKey);
  assert(!Q_STATUS_EQUAL(pQuery->status, QUERY_NOT_COMPLETED));

  finalizeQueryResult(pRuntimeEnv);

  // skip offset result rows
  pQuery->rec.rows = 0;

  if (pQuery->fillType == TSDB_FILL_NONE) {
    // all data scanned, the group by normal column can return
    int32_t numOfClosed = numOfClosedResultRows(&pRuntimeEnv->windowResInfo);
    if (pQuery->limit.offset > numOfClosed) {
      return;
    }

    pQInfo->groupIndex = (int32_t) pQuery->limit.offset;

    copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
    doSecondaryArithmeticProcess(pQuery);

    limitResults(pRuntimeEnv);
  } else {

    copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
    doSecondaryArithmeticProcess(pQuery);

    taosFillSetStartInfo(pRuntimeEnv->pFillInfo, (int32_t)pQuery->rec.rows, pQuery->window.ekey);
    taosFillCopyInputDataFromFilePage(pRuntimeEnv->pFillInfo, (const tFilePage **)pQuery->sdata);

    int32_t numOfFilled = 0;
    pQuery->rec.rows = doFillGapsInResults(pRuntimeEnv, (tFilePage **)pQuery->sdata, &numOfFilled);

    if (pQuery->rec.rows > 0 || Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      limitResults(pRuntimeEnv);
    }
  }
}

static void tableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (queryHasRemainResForTableQuery(pRuntimeEnv)) {
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
      assert(pRuntimeEnv->windowResInfo.size > 0);

      if (pQInfo->groupIndex < pRuntimeEnv->windowResInfo.size) {
        copyFromWindowResToSData(pQInfo, &pRuntimeEnv->windowResInfo);
      }

      if (pQuery->rec.rows > 0) {
        qDebug("QInfo:%p %" PRId64 " rows returned from group results, total:%" PRId64 "", pQInfo, pQuery->rec.rows,
               pQuery->rec.total);
      }

      // there are not data remains
      if (pQuery->rec.rows <= 0 || pRuntimeEnv->windowResInfo.size <= pQInfo->groupIndex) {
        qDebug("QInfo:%p query over, %" PRId64 " rows are returned", pQInfo, pQuery->rec.total);
      }

      return;
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
      (isFixedOutputQuery(pRuntimeEnv) && (!isPointInterpoQuery(pQuery)) && (!pRuntimeEnv->groupbyNormalCol))) {
    multiTableQueryProcess(pQInfo);
  } else {
    assert((pQuery->checkBuffer == 1 && pQuery->interval.interval == 0) || isPointInterpoQuery(pQuery) ||
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
      return TSDB_TBNAME_COLUMN_INDEX;
    }

    while(j < pQueryMsg->numOfTags) {
      if (pExprMsg->colInfo.colId == pTagCols[j].colId) {
        return j;
      }

      j += 1;
    }

  } else if (TSDB_COL_IS_UD_COL(pExprMsg->colInfo.flag)) {  // user specified column data
    return TSDB_UD_COLUMN_INDEX;
  } else {
    while (j < pQueryMsg->numOfCols) {
      if (pExprMsg->colInfo.colId == pQueryMsg->colList[j].colId) {
        return j;
      }

      j += 1;
    }
  }
  assert(0);
  return -1;
}

bool validateExprColumnInfo(SQueryTableMsg *pQueryMsg, SSqlFuncMsg *pExprMsg, SColumnInfo* pTagCols) {
  int32_t j = getColumnIndexInSource(pQueryMsg, pExprMsg, pTagCols);
  return j < pQueryMsg->numOfCols || j < pQueryMsg->numOfTags;
}

static bool validateQueryMsg(SQueryTableMsg *pQueryMsg) {
  if (pQueryMsg->interval.interval < 0) {
    qError("qmsg:%p illegal value of interval time %" PRId64, pQueryMsg, pQueryMsg->interval.interval);
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
static int32_t convertQueryMsg(SQueryTableMsg *pQueryMsg, SArray **pTableIdList, SSqlFuncMsg ***pExpr, SSqlFuncMsg ***pSecStageExpr,
                               char **tagCond, char** tbnameCond, SColIndex **groupbyCols, SColumnInfo** tagCols) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (taosCheckVersion(pQueryMsg->version, version, 3) != 0) {
    return TSDB_CODE_QRY_INVALID_MSG;
  }

  pQueryMsg->numOfTables = htonl(pQueryMsg->numOfTables);

  pQueryMsg->window.skey = htobe64(pQueryMsg->window.skey);
  pQueryMsg->window.ekey = htobe64(pQueryMsg->window.ekey);
  pQueryMsg->interval.interval = htobe64(pQueryMsg->interval.interval);
  pQueryMsg->interval.sliding = htobe64(pQueryMsg->interval.sliding);
  pQueryMsg->interval.offset = htobe64(pQueryMsg->interval.offset);
  pQueryMsg->limit = htobe64(pQueryMsg->limit);
  pQueryMsg->offset = htobe64(pQueryMsg->offset);
  pQueryMsg->vgroupLimit = htobe64(pQueryMsg->vgroupLimit);

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
  pQueryMsg->secondStageOutput = htonl(pQueryMsg->secondStageOutput);

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
      if (pColInfo->filters == NULL) {
        code = TSDB_CODE_QRY_OUT_OF_MEMORY;
        goto _cleanup;
      }
    }

    for (int32_t f = 0; f < numOfFilters; ++f) {
      SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)pMsg;

      SColumnFilterInfo *pColFilter = &pColInfo->filters[f];
      pColFilter->filterstr = htons(pFilterMsg->filterstr);

      pMsg += sizeof(SColumnFilterInfo);

      if (pColFilter->filterstr) {
        pColFilter->len = htobe64(pFilterMsg->len);

        pColFilter->pz = (int64_t)calloc(1, (size_t)(pColFilter->len + 1 * TSDB_NCHAR_SIZE)); // note: null-terminator
        if (pColFilter->pz == 0) {
          code = TSDB_CODE_QRY_OUT_OF_MEMORY;
          goto _cleanup;
        }

        memcpy((void *)pColFilter->pz, pMsg, (size_t)pColFilter->len);
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
  if (*pExpr == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _cleanup;
  }

  SSqlFuncMsg *pExprMsg = (SSqlFuncMsg *)pMsg;

  for (int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
    (*pExpr)[i] = pExprMsg;

    pExprMsg->colInfo.colIndex = htons(pExprMsg->colInfo.colIndex);
    pExprMsg->colInfo.colId = htons(pExprMsg->colInfo.colId);
    pExprMsg->colInfo.flag  = htons(pExprMsg->colInfo.flag);
    pExprMsg->functionId    = htons(pExprMsg->functionId);
    pExprMsg->numOfParams   = htons(pExprMsg->numOfParams);
    pExprMsg->resColId      = htons(pExprMsg->resColId);

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
      if (!TSDB_COL_IS_TAG(pExprMsg->colInfo.flag)) {  // ignore the column  index check for arithmetic expression.
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

  if (pQueryMsg->secondStageOutput) {
    pExprMsg = (SSqlFuncMsg *)pMsg;
    *pSecStageExpr = calloc(pQueryMsg->secondStageOutput, POINTER_BYTES);
    
    for (int32_t i = 0; i < pQueryMsg->secondStageOutput; ++i) {
      (*pSecStageExpr)[i] = pExprMsg;

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
        if (!TSDB_COL_IS_TAG(pExprMsg->colInfo.flag)) {  // ignore the column  index check for arithmetic expression.
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
  }

  pMsg = createTableIdList(pQueryMsg, pMsg, pTableIdList);

  if (pQueryMsg->numOfGroupCols > 0) {  // group by tag columns
    *groupbyCols = malloc(pQueryMsg->numOfGroupCols * sizeof(SColIndex));
    if (*groupbyCols == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    for (int32_t i = 0; i < pQueryMsg->numOfGroupCols; ++i) {
      (*groupbyCols)[i].colId = htons(*(int16_t *)pMsg);
      pMsg += sizeof((*groupbyCols)[i].colId);

      (*groupbyCols)[i].colIndex = htons(*(int16_t *)pMsg);
      pMsg += sizeof((*groupbyCols)[i].colIndex);

      (*groupbyCols)[i].flag = htons(*(int16_t *)pMsg);
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
    if (*tagCols == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

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

    if (*tagCond == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;

    }
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
         pQueryMsg->order, pQueryMsg->numOfOutput, pQueryMsg->numOfCols, pQueryMsg->interval.interval,
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

static int32_t buildArithmeticExprFromMsg(SExprInfo *pArithExprInfo, SQueryTableMsg *pQueryMsg) {
  qDebug("qmsg:%p create arithmetic expr from binary", pQueryMsg);

  tExprNode* pExprNode = NULL;
  TRY(TSDB_MAX_TAG_CONDITIONS) {
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

static int32_t createQueryFuncExprFromMsg(SQueryTableMsg *pQueryMsg, int32_t numOfOutput, SExprInfo **pExprInfo, SSqlFuncMsg **pExprMsg,
    SColumnInfo* pTagCols) {
  *pExprInfo = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SExprInfo *pExprs = (SExprInfo *)calloc(pQueryMsg->numOfOutput, sizeof(SExprInfo));
  if (pExprs == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  bool    isSuperTable = QUERY_IS_STABLE_QUERY(pQueryMsg->queryType);
  int16_t tagLen = 0;

  for (int32_t i = 0; i < numOfOutput; ++i) {
    pExprs[i].base = *pExprMsg[i];
    pExprs[i].bytes = 0;

    int16_t type = 0;
    int16_t bytes = 0;

    // parse the arithmetic expression
    if (pExprs[i].base.functionId == TSDB_FUNC_ARITHM) {
      code = buildArithmeticExprFromMsg(&pExprs[i], pQueryMsg);

      if (code != TSDB_CODE_SUCCESS) {
        tfree(pExprs);
        return code;
      }

      type  = TSDB_DATA_TYPE_DOUBLE;
      bytes = tDataTypeDesc[type].nSize;
    } else if (pExprs[i].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX && pExprs[i].base.functionId == TSDB_FUNC_TAGPRJ) {  // parse the normal column
      SSchema s = tGetTableNameColumnSchema();
      type = s.type;
      bytes = s.bytes;
    } else if (pExprs[i].base.colInfo.colId <= TSDB_UD_COLUMN_INDEX) {
      // it is a user-defined constant value column
      assert(pExprs[i].base.functionId == TSDB_FUNC_PRJ);

      type = pExprs[i].base.arg[1].argType;
      bytes = pExprs[i].base.arg[1].argBytes;

      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
        bytes += VARSTR_HEADER_SIZE;
      }
    } else {
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

    int32_t param = (int32_t)pExprs[i].base.arg[0].argValue.i64;
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
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pExprs[i].base = *pExprMsg[i];
    int16_t functId = pExprs[i].base.functionId;

    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].base, pTagCols);
      if (j < 0 || j >= pQueryMsg->numOfCols) {
        assert(0);
      } else {
        SColumnInfo *pCol = &pQueryMsg->colList[j];
        int32_t ret =
            getResultDataInfo(pCol->type, pCol->bytes, functId, (int32_t)pExprs[i].base.arg[0].argValue.i64,
                              &pExprs[i].type, &pExprs[i].bytes, &pExprs[i].interBytes, tagLen, isSuperTable);
        assert(ret == TSDB_CODE_SUCCESS);
      }
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
  if (pQuery->pFilterInfo == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0, j = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].numOfFilters > 0) {
      SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[j];

      memcpy(&pFilterInfo->info, &pQuery->colList[i], sizeof(SColumnInfo));
      pFilterInfo->info = pQuery->colList[i];

      pFilterInfo->numOfFilters = pQuery->colList[i].numOfFilters;
      pFilterInfo->pFilters = calloc(pFilterInfo->numOfFilters, sizeof(SColumnFilterElem));
      if (pFilterInfo->pFilters == NULL) {
        return TSDB_CODE_QRY_OUT_OF_MEMORY;
      }

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
  assert(pQuery->pExpr1 != NULL && pQuery != NULL);

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    SSqlFuncMsg *pSqlExprMsg = &pQuery->pExpr1[k].base;
    if (pSqlExprMsg->functionId == TSDB_FUNC_ARITHM) {
      continue;
    }

    // todo opt performance
    SColIndex *pColIndex = &pSqlExprMsg->colInfo;
    if (TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
      int32_t f = 0;
      for (f = 0; f < pQuery->numOfCols; ++f) {
        if (pColIndex->colId == pQuery->colList[f].colId) {
          pColIndex->colIndex = f;
          break;
        }
      }

      assert(f < pQuery->numOfCols);
    } else if (pColIndex->colId <= TSDB_UD_COLUMN_INDEX) {
      // do nothing for user-defined constant value result columns
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

static void freeQInfo(SQInfo *pQInfo);

static void calResultBufSize(SQuery* pQuery) {
  const int32_t RESULT_MSG_MIN_SIZE  = 1024 * (1024 + 512);  // bytes
  const int32_t RESULT_MSG_MIN_ROWS  = 8192;
  const float RESULT_THRESHOLD_RATIO = 0.85f;

  if (isProjQuery(pQuery)) {
    int32_t numOfRes = RESULT_MSG_MIN_SIZE / pQuery->rowSize;
    if (numOfRes < RESULT_MSG_MIN_ROWS) {
      numOfRes = RESULT_MSG_MIN_ROWS;
    }

    pQuery->rec.capacity  = numOfRes;
    pQuery->rec.threshold = (int32_t)(numOfRes * RESULT_THRESHOLD_RATIO);
  } else {  // in case of non-prj query, a smaller output buffer will be used.
    pQuery->rec.capacity = 4096;
    pQuery->rec.threshold = (int32_t)(pQuery->rec.capacity * RESULT_THRESHOLD_RATIO);
  }
}

static SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SSqlGroupbyExpr *pGroupbyExpr, SExprInfo *pExprs,
                               SExprInfo *pSecExprs, STableGroupInfo *pTableGroupInfo, SColumnInfo* pTagCols, bool stableQuery) {
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
  pQuery->pExpr1          = pExprs;
  pQuery->pExpr2          = pSecExprs;
  pQuery->numOfExpr2      = pQueryMsg->secondStageOutput;
  pQuery->pGroupbyExpr    = pGroupbyExpr;
  memcpy(&pQuery->interval, &pQueryMsg->interval, sizeof(pQuery->interval));
  pQuery->fillType        = pQueryMsg->fillType;
  pQuery->numOfTags       = pQueryMsg->numOfTags;
  pQuery->tagColList      = pTagCols;
  pQuery->prjInfo.vgroupLimit = pQueryMsg->vgroupLimit;
  pQuery->prjInfo.ts      = (pQueryMsg->order == TSDB_ORDER_ASC)? INT64_MIN:INT64_MAX;

  pQuery->colList = calloc(numOfCols, sizeof(SSingleColumnFilterInfo));
  if (pQuery->colList == NULL) {
    goto _cleanup;
  }

  int32_t srcSize = 0;
  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i] = pQueryMsg->colList[i];
    pQuery->colList[i].filters = tscFilterInfoClone(pQueryMsg->colList[i].filters, pQuery->colList[i].numOfFilters);
    srcSize += pQuery->colList[i].bytes;
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
    // allocate additional memory for interResults that are usually larger then final results
    // TODO refactor
    int16_t bytes = 0;
    if (pQuery->pExpr2 == NULL || col > pQuery->numOfExpr2) {
      bytes = pExprs[col].bytes;
    } else {
      bytes = MAX(pQuery->pExpr2[col].bytes, pExprs[col].bytes);
    }

    size_t size = (size_t)((pQuery->rec.capacity + 1) * bytes + pExprs[col].interBytes + sizeof(tFilePage));
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
                                                   taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  }

  pQInfo->runtimeEnv.interBufSize = getOutputInterResultBufSize(pQuery);
  pQInfo->runtimeEnv.summary.tableInfoSize += (pTableGroupInfo->numOfTables * sizeof(STableQueryInfo));

  pQInfo->runtimeEnv.pResultRowHashTable = taosHashInit(pTableGroupInfo->numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pQInfo->runtimeEnv.keyBuf = malloc(TSDB_MAX_BYTES_PER_ROW);
  pQInfo->runtimeEnv.pool = initResultRowPool(getResultRowSize(&pQInfo->runtimeEnv));
  pQInfo->runtimeEnv.prevRow = malloc(POINTER_BYTES * pQuery->numOfCols + srcSize);

  char* start = POINTER_BYTES * pQuery->numOfCols + (char*) pQInfo->runtimeEnv.prevRow;
  pQInfo->runtimeEnv.prevRow[0] = start;

  for(int32_t i = 1; i < pQuery->numOfCols; ++i) {
    pQInfo->runtimeEnv.prevRow[i] = pQInfo->runtimeEnv.prevRow[i - 1] + pQuery->colList[i-1].bytes;
  }

  pQInfo->pBuf = calloc(pTableGroupInfo->numOfTables, sizeof(STableQueryInfo));
  if (pQInfo->pBuf == NULL) {
    goto _cleanup;
  }

  // NOTE: pTableCheckInfo need to update the query time range and the lastKey info
  pQInfo->arrTableIdInfo = taosHashInit(pTableGroupInfo->numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  pQInfo->dataReady = QUERY_RESULT_NOT_READY;
  pQInfo->rspContext = NULL;
  pthread_mutex_init(&pQInfo->lock, NULL);
  tsem_init(&pQInfo->ready, 0, 0);

  pQuery->pos = -1;
  pQuery->window = pQueryMsg->window;
  changeExecuteScanOrder(pQInfo, pQueryMsg, stableQuery);

  pQInfo->runtimeEnv.queryWindowIdentical = true;
  STimeWindow window = pQuery->window;

  int32_t index = 0;
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray* pa = taosArrayGetP(pQInfo->tableGroupInfo.pGroupList, i);

    size_t s = taosArrayGetSize(pa);
    SArray* p1 = taosArrayInit(s, POINTER_BYTES);
    if (p1 == NULL) {
      goto _cleanup;
    }

    taosArrayPush(pQInfo->tableqinfoGroupInfo.pGroupList, &p1);

    for(int32_t j = 0; j < s; ++j) {
      STableKeyInfo* info = taosArrayGet(pa, j);

      window.skey = info->lastKey;
      if (info->lastKey != pQuery->window.skey) {
        pQInfo->runtimeEnv.queryWindowIdentical = false;
      }

      void* buf = (char*) pQInfo->pBuf + index * sizeof(STableQueryInfo);
      STableQueryInfo* item = createTableQueryInfo(&pQInfo->runtimeEnv, info->pTable, window, buf);
      if (item == NULL) {
        goto _cleanup;
      }

      item->groupIndex = i;
      taosArrayPush(p1, &item);

      STableId* id = TSDB_TABLEID(info->pTable);
      taosHashPut(pQInfo->tableqinfoGroupInfo.map, &id->tid, sizeof(id->tid), &item, POINTER_BYTES);
      index += 1;
    }
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

static int32_t initQInfo(SQueryTableMsg *pQueryMsg, void *tsdb, int32_t vgId, SQInfo *pQInfo, bool isSTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  STSBuf *pTSBuf = NULL;
  if (pQueryMsg->tsLen > 0) { // open new file to save the result
    char *tsBlock = (char *) pQueryMsg + pQueryMsg->tsOffset;
    pTSBuf = tsBufCreateFromCompBlocks(tsBlock, pQueryMsg->tsNumOfBlocks, pQueryMsg->tsLen, pQueryMsg->tsOrder, vgId);

    tsBufResetPos(pTSBuf);
    bool ret = tsBufNextPos(pTSBuf);

    UNUSED(ret);
  }
  
  pQuery->precision = tsdbGetCfg(tsdb)->precision;

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.skey > pQuery->window.ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.ekey > pQuery->window.skey))) {
    qDebug("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->window.skey,
           pQuery->window.ekey, pQuery->order.order);
    setQueryStatus(pQuery, QUERY_COMPLETED);
    pQInfo->tableqinfoGroupInfo.numOfTables = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (pQInfo->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:%p no table qualified for tag filter, abort query", pQInfo);
    setQueryStatus(pQuery, QUERY_COMPLETED);
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
    if (pFilter == NULL || numOfFilters == 0) {
      return;
    }

    for (int32_t i = 0; i < numOfFilters; i++) {
      if (pFilter[i].filterstr) {
        free((void*)(pFilter[i].pz));
      }
    }

    free(pFilter);
}

static void doDestroyTableQueryInfo(STableGroupInfo* pTableqinfoGroupInfo) {
  if (pTableqinfoGroupInfo->pGroupList != NULL) {
    int32_t numOfGroups = (int32_t) taosArrayGetSize(pTableqinfoGroupInfo->pGroupList);
    for (int32_t i = 0; i < numOfGroups; ++i) {
      SArray *p = taosArrayGetP(pTableqinfoGroupInfo->pGroupList, i);

      size_t num = taosArrayGetSize(p);
      for(int32_t j = 0; j < num; ++j) {
        STableQueryInfo* item = taosArrayGetP(p, j);
        destroyTableQueryInfoImpl(item);
      }

      taosArrayDestroy(p);
    }
  }

  taosArrayDestroy(pTableqinfoGroupInfo->pGroupList);
  taosHashCleanup(pTableqinfoGroupInfo->map);

  pTableqinfoGroupInfo->pGroupList = NULL;
  pTableqinfoGroupInfo->map = NULL;
  pTableqinfoGroupInfo->numOfTables = 0;
}

static void* destroyQueryFuncExpr(SExprInfo* pExprInfo, int32_t numOfExpr) {
  if (pExprInfo == NULL) {
    assert(numOfExpr == 0);
    return NULL;
  }

  for (int32_t i = 0; i < numOfExpr; ++i) {
    if (pExprInfo[i].pExpr != NULL) {
      tExprNodeDestroy(pExprInfo[i].pExpr, NULL);
    }
  }

  tfree(pExprInfo);
  return NULL;
}

static void freeQInfo(SQInfo *pQInfo) {
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  qDebug("QInfo:%p start to free QInfo", pQInfo);

  releaseQueryBuf(pQInfo->tableqinfoGroupInfo.numOfTables);

  teardownQueryRuntimeEnv(&pQInfo->runtimeEnv);

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  if (pQuery != NULL) {
    if (pQuery->sdata != NULL) {
      for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
        tfree(pQuery->sdata[col]);
      }
      tfree(pQuery->sdata);
    }

    if (pQuery->fillVal != NULL) {
      tfree(pQuery->fillVal);
    }

    for (int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
      SSingleColumnFilterInfo *pColFilter = &pQuery->pFilterInfo[i];
      if (pColFilter->numOfFilters > 0) {
        tfree(pColFilter->pFilters);
      }
    }

    pQuery->pExpr1 = destroyQueryFuncExpr(pQuery->pExpr1, pQuery->numOfOutput);
    pQuery->pExpr2 = destroyQueryFuncExpr(pQuery->pExpr2, pQuery->numOfExpr2);

    tfree(pQuery->tagColList);
    tfree(pQuery->pFilterInfo);

    if (pQuery->colList != NULL) {
      for (int32_t i = 0; i < pQuery->numOfCols; i++) {
        SColumnInfo *column = pQuery->colList + i;
        freeColumnFilterInfo(column->filters, column->numOfFilters);
      }
      tfree(pQuery->colList);
    }

    if (pQuery->pGroupbyExpr != NULL) {
      taosArrayDestroy(pQuery->pGroupbyExpr->columnInfo);
      tfree(pQuery->pGroupbyExpr);
    }

    tfree(pQuery);
  }

  doDestroyTableQueryInfo(&pQInfo->tableqinfoGroupInfo);

  tfree(pQInfo->pBuf);
  tsdbDestroyTableGroup(&pQInfo->tableGroupInfo);
  taosHashCleanup(pQInfo->arrTableIdInfo);

  taosArrayDestroy(pQInfo->groupResInfo.pRows);

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
    return (size_t)(pQuery->rowSize * (*numOfRows));
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
      uint64_t s = lseek(fd, 0, SEEK_END);

      qDebug("QInfo:%p ts comp data return, file:%s, size:%"PRId64, pQInfo, pQuery->sdata[0]->data, s);
      if (lseek(fd, 0, SEEK_SET) >= 0) {
        size_t sz = read(fd, data, (uint32_t) s);
        if(sz < s) {  // todo handle error
          assert(0);
        }
      } else {
        UNUSED(s);
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
    doCopyQueryResultToMsg(pQInfo, (int32_t)pQuery->rec.rows, data);
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

int32_t qCreateQueryInfo(void* tsdb, int32_t vgId, SQueryTableMsg* pQueryMsg, qinfo_t* pQInfo) {
  assert(pQueryMsg != NULL && tsdb != NULL);

  int32_t code = TSDB_CODE_SUCCESS;

  char            *tagCond      = NULL;
  char            *tbnameCond   = NULL;
  SArray          *pTableIdList = NULL;
  SSqlFuncMsg    **pExprMsg     = NULL;
  SSqlFuncMsg    **pSecExprMsg  = NULL;
  SExprInfo       *pExprs       = NULL;
  SExprInfo       *pSecExprs    = NULL;

  SColIndex       *pGroupColIndex = NULL;
  SColumnInfo     *pTagColumnInfo = NULL;
  SSqlGroupbyExpr *pGroupbyExpr   = NULL;

  code = convertQueryMsg(pQueryMsg, &pTableIdList, &pExprMsg, &pSecExprMsg, &tagCond, &tbnameCond, &pGroupColIndex, &pTagColumnInfo);
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

  if ((code = createQueryFuncExprFromMsg(pQueryMsg, pQueryMsg->numOfOutput, &pExprs, pExprMsg, pTagColumnInfo)) != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  if (pSecExprMsg != NULL) {
    if ((code = createQueryFuncExprFromMsg(pQueryMsg, pQueryMsg->secondStageOutput, &pSecExprs, pSecExprMsg, pTagColumnInfo)) != TSDB_CODE_SUCCESS) {
      goto _over;
    }
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
    if ((code = tsdbGetOneTableGroup(tsdb, id->uid, pQueryMsg->window.skey, &tableGroupInfo)) != TSDB_CODE_SUCCESS) {
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
      code = tsdbQuerySTableByTagCond(tsdb, id->uid, pQueryMsg->window.skey, tagCond, pQueryMsg->tagCondLen,
          pQueryMsg->tagNameRelType, tbnameCond, &tableGroupInfo, pGroupColIndex, numOfGroupByCols);

      if (code != TSDB_CODE_SUCCESS) {
        qError("qmsg:%p failed to query stable, reason: %s", pQueryMsg, tstrerror(code));
        goto _over;
      }
    } else {
      code = tsdbGetTableGroupFromIdList(tsdb, pTableIdList, &tableGroupInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _over;
      }

      qDebug("qmsg:%p query on %" PRIzu " tables in one group from client", pQueryMsg, tableGroupInfo.numOfTables);
    }

    int64_t el = taosGetTimestampUs() - st;
    qDebug("qmsg:%p tag filter completed, numOfTables:%" PRIzu ", elapsed time:%"PRId64"us", pQueryMsg, tableGroupInfo.numOfTables, el);
  } else {
    assert(0);
  }

  code = checkForQueryBuf(tableGroupInfo.numOfTables);
  if (code != TSDB_CODE_SUCCESS) {  // not enough query buffer, abort
    goto _over;
  }

  (*pQInfo) = createQInfoImpl(pQueryMsg, pGroupbyExpr, pExprs, pSecExprs, &tableGroupInfo, pTagColumnInfo, isSTableQuery);

  pExprs = NULL;
  pSecExprs = NULL;
  pGroupbyExpr = NULL;
  pTagColumnInfo = NULL;

  if ((*pQInfo) == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _over;
  }

  code = initQInfo(pQueryMsg, tsdb, vgId, *pQInfo, isSTableQuery);

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
  free(pSecExprs);

  free(pExprMsg);
  free(pSecExprMsg);

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

static bool doBuildResCheck(SQInfo* pQInfo) {
  bool buildRes = false;

  pthread_mutex_lock(&pQInfo->lock);

  pQInfo->dataReady = QUERY_RESULT_READY;
  buildRes = needBuildResAfterQueryComplete(pQInfo);

  // clear qhandle owner, it must be in the secure area. other thread may run ahead before current, after it is
  // put into task to be executed.
  assert(pQInfo->owner == taosGetSelfPthreadId());
  pQInfo->owner = 0;

  pthread_mutex_unlock(&pQInfo->lock);

  // used in retrieve blocking model.
  tsem_post(&pQInfo->ready);
  return buildRes;
}

bool qTableQuery(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;
  assert(pQInfo && pQInfo->signature == pQInfo);
  int64_t threadId = taosGetSelfPthreadId();

  int64_t curOwner = 0;
  if ((curOwner = atomic_val_compare_exchange_64(&pQInfo->owner, 0, threadId)) != 0) {
    qError("QInfo:%p qhandle is now executed by thread:%p", pQInfo, (void*) curOwner);
    pQInfo->code = TSDB_CODE_QRY_IN_EXEC;
    return false;
  }

  pQInfo->startExecTs = taosGetTimestampSec();

  if (isQueryKilled(pQInfo)) {
    qDebug("QInfo:%p it is already killed, abort", pQInfo);
    return doBuildResCheck(pQInfo);
  }

  if (pQInfo->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:%p no table exists for query, abort", pQInfo);
    setQueryStatus(pQInfo->runtimeEnv.pQuery, QUERY_COMPLETED);
    return doBuildResCheck(pQInfo);
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pQInfo->runtimeEnv.env);
  if (ret != TSDB_CODE_SUCCESS) {
    pQInfo->code = ret;
    qDebug("QInfo:%p query abort due to error/cancel occurs, code:%s", pQInfo, tstrerror(pQInfo->code));
    return doBuildResCheck(pQInfo);
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
  if (isQueryKilled(pQInfo)) {
    qDebug("QInfo:%p query is killed", pQInfo);
  } else if (pQuery->rec.rows == 0) {
    qDebug("QInfo:%p over, %" PRIzu " tables queried, %"PRId64" rows are returned", pQInfo, pQInfo->tableqinfoGroupInfo.numOfTables, pQuery->rec.total);
  } else {
    qDebug("QInfo:%p query paused, %" PRId64 " rows returned, numOfTotal:%" PRId64 " rows",
           pQInfo, pQuery->rec.rows, pQuery->rec.total + pQuery->rec.rows);
  }

  return doBuildResCheck(pQInfo);
}

int32_t qRetrieveQueryResultInfo(qinfo_t qinfo, bool* buildRes, void* pRspContext) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    qError("QInfo:%p invalid qhandle", pQInfo);
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  *buildRes = false;
  if (IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:%p query is killed, code:0x%08x", pQInfo, pQInfo->code);
    return pQInfo->code;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  if (tsRetrieveBlockingModel) {
    pQInfo->rspContext = pRspContext;
    tsem_wait(&pQInfo->ready);
    *buildRes = true;
    code = pQInfo->code;
  } else {
    SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

    pthread_mutex_lock(&pQInfo->lock);

    assert(pQInfo->rspContext == NULL);
    if (pQInfo->dataReady == QUERY_RESULT_READY) {
      *buildRes = true;
      qDebug("QInfo:%p retrieve result info, rowsize:%d, rows:%" PRId64 ", code:%s", pQInfo, pQuery->rowSize,
             pQuery->rec.rows, tstrerror(pQInfo->code));
    } else {
      *buildRes = false;
      qDebug("QInfo:%p retrieve req set query return result after paused", pQInfo);
      pQInfo->rspContext = pRspContext;
      assert(pQInfo->rspContext != NULL);
    }

    code = pQInfo->code;
    pthread_mutex_unlock(&pQInfo->lock);
  }

  return code;
}

int32_t qDumpRetrieveResult(qinfo_t qinfo, SRetrieveTableRsp **pRsp, int32_t *contLen, bool* continueExec) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  size_t  size = getResultSize(pQInfo, &pQuery->rec.rows);

  size += sizeof(int32_t);
  size += sizeof(STableIdInfo) * taosHashGetSize(pQInfo->arrTableIdInfo);

  *contLen = (int32_t)(size + sizeof(SRetrieveTableRsp));

  // todo proper handle failed to allocate memory,
  // current solution only avoid crash, but cannot return error code to client
  *pRsp = (SRetrieveTableRsp *)rpcMallocCont(*contLen);
  if (*pRsp == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  (*pRsp)->numOfRows = htonl((int32_t)pQuery->rec.rows);

  if (pQInfo->code == TSDB_CODE_SUCCESS) {
    (*pRsp)->offset   = htobe64(pQuery->limit.offset);
    (*pRsp)->useconds = htobe64(pRuntimeEnv->summary.elapsedTime);
  } else {
    (*pRsp)->offset   = 0;
    (*pRsp)->useconds = htobe64(pRuntimeEnv->summary.elapsedTime);
  }

  (*pRsp)->precision = htons(pQuery->precision);
  if (pQuery->rec.rows > 0 && pQInfo->code == TSDB_CODE_SUCCESS) {
    doDumpQueryResult(pQInfo, (*pRsp)->data);
  } else {
    setQueryStatus(pQuery, QUERY_OVER);
  }

  pQInfo->rspContext = NULL;
  pQInfo->dataReady  = QUERY_RESULT_NOT_READY;

  if (IS_QUERY_KILLED(pQInfo) || Q_STATUS_EQUAL(pQuery->status, QUERY_OVER)) {
    // here current thread hold the refcount, so it is safe to free tsdbQueryHandle.
    *continueExec = false;
    (*pRsp)->completed = 1;  // notify no more result to client
  } else {
    *continueExec = true;
    qDebug("QInfo:%p has more results to retrieve", pQInfo);
  }

  return pQInfo->code;
}

int32_t qQueryCompleted(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  SQuery* pQuery = pQInfo->runtimeEnv.pQuery;
  return isQueryKilled(pQInfo) || Q_STATUS_EQUAL(pQuery->status, QUERY_OVER);
}

int32_t qKillQuery(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  setQueryKilled(pQInfo);

  // Wait for the query executing thread being stopped/
  // Once the query is stopped, the owner of qHandle will be cleared immediately.
  while (pQInfo->owner != 0) {
    taosMsleep(100);
  }

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
  int32_t functionId = pQuery->pExpr1[0].base.functionId;
  if (functionId == TSDB_FUNC_TID_TAG) { // return the tags & table Id
    assert(pQuery->numOfOutput == 1);

    SExprInfo* pExprInfo = &pQuery->pExpr1[0];
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

      char *output = pQuery->sdata[0]->data + count * rsize;
      varDataSetLen(output, rsize - VARSTR_HEADER_SIZE);

      output = varDataVal(output);
      STableId* id = TSDB_TABLEID(item->pTable);

      *(int16_t *)output = 0;
      output += sizeof(int16_t);

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
    SET_STABLE_QUERY_OVER(pQInfo);
    qDebug("QInfo:%p create count(tbname) query, res:%d rows:1", pQInfo, count);
  } else {  // return only the tags|table name etc.
    count = 0;
    SSchema tbnameSchema = tGetTableNameColumnSchema();

    int32_t maxNumOfTables = (int32_t)pQuery->rec.capacity;
    if (pQuery->limit.limit >= 0 && pQuery->limit.limit < pQuery->rec.capacity) {
      maxNumOfTables = (int32_t)pQuery->limit.limit;
    }

    while(pQInfo->tableIndex < num && count < maxNumOfTables) {
      int32_t i = pQInfo->tableIndex++;

      // discard current result due to offset
      if (pQuery->limit.offset > 0) {
        pQuery->limit.offset -= 1;
        continue;
      }

      SExprInfo* pExprInfo = pQuery->pExpr1;
      STableQueryInfo* item = taosArrayGetP(pa, i);

      char *data = NULL, *dst = NULL;
      int16_t type = 0, bytes = 0;
      for(int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        // not assign value in case of user defined constant output column
        if (TSDB_COL_IS_UD_COL(pExprInfo[j].base.colInfo.flag)) {
          continue;
        }

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

static int64_t getQuerySupportBufSize(size_t numOfTables) {
  size_t s1 = sizeof(STableQueryInfo);
  size_t s2 = sizeof(SHashNode);

//  size_t s3 = sizeof(STableCheckInfo);  buffer consumption in tsdb
  return (int64_t)((s1 + s2) * 1.5 * numOfTables);
}

int32_t checkForQueryBuf(size_t numOfTables) {
  int64_t t = getQuerySupportBufSize(numOfTables);
  if (tsQueryBufferSize < 0) {
    return TSDB_CODE_SUCCESS;
  } else if (tsQueryBufferSize > 0) {

    while(1) {
      int64_t s = tsQueryBufferSize;
      int64_t remain = s - t;
      if (remain >= 0) {
        if (atomic_val_compare_exchange_64(&tsQueryBufferSize, s, remain) == s) {
          return TSDB_CODE_SUCCESS;
        }
      } else {
        return TSDB_CODE_QRY_NOT_ENOUGH_BUFFER;
      }
    }
  }

  // disable query processing if the value of tsQueryBufferSize is zero.
  return TSDB_CODE_QRY_NOT_ENOUGH_BUFFER;
}

void releaseQueryBuf(size_t numOfTables) {
  if (tsQueryBufferSize <= 0) {
    return;
  }

  int64_t t = getQuerySupportBufSize(numOfTables);

  // restore value is not enough buffer available
  atomic_add_fetch_64(&tsQueryBufferSize, t);
}

void* qGetResultRetrieveMsg(qinfo_t qinfo) {
  SQInfo* pQInfo = (SQInfo*) qinfo;
  assert(pQInfo != NULL);

  return pQInfo->rspContext;
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
  const int32_t REFRESH_HANDLE_INTERVAL = 30; // every 30 seconds, refresh handle pool

  char cacheName[128] = {0};
  sprintf(cacheName, "qhandle_%d", vgId);

  SQueryMgmt* pQueryMgmt = calloc(1, sizeof(SQueryMgmt));
  if (pQueryMgmt == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryMgmt->qinfoPool = taosCacheInit(TSDB_CACHE_PTR_KEY, REFRESH_HANDLE_INTERVAL, true, freeqinfoFn, cacheName);
  pQueryMgmt->closed    = false;
  pQueryMgmt->vgId      = vgId;

  pthread_mutex_init(&pQueryMgmt->lock, NULL);

  qDebug("vgId:%d, open querymgmt success", vgId);
  return pQueryMgmt;
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

void qQueryMgmtReOpen(void *pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  SQueryMgmt *pQueryMgmt = pQMgmt;
  qDebug("vgId:%d, set querymgmt reopen", pQueryMgmt->vgId);

  pthread_mutex_lock(&pQueryMgmt->lock);
  pQueryMgmt->closed = false;
  pthread_mutex_unlock(&pQueryMgmt->lock);
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

  qDebug("vgId:%d, queryMgmt cleanup completed", vgId);
}

void** qRegisterQInfo(void* pMgmt, uint64_t qInfo) {
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  SQueryMgmt *pQueryMgmt = pMgmt;
  if (pQueryMgmt->qinfoPool == NULL) {
    qError("QInfo:%p failed to add qhandle into qMgmt, since qMgmt is closed", (void *)qInfo);
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  pthread_mutex_lock(&pQueryMgmt->lock);
  if (pQueryMgmt->closed) {
    pthread_mutex_unlock(&pQueryMgmt->lock);
    qError("QInfo:%p failed to add qhandle into cache, since qMgmt is colsing", (void *)qInfo);
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  } else {
    TSDB_CACHE_PTR_TYPE handleVal = (TSDB_CACHE_PTR_TYPE) qInfo;
    void** handle = taosCachePut(pQueryMgmt->qinfoPool, &handleVal, sizeof(TSDB_CACHE_PTR_TYPE), &qInfo, sizeof(TSDB_CACHE_PTR_TYPE),
        (getMaximumIdleDurationSec()*1000));
    pthread_mutex_unlock(&pQueryMgmt->lock);

    return handle;
  }
}

void** qAcquireQInfo(void* pMgmt, uint64_t _key) {
  SQueryMgmt *pQueryMgmt = pMgmt;

  if (pQueryMgmt->closed) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  if (pQueryMgmt->qinfoPool == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_QHANDLE;
    return NULL;
  }

  TSDB_CACHE_PTR_TYPE key = (TSDB_CACHE_PTR_TYPE)_key;
  void** handle = taosCacheAcquireByKey(pQueryMgmt->qinfoPool, &key, sizeof(TSDB_CACHE_PTR_TYPE));
  if (handle == NULL || *handle == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_QHANDLE;
    return NULL;
  } else {
    return handle;
  }
}

void** qReleaseQInfo(void* pMgmt, void* pQInfo, bool freeHandle) {
  SQueryMgmt *pQueryMgmt = pMgmt;
  if (pQueryMgmt->qinfoPool == NULL) {
    return NULL;
  }

  taosCacheRelease(pQueryMgmt->qinfoPool, pQInfo, freeHandle);
  return 0;
}