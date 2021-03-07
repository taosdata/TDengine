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
#include "tglobal.h"

#include "exception.h"
#include "hash.h"
#include "texpr.h"
#include "qExecutor.h"
#include "qResultbuf.h"
#include "qUtil.h"
#include "queryLog.h"
#include "tlosertree.h"
#include "ttype.h"
#include "tscompression.h"

#define IS_MASTER_SCAN(runtime)        ((runtime)->scanFlag == MASTER_SCAN)
#define IS_REVERSE_SCAN(runtime)       ((runtime)->scanFlag == REVERSE_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime)  ((runtime)->scanFlag = MASTER_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define GET_COL_DATA_POS(query, index, step) ((query)->pos + (index) * (step))
#define SWITCH_ORDER(n) (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

#define SDATA_BLOCK_INITIALIZER (SDataBlockInfo) {{0}, 0}

#define TIME_WINDOW_COPY(_dst, _src)  do {\
   (_dst).skey = (_src).skey;\
   (_dst).ekey = (_src).ekey;\
} while (0)

enum {
  TS_JOIN_TS_EQUAL       = 0,
  TS_JOIN_TS_NOT_EQUALS  = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

typedef struct {
  int32_t     status;       // query status
  TSKEY       lastKey;      // the lastKey value before query executed
  STimeWindow w;            // whole query time window
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
#define QUERY_IS_INTERVAL_QUERY(_q) ((_q)->interval.interval > 0)

int32_t getMaximumIdleDurationSec() {
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
static void doSetTagValueToResultBuf(char* output, const char* val, int16_t type, int16_t bytes);

static void setResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx,
    int32_t numOfCols, int32_t* rowCellInfoOffset);

void setResultRowOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset);
static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);

static void setBlockStatisInfo(SQLFunctionCtx *pCtx, SSDataBlock* pSDataBlock, SColIndex* pColIndex);

static void destroyTableQueryInfoImpl(STableQueryInfo *pTableQueryInfo);
static bool hasMainOutput(SQuery *pQuery);

//static int32_t setTimestampListJoinInfo(SQueryRuntimeEnv* pRuntimeEnv, STableQueryInfo *pTableQueryInfo);
static void releaseQueryBuf(size_t numOfTables);
static int32_t binarySearchForKey(char *pValue, int num, TSKEY key, int order);
static STsdbQueryCond createTsdbQueryCond(SQuery* pQuery, STimeWindow* win);
static STableIdInfo createTableIdInfo(SQuery* pQuery);

static void setTableScanFilterOperatorInfo(STableScanInfo* pTableScanInfo, SOperatorInfo* pDownstream);

static int32_t getNumOfScanTimes(SQuery* pQuery);
static bool isFixedOutputQuery(SQuery* pQuery);

static SOperatorInfo* createDataBlocksOptScanInfo(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime, int32_t reverseTime);
static SOperatorInfo* createTableScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime);
static SOperatorInfo* createTableSeqScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, bool loadExternalRows);

static SOperatorInfo* createAggregateOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createArithOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createLimitOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream);
static SOperatorInfo* createOffsetOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream);
static SOperatorInfo* createTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createFillOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createGroupbyOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createMultiTableAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createMultiTableTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createTagScanOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput);
static SOperatorInfo* createTableBlockInfoScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv);

static void destroyBasicOperatorInfo(void* param, int32_t numOfOutput);
static void destroySFillOperatorInfo(void* param, int32_t numOfOutput);
static void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput);
static void destroyArithOperatorInfo(void* param, int32_t numOfOutput);

static int32_t doCopyToSDataBlock(SQueryRuntimeEnv* pRuntimeEnv, SGroupResInfo* pGroupResInfo, int32_t orderType, SSDataBlock* pBlock);

static int32_t getGroupbyColumnIndex(SSqlGroupbyExpr *pGroupbyExpr, SSDataBlock* pDataBlock);

static int32_t setGroupResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo* pResultRowInfo,
                                          SQLFunctionCtx * pCtx, int32_t numOfCols, char *pData, int16_t type, int16_t bytes, int32_t groupIndex, int32_t* offset);
static void destroyOperatorInfo(SOperatorInfo* pOperator);
void initCtxOutputBuffer(SQLFunctionCtx* pCtx, int32_t size);
void getAlignQueryTimeWindow(SQuery *pQuery, int64_t key, int64_t keyFirst, int64_t keyLast, STimeWindow *win);
static bool isPointInterpoQuery(SQuery *pQuery);

// setup the output buffer for each operator
static SSDataBlock* createOutputBuf(SExprInfo* pExpr, int32_t numOfOutput, int32_t numOfRows) {
  SSDataBlock *res = calloc(1, sizeof(SSDataBlock));
  res->info.numOfCols = numOfOutput;

  res->pDataBlock = taosArrayInit(numOfOutput, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData idata = {0};
    idata.info.type = pExpr[i].type;
    idata.info.bytes = pExpr[i].bytes;
    idata.info.colId = pExpr[i].base.resColId;
    idata.pData = calloc(numOfRows, idata.info.bytes);

    taosArrayPush(res->pDataBlock, &idata);
  }

  return res;
}

static void* destroyOutputBuf(SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return NULL;
  }

  int32_t numOfOutput = pBlock->info.numOfCols;
  for(int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    tfree(pColInfoData->pData);
  }

  taosArrayDestroy(pBlock->pDataBlock);
  tfree(pBlock->pBlockStatis);
  tfree(pBlock);
  return NULL;
}

bool doFilterData(SQuery *pQuery, int32_t elemPos) {
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];

    char *pElem = (char*)pFilterInfo->pData + pFilterInfo->info.bytes * elemPos;

    bool qualified = false;
    for (int32_t j = 0; j < pFilterInfo->numOfFilters; ++j) {
      SColumnFilterElem *pFilterElem = &pFilterInfo->pFilters[j];

      bool isnull = isNull(pElem, pFilterInfo->info.type);
      if (isnull) {
        if (pFilterElem->fp == isNullOperator) {
          qualified = true;
          break;
        } else {
          continue;
        }
      } else {
        if (pFilterElem->fp == notNullOperator) {
          qualified = true;
          break;
        } else if (pFilterElem->fp == isNullOperator) {
          continue;
        }
      }

      if (pFilterElem->fp(pFilterElem, pElem, pElem, pFilterInfo->info.type)) {
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

int64_t getNumOfResult(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool    hasMainFunction = hasMainOutput(pQuery);

  int64_t maxOutput = 0;
  for (int32_t j = 0; j < numOfOutput; ++j) {
    int32_t functionId = pCtx[j].functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (hasMainFunction &&
        (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ)) {
      continue;
    }

    SResultRowCellInfo *pResInfo = GET_RES_INFO(&pCtx[j]);
    if (pResInfo != NULL && maxOutput < pResInfo->numOfRes) {
      maxOutput = pResInfo->numOfRes;
    }
  }

  assert(maxOutput >= 0);
  return maxOutput;
}

static void setNumOfRes(SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t j = 0; j < numOfOutput; ++j) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(&pCtx[j]);
    pResInfo->numOfRes = 0;
  }
}

bool isGroupbyColumn(SSqlGroupbyExpr *pGroupbyExpr) {
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

bool isStabledev(SQuery* pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functId = pQuery->pExpr1[i].base.functionId;
    if (functId == TSDB_FUNC_STDDEV_DST) {
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

static bool isSelectivityWithTagsQuery(SQLFunctionCtx *pCtx, int32_t numOfOutput) {
  bool    hasTags = false;
  int32_t numOfSelectivity = 0;

  for (int32_t i = 0; i < numOfOutput; ++i) {
    int32_t functId = pCtx[i].functionId;
    if (functId == TSDB_FUNC_TAG_DUMMY || functId == TSDB_FUNC_TS_DUMMY) {
      hasTags = true;
      continue;
    }

    if ((aAggs[functId].status & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    }
  }

  return (numOfSelectivity > 0 && hasTags);
}

static bool isProjQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functId = pQuery->pExpr1[i].base.functionId;
    if (functId != TSDB_FUNC_PRJ && functId != TSDB_FUNC_TAGPRJ) {
      return false;
    }
  }

  return true;
}

static bool isTsCompQuery(SQuery *pQuery) { return pQuery->pExpr1[0].base.functionId == TSDB_FUNC_TS_COMP; }

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
    if (functionId == TSDB_FUNC_TWA || functionId == TSDB_FUNC_INTERP) {
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

static bool hasNullRv(SColIndex* pColIndex, SDataStatis *pStatis) {
  if (TSDB_COL_IS_TAG(pColIndex->flag) || TSDB_COL_IS_UD_COL(pColIndex->flag) || pColIndex->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return false;
  }

  if (pStatis != NULL && pStatis->numOfNull == 0) {
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
static STimeWindow getActiveTimeWindow(SResultRowInfo * pResultRowInfo, int64_t ts, SQuery *pQuery) {
  STimeWindow w = {0};

 if (pResultRowInfo->curIndex == -1) {  // the first window, from the previous stored value
   if (pResultRowInfo->prevSKey == TSKEY_INITIAL_VAL) {
     if (QUERY_IS_ASC_QUERY(pQuery)) {
       getAlignQueryTimeWindow(pQuery, ts, ts, pQuery->window.ekey, &w);
     } else { // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
       getAlignQueryTimeWindow(pQuery, ts, pQuery->window.ekey, ts, &w);
     }

     pResultRowInfo->prevSKey = w.skey;
   } else {
    w.skey = pResultRowInfo->prevSKey;
   }

    if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
      w.ekey = taosTimeAdd(w.skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision) - 1;
    } else {
      w.ekey = w.skey + pQuery->interval.interval - 1;
    }
  } else {
    int32_t slot = curTimeWindowIndex(pResultRowInfo);
    SResultRow* pWindowRes = getResultRow(pResultRowInfo, slot);
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

// a new buffer page for each table. Needs to opt this design
static int32_t addNewWindowResultBuf(SResultRow *pWindowRes, SDiskbasedResultBuf *pResultBuf, int32_t tid, uint32_t size) {
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

    if (pData->num + size > pResultBuf->pageSize) {
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
    pWindowRes->offset = pData->num;

    pData->num += size;
    assert(pWindowRes->pageId >= 0);
  }

  return 0;
}

static int32_t setWindowOutputBufByKey(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, STimeWindow *win,
                                       bool masterscan, SResultRow **pResult, int64_t groupId, SQLFunctionCtx* pCtx,
                                       int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  assert(win->skey <= win->ekey);
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SResultRow *pResultRow = doPrepareResultRowFromKey(pRuntimeEnv, pResultRowInfo, (char *)&win->skey, TSDB_KEYSIZE, masterscan, groupId);
  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // not assign result buffer yet, add new result buffer
  if (pResultRow->pageId == -1) {

    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, (int32_t) groupId, pRuntimeEnv->pQuery->intermediateResultRowSize);
    if (ret != TSDB_CODE_SUCCESS) {
      return -1;
    }
  }

  // set time window for current result
  pResultRow->win = (*win);
  *pResult = pResultRow;
  setResultRowOutputBufInitCtx(pRuntimeEnv, pResultRow, pCtx, numOfOutput, rowCellInfoOffset);

  return TSDB_CODE_SUCCESS;
}

static UNUSED_FUNC bool getResultRowStatus(SResultRowInfo *pWindowResInfo, int32_t slot) {
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

static void doUpdateResultRowIndex(SResultRowInfo*pResultRowInfo, TSKEY lastKey, bool ascQuery, bool timeWindowInterpo) {
  int64_t skey = TSKEY_INITIAL_VAL;
  int32_t i = 0;
  for (i = pResultRowInfo->size - 1; i >= 0; --i) {
    SResultRow *pResult = pResultRowInfo->pResult[i];
    if (pResult->closed) {
      break;
    }

    // new closed result rows
    if (timeWindowInterpo) {
      if (pResult->endInterp && ((pResult->win.skey <= lastKey && ascQuery) || (pResult->win.skey >= lastKey && !ascQuery))) {
        if (i > 0) { // the first time window, the startInterp is false.
          assert(pResult->startInterp);
        }

        closeResultRow(pResultRowInfo, i);
      } else {
        skey = pResult->win.skey;
      }
    } else {
      if ((pResult->win.ekey <= lastKey && ascQuery) || (pResult->win.skey >= lastKey && !ascQuery)) {
        closeResultRow(pResultRowInfo, i);
      } else {
        skey = pResult->win.skey;
      }
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

    if (i == pResultRowInfo->size - 1) {
      pResultRowInfo->curIndex = i;
    } else {
      pResultRowInfo->curIndex = i + 1;  // current not closed result object
    }

    pResultRowInfo->prevSKey = pResultRowInfo->pResult[pResultRowInfo->curIndex]->win.skey;
  }
}

static UNUSED_FUNC void updateResultRowIndex(SResultRowInfo* pResultRowInfo, STableQueryInfo* pTableQueryInfo, bool ascQuery, bool timeWindowInterpo) {
  if ((pTableQueryInfo->lastKey > pTableQueryInfo->win.ekey && ascQuery) || (pTableQueryInfo->lastKey < pTableQueryInfo->win.ekey && (!ascQuery))) {
    closeAllResultRows(pResultRowInfo);
    pResultRowInfo->curIndex = pResultRowInfo->size - 1;
  } else {
    int32_t step = ascQuery? 1:-1;
    doUpdateResultRowIndex(pResultRowInfo, pTableQueryInfo->lastKey - step, ascQuery, timeWindowInterpo);
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

static void doApplyFunctions(SQueryRuntimeEnv* pRuntimeEnv, SQLFunctionCtx* pCtx, STimeWindow* pWin, int32_t offset,
                             int32_t forwardStep, TSKEY* tsCol, int32_t numOfTotal, int32_t numOfOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool hasPrev = pCtx[0].preAggVals.isSet;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    pCtx[k].size = forwardStep;
    pCtx[k].startTs = pWin->skey;

    char* start = pCtx[k].pInput;

    int32_t pos = (QUERY_IS_ASC_QUERY(pQuery)) ? offset : offset - (forwardStep - 1);
    if (pCtx[k].pInput != NULL) {
      pCtx[k].pInput = (char *)pCtx[k].pInput + pos * pCtx[k].inputBytes;
    }

    if (tsCol != NULL) {
      pCtx[k].ptsList = &tsCol[pos];
    }

    int32_t functionId = pCtx[k].functionId;

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
    pCtx[k].pInput = start;
  }
}


static int32_t getNextQualifiedWindow(SQuery* pQuery, STimeWindow *pNext, SDataBlockInfo *pDataBlockInfo,
    TSKEY *primaryKeys, __block_search_fn_t searchFn, int32_t prevPosition) {
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


static void saveDataBlockLastRow(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pDataBlockInfo, SArray* pDataBlock,
    int32_t rowIndex) {
  if (pDataBlock == NULL) {
    return;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  for (int32_t k = 0; k < pQuery->numOfCols; ++k) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, k);
    memcpy(pRuntimeEnv->prevRow[k], ((char*)pColInfo->pData) + (pColInfo->info.bytes * rowIndex), pColInfo->info.bytes);
  }
}

static TSKEY getStartTsKey(SQuery* pQuery, STimeWindow* win, const TSKEY* tsCols, int32_t rows) {
  TSKEY ts = TSKEY_INITIAL_VAL;

  bool ascQuery = QUERY_IS_ASC_QUERY(pQuery);
  if (tsCols == NULL) {
    ts = ascQuery? win->skey : win->ekey;
  } else {
    int32_t offset = ascQuery? 0:rows-1;
    ts = tsCols[offset];
  }

  return ts;
}

static void setArithParams(SArithmeticSupport* sas, SExprInfo *pExprInfo, SSDataBlock* pSDataBlock) {
  sas->numOfCols = (int32_t) pSDataBlock->info.numOfCols;
  sas->pArithExpr = pExprInfo;

  sas->colList = calloc(1, pSDataBlock->info.numOfCols*sizeof(SColumnInfo));
  for(int32_t i = 0; i < sas->numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pSDataBlock->pDataBlock, i);
    sas->colList[i] = pColData->info;
  }

  sas->data = calloc(sas->numOfCols, POINTER_BYTES);

  // set the input column data
  for (int32_t f = 0; f < pSDataBlock->info.numOfCols; ++f) {
    SColumnInfoData *pColumnInfoData = taosArrayGet(pSDataBlock->pDataBlock, f);
    sas->data[f] = pColumnInfoData->pData;
  }
}

static void doSetInputDataBlock(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order);
static void doSetInputDataBlockInfo(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order) {
  for (int32_t i = 0; i < pOperator->numOfOutput; ++i) {
    pCtx[i].order = order;
    pCtx[i].size  = pBlock->info.rows;
    pCtx[i].currentStage = pOperator->pRuntimeEnv->scanFlag;

    setBlockStatisInfo(&pCtx[i], pBlock, &pOperator->pExpr[i].base.colInfo);
  }
}

static void setInputDataBlock(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order) {
  if (pCtx[0].functionId == TSDB_FUNC_ARITHM) {
    SArithmeticSupport* pSupport = (SArithmeticSupport*) pCtx[0].param[1].pz;
    if (pSupport->colList == NULL) {
      doSetInputDataBlock(pOperator, pCtx, pBlock, order);
    } else {
      doSetInputDataBlockInfo(pOperator, pCtx, pBlock, order);
    }
  } else {
    if (pCtx[0].pInput == NULL && pBlock->pDataBlock != NULL) {
      doSetInputDataBlock(pOperator, pCtx, pBlock, order);
    } else {
      doSetInputDataBlockInfo(pOperator, pCtx, pBlock, order);
    }
  }
}

static void doSetInputDataBlock(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order) {
  for (int32_t i = 0; i < pOperator->numOfOutput; ++i) {
    pCtx[i].order = order;
    pCtx[i].size  = pBlock->info.rows;
    pCtx[i].currentStage = pOperator->pRuntimeEnv->scanFlag;

    setBlockStatisInfo(&pCtx[i], pBlock, &pOperator->pExpr[i].base.colInfo);

    if (pCtx[i].functionId == TSDB_FUNC_ARITHM) {
      setArithParams((SArithmeticSupport*)pCtx[i].param[1].pz, &pOperator->pExpr[i], pBlock);
    } else {
      SColIndex* pCol = &pOperator->pExpr[i].base.colInfo;
      if (TSDB_COL_IS_NORMAL_COL(pCol->flag) || pCol->colId == TSDB_BLOCK_DIST_COLUMN_INDEX) {
        SColIndex*       pColIndex = &pOperator->pExpr[i].base.colInfo;
        SColumnInfoData* p = taosArrayGet(pBlock->pDataBlock, pColIndex->colIndex);

        // in case of the block distribution query, the inputBytes is not a constant value.
        pCtx[i].pInput = p->pData;
        assert(p->info.colId == pColIndex->colId && pCtx[i].inputType == p->info.type);// && pCtx[i].inputBytes == p->info.bytes);

        uint32_t status = aAggs[pCtx[i].functionId].status;
        if ((status & (TSDB_FUNCSTATE_SELECTIVITY | TSDB_FUNCSTATE_NEED_TS)) != 0) {
          SColumnInfoData* tsInfo = taosArrayGet(pBlock->pDataBlock, 0);
          pCtx[i].ptsList = tsInfo->pData;
        }
      }
    }
  }
}

static void doAggregateImpl(SOperatorInfo* pOperator, TSKEY startTs, SQLFunctionCtx* pCtx, SSDataBlock* pSDataBlock) {
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
    int32_t functionId = pCtx[k].functionId;
    if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
      pCtx[k].startTs = startTs;// this can be set during create the struct
      aAggs[functionId].xFunction(&pCtx[k]);
    }
  }
}

static void arithmeticApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t numOfOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    pCtx[k].startTs = pQuery->window.skey;
    aAggs[pCtx[k].functionId].xFunction(&pCtx[k]);
  }
}

void doTimeWindowInterpolation(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SArray* pDataBlock, TSKEY prevTs,
                               int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey, int32_t type) {
  SQueryRuntimeEnv *pRuntimeEnv = pOperator->pRuntimeEnv;
  SExprInfo* pExpr = pOperator->pExpr;

  SQLFunctionCtx* pCtx = pInfo->pCtx;

  for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
    int32_t functionId = pCtx[k].functionId;
    if (functionId != TSDB_FUNC_TWA && functionId != TSDB_FUNC_INTERP) {
      pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SColIndex *      pColIndex = &pExpr[k].base.colInfo;
    int16_t          index = pColIndex->colIndex;
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, index);

    assert(pColInfo->info.colId == pColIndex->colId && curTs != windowKey);
    double v1 = 0, v2 = 0, v = 0;

    if (prevRowIndex == -1) {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char *)pRuntimeEnv->prevRow[index]);
    } else {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char *)pColInfo->pData + prevRowIndex * pColInfo->info.bytes);
    }

    GET_TYPED_DATA(v2, double, pColInfo->info.type, (char *)pColInfo->pData + curRowIndex * pColInfo->info.bytes);

    if (functionId == TSDB_FUNC_INTERP) {
      if (type == RESULT_ROW_START_INTERP) {
        pCtx[k].start.key = prevTs;
        pCtx[k].start.val = v1;

        pCtx[k].end.key = curTs;
        pCtx[k].end.val = v2;
      }
    } else if (functionId == TSDB_FUNC_TWA) {
      SPoint point1 = (SPoint){.key = prevTs,    .val = &v1};
      SPoint point2 = (SPoint){.key = curTs,     .val = &v2};
      SPoint point  = (SPoint){.key = windowKey, .val = &v };

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

static bool setTimeWindowInterpolationStartTs(SOperatorInfo* pOperatorInfo, SQLFunctionCtx* pCtx, int32_t pos,
                                              int32_t numOfRows, SArray* pDataBlock, const TSKEY* tsCols, STimeWindow* win) {
  SQueryRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;

  bool ascQuery = QUERY_IS_ASC_QUERY(pQuery);

  TSKEY curTs  = tsCols[pos];
  TSKEY lastTs = *(TSKEY *) pRuntimeEnv->prevRow[0];

  // lastTs == INT64_MIN and pos == 0 means this is the first time window, interpolation is not needed.
  // start exactly from this point, no need to do interpolation
  TSKEY key = ascQuery? win->skey:win->ekey;
  if (key == curTs) {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);
    return true;
  }

  if (lastTs == INT64_MIN && ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery))) {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);
    return true;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  TSKEY   prevTs = ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery))? lastTs:tsCols[pos - step];

  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, prevTs, pos - step, curTs, pos,
      key, RESULT_ROW_START_INTERP);
  return true;
}

static bool setTimeWindowInterpolationEndTs(SOperatorInfo* pOperatorInfo, SQLFunctionCtx* pCtx,
    int32_t endRowIndex, SArray* pDataBlock, const TSKEY* tsCols, TSKEY blockEkey, STimeWindow* win) {
  SQueryRuntimeEnv *pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t numOfOutput = pOperatorInfo->numOfOutput;

  TSKEY   actualEndKey = tsCols[endRowIndex];

  TSKEY key = QUERY_IS_ASC_QUERY(pQuery)? win->ekey:win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey && QUERY_IS_ASC_QUERY(pQuery)) || (key < blockEkey && !QUERY_IS_ASC_QUERY(pQuery))) {
    setNotInterpoWindowKey(pCtx, numOfOutput, RESULT_ROW_END_INTERP);
    return false;
  }

  // there is actual end point of current time window, no interpolation need
  if (key == actualEndKey) {
    setNotInterpoWindowKey(pCtx, numOfOutput, RESULT_ROW_END_INTERP);
    return true;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  int32_t nextRowIndex = endRowIndex + step;
  assert(nextRowIndex >= 0);

  TSKEY nextKey = tsCols[nextRowIndex];
  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, actualEndKey, endRowIndex, nextKey,
      nextRowIndex, key, RESULT_ROW_END_INTERP);
  return true;
}

static void doWindowBorderInterpolation(SOperatorInfo* pOperatorInfo, SSDataBlock* pBlock, SQLFunctionCtx* pCtx,
    SResultRow* pResult, STimeWindow* win, int32_t startPos, int32_t forwardStep) {
  SQueryRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (!pQuery->timeWindowInterpo) {
    return;
  }

  assert(pBlock != NULL);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, 0);

  TSKEY  *tsCols = (TSKEY *)(pColInfo->pData);
  bool done = resultRowInterpolated(pResult, RESULT_ROW_START_INTERP);
  if (!done) { // it is not interpolated, now start to generated the interpolated value
    int32_t startRowIndex = startPos;
    bool interp = setTimeWindowInterpolationStartTs(pOperatorInfo, pCtx, startRowIndex, pBlock->info.rows, pBlock->pDataBlock,
        tsCols, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_START_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);
  }

  // point interpolation does not require the end key time window interpolation.
  if (isPointInterpoQuery(pQuery)) {
    return;
  }

  // interpolation query does not generate the time window end interpolation
  done = resultRowInterpolated(pResult, RESULT_ROW_END_INTERP);
  if (!done) {
    int32_t endRowIndex = startPos + (forwardStep - 1) * step;

    TSKEY endKey = QUERY_IS_ASC_QUERY(pQuery)? pBlock->info.window.ekey:pBlock->info.window.skey;
    bool  interp = setTimeWindowInterpolationEndTs(pOperatorInfo, pCtx, endRowIndex, pBlock->pDataBlock, tsCols, endKey, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pCtx, pQuery->numOfOutput, RESULT_ROW_END_INTERP);
  }
}

static void hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo,
                            STableIntervalOperatorInfo* pInfo, SSDataBlock* pSDataBlock, int32_t groupId) {
  SQueryRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  int32_t           numOfOutput = pOperatorInfo->numOfOutput;
  SQuery*           pQuery = pRuntimeEnv->pQuery;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  int32_t prevIndex = curTimeWindowIndex(pResultRowInfo);

  TSKEY* tsCols = NULL;
  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, 0);
    tsCols = pColDataInfo->pData;
    assert(tsCols[0] == pSDataBlock->info.window.skey &&
           tsCols[pSDataBlock->info.rows - 1] == pSDataBlock->info.window.ekey);
  }

  int32_t startPos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : (pSDataBlock->info.rows - 1);
  TSKEY ts = getStartTsKey(pQuery, &pSDataBlock->info.window, tsCols, pSDataBlock->info.rows);

  STimeWindow win = getActiveTimeWindow(pResultRowInfo, ts, pQuery);
  bool        masterScan = (pRuntimeEnv->scanFlag == MASTER_SCAN) ? true : false;

  SResultRow* pResult = NULL;
  int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pResultRowInfo, &win, masterScan, &pResult, groupId, pInfo->pCtx,
                                        numOfOutput, pInfo->rowCellInfoOffset);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    //      goto _end;
  }

  int32_t forwardStep = 0;
  TSKEY   ekey = reviseWindowEkey(pQuery, &win);
  forwardStep =
      getNumOfRowsInTimeWindow(pQuery, &pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, true);

  // prev time window not interpolation yet.
  int32_t curIndex = curTimeWindowIndex(pResultRowInfo);
  if (prevIndex != -1 && prevIndex < curIndex && pQuery->timeWindowInterpo) {
    for (int32_t j = prevIndex; j < curIndex; ++j) {  // previous time window may be all closed already.
      SResultRow* pRes = pResultRowInfo->pResult[j];
      if (pRes->closed) {
        assert(resultRowInterpolated(pRes, RESULT_ROW_START_INTERP) &&
               resultRowInterpolated(pRes, RESULT_ROW_END_INTERP));
        continue;
      }

      STimeWindow w = pRes->win;
      ret = setWindowOutputBufByKey(pRuntimeEnv, pResultRowInfo, &w, masterScan, &pResult, groupId, pInfo->pCtx,
                                    numOfOutput, pInfo->rowCellInfoOffset);
      assert(ret == TSDB_CODE_SUCCESS && !resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

      doTimeWindowInterpolation(pOperatorInfo, pInfo, pSDataBlock->pDataBlock, *(TSKEY *)pRuntimeEnv->prevRow[0],
            -1, tsCols[startPos], startPos, w.ekey, RESULT_ROW_END_INTERP);

      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
      setNotInterpoWindowKey(pInfo->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);

      doApplyFunctions(pRuntimeEnv, pInfo->pCtx, &w, startPos, 0, tsCols, pSDataBlock->info.rows,
                                   numOfOutput);
    }

    // restore current time window
    ret = setWindowOutputBufByKey(pRuntimeEnv, pResultRowInfo, &win, masterScan, &pResult, groupId, pInfo->pCtx,
                                  numOfOutput, pInfo->rowCellInfoOffset);
    assert(ret == TSDB_CODE_SUCCESS);
  }

  // window start key interpolation
  doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->pCtx, pResult, &win, startPos, forwardStep);
  doApplyFunctions(pRuntimeEnv, pInfo->pCtx, &win, startPos, forwardStep, tsCols, pSDataBlock->info.rows,
                               numOfOutput);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
    startPos = getNextQualifiedWindow(pQuery, &nextWin, &pSDataBlock->info, tsCols, binarySearchForKey, prevEndPos);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    int32_t code = setWindowOutputBufByKey(pRuntimeEnv, pResultRowInfo, &nextWin, masterScan, &pResult, groupId,
                                           pInfo->pCtx, numOfOutput, pInfo->rowCellInfoOffset);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      break;
    }

    ekey = reviseWindowEkey(pQuery, &nextWin);
    forwardStep =
        getNumOfRowsInTimeWindow(pQuery, &pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, true);

    // window start(end) key interpolation
    doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->pCtx, pResult, &nextWin, startPos, forwardStep);
    doApplyFunctions(pRuntimeEnv, pInfo->pCtx, &nextWin, startPos, forwardStep, tsCols,
                                 pSDataBlock->info.rows, numOfOutput);
  }

  if (pQuery->timeWindowInterpo) {
    int32_t rowIndex = QUERY_IS_ASC_QUERY(pQuery)? pSDataBlock->info.rows-1:0;
    saveDataBlockLastRow(pRuntimeEnv, &pSDataBlock->info, pSDataBlock->pDataBlock, rowIndex);
  }
}

static void hashGroupbyAgg(SQueryRuntimeEnv *pRuntimeEnv, SOperatorInfo* pOperator, SGroupbyOperatorInfo *pInfo,
                           SSDataBlock *pSDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STableQueryInfo* item = pQuery->current;

  SColumnInfoData* pColInfoData = taosArrayGet(pSDataBlock->pDataBlock, pInfo->colIndex);
  int16_t bytes = pColInfoData->info.bytes;
  int16_t type  = pColInfoData->info.type;

  for (int32_t j = 0; j < pSDataBlock->info.rows; ++j) {
    char *val = pColInfoData->pData + bytes * j;
    if (isNull(val, type)) {  // TODO: ignore the null value
      continue;
    }

    // TODO compare with the previous value to speedup the query processing
    int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, &pInfo->binfo.resultRowInfo, pInfo->binfo.pCtx, pOperator->numOfOutput, val, type, bytes, item->groupIndex, pInfo->binfo.rowCellInfoOffset);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
    }

    for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
      pInfo->binfo.pCtx[k].size = 1;  // TODO refactor: extract from here
      int32_t functionId = pInfo->binfo.pCtx[k].functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pInfo->binfo.pCtx[k], functionId)) {
        aAggs[functionId].xFunctionF(&pInfo->binfo.pCtx[k], j);
      }
    }
  }
}

static int32_t setGroupResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo* pResultRowInfo,
    SQLFunctionCtx * pCtx, int32_t numOfCols, char *pData, int16_t type, int16_t bytes, int32_t groupIndex, int32_t* rowCellInfoOffset) {
  SDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  // not assign result buffer yet, add new result buffer, TODO remove it
  char* d = pData;
  int16_t len = bytes;
  if (type == TSDB_DATA_TYPE_BINARY||type == TSDB_DATA_TYPE_NCHAR) {
    d = varDataVal(pData);
    len = varDataLen(pData);
  } else if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
    SQInfo* pQInfo = pRuntimeEnv->qinfo;
    qError("QInfo:%p group by not supported on double/float columns, abort", pQInfo);
    return -1;
  }

  SResultRow *pResultRow = doPrepareResultRowFromKey(pRuntimeEnv, pResultRowInfo, d, len, true, groupIndex);
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
    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, groupIndex, pRuntimeEnv->pQuery->resultRowSize);
    if (ret != 0) {
      return -1;
    }
  }

  setResultOutputBuf(pRuntimeEnv, pResultRow, pCtx, numOfCols, rowCellInfoOffset);
  initCtxOutputBuffer(pCtx, numOfCols);
  return TSDB_CODE_SUCCESS;
}

static int32_t getGroupbyColumnIndex(SSqlGroupbyExpr *pGroupbyExpr, SSDataBlock* pDataBlock) {
  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    SColIndex* pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, k);
    if (TSDB_COL_IS_TAG(pColIndex->flag)) {
      continue;
    }

    int32_t colId = pColIndex->colId;

    for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
      if (pColInfo->info.colId == colId) {
        return i;
      }
    }
  }

  assert(0);
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
    return pCtx->param[0].i64 == pQuery->order.order;
  }

  // in the reverse table scan, only the following functions need to be executed
  if (IS_REVERSE_SCAN(pRuntimeEnv) ||
      (pRuntimeEnv->scanFlag == REPEAT_SCAN && functionId != TSDB_FUNC_STDDEV && functionId != TSDB_FUNC_PERCT)) {
    return false;
  }

  return true;
}

void setBlockStatisInfo(SQLFunctionCtx *pCtx, SSDataBlock* pSDataBlock, SColIndex* pColIndex) {
  SDataStatis *pStatis = NULL;

  if (pSDataBlock->pBlockStatis != NULL && TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
    pStatis = &pSDataBlock->pBlockStatis[pColIndex->colIndex];

    pCtx->preAggVals.statis = *pStatis;
    pCtx->preAggVals.isSet  = true;
    assert(pCtx->preAggVals.statis.numOfNull <= pSDataBlock->info.rows);
  } else {
    pCtx->preAggVals.isSet = false;
  }

  pCtx->hasNull = hasNullRv(pColIndex, pStatis);

  // set the statistics data for primary time stamp column
  if (pCtx->functionId == TSDB_FUNC_SPREAD && pColIndex->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    pCtx->preAggVals.isSet  = true;
    pCtx->preAggVals.statis.min = pSDataBlock->info.window.skey;
    pCtx->preAggVals.statis.max = pSDataBlock->info.window.ekey;
  }
}

// set the output buffer for the selectivity + tag query
static int32_t setCtxTagColumnInfo(SQLFunctionCtx *pCtx, int32_t numOfOutput) {
  if (!isSelectivityWithTagsQuery(pCtx, numOfOutput)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t num = 0;
  int16_t tagLen = 0;

  SQLFunctionCtx*  p = NULL;
  SQLFunctionCtx** pTagCtx = calloc(numOfOutput, POINTER_BYTES);
  if (pTagCtx == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    int32_t functionId = pCtx[i].functionId;

    if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
      tagLen += pCtx[i].outputBytes;
      pTagCtx[num++] = &pCtx[i];
    } else if ((aAggs[functionId].status & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      p = &pCtx[i];
    } else if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG) {
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

  return TSDB_CODE_SUCCESS;
}

static SQLFunctionCtx* createSQLFunctionCtx(SQueryRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput,
                                            int32_t** rowCellInfoOffset) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  SQLFunctionCtx * pFuncCtx = (SQLFunctionCtx *)calloc(numOfOutput, sizeof(SQLFunctionCtx));
  if (pFuncCtx == NULL) {
    return NULL;
  }

  *rowCellInfoOffset = calloc(numOfOutput, sizeof(int32_t));
  if (*rowCellInfoOffset == 0) {
    tfree(pFuncCtx);
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SSqlFuncMsg *pSqlFuncMsg = &pExpr[i].base;
    SQLFunctionCtx* pCtx = &pFuncCtx[i];

    SColIndex *pIndex = &pSqlFuncMsg->colInfo;

    if (TSDB_COL_REQ_NULL(pIndex->flag)) {
      pCtx->requireNull = true;
      pIndex->flag &= ~(TSDB_COL_NULL);
    } else {
      pCtx->requireNull = false;
    }

    pCtx->inputBytes = pSqlFuncMsg->colBytes;
    pCtx->inputType  = pSqlFuncMsg->colType;

    pCtx->ptsOutputBuf = NULL;

    pCtx->outputBytes  = pExpr[i].bytes;
    pCtx->outputType   = pExpr[i].type;

    pCtx->order        = pQuery->order.order;
    pCtx->functionId   = pSqlFuncMsg->functionId;
    pCtx->stableQuery  = pQuery->stableQuery;
    pCtx->interBufBytes = pExpr[i].interBytes;
    pCtx->start.key    = INT64_MIN;
    pCtx->end.key      = INT64_MIN;

    pCtx->numOfParams  = pSqlFuncMsg->numOfParams;
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
      int16_t type = pSqlFuncMsg->arg[j].argType;
      int16_t bytes = pSqlFuncMsg->arg[j].argBytes;
      if (pSqlFuncMsg->functionId == TSDB_FUNC_STDDEV_DST) {
        continue;
      }

      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
        tVariantCreateFromBinary(&pCtx->param[j], pSqlFuncMsg->arg[j].argValue.pz, bytes, type);
      } else {
        tVariantCreateFromBinary(&pCtx->param[j], (char *)&pSqlFuncMsg->arg[j].argValue.i64, bytes, type);
      }
    }

    // set the order information for top/bottom query
    int32_t functionId = pCtx->functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      int32_t f = pExpr[0].base.functionId;
      assert(f == TSDB_FUNC_TS || f == TSDB_FUNC_TS_DUMMY);

      pCtx->param[2].i64 = pQuery->order.order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i64 = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i64 = pQuery->order.orderColId;
    } else if (functionId == TSDB_FUNC_INTERP) {
      pCtx->param[2].i64 = (int8_t)pQuery->fillType;
      if (pQuery->fillVal != NULL) {
        if (isNull((const char *)&pQuery->fillVal[i], pCtx->inputType)) {
          pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
        } else {  // todo refactor, tVariantCreateFromBinary should handle the NULL value
          if (pCtx->inputType != TSDB_DATA_TYPE_BINARY && pCtx->inputType != TSDB_DATA_TYPE_NCHAR) {
            tVariantCreateFromBinary(&pCtx->param[1], (char *)&pQuery->fillVal[i], pCtx->inputBytes, pCtx->inputType);
          }
        }
      }
    } else if (functionId == TSDB_FUNC_TS_COMP) {
      pCtx->param[0].i64 = pQuery->vgId;  //TODO this should be the parameter from client
      pCtx->param[0].nType = TSDB_DATA_TYPE_BIGINT;
    } else if (functionId == TSDB_FUNC_TWA) {
      pCtx->param[1].i64 = pQuery->window.skey;
      pCtx->param[1].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[2].i64 = pQuery->window.ekey;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
    } else if (functionId == TSDB_FUNC_ARITHM) {
      pCtx->param[1].pz = (char*) &pRuntimeEnv->sasArray[i];
    }

    if (i > 0) {
      (*rowCellInfoOffset)[i] = (*rowCellInfoOffset)[i - 1] + sizeof(SResultRowCellInfo) + pExpr[i - 1].interBytes;
    }
  }

  setCtxTagColumnInfo(pFuncCtx, numOfOutput);

  return pFuncCtx;
}

static void* destroySQLFunctionCtx(SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  if (pCtx == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    for (int32_t j = 0; j < pCtx[i].numOfParams; ++j) {
      tVariantDestroy(&pCtx[i].param[j]);
    }

    tVariantDestroy(&pCtx[i].tag);
    tfree(pCtx[i].tagInfo.pTagCtxList);
  }

  tfree(pCtx);
  return NULL;
}

static void calResultBufSize(SQuery* pQuery, SRspResultInfo* pResultInfo);

static int32_t setupQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv, int32_t numOfTables) {
  qDebug("QInfo:%p setup runtime env", pRuntimeEnv->qinfo);
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->pQuery = pQuery;

  pQuery->interBufSize = getOutputInterResultBufSize(pQuery);

  calResultBufSize(pQuery, &pRuntimeEnv->resultInfo);

  pRuntimeEnv->pResultRowHashTable = taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pRuntimeEnv->keyBuf  = malloc(pQuery->maxSrcColumnSize + sizeof(int64_t));
  pRuntimeEnv->pool    = initResultRowPool(getResultRowSize(pRuntimeEnv));
  pRuntimeEnv->prevRow = malloc(POINTER_BYTES * pQuery->numOfCols + pQuery->srcRowSize);
  pRuntimeEnv->tagVal  = malloc(pQuery->tagLen);
  pRuntimeEnv->currentOffset = pQuery->limit.offset;

  pRuntimeEnv->sasArray = calloc(pQuery->numOfOutput, sizeof(SArithmeticSupport));

  if (pRuntimeEnv->sasArray == NULL || pRuntimeEnv->pResultRowHashTable == NULL || pRuntimeEnv->keyBuf == NULL ||
      pRuntimeEnv->prevRow == NULL  || pRuntimeEnv->tagVal == NULL) {
    goto _clean;
  }

  if (pQuery->numOfCols) {
    char* start = POINTER_BYTES * pQuery->numOfCols + (char*) pRuntimeEnv->prevRow;
    pRuntimeEnv->prevRow[0] = start;
    for(int32_t i = 1; i < pQuery->numOfCols; ++i) {
      pRuntimeEnv->prevRow[i] = pRuntimeEnv->prevRow[i - 1] + pQuery->colList[i-1].bytes;
    }

    *(int64_t*) pRuntimeEnv->prevRow[0] = INT64_MIN;
  }

  qDebug("QInfo:%p init runtime completed", pRuntimeEnv->qinfo);

  // group by normal column, sliding window query, interval query are handled by interval query processor
  // interval (down sampling operation)
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    if (pQuery->stableQuery) {
      pRuntimeEnv->proot = createMultiTableTimeIntervalOperatorInfo(pRuntimeEnv, pRuntimeEnv->pTableScanner, pQuery->pExpr1, pQuery->numOfOutput);
      setTableScanFilterOperatorInfo(pRuntimeEnv->pTableScanner->info, pRuntimeEnv->proot);
    } else {
      pRuntimeEnv->proot = createTimeIntervalOperatorInfo(pRuntimeEnv, pRuntimeEnv->pTableScanner, pQuery->pExpr1, pQuery->numOfOutput);
      setTableScanFilterOperatorInfo(pRuntimeEnv->pTableScanner->info, pRuntimeEnv->proot);

      if (pQuery->pExpr2 != NULL) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot, pQuery->pExpr2, pQuery->numOfExpr2);
      }

      if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
        SOperatorInfo* pInfo = pRuntimeEnv->proot;
        pRuntimeEnv->proot = createFillOperatorInfo(pRuntimeEnv, pInfo, pInfo->pExpr, pInfo->numOfOutput);
      }
    }

    } else if (pQuery->groupbyColumn) {
      pRuntimeEnv->proot = createGroupbyOperatorInfo(pRuntimeEnv, pRuntimeEnv->pTableScanner, pQuery->pExpr1, pQuery->numOfOutput);
      setTableScanFilterOperatorInfo(pRuntimeEnv->pTableScanner->info, pRuntimeEnv->proot);

      if (pQuery->pExpr2 != NULL) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot, pQuery->pExpr2, pQuery->numOfExpr2);
      }
    } else if (isFixedOutputQuery(pQuery)) {
      if (pQuery->stableQuery && !isTsCompQuery(pQuery)) {
        pRuntimeEnv->proot = createMultiTableAggOperatorInfo(pRuntimeEnv, pRuntimeEnv->pTableScanner, pQuery->pExpr1, pQuery->numOfOutput);
      } else {
        pRuntimeEnv->proot = createAggregateOperatorInfo(pRuntimeEnv, pRuntimeEnv->pTableScanner, pQuery->pExpr1, pQuery->numOfOutput);
      }

      setTableScanFilterOperatorInfo(pRuntimeEnv->pTableScanner->info, pRuntimeEnv->proot);

      if (pQuery->pExpr2 != NULL) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot, pQuery->pExpr2, pQuery->numOfExpr2);
      }
    } else {  // diff/add/multiply/subtract/division
      assert(pQuery->checkResultBuf == 1);
      if (!onlyQueryTags(pQuery)) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->pTableScanner, pQuery->pExpr1, pQuery->numOfOutput);
        setTableScanFilterOperatorInfo(pRuntimeEnv->pTableScanner->info, pRuntimeEnv->proot);
      }
    }

    if (pQuery->limit.offset > 0) {
      pRuntimeEnv->proot = createOffsetOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot);
    }

    if (pQuery->limit.limit > 0) {
      pRuntimeEnv->proot = createLimitOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot);
    }

    return TSDB_CODE_SUCCESS;

_clean:
  tfree(pRuntimeEnv->sasArray);
  tfree(pRuntimeEnv->pResultRowHashTable);
  tfree(pRuntimeEnv->keyBuf);
  tfree(pRuntimeEnv->prevRow);
  tfree(pRuntimeEnv->tagVal);

  return TSDB_CODE_QRY_OUT_OF_MEMORY;
}

static void doFreeQueryHandle(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
  pRuntimeEnv->pQueryHandle = NULL;

  SMemRef* pMemRef = &pQuery->memRef;
  assert(pMemRef->ref == 0 && pMemRef->imem == NULL && pMemRef->mem == NULL);
}

static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo* pQInfo = (SQInfo*) pRuntimeEnv->qinfo;

  qDebug("QInfo:%p teardown runtime env", pQInfo);

  if (pRuntimeEnv->sasArray != NULL) {
    for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      tfree(pRuntimeEnv->sasArray[i].data);
      tfree(pRuntimeEnv->sasArray[i].colList);
    }

    tfree(pRuntimeEnv->sasArray);
  }

  destroyResultBuf(pRuntimeEnv->pResultBuf);
  doFreeQueryHandle(pRuntimeEnv);

  pRuntimeEnv->pTsBuf = tsBufDestroy(pRuntimeEnv->pTsBuf);

  tfree(pRuntimeEnv->keyBuf);
  tfree(pRuntimeEnv->prevRow);
  tfree(pRuntimeEnv->tagVal);

  taosHashCleanup(pRuntimeEnv->pResultRowHashTable);
  pRuntimeEnv->pResultRowHashTable = NULL;

  pRuntimeEnv->pool = destroyResultRowPool(pRuntimeEnv->pool);
  taosArrayDestroyEx(pRuntimeEnv->prevResult, freeInterResult);
  pRuntimeEnv->prevResult = NULL;

  destroyOperatorInfo(pRuntimeEnv->proot);
}

static bool needBuildResAfterQueryComplete(SQInfo* pQInfo) {
  return pQInfo->rspContext != NULL;
}

bool isQueryKilled(SQInfo *pQInfo) {
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

void setQueryKilled(SQInfo *pQInfo) { pQInfo->code = TSDB_CODE_TSC_QUERY_CANCELLED;}

static bool isFixedOutputQuery(SQuery* pQuery) {
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    return false;
  }

  // Note:top/bottom query is fixed output query
  if (pQuery->topBotQuery || pQuery->groupbyColumn || isTsCompQuery(pQuery)) {
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

    if (!IS_MULTIOUTPUT(aAggs[pExprMsg->functionId].status)) {
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
static UNUSED_FUNC bool isSumAvgRateQuery(SQuery *pQuery) {
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
bool onlyQueryTags(SQuery* pQuery) {
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
    pQuery->checkResultBuf = 0;
  } else if (isGroupbyColumn(pQuery->pGroupbyExpr)) {
    pQuery->checkResultBuf = 0;
  } else {
    bool hasMultioutput = false;
    for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      SSqlFuncMsg *pExprMsg = &pQuery->pExpr1[i].base;
      if (pExprMsg->functionId == TSDB_FUNC_TS || pExprMsg->functionId == TSDB_FUNC_TS_DUMMY) {
        continue;
      }

      hasMultioutput = IS_MULTIOUTPUT(aAggs[pExprMsg->functionId].status);
      if (!hasMultioutput) {
        break;
      }
    }

    pQuery->checkResultBuf = hasMultioutput ? 1 : 0;
  }
}

/*
 * todo add more parameters to check soon..
 */
bool colIdCheck(SQuery *pQuery, void* qinfo) {
  // load data column information is incorrect
  for (int32_t i = 0; i < pQuery->numOfCols - 1; ++i) {
    if (pQuery->colList[i].colId == pQuery->colList[i + 1].colId) {
      qError("QInfo:%p invalid data load column for query", qinfo);
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

static void doExchangeTimeWindow(SQInfo* pQInfo, STimeWindow* win) {
  SQuery* pQuery = &pQInfo->query;
  size_t t = taosArrayGetSize(pQuery->tableGroupInfo.pGroupList);
  for(int32_t i = 0; i < t; ++i) {
    SArray* p1 = taosArrayGetP(pQuery->tableGroupInfo.pGroupList, i);

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

  if (isGroupbyColumn(pQuery->pGroupbyExpr) && pQuery->order.order == TSDB_ORDER_DESC) {
    pQuery->order.order = TSDB_ORDER_ASC;
    if (pQuery->window.skey > pQuery->window.ekey) {
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    doExchangeTimeWindow(pQInfo, &pQuery->window);
    return;
  }

  if (isPointInterpoQuery(pQuery) && pQuery->interval.interval == 0) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      qDebug(msg, pQInfo, "interp", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);
      SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
    }

    pQuery->order.order = TSDB_ORDER_ASC;
    return;
  }

  if (pQuery->interval.interval == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        qDebug(msg, pQInfo, "only-first", pQuery->order.order, TSDB_ORDER_ASC, pQuery->window.skey,
               pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

        SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
        doExchangeTimeWindow(pQInfo, &pQuery->window);
      }

      pQuery->order.order = TSDB_ORDER_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        qDebug(msg, pQInfo, "only-last", pQuery->order.order, TSDB_ORDER_DESC, pQuery->window.skey,
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
          qDebug(msg, pQInfo, "only-first stable", pQuery->order.order, TSDB_ORDER_ASC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
          doExchangeTimeWindow(pQInfo, &pQuery->window);
        }

        pQuery->order.order = TSDB_ORDER_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          qDebug(msg, pQInfo, "only-last stable", pQuery->order.order, TSDB_ORDER_DESC,
                 pQuery->window.skey, pQuery->window.ekey, pQuery->window.ekey, pQuery->window.skey);

          SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
          doExchangeTimeWindow(pQInfo, &pQuery->window);
        }

        pQuery->order.order = TSDB_ORDER_DESC;
      }
    }
  }
}

static void getIntermediateBufInfo(SQueryRuntimeEnv* pRuntimeEnv, int32_t* ps, int32_t* rowsize) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t MIN_ROWS_PER_PAGE = 4;

  *rowsize = (int32_t)(pQuery->resultRowSize * GET_ROW_PARAM_FOR_MULTIOUTPUT(pQuery, pQuery->topBotQuery, pQuery->stableQuery));
  int32_t overhead = sizeof(tFilePage);

  // one page contains at least two rows
  *ps = DEFAULT_INTERN_BUF_PAGE_SIZE;
  while(((*rowsize) * MIN_ROWS_PER_PAGE) > (*ps) - overhead) {
    *ps = (*ps << 1u);
  }

//  pRuntimeEnv->numOfRowsPerPage = ((*ps) - sizeof(tFilePage)) / (*rowsize);
//  assert(pRuntimeEnv->numOfRowsPerPage <= MAX_ROWS_PER_RESBUF_PAGE);
}

#define IS_PREFILTER_TYPE(_t) ((_t) != TSDB_DATA_TYPE_BINARY && (_t) != TSDB_DATA_TYPE_NCHAR)

static bool doFilterOnBlockStatistics(SQueryRuntimeEnv* pRuntimeEnv, SDataStatis *pDataStatis, SQLFunctionCtx *pCtx, int32_t numOfRows) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (pDataStatis == NULL || pQuery->numOfFilterCols == 0) {
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
        if (pFilterElem->fp == isNullOperator) {
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
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&minval, (char *)&maxval, TSDB_DATA_TYPE_FLOAT)) {
          return true;
        }
      }
    } else {
      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&pDataBlockst->min, (char *)&pDataBlockst->max, pFilterInfo->info.type)) {
          return true;
        }
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

static int32_t doTSJoinFilter(SQueryRuntimeEnv *pRuntimeEnv, TSKEY key, bool ascQuery) {
  STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);

#if defined(_DEBUG_VIEW)
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64 ", tag:%"PRIu64", query order:%d, ts order:%d, traverse:%d, index:%d\n",
         elem.ts, key, elem.tag.i64, pQuery->order.order, pRuntimeEnv->pTsBuf->tsOrder,
         pRuntimeEnv->pTsBuf->cur.order, pRuntimeEnv->pTsBuf->cur.tsIndex);
#endif

  if (ascQuery) {
    if (key < elem.ts) {
      return TS_JOIN_TS_NOT_EQUALS;
    } else if (key > elem.ts) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_INCONSISTAN);
    }
  } else {
    if (key > elem.ts) {
      return TS_JOIN_TS_NOT_EQUALS;
    } else if (key < elem.ts) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_INCONSISTAN);
    }
  }

  return TS_JOIN_TS_EQUAL;
}

void filterRowsInDataBlock(SQueryRuntimeEnv* pRuntimeEnv, SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols,
                        SSDataBlock* pBlock, STSBuf* pTsBuf, bool ascQuery) {
  int32_t numOfRows = pBlock->info.rows;

  int8_t *p = calloc(numOfRows, sizeof(int8_t));
  bool    all = true;

  if (pTsBuf != NULL) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, 0);

    TSKEY* k = (TSKEY*) pColInfoData->pData;
    for (int32_t i = 0; i < numOfRows; ++i) {
      int32_t offset = ascQuery? i:(numOfRows - i - 1);
      int32_t ret = doTSJoinFilter(pRuntimeEnv, k[offset], ascQuery);
      if (ret == TS_JOIN_TAG_NOT_EQUALS) {
        break;
      } else if (ret == TS_JOIN_TS_NOT_EQUALS) {
        all = false;
        continue;
      } else {
        assert(ret == TS_JOIN_TS_EQUAL);
        p[offset] = true;
      }

      if (!tsBufNextPos(pRuntimeEnv->pTsBuf)) {
        break;
      }
    }
  } else {
    for (int32_t i = 0; i < numOfRows; ++i) {
      bool qualified = false;

      for (int32_t k = 0; k < numOfFilterCols; ++k) {
        char* pElem = (char*)pFilterInfo[k].pData + pFilterInfo[k].info.bytes * i;

        qualified = false;
        for (int32_t j = 0; j < pFilterInfo[k].numOfFilters; ++j) {
          SColumnFilterElem* pFilterElem = &pFilterInfo[k].pFilters[j];

          bool isnull = isNull(pElem, pFilterInfo[k].info.type);
          if (isnull) {
            if (pFilterElem->fp == isNullOperator) {
              qualified = true;
              break;
            } else {
              continue;
            }
          } else {
            if (pFilterElem->fp == notNullOperator) {
              qualified = true;
              break;
            } else if (pFilterElem->fp == isNullOperator) {
              continue;
            }
          }

          if (pFilterElem->fp(pFilterElem, pElem, pElem, pFilterInfo[k].info.type)) {
            qualified = true;
            break;
          }
        }

        if (!qualified) {
          break;
        }
      }

      p[i] = qualified ? 1 : 0;
      if (!qualified) {
        all = false;
      }
    }
  }

  if (!all) {
    int32_t start = 0;
    int32_t len = 0;
    for (int32_t j = 0; j < numOfRows; ++j) {
      if (p[j] == 1) {
        len++;
      } else {
        if (len > 0) {
          int32_t cstart = j - len;
          for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
            SColumnInfoData *pColumnInfoData = taosArrayGet(pBlock->pDataBlock, i);

            int16_t bytes = pColumnInfoData->info.bytes;
            memmove(pColumnInfoData->pData + start * bytes, pColumnInfoData->pData + cstart * bytes, len * bytes);
          }

          start += len;
          len = 0;
        }
      }
    }

    if (len > 0) {
      int32_t cstart = numOfRows - len;
      for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
        SColumnInfoData *pColumnInfoData = taosArrayGet(pBlock->pDataBlock, i);

        int16_t bytes = pColumnInfoData->info.bytes;
        memmove(pColumnInfoData->pData + start * bytes, pColumnInfoData->pData + cstart * bytes, len * bytes);
      }

      start += len;
      len = 0;
    }

    pBlock->info.rows = start;
    pBlock->pBlockStatis = NULL;  // clean the block statistics info

    if (start > 0) {
      SColumnInfoData* pColumnInfoData = taosArrayGet(pBlock->pDataBlock, 0);
      assert(pColumnInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP &&
             pColumnInfoData->info.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX);

      pBlock->info.window.skey = *(int64_t*)pColumnInfoData->pData;
      pBlock->info.window.ekey = *(int64_t*)(pColumnInfoData->pData + TSDB_KEYSIZE * (start - 1));
    }
  }

  tfree(p);
}

static SColumnInfo* doGetTagColumnInfoById(SColumnInfo* pTagColList, int32_t numOfTags, int16_t colId);
static void doSetTagValueInParam(void* pTable, int32_t tagColId, tVariant *tag, int16_t type, int16_t bytes);

//TODO refactor
int32_t loadDataBlockOnDemand(SQueryRuntimeEnv* pRuntimeEnv, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock,
                              uint32_t* status) {
  *status = BLK_DATA_NO_NEEDED;
  pBlock->pDataBlock = NULL;
  pBlock->pBlockStatis = NULL;

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int64_t groupId = pQuery->current->groupIndex;

  SQInfo*         pQInfo = pRuntimeEnv->qinfo;
  SQueryCostInfo* pCost = &pQInfo->summary;

  if (pRuntimeEnv->pTsBuf != NULL && pQuery->stableQuery) {
    SExprInfo*   pExprInfo = &pTableScanInfo->pExpr[0];
    int16_t      tagId = (int16_t)pExprInfo->base.arg->argValue.i64;
    SColumnInfo* pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagId);

    // compare tag first
    tVariant t = {0};
    doSetTagValueInParam(pQuery->current->pTable, tagId, &t, pColInfo->type, pColInfo->bytes);

    STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);
    if (tsBufIsValidElem(&elem) && tVariantCompare(&t, elem.tag) == 0) {
      *status = BLK_DATA_ALL_NEEDED;
    } else {
      (*status) = BLK_DATA_DISCARD;
      return TSDB_CODE_SUCCESS;
    }
  }

  if (pQuery->numOfFilterCols > 0) {
    *status = BLK_DATA_ALL_NEEDED;
  } else {  // check if this data block is required to load
    // Calculate all time windows that are overlapping or contain current data block.
    // If current data block is contained by all possible time window, do not load current data block.
    if (QUERY_IS_INTERVAL_QUERY(pQuery) && overlapWithTimeWindow(pQuery, &pBlock->info)) {
      *status = BLK_DATA_ALL_NEEDED;
    }

    if ((*status) != BLK_DATA_ALL_NEEDED) {
      // the pCtx[i] result is belonged to previous time window since the outputBuf has not been set yet,
      // the filter result may be incorrect. So in case of interval query, we need to set the correct time output buffer
      if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
        SResultRow* pResult = NULL;

        bool        masterScan = IS_MASTER_SCAN(pRuntimeEnv);
        TSKEY       k = QUERY_IS_ASC_QUERY(pQuery) ? pBlock->info.window.skey : pBlock->info.window.ekey;
        STimeWindow win = getActiveTimeWindow(pTableScanInfo->pResultRowInfo, k, pQuery);
        if (setWindowOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, &win, masterScan, &pResult, groupId,
                                    pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                    pTableScanInfo->rowCellInfoOffset) != TSDB_CODE_SUCCESS) {
          // todo handle error in set result for timewindow
        }
      }

      int32_t         numOfOutput = pTableScanInfo->numOfOutput;
      SQLFunctionCtx* pCtx = pTableScanInfo->pCtx;

      if (pQuery->groupbyColumn) {
        (*status) = BLK_DATA_ALL_NEEDED;
      } else {
        for (int32_t i = 0; i < numOfOutput; ++i) {
          int32_t functionId = pCtx[i].functionId;
          int32_t colId = pTableScanInfo->pExpr[i].base.colInfo.colId;

          // group by + first/last should not apply the first/last block filter
          if (functionId != TSDB_FUNC_FIRST_DST && functionId != TSDB_FUNC_LAST_DST) {
            (*status) |= aAggs[functionId].dataReqFunc(&pTableScanInfo->pCtx[i], &pBlock->info.window, colId);
            if (((*status) & BLK_DATA_ALL_NEEDED) == BLK_DATA_ALL_NEEDED) {
              break;
            }
          } else {
            (*status) |= BLK_DATA_ALL_NEEDED;
            break;
          }
        }
      }
    }
  }

  SDataBlockInfo* pBlockInfo = &pBlock->info;

  if ((*status) == BLK_DATA_NO_NEEDED) {
    qDebug("QInfo:%p data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo, pBlockInfo->window.skey,
           pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->discardBlocks += 1;
  } else if ((*status) == BLK_DATA_STATIS_NEEDED) {
    // this function never returns error?
    pCost->loadBlockStatis += 1;
    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->pQueryHandle, &pBlock->pBlockStatis);

    if (pBlock->pBlockStatis == NULL) {  // data block statistics does not exist, load data block
      pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pQueryHandle, NULL);
      pCost->totalCheckedRows += pBlock->info.rows;
    }
  } else {
    assert((*status) == BLK_DATA_ALL_NEEDED);

    // load the data block statistics to perform further filter
    pCost->loadBlockStatis += 1;
    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->pQueryHandle, &pBlock->pBlockStatis);

    if (pQuery->topBotQuery && pBlock->pBlockStatis != NULL) {
      bool load = false;
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        int32_t functionId = pTableScanInfo->pCtx[i].functionId;
        if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
          load = topbot_datablock_filter(&pTableScanInfo->pCtx[i], (char*)&(pBlock->pBlockStatis[i].min),
                                         (char*)&(pBlock->pBlockStatis[i].max));
          if (!load) {
            // current block has been discard due to filter applied
            pCost->discardBlocks += 1;
            qDebug("QInfo:%p data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo,
                   pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
            (*status) = BLK_DATA_DISCARD;
            return TSDB_CODE_SUCCESS;
          }
        }
      }
    }

    // current block has been discard due to filter applied
    if (!doFilterOnBlockStatistics(pRuntimeEnv, pBlock->pBlockStatis, pTableScanInfo->pCtx, pBlockInfo->rows)) {
      pCost->discardBlocks += 1;
      qDebug("QInfo:%p data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo, pBlockInfo->window.skey,
             pBlockInfo->window.ekey, pBlockInfo->rows);
      (*status) = BLK_DATA_DISCARD;
      return TSDB_CODE_SUCCESS;
    }

    pCost->totalCheckedRows += pBlockInfo->rows;
    pCost->loadBlocks += 1;
    pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pQueryHandle, NULL);
    if (pBlock->pDataBlock == NULL) {
      return terrno;
    }

    if (pQuery->numOfFilterCols > 0 && pQuery->pFilterInfo[0].pData == NULL) {
      // set the initial static data value filter expression
      for (int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
        for (int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
          SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, j);

          if (pQuery->pFilterInfo[i].info.colId == pColInfo->info.colId) {
            pQuery->pFilterInfo[i].pData = pColInfo->pData;
            break;
          }
        }
      }
    }

    if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL) {
      filterRowsInDataBlock(pRuntimeEnv, pQuery->pFilterInfo, pQuery->numOfFilterCols, pBlock, pRuntimeEnv->pTsBuf,
                         QUERY_IS_ASC_QUERY(pQuery));
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

/*
 * set tag value in SQLFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(void* pTable, int32_t tagColId, tVariant *tag, int16_t type, int16_t bytes) {
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

void setTagValue(SOperatorInfo* pOperatorInfo, void *pTable, SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  SQueryRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;

  SExprInfo *pExpr  = pOperatorInfo->pExpr;
  SQuery    *pQuery = pRuntimeEnv->pQuery;

  SExprInfo* pExprInfo = &pExpr[0];
  if (pQuery->numOfOutput == 1 && pExprInfo->base.functionId == TSDB_FUNC_TS_COMP && pQuery->stableQuery) {
    assert(pExprInfo->base.numOfParams == 1);

    int16_t      tagColId = (int16_t)pExprInfo->base.arg->argValue.i64;
    SColumnInfo* pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagColId);

    doSetTagValueInParam(pTable, tagColId, &pCtx[0].tag, pColInfo->type, pColInfo->bytes);
  } else {
    // set tag value, by which the results are aggregated.
    int32_t offset = 0;
    memset(pRuntimeEnv->tagVal, 0, pQuery->tagLen);

    for (int32_t idx = 0; idx < numOfOutput; ++idx) {
      SExprInfo* pLocalExprInfo = &pExpr[idx];

      // ts_comp column required the tag value for join filter
      if (!TSDB_COL_IS_TAG(pLocalExprInfo->base.colInfo.flag)) {
        continue;
      }

      // todo use tag column index to optimize performance
      doSetTagValueInParam(pTable, pLocalExprInfo->base.colInfo.colId, &pCtx[idx].tag, pLocalExprInfo->type,
                           pLocalExprInfo->bytes);

      if (IS_NUMERIC_TYPE(pLocalExprInfo->type) || pLocalExprInfo->type == TSDB_DATA_TYPE_BOOL) {
        memcpy(pRuntimeEnv->tagVal + offset, &pCtx[idx].tag.i64, pLocalExprInfo->bytes);
      } else {
        memcpy(pRuntimeEnv->tagVal + offset, pCtx[idx].tag.pz, pCtx[idx].tag.nLen);
      }

      offset += pLocalExprInfo->bytes;
    }

    if (pQuery->stableQuery && pQuery->stabledev && (pRuntimeEnv->prevResult != NULL)) {
      //todo : use index to avoid iterator all possible output columns
      for(int32_t i = 0; i < numOfOutput; ++i) {
        if(pExpr[i].base.functionId != TSDB_FUNC_STDDEV_DST) {
          continue;
        }

        SSqlFuncMsg* pFuncMsg = &pExpr[i].base;

        pCtx[i].param[0].arr = NULL;
        pCtx[i].param[0].nType = TSDB_DATA_TYPE_INT; // avoid freeing the memory by setting the type to be int

        // TODO use hash to speedup this loop
        int32_t numOfGroup = (int32_t) taosArrayGetSize(pRuntimeEnv->prevResult);
        for(int32_t j = 0; j < numOfGroup; ++j) {
          SInterResult *p = taosArrayGet(pRuntimeEnv->prevResult, j);
          if (pQuery->tagLen == 0 || memcmp(p->tags, pRuntimeEnv->tagVal, pQuery->tagLen) == 0) {
            int32_t numOfCols = (int32_t) taosArrayGetSize(p->pResult);
            for(int32_t k = 0; k < numOfCols; ++k) {
              SStddevInterResult* pres = taosArrayGet(p->pResult, k);
              if (pres->colId == pFuncMsg->colInfo.colId) {
                pCtx[i].param[0].arr = pres->pResult;
                break;
              }
            }
          }
        }
      }
    }

    // set the join tag for first column
    SSqlFuncMsg* pFuncMsg = &pExprInfo->base;
    if (pQuery->stableQuery &&
       (pFuncMsg->functionId == TSDB_FUNC_TS || pFuncMsg->functionId == TSDB_FUNC_PRJ) &&
       (pRuntimeEnv->pTsBuf != NULL) &&
        pFuncMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      assert(pFuncMsg->numOfParams == 1);

      int16_t      tagColId = (int16_t)pExprInfo->base.arg->argValue.i64;
      SColumnInfo* pColInfo = doGetTagColumnInfoById(pQuery->tagColList, pQuery->numOfTags, tagColId);

      doSetTagValueInParam(pTable, tagColId, &pCtx[0].tag, pColInfo->type, pColInfo->bytes);

      int16_t tagType = pCtx[0].tag.nType;
      if (tagType == TSDB_DATA_TYPE_BINARY || tagType == TSDB_DATA_TYPE_NCHAR) {
        qDebug("QInfo:%p set tag value for join comparison, colId:%" PRId64 ", val:%s", pRuntimeEnv->qinfo,
               pExprInfo->base.arg->argValue.i64, pCtx[0].tag.pz);
      } else {
        qDebug("QInfo:%p set tag value for join comparison, colId:%" PRId64 ", val:%" PRId64, pRuntimeEnv->qinfo,
               pExprInfo->base.arg->argValue.i64, pCtx[0].tag.i64);
      }
    }
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

void copyToSDataBlock(SQueryRuntimeEnv* pRuntimeEnv, int32_t threshold, SSDataBlock* pBlock, int32_t* offset) {
  SGroupResInfo* pGroupResInfo = &pRuntimeEnv->groupResInfo;
  pBlock->info.rows = 0;

  int32_t code = TSDB_CODE_SUCCESS;
  while (pGroupResInfo->currentGroup < pGroupResInfo->totalGroup) {
    // all results in current group have been returned to client, try next group
    if ((pGroupResInfo->pRows == NULL) || taosArrayGetSize(pGroupResInfo->pRows) == 0) {
      assert(pGroupResInfo->index == 0);
      if ((code = mergeIntoGroupResult(&pRuntimeEnv->groupResInfo, pRuntimeEnv, offset)) != TSDB_CODE_SUCCESS) {
        return;
      }
    }

    doCopyToSDataBlock(pRuntimeEnv, pGroupResInfo, TSDB_ORDER_ASC, pBlock);

    // current data are all dumped to result buffer, clear it
    if (!hasRemainDataInCurrentGroup(pGroupResInfo)) {
      cleanupGroupResInfo(pGroupResInfo);
      if (!incNextGroup(pGroupResInfo)) {
        break;
      }
    }

      // enough results in data buffer, return
      if (pBlock->info.rows >= threshold) {
        break;
      }
    }

}

static void updateTableQueryInfoForReverseScan(SQuery *pQuery, STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  SWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, TSKEY);
  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;

  SWITCH_ORDER(pTableQueryInfo->cur.order);
  pTableQueryInfo->cur.vgroupIndex = -1;

  // set the index to be the end slot of result rows array
  pTableQueryInfo->resInfo.curIndex = pTableQueryInfo->resInfo.size - 1;
}

static void setupQueryRangeForReverseScan(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  int32_t numOfGroups = (int32_t)(GET_NUM_OF_TABLEGROUP(pRuntimeEnv));
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray *group = GET_TABLEGROUP(pRuntimeEnv, i);
    SArray *tableKeyGroup = taosArrayGetP(pQuery->tableGroupInfo.pGroupList, i);

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

void switchCtxOrder(SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SWITCH_ORDER(pCtx[i].order);
  }
}

int32_t initResultRow(SResultRow *pResultRow) {
  pResultRow->pCellInfo = (SResultRowCellInfo*)((char*)pResultRow + sizeof(SResultRow));
  pResultRow->pageId    = -1;
  pResultRow->offset    = -1;
  return TSDB_CODE_SUCCESS;
}

void setDefaultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SOptrBasicInfo *pInfo, int64_t uid) {
  SQLFunctionCtx* pCtx           = pInfo->pCtx;
  SSDataBlock* pDataBlock        = pInfo->pRes;
  int32_t* rowCellInfoOffset     = pInfo->rowCellInfoOffset;
  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;

  int32_t tid = 0;
  SResultRow* pRow = doPrepareResultRowFromKey(pRuntimeEnv, pResultRowInfo, (char *)&tid, sizeof(tid), true, uid);

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData* pData = taosArrayGet(pDataBlock->pDataBlock, i);

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT/TAGPRJ/PRJ/TAG etc.
     */
    SResultRowCellInfo* pCellInfo = getResultCell(pRow, i, rowCellInfoOffset);
    RESET_RESULT_INFO(pCellInfo);

    pCtx[i].resultInfo = pCellInfo;
    pCtx[i].pOutput = pData->pData;
    assert(pCtx[i].pOutput != NULL);

    // set the timestamp output buffer for top/bottom/diff query
    int32_t functionId = pCtx[i].functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx[i].ptsOutputBuf = pCtx[0].pOutput;
    }
  }

  initCtxOutputBuffer(pCtx, pDataBlock->info.numOfCols);
}

void updateOutputBuf(SArithOperatorInfo* pInfo, int32_t numOfInputRows) {
  SOptrBasicInfo* pBInfo = &pInfo->binfo;
  SSDataBlock* pDataBlock = pBInfo->pRes;

  int32_t newSize = pDataBlock->info.rows + numOfInputRows;
  if (pInfo->bufCapacity < newSize) {
    for(int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
      SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
      char* p = realloc(pColInfo->pData, newSize * pColInfo->info.bytes);
      if (p != NULL) {
        pColInfo->pData = p;

        // it starts from the tail of the previously generated results.
        pBInfo->pCtx[i].pOutput = pColInfo->pData;
        pInfo->bufCapacity = newSize;
      } else {
        // longjmp
      }
    }
  }

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
    pBInfo->pCtx[i].pOutput = pColInfo->pData + pColInfo->info.bytes * pDataBlock->info.rows;

    // re-estabilish output buffer pointer.
    int32_t functionId = pBInfo->pCtx[i].functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pBInfo->pCtx[i].ptsOutputBuf = pBInfo->pCtx[0].pOutput;
    }
  }
}

void initCtxOutputBuffer(SQLFunctionCtx* pCtx, int32_t size) {
  for (int32_t j = 0; j < size; ++j) {
    SResultRowCellInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (pResInfo->initialized) {
      continue;
    }

    aAggs[pCtx[j].functionId].init(&pCtx[j]);
  }
}

void setQueryStatus(SQueryRuntimeEnv *pRuntimeEnv, int8_t status) {
  if (status == QUERY_NOT_COMPLETED) {
    pRuntimeEnv->status = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    CLEAR_QUERY_STATUS(pRuntimeEnv, QUERY_NOT_COMPLETED);
    pRuntimeEnv->status |= status;
  }
}

static void setupEnvForReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pRuntimeEnv->pTsBuf) {
    SWITCH_ORDER(pRuntimeEnv->pTsBuf->cur.order);
    bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
    assert(ret);
  }

  // reverse order time range
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);

  SET_REVERSE_SCAN_FLAG(pRuntimeEnv);
  setQueryStatus(pRuntimeEnv, QUERY_NOT_COMPLETED);

  switchCtxOrder(pCtx, numOfOutput);
  SWITCH_ORDER(pQuery->order.order);

  setupQueryRangeForReverseScan(pRuntimeEnv);
}

void finalizeQueryResult(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset) {
  SQueryRuntimeEnv *pRuntimeEnv = pOperator->pRuntimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t numOfOutput = pOperator->numOfOutput;
  if (pQuery->groupbyColumn || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    // for each group result, call the finalize function for each column
    if (pQuery->groupbyColumn) {
      closeAllResultRows(pResultRowInfo);
    }

    for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
      SResultRow *buf = pResultRowInfo->pResult[i];
      if (!isResultRowClosed(pResultRowInfo, i)) {
        continue;
      }

      setResultOutputBuf(pRuntimeEnv, buf, pCtx, numOfOutput, rowCellInfoOffset);

      for (int32_t j = 0; j < numOfOutput; ++j) {
        aAggs[pCtx[j].functionId].xFinalize(&pCtx[j]);
      }

      /*
       * set the number of output results for group by normal columns, the number of output rows usually is 1 except
       * the top and bottom query
       */
      buf->numOfRows = (uint16_t)getNumOfResult(pRuntimeEnv, pCtx, numOfOutput);
    }

  } else {
    for (int32_t j = 0; j < numOfOutput; ++j) {
      aAggs[pCtx[j].functionId].xFinalize(&pCtx[j]);
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

static STableQueryInfo *createTableQueryInfo(SQuery* pQuery, void* pTable, bool groupbyColumn, STimeWindow win, void* buf) {
  STableQueryInfo *pTableQueryInfo = buf;

  pTableQueryInfo->win = win;
  pTableQueryInfo->lastKey = win.skey;

  pTableQueryInfo->pTable = pTable;
  pTableQueryInfo->cur.vgroupIndex = -1;

  // set more initial size of interval/groupby query
  if (QUERY_IS_INTERVAL_QUERY(pQuery) || groupbyColumn) {
    int32_t initialSize = 128;
    int32_t code = initResultRowInfo(&pTableQueryInfo->resInfo, initialSize, TSDB_DATA_TYPE_INT);
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
  cleanupResultRowInfo(&pTableQueryInfo->resInfo);
}

void setResultRowOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx,
    int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  tFilePage* bufPage = getResBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  int16_t offset = 0;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);
    if (pCtx->resultInfo->initialized && pCtx->resultInfo->complete) {
      offset += pCtx[i].outputBytes;
      continue;
    }

    pCtx[i].pOutput = getPosInResultPage(pRuntimeEnv->pQuery, bufPage, pResult->offset, offset);
    offset += pCtx[i].outputBytes;

    int32_t functionId = pCtx[i].functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx[i].ptsOutputBuf = pCtx[0].pOutput;
    }

    if (!pCtx[i].resultInfo->initialized) {
      aAggs[functionId].init(&pCtx[i]);
    }
  }
}

void setExecutionContext(SQueryRuntimeEnv* pRuntimeEnv, SOptrBasicInfo* pInfo, int32_t numOfOutput, int32_t groupIndex,
                         TSKEY nextKey) {
  STableQueryInfo  *pTableQueryInfo = pRuntimeEnv->pQuery->current;

  // lastKey needs to be updated
  pTableQueryInfo->lastKey = nextKey;
  if (pRuntimeEnv->prevGroupId != INT32_MIN && pRuntimeEnv->prevGroupId == groupIndex) {
    return;
  }

  int64_t uid = 0;
  SResultRow *pResultRow = doPrepareResultRowFromKey(pRuntimeEnv, &pInfo->resultRowInfo, (char *)&groupIndex,
      sizeof(groupIndex), true, uid);
  assert (pResultRow != NULL);

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pResultRow->pageId == -1) {
    if (addNewWindowResultBuf(pResultRow, pRuntimeEnv->pResultBuf, groupIndex, pRuntimeEnv->pQuery->resultRowSize) !=
        TSDB_CODE_SUCCESS) {
      return;
    }
  }

  // record the current active group id
  pRuntimeEnv->prevGroupId = groupIndex;
  setResultOutputBuf(pRuntimeEnv, pResultRow, pInfo->pCtx, numOfOutput, pInfo->rowCellInfoOffset);
  initCtxOutputBuffer(pInfo->pCtx, numOfOutput);
}

void setResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx,
    int32_t numOfCols, int32_t* rowCellInfoOffset) {
  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  int16_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pCtx[i].pOutput = getPosInResultPage(pRuntimeEnv->pQuery, page, pResult->offset, offset);
    offset += pCtx[i].outputBytes;

    int32_t functionId = pCtx[i].functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx[i].ptsOutputBuf = pCtx[0].pOutput;
    }

    /*
     * set the output buffer information and intermediate buffer,
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);
  }
}

//int32_t setTimestampListJoinInfo(SQueryRuntimeEnv* pRuntimeEnv, STableQueryInfo *pTableQueryInfo) {
//  SQuery* pQuery = pRuntimeEnv->pQuery;
//
//  assert(pRuntimeEnv->pTsBuf != NULL);
//
//  // both the master and supplement scan needs to set the correct ts comp start position
//  tVariant* pTag = &pRuntimeEnv->pCtx[0].tag;
//
//  if (pTableQueryInfo->cur.vgroupIndex == -1) {
//    tVariantAssign(&pTableQueryInfo->tag, pTag);
//
//    STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQuery->vgId, &pTableQueryInfo->tag);
//
//    // failed to find data with the specified tag value and vnodeId
//    if (!tsBufIsValidElem(&elem)) {
//      if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
//        qError("QInfo:%p failed to find tag:%s in ts_comp", pRuntimeEnv->qinfo, pTag->pz);
//      } else {
//        qError("QInfo:%p failed to find tag:%" PRId64 " in ts_comp", pRuntimeEnv->qinfo, pTag->i64);
//      }
//
//      return false;
//    }
//
//    // keep the cursor info of current meter
//    pTableQueryInfo->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
//    if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
//      qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pRuntimeEnv->qinfo, pTag->pz, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
//    } else {
//      qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pRuntimeEnv->qinfo, pTag->i64, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
//    }
//
//  } else {
//    tsBufSetCursor(pRuntimeEnv->pTsBuf, &pTableQueryInfo->cur);
//
//    if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
//      qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pRuntimeEnv->qinfo, pTag->pz, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
//    } else {
//      qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pRuntimeEnv->qinfo, pTag->i64, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
//    }
//  }
//
//  return 0;
//}

int32_t setParamValue(SQueryRuntimeEnv* pRuntimeEnv, SQLFunctionCtx* pCtx, int32_t numOfOutput,
    SExprInfo* pExpr) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (pRuntimeEnv->prevResult == NULL || pQuery->groupbyColumn) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfExprs = pQuery->numOfOutput;
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExprInfo = &(pExpr[i]);
    if(pExprInfo->base.functionId != TSDB_FUNC_STDDEV_DST) {
      continue;
    }

    SSqlFuncMsg* pFuncMsg = &pExprInfo->base;

    pCtx[i].param[0].arr = NULL;
    pCtx[i].param[0].nType = TSDB_DATA_TYPE_INT; // avoid freeing the memory by setting the type to be int

    // TODO use hash to speedup this loop
    int32_t numOfGroup = (int32_t) taosArrayGetSize(pRuntimeEnv->prevResult);
    for(int32_t j = 0; j < numOfGroup; ++j) {
      SInterResult *p = taosArrayGet(pRuntimeEnv->prevResult, j);
      if (pQuery->tagLen == 0 || memcmp(p->tags, pRuntimeEnv->tagVal, pQuery->tagLen) == 0) {
        int32_t numOfCols = (int32_t) taosArrayGetSize(p->pResult);
        for(int32_t k = 0; k < numOfCols; ++k) {
          SStddevInterResult* pres = taosArrayGet(p->pResult, k);
          if (pres->colId == pFuncMsg->colInfo.colId) {
            pCtx[i].param[0].arr = pres->pResult;
            break;
          }
        }
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
void setIntervalQueryRange(SQueryRuntimeEnv *pRuntimeEnv, TSKEY key) {
  SQuery           *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo  *pTableQueryInfo = pQuery->current;
  SResultRowInfo   *pWindowResInfo = &pTableQueryInfo->resInfo;

  if (pWindowResInfo->prevSKey != TSKEY_INITIAL_VAL) {
    return;
  }

  pTableQueryInfo->win.skey = key;
  STimeWindow win = {.skey = key, .ekey = pQuery->window.ekey};

  /**
   * In handling the both ascending and descending order super table query, we need to find the first qualified
   * timestamp of this table, and then set the first qualified start timestamp.
   * In ascending query, the key is the first qualified timestamp. However, in the descending order query, additional
   * operations involve.
   */
  STimeWindow w = TSWINDOW_INITIALIZER;

  TSKEY sk = MIN(win.skey, win.ekey);
  TSKEY ek = MAX(win.skey, win.ekey);
  getAlignQueryTimeWindow(pQuery, win.skey, sk, ek, &w);

  if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      assert(win.ekey == pQuery->window.ekey);
    }

    pWindowResInfo->prevSKey = w.skey;
  }

  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;
}

bool requireTimestamp(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutput; i++) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if ((aAggs[functionId].status & TSDB_FUNCSTATE_NEED_TS) != 0) {
      return true;
    }
  }
  return false;
}

/**
 * copyToOutputBuf support copy data in ascending/descending order
 * For interval query of both super table and table, copy the data in ascending order, since the output results are
 * ordered in SWindowResutl already. While handling the group by query for both table and super table,
 * all group result are completed already.
 *
 * @param pQInfo
 * @param result
 */

static int32_t doCopyToSDataBlock(SQueryRuntimeEnv* pRuntimeEnv, SGroupResInfo* pGroupResInfo, int32_t orderType, SSDataBlock* pBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
  int32_t numOfResult = pBlock->info.rows; // there are already exists result rows

  int32_t start = 0;
  int32_t step = -1;

  qDebug("QInfo:%p start to copy data from windowResInfo to output buf", pRuntimeEnv->qinfo);
  if (orderType == TSDB_ORDER_ASC) {
    start = pGroupResInfo->index;
    step = 1;
  } else {  // desc order copy all data
    start = numOfRows - pGroupResInfo->index - 1;
    step = -1;
  }

  for (int32_t i = start; (i < numOfRows) && (i >= 0); i += step) {
    SResultRow* pRow = taosArrayGetP(pGroupResInfo->pRows, i);
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }

    int32_t numOfRowsToCopy = pRow->numOfRows;

    pGroupResInfo->index += 1;

    tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pRow->pageId);

    int16_t offset = 0;
    for (int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, j);
      int32_t bytes = pColInfoData->info.bytes;

      char *out = pColInfoData->pData + numOfResult * bytes;
      char *in  = getPosInResultPage(pQuery, page, pRow->offset, offset);
      memcpy(out, in, bytes * numOfRowsToCopy);

      offset += bytes;
    }

    numOfResult += numOfRowsToCopy;
    if (numOfResult == pRuntimeEnv->resultInfo.capacity) {  // output buffer is full
      break;
    }
  }

  qDebug("QInfo:%p copy data to query buf completed", pRuntimeEnv->qinfo);
  pBlock->info.rows = numOfResult;
  return 0;
}

static void toSSDataBlock(SGroupResInfo *pGroupResInfo, SQueryRuntimeEnv* pRuntimeEnv, SSDataBlock* pBlock) {
  assert(pGroupResInfo->currentGroup <= pGroupResInfo->totalGroup);

  pBlock->info.rows = 0;
  if (!hasRemainDataInCurrentGroup(pGroupResInfo)) {
    return;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSDB_ORDER_ASC;
  doCopyToSDataBlock(pRuntimeEnv, pGroupResInfo, orderType, pBlock);

  SColumnInfoData* pInfoData = taosArrayGet(pBlock->pDataBlock, 0);

  if (pInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    STimeWindow* w = &pBlock->info.window;
    w->skey = *(int64_t*)pInfoData->pData;
    w->ekey = *(int64_t*)(pInfoData->pData + TSDB_KEYSIZE * (pBlock->info.rows - 1));
  }
}

static void updateNumOfRowsInResultRows(SQueryRuntimeEnv *pRuntimeEnv,
    SQLFunctionCtx* pCtx, int32_t numOfOutput, SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // update the number of result for each, only update the number of rows for the corresponding window result.
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    return;
  }

  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRow *pResult = pResultRowInfo->pResult[i];

    for (int32_t j = 0; j < numOfOutput; ++j) {
      int32_t functionId = pCtx[j].functionId;
      if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
        continue;
      }

      SResultRowCellInfo* pCell = getResultCell(pResult, j, rowCellInfoOffset);
      pResult->numOfRows = (uint16_t)(MAX(pResult->numOfRows, pCell->numOfRes));
    }
  }
}

static void doCopyQueryResultToMsg(SQInfo *pQInfo, int32_t numOfRows, char *data) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;

  SSDataBlock* pRes = pRuntimeEnv->outputBuf;

  if (pQuery->pExpr2 == NULL) {
    for (int32_t col = 0; col < pQuery->numOfOutput; ++col) {
      SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, col);
      memmove(data, pColRes->pData, pColRes->info.bytes * pRes->info.rows);
      data += pColRes->info.bytes * pRes->info.rows;
    }
  } else {
    for (int32_t col = 0; col < pQuery->numOfExpr2; ++col) {
      SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, col);
      memmove(data, pColRes->pData, pColRes->info.bytes * numOfRows);
      data += pColRes->info.bytes * numOfRows;
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
  if (Q_STATUS_EQUAL(pRuntimeEnv->status, QUERY_COMPLETED) && pRuntimeEnv->proot->status == OP_EXEC_DONE) {
    setQueryStatus(pRuntimeEnv, QUERY_OVER);
  }
}

int32_t doFillTimeIntervalGapsInResults(SFillInfo* pFillInfo, SSDataBlock *pOutput, int32_t capacity) {
  void** p = calloc(pFillInfo->numOfCols, POINTER_BYTES);
  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pOutput->pDataBlock, i);
    p[i] = pColInfoData->pData;
  }

  pOutput->info.rows = (int32_t)taosFillResultDataBlock(pFillInfo, p, capacity);
  tfree(p);
  return pOutput->info.rows;
}

void queryCostStatis(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQueryCostInfo *pSummary = &pQInfo->summary;

  uint64_t hashSize = taosHashGetMemSize(pQInfo->runtimeEnv.pResultRowHashTable);
  hashSize += taosHashGetMemSize(pRuntimeEnv->tableqinfoGroupInfo.map);
  pSummary->hashSize = hashSize;

  // add the merge time
  pSummary->elapsedTime += pSummary->firstStageMergeTime;

  SResultRowPool* p = pQInfo->runtimeEnv.pool;
  if (p != NULL) {
    pSummary->winInfoSize = getResultRowPoolMemSize(p);
    pSummary->numOfTimeWindows = getNumOfAllocatedResultRows(p);
  } else {
    pSummary->winInfoSize = 0;
    pSummary->numOfTimeWindows = 0;
  }

  qDebug("QInfo:%p :cost summary: elapsed time:%"PRId64" us, first merge:%"PRId64" us, total blocks:%d, "
         "load block statis:%d, load data block:%d, total rows:%"PRId64 ", check rows:%"PRId64,
         pQInfo, pSummary->elapsedTime, pSummary->firstStageMergeTime, pSummary->totalBlocks, pSummary->loadBlockStatis,
         pSummary->loadBlocks, pSummary->totalRows, pSummary->totalCheckedRows);

  qDebug("QInfo:%p :cost summary: winResPool size:%.2f Kb, numOfWin:%"PRId64", tableInfoSize:%.2f Kb, hashTable:%.2f Kb", pQInfo, pSummary->winInfoSize/1024.0,
      pSummary->numOfTimeWindows, pSummary->tableInfoSize/1024.0, pSummary->hashSize/1024.0);
}

//static void updateOffsetVal(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pBlockInfo) {
//  SQuery *pQuery = pRuntimeEnv->pQuery;
//  STableQueryInfo* pTableQueryInfo = pQuery->current;
//
//  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
//
//  if (pQuery->limit.offset == pBlockInfo->rows) {  // current block will ignore completed
//    pTableQueryInfo->lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pBlockInfo->window.ekey + step : pBlockInfo->window.skey + step;
//    pQuery->limit.offset = 0;
//    return;
//  }
//
//  if (QUERY_IS_ASC_QUERY(pQuery)) {
//    pQuery->pos = (int32_t)pQuery->limit.offset;
//  } else {
//    pQuery->pos = pBlockInfo->rows - (int32_t)pQuery->limit.offset - 1;
//  }
//
//  assert(pQuery->pos >= 0 && pQuery->pos <= pBlockInfo->rows - 1);
//
//  SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
//  SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//  // update the pQuery->limit.offset value, and pQuery->pos value
//  TSKEY *keys = (TSKEY *) pColInfoData->pData;
//
//  // update the offset value
//  pTableQueryInfo->lastKey = keys[pQuery->pos];
//  pQuery->limit.offset = 0;
//
//  int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
//
//  qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%"PRId64, pRuntimeEnv->qinfo,
//         pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes, pQuery->current->lastKey);
//}

static void freeTableBlockDist(STableBlockDist *pTableBlockDist) {
  if (pTableBlockDist != NULL) {
    taosArrayDestroy(pTableBlockDist->dataBlockInfos); 
    free(pTableBlockDist);
  }
}

#if 0
static int32_t getPercentileFromSortedArray(const SArray* pArray, double rate) {
  int32_t len = (int32_t)taosArrayGetSize(pArray);
  if (len <= 0) {
    return 0;
  }
  assert(rate >= 0 && rate <= 1.0);
  int idx = (int32_t)((len - 1) * rate);
  return ((SDataBlockInfo *)(taosArrayGet(pArray, idx)))->rows;
}


static int compareBlockInfo(const void *pLeft, const void *pRight) {
  int32_t left = ((SDataBlockInfo *)pLeft)->rows;
  int32_t right = ((SDataBlockInfo *)pRight)->rows; 
  if (left > right) return 1; 
  if (left < right) return -1; 
  return 0;
}

#endif

//void skipBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
//  SQuery *pQuery = pRuntimeEnv->pQuery;
//
//  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0) {
//    return;
//  }
//
//  pQuery->pos = 0;
//  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
//
//  STableQueryInfo* pTableQueryInfo = pQuery->current;
//  TsdbQueryHandleT pQueryHandle = pRuntimeEnv->pQueryHandle;
//
//  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
//  while (tsdbNextDataBlock(pQueryHandle)) {
//    if (isQueryKilled(pRuntimeEnv->qinfo)) {
//      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
//    }
//
//    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
//
//    if (pQuery->limit.offset > blockInfo.rows) {
//      pQuery->limit.offset -= blockInfo.rows;
//      pTableQueryInfo->lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.window.ekey : blockInfo.window.skey;
//      pTableQueryInfo->lastKey += step;
//
//      qDebug("QInfo:%p skip rows:%d, offset:%" PRId64, pRuntimeEnv->qinfo, blockInfo.rows,
//             pQuery->limit.offset);
//    } else {  // find the appropriated start position in current block
//      updateOffsetVal(pRuntimeEnv, &blockInfo);
//      break;
//    }
//  }
//
//  if (terrno != TSDB_CODE_SUCCESS) {
//    longjmp(pRuntimeEnv->env, terrno);
//  }
//}

//static TSKEY doSkipIntervalProcess(SQueryRuntimeEnv* pRuntimeEnv, STimeWindow* win, SDataBlockInfo* pBlockInfo, STableQueryInfo* pTableQueryInfo) {
//  SQuery *pQuery = pRuntimeEnv->pQuery;
//  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
//
//  assert(pQuery->limit.offset == 0);
//  STimeWindow tw = *win;
//  getNextTimeWindow(pQuery, &tw);
//
//  if ((tw.skey <= pBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
//      (tw.ekey >= pBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
//
//    // load the data block and check data remaining in current data block
//    // TODO optimize performance
//    SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
//    SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//    tw = *win;
//    int32_t startPos =
//        getNextQualifiedWindow(pQuery, &tw, pBlockInfo, pColInfoData->pData, binarySearchForKey, -1);
//    assert(startPos >= 0);
//
//    // set the abort info
//    pQuery->pos = startPos;
//
//    // reset the query start timestamp
//    pTableQueryInfo->win.skey = ((TSKEY *)pColInfoData->pData)[startPos];
//    pQuery->window.skey = pTableQueryInfo->win.skey;
//    TSKEY key = pTableQueryInfo->win.skey;
//
//    pWindowResInfo->prevSKey = tw.skey;
//    int32_t index = pRuntimeEnv->resultRowInfo.curIndex;
//
//    int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
//    pRuntimeEnv->resultRowInfo.curIndex = index;  // restore the window index
//
//    qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%" PRId64,
//           pRuntimeEnv->qinfo, pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes,
//           pQuery->current->lastKey);
//
//    return key;
//  } else {  // do nothing
//    pQuery->window.skey      = tw.skey;
//    pWindowResInfo->prevSKey = tw.skey;
//    pTableQueryInfo->lastKey = tw.skey;
//
//    return tw.skey;
//  }
//
//  return true;
//}

//static bool skipTimeInterval(SQueryRuntimeEnv *pRuntimeEnv, TSKEY* start) {
//  SQuery *pQuery = pRuntimeEnv->pQuery;
//  if (QUERY_IS_ASC_QUERY(pQuery)) {
//    assert(*start <= pQuery->current->lastKey);
//  } else {
//    assert(*start >= pQuery->current->lastKey);
//  }
//
//  // if queried with value filter, do NOT forward query start position
//  if (pQuery->limit.offset <= 0 || pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pRuntimeEnv->pFillInfo != NULL) {
//    return true;
//  }
//
//  /*
//   * 1. for interval without interpolation query we forward pQuery->interval.interval at a time for
//   *    pQuery->limit.offset times. Since hole exists, pQuery->interval.interval*pQuery->limit.offset value is
//   *    not valid. otherwise, we only forward pQuery->limit.offset number of points
//   */
//  assert(pRuntimeEnv->resultRowInfo.prevSKey == TSKEY_INITIAL_VAL);
//
//  STimeWindow w = TSWINDOW_INITIALIZER;
//  bool ascQuery = QUERY_IS_ASC_QUERY(pQuery);
//
//  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
//  STableQueryInfo *pTableQueryInfo = pQuery->current;
//
//  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
//  while (tsdbNextDataBlock(pRuntimeEnv->pQueryHandle)) {
//    tsdbRetrieveDataBlockInfo(pRuntimeEnv->pQueryHandle, &blockInfo);
//
//    if (QUERY_IS_ASC_QUERY(pQuery)) {
//      if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
//        getAlignQueryTimeWindow(pQuery, blockInfo.window.skey, blockInfo.window.skey, pQuery->window.ekey, &w);
//        pWindowResInfo->prevSKey = w.skey;
//      }
//    } else {
//      getAlignQueryTimeWindow(pQuery, blockInfo.window.ekey, pQuery->window.ekey, blockInfo.window.ekey, &w);
//      pWindowResInfo->prevSKey = w.skey;
//    }
//
//    // the first time window
//    STimeWindow win = getActiveTimeWindow(pWindowResInfo, pWindowResInfo->prevSKey, pQuery);
//
//    while (pQuery->limit.offset > 0) {
//      STimeWindow tw = win;
//
//      if ((win.ekey <= blockInfo.window.ekey && ascQuery) || (win.ekey >= blockInfo.window.skey && !ascQuery)) {
//        pQuery->limit.offset -= 1;
//        pWindowResInfo->prevSKey = win.skey;
//
//        // current time window is aligned with blockInfo.window.ekey
//        // restart it from next data block by set prevSKey to be TSKEY_INITIAL_VAL;
//        if ((win.ekey == blockInfo.window.ekey && ascQuery) || (win.ekey == blockInfo.window.skey && !ascQuery)) {
//          pWindowResInfo->prevSKey = TSKEY_INITIAL_VAL;
//        }
//      }
//
//      if (pQuery->limit.offset == 0) {
//        *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
//        return true;
//      }
//
//      // current window does not ended in current data block, try next data block
//      getNextTimeWindow(pQuery, &tw);
//
//      /*
//       * If the next time window still starts from current data block,
//       * load the primary timestamp column first, and then find the start position for the next queried time window.
//       * Note that only the primary timestamp column is required.
//       * TODO: Optimize for this cases. All data blocks are not needed to be loaded, only if the first actually required
//       * time window resides in current data block.
//       */
//      if ((tw.skey <= blockInfo.window.ekey && ascQuery) || (tw.ekey >= blockInfo.window.skey && !ascQuery)) {
//
//        SArray *pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
//        SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//        if ((win.ekey > blockInfo.window.ekey && ascQuery) || (win.ekey < blockInfo.window.skey && !ascQuery)) {
//          pQuery->limit.offset -= 1;
//        }
//
//        if (pQuery->limit.offset == 0) {
//          *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
//          return true;
//        } else {
//          tw = win;
//          int32_t startPos =
//              getNextQualifiedWindow(pQuery, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey, -1);
//          assert(startPos >= 0);
//
//          // set the abort info
//          pQuery->pos = startPos;
//          pTableQueryInfo->lastKey = ((TSKEY *)pColInfoData->pData)[startPos];
//          pWindowResInfo->prevSKey = tw.skey;
//          win = tw;
//        }
//      } else {
//        break;  // offset is not 0, and next time window begins or ends in the next block.
//      }
//    }
//  }
//
//  // check for error
//  if (terrno != TSDB_CODE_SUCCESS) {
//    longjmp(pRuntimeEnv->env, terrno);
//  }
//
//  return true;
//}

static void doDestroyTableQueryInfo(STableGroupInfo* pTableqinfoGroupInfo);

static void setTableQueryHandle(SQueryRuntimeEnv* pRuntimeEnv, int32_t tableIndex) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  int32_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);

  STableQueryInfo* pCheckInfo = NULL;
  if (numOfGroup == 1) {
    SArray *group = GET_TABLEGROUP(pRuntimeEnv, 0);
    pCheckInfo = taosArrayGetP(group, tableIndex);
  } else {
    assert(numOfGroup == pRuntimeEnv->tableqinfoGroupInfo.numOfTables);
    SArray *group = GET_TABLEGROUP(pRuntimeEnv, tableIndex);
    pCheckInfo = taosArrayGetP(group, 0);
  }

  // handle the first table
  STsdbQueryCond cond = {
      .twindow   = {pCheckInfo->lastKey, pCheckInfo->win.ekey},
      .order     = pQuery->order.order,
      .colList   = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
      .loadExternalRows = isPointInterpoQuery(pQuery),
  };

  SArray *g1 = taosArrayInit(1, POINTER_BYTES);
  SArray *tx = taosArrayInit(1, sizeof(STableKeyInfo));

  STableKeyInfo info = {.pTable = pCheckInfo->pTable, .lastKey = pCheckInfo->lastKey};
  taosArrayPush(tx, &info);

  taosArrayPush(g1, &tx);
  STableGroupInfo gp = {.numOfTables = 1, .pGroupList = g1};

  if (pRuntimeEnv->pQueryHandle == NULL) {
    pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQuery->tsdb, &cond, &gp, pRuntimeEnv->qinfo, &pQuery->memRef);
  } else {
    tsdbResetQueryHandleForNewTable(pRuntimeEnv->pQueryHandle, &cond, &gp);
  }

  taosArrayDestroy(tx);
  taosArrayDestroy(g1);
}

static int32_t setupQueryHandle(void* tsdb, SQInfo* pQInfo, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  // TODO set the tags scan handle
  if (onlyQueryTags(pQuery)) {
    return TSDB_CODE_SUCCESS;
  } else if (isTsCompQuery(pQuery)) {
    setTableQueryHandle(pRuntimeEnv, 0);
    return terrno;
  }

  STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

  if (!isSTableQuery
    && (pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 1)
    && (cond.order == TSDB_ORDER_ASC)
    && (!QUERY_IS_INTERVAL_QUERY(pQuery))
    && (!isGroupbyColumn(pQuery->pGroupbyExpr))
    && (!isFixedOutputQuery(pQuery))
  ) {
    SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);
    STableQueryInfo* pCheckInfo = taosArrayGetP(pa, 0);
    cond.twindow = pCheckInfo->win;
  }

  terrno = TSDB_CODE_SUCCESS;
  if (isFirstLastRowQuery(pQuery)) {
    pRuntimeEnv->pQueryHandle = tsdbQueryLastRow(tsdb, &cond, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);

    // update the query time window
    pQuery->window = cond.twindow;
    if (pQuery->tableGroupInfo.numOfTables == 0) {
      pRuntimeEnv->tableqinfoGroupInfo.numOfTables = 0;
    } else {
      size_t numOfGroups = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
      for(int32_t i = 0; i < numOfGroups; ++i) {
        SArray *group = GET_TABLEGROUP(pRuntimeEnv, i);

        size_t t = taosArrayGetSize(group);
        for (int32_t j = 0; j < t; ++j) {
          STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);

          pCheckInfo->win = pQuery->window;
          pCheckInfo->lastKey = pCheckInfo->win.skey;
        }
      }
    }
  } else if (isPointInterpoQuery(pQuery)) {
    pRuntimeEnv->pQueryHandle = tsdbQueryRowsInExternalWindow(tsdb, &cond, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);
  } else {
    pRuntimeEnv->pQueryHandle = tsdbQueryTables(tsdb, &cond, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);
  }

  return terrno;
}

static SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfOutput, int64_t* fillVal) {
  int32_t offset = 0;

  SFillColInfo* pFillCol = calloc(numOfOutput, sizeof(SFillColInfo));
  if (pFillCol == NULL) {
    return NULL;
  }

  for(int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo   = &pExpr[i];

    pFillCol[i].col.bytes  = pExprInfo->bytes;
    pFillCol[i].col.type   = (int8_t)pExprInfo->type;
    pFillCol[i].col.offset = offset;
    pFillCol[i].tagIndex   = -2;
    pFillCol[i].flag       = TSDB_COL_NORMAL;    // always be ta normal column for table query
    pFillCol[i].functionId = pExprInfo->base.functionId;
    pFillCol[i].fillVal.i  = fillVal[i];

    offset += pExprInfo->bytes;
  }

  return pFillCol;
}

int32_t doInitQInfo(SQInfo *pQInfo, STSBuf *pTsBuf, SArray* prevResult, void *tsdb, int32_t vgId, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  pQuery->tsdb  = tsdb;

  pQuery->topBotQuery = isTopBottomQuery(pQuery);
  pQuery->hasTagResults = hasTagValOutput(pQuery);
  pQuery->timeWindowInterpo = timeWindowInterpoRequired(pQuery);
  pQuery->stabledev = isStabledev(pQuery);

  pRuntimeEnv->prevResult = prevResult;
  pRuntimeEnv->qinfo = pQInfo;

  setScanLimitationByResultBuffer(pQuery);

  int32_t code = setupQueryHandle(tsdb, pQInfo, isSTableQuery);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pQuery->tsdb = tsdb;
  pQuery->vgId = vgId;
  pQuery->stableQuery = isSTableQuery;
  pQuery->groupbyColumn = isGroupbyColumn(pQuery->pGroupbyExpr);

  pRuntimeEnv->groupResInfo.totalGroup = isSTableQuery? GET_NUM_OF_TABLEGROUP(pRuntimeEnv):0;

  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pTsBuf = pTsBuf;
  pRuntimeEnv->cur.vgroupIndex = -1;

  if (onlyQueryTags(pQuery)) {
    // TODO refactor.
    pRuntimeEnv->resultInfo.capacity = 4096;
    pRuntimeEnv->proot = createTagScanOperatorInfo(pRuntimeEnv, pQuery->pExpr1, pQuery->numOfOutput);
  } else if (pQuery->queryBlockDist) {
    pRuntimeEnv->pTableScanner = createTableBlockInfoScanOperator(pRuntimeEnv->pQueryHandle, pRuntimeEnv);
  } else if (isTsCompQuery(pQuery) || isPointInterpoQuery(pQuery)) {
    pRuntimeEnv->pTableScanner = createTableSeqScanOperator(pRuntimeEnv->pQueryHandle, pRuntimeEnv, isPointInterpoQuery(pQuery));
  } else if (needReverseScan(pQuery)) {
    pRuntimeEnv->pTableScanner = createDataBlocksOptScanInfo(pRuntimeEnv->pQueryHandle, pRuntimeEnv, getNumOfScanTimes(pQuery), 1);
  } else {
    pRuntimeEnv->pTableScanner = createTableScanOperator(pRuntimeEnv->pQueryHandle, pRuntimeEnv, getNumOfScanTimes(pQuery));
  }

  if (pTsBuf != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTsBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTsBuf, order);
  }

  int32_t ps = DEFAULT_PAGE_SIZE;
  int32_t rowsize = 0;
  getIntermediateBufInfo(pRuntimeEnv, &ps, &pQuery->intermediateResultRowSize);
  int32_t TENMB = 1024*1024*10;

  if (isSTableQuery && !onlyQueryTags(pQuery)) {
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, ps, TENMB, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else if (pQuery->groupbyColumn || QUERY_IS_INTERVAL_QUERY(pQuery) || (!isSTableQuery)) {
    getIntermediateBufInfo(pRuntimeEnv, &ps, &rowsize);
    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, ps, TENMB, pQInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // create runtime environment
  int32_t numOfTables = pQuery->tableGroupInfo.numOfTables;
  pQInfo->summary.tableInfoSize += (numOfTables * sizeof(STableQueryInfo));
  code = setupQueryRuntimeEnv(pRuntimeEnv, (int32_t) pQuery->tableGroupInfo.numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  setQueryStatus(pRuntimeEnv, QUERY_NOT_COMPLETED);
  return TSDB_CODE_SUCCESS;
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

STsdbQueryCond createTsdbQueryCond(SQuery* pQuery, STimeWindow* win) {
  STsdbQueryCond cond = {
      .colList   = pQuery->colList,
      .order     = pQuery->order.order,
      .numOfCols = pQuery->numOfCols,
      .loadExternalRows = false,
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

static UNUSED_FUNC void updateTableIdInfo(SQuery* pQuery, SHashObj* pTableIdInfo) {
  STableIdInfo tidInfo = createTableIdInfo(pQuery);
  STableIdInfo* idinfo = taosHashGet(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid));
  if (idinfo != NULL) {
    assert(idinfo->tid == tidInfo.tid && idinfo->uid == tidInfo.uid);
    idinfo->key = tidInfo.key;
  } else {
    taosHashPut(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid), &tidInfo, sizeof(STableIdInfo));
  }
}

static void doCloseAllTimeWindow(SQueryRuntimeEnv* pRuntimeEnv) {
  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  for (int32_t i = 0; i < numOfGroup; ++i) {
    SArray* group = GET_TABLEGROUP(pRuntimeEnv, i);

    size_t num = taosArrayGetSize(group);
    for (int32_t j = 0; j < num; ++j) {
      STableQueryInfo* item = taosArrayGetP(group, j);
      closeAllResultRows(&item->resInfo);
    }
  }
}

static SSDataBlock* doTableScanImpl(STableScanInfo *pTableScanInfo) {
  SSDataBlock*     pBlock = &pTableScanInfo->block;
  SQuery*          pQuery = pTableScanInfo->pRuntimeEnv->pQuery;
  STableGroupInfo* pTableGroupInfo = &pTableScanInfo->pRuntimeEnv->tableqinfoGroupInfo;

  while (tsdbNextDataBlock(pTableScanInfo->pQueryHandle)) {
    pTableScanInfo->numOfBlocks += 1;

    // todo check for query cancel
    tsdbRetrieveDataBlockInfo(pTableScanInfo->pQueryHandle, &pBlock->info);

    if (pTableGroupInfo->numOfTables > 1 || (pQuery->current == NULL && pTableGroupInfo->numOfTables == 1)) {
      STableQueryInfo** pTableQueryInfo =
          (STableQueryInfo**)taosHashGet(pTableGroupInfo->map, &pBlock->info.tid, sizeof(pBlock->info.tid));
      if (pTableQueryInfo == NULL) {
        break;
      }

      pQuery->current = *pTableQueryInfo;
      doTableQueryInfoTimeWindowCheck(pQuery, *pTableQueryInfo);
    }

    // this function never returns error?
    uint32_t status;
    int32_t  code = loadDataBlockOnDemand(pTableScanInfo->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTableScanInfo->pRuntimeEnv->env, code);
    }

    // current block is ignored according to filter result by block statistics data, continue load the next block
    if (status == BLK_DATA_DISCARD || pBlock->info.rows == 0) {
      continue;
    }

    if (pTableScanInfo->loadExternalRows) {
       pTableScanInfo->externalLoaded = true;
    }

    return pBlock;
  }

  if (pTableScanInfo->loadExternalRows && (!pTableScanInfo->externalLoaded)) {
    pBlock->pDataBlock = tsdbGetExternalRow(pTableScanInfo->pQueryHandle, &pBlock->info);
    pTableScanInfo->externalLoaded = true;

    if (pBlock->pDataBlock != NULL) {
      STableQueryInfo** pTableQueryInfo =
          (STableQueryInfo**)taosHashGet(pTableGroupInfo->map, &pBlock->info.tid, sizeof(pBlock->info.tid));
      assert(*pTableQueryInfo != NULL);
      pQuery->current = *pTableQueryInfo;
    }

    return (pBlock->pDataBlock != NULL)? pBlock:NULL;
  }

  return NULL;
}

static SSDataBlock* doTableScan(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;

  STableScanInfo   *pTableScanInfo = pOperator->info;
  SQueryRuntimeEnv *pRuntimeEnv = pTableScanInfo->pRuntimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;

  SResultRowInfo* pResultRowInfo = pTableScanInfo->pResultRowInfo;

  while (pTableScanInfo->current < pTableScanInfo->times) {
    SSDataBlock* p = doTableScanImpl(pTableScanInfo);
    if (p != NULL) {
      return p;
    }

    if (++pTableScanInfo->current >= pTableScanInfo->times) {
      if (pTableScanInfo->reverseTimes <= 0) {
        return NULL;
      } else {
        break;
      }
    }

    // do prepare for the next round table scan operation
    STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);
    tsdbResetQueryHandle(pTableScanInfo->pQueryHandle, &cond);

    setQueryStatus(pRuntimeEnv, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    if (pRuntimeEnv->pTsBuf) {
      bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
      assert(ret);
    }

    pResultRowInfo->curIndex = 0;
    pResultRowInfo->prevSKey = pResultRowInfo->pResult[0]->win.skey;

    qDebug("QInfo:%p start to repeat scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           pRuntimeEnv->qinfo, cond.twindow.skey, cond.twindow.ekey);
  }

  if (pTableScanInfo->reverseTimes > 0) {
    setupEnvForReverseScan(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pTableScanInfo->pCtx, pTableScanInfo->numOfOutput);

    STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);
    tsdbResetQueryHandle(pTableScanInfo->pQueryHandle, &cond);

    qDebug("QInfo:%p start to reverse scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           pRuntimeEnv->qinfo, cond.twindow.skey, cond.twindow.ekey);

    pRuntimeEnv->scanFlag = REVERSE_SCAN;

    pTableScanInfo->times = 1;
    pTableScanInfo->current = 0;
    pTableScanInfo->reverseTimes = 0;
    pTableScanInfo->order = cond.order;

    if (pResultRowInfo->size > 0) {
      pResultRowInfo->curIndex = pResultRowInfo->size-1;
      pResultRowInfo->prevSKey = pResultRowInfo->pResult[pResultRowInfo->size-1]->win.skey;
    }

    SSDataBlock* p = doTableScanImpl(pTableScanInfo);
    if (p != NULL) {
      return p;
    }
  }

  return NULL;
}

static SSDataBlock* doSeqTableBlocksScan(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;

  STableScanInfo   *pTableScanInfo = pOperator->info;
  SQueryRuntimeEnv *pRuntimeEnv = pTableScanInfo->pRuntimeEnv;

  int32_t totalTables = pRuntimeEnv->tableqinfoGroupInfo.numOfTables;

  while (1) {
    SSDataBlock* p = doTableScanImpl(pTableScanInfo);
    if (p != NULL) {
      return p;
    }

    // try the next table
    if (++pTableScanInfo->tableIndex >= totalTables) {
      return NULL;
    }

    setTableQueryHandle(pRuntimeEnv, pTableScanInfo->tableIndex);
    pTableScanInfo->pQueryHandle = pRuntimeEnv->pQueryHandle;
    pTableScanInfo->externalLoaded = false;
  }
}

static SSDataBlock* doBlockInfoScan(void* param) {
  SOperatorInfo *pOperator = (SOperatorInfo*)param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableScanInfo *pTableScanInfo = pOperator->info;

  STableBlockDist tableBlockDist = {0};
  tableBlockDist.numOfTables     = pTableScanInfo->pRuntimeEnv->tableqinfoGroupInfo.numOfTables;
  tableBlockDist.dataBlockInfos  = taosArrayInit(512, sizeof(SFileBlockInfo));

  tsdbGetFileBlocksDistInfo(pTableScanInfo->pQueryHandle, &tableBlockDist);
  tableBlockDist.numOfRowsInMemTable = tsdbGetNumOfRowsInMemTable(pTableScanInfo->pQueryHandle);

  SSDataBlock* pBlock = &pTableScanInfo->block;
  pBlock->info.rows   = 1;
  pBlock->info.numOfCols = 1;

  SBufferWriter bw = tbufInitWriter(NULL, false);
  blockDistInfoToBinary(&tableBlockDist, &bw);
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 0);

  int32_t len = (int32_t) tbufTell(&bw);
  pColInfo->pData = malloc(len + sizeof(int32_t));

  *(int32_t*) pColInfo->pData = len;
  memcpy(pColInfo->pData + sizeof(int32_t), tbufGetData(&bw, false), len);

  tbufCloseWriter(&bw);

  SArray* g = GET_TABLEGROUP(pOperator->pRuntimeEnv, 0);
  pOperator->pRuntimeEnv->pQuery->current = taosArrayGetP(g, 0);

  pOperator->status = OP_EXEC_DONE;
  return pBlock;
}

SOperatorInfo* createTableScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime) {
  assert(repeatTime > 0);

  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));
  pInfo->pQueryHandle = pTsdbQueryHandle;
  pInfo->times        = repeatTime;
  pInfo->reverseTimes = 0;
  pInfo->order        = pRuntimeEnv->pQuery->order.order;
  pInfo->current      = 0;
  pInfo->pRuntimeEnv  = pRuntimeEnv;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableScanOperator";
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = pRuntimeEnv->pQuery->numOfCols;
  pOperator->exec         = doTableScan;

  return pOperator;
}

SOperatorInfo* createTableSeqScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, bool loadExternalRows) {
  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));

  pInfo->pQueryHandle     = pTsdbQueryHandle;
  pInfo->times            = 1;
  pInfo->reverseTimes     = 0;
  pInfo->order            = pRuntimeEnv->pQuery->order.order;
  pInfo->current          = 0;
  pInfo->pRuntimeEnv      = pRuntimeEnv;
  pInfo->loadExternalRows = loadExternalRows;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableSeqScanOperator";
  pOperator->operatorType = OP_TableSeqScan;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = pRuntimeEnv->pQuery->numOfCols;
  pOperator->exec         = doSeqTableBlocksScan;

  return pOperator;
}

SOperatorInfo* createTableBlockInfoScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv) {
  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));

  pInfo->pQueryHandle     = pTsdbQueryHandle;
  pInfo->pRuntimeEnv      = pRuntimeEnv;
  pInfo->block.pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {{0}};
  infoData.info.type = TSDB_DATA_TYPE_BINARY;
  infoData.info.bytes = 1024;
  infoData.info.colId = TSDB_BLOCK_DIST_COLUMN_INDEX;
  taosArrayPush(pInfo->block.pDataBlock, &infoData);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableBlockInfoScanOperator";
  pOperator->operatorType = OP_TableBlockInfoScan;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->numOfOutput  = pRuntimeEnv->pQuery->numOfCols;
  pOperator->exec         = doBlockInfoScan;

  return pOperator;
}

void setTableScanFilterOperatorInfo(STableScanInfo* pTableScanInfo, SOperatorInfo* pDownstream) {
  assert(pTableScanInfo != NULL && pDownstream != NULL);

  pTableScanInfo->pExpr = pDownstream->pExpr;   // TODO refactor to use colId instead of pExpr
  pTableScanInfo->numOfOutput = pDownstream->numOfOutput;

  if (pDownstream->operatorType == OP_Aggregate || pDownstream->operatorType == OP_MultiTableAggregate) {
    SAggOperatorInfo* pAggInfo = pDownstream->info;

    pTableScanInfo->pCtx = pAggInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pAggInfo->binfo.rowCellInfoOffset;
  } else if (pDownstream->operatorType == OP_TimeInterval) {
    STableIntervalOperatorInfo *pIntervalInfo = pDownstream->info;

    pTableScanInfo->pCtx = pIntervalInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pIntervalInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pIntervalInfo->rowCellInfoOffset;

  } else if (pDownstream->operatorType == OP_Groupby) {
    SGroupbyOperatorInfo *pGroupbyInfo = pDownstream->info;

    pTableScanInfo->pCtx = pGroupbyInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pGroupbyInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pGroupbyInfo->binfo.rowCellInfoOffset;

  } else if (pDownstream->operatorType == OP_MultiTableTimeInterval) {
    STableIntervalOperatorInfo *pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->rowCellInfoOffset;

  } else if (pDownstream->operatorType == OP_Arithmetic) {
    SArithOperatorInfo *pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->binfo.rowCellInfoOffset;
  } else {
    assert(0);
  }
}

static SOperatorInfo* createDataBlocksOptScanInfo(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime, int32_t reverseTime) {
  assert(repeatTime > 0);

  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));
  pInfo->pQueryHandle = pTsdbQueryHandle;
  pInfo->times        = repeatTime;
  pInfo->reverseTimes = reverseTime;
  pInfo->current      = 0;
  pInfo->order        = pRuntimeEnv->pQuery->order.order;
  pInfo->pRuntimeEnv  = pRuntimeEnv;

  SOperatorInfo* pOptr = calloc(1, sizeof(SOperatorInfo));
  pOptr->name          = "DataBlocksOptimizedScanOperator";
  pOptr->operatorType  = OP_DataBlocksOptScan;
  pOptr->blockingOptr  = false;
  pOptr->info          = pInfo;
  pOptr->exec          = doTableScan;

  return pOptr;
}

static int32_t getTableScanOrder(STableScanInfo* pTableScanInfo) {
  return pTableScanInfo->order;
}

// this is a blocking operator
static SSDataBlock* doAggregate(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo* pInfo = &pAggInfo->binfo;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  SOperatorInfo* upstream = pOperator->upstream;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    setTagValue(pOperator, pQuery->current->pTable, pInfo->pCtx, pOperator->numOfOutput);

    if (upstream->operatorType == OP_DataBlocksOptScan) {
      STableScanInfo* pScanInfo = upstream->info;
      order = getTableScanOrder(pScanInfo);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, order);
    doAggregateImpl(pOperator, pQuery->window.skey, pInfo->pCtx, pBlock);
  }

  pOperator->status = OP_EXEC_DONE;
  setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);

  finalizeQueryResult(pOperator, pInfo->pCtx, &pInfo->resultRowInfo, pInfo->rowCellInfoOffset);
  pInfo->pRes->info.rows = getNumOfResult(pRuntimeEnv, pInfo->pCtx, pOperator->numOfOutput);

  return pInfo->pRes;
}

static SSDataBlock* doSTableAggregate(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo* pInfo = &pAggInfo->binfo;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->pRes);

    if (pInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pInfo->pRes;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  SOperatorInfo* upstream = pOperator->upstream;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    setTagValue(pOperator, pRuntimeEnv->pQuery->current->pTable, pInfo->pCtx, pOperator->numOfOutput);

    if (upstream->operatorType == OP_DataBlocksOptScan) {
      STableScanInfo* pScanInfo = upstream->info;
      order = getTableScanOrder(pScanInfo);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, order);

    TSKEY k = (pQuery->order.order == TSDB_ORDER_ASC)? pBlock->info.window.ekey + 1:pBlock->info.window.skey-1;
    setExecutionContext(pRuntimeEnv, pInfo, pOperator->numOfOutput, pQuery->current->groupIndex, k);
    doAggregateImpl(pOperator, pQuery->window.skey, pInfo->pCtx, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->resultRowInfo);

  updateNumOfRowsInResultRows(pRuntimeEnv, pInfo->pCtx, pOperator->numOfOutput, &pInfo->resultRowInfo,
                             pInfo->rowCellInfoOffset);

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pInfo->resultRowInfo);

  toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->pRes);
  if (pInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pInfo->pRes;
}

static SSDataBlock* doArithmeticOperation(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;

  SArithOperatorInfo* pArithInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  SOptrBasicInfo *pInfo = &pArithInfo->binfo;
  
  pInfo->pRes->info.rows = 0;

  while(1) {
    SSDataBlock* pBlock = pOperator->upstream->exec(pOperator->upstream);
    if (pBlock == NULL) {
      setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
      break;
    }

    setTagValue(pOperator, pRuntimeEnv->pQuery->current->pTable, pInfo->pCtx, pOperator->numOfOutput);

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, pRuntimeEnv->pQuery->order.order);
    updateOutputBuf(pArithInfo, pBlock->info.rows);
    arithmeticApplyFunctions(pRuntimeEnv, pInfo->pCtx, pOperator->numOfOutput);

    pInfo->pRes->info.rows = getNumOfResult(pRuntimeEnv, pInfo->pCtx, pOperator->numOfOutput);
    if (pInfo->pRes->info.rows >= 4096) {
      break;
    }
  }

  setNumOfRes(pInfo->pCtx, pOperator->numOfOutput);
  return (pInfo->pRes->info.rows > 0)? pInfo->pRes:NULL;
}

static SSDataBlock* doLimit(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SLimitOperatorInfo* pInfo = pOperator->info;

  SSDataBlock* pBlock = pOperator->upstream->exec(pOperator->upstream);
  if (pBlock == NULL) {
    setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
    pOperator->status = OP_EXEC_DONE;
    return NULL;
  }

  if (pInfo->total + pBlock->info.rows >= pInfo->limit) {
    pBlock->info.rows = (pInfo->limit - pInfo->total);

    pInfo->total = pInfo->limit;

    setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
    pOperator->status = OP_EXEC_DONE;
  } else {
    pInfo->total += pBlock->info.rows;
  }

  return pBlock;
}

// TODO add log
static SSDataBlock* doOffset(void* param) {
  SOperatorInfo *pOperator = (SOperatorInfo *)param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  while (1) {
    SSDataBlock *pBlock = pOperator->upstream->exec(pOperator->upstream);
    if (pBlock == NULL) {
      setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }

    if (pRuntimeEnv->currentOffset == 0) {
      return pBlock;
    } else if (pRuntimeEnv->currentOffset > pBlock->info.rows) {
      pRuntimeEnv->currentOffset -= pBlock->info.rows;
    } else {
      int32_t remain = pBlock->info.rows - pRuntimeEnv->currentOffset;
      pBlock->info.rows = remain;

      for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
        SColumnInfoData *pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

        int16_t bytes = pColInfoData->info.bytes;
        memmove(pColInfoData->pData, pColInfoData->pData + bytes * pRuntimeEnv->currentOffset, remain * bytes);
      }

      pRuntimeEnv->currentOffset = 0;
      return pBlock;
    }
  }
}

static SSDataBlock* doIntervalAgg(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (pOperator->status == OP_RES_TO_RETURN) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

    if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pIntervalInfo->pRes;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;
  STimeWindow win = pQuery->window;

  SOperatorInfo* upstream = pOperator->upstream;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pIntervalInfo->pCtx, pBlock, pQuery->order.order);
    hashIntervalAgg(pOperator, &pIntervalInfo->resultRowInfo, pIntervalInfo, pBlock, 0);
  }

  // restore the value
  pQuery->order.order = order;
  pQuery->window = win;

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pIntervalInfo->resultRowInfo);
  setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
  finalizeQueryResult(pOperator, pIntervalInfo->pCtx, &pIntervalInfo->resultRowInfo, pIntervalInfo->rowCellInfoOffset);

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pIntervalInfo->resultRowInfo);
  toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

  if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pIntervalInfo->pRes->info.rows == 0? NULL:pIntervalInfo->pRes;
}

static SSDataBlock* doSTableIntervalAgg(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (pOperator->status == OP_RES_TO_RETURN) {
    copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
    if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pIntervalInfo->pRes;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  SOperatorInfo* upstream = pOperator->upstream;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    STableQueryInfo* pTableQueryInfo = pRuntimeEnv->pQuery->current;

    setTagValue(pOperator, pTableQueryInfo->pTable, pIntervalInfo->pCtx, pOperator->numOfOutput);
    setInputDataBlock(pOperator, pIntervalInfo->pCtx, pBlock, pQuery->order.order);
    setIntervalQueryRange(pRuntimeEnv, pBlock->info.window.skey);

    hashIntervalAgg(pOperator, &pTableQueryInfo->resInfo, pIntervalInfo, pBlock, pTableQueryInfo->groupIndex);
  }

  pOperator->status = OP_RES_TO_RETURN;
  pQuery->order.order = order;   // TODO : restore the order
  doCloseAllTimeWindow(pRuntimeEnv);
  setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);

  copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
  if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pIntervalInfo->pRes;
}

static SSDataBlock* doHashGroupbyAgg(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SGroupbyOperatorInfo *pInfo = pOperator->info;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (pOperator->status == OP_RES_TO_RETURN) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);

    if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pInfo->binfo.pRes;
  }

  SOperatorInfo* upstream = pOperator->upstream;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, pRuntimeEnv->pQuery->order.order);
    setTagValue(pOperator, pRuntimeEnv->pQuery->current->pTable, pInfo->binfo.pCtx, pOperator->numOfOutput);
    if (pInfo->colIndex == -1) {
      pInfo->colIndex = getGroupbyColumnIndex(pRuntimeEnv->pQuery->pGroupbyExpr, pBlock);
    }

    hashGroupbyAgg(pRuntimeEnv, pOperator, pInfo, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->binfo.resultRowInfo);
  setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);

  // todo need finalize the result
  if (!pRuntimeEnv->pQuery->stableQuery) {
    // finalize include the update of result rows
    finalizeQueryResult(pOperator, pInfo->binfo.pCtx, &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);
  } else {
    updateNumOfRowsInResultRows(pRuntimeEnv, pInfo->binfo.pCtx, pOperator->numOfOutput, &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);
  }

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pInfo->binfo.resultRowInfo);
  toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);

  if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pInfo->binfo.pRes;
}

static SSDataBlock* doFill(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SFillOperatorInfo *pInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pRuntimeEnv->resultInfo.capacity);
    return pInfo->pRes;
  }

  while(1) {
    SSDataBlock *pBlock = pOperator->upstream->exec(pOperator->upstream);
    if (pBlock == NULL) {
      if (pInfo->totalInputRows == 0) {
        pOperator->status = OP_EXEC_DONE;
        return NULL;
      }

      taosFillSetStartInfo(pInfo->pFillInfo, 0, pRuntimeEnv->pQuery->window.ekey);
    } else {
      pInfo->totalInputRows += pBlock->info.rows;

      int64_t ekey = Q_STATUS_EQUAL(pRuntimeEnv->status, QUERY_COMPLETED)?pRuntimeEnv->pQuery->window.ekey:pBlock->info.window.ekey;

      taosFillSetStartInfo(pInfo->pFillInfo, pBlock->info.rows, ekey);
      taosFillSetInputDataBlock(pInfo->pFillInfo, pBlock);
    }

    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pRuntimeEnv->resultInfo.capacity);
    return (pInfo->pRes->info.rows > 0)? pInfo->pRes:NULL;
  }
}

// todo set the attribute of query scan count
static int32_t getNumOfScanTimes(SQuery* pQuery) {
  for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_STDDEV || functionId == TSDB_FUNC_PERCT) {
      return 2;
    }
  }

  return 1;
}

static void destroyOperatorInfo(SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return;
  }

  if (pOperator->cleanup != NULL) {
    pOperator->cleanup(pOperator->info, pOperator->numOfOutput);
  }

  destroyOperatorInfo(pOperator->upstream);
  tfree(pOperator->info);
  tfree(pOperator);
}

static SOperatorInfo* createAggregateOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SAggOperatorInfo* pInfo = calloc(1, sizeof(SAggOperatorInfo));

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t numOfRows = (int32_t)(GET_ROW_PARAM_FOR_MULTIOUTPUT(pQuery, pQuery->topBotQuery, pQuery->stableQuery));

  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, numOfRows);
  pInfo->binfo.pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  pInfo->seed = rand();
  setDefaultOutputBuf(pRuntimeEnv, &pInfo->binfo, pInfo->seed);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableAggregate";
  pOperator->operatorType = OP_Aggregate;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doAggregate;
  pOperator->cleanup      = destroyBasicOperatorInfo;
  return pOperator;
}

static void doDestroyBasicInfo(SOptrBasicInfo* pInfo, int32_t numOfOutput) {
  assert(pInfo != NULL);

  destroySQLFunctionCtx(pInfo->pCtx, numOfOutput);
  tfree(pInfo->rowCellInfoOffset);

  cleanupResultRowInfo(&pInfo->resultRowInfo);
  pInfo->pRes = destroyOutputBuf(pInfo->pRes);
}

static void destroyBasicOperatorInfo(void* param, int32_t numOfOutput) {
  SOptrBasicInfo* pInfo = (SOptrBasicInfo*) param;
  doDestroyBasicInfo(pInfo, numOfOutput);
}

static void destroySFillOperatorInfo(void* param, int32_t numOfOutput) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*) param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  pInfo->pRes = destroyOutputBuf(pInfo->pRes);
}

static void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

static void destroyArithOperatorInfo(void* param, int32_t numOfOutput) {
  SArithOperatorInfo* pInfo = (SArithOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

SOperatorInfo* createMultiTableAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SAggOperatorInfo* pInfo = calloc(1, sizeof(SAggOperatorInfo));

  size_t tableGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, (int32_t) tableGroup);
  pInfo->binfo.pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, tableGroup, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "MultiTableAggregate";
  pOperator->operatorType = OP_MultiTableAggregate;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doSTableAggregate;
  pOperator->cleanup      = destroyBasicOperatorInfo;

  return pOperator;
}

SOperatorInfo* createArithOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SArithOperatorInfo* pInfo = calloc(1, sizeof(SArithOperatorInfo));

  pInfo->seed = rand();
  pInfo->bufCapacity = pRuntimeEnv->resultInfo.capacity;

  SOptrBasicInfo* pBInfo = &pInfo->binfo;
  pBInfo->pRes  = createOutputBuf(pExpr, numOfOutput, pInfo->bufCapacity);
  pBInfo->pCtx  = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pBInfo->rowCellInfoOffset);

  initResultRowInfo(&pBInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);
  setDefaultOutputBuf(pRuntimeEnv, pBInfo, pInfo->seed);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "ArithmeticOperator";
  pOperator->operatorType = OP_Arithmetic;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doArithmeticOperation;
  pOperator->cleanup      = destroyArithOperatorInfo;

  return pOperator;
}

SOperatorInfo* createLimitOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream) {
  SLimitOperatorInfo* pInfo = calloc(1, sizeof(SLimitOperatorInfo));
  pInfo->limit = pRuntimeEnv->pQuery->limit.limit;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "LimitOperator";
  pOperator->operatorType = OP_Limit;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->upstream     = upstream;
  pOperator->exec         = doLimit;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  return pOperator;
}

SOperatorInfo* createOffsetOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream) {
  SOffsetOperatorInfo* pInfo = calloc(1, sizeof(SOffsetOperatorInfo));

  pInfo->offset = pRuntimeEnv->pQuery->limit.offset;
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "OffsetOperator";
  pOperator->operatorType = OP_Offset;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->upstream     = upstream;
  pOperator->exec         = doOffset;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  return pOperator;
}

SOperatorInfo* createTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = calloc(1, sizeof(STableIntervalOperatorInfo));

  pInfo->pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->rowCellInfoOffset);
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "TimeIntervalAggOperator";
  pOperator->operatorType = OP_TimeInterval;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->exec         = doIntervalAgg;
  pOperator->cleanup      = destroyBasicOperatorInfo;

  return pOperator;
}

SOperatorInfo* createMultiTableTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = calloc(1, sizeof(STableIntervalOperatorInfo));

  pInfo->pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->rowCellInfoOffset);
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "MultiTableTimeIntervalOperator";
  pOperator->operatorType = OP_MultiTableTimeInterval;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doSTableIntervalAgg;
  pOperator->cleanup      = destroyBasicOperatorInfo;

  return pOperator;
}

SOperatorInfo* createGroupbyOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = calloc(1, sizeof(SGroupbyOperatorInfo));
  pInfo->colIndex = -1;  // group by column index

  pInfo->binfo.pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "GroupbyAggOperator";
  pOperator->blockingOptr = true;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->operatorType = OP_Groupby;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->exec         = doHashGroupbyAgg;
  pOperator->cleanup      = destroyGroupbyOperatorInfo;

  return pOperator;
}

SOperatorInfo* createFillOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr,
    int32_t numOfOutput) {
  SFillOperatorInfo* pInfo = calloc(1, sizeof(SFillOperatorInfo));
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);

  {
    SQuery* pQuery = pRuntimeEnv->pQuery;
    SFillColInfo* pColInfo = createFillColInfo(pExpr, numOfOutput, pQuery->fillVal);
    STimeWindow w = TSWINDOW_INITIALIZER;

    TSKEY sk = MIN(pQuery->window.skey, pQuery->window.ekey);
    TSKEY ek = MAX(pQuery->window.skey, pQuery->window.ekey);
    getAlignQueryTimeWindow(pQuery, pQuery->window.skey, sk, ek, &w);

    pInfo->pFillInfo = taosCreateFillInfo(pQuery->order.order, w.skey, 0, (int32_t)pRuntimeEnv->resultInfo.capacity, numOfOutput,
                                                pQuery->interval.sliding, pQuery->interval.slidingUnit, (int8_t)pQuery->precision,
                                                pQuery->fillType, pColInfo, pRuntimeEnv->qinfo);
  }

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "FillOperator";
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->operatorType = OP_Fill;

  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doFill;
  pOperator->cleanup      = destroySFillOperatorInfo;

  return pOperator;
}

static SSDataBlock* doTagScan(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  int32_t maxNumOfTables = (int32_t)pRuntimeEnv->resultInfo.capacity;

  STagScanInfo *pInfo = pOperator->info;
  SSDataBlock  *pRes = pInfo->pRes;

  int32_t count = 0;
  SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);

  int32_t functionId = pOperator->pExpr[0].base.functionId;
  if (functionId == TSDB_FUNC_TID_TAG) { // return the tags & table Id
    SQuery* pQuery = pRuntimeEnv->pQuery;
    assert(pQuery->numOfOutput == 1);

    SExprInfo* pExprInfo = &pOperator->pExpr[0];
    int32_t rsize = pExprInfo->bytes;

    count = 0;

    int16_t bytes = pExprInfo->bytes;
    int16_t type  = pExprInfo->type;

    for(int32_t i = 0; i < pQuery->numOfTags; ++i) {
      if (pQuery->tagColList[i].colId == pExprInfo->base.colInfo.colId) {
        bytes = pQuery->tagColList[i].bytes;
        type = pQuery->tagColList[i].type;
        break;
      }
    }

    SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, 0);

    while(pInfo->currentIndex < pInfo->totalTables && count < maxNumOfTables) {
      int32_t i = pInfo->currentIndex++;
      STableQueryInfo *item = taosArrayGetP(pa, i);

      char *output = pColInfo->pData + count * rsize;
      varDataSetLen(output, rsize - VARSTR_HEADER_SIZE);

      output = varDataVal(output);
      STableId* id = TSDB_TABLEID(item->pTable);

      *(int16_t *)output = 0;
      output += sizeof(int16_t);

      *(int64_t *)output = id->uid;  // memory align problem, todo serialize
      output += sizeof(id->uid);

      *(int32_t *)output = id->tid;
      output += sizeof(id->tid);

      *(int32_t *)output = pQuery->vgId;
      output += sizeof(pQuery->vgId);

      char* data = NULL;
      if (pExprInfo->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
        data = tsdbGetTableName(item->pTable);
      } else {
        data = tsdbGetTableTagVal(item->pTable, pExprInfo->base.colInfo.colId, type, bytes);
      }

      doSetTagValueToResultBuf(output, data, type, bytes);
      count += 1;
    }

    qDebug("QInfo:%p create (tableId, tag) info completed, rows:%d", pRuntimeEnv->qinfo, count);
  } /*else if (functionId == TSDB_FUNC_COUNT) {// handle the "count(tbname)" query
    *(int64_t*) pQuery->sdata[0]->data = num;

    count = 1;
    SET_STABLE_QUERY_OVER(pRuntimeEnv);
    qDebug("QInfo:%p create count(tbname) query, res:%d rows:1", pRuntimeEnv->qinfo, count);
  }*/ else {  // return only the tags|table name etc.
    SExprInfo* pExprInfo = pOperator->pExpr;  // todo use the column list instead of exprinfo

    count = 0;
    while(pInfo->currentIndex < pInfo->totalTables && count < maxNumOfTables) {
      int32_t i = pInfo->currentIndex++;

      STableQueryInfo* item = taosArrayGetP(pa, i);

      char *data = NULL, *dst = NULL;
      int16_t type = 0, bytes = 0;
      for(int32_t j = 0; j < pOperator->numOfOutput; ++j) {
        // not assign value in case of user defined constant output column
        if (TSDB_COL_IS_UD_COL(pExprInfo[j].base.colInfo.flag)) {
          continue;
        }

        SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, j);
        type  = pExprInfo[j].type;
        bytes = pExprInfo[j].bytes;

        if (pExprInfo[j].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
          data = tsdbGetTableName(item->pTable);
        } else {
          data = tsdbGetTableTagVal(item->pTable, pExprInfo[j].base.colInfo.colId, type, bytes);
        }

        dst  = pColInfo->pData + count * pExprInfo[j].bytes;
        doSetTagValueToResultBuf(dst, data, type, bytes);
      }

      count += 1;
    }

    qDebug("QInfo:%p create tag values results completed, rows:%d", pRuntimeEnv->qinfo, count);
  }

  pRes->info.rows = count;
  return (pRes->info.rows == 0)? NULL:pInfo->pRes;
}

SOperatorInfo* createTagScanOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput) {
  STagScanInfo* pInfo = calloc(1, sizeof(STagScanInfo));
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  assert(numOfGroup == 0 || numOfGroup == 1);

  pInfo->totalTables = pRuntimeEnv->tableqinfoGroupInfo.numOfTables;
  pInfo->currentIndex = 0;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "SeqTableTagScan";
  pOperator->operatorType = OP_TagScan;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_IN_EXECUTING;
  pOperator->info         = pInfo;
  pOperator->exec         = doTagScan;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  return pOperator;
}

void tableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  // number of points returned during this query
  int64_t st = taosGetTimestampUs();

  assert(pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 1);

  SArray* g = GET_TABLEGROUP(pRuntimeEnv, 0);
  pQuery->current = taosArrayGetP(g, 0);

  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);

  // record the total elapsed time
  pQInfo->summary.elapsedTime += (taosGetTimestampUs() - st);
}



void buildTableBlockDistResult(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STableBlockDist *pTableBlockDist = calloc(1, sizeof(STableBlockDist));
  pTableBlockDist->dataBlockInfos  = taosArrayInit(512, sizeof(SFileBlockInfo));

  TsdbQueryHandleT pQueryHandle = pRuntimeEnv->pQueryHandle;
  SSchema blockDistSchema = tGetBlockDistColumnSchema();

//  int64_t startTime = taosGetTimestampUs();
//  tsdbGetFileBlocksDistInfo(pQueryHandle, pTableBlockDist->dataBlockInfos, );
  pTableBlockDist->numOfRowsInMemTable = tsdbGetNumOfRowsInMemTable(pQueryHandle);
  
//  generateBlockDistResult(pTableBlockDist);

  int type = -1;
  assert(pQuery->numOfOutput == 1);
  SExprInfo* pExprInfo = pQuery->pExpr1;
  for (int32_t j = 0; j < pQuery->numOfOutput; j++) {
    if (pExprInfo[j].base.colInfo.colId == TSDB_BLOCK_DIST_COLUMN_INDEX) {
      type = blockDistSchema.type;
    }

    assert(type == TSDB_DATA_TYPE_BINARY);
//    STR_WITH_SIZE_TO_VARSTR(pQuery->sdata[j]->data, pTableBlockDist->result, (VarDataLenT)strlen(pTableBlockDist->result));
  }

  freeTableBlockDist(pTableBlockDist);

//  pRuntimeEnv->resultInfo.rows = 1;
  setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
}

void stableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;

  int64_t st = taosGetTimestampUs();
  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);

  // record the total elapsed time
  pQInfo->summary.elapsedTime += (taosGetTimestampUs() - st);
}

static int32_t getColumnIndexInSource(SQueryTableMsg *pQueryMsg, SSqlFuncMsg *pExprMsg, SColumnInfo* pTagCols) {
  int32_t j = 0;

  if (TSDB_COL_IS_TAG(pExprMsg->colInfo.flag)) {
    if (pExprMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
      return TSDB_TBNAME_COLUMN_INDEX;
    } else if (pExprMsg->colInfo.colId == TSDB_BLOCK_DIST_COLUMN_INDEX) {
      return TSDB_BLOCK_DIST_COLUMN_INDEX;     
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

  return INT32_MIN;  // return a less than TSDB_TBNAME_COLUMN_INDEX value
}

bool validateExprColumnInfo(SQueryTableMsg *pQueryMsg, SSqlFuncMsg *pExprMsg, SColumnInfo* pTagCols) {
  int32_t j = getColumnIndexInSource(pQueryMsg, pExprMsg, pTagCols);
  return j != INT32_MIN;
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

static bool validateQuerySourceCols(SQueryTableMsg *pQueryMsg, SSqlFuncMsg** pExprMsg, SColumnInfo* pTagCols) {
  int32_t numOfTotal = pQueryMsg->numOfCols + pQueryMsg->numOfTags;
  if (pQueryMsg->numOfCols < 0 || pQueryMsg->numOfTags < 0 || numOfTotal > TSDB_MAX_COLUMNS) {
    qError("qmsg:%p illegal value of numOfCols %d numOfTags:%d", pQueryMsg, pQueryMsg->numOfCols, pQueryMsg->numOfTags);
    return false;
  }

  if (numOfTotal == 0) {
    for(int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
      SSqlFuncMsg* pFuncMsg = pExprMsg[i];

      if ((pFuncMsg->functionId == TSDB_FUNC_TAGPRJ) ||
          (pFuncMsg->functionId == TSDB_FUNC_TID_TAG && pFuncMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) ||
          (pFuncMsg->functionId == TSDB_FUNC_COUNT && pFuncMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) ||
          (pFuncMsg->functionId == TSDB_FUNC_BLKINFO)) {
        continue;
      }

      return false;
    }
  }

  for(int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
    if (!validateExprColumnInfo(pQueryMsg, pExprMsg[i], pTagCols)) {
      return TSDB_CODE_QRY_INVALID_MSG;
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
int32_t convertQueryMsg(SQueryTableMsg *pQueryMsg, SQueryParam* param) {
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
  pQueryMsg->tbnameCondLen = htonl(pQueryMsg->tbnameCondLen);
  pQueryMsg->secondStageOutput = htonl(pQueryMsg->secondStageOutput);
  pQueryMsg->sqlstrLen = htonl(pQueryMsg->sqlstrLen);
  pQueryMsg->prevResultLen = htonl(pQueryMsg->prevResultLen);

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

    if (!isValidDataType(pColInfo->type)) {
      qDebug("qmsg:%p, invalid data type in source column, index:%d, type:%d", pQueryMsg, col, pColInfo->type);
      code = TSDB_CODE_QRY_INVALID_MSG;
      goto _cleanup;
    }

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

  param->pExprMsg = calloc(pQueryMsg->numOfOutput, POINTER_BYTES);
  if (param->pExprMsg == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _cleanup;
  }

  SSqlFuncMsg *pExprMsg = (SSqlFuncMsg *)pMsg;

  for (int32_t i = 0; i < pQueryMsg->numOfOutput; ++i) {
    param->pExprMsg[i] = pExprMsg;

    pExprMsg->colInfo.colIndex = htons(pExprMsg->colInfo.colIndex);
    pExprMsg->colInfo.colId = htons(pExprMsg->colInfo.colId);
    pExprMsg->colInfo.flag  = htons(pExprMsg->colInfo.flag);
    pExprMsg->colType       = htons(pExprMsg->colType);
    pExprMsg->colBytes      = htons(pExprMsg->colBytes);

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
    }

    pExprMsg = (SSqlFuncMsg *)pMsg;
  }

  if (pQueryMsg->secondStageOutput) {
    pExprMsg = (SSqlFuncMsg *)pMsg;
    param->pSecExprMsg = calloc(pQueryMsg->secondStageOutput, POINTER_BYTES);

    for (int32_t i = 0; i < pQueryMsg->secondStageOutput; ++i) {
      param->pSecExprMsg[i] = pExprMsg;

      pExprMsg->colInfo.colIndex = htons(pExprMsg->colInfo.colIndex);
      pExprMsg->colInfo.colId = htons(pExprMsg->colInfo.colId);
      pExprMsg->colInfo.flag  = htons(pExprMsg->colInfo.flag);
      pExprMsg->colType       = htons(pExprMsg->colType);
      pExprMsg->colBytes      = htons(pExprMsg->colBytes);

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
      }

      pExprMsg = (SSqlFuncMsg *)pMsg;
    }
  }

  pMsg = createTableIdList(pQueryMsg, pMsg, &(param->pTableIdList));

  if (pQueryMsg->numOfGroupCols > 0) {  // group by tag columns
    param->pGroupColIndex = malloc(pQueryMsg->numOfGroupCols * sizeof(SColIndex));
    if (param->pGroupColIndex == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    for (int32_t i = 0; i < pQueryMsg->numOfGroupCols; ++i) {
      param->pGroupColIndex[i].colId = htons(*(int16_t *)pMsg);
      pMsg += sizeof(param->pGroupColIndex[i].colId);

      param->pGroupColIndex[i].colIndex = htons(*(int16_t *)pMsg);
      pMsg += sizeof(param->pGroupColIndex[i].colIndex);

      param->pGroupColIndex[i].flag = htons(*(int16_t *)pMsg);
      pMsg += sizeof(param->pGroupColIndex[i].flag);

      memcpy(param->pGroupColIndex[i].name, pMsg, tListLen(param->pGroupColIndex[i].name));
      pMsg += tListLen(param->pGroupColIndex[i].name);
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
    param->pTagColumnInfo = calloc(1, sizeof(SColumnInfo) * pQueryMsg->numOfTags);
    if (param->pTagColumnInfo == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    for (int32_t i = 0; i < pQueryMsg->numOfTags; ++i) {
      SColumnInfo* pTagCol = (SColumnInfo*) pMsg;

      pTagCol->colId = htons(pTagCol->colId);
      pTagCol->bytes = htons(pTagCol->bytes);
      pTagCol->type  = htons(pTagCol->type);
      pTagCol->numOfFilters = 0;

      param->pTagColumnInfo[i] = *pTagCol;
      pMsg += sizeof(SColumnInfo);
    }
  }

  // the tag query condition expression string is located at the end of query msg
  if (pQueryMsg->tagCondLen > 0) {
    param->tagCond = calloc(1, pQueryMsg->tagCondLen);
    if (param->tagCond == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    memcpy(param->tagCond, pMsg, pQueryMsg->tagCondLen);
    pMsg += pQueryMsg->tagCondLen;
  }

  if (pQueryMsg->prevResultLen > 0) {
    param->prevResult = calloc(1, pQueryMsg->prevResultLen);
    if (param->prevResult == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    memcpy(param->prevResult, pMsg, pQueryMsg->prevResultLen);
    pMsg += pQueryMsg->prevResultLen;
  }

  if (pQueryMsg->tbnameCondLen > 0) {
    param->tbnameCond = calloc(1, pQueryMsg->tbnameCondLen + 1);
    if (param->tbnameCond == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _cleanup;
    }

    strncpy(param->tbnameCond, pMsg, pQueryMsg->tbnameCondLen);
    pMsg += pQueryMsg->tbnameCondLen;
  }

  //skip ts buf
  if ((pQueryMsg->tsOffset + pQueryMsg->tsLen) > 0) {
    pMsg = (char *)pQueryMsg + pQueryMsg->tsOffset + pQueryMsg->tsLen;
  }

  param->sql = strndup(pMsg, pQueryMsg->sqlstrLen);

  if (!validateQuerySourceCols(pQueryMsg, param->pExprMsg, param->pTagColumnInfo)) {
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _cleanup;
  }

  qDebug("qmsg:%p query %d tables, type:%d, qrange:%" PRId64 "-%" PRId64 ", numOfGroupbyTagCols:%d, order:%d, "
         "outputCols:%d, numOfCols:%d, interval:%" PRId64 ", fillType:%d, comptsLen:%d, compNumOfBlocks:%d, limit:%" PRId64 ", offset:%" PRId64,
         pQueryMsg, pQueryMsg->numOfTables, pQueryMsg->queryType, pQueryMsg->window.skey, pQueryMsg->window.ekey, pQueryMsg->numOfGroupCols,
         pQueryMsg->order, pQueryMsg->numOfOutput, pQueryMsg->numOfCols, pQueryMsg->interval.interval,
         pQueryMsg->fillType, pQueryMsg->tsLen, pQueryMsg->tsNumOfBlocks, pQueryMsg->limit, pQueryMsg->offset);

  qDebug("qmsg:%p, sql:%s", pQueryMsg, param->sql);
  return TSDB_CODE_SUCCESS;

_cleanup:
  freeParam(param);
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

static UNUSED_FUNC int32_t ddx(SQueryTableMsg* pQueryMsg, SSqlFuncMsg** pExprMsg, SColumnInfo* pTagCols, SExprInfo* pExprs,
    int32_t numOfOutput, int32_t tagLen, bool superTable) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    int16_t functId = pExprs[i].base.functionId;

    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].base, pTagCols);
      if (j < 0 || j >= pQueryMsg->numOfCols) {
        return TSDB_CODE_QRY_INVALID_MSG;
      } else {
        SColumnInfo* pCol = &pQueryMsg->colList[j];
        int32_t ret = getResultDataInfo(pCol->type, pCol->bytes, functId, (int32_t)pExprs[i].base.arg[0].argValue.i64,
                                        &pExprs[i].type, &pExprs[i].bytes, &pExprs[i].interBytes, tagLen, superTable);
        assert(ret == TSDB_CODE_SUCCESS);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createQueryFuncExprFromMsg(SQueryTableMsg* pQueryMsg, int32_t numOfOutput, SExprInfo** pExprInfo,
                                   SSqlFuncMsg** pExprMsg, SColumnInfo* pTagCols) {
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
      bytes = tDataTypes[type].bytes;
    } else if (pExprs[i].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX && pExprs[i].base.functionId == TSDB_FUNC_TAGPRJ) {  // parse the normal column
      SSchema* s = tGetTbnameColumnSchema();
      type = s->type;
      bytes = s->bytes;
    } else if (pExprs[i].base.colInfo.colId == TSDB_BLOCK_DIST_COLUMN_INDEX) {
      SSchema s = tGetBlockDistColumnSchema(); 
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
      if (TSDB_COL_IS_TAG(pExprs[i].base.colInfo.flag)) {
        if (j < TSDB_BLOCK_DIST_COLUMN_INDEX || j >= pQueryMsg->numOfTags) {
          return TSDB_CODE_QRY_INVALID_MSG;
        }
      } else {
        if (j < PRIMARYKEY_TIMESTAMP_COL_INDEX || j >= pQueryMsg->numOfCols) {
          return TSDB_CODE_QRY_INVALID_MSG;
        }
      }

      if (pExprs[i].base.colInfo.colId != TSDB_TBNAME_COLUMN_INDEX && j >= 0) {
        SColumnInfo* pCol = (TSDB_COL_IS_TAG(pExprs[i].base.colInfo.flag))? &pTagCols[j]:&pQueryMsg->colList[j];
        type = pCol->type;
        bytes = pCol->bytes;
      } else {
        SSchema* s = tGetTbnameColumnSchema();

        type  = s->type;
        bytes = s->bytes;
      }
    }

    int32_t param = (int32_t)pExprs[i].base.arg[0].argValue.i64;
    if (pExprs[i].base.functionId != TSDB_FUNC_ARITHM &&
       (type != pExprs[i].base.colType || bytes != pExprs[i].base.colBytes)) {
      tfree(pExprs);
      return TSDB_CODE_QRY_INVALID_MSG;
    }

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

//  ddx(pQueryMsg, pExprMsg, pTagCols, pExprs, numOfOutput, tagLen, isSuperTable);

  *pExprInfo = pExprs;
  return TSDB_CODE_SUCCESS;
}

int32_t createIndirectQueryFuncExprFromMsg(SQueryTableMsg *pQueryMsg, int32_t numOfOutput, SExprInfo **pExprInfo,
    SSqlFuncMsg **pExprMsg, SExprInfo *prevExpr) {
  *pExprInfo = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SExprInfo *pExprs = (SExprInfo *)calloc(numOfOutput, sizeof(SExprInfo));
  if (pExprs == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  bool isSuperTable = QUERY_IS_STABLE_QUERY(pQueryMsg->queryType);

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
      bytes = tDataTypes[type].bytes;
    } else {
      int32_t index = pExprs[i].base.colInfo.colIndex;
      assert(prevExpr[index].base.resColId == pExprs[i].base.colInfo.colId);

      type  = prevExpr[index].type;
      bytes = prevExpr[index].bytes;
    }

    int32_t param = (int32_t)pExprs[i].base.arg[0].argValue.i64;
    if (getResultDataInfo(type, bytes, pExprs[i].base.functionId, param, &pExprs[i].type, &pExprs[i].bytes,
                          &pExprs[i].interBytes, 0, isSuperTable) != TSDB_CODE_SUCCESS) {
      tfree(pExprs);
      return TSDB_CODE_QRY_INVALID_MSG;
    }

    assert(isValidDataType(pExprs[i].type));
  }

  *pExprInfo = pExprs;
  return TSDB_CODE_SUCCESS;
}

SSqlGroupbyExpr *createGroupbyExprFromMsg(SQueryTableMsg *pQueryMsg, SColIndex *pColIndex, int32_t *code) {
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

        pSingleColFilter->fp = getFilterOperator(lower, upper);
        if (pSingleColFilter->fp == NULL) {
          qError("QInfo:%p invalid filter info", pQInfo);
          return TSDB_CODE_QRY_INVALID_MSG;
        }

        pSingleColFilter->bytes = pQuery->colList[i].bytes;
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
    } else if (pColIndex->colId == TSDB_BLOCK_DIST_COLUMN_INDEX) {
      pColIndex->colIndex = 0;// only one source column, so it must be 0;
      assert(pQuery->numOfOutput == 1);
    } else {
      int32_t f = 0;
      for (f = 0; f < pQuery->numOfTags; ++f) {
        if (pColIndex->colId == pQuery->tagColList[f].colId) {
          pColIndex->colIndex = f;
          break;
        }
      }

      assert(f < pQuery->numOfTags || pColIndex->colId == TSDB_TBNAME_COLUMN_INDEX || pColIndex->colId == TSDB_BLOCK_DIST_COLUMN_INDEX);
    }
  }
}

static void calResultBufSize(SQuery* pQuery, SRspResultInfo* pResultInfo) {
  const int32_t RESULT_MSG_MIN_SIZE  = 1024 * (1024 + 512);  // bytes
  const int32_t RESULT_MSG_MIN_ROWS  = 8192;
  const float RESULT_THRESHOLD_RATIO = 0.85f;

  if (isProjQuery(pQuery)) {
    int32_t numOfRes = RESULT_MSG_MIN_SIZE / pQuery->resultRowSize;
    if (numOfRes < RESULT_MSG_MIN_ROWS) {
      numOfRes = RESULT_MSG_MIN_ROWS;
    }

    pResultInfo->capacity  = numOfRes;
    pResultInfo->threshold = (int32_t)(numOfRes * RESULT_THRESHOLD_RATIO);
  } else {  // in case of non-prj query, a smaller output buffer will be used.
    pResultInfo->capacity = 4096;
    pResultInfo->threshold = (int32_t)(pResultInfo->capacity * RESULT_THRESHOLD_RATIO);
  }
}

SQInfo* createQInfoImpl(SQueryTableMsg* pQueryMsg, SSqlGroupbyExpr* pGroupbyExpr, SExprInfo* pExprs,
                        SExprInfo* pSecExprs, STableGroupInfo* pTableGroupInfo, SColumnInfo* pTagCols, bool stableQuery,
                        char* sql) {
  int16_t numOfCols = pQueryMsg->numOfCols;
  int16_t numOfOutput = pQueryMsg->numOfOutput;

  SQInfo *pQInfo = (SQInfo *)calloc(1, sizeof(SQInfo));
  if (pQInfo == NULL) {
    goto _cleanup_qinfo;
  }

  // to make sure third party won't overwrite this structure
  pQInfo->signature = pQInfo;
  SQuery* pQuery = &pQInfo->query;
  pQInfo->runtimeEnv.pQuery = pQuery;

  pQuery->tableGroupInfo  = *pTableGroupInfo;
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

  pQuery->srcRowSize = 0;
  pQuery->maxSrcColumnSize = 0;
  for (int16_t i = 0; i < numOfCols; ++i) {
    pQuery->colList[i] = pQueryMsg->colList[i];
    pQuery->colList[i].filters = tFilterInfoDup(pQueryMsg->colList[i].filters, pQuery->colList[i].numOfFilters);

    pQuery->srcRowSize += pQuery->colList[i].bytes;
    if (pQuery->maxSrcColumnSize < pQuery->colList[i].bytes) {
      pQuery->maxSrcColumnSize = pQuery->colList[i].bytes;
    }
  }

  // calculate the result row size
  for (int16_t col = 0; col < numOfOutput; ++col) {
    assert(pExprs[col].bytes > 0);
    pQuery->resultRowSize += pExprs[col].bytes;

    // keep the tag length
    if (TSDB_COL_IS_TAG(pExprs[col].base.colInfo.flag)) {
      pQuery->tagLen += pExprs[col].bytes;
    }
  }

  doUpdateExprColumnIndex(pQuery);
  int32_t ret = createFilterInfo(pQInfo, pQuery);
  if (ret != TSDB_CODE_SUCCESS) {
    goto _cleanup;
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
    STableGroupInfo* pTableqinfo = &pQInfo->runtimeEnv.tableqinfoGroupInfo;

    pTableqinfo->pGroupList = taosArrayInit(numOfGroups, POINTER_BYTES);
    pTableqinfo->numOfTables = pTableGroupInfo->numOfTables;
    pTableqinfo->map = taosHashInit(pTableGroupInfo->numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  }

  pQInfo->pBuf = calloc(pTableGroupInfo->numOfTables, sizeof(STableQueryInfo));
  if (pQInfo->pBuf == NULL) {
    goto _cleanup;
  }

  // NOTE: pTableCheckInfo need to update the query time range and the lastKey info
  pQInfo->arrTableIdInfo = taosHashInit(pTableGroupInfo->numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  pQInfo->dataReady = QUERY_RESULT_NOT_READY;
  pQInfo->rspContext = NULL;
  pQInfo->sql = sql;
  pthread_mutex_init(&pQInfo->lock, NULL);
  tsem_init(&pQInfo->ready, 0, 0);

  pQuery->window = pQueryMsg->window;
  changeExecuteScanOrder(pQInfo, pQueryMsg, stableQuery);

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  pQuery->queryWindowIdentical = true;
  bool groupByCol = isGroupbyColumn(pQuery->pGroupbyExpr);

  STimeWindow window = pQuery->window;

  int32_t index = 0;
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray* pa = taosArrayGetP(pQuery->tableGroupInfo.pGroupList, i);

    size_t s = taosArrayGetSize(pa);
    SArray* p1 = taosArrayInit(s, POINTER_BYTES);
    if (p1 == NULL) {
      goto _cleanup;
    }

    taosArrayPush(pRuntimeEnv->tableqinfoGroupInfo.pGroupList, &p1);

    for(int32_t j = 0; j < s; ++j) {
      STableKeyInfo* info = taosArrayGet(pa, j);

      window.skey = info->lastKey;
      if (info->lastKey != pQuery->window.skey) {
        pQInfo->query.queryWindowIdentical = false;
      }

      void* buf = (char*) pQInfo->pBuf + index * sizeof(STableQueryInfo);
      STableQueryInfo* item = createTableQueryInfo(pQuery, info->pTable, groupByCol, window, buf);
      if (item == NULL) {
        goto _cleanup;
      }

      item->groupIndex = i;
      taosArrayPush(p1, &item);

      STableId* id = TSDB_TABLEID(info->pTable);
      taosHashPut(pRuntimeEnv->tableqinfoGroupInfo.map, &id->tid, sizeof(id->tid), &item, POINTER_BYTES);
      index += 1;
    }
  }

  colIdCheck(pQuery, pQInfo);

  // todo refactor
  pQInfo->query.queryBlockDist = (numOfOutput == 1 && pExprs[0].base.colInfo.colId == TSDB_BLOCK_DIST_COLUMN_INDEX);
   
  qDebug("qmsg:%p QInfo:%p created", pQueryMsg, pQInfo);
  return pQInfo;

_cleanup_qinfo:
  tsdbDestroyTableGroup(pTableGroupInfo);

  if (pGroupbyExpr != NULL) {
    taosArrayDestroy(pGroupbyExpr->columnInfo);
    free(pGroupbyExpr);
  }

  tfree(pTagCols);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo = &pExprs[i];
    if (pExprInfo->pExpr != NULL) {
      tExprTreeDestroy(pExprInfo->pExpr, NULL);
      pExprInfo->pExpr = NULL;
    }
  }

  tfree(pExprs);

_cleanup:
  freeQInfo(pQInfo);
  return NULL;
}

bool isValidQInfo(void *param) {
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

int32_t initQInfo(SQueryTableMsg *pQueryMsg, void *tsdb, int32_t vgId, SQInfo *pQInfo, SQueryParam* param, bool isSTable) {
  int32_t code = TSDB_CODE_SUCCESS;

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  STSBuf *pTsBuf = NULL;
  if (pQueryMsg->tsLen > 0) { // open new file to save the result
    char *tsBlock = (char *) pQueryMsg + pQueryMsg->tsOffset;
    pTsBuf = tsBufCreateFromCompBlocks(tsBlock, pQueryMsg->tsNumOfBlocks, pQueryMsg->tsLen, pQueryMsg->tsOrder, vgId);

    tsBufResetPos(pTsBuf);
    bool ret = tsBufNextPos(pTsBuf);

    UNUSED(ret);
  }

  SArray* prevResult = NULL;
  if (pQueryMsg->prevResultLen > 0) {
    prevResult = interResFromBinary(param->prevResult, pQueryMsg->prevResultLen);
  }

  pQuery->precision = tsdbGetCfg(tsdb)->precision;

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.skey > pQuery->window.ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->window.ekey > pQuery->window.skey))) {
    qDebug("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->window.skey,
           pQuery->window.ekey, pQuery->order.order);
    setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
    pRuntimeEnv->tableqinfoGroupInfo.numOfTables = 0;
    // todo free memory
    return TSDB_CODE_SUCCESS;
  }

  if (pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:%p no table qualified for tag filter, abort query", pQInfo);
    setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
    return TSDB_CODE_SUCCESS;
  }

  // filter the qualified
  if ((code = doInitQInfo(pQInfo, pTsBuf, prevResult, tsdb, vgId, isSTable)) != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return code;

_error:
  // table query ref will be decrease during error handling
  freeQInfo(pQInfo);
  return code;
}

void freeColumnFilterInfo(SColumnFilterInfo* pFilter, int32_t numOfFilters) {
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
      tExprTreeDestroy(pExprInfo[i].pExpr, NULL);
    }
  }

  tfree(pExprInfo);
  return NULL;
}

void freeQInfo(SQInfo *pQInfo) {
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  qDebug("QInfo:%p start to free QInfo", pQInfo);

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  releaseQueryBuf(pRuntimeEnv->tableqinfoGroupInfo.numOfTables);
  teardownQueryRuntimeEnv(&pQInfo->runtimeEnv);

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;
  if (pQuery != NULL) {
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
  }

  doDestroyTableQueryInfo(&pRuntimeEnv->tableqinfoGroupInfo);

  tfree(pQInfo->pBuf);
  tfree(pQInfo->sql);

  tsdbDestroyTableGroup(&pQuery->tableGroupInfo);
  taosHashCleanup(pQInfo->arrTableIdInfo);

  taosArrayDestroy(pRuntimeEnv->groupResInfo.pRows);

  pQInfo->signature = 0;

  qDebug("QInfo:%p QInfo is freed", pQInfo);

  tfree(pQInfo);
}

int32_t doDumpQueryResult(SQInfo *pQInfo, char *data) {
  // the remained number of retrieved rows, not the interpolated result
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  // load data from file to msg buffer
  if (isTsCompQuery(pQuery)) {
    SColumnInfoData* pColInfoData = taosArrayGet(pRuntimeEnv->outputBuf->pDataBlock, 0);
    FILE *f = *(FILE **)pColInfoData->pData;  // TODO refactor

    // make sure file exist
    if (f) {
      off_t s = lseek(fileno(f), 0, SEEK_END);
      assert(s == pRuntimeEnv->outputBuf->info.rows);

      qDebug("QInfo:%p ts comp data return, file:%p, size:%"PRId64, pQInfo, f, (uint64_t)s);
      if (fseek(f, 0, SEEK_SET) >= 0) {
        size_t sz = fread(data, 1, s, f);
        if(sz < s) {  // todo handle error
          qError("fread(f:%p,%d) failed, rsize:%" PRId64 ", expect size:%" PRId64, f, fileno(f), (uint64_t)sz, (uint64_t)s);
          assert(0);
        }
      } else {
        UNUSED(s);
        qError("fseek(f:%p,%d) failed, error:%s", f, fileno(f), strerror(errno));
        assert(0);
      }

      // dump error info
      if (s <= (sizeof(STSBufFileHeader) + sizeof(STSGroupBlockInfo) + 6 * sizeof(int32_t))) {
        qDump(data, s);
        assert(0);
      }

      fclose(f);
    }

    // all data returned, set query over
    if (Q_STATUS_EQUAL(pRuntimeEnv->status, QUERY_COMPLETED)) {
      setQueryStatus(pRuntimeEnv, QUERY_OVER);
    }
  } else {
    doCopyQueryResultToMsg(pQInfo, (int32_t)pRuntimeEnv->outputBuf->info.rows, data);
  }

  pRuntimeEnv->resultInfo.total += pRuntimeEnv->outputBuf->info.rows;
  qDebug("QInfo:%p current numOfRes rows:%d, total:%" PRId64, pQInfo,
         pRuntimeEnv->outputBuf->info.rows, pRuntimeEnv->resultInfo.total);

  if (pQuery->limit.limit > 0 && pQuery->limit.limit == pRuntimeEnv->resultInfo.total) {
    qDebug("QInfo:%p results limitation reached, limitation:%"PRId64, pQInfo, pQuery->limit.limit);
    setQueryStatus(pRuntimeEnv, QUERY_OVER);
  }

  return TSDB_CODE_SUCCESS;
}

bool doBuildResCheck(SQInfo* pQInfo) {
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

static void doSetTagValueToResultBuf(char* output, const char* val, int16_t type, int16_t bytes) {
  if (val == NULL) {
    setNull(output, type, bytes);
    return;
  }

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    memcpy(output, val, varDataTLen(val));
  } else {
    memcpy(output, val, bytes);
  }
}

void buildTagQueryResult(SQInfo* pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  assert(numOfGroup == 0 || numOfGroup == 1);

  if (numOfGroup == 0) {
    return;
  }

  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);

#if 0
  SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);

  size_t num = taosArrayGetSize(pa);
  assert(num == pRuntimeEnv->tableqinfoGroupInfo.numOfTables);

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

    while(pRuntimeEnv->tableIndex < num && count < pRuntimeEnv->resultInfo.capacity) {
      int32_t i = pRuntimeEnv->tableIndex++;
      STableQueryInfo *item = taosArrayGetP(pa, i);

      char *output = NULL;//pQuery->sdata[0]->data + count * rsize;
      varDataSetLen(output, rsize - VARSTR_HEADER_SIZE);

      output = varDataVal(output);
      STableId* id = TSDB_TABLEID(item->pTable);

      *(int16_t *)output = 0;
      output += sizeof(int16_t);

      *(int64_t *)output = id->uid;  // memory align problem, todo serialize
      output += sizeof(id->uid);

      *(int32_t *)output = id->tid;
      output += sizeof(id->tid);

      *(int32_t *)output = pQuery->vgId;
      output += sizeof(pQuery->vgId);

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
//    *(int64_t*) pQuery->sdata[0]->data = num;

    count = 1;
    SET_STABLE_QUERY_OVER(pRuntimeEnv);
    qDebug("QInfo:%p create count(tbname) query, res:%d rows:1", pQInfo, count);
  } else {  // return only the tags|table name etc.
    count = 0;
    SSchema* tbnameSchema = tGetTbnameColumnSchema();

    int32_t maxNumOfTables = (int32_t)pRuntimeEnv->resultInfo.capacity;
    if (pQuery->limit.limit >= 0 && pQuery->limit.limit < pRuntimeEnv->resultInfo.capacity) {
      maxNumOfTables = (int32_t)pQuery->limit.limit;
    }

    while(pRuntimeEnv->tableIndex < num && count < maxNumOfTables) {
      int32_t i = pRuntimeEnv->tableIndex++;

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
          bytes = tbnameSchema->bytes;
          type = tbnameSchema->type;

          data = tsdbGetTableName(item->pTable);
//          dst = pQuery->sdata[j]->data + count * tbnameSchema->bytes;
        } else {
          type = pExprInfo[j].type;
          bytes = pExprInfo[j].bytes;

          data = tsdbGetTableTagVal(item->pTable, pExprInfo[j].base.colInfo.colId, type, bytes);
//          dst = pQuery->sdata[j]->data + count * pExprInfo[j].bytes;

        }

        doSetTagValueToResultBuf(dst, data, type, bytes);
      }
      count += 1;
    }

    qDebug("QInfo:%p create tag values results completed, rows:%d", pQInfo, count);
  }

  pRuntimeEnv->resultInfo.rows = count;
  setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
#endif
}

static int64_t getQuerySupportBufSize(size_t numOfTables) {
  size_t s1 = sizeof(STableQueryInfo);
  size_t s2 = sizeof(SHashNode);

//  size_t s3 = sizeof(STableCheckInfo);  buffer consumption in tsdb
  return (int64_t)((s1 + s2) * 1.5 * numOfTables);
}

int32_t checkForQueryBuf(size_t numOfTables) {
  int64_t t = getQuerySupportBufSize(numOfTables);
  if (tsQueryBufferSizeBytes < 0) {
    return TSDB_CODE_SUCCESS;
  } else if (tsQueryBufferSizeBytes > 0) {

    while(1) {
      int64_t s = tsQueryBufferSizeBytes;
      int64_t remain = s - t;
      if (remain >= 0) {
        if (atomic_val_compare_exchange_64(&tsQueryBufferSizeBytes, s, remain) == s) {
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
  if (tsQueryBufferSizeBytes < 0) {
    return;
  }

  int64_t t = getQuerySupportBufSize(numOfTables);

  // restore value is not enough buffer available
  atomic_add_fetch_64(&tsQueryBufferSizeBytes, t);
}
