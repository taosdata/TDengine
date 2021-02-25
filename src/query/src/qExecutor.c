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

#define MAX_ROWS_PER_RESBUF_PAGE  ((1u<<12) - 1)

/**
 * check if the primary column is load by default, otherwise, the program will
 * forced to load primary column explicitly.
 */

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

typedef struct {
  SArray  *dataBlockInfos; 
  int64_t firstSeekTimeUs; 
  int64_t numOfRowsInMemTable;
  char    *result;
} STableBlockDist;

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

//static void setResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult);
static void setResultOutputBuf_rv(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx,
    int32_t numOfCols, int32_t* rowCellInfoOffset);

void setResultRowOutputBufInitCtx(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset);
static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void* inputData, TSKEY *tsCol, SDataBlockInfo* pBlockInfo,
                          SDataStatis *pStatis, SExprInfo* pExprInfo);
static void setBlockStatisInfo(SQLFunctionCtx *pCtx, SSDataBlock* pSDataBlock, SColIndex* pColIndex);

//static void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx);
static void destroyTableQueryInfoImpl(STableQueryInfo *pTableQueryInfo);
//static void resetDefaultResInfoOutputBuf(SQueryRuntimeEnv *pRuntimeEnv);
static bool hasMainOutput(SQuery *pQuery);

//static int32_t setTimestampListJoinInfo(SQueryRuntimeEnv* pRuntimeEnv, STableQueryInfo *pTableQueryInfo);
static void releaseQueryBuf(size_t numOfTables);
static int32_t binarySearchForKey(char *pValue, int num, TSKEY key, int order);
//static void doRowwiseTimeWindowInterpolation(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey, int32_t type);
static STsdbQueryCond createTsdbQueryCond(SQuery* pQuery, STimeWindow* win);
static STableIdInfo createTableIdInfo(SQuery* pQuery);

static SOperatorInfo* createBiDirectionTableScanInfo(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime, int32_t reverseTime);
static SOperatorInfo* createTableScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime);
static void setTableScanFilterOperatorInfo(STableScanInfo* pTableScanInfo, SOperatorInfo* pDownstream);

static int32_t getNumOfScanTimes(SQuery* pQuery);
static bool isFixedOutputQuery(SQuery* pQuery);

static SOperatorInfo* createAggregateOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
static void destroyBasicOperatorInfo(void* param, int32_t numOfOutput);
static void destroySFillOperatorInfo(void* param, int32_t numOfOutput);
static void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput);
static void destroyArithOperatorInfo(void* param, int32_t numOfOutput);

static SOperatorInfo* createArithOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);

static SOperatorInfo* createLimitOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream);

static SOperatorInfo* createOffsetOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream);

static SOperatorInfo* createIntervalAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);

static SOperatorInfo* createFillOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr,
                                      int32_t numOfOutput);

static SOperatorInfo* createHashGroupbyOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr,
                                                    int32_t numOfOutput);

static SOperatorInfo* createStableAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);

static SOperatorInfo* createStableIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);

static SOperatorInfo* createTagScanOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv);

static int32_t doCopyToSData_rv(SQueryRuntimeEnv* pRuntimeEnv, SGroupResInfo* pGroupResInfo, int32_t orderType, SSDataBlock* pBlock);

static char *getGroupbyColumnData(SQuery *pQuery, int16_t *type, int16_t *bytes, SArray* pDataBlock);
static int32_t getGroupbyColumnData_rv(SSqlGroupbyExpr *pGroupbyExpr, SSDataBlock* pDataBlock);

//static int32_t setGroupResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo* pResultRowInfo, char *pData, int16_t type, int16_t bytes, int32_t groupIndex);
static int32_t setGroupResultOutputBuf_rv(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo* pResultRowInfo,
                                          SQLFunctionCtx * pCtx, int32_t numOfCols, char *pData, int16_t type, int16_t bytes, int32_t groupIndex, int32_t* offset);
static void destroyOperatorInfo(SOperatorInfo* pOperator);
void initCtxOutputBuf_rv(SQLFunctionCtx* pCtx, int32_t size);
void getAlignQueryTimeWindow(SQuery *pQuery, int64_t key, int64_t keyFirst, int64_t keyLast, STimeWindow *win);

// setup the output buffer
static SSDataBlock* createOutputBuf(SExprInfo* pExpr, int32_t numOfOutput) {
  SSDataBlock *res = calloc(1, sizeof(SSDataBlock));
  res->info.numOfCols = numOfOutput;

  res->pDataBlock = taosArrayInit(numOfOutput, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData idata = {0};
    idata.info.type = pExpr[i].type;
    idata.info.bytes = pExpr[i].bytes;
    idata.info.colId = pExpr[i].base.resColId;
    idata.pData = calloc(4096, idata.info.bytes);

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

int64_t getNumOfResult_rv(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx, int32_t numOfOutput) {
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
   if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
     if (QUERY_IS_ASC_QUERY(pQuery)) {
       getAlignQueryTimeWindow(pQuery, ts, ts, pQuery->window.ekey, &w);
     } else { // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
       getAlignQueryTimeWindow(pQuery, ts, pQuery->window.ekey, ts, &w);
     }

     pWindowResInfo->prevSKey = w.skey;
   } else {
    w.skey = pWindowResInfo->prevSKey;
   }

    if (pQuery->interval.intervalUnit == 'n' || pQuery->interval.intervalUnit == 'y') {
      w.ekey = taosTimeAdd(w.skey, pQuery->interval.interval, pQuery->interval.intervalUnit, pQuery->precision) - 1;
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
    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, (int32_t) groupId, pRuntimeEnv->pQuery->resultRowSize);
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

static void updateResultRowIndex(SResultRowInfo* pResultRowInfo, STableQueryInfo* pTableQueryInfo, bool ascQuery, bool timeWindowInterpo) {
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

static char *getDataBlock(SQuery* pQuery, SArithmeticSupport *sas, int32_t col, int32_t size, SArray *pDataBlock);


static void doBlockwiseApplyFunctions_rv(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx, STimeWindow *pWin,
    int32_t offset, int32_t forwardStep, TSKEY *tsCol, int32_t numOfTotal, int32_t numOfOutput) {
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

//todo binary search
static UNUSED_FUNC void* getDataBlockImpl(SArray* pDataBlock, int32_t colId) {
  int32_t numOfCols = (int32_t)taosArrayGetSize(pDataBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData *p = taosArrayGet(pDataBlock, i);
    if (colId == p->info.colId) {
      return p->pData;
    }
  }

  return NULL;
}


// todo refactor
static UNUSED_FUNC char *getDataBlock(SQuery* pQuery, SArithmeticSupport *sas, int32_t col, int32_t size, SArray *pDataBlock) {
  if (pDataBlock == NULL) {
    return NULL;
  }

  char *dataBlock = NULL;
  int32_t functionId = pQuery->pExpr1[col].base.functionId;
  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pArithExpr = &pQuery->pExpr1[col];
    sas->offset     = (QUERY_IS_ASC_QUERY(pQuery))? pQuery->pos : pQuery->pos - (size - 1);
    sas->colList    = pQuery->colList;
    sas->numOfCols  = pQuery->numOfCols;

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


static UNUSED_FUNC void saveDataBlockLastRow(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pDataBlockInfo, SArray* pDataBlock,
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

static TSKEY getStartTsKey(SQuery* pQuery, SDataBlockInfo* pDataBlockInfo, const TSKEY* tsCols, int32_t step) {
  TSKEY ts = TSKEY_INITIAL_VAL;

  if (tsCols == NULL) {
    ts = QUERY_IS_ASC_QUERY(pQuery) ? pDataBlockInfo->window.skey : pDataBlockInfo->window.ekey;
  } else {
    int32_t offset = GET_COL_DATA_POS(pQuery, 0, step);
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
    pCtx[i].size = pBlock->info.rows;
    pCtx[i].order = order;
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
    pCtx[i].size = pBlock->info.rows;
    pCtx[i].order = order;
    pCtx[i].currentStage = pOperator->pRuntimeEnv->scanFlag;

    setBlockStatisInfo(&pCtx[i], pBlock, &pOperator->pExpr[i].base.colInfo);

    if (pCtx[i].functionId == TSDB_FUNC_ARITHM) {
      setArithParams((SArithmeticSupport*)pCtx[i].param[1].pz, &pOperator->pExpr[i], pBlock);
    } else {
      SColIndex* pCol = &pOperator->pExpr[i].base.colInfo;
      if (TSDB_COL_IS_NORMAL_COL(pCol->flag)) {
        SColIndex*       pColIndex = &pOperator->pExpr[i].base.colInfo;
        SColumnInfoData* p = taosArrayGet(pBlock->pDataBlock, pColIndex->colIndex);

        pCtx[i].pInput = p->pData;

        assert(p->info.colId == pColIndex->colId && pCtx[i].inputType == p->info.type && pCtx[i].inputBytes == p->info.bytes);

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
//    setBlockStatisInfo(&pCtx[k], pSDataBlock, &pOperator->pExpr[k].base.colInfo);

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

static void hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo,
                            SHashIntervalOperatorInfo* pInfo, SSDataBlock* pSDataBlock, int32_t groupId) {
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

  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : (pSDataBlock->info.rows - 1);
  int32_t startPos = pQuery->pos;

  TSKEY ts = getStartTsKey(pQuery, &pSDataBlock->info, tsCols, step);

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
      getNumOfRowsInTimeWindow(pQuery, &pSDataBlock->info, tsCols, pQuery->pos, ekey, binarySearchForKey, true);

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

      //      int32_t p = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pSDataBlock->info.rows - 1;
      //      doRowwiseTimeWindowInterpolation(pRuntimeEnv, pSDataBlock->pDataBlock, *(TSKEY *)pRuntimeEnv->prevRow[0],
      //      -1, tsCols[0], p,
      //                                       w.ekey, RESULT_ROW_END_INTERP);
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
      setNotInterpoWindowKey(pInfo->pCtx, pQuery->numOfOutput, RESULT_ROW_START_INTERP);

      doBlockwiseApplyFunctions_rv(pRuntimeEnv, pInfo->pCtx, &w, startPos, 0, tsCols, pSDataBlock->info.rows,
                                   numOfOutput);
    }

    // restore current time window
    ret = setWindowOutputBufByKey(pRuntimeEnv, pResultRowInfo, &win, masterScan, &pResult, groupId, pInfo->pCtx,
                                  numOfOutput, pInfo->rowCellInfoOffset);
    assert(ret == TSDB_CODE_SUCCESS);
  }

  // window start key interpolation
  // doWindowBorderInterpolation(pRuntimeEnv, &pSDataBlock->info, pSDataBlock->pDataBlock, pResult, &win, pQuery->pos,
  // forwardStep);
  doBlockwiseApplyFunctions_rv(pRuntimeEnv, pInfo->pCtx, &win, startPos, forwardStep, tsCols, pSDataBlock->info.rows,
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
    //    doWindowBorderInterpolation(pRuntimeEnv, &pSDataBlock->info, pSDataBlock->pDataBlock, pResult, &nextWin,
    //    startPos, forwardStep);
    doBlockwiseApplyFunctions_rv(pRuntimeEnv, pInfo->pCtx, &nextWin, startPos, forwardStep, tsCols,
                                 pSDataBlock->info.rows, numOfOutput);
  }
}

static void hashGroupbyAgg(SQueryRuntimeEnv *pRuntimeEnv, SOperatorInfo* pOperator, SHashGroupbyOperatorInfo *pInfo,
                           SSDataBlock *pSDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STableQueryInfo* item = pQuery->current;

  SColumnInfoData* pColInfoData = taosArrayGet(pSDataBlock->pDataBlock, pInfo->colIndex);
  int16_t bytes = pColInfoData->info.bytes;
  int16_t type  = pColInfoData->info.type;

  for (int32_t j = 0; j < pSDataBlock->info.rows; ++j) {
    int32_t offset = GET_COL_DATA_POS(pQuery, j, 1);

    char *val = pColInfoData->pData + bytes * offset;
    if (isNull(val, type)) {  // ignore the null value
      continue;
    }

    // TODO compare with the previous value to speedup the query processing
    int32_t ret = setGroupResultOutputBuf_rv(pRuntimeEnv, &pInfo->binfo.resultRowInfo, pInfo->binfo.pCtx, pOperator->numOfOutput, val, type, bytes, item->groupIndex, pInfo->binfo.rowCellInfoOffset);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
    }

    for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
      pInfo->binfo.pCtx[k].size = 1;  // TODO refactor: extract from here
      int32_t functionId = pInfo->binfo.pCtx[k].functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pInfo->binfo.pCtx[k], functionId)) {
        aAggs[functionId].xFunctionF(&pInfo->binfo.pCtx[k], offset);
      }
    }
  }
}

#if 0

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

  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    char *dataBlock = getDataBlock(pQuery, &pRuntimeEnv->sasArray[k], k, pDataBlockInfo->rows, pDataBlock);
    setExecParams(pQuery, &pCtx[k], dataBlock, tsCols, pDataBlockInfo, pStatis, &pQuery->pExpr1[k]);
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
    if (prevIndex != -1 && prevIndex < curIndex && pQuery->timeWindowInterpo) {
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

        doBlockwiseApplyFunctions(pRuntimeEnv, &w, startPos, 0, tsCols, pDataBlockInfo->rows, pDataBlock);
      }

      // restore current time window
      ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId);
      assert (ret == TSDB_CODE_SUCCESS);
    }

    // window start key interpolation
    doWindowBorderInterpolation(pRuntimeEnv, pDataBlockInfo, pDataBlock, pResult, &win, pQuery->pos, forwardStep);
    doBlockwiseApplyFunctions(pRuntimeEnv, &win, startPos, forwardStep, tsCols, pDataBlockInfo->rows, pDataBlock);

    STimeWindow nextWin = win;
    while (1) {
      int32_t prevEndPos = (forwardStep - 1) * step + startPos;
      startPos = getNextQualifiedWindow(pQuery, &nextWin, pDataBlockInfo, tsCols, searchFn, prevEndPos);
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
      doBlockwiseApplyFunctions(pRuntimeEnv, &nextWin, startPos, forwardStep, tsCols, pDataBlockInfo->rows, pDataBlock);
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
        pCtx[k].startTs = pQuery->window.skey;
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
  }

  _end:
  if (pQuery->timeWindowInterpo) {
    int32_t rowIndex = QUERY_IS_ASC_QUERY(pQuery)? pDataBlockInfo->rows-1:0;
    saveDataBlockLastRow(pRuntimeEnv, pDataBlockInfo, pDataBlock, rowIndex);
  }
}
#endif

static int32_t setGroupResultOutputBuf_rv(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo* pResultRowInfo,
    SQLFunctionCtx * pCtx, int32_t numOfCols, char *pData, int16_t type, int16_t bytes, int32_t groupIndex, int32_t* rowCellInfoOffset) {
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

  setResultOutputBuf_rv(pRuntimeEnv, pResultRow, pCtx, numOfCols, rowCellInfoOffset);
  initCtxOutputBuf_rv(pCtx, numOfCols);
  return TSDB_CODE_SUCCESS;
}

static UNUSED_FUNC char *getGroupbyColumnData(SQuery *pQuery, int16_t *type, int16_t *bytes, SArray* pDataBlock) {
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

static int32_t getGroupbyColumnData_rv(SSqlGroupbyExpr *pGroupbyExpr, SSDataBlock* pDataBlock) {
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


#if 0
static UNUSED_FUNC void setTimeWindowSKeyInterp(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY ts, int32_t offset, SResultRow* pResult, STimeWindow* win) {
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

static UNUSED_FUNC void setTimeWindowEKeyInterp(SQueryRuntimeEnv* pRuntimeEnv, SArray* pDataBlock, TSKEY prevTs, int32_t prevRowIndex, TSKEY ts, int32_t offset, SResultRow* pResult, STimeWindow* win) {
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
  bool    groupbyColumnValue = pQuery->groupbyColumn;

  int16_t type = 0;
  int16_t bytes = 0;

  char *groupbyColumnData = NULL;
  if (groupbyColumnValue) {
    groupbyColumnData = getGroupbyColumnData(pQuery, &type, &bytes, pDataBlock);
  }

  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
    char *dataBlock = getDataBlock(pQuery, &pRuntimeEnv->sasArray[k], k, pDataBlockInfo->rows, pDataBlock);
    setExecParams(pQuery, &pCtx[k], dataBlock, tsCols, pDataBlockInfo, pStatis, &pQuery->pExpr1[k]);
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
      int64_t ts = tsCols[offset];

      STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

      SResultRow* pResult = NULL;
      int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, &win, masterScan, &pResult, groupId);
      if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {  // null data, too many state code
        goto _end;
      }

      // window start key interpolation
      if (pQuery->timeWindowInterpo) {
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
      int32_t index = pWindowResInfo->curIndex;

      STimeWindow nextWin = win;
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

      // restore the index, add the result row will move the index
      pWindowResInfo->curIndex = index;
    } else {  // other queries
      // decide which group this rows belongs to according to current state value
      char* val = NULL;
      if (groupbyColumnValue) {
        val = groupbyColumnData + bytes * offset;
        if (isNull(val, type)) {  // ignore the null value
          continue;
        }

        int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, &pRuntimeEnv->resultRowInfo, val, type, bytes, item->groupIndex);
        if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
          longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
        }
      }

      if (pQuery->stabledev) {
        for (int32_t k = 0; k < pQuery->numOfOutput; ++k) {
          int32_t functionId = pQuery->pExpr1[k].base.functionId;
          if (functionId != TSDB_FUNC_STDDEV_DST) {
            continue;
          }

          pRuntimeEnv->pCtx[k].param[0].arr = NULL;
          pRuntimeEnv->pCtx[k].param[0].nType = TSDB_DATA_TYPE_INT;  // avoid freeing the memory by setting the type to be int

          // todo opt perf
          int32_t numOfGroup = (int32_t)taosArrayGetSize(pRuntimeEnv->prevResult);
          for (int32_t i = 0; i < numOfGroup; ++i) {
            SInterResult *p = taosArrayGet(pRuntimeEnv->prevResult, i);
            if (memcmp(p->tags, val, bytes) == 0) {
              int32_t numOfCols = (int32_t)taosArrayGetSize(p->pResult);
              for (int32_t f = 0; f < numOfCols; ++f) {
                SStddevInterResult *pres = taosArrayGet(p->pResult, f);
                if (pres->colId == pQuery->pExpr1[k].base.colInfo.colId) {
                  pRuntimeEnv->pCtx[k].param[0].arr = pres->pResult;
                  break;
                }
              }
            }
          }
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
  assert(offset >= 0 && tsCols != NULL);
  if (prevTs != INT64_MIN && prevTs != *(int64_t*)pRuntimeEnv->prevRow[0]) {
    assert(prevRowIndex >= 0);
    item->lastKey = prevTs + step;
  }

  // In case of all rows in current block are not qualified
  if (pQuery->timeWindowInterpo && prevRowIndex != -1) {
    saveDataBlockLastRow(pRuntimeEnv, pDataBlockInfo, pDataBlock, prevRowIndex);
  }

  if (pRuntimeEnv->pTsBuf != NULL) {
    item->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
  }
}



static int32_t tableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo,
                                          SDataStatis *pStatis, __block_search_fn_t searchFn, SArray *pDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STableQueryInfo* pTableQueryInfo = pQuery->current;
  SResultRowInfo*  pResultRowInfo = &pRuntimeEnv->resultRowInfo;

//  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pQuery->groupbyColumn) {
//    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, pDataBlock);
//  } else {
//    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, searchFn, pDataBlock);
//  }

  // update the lastkey of current table for projection/aggregation query
  TSKEY lastKey = QUERY_IS_ASC_QUERY(pQuery) ? pDataBlockInfo->window.ekey : pDataBlockInfo->window.skey;
  pTableQueryInfo->lastKey = lastKey + GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // interval query with limit applied
  int32_t numOfRes = 0;
  if (QUERY_IS_INTERVAL_QUERY(pQuery) || pQuery->groupbyColumn) {
    numOfRes = pResultRowInfo->size;
    updateResultRowIndex(pResultRowInfo, pTableQueryInfo, QUERY_IS_ASC_QUERY(pQuery), pQuery->timeWindowInterpo);
  } else { // projection query
    numOfRes = (int32_t) getNumOfResult(pRuntimeEnv);

    // update the number of output result
    if (numOfRes > 0 && pQuery->checkResultBuf == 1) {
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

#endif

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

#if 0
  // set the statistics data for primary time stamp column
  if (pCtx->functionId == TSDB_FUNC_SPREAD &&colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    pCtx->preAggVals.isSet  = true;
    pCtx->preAggVals.statis.min = pBlockInfo->window.skey;
    pCtx->preAggVals.statis.max = pBlockInfo->window.ekey;
  }
#endif
}

void UNUSED_FUNC  setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void* inputData, TSKEY *tsCol, SDataBlockInfo* pBlockInfo,
                   SDataStatis *pStatis, SExprInfo* pExprInfo) {

//  int32_t functionId = pExprInfo->base.functionId;
//  int32_t colId = pExprInfo->base.colInfo.colId;

  SDataStatis *tpField = NULL;
  pCtx->hasNull = hasNullValue(&pExprInfo->base.colInfo, pStatis, &tpField);

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

  // set the start position in current block
//  int32_t offset = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->pos: (pQuery->pos - pCtx->size + 1);
//  if (inputData != NULL) {
//    pCtx->pInput = (char*)inputData + offset * pCtx->inputBytes;
//  }
//
//  uint32_t status = aAggs[functionId].status;
//  if (((status & (TSDB_FUNCSTATE_SELECTIVITY | TSDB_FUNCSTATE_NEED_TS)) != 0) && (tsCol != NULL)) {
//    pCtx->ptsList = tsCol + offset;
//  }

  // set the statistics data for primary time stamp column
//  if (functionId == TSDB_FUNC_SPREAD && colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
//    pCtx->preAggVals.isSet  = true;
//    pCtx->preAggVals.statis.min = pBlockInfo->window.skey;
//    pCtx->preAggVals.statis.max = pBlockInfo->window.ekey;
//  }
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

static int32_t setupQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv, int32_t numOfTables, int16_t order, int32_t vgId) {
  qDebug("QInfo:%p setup runtime env", GET_QINFO_ADDR(pRuntimeEnv));
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->pQuery = pQuery;

  pQuery->interBufSize = getOutputInterResultBufSize(pQuery);

  pRuntimeEnv->pResultRowHashTable = taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pRuntimeEnv->keyBuf  = malloc(pQuery->maxSrcColumnSize + sizeof(int64_t));
  pRuntimeEnv->pool    = initResultRowPool(getResultRowSize(pRuntimeEnv));
  pRuntimeEnv->prevRow = malloc(POINTER_BYTES * pQuery->numOfCols + pQuery->srcRowSize);
  pRuntimeEnv->tagVal  = malloc(pQuery->tagLen);
//  pRuntimeEnv->rowCellInfoOffset = calloc(pQuery->numOfOutput, sizeof(int32_t));
  pRuntimeEnv->sasArray = calloc(pQuery->numOfOutput, sizeof(SArithmeticSupport));

  if (/*pRuntimeEnv->rowCellInfoOffset == NULL || */pRuntimeEnv->sasArray == NULL ||
      pRuntimeEnv->pResultRowHashTable == NULL || pRuntimeEnv->keyBuf == NULL || pRuntimeEnv->prevRow == NULL ||
      pRuntimeEnv->tagVal == NULL) {
    goto _clean;
  }

  char* start = POINTER_BYTES * pQuery->numOfCols + (char*) pRuntimeEnv->prevRow;
  pRuntimeEnv->prevRow[0] = start;
  for(int32_t i = 1; i < pQuery->numOfCols; ++i) {
    pRuntimeEnv->prevRow[i] = pRuntimeEnv->prevRow[i - 1] + pQuery->colList[i-1].bytes;
  }

  *(int64_t*) pRuntimeEnv->prevRow[0] = INT64_MIN;

  qDebug("QInfo:%p init runtime completed", GET_QINFO_ADDR(pRuntimeEnv));

  // group by normal column, sliding window query, interval query are handled by interval query processor
  // interval (down sampling operation)
  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    if (pQuery->stableQuery) {
      pRuntimeEnv->proot = createStableIntervalOperatorInfo(pRuntimeEnv, pRuntimeEnv->pi, pQuery->pExpr1, pQuery->numOfOutput);
      setTableScanFilterOperatorInfo(pRuntimeEnv->pi->info, pRuntimeEnv->proot);
    } else {
      pRuntimeEnv->proot = createIntervalAggOperatorInfo(pRuntimeEnv, pRuntimeEnv->pi, pQuery->pExpr1, pQuery->numOfOutput);
      setTableScanFilterOperatorInfo(pRuntimeEnv->pi->info, pRuntimeEnv->proot);

      if (pQuery->pExpr2 != NULL) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot, pQuery->pExpr2, pQuery->numOfExpr2);
      }

      if (pQuery->fillType != TSDB_FILL_NONE) {
        SOperatorInfo* pInfo = pRuntimeEnv->proot;
        pRuntimeEnv->proot = createFillOperatorInfo(pRuntimeEnv, pInfo, pInfo->pExpr, pInfo->numOfOutput);
      }
    }

    } else if (pQuery->groupbyColumn) {
      pRuntimeEnv->proot = createHashGroupbyOperatorInfo(pRuntimeEnv, pRuntimeEnv->pi, pQuery->pExpr1, pQuery->numOfOutput);
      setTableScanFilterOperatorInfo(pRuntimeEnv->pi->info, pRuntimeEnv->proot);

      if (pQuery->pExpr2 != NULL) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot, pQuery->pExpr2, pQuery->numOfExpr2);
      }
    } else if (isFixedOutputQuery(pQuery)) {
      if (!pQuery->stableQuery) {
        pRuntimeEnv->proot = createAggregateOperatorInfo(pRuntimeEnv, pRuntimeEnv->pi, pQuery->pExpr1, pQuery->numOfOutput);
      } else {
        pRuntimeEnv->proot = createStableAggOperatorInfo(pRuntimeEnv, pRuntimeEnv->pi, pQuery->pExpr1, pQuery->numOfOutput);
      }

      setTableScanFilterOperatorInfo(pRuntimeEnv->pi->info, pRuntimeEnv->proot);

      if (pQuery->pExpr2 != NULL) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot, pQuery->pExpr2, pQuery->numOfExpr2);
      }
    } else {  // diff/add/multiply/subtract/division
      assert(pQuery->checkResultBuf == 1);
      if (!onlyQueryTags(pQuery)) {
        pRuntimeEnv->proot = createArithOperatorInfo(pRuntimeEnv, pRuntimeEnv->pi, pQuery->pExpr1, pQuery->numOfOutput);
        setTableScanFilterOperatorInfo(pRuntimeEnv->pi->info, pRuntimeEnv->proot);
      }
    }

    if (!pQuery->stableQuery || isProjQuery(pQuery)) {  // TODO this problem should be handed at the client side
      if (pQuery->limit.offset > 0) {
        pRuntimeEnv->proot = createOffsetOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot);
      }

      if (pQuery->limit.limit > 0) {
        pRuntimeEnv->proot = createLimitOperatorInfo(pRuntimeEnv, pRuntimeEnv->proot);
      }
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

static void doFreeQueryHandle(SQInfo* pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;

  tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
//  tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);

  pRuntimeEnv->pQueryHandle = NULL;
//  pRuntimeEnv->pSecQueryHandle = NULL;

  SMemRef* pMemRef = &pQuery->memRef;
  assert(pMemRef->ref == 0 && pMemRef->imem == NULL && pMemRef->mem == NULL);
}



static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo* pQInfo = (SQInfo*) GET_QINFO_ADDR(pRuntimeEnv);

  qDebug("QInfo:%p teardown runtime env", pQInfo);

  if (isTsCompQuery(pQuery)) {
    FILE *f = *(FILE **)pQuery->sdata[0]->data;

    if (f) {
      fclose(f);
      *(FILE **)pQuery->sdata[0]->data = NULL;
    }
  }

  if (pRuntimeEnv->sasArray != NULL) {
    for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
      tfree(pRuntimeEnv->sasArray[i].data);
      tfree(pRuntimeEnv->sasArray[i].colList);
    }

    tfree(pRuntimeEnv->sasArray);
  }

  pRuntimeEnv->pFillInfo = taosDestroyFillInfo(pRuntimeEnv->pFillInfo);

  destroyResultBuf(pRuntimeEnv->pResultBuf);
  doFreeQueryHandle(pQInfo);

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
  if (pQuery->topBotQuery || pQuery->groupbyColumn) {
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


void filterDataBlock_rv(SSingleColumnFilterInfo *pFilterInfo, int32_t numOfFilterCols, SSDataBlock* pBlock) {
  int32_t numOfRows = pBlock->info.rows;

  int8_t *p = calloc(numOfRows, sizeof(int8_t));
  bool    all = true;

  for (int32_t i = 0; i < numOfRows; ++i) {
    bool qualified = false;

    for (int32_t k = 0; k < numOfFilterCols; ++k) {
      char *pElem = (char *)pFilterInfo[k].pData + pFilterInfo[k].info.bytes * i;

      qualified = false;
      for (int32_t j = 0; j < pFilterInfo[k].numOfFilters; ++j) {
        SColumnFilterElem *pFilterElem = &pFilterInfo[k].pFilters[j];

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

int32_t loadDataBlockOnDemand_rv(SQueryRuntimeEnv *pRuntimeEnv, STableScanInfo* pTableScanInfo, SSDataBlock *pBlock, uint32_t *status) {
  *status = BLK_DATA_NO_NEEDED;
  pBlock->pDataBlock   = NULL;
  pBlock->pBlockStatis = NULL;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  int64_t groupId = pQuery->current->groupIndex;

  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  SQueryCostInfo* pCost = &pQInfo->summary;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf > 0) {
    *status = BLK_DATA_ALL_NEEDED;
  } else { // check if this data block is required to load
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

        bool  masterScan = IS_MASTER_SCAN(pRuntimeEnv);
        TSKEY k = QUERY_IS_ASC_QUERY(pQuery)? pBlock->info.window.skey:pBlock->info.window.ekey;
        STimeWindow win = getActiveTimeWindow(pTableScanInfo->pResultRowInfo, k, pQuery);
        if (setWindowOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, &win, masterScan, &pResult,
            groupId, pTableScanInfo->pCtx, pTableScanInfo->numOfOutput, pTableScanInfo->rowCellInfoOffset) != TSDB_CODE_SUCCESS) {
          // todo handle error in set result for timewindow
        }
      }

      int32_t numOfOutput = pTableScanInfo->numOfOutput;
      SQLFunctionCtx* pCtx = pTableScanInfo->pCtx;

      for (int32_t i = 0; i < numOfOutput; ++i) {
        int32_t functionId = pCtx[i].functionId;
        int32_t colId = pTableScanInfo->pExpr[i].base.colInfo.colId;

        // group by + first/last should not apply the first/last block filter
        if (!pQuery->groupbyColumn && (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST)) {
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

  SDataBlockInfo* pBlockInfo = &pBlock->info;

  if ((*status) == BLK_DATA_NO_NEEDED) {
    qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", pQInfo, pBlockInfo->window.skey,
           pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->discardBlocks += 1;
  } else if ((*status) == BLK_DATA_STATIS_NEEDED) {

    // this function never returns error?
    pCost->loadBlockStatis += 1;
    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->pQueryHandle, &pBlock->pBlockStatis);

    if (pBlock->pBlockStatis == NULL) { // data block statistics does not exist, load data block
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
          load = topbot_datablock_filter(&pTableScanInfo->pCtx[i], (char *)&(pBlock->pBlockStatis[i].min), (char *)&(pBlock->pBlockStatis[i].max));
          if (!load) {
            // current block has been discard due to filter applied
            pCost->discardBlocks += 1;
            qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", pQInfo, pBlockInfo->window.skey,
                   pBlockInfo->window.ekey, pBlockInfo->rows);
            (*status) = BLK_DATA_DISCARD;
            return TSDB_CODE_SUCCESS;
          }
        }
      }
    }

    // current block has been discard due to filter applied
    if (!doFilterOnBlockStatistics(pRuntimeEnv, pBlock->pBlockStatis, pTableScanInfo->pCtx, pBlockInfo->rows)) {
      pCost->discardBlocks += 1;
      qDebug("QInfo:%p data block discard, brange:%"PRId64 "-%"PRId64", rows:%d", pQInfo, pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      (*status) = BLK_DATA_DISCARD;
      return TSDB_CODE_SUCCESS;
    }

    pCost->totalCheckedRows += pBlockInfo->rows;
    pCost->loadBlocks += 1;
    pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pQueryHandle, NULL);
    if (pBlock->pDataBlock == NULL) {
      return terrno;
    }

    if (pQuery->numOfFilterCols > 0) {
      // set the initial static data value filter expression
      if (pQuery->pFilterInfo[0].pData == NULL) {
        for(int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
          for(int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
            SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, j);

            if (pQuery->pFilterInfo[i].info.colId == pColInfo->info.colId) {
              pQuery->pFilterInfo[i].pData = pColInfo->pData;
              break;
            }
          }
        }
      }

      filterDataBlock_rv(pQuery->pFilterInfo, pQuery->numOfFilterCols, pBlock);
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



#if 0

static UNUSED_FUNC void doSetInitialTimewindow(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pBlockInfo) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  if (QUERY_IS_INTERVAL_QUERY(pQuery) && pRuntimeEnv->resultRowInfo.prevSKey == TSKEY_INITIAL_VAL) {
    STimeWindow w = TSWINDOW_INITIALIZER;
    SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;

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

  SQInfo* pQInfo = GET_QINFO_ADDR(pRuntimeEnv);
  SQueryCostInfo*  summary  = &pQInfo->summary;

  qDebug("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", lastkey:%" PRId64 ", order:%d",
         GET_QINFO_ADDR(pRuntimeEnv), pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, pTableQueryInfo->lastKey,
         pQuery->order.order);

  TsdbQueryHandleT pQueryHandle = IS_MASTER_SCAN(pRuntimeEnv)? pRuntimeEnv->pQueryHandle : pRuntimeEnv->pSecQueryHandle;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
  while (tsdbNextDataBlock(pQueryHandle)) {
    summary->totalBlocks += 1;

    if (IS_MASTER_SCAN(pRuntimeEnv)) {
      pQuery->numOfCheckedBlocks += 1;
    }

    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
    doSetInitialTimewindow(pRuntimeEnv, &blockInfo);

    // in case of prj/diff query, ensure the output buffer is sufficient to accommodate the results of current block
    ensureOutputBuffer(pRuntimeEnv, blockInfo.rows);

    SDataStatis *pStatis = NULL;
    SArray *     pDataBlock = NULL;
    uint32_t     status = 0;

    int32_t ret = loadDataBlockOnDemand(pRuntimeEnv, &pRuntimeEnv->resultRowInfo, pQueryHandle, &blockInfo, &pStatis, &pDataBlock, &status);
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
    closeAllResultRows(&pRuntimeEnv->resultRowInfo);
    pRuntimeEnv->resultRowInfo.curIndex = pRuntimeEnv->resultRowInfo.size - 1;  // point to the last time window
  }

  return 0;
}

#endif

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


void setTagVal_rv(SOperatorInfo* pOperatorInfo, void *pTable, SQLFunctionCtx* pCtx, int32_t numOfOutput) {
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

    // set the join tag for first column
    SSqlFuncMsg* pFuncMsg = &pExprInfo->base;
    if ((pFuncMsg->functionId == TSDB_FUNC_TS || pFuncMsg->functionId == TSDB_FUNC_PRJ) &&
        pRuntimeEnv->pTsBuf != NULL && pFuncMsg->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
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

void copyResToQueryResultBuf_rv(SQueryRuntimeEnv* pRuntimeEnv, int32_t threshold, SSDataBlock* pBlock, int32_t* offset) {
  SGroupResInfo* pGroupResInfo = &pRuntimeEnv->groupResInfo;

  int32_t code = TSDB_CODE_SUCCESS;
  while(pGroupResInfo->currentGroup < pGroupResInfo->totalGroup) {
    // all results in current group have been returned to client, try next group
    if ((pGroupResInfo->pRows == NULL) || taosArrayGetSize(pGroupResInfo->pRows) == 0) {
      assert(pGroupResInfo->index == 0);
      if ((code = mergeIntoGroupResult(&pRuntimeEnv->groupResInfo, pRuntimeEnv, offset)) != TSDB_CODE_SUCCESS) {
        return;
      }
    }

    doCopyToSData_rv(pRuntimeEnv, pGroupResInfo, TSDB_ORDER_ASC, pBlock);

    // current data are all dumped to result buffer, clear it
    if (!hasRemainData(pGroupResInfo)) {
      cleanupGroupResInfo(pGroupResInfo);
      if (!incNextGroup(pGroupResInfo)) {
        SET_STABLE_QUERY_OVER(pRuntimeEnv);
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

  // set the index at the end of time window
  pTableQueryInfo->resInfo.curIndex = pTableQueryInfo->resInfo.size - 1;
}

#if 0
static void disableFuncInReverseScanImpl(SQueryRuntimeEnv* pRuntimeEnv, SResultRowInfo *pWindowResInfo, int32_t order, int32_t numOfOutput) {
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
      SResultRowCellInfo* pInfo = getResultCell(pRow, numOfOutput, j);

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_ASC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_DESC)) {
        pInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pInfo->complete = true;
      }
    }
  }
}

#endif
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
#if 0
void resetDefaultResInfoOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t tid = 0;
  int64_t uid = 0;
  SResultRow* pRow = doPrepareResultRowFromKey(pRuntimeEnv, &pRuntimeEnv->resultRowInfo, (char *)&tid, sizeof(tid), true, uid);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->pOutput = pQuery->sdata[i]->data;

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
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].pOutput;
    }

    memset(pQuery->sdata[i]->data, 0, (size_t)(pQuery->pExpr1[i].bytes * pQuery->rec.capacity));
  }

  initCtxOutputBuf(pRuntimeEnv, pRuntimeEnv->pCtx);
}

#endif

int32_t initResultRow(SResultRow *pResultRow) {
  pResultRow->pCellInfo = (SResultRowCellInfo*)((char*)pResultRow + sizeof(SResultRow));
  pResultRow->pageId    = -1;
  pResultRow->offset    = -1;
  return TSDB_CODE_SUCCESS;
}

void setDefaultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx, SResultRowInfo* pResultRowInfo,
    SSDataBlock* pDataBlock, int32_t* rowCellInfoOffset) {
  int32_t tid = 0;
  int64_t uid = 0;
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

  initCtxOutputBuf_rv(pCtx, pDataBlock->info.numOfCols);
}

void updateOutputBuf(SArithOperatorInfo* pInfo, int32_t numOfInputRows) {
  SSDataBlock* pDataBlock = pInfo->binfo.pRes;

  if (pInfo->bufCapacity < pDataBlock->info.rows + numOfInputRows) {
    int32_t newSize = pDataBlock->info.rows + numOfInputRows;

    for(int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
      SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
      char* p = realloc(pColInfo->pData, newSize * pColInfo->info.bytes);
      if (p != NULL) {
        pColInfo->pData = p;
      } else {
        // longjmp
      }
    }
  }

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
    pInfo->binfo.pCtx[i].pOutput = pColInfo->pData + pColInfo->info.bytes * pDataBlock->info.rows;
  }
}

#if 0
void forwardCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, int64_t output) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // reset the execution contexts
  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pExpr1[j].base.functionId;
    assert(functionId != TSDB_FUNC_DIFF);

    // set next output position
    if (IS_OUTER_FORWARD(aAggs[functionId].status)) {
      pRuntimeEnv->pCtx[j].pOutput += pRuntimeEnv->pCtx[j].outputBytes * output;
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

void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    pCtx[j].currentStage = 0;

    SResultRowCellInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (pResInfo->initialized) {
      continue;
    }

    aAggs[pCtx[j].functionId].init(&pCtx[j]);
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
      pRuntimeEnv->pCtx[i].pOutput = ((char*) pQuery->sdata[i]->data) + pQuery->rec.rows * bytes;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf = pRuntimeEnv->pCtx[0].pOutput;
      }
    }

    updateNumOfResult(pRuntimeEnv, (int32_t)pQuery->rec.rows);
  }
}

void prepareRepeatTableScan(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pQuery->groupbyColumn || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    // for each group result, call the finalize function for each column
    SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SResultRow *pResult = getResultRow(pWindowResInfo, i);

      setResultOutputBuf(pRuntimeEnv, pResult);
      for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        SQLFunctionCtx* pCtx = &pRuntimeEnv->pCtx[j];
        if (pCtx->functionId == TSDB_FUNC_TS) { // ignore more table
          continue;
        }

        aAggs[pCtx->functionId].xNextStep(pCtx);
      }
    }
  } else {
    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      SQLFunctionCtx* pCtx = &pRuntimeEnv->pCtx[j];
      if (pCtx->functionId == TSDB_FUNC_TS) {
        continue;
      }

      aAggs[pCtx->functionId].xNextStep(pCtx);
    }
  }
}


#endif

void initCtxOutputBuf_rv(SQLFunctionCtx* pCtx, int32_t size) {
  for (int32_t j = 0; j < size; ++j) {
    pCtx[j].currentStage = 0;

    SResultRowCellInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (pResInfo->initialized) {
      continue;
    }

    aAggs[pCtx[j].functionId].init(&pCtx[j]);
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


#if 0

bool needRepeatScan(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  bool toContinue = false;
  if (pQuery->groupbyColumn || QUERY_IS_INTERVAL_QUERY(pQuery)) {
    // for each group result, call the finalize function for each column
    SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;

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

static UNUSED_FUNC SQueryStatusInfo getQueryStatusInfo(SQueryRuntimeEnv *pRuntimeEnv, TSKEY start) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;

  assert((start <= pTableQueryInfo->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (start >= pTableQueryInfo->lastKey && !QUERY_IS_ASC_QUERY(pQuery)));

  SQueryStatusInfo info = {
      .status      = pQuery->status,
      .windowIndex = pRuntimeEnv->resultRowInfo.curIndex,
      .lastKey     = start,
  };

  TIME_WINDOW_COPY(info.w, pQuery->window);
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
  disableFuncInReverseScan(pRuntimeEnv);
  setupQueryRangeForReverseScan(pRuntimeEnv);

  // clean unused handle
  if (pRuntimeEnv->pSecQueryHandle != NULL) {
    tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
  }

  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQuery->tsdb, &cond, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);
  if (pRuntimeEnv->pSecQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }
}



void disableFuncInReverseScan(SQueryRuntimeEnv* pRuntimeEnv, SResultRowInfo *pWindowResInfo, SQLFunctionCtx* pCtx,
    int32_t numOfOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  // group by normal columns and interval query on normal table
  if (pQuery->groupbyColumn || QUERY_IS_INTERVAL_QUERY(pQuery)) {
//    disableFuncInReverseScanImpl(pRuntimeEnv, pWindowResInfo, order, numOfOutput);
  } else {  // for simple result of table query,
    for (int32_t j = 0; j < numOfOutput; ++j) {  // todo refactor
      int32_t functId = pCtx[j].functionId;
      if (pCtx[j].resultInfo == NULL) {
        continue; // resultInfo is NULL, means no data checked in previous scan
      }

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSDB_ORDER_ASC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSDB_ORDER_DESC)) {
        pCtx[j].resultInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pCtx[j].resultInfo->complete = true;
      }
    }
  }
}
#endif

static void setEnvBeforeReverseScan_rv(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, SQLFunctionCtx* pCtx, int32_t numOfOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (pRuntimeEnv->pTsBuf) {
    SWITCH_ORDER(pRuntimeEnv->pTsBuf->cur.order);
    bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
    assert(ret);
  }

  // reverse order time range
  SWAP(pQuery->window.skey, pQuery->window.ekey, TSKEY);
  SWITCH_ORDER(pQuery->order.order);

  SET_REVERSE_SCAN_FLAG(pRuntimeEnv);
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  switchCtxOrder(pCtx, numOfOutput);

//  disableFuncInReverseScan(pRuntimeEnv, pResultRowInfo, pCtx, numOfOutput);
  setupQueryRangeForReverseScan(pRuntimeEnv);
}

#if 0

static UNUSED_FUNC void clearEnvAfterReverseScan(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatusInfo *pStatus) {
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

static UNUSED_FUNC void restoreTimeWindow(STableGroupInfo* pTableGroupInfo, STsdbQueryCond* pCond) {
  assert(pTableGroupInfo->numOfTables == 1);
  SArray* pTableKeyGroup = taosArrayGetP(pTableGroupInfo->pGroupList, 0);
  STableKeyInfo* pKeyInfo = taosArrayGet(pTableKeyGroup, 0);
  pKeyInfo->lastKey = pCond->twindow.skey;
}

static UNUSED_FUNC void handleInterpolationQuery(SQInfo* pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pQuery->numOfCheckedBlocks > 0 || !isPointInterpoQuery(pQuery)) {
    return;
  }

  SArray *prev = tsdbGetExternalRow(pRuntimeEnv->pQueryHandle, &pQuery->memRef, TSDB_PREV_ROW);
  SArray *next = tsdbGetExternalRow(pRuntimeEnv->pQueryHandle, &pQuery->memRef, TSDB_NEXT_ROW);
  if (prev == NULL || next == NULL) {
    return;
  }

  // setup the pCtx->start/end info and calculate the interpolation value
  SColumnInfoData *startTs = taosArrayGet(prev, 0);
  SColumnInfoData *endTs = taosArrayGet(next, 0);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

    int32_t    functionId = pQuery->pExpr1[i].base.functionId;
    SColIndex *pColIndex = &pQuery->pExpr1[i].base.colInfo;

    if (!TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
      aAggs[functionId].xFunction(pCtx);
      continue;
    }

    SColumnInfoData *p = taosArrayGet(prev, pColIndex->colIndex);
    SColumnInfoData *n = taosArrayGet(next, pColIndex->colIndex);

    assert(p->info.colId == pColIndex->colId);

    pCtx->start.key = *(TSKEY *)startTs->pData;
    pCtx->end.key = *(TSKEY *)endTs->pData;

    if (p->info.type != TSDB_DATA_TYPE_BINARY && p->info.type != TSDB_DATA_TYPE_NCHAR) {
      GET_TYPED_DATA(pCtx->start.val, double, p->info.type, p->pData);
      GET_TYPED_DATA(pCtx->end.val, double, n->info.type, n->pData);
    } else {  // string pointer
      pCtx->start.ptr = p->pData;
      pCtx->end.ptr = n->pData;
    }

    pCtx->param[2].i64 = (int8_t)pQuery->fillType;
    pCtx->startTs = pQuery->window.skey;
    if (pQuery->fillVal != NULL) {
      if (isNull((const char *)&pQuery->fillVal[i], pCtx->inputType)) {
        pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
      } else {  // todo refactor, tVariantCreateFromBinary should handle the NULL value
        if (pCtx->inputType != TSDB_DATA_TYPE_BINARY && pCtx->inputType != TSDB_DATA_TYPE_NCHAR) {
          tVariantCreateFromBinary(&pCtx->param[1], (char *)&pQuery->fillVal[i], pCtx->inputBytes, pCtx->inputType);
        }
      }
    }

    aAggs[functionId].xFunction(pCtx);
  }
}


void scanOneTableDataBlocks(SQueryRuntimeEnv *pRuntimeEnv, TSKEY start) {
  SQInfo *pQInfo = (SQInfo *) GET_QINFO_ADDR(pRuntimeEnv);
  SQuery *pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo *pTableQueryInfo = pQuery->current;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  // store the start query position
  SQueryStatusInfo qstatus = getQueryStatusInfo(pRuntimeEnv, start);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);

  if (!pQuery->groupbyColumn && pQuery->hasTagResults) {
    setTagVal(pRuntimeEnv, pTableQueryInfo->pTable);
  }

  while (1) {
//    doScanAllDataBlocks(pRuntimeEnv);

    if (pRuntimeEnv->scanFlag == MASTER_SCAN) {
      qstatus.status = pQuery->status;

      // do nothing if no data blocks are found qualified during scan
      if (qstatus.lastKey == pTableQueryInfo->lastKey) {
        qDebug("QInfo:%p no results generated in this scan", pQInfo);
      }
    }

    if (!needRepeatScan(pRuntimeEnv)) {
      // restore the status code and jump out of loop
      if (pRuntimeEnv->scanFlag == REPEAT_SCAN) {
        pQuery->status = qstatus.status;
      }

      break;
    }

    if (pRuntimeEnv->pSecQueryHandle != NULL) {
      tsdbCleanupQueryHandle(pRuntimeEnv->pSecQueryHandle);
    }

    STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);
    restoreTimeWindow(&pQuery->tableGroupInfo, &cond);
    pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQuery->tsdb, &cond, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);
    if (pRuntimeEnv->pSecQueryHandle == NULL) {
      longjmp(pRuntimeEnv->env, terrno);
    }

    pRuntimeEnv->resultRowInfo.curIndex = qstatus.windowIndex;
    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    if (pRuntimeEnv->pTsBuf) {
      bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
      assert(ret);
    }

    qDebug("QInfo:%p start to repeat scan data blocks due to query func required, qrange:%"PRId64"-%"PRId64, pQInfo,
        cond.twindow.skey, cond.twindow.ekey);
  }

  if (needReverseScan(pQuery)) {
    setEnvBeforeReverseScan(pRuntimeEnv, &qstatus);

    // reverse scan from current position
    qDebug("QInfo:%p start to reverse scan", pQInfo);
//    doScanAllDataBlocks(pRuntimeEnv);

    clearEnvAfterReverseScan(pRuntimeEnv, &qstatus);
  }

  handleInterpolationQuery(pQInfo);
}

#endif

void finalizeQueryResult_rv(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset) {
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

      setResultOutputBuf_rv(pRuntimeEnv, buf, pCtx, numOfOutput, rowCellInfoOffset);

      for (int32_t j = 0; j < numOfOutput; ++j) {
        aAggs[pCtx[j].functionId].xFinalize(&pCtx[j]);
      }

      /*
       * set the number of output results for group by normal columns, the number of output rows usually is 1 except
       * the top and bottom query
       */
      buf->numOfRows = (uint16_t)getNumOfResult_rv(pRuntimeEnv, pCtx, numOfOutput);
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

#if 0
/**
 * set output buffer for different group
 * @param pRuntimeEnv
 * @param pDataBlockInfo
 */
void setExecutionContext(SQueryRuntimeEnv *pRuntimeEnv, int32_t groupIndex, TSKEY nextKey) {
  STableQueryInfo  *pTableQueryInfo = pRuntimeEnv->pQuery->current;
  SResultRowInfo   *pWindowResInfo  = &pRuntimeEnv->resultRowInfo;

  // lastKey needs to be updated
  pTableQueryInfo->lastKey = nextKey;
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
  initCtxOutputBuf(pRuntimeEnv, pRuntimeEnv->pCtx);
}

void setResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->pOutput = getPosInResultPage(pRuntimeEnv, i, pResult, page);

    int32_t functionId = pQuery->pExpr1[i].base.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].pOutput;
    }

    /*
     * set the output buffer information and intermediate buffer,
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx->resultInfo = getResultCell(pRuntimeEnv, pResult, i);
  }
}
#endif

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


void setExecutionContext_rv(SQueryRuntimeEnv *pRuntimeEnv, SAggOperatorInfo *pInfo, int32_t numOfOutput,
    int32_t groupIndex, TSKEY nextKey) {
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
  setResultOutputBuf_rv(pRuntimeEnv, pResultRow, pInfo->pCtx, numOfOutput, pInfo->rowCellInfoOffset);
  initCtxOutputBuf_rv(pInfo->pCtx, numOfOutput);
}

void setResultOutputBuf_rv(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SQLFunctionCtx* pCtx,
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

//int32_t setParamValue(SQueryRuntimeEnv* pRuntimeEnv) {
//  SQuery* pQuery = pRuntimeEnv->pQuery;
//
//  if (pRuntimeEnv->prevResult == NULL || pQuery->groupbyColumn) {
//    return TSDB_CODE_SUCCESS;
//  }
//
//  int32_t numOfExprs = pQuery->numOfOutput;
//  for(int32_t i = 0; i < numOfExprs; ++i) {
//    SExprInfo* pExprInfo = &(pQuery->pExpr1[i]);
//    if(pExprInfo->base.functionId != TSDB_FUNC_STDDEV_DST) {
//      continue;
//    }
//
//    SSqlFuncMsg* pFuncMsg = &pExprInfo->base;
//
//    pRuntimeEnv->pCtx[i].param[0].arr = NULL;
//    pRuntimeEnv->pCtx[i].param[0].nType = TSDB_DATA_TYPE_INT; // avoid freeing the memory by setting the type to be int
//
//    int32_t numOfGroup = (int32_t) taosArrayGetSize(pRuntimeEnv->prevResult);
//    for(int32_t j = 0; j < numOfGroup; ++j) {
//      SInterResult *p = taosArrayGet(pRuntimeEnv->prevResult, j);
//      if (pQuery->tagLen == 0 || memcmp(p->tags, pRuntimeEnv->tagVal, pQuery->tagLen) == 0) {
//
//        int32_t numOfCols = (int32_t) taosArrayGetSize(p->pResult);
//        for(int32_t k = 0; k < numOfCols; ++k) {
//          SStddevInterResult* pres = taosArrayGet(p->pResult, k);
//          if (pres->colId == pFuncMsg->colInfo.colId) {
//            pRuntimeEnv->pCtx[i].param[0].arr = pres->pResult;
//            break;
//          }
//        }
//      }
//    }
//  }
//
//  return 0;
//}

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

//static int32_t doCopyToSData(SQueryRuntimeEnv* pRuntimeEnv, SGroupResInfo* pGroupResInfo, int32_t orderType) {
//  void* qinfo = GET_QINFO_ADDR(pRuntimeEnv);
//  SQuery *pQuery = pRuntimeEnv->pQuery;
//
//  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
//  int32_t numOfResult = pQuery->rec.rows; // there are already exists result rows
//
//  int32_t start = 0;
//  int32_t step = -1;
//
//  qDebug("QInfo:%p start to copy data from windowResInfo to output buf", qinfo);
//  if (orderType == TSDB_ORDER_ASC) {
//    start = pGroupResInfo->index;
//    step = 1;
//  } else {  // desc order copy all data
//    start = numOfRows - pGroupResInfo->index - 1;
//    step = -1;
//  }
//
//  for (int32_t i = start; (i < numOfRows) && (i >= 0); i += step) {
//    SResultRow* pRow = taosArrayGetP(pGroupResInfo->pRows, i);
//    if (pRow->numOfRows == 0) {
//      pGroupResInfo->index += 1;
//      continue;
//    }
//
//    int32_t numOfRowsToCopy = pRow->numOfRows;
//
//    //current output space is not enough to accommodate all data of this page, prepare more space
//    if (numOfRowsToCopy > (pQuery->rec.capacity - numOfResult)) {
//      int32_t newSize = pQuery->rec.capacity + (numOfRowsToCopy - numOfResult);
//      expandBuffer(pRuntimeEnv, newSize, GET_QINFO_ADDR(pRuntimeEnv));
//    }
//
//    pGroupResInfo->index += 1;
//
//    tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pRow->pageId);
//    for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
//      int32_t size = pRuntimeEnv->pCtx[j].outputBytes;
//
//      char *out = pQuery->sdata[j]->data + numOfResult * size;
//      char *in  = getPosInResultPage(pRuntimeEnv, j, pRow, page);
//      memcpy(out, in, size * numOfRowsToCopy);
//    }
//
//    numOfResult += numOfRowsToCopy;
//    if (numOfResult == pQuery->rec.capacity) {  // output buffer is full
//      break;
//    }
//  }
//
//  qDebug("QInfo:%p copy data to query buf completed", qinfo);
//  return numOfResult;
//}

/**
 * copyToOutputBuf support copy data in ascending/descending order
 * For interval query of both super table and table, copy the data in ascending order, since the output results are
 * ordered in SWindowResutl already. While handling the group by query for both table and super table,
 * all group result are completed already.
 *
 * @param pQInfo
 * @param result
 */

static int32_t doCopyToSData_rv(SQueryRuntimeEnv* pRuntimeEnv, SGroupResInfo* pGroupResInfo, int32_t orderType, SSDataBlock* pBlock) {
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

    //current output space is not enough to accommodate all data of this page, prepare more space
//    if (numOfRowsToCopy > (pQuery->rec.capacity - numOfResult)) {
//      int32_t newSize = pQuery->rec.capacity + (numOfRowsToCopy - numOfResult);
//      expandBuffer(pRuntimeEnv, newSize, GET_QINFO_ADDR(pRuntimeEnv));
//    }

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
    if (numOfResult == pQuery->rec.capacity) {  // output buffer is full
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
  if (!hasRemainData(pGroupResInfo)) {
    return;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSDB_ORDER_ASC;
  doCopyToSData_rv(pRuntimeEnv, pGroupResInfo, orderType, pBlock);

  SColumnInfoData* pInfoData = taosArrayGet(pBlock->pDataBlock, 0);

  if (pInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    STimeWindow* w = &pBlock->info.window;
    w->skey = *(int64_t*)pInfoData->pData;
    w->ekey = *(int64_t*)(pInfoData->pData + TSDB_KEYSIZE * (pBlock->info.rows - 1));
  }
}

static void updateWindowResNumOfRes_rv(SQueryRuntimeEnv *pRuntimeEnv,
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

static UNUSED_FUNC void stableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo, SDataStatis *pStatis,
    SArray *pDataBlock, __block_search_fn_t searchFn) {
  SQuery *         pQuery = pRuntimeEnv->pQuery;
  STableQueryInfo* pTableQueryInfo = pQuery->current;

  SResultRowInfo * pResultRowInfo = &pTableQueryInfo->resInfo;
  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery)? 0 : pDataBlockInfo->rows - 1;

//  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pQuery->groupbyColumn) {
//    rowwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, pDataBlock);
//  } else {
//    blockwiseApplyFunctions(pRuntimeEnv, pStatis, pDataBlockInfo, pResultRowInfo, searchFn, pDataBlock);
//  }

  if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
    updateResultRowIndex(pResultRowInfo, pTableQueryInfo, QUERY_IS_ASC_QUERY(pQuery), pQuery->timeWindowInterpo);
  }
}

bool hasNotReturnedResults(SQueryRuntimeEnv* pRuntimeEnv, SGroupResInfo* pGroupResInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SFillInfo *pFillInfo = pRuntimeEnv->pFillInfo;

  if (!Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
      return false;
  }

  if (pQuery->limit.limit > 0 && pQuery->rec.total >= pQuery->limit.limit) {
    return false;
  }

  if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
    // There are results not returned to client yet, so filling applied to the remain result is required firstly.
    if (taosFillHasMoreResults(pFillInfo)) {
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
    int32_t numOfTotal = (int32_t)getNumOfResultsAfterFillGap(pFillInfo, pQuery->window.ekey, (int32_t)pQuery->rec.capacity);
    return numOfTotal > 0;
  } else { // there are results waiting for returned to client.
    if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED) && hasRemainData(pGroupResInfo) &&
        (pQuery->groupbyColumn || QUERY_IS_INTERVAL_QUERY(pQuery))) {
      return true;
    }
  }

  return false;
}

static int16_t getNumOfFinalResCol(SQuery* pQuery) {
  return pQuery->pExpr2 == NULL? pQuery->numOfOutput:pQuery->numOfExpr2;
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
  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    if (pQInfo->query.stableQuery) {
      if (IS_STASBLE_QUERY_OVER(&pQInfo->runtimeEnv)) {
        setQueryStatus(pQuery, QUERY_OVER);
      }
    } else {
      if (!hasNotReturnedResults(&pQInfo->runtimeEnv, &pRuntimeEnv->groupResInfo)) {
        setQueryStatus(pQuery, QUERY_OVER);
      }
    }
  }
}

#if 0
int32_t doFillGapsInResults(SQueryRuntimeEnv* pRuntimeEnv, tFilePage **pDst) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SFillInfo* pFillInfo = pRuntimeEnv->pFillInfo;

  while (1) {
    int32_t ret = (int32_t)taosFillResultDataBlock(pFillInfo, (tFilePage**)pQuery->sdata, (int32_t)pQuery->rec.capacity);

    // todo apply limit output function
    /* reached the start position of according to offset value, return immediately */
    if (pQuery->limit.offset == 0) {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows", pRuntimeEnv->qinfo, pFillInfo->numOfRows, ret);
      return ret;
    }

    if (pQuery->limit.offset < ret) {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows, offset:%" PRId64 ". Discard due to offset, remain:%" PRId64 ", new offset:%d",
             pRuntimeEnv->qinfo, pFillInfo->numOfRows, ret, pQuery->limit.offset, ret - pQuery->limit.offset, 0);

      ret -= (int32_t)pQuery->limit.offset;
      for (int32_t i = 0; i < pQuery->numOfOutput; ++i) { //???pExpr1 or pExpr2
        memmove(pDst[i]->data, pDst[i]->data + pQuery->pExpr1[i].bytes * pQuery->limit.offset,
                ret * pQuery->pExpr1[i].bytes);
      }

      pQuery->limit.offset = 0;
      return ret;
    } else {
      qDebug("QInfo:%p initial numOfRows:%d, generate filled result:%d rows, offset:%" PRId64 ". Discard due to offset, "
             "remain:%d, new offset:%" PRId64, pRuntimeEnv->qinfo, pFillInfo->numOfRows, ret, pQuery->limit.offset, 0,
          pQuery->limit.offset - ret);

      pQuery->limit.offset -= ret;
      ret = 0;
    }

    // no data in current data after fill
    int32_t numOfTotal = (int32_t)getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, (int32_t)pQuery->rec.capacity);
    if (numOfTotal == 0) {
      return 0;
    }
  }
}
#endif

int32_t doFillGapsInResults_rv(SQueryRuntimeEnv* pRuntimeEnv, SSDataBlock *pOutput) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SFillInfo* pFillInfo = pRuntimeEnv->pFillInfo;

  void** p = calloc(pFillInfo->numOfCols, POINTER_BYTES);
  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pOutput->pDataBlock, i);
    p[i] = pColInfoData->pData;
  }

  pOutput->info.rows = (int32_t)taosFillResultDataBlock(pFillInfo, p, (int32_t)pQuery->rec.capacity);
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
//  qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%"PRId64, GET_QINFO_ADDR(pRuntimeEnv),
//         pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes, pQuery->current->lastKey);
//}

static void freeTableBlockDist(STableBlockDist *pTableBlockDist) {
  if (pTableBlockDist != NULL) {
    taosArrayDestroy(pTableBlockDist->dataBlockInfos); 
    free(pTableBlockDist->result);
    free(pTableBlockDist);
  }
}

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

static void generateBlockDistResult(STableBlockDist *pTableBlockDist) {
  if (pTableBlockDist == NULL) {
     return;
  }
  int64_t min = INT64_MAX, max = INT64_MIN, avg = 0;    
  SArray* blockInfos= pTableBlockDist->dataBlockInfos;  
  int64_t totalRows = 0, totalBlocks = taosArrayGetSize(blockInfos); 
  for (size_t i = 0; i < taosArrayGetSize(blockInfos); i++) {
    SDataBlockInfo *blockInfo = taosArrayGet(blockInfos, i); 
    int64_t rows = blockInfo->rows;
    min = MIN(min, rows);       
    max = MAX(max, rows);
    totalRows += rows;  
  }
  avg = totalBlocks > 0 ? (int64_t)(totalRows/totalBlocks) : 0;

  taosArraySort(blockInfos, compareBlockInfo);

  int sz = sprintf(pTableBlockDist->result, 
          "summery: \n\t 5th=[%d], 25th=[%d], 50th=[%d],75th=[%d], 95th=[%d], 99th=[%d] \n\t min=[%"PRId64"], max=[%"PRId64"], avg = [%"PRId64"] \n\t totalRows=[%"PRId64"], totalBlocks=[%"PRId64"] \n\t seekHeaderTimeCost=[%"PRId64"(us)] \n\t rowsInMem=[%"PRId64"]",  
          getPercentileFromSortedArray(blockInfos, 0.05), getPercentileFromSortedArray(blockInfos, 0.25), getPercentileFromSortedArray(blockInfos, 0.50), 
          getPercentileFromSortedArray(blockInfos, 0.75), getPercentileFromSortedArray(blockInfos, 0.95), getPercentileFromSortedArray(blockInfos, 0.99),
          min, max, avg,
          totalRows, totalBlocks,
          pTableBlockDist->firstSeekTimeUs,
          pTableBlockDist->numOfRowsInMemTable);
  UNUSED(sz);
  return;
} 
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
//    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
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
//      qDebug("QInfo:%p skip rows:%d, offset:%" PRId64, GET_QINFO_ADDR(pRuntimeEnv), blockInfo.rows,
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
//           GET_QINFO_ADDR(pRuntimeEnv), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes,
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

static int32_t setupQueryHandle(void* tsdb, SQInfo* pQInfo, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  if (onlyQueryTags(pQuery)) {
    return TSDB_CODE_SUCCESS;
  }

//  if (isSTableQuery && (!QUERY_IS_INTERVAL_QUERY(pQuery)) && (!isFixedOutputQuery(pQuery))) {
//    return TSDB_CODE_SUCCESS;
//  }

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

int32_t doInitQInfo(SQInfo *pQInfo, STSBuf *pTsBuf, SArray* prevResult, void *tsdb, int32_t vgId, bool isSTableQuery) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  pQuery->topBotQuery = isTopBottomQuery(pQuery);
  pQuery->hasTagResults = hasTagValOutput(pQuery);
  pQuery->timeWindowInterpo = timeWindowInterpoRequired(pQuery);
  pRuntimeEnv->prevResult = prevResult;

  setScanLimitationByResultBuffer(pQuery);

  int32_t code = setupQueryHandle(tsdb, pQInfo, isSTableQuery);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pQuery->tsdb = tsdb;
  pQuery->vgId = vgId;
  pRuntimeEnv->groupResInfo.totalGroup = isSTableQuery? GET_NUM_OF_TABLEGROUP(pRuntimeEnv):0;

  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pTsBuf = pTsBuf;
  pRuntimeEnv->cur.vgroupIndex = -1;

  pQuery->stableQuery = isSTableQuery;

  pQuery->groupbyColumn = isGroupbyColumn(pQuery->pGroupbyExpr);

  if (onlyQueryTags(pQuery)) {
    pRuntimeEnv->proot = createTagScanOperatorInfo(pRuntimeEnv);
  } else if (needReverseScan(pQuery)) {
    pRuntimeEnv->pi = createBiDirectionTableScanInfo(pRuntimeEnv->pQueryHandle, pRuntimeEnv, getNumOfScanTimes(pQuery), 1);
  } else {
    pRuntimeEnv->pi = createTableScanOperator(pRuntimeEnv->pQueryHandle, pRuntimeEnv, getNumOfScanTimes(pQuery));
  }

  if (pTsBuf != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTsBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTsBuf, order);
  }

  int32_t ps = DEFAULT_PAGE_SIZE;
  int32_t rowsize = 0;
  getIntermediateBufInfo(pRuntimeEnv, &ps, &rowsize);
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
  code = setupQueryRuntimeEnv(pRuntimeEnv, (int32_t) pQuery->tableGroupInfo.numOfTables, pQuery->order.order, pQuery->vgId);
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
    pRuntimeEnv->pFillInfo = taosCreateFillInfo(pQuery->order.order, w.skey, 0, (int32_t)pQuery->rec.capacity, numOfCols,
                                              pQuery->interval.sliding, pQuery->interval.slidingUnit, (int8_t)pQuery->precision,
                                              pQuery->fillType, pColInfo, pQInfo);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
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

#if 0
static int64_t scanMultiTableDataBlocks(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery*           pQuery = pRuntimeEnv->pQuery;
  SQueryCostInfo*   summary = &pQInfo->summary;

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
    STableQueryInfo **pTableQueryInfo = (STableQueryInfo**) taosHashGet(pRuntimeEnv->tableqinfoGroupInfo.map, &blockInfo.tid, sizeof(blockInfo.tid));
    if(pTableQueryInfo == NULL) {
      break;
    }

    pQuery->current = *pTableQueryInfo;
    doTableQueryInfoTimeWindowCheck(pQuery, *pTableQueryInfo);

    if (!pQuery->groupbyColumn) {
      setEnvForEachBlock(pRuntimeEnv, *pTableQueryInfo, &blockInfo);
    }

    if (pQuery->stabledev) {
      for(int32_t i = 0; i < pQuery->numOfOutput; ++i) {
        if (pQuery->pExpr1[i].base.functionId == TSDB_FUNC_STDDEV_DST) {
          setParamValue(pRuntimeEnv);
          break;
        }
      }
    }

    uint32_t     status = 0;
    SDataStatis *pStatis = NULL;
    SArray      *pDataBlock = NULL;

    int32_t ret = loadDataBlockOnDemand(pRuntimeEnv, &pQuery->current->resInfo, pQueryHandle, &blockInfo, &pStatis, &pDataBlock, &status);
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



static UNUSED_FUNC  bool multiTableMultioutputHelper(SQInfo *pQInfo, int32_t index) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  SArray *group = GET_TABLEGROUP(pRuntimeEnv, 0);
  STableQueryInfo* pCheckInfo = taosArrayGetP(group, index);

  if (pQuery->hasTagResults || pRuntimeEnv->pTsBuf != NULL) {
    setTagVal(pRuntimeEnv, pCheckInfo->pTable);
  }

  STableId* id = TSDB_TABLEID(pCheckInfo->pTable);
  qDebug("QInfo:%p query on (%d): uid:%" PRIu64 ", tid:%d, qrange:%" PRId64 "-%" PRId64, pQInfo, index,
         id->uid, id->tid, pCheckInfo->lastKey, pCheckInfo->win.ekey);

  STsdbQueryCond cond = {
      .twindow   = {pCheckInfo->lastKey, pCheckInfo->win.ekey},
      .order     = pQuery->order.order,
      .colList   = pQuery->colList,
      .numOfCols = pQuery->numOfCols,
      .loadExternalRows = false,
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

  pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQuery->tsdb, &cond, &gp, pQInfo, &pQuery->memRef);
  taosArrayDestroy(tx);
  taosArrayDestroy(g1);
  if (pRuntimeEnv->pQueryHandle == NULL) {
    longjmp(pRuntimeEnv->env, terrno);
  }

  if (pRuntimeEnv->pTsBuf != NULL) {
      tVariant* pTag = &pRuntimeEnv->pCtx[0].tag;

    if (pRuntimeEnv->cur.vgroupIndex == -1) {
      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQuery->vgId, pTag);
      // failed to find data with the specified tag value and vnodeId
      if (!tsBufIsValidElem(&elem)) {
        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qError("QInfo:%p failed to find tag:%s in ts_comp", pQInfo, pTag->pz);
        } else {
          qError("QInfo:%p failed to find tag:%"PRId64" in ts_comp", pQInfo, pTag->i64);
        }

        return false;
      } else {
        STSCursor cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);

        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz,
                 cur.blockIndex, cur.tsIndex);
        } else {
          qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64,
                 cur.blockIndex, cur.tsIndex);
        }
      }
    } else {
      STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);
      if (tVariantCompare(elem.tag, &pRuntimeEnv->pCtx[0].tag) != 0) {

        STSElem elem1 = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQuery->vgId, pTag);
        // failed to find data with the specified tag value and vnodeId
        if (!tsBufIsValidElem(&elem1)) {
          if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
            qError("QInfo:%p failed to find tag:%s in ts_comp", pQInfo, pTag->pz);
          } else {
            qError("QInfo:%p failed to find tag:%"PRId64" in ts_comp", pQInfo, pTag->i64);
          }

          return false;
        } else {
          STSCursor cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
          if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
            qDebug("QInfo:%p find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz, cur.blockIndex, cur.tsIndex);
          } else {
            qDebug("QInfo:%p find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64, cur.blockIndex, cur.tsIndex);
          }
        }

      } else {
        tsBufSetCursor(pRuntimeEnv->pTsBuf, &pRuntimeEnv->cur);
        STSCursor cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
        if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
          qDebug("QInfo:%p continue scan ts_comp file, tag:%s blockIndex:%d, tsIndex:%d", pQInfo, pTag->pz, cur.blockIndex, cur.tsIndex);
        } else {
          qDebug("QInfo:%p continue scan ts_comp file, tag:%"PRId64" blockIndex:%d, tsIndex:%d", pQInfo, pTag->i64, cur.blockIndex, cur.tsIndex);
        }
      }
    }
  }

  initCtxOutputBuf(pRuntimeEnv, pRuntimeEnv->pCtx);
  return true;
}
#endif
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

#if 0
/**
 * super table query handler
 * 1. super table projection query, group-by on normal columns query, ts-comp query
 * 2. point interpolation query, last row query
 *
 * @param pQInfo
 */
static UNUSED_FUNC void sequentialTableProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_COMPLETED);

  size_t numOfGroups = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);

  if (isPointInterpoQuery(pQuery)) {
    resetDefaultResInfoOutputBuf(pRuntimeEnv);
    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pRuntimeEnv->groupIndex < numOfGroups) {
      SArray *group = taosArrayGetP(pQuery->tableGroupInfo.pGroupList, pRuntimeEnv->groupIndex);

      qDebug("QInfo:%p point interpolation query on group:%d, total group:%" PRIzu ", current group:%p", pQInfo,
             pRuntimeEnv->groupIndex, numOfGroups, group);
      STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

      SArray *g1 = taosArrayInit(1, POINTER_BYTES);
      SArray *tx = taosArrayDup(group);
      taosArrayPush(g1, &tx);

      STableGroupInfo gp = {.numOfTables = taosArrayGetSize(tx), .pGroupList = g1};

      // include only current table
      if (pRuntimeEnv->pQueryHandle != NULL) {
        tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
        pRuntimeEnv->pQueryHandle = NULL;
      }

      pRuntimeEnv->pQueryHandle = tsdbQueryRowsInExternalWindow(pQuery->tsdb, &cond, &gp, pQInfo, &pQuery->memRef);

      taosArrayDestroy(tx);
      taosArrayDestroy(g1);
      if (pRuntimeEnv->pQueryHandle == NULL) {
        longjmp(pRuntimeEnv->env, terrno);
      }

      initCtxOutputBuf(pRuntimeEnv, pRuntimeEnv->pCtx);

      SArray *s = tsdbGetQueriedTableList(pRuntimeEnv->pQueryHandle);
      assert(taosArrayGetSize(s) >= 1);

      setTagVal(pRuntimeEnv, taosArrayGetP(s, 0));
      taosArrayDestroy(s);

      // here we simply set the first table as current table
      SArray *first = GET_TABLEGROUP(pRuntimeEnv, pRuntimeEnv->groupIndex);
      pQuery->current = taosArrayGetP(first, 0);

      scanOneTableDataBlocks(pRuntimeEnv, pQuery->current->lastKey);

      int64_t numOfRes = getNumOfResult(pRuntimeEnv);
      if (numOfRes > 0) {
        pQuery->rec.rows += numOfRes;
        forwardCtxOutputBuf(pRuntimeEnv, numOfRes);
      }

      skipResults(pRuntimeEnv);
      pRuntimeEnv->groupIndex += 1;

      // enable execution for next table, when handling the projection query
      enableExecutionForNextTable(pRuntimeEnv);

      if (pQuery->rec.rows >= pQuery->rec.capacity) {
        setQueryStatus(pQuery, QUERY_RESBUF_FULL);
        break;
      }
    }
  } else if (pQuery->groupbyColumn) {  // group-by on normal columns query
    while (pRuntimeEnv->groupIndex < numOfGroups) {
      SArray *group = taosArrayGetP(pQuery->tableGroupInfo.pGroupList, pRuntimeEnv->groupIndex);

      qDebug("QInfo:%p group by normal columns group:%d, total group:%" PRIzu "", pQInfo, pRuntimeEnv->groupIndex,
             numOfGroups);

      STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);

      SArray *g1 = taosArrayInit(1, POINTER_BYTES);
      SArray *tx = taosArrayDup(group);
      taosArrayPush(g1, &tx);

      STableGroupInfo gp = {.numOfTables = taosArrayGetSize(tx), .pGroupList = g1};

      // include only current table
      if (pRuntimeEnv->pQueryHandle != NULL) {
        tsdbCleanupQueryHandle(pRuntimeEnv->pQueryHandle);
        pRuntimeEnv->pQueryHandle = NULL;
      }

      // no need to update the lastkey for each table
      pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQuery->tsdb, &cond, &gp, pQInfo, &pQuery->memRef);

      taosArrayDestroy(g1);
      taosArrayDestroy(tx);
      if (pRuntimeEnv->pQueryHandle == NULL) {
        longjmp(pRuntimeEnv->env, terrno);
      }

      SArray *s = tsdbGetQueriedTableList(pRuntimeEnv->pQueryHandle);
      assert(taosArrayGetSize(s) >= 1);

      setTagVal(pRuntimeEnv, taosArrayGetP(s, 0));

      // here we simply set the first table as current table
      scanMultiTableDataBlocks(pQInfo);
      pRuntimeEnv->groupIndex += 1;

      taosArrayDestroy(s);

      // no results generated for current group, continue to try the next group
      SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
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
             pRuntimeEnv->groupIndex);

      pQuery->rec.rows = 0;
      if (pWindowResInfo->size > pQuery->rec.capacity) {
        expandBuffer(pRuntimeEnv, pWindowResInfo->size, pQInfo);
      }

      initGroupResInfo(&pRuntimeEnv->groupResInfo, &pRuntimeEnv->resultRowInfo, 0);
      copyToOutputBuf(pRuntimeEnv, pWindowResInfo);
      assert(pQuery->rec.rows == pWindowResInfo->size);

      resetResultRowInfo(pRuntimeEnv, &pRuntimeEnv->resultRowInfo);
      cleanupGroupResInfo(&pRuntimeEnv->groupResInfo);
      break;
    }
  } else if (pQuery->queryWindowIdentical && pRuntimeEnv->pTsBuf == NULL && !isTsCompQuery(pQuery)) {
    //super table projection query with identical query time range for all tables.
    SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
    resetDefaultResInfoOutputBuf(pRuntimeEnv);

    SArray *group = GET_TABLEGROUP(pRuntimeEnv, 0);
    assert(taosArrayGetSize(group) == pRuntimeEnv->tableqinfoGroupInfo.numOfTables &&
           1 == taosArrayGetSize(pRuntimeEnv->tableqinfoGroupInfo.pGroupList));

    void *pQueryHandle = pRuntimeEnv->pQueryHandle;
    if (pQueryHandle == NULL) {
      STsdbQueryCond con = createTsdbQueryCond(pQuery, &pQuery->window);
      pRuntimeEnv->pQueryHandle = tsdbQueryTables(pQuery->tsdb, &con, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);
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
    SQueryCostInfo *summary = &pQInfo->summary;
    while ((hasMoreBlock = tsdbNextDataBlock(pQueryHandle)) == true) {
      summary->totalBlocks += 1;

      if (isQueryKilled(pQInfo)) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
      }

      tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
      STableQueryInfo **pTableQueryInfo =
          (STableQueryInfo **) taosHashGet(pRuntimeEnv->tableqinfoGroupInfo.map, &blockInfo.tid, sizeof(blockInfo.tid));
      if (pTableQueryInfo == NULL) {
        break;
      }

      pQuery->current = *pTableQueryInfo;
      doTableQueryInfoTimeWindowCheck(pQuery, *pTableQueryInfo);

      if (pQuery->hasTagResults) {
        setTagVal(pRuntimeEnv, pQuery->current->pTable);
      }

      if (pQuery->prjInfo.vgroupLimit > 0 && pQuery->current->resInfo.size > pQuery->prjInfo.vgroupLimit) {
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

      int32_t ret = loadDataBlockOnDemand(pRuntimeEnv, &pQuery->current->resInfo, pQueryHandle, &blockInfo,
                                          &pStatis, &pDataBlock, &status);
      if (ret != TSDB_CODE_SUCCESS) {
        break;
      }

      if(status == BLK_DATA_DISCARD) {
        pQuery->current->lastKey =
                QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.window.ekey + step : blockInfo.window.skey + step;
        continue;
      }

      ensureOutputBuffer(pRuntimeEnv, blockInfo.rows);
      int64_t prev = getNumOfResult(pRuntimeEnv);

      pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : blockInfo.rows - 1;
      int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, &blockInfo, pStatis, binarySearchForKey, pDataBlock);

      summary->totalRows += blockInfo.rows;
      qDebug("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%" PRId64,
             GET_QINFO_ADDR(pRuntimeEnv), blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows, numOfRes,
             pQuery->current->lastKey);

      pQuery->rec.rows = getNumOfResult(pRuntimeEnv);

      int64_t inc = pQuery->rec.rows - prev;
      pQuery->current->resInfo.size += (int32_t) inc;

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
        if (limitOperator(pQuery, pQInfo)) {
          SET_STABLE_QUERY_OVER(pRuntimeEnv);
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
      SET_STABLE_QUERY_OVER(pRuntimeEnv);
    }
  } else {
    /*
     * the following two cases handled here.
     * 1. ts-comp query, and 2. the super table projection query with different query time range for each table.
     * If the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (hasRemainData(&pRuntimeEnv->groupResInfo)) {
      copyToOutputBuf(pRuntimeEnv, &pRuntimeEnv->resultRowInfo);
      pQuery->rec.total += pQuery->rec.rows;

      if (pQuery->rec.rows > 0) {
        return;
      }
    }

    // all data have returned already
    if (pRuntimeEnv->tableIndex >= pRuntimeEnv->tableqinfoGroupInfo.numOfTables) {
      return;
    }

    resetDefaultResInfoOutputBuf(pRuntimeEnv);
    resetResultRowInfo(pRuntimeEnv, &pRuntimeEnv->resultRowInfo);

    SArray *group = GET_TABLEGROUP(pRuntimeEnv, 0);
    assert(taosArrayGetSize(group) == pRuntimeEnv->tableqinfoGroupInfo.numOfTables &&
           1 == taosArrayGetSize(pRuntimeEnv->tableqinfoGroupInfo.pGroupList));

    while (pRuntimeEnv->tableIndex < pRuntimeEnv->tableqinfoGroupInfo.numOfTables) {
      if (isQueryKilled(pQInfo)) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
      }

      pQuery->current = taosArrayGetP(group, pRuntimeEnv->tableIndex);
      if (!multiTableMultioutputHelper(pQInfo, pRuntimeEnv->tableIndex)) {
        pRuntimeEnv->tableIndex++;
        continue;
      }

      // TODO handle the limit offset problem
      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
          pRuntimeEnv->tableIndex++;
          continue;
        }
      }

      scanOneTableDataBlocks(pRuntimeEnv, pQuery->current->lastKey);
      skipResults(pRuntimeEnv);

      // the limitation of output result is reached, set the query completed
      if (limitOperator(pQuery, pQInfo)) {
        SET_STABLE_QUERY_OVER(pRuntimeEnv);
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
        pRuntimeEnv->tableIndex++;
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

    if (pRuntimeEnv->tableIndex >= pRuntimeEnv->tableqinfoGroupInfo.numOfTables) {
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
    if (isTsCompQuery(pQuery)) {
      finalizeQueryResult(pRuntimeEnv);
    }

    if (pRuntimeEnv->pTsBuf != NULL) {
      pRuntimeEnv->cur = pRuntimeEnv->pTsBuf->cur;
    }

    qDebug("QInfo %p numOfTables:%" PRIu64 ", index:%d, numOfGroups:%" PRIzu ", %" PRId64
           " points returned, total:%" PRId64 ", offset:%" PRId64,
           pQInfo, (uint64_t)pRuntimeEnv->tableqinfoGroupInfo.numOfTables, pRuntimeEnv->tableIndex, numOfGroups, pQuery->rec.rows,
           pQuery->rec.total, pQuery->limit.offset);
  }
}

#endif

#if 0
static int32_t doSaveContext(SQInfo *pQInfo) {
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
  disableFuncInReverseScan(pRuntimeEnv);
  setupQueryRangeForReverseScan(pRuntimeEnv);

  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->pSecQueryHandle = tsdbQueryTables(pQuery->tsdb, &cond, &pQuery->tableGroupInfo, pQInfo, &pQuery->memRef);
  return (pRuntimeEnv->pSecQueryHandle == NULL)? -1:0;
}

static UNUSED_FUNC void doRestoreContext(SQInfo *pQInfo) {
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

#endif

static void doCloseAllTimeWindow(SQueryRuntimeEnv* pRuntimeEnv) {
//  if (QUERY_IS_INTERVAL_QUERY(pRuntimeEnv->pQuery)) {
    size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
    for (int32_t i = 0; i < numOfGroup; ++i) {
      SArray* group = GET_TABLEGROUP(pRuntimeEnv, i);

      size_t num = taosArrayGetSize(group);
      for (int32_t j = 0; j < num; ++j) {
        STableQueryInfo* item = taosArrayGetP(group, j);
        closeAllResultRows(&item->resInfo);
      }
    }
    //  } else {  // close results for group result
    //    closeAllResultRows(&pQInfo->runtimeEnv.resultRowInfo);
    //  }
//  }
}

#if 0
static UNUSED_FUNC void multiTableQueryProcess(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery           *pQuery = pRuntimeEnv->pQuery;

  if (Q_STATUS_EQUAL(pQuery->status, QUERY_COMPLETED)) {
    if (QUERY_IS_INTERVAL_QUERY(pQuery)) {
      copyResToQueryResultBuf(pQInfo, pQuery);
    } else {
      copyToOutputBuf(pRuntimeEnv, &pRuntimeEnv->resultRowInfo);
    }

    qDebug("QInfo:%p current:%"PRId64", total:%"PRId64, pQInfo, pQuery->rec.rows, pQuery->rec.total);
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
  doCloseAllTimeWindow(pQInfo);

  if (needReverseScan(pQuery)) {
    int32_t code = doSaveContext(pQInfo);
    if (code == TSDB_CODE_SUCCESS) {
      el = scanMultiTableDataBlocks(pQInfo);
      qDebug("QInfo:%p reversed scan completed, elapsed time: %" PRId64 "ms", pQInfo, el);
      doRestoreContext(pQInfo);
    } else {
      pQInfo->code = code;
    }
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
    copyResToQueryResultBuf(pQInfo, pQuery);
  } else {  // not a interval query
    initGroupResInfo(&pRuntimeEnv->groupResInfo, &pRuntimeEnv->resultRowInfo, 0);
    copyToOutputBuf(pRuntimeEnv, &pRuntimeEnv->resultRowInfo);
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

#endif

static SSDataBlock* doTableScanImpl(STableScanInfo *pTableScanInfo) {
  SSDataBlock *pBlock = &pTableScanInfo->block;
  SQuery* pQuery = pTableScanInfo->pRuntimeEnv->pQuery;
  STableGroupInfo* pTableGroupInfo = &pTableScanInfo->pRuntimeEnv->tableqinfoGroupInfo;

  while (tsdbNextDataBlock(pTableScanInfo->pQueryHandle)) {
    pTableScanInfo->numOfBlocks += 1;

    // todo check for query cancel
    tsdbRetrieveDataBlockInfo(pTableScanInfo->pQueryHandle, &pBlock->info);

    if (pTableGroupInfo->numOfTables > 1 || (pQuery->current == NULL && pTableGroupInfo->numOfTables == 1)) {
      STableQueryInfo **pTableQueryInfo = (STableQueryInfo **)taosHashGet( pTableGroupInfo->map, &pBlock->info.tid, sizeof(pBlock->info.tid));
      if (pTableQueryInfo == NULL) {
        break;
      }

      pQuery->current = *pTableQueryInfo;
      doTableQueryInfoTimeWindowCheck(pQuery, *pTableQueryInfo);
    }

    // this function never returns error?
    uint32_t status;
    int32_t  code = loadDataBlockOnDemand_rv(pTableScanInfo->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTableScanInfo->pRuntimeEnv->env, code);
    }

    // current block is ignored according to filter result by block statistics data, continue load the next block
    if (status == BLK_DATA_DISCARD || pBlock->info.rows == 0) {
      continue;
    }

    return pBlock;
  }

  return NULL;
}

static SSDataBlock* doTableScan(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;

  STableScanInfo   *pTableScanInfo = pOperator->info;
  SQueryRuntimeEnv *pRuntimeEnv = pTableScanInfo->pRuntimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;

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

    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    if (pRuntimeEnv->pTsBuf) {
      bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
      assert(ret);
    }

    qDebug("QInfo:%p start to repeat scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           pRuntimeEnv->qinfo, cond.twindow.skey, cond.twindow.ekey);
  }

  if (pTableScanInfo->reverseTimes > 0) {
    setEnvBeforeReverseScan_rv(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pTableScanInfo->pCtx, pTableScanInfo->numOfOutput);

    STsdbQueryCond cond = createTsdbQueryCond(pQuery, &pQuery->window);
    tsdbResetQueryHandle(pTableScanInfo->pQueryHandle, &cond);

    qDebug("QInfo:%p start to reverse scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           pRuntimeEnv->qinfo, cond.twindow.skey, cond.twindow.ekey);

    pRuntimeEnv->scanFlag = REVERSE_SCAN;

    pTableScanInfo->times = 1;
    pTableScanInfo->current = 0;
    pTableScanInfo->reverseTimes = 0;
    pTableScanInfo->order = cond.order;

    SSDataBlock* p = doTableScanImpl(pTableScanInfo);
    if (p != NULL) {
      return p;
    }
  }

  return NULL;
}

SOperatorInfo* createTableScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime) {
  assert(repeatTime > 0);

  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));
  pInfo->pQueryHandle = pTsdbQueryHandle;
  pInfo->times = repeatTime;
  pInfo->reverseTimes = 0;
  pInfo->order = pRuntimeEnv->pQuery->order.order;

  pInfo->current = 0;
  pInfo->pRuntimeEnv = pRuntimeEnv;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name = "SeqScanTableOp";
  pOperator->blockingOptr = false;
  pOperator->completed    = false;
  pOperator->info = pInfo;
  pOperator->numOfOutput  = pRuntimeEnv->pQuery->numOfCols;
  pOperator->exec = doTableScan;

  return pOperator;
}

void setTableScanFilterOperatorInfo(STableScanInfo* pTableScanInfo, SOperatorInfo* pDownstream) {
  assert(pTableScanInfo != NULL && pDownstream != NULL);

  pTableScanInfo->pExpr = pDownstream->pExpr;   // TODO refactor to use colId instead of pExpr
  pTableScanInfo->numOfOutput = pDownstream->numOfOutput;

  char* name = pDownstream->name;
  if ((strcasecmp(name, "TableAggregate") == 0) || (strcasecmp(name, "STableAggregate") == 0)) {
    SAggOperatorInfo* pAggInfo = pDownstream->info;

    pTableScanInfo->pCtx = pAggInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pAggInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pAggInfo->rowCellInfoOffset;
  } else if (strcasecmp(name, "HashIntervalAgg") == 0) {
    SHashIntervalOperatorInfo *pIntervalInfo = pDownstream->info;

    pTableScanInfo->pCtx = pIntervalInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pIntervalInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pIntervalInfo->rowCellInfoOffset;

  } else if (strcasecmp(name, "HashGroupbyAgg") == 0) {
    SHashGroupbyOperatorInfo *pGroupbyInfo = pDownstream->info;

    pTableScanInfo->pCtx = pGroupbyInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pGroupbyInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pGroupbyInfo->binfo.rowCellInfoOffset;

  } else if (strcasecmp(name, "STableIntervalAggOp") == 0) {
    SHashIntervalOperatorInfo *pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->rowCellInfoOffset;

  } else if (strcasecmp(name, "ArithmeticOp") == 0) {
    SArithOperatorInfo *pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->binfo.rowCellInfoOffset;
  } else {
    assert(0);
  }
}

static SOperatorInfo* createBiDirectionTableScanInfo(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime, int32_t reverseTime) {
  assert(repeatTime > 0);

  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));
  pInfo->pQueryHandle = pTsdbQueryHandle;
  pInfo->times = repeatTime;
  pInfo->reverseTimes = reverseTime;

  pInfo->current = 0;
  pInfo->order = pRuntimeEnv->pQuery->order.order;

  pInfo->pRuntimeEnv = pRuntimeEnv;

  SOperatorInfo* pOptr = calloc(1, sizeof(SOperatorInfo));
  pOptr->name = "BidirectionSeqScanTableOp";
  pOptr->blockingOptr = false;
  pOptr->info = pInfo;
  pOptr->exec = doTableScan;

  return pOptr;
}

static int32_t getTableScanOrder(STableScanInfo* pTableScanInfo) {
  return pTableScanInfo->order;
}

//static int32_t getTableScanFlag(STableScanInfo* pTableScanInfo) {
//  return pTableScanInfo->
//}

// this is a blocking operator
static SSDataBlock* doAggregate(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  SOperatorInfo* upstream = pOperator->upstream;
  pQuery->pos = 0;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    setTagVal_rv(pOperator, pQuery->current->pTable, pAggInfo->pCtx, pOperator->numOfOutput);

    // TODO opt perf
    if (strncasecmp(upstream->name, "BidirectionSeqScanTableOp", strlen("BidirectionSeqScanTableOp")) == 0) {
      STableScanInfo* pScanInfo = upstream->info;
      order = getTableScanOrder(pScanInfo);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pAggInfo->pCtx, pBlock, order);
    doAggregateImpl(pOperator, pQuery->window.skey, pAggInfo->pCtx, pBlock);
  }

  pOperator->completed = true;
  setQueryStatus(pRuntimeEnv->pQuery, QUERY_COMPLETED);

  finalizeQueryResult_rv(pOperator, pAggInfo->pCtx, &pAggInfo->resultRowInfo, pAggInfo->rowCellInfoOffset);
  pAggInfo->pRes->info.rows = getNumOfResult_rv(pRuntimeEnv, pAggInfo->pCtx, pOperator->numOfOutput);

  return pAggInfo->pRes;
}

static SSDataBlock* doSTableAggregate(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (hasRemainData(&pRuntimeEnv->groupResInfo)) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pAggInfo->pRes);

    if (pAggInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->completed = true;
    }

    return pAggInfo->pRes;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  SOperatorInfo* upstream = pOperator->upstream;
  pQuery->pos = 0;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    setTagVal_rv(pOperator, pRuntimeEnv->pQuery->current->pTable, pAggInfo->pCtx, pOperator->numOfOutput);

    // TODO opt perf
    if (strncasecmp(upstream->name, "BidirectionSeqScanTableOp", strlen("BidirectionSeqScanTableOp")) == 0) {
      STableScanInfo* pScanInfo = upstream->info;
      order = getTableScanOrder(pScanInfo);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pAggInfo->pCtx, pBlock, order);

    TSKEY k = (pQuery->order.order == TSDB_ORDER_ASC)? pBlock->info.window.ekey + 1:pBlock->info.window.skey-1;
    setExecutionContext_rv(pRuntimeEnv, pAggInfo, pOperator->numOfOutput, pQuery->current->groupIndex, k);
    doAggregateImpl(pOperator, pQuery->window.skey, pAggInfo->pCtx, pBlock);
  }

  closeAllResultRows(&pAggInfo->resultRowInfo);
  updateWindowResNumOfRes_rv(pRuntimeEnv, pAggInfo->pCtx, pOperator->numOfOutput, &pAggInfo->resultRowInfo, pAggInfo->rowCellInfoOffset);
  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pAggInfo->resultRowInfo, 0);

  toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pAggInfo->pRes);
  if (pAggInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->completed = true;
  }

  return pAggInfo->pRes;
}

static SSDataBlock* doArithmeticOperation(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;

  SArithOperatorInfo* pArithInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  pRuntimeEnv->pQuery->pos = 0;
  pArithInfo->binfo.pRes->info.rows = 0;

  while(1) {
    SSDataBlock* pBlock = pOperator->upstream->exec(pOperator->upstream);
    if (pBlock == NULL) {
      setQueryStatus(pRuntimeEnv->pQuery, QUERY_COMPLETED);
      break;
    }

    setTagVal_rv(pOperator, pRuntimeEnv->pQuery->current->pTable, pArithInfo->binfo.pCtx, pOperator->numOfOutput);

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pArithInfo->binfo.pCtx, pBlock, pRuntimeEnv->pQuery->order.order);
    updateOutputBuf(pArithInfo, pBlock->info.rows);
    arithmeticApplyFunctions(pRuntimeEnv, pArithInfo->binfo.pCtx, pOperator->numOfOutput);

    pArithInfo->binfo.pRes->info.rows += pBlock->info.rows;
    if (pArithInfo->binfo.pRes->info.rows >= 4096) {
      break;
    }
  }

  if (pArithInfo->binfo.pRes->info.rows > 0) {
    return pArithInfo->binfo.pRes;
  } else {
    return NULL;
  }
}

static SSDataBlock* doLimit(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SLimitOperatorInfo* pInfo = pOperator->info;

  SSDataBlock* pBlock = pOperator->upstream->exec(pOperator->upstream);
  if (pBlock == NULL) {
    setQueryStatus(pOperator->pRuntimeEnv->pQuery, QUERY_COMPLETED);
    pOperator->completed = true;
    return NULL;
  }

  if (pInfo->total + pBlock->info.rows >= pInfo->limit) {
    pBlock->info.rows = (pInfo->limit - pInfo->total);

    setQueryStatus(pOperator->pRuntimeEnv->pQuery, QUERY_COMPLETED);
    pOperator->completed = true;
  }

  return pBlock;
}

static SSDataBlock* doOffset(void* param) {
  SOperatorInfo *pOperator = (SOperatorInfo *)param;

  SOffsetOperatorInfo *pInfo = pOperator->info;

  while (1) {
    SSDataBlock *pBlock = pOperator->upstream->exec(pOperator->upstream);
    if (pBlock == NULL) {
      setQueryStatus(pOperator->pRuntimeEnv->pQuery, QUERY_COMPLETED);
      return NULL;
    }

    if (pInfo->currentOffset == 0) {
      return pBlock;
    } else if (pInfo->currentOffset > pBlock->info.rows) {
      pInfo->currentOffset -= pBlock->info.rows;
    } else {
      int32_t remain = pBlock->info.rows - pInfo->currentOffset;
      pBlock->info.rows = remain;

      for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
        SColumnInfoData *pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

        int16_t bytes = pColInfoData->info.bytes;
        memmove(pColInfoData->pData, pColInfoData->pData + bytes * pInfo->currentOffset, remain * bytes);
      }

      pInfo->currentOffset = 0;
      return pBlock;
    }
  }
}

static SSDataBlock* doHashIntervalAgg(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SHashIntervalOperatorInfo* pIntervalInfo = pOperator->info;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (hasRemainData(&pRuntimeEnv->groupResInfo)) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

    if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->completed = true;
    }

    return pIntervalInfo->pRes;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;
  STimeWindow win = pQuery->window;

  SOperatorInfo* upstream = pOperator->upstream;
  pQuery->pos = 0;

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

  closeAllResultRows(&pIntervalInfo->resultRowInfo);
  setQueryStatus(pRuntimeEnv->pQuery, QUERY_COMPLETED);
  finalizeQueryResult_rv(pOperator, pIntervalInfo->pCtx, &pIntervalInfo->resultRowInfo, pIntervalInfo->rowCellInfoOffset);

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pIntervalInfo->resultRowInfo, 0);
  toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

  if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->completed = true;
  }

  return pIntervalInfo->pRes;
}

static SSDataBlock* doSTableIntervalAgg(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SHashIntervalOperatorInfo* pIntervalInfo = pOperator->info;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (hasRemainData(&pRuntimeEnv->groupResInfo)) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

    if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->completed = true;
    }

    return pIntervalInfo->pRes;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  int32_t order = pQuery->order.order;

  SOperatorInfo* upstream = pOperator->upstream;
  pQuery->pos = 0;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    STableQueryInfo* pTableQueryInfo = pRuntimeEnv->pQuery->current;

    setTagVal_rv(pOperator, pTableQueryInfo->pTable, pIntervalInfo->pCtx, pOperator->numOfOutput);
    setInputDataBlock(pOperator, pIntervalInfo->pCtx, pBlock, pQuery->order.order);
    setIntervalQueryRange(pRuntimeEnv, pBlock->info.window.skey);

    hashIntervalAgg(pOperator, &pTableQueryInfo->resInfo, pIntervalInfo, pBlock, pTableQueryInfo->groupIndex);
  }

  pQuery->order.order = order;   // TODO : restore the order
  doCloseAllTimeWindow(pRuntimeEnv);
  setQueryStatus(pRuntimeEnv->pQuery, QUERY_COMPLETED);

  copyResToQueryResultBuf_rv(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
  if (pIntervalInfo->pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->completed = true;
  }

  return pIntervalInfo->pRes;
}

static SSDataBlock* doHashGroupbyAgg(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SHashGroupbyOperatorInfo *pInfo = pOperator->info;

  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (hasRemainData(&pRuntimeEnv->groupResInfo)) {
    toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);
    if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->completed = true;
    }
    return pInfo->binfo.pRes;
  }

  SOperatorInfo* upstream = pOperator->upstream;
  pRuntimeEnv->pQuery->pos = 0;

  while(1) {
    SSDataBlock* pBlock = upstream->exec(upstream);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, pRuntimeEnv->pQuery->order.order);
    if (pInfo->colIndex == -1) {
      pInfo->colIndex = getGroupbyColumnData_rv(pRuntimeEnv->pQuery->pGroupbyExpr, pBlock);
    }

    hashGroupbyAgg(pRuntimeEnv, pOperator, pInfo, pBlock);
  }

  closeAllResultRows(&pInfo->binfo.resultRowInfo);
  setQueryStatus(pRuntimeEnv->pQuery, QUERY_COMPLETED);

  if (!pRuntimeEnv->pQuery->stableQuery) {
    finalizeQueryResult_rv(pOperator, pInfo->binfo.pCtx, &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);
  }

  updateWindowResNumOfRes_rv(pRuntimeEnv, pInfo->binfo.pCtx, pOperator->numOfOutput, &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pInfo->binfo.resultRowInfo, 0);
  toSSDataBlock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);

  if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->completed = true;
  }

  return pInfo->binfo.pRes;
}

static SSDataBlock* doFill(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->completed) {
    return NULL;
  }

  SFillOperatorInfo *pInfo = pOperator->info;
  SQueryRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (taosFillHasMoreResults(pRuntimeEnv->pFillInfo)) {
    doFillGapsInResults_rv(pRuntimeEnv, pInfo->pRes);
    return pInfo->pRes;
  }

  while(1) {
    SSDataBlock *pBlock = pOperator->upstream->exec(pOperator->upstream);
    if (pBlock == NULL) {
      taosFillSetStartInfo(pRuntimeEnv->pFillInfo, 0, pRuntimeEnv->pQuery->window.ekey);
    } else {
      int64_t ekey = Q_STATUS_EQUAL(pRuntimeEnv->pQuery->status, QUERY_COMPLETED)?pRuntimeEnv->pQuery->window.ekey:pBlock->info.window.ekey;
      taosFillSetStartInfo(pRuntimeEnv->pFillInfo, pBlock->info.rows, ekey);
      taosFillSetInputDataBlock(pRuntimeEnv->pFillInfo, pBlock);
    }

    doFillGapsInResults_rv(pRuntimeEnv, pInfo->pRes);
    return pInfo->pRes;
  }

  return pInfo->pRes;
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

  pInfo->pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->rowCellInfoOffset);
  initResultRowInfo(&pInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  pInfo->pRes = createOutputBuf(pExpr, numOfOutput);
  setDefaultOutputBuf(pRuntimeEnv, pInfo->pCtx, &pInfo->resultRowInfo, pInfo->pRes, pInfo->rowCellInfoOffset);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableAggregate";
  pOperator->blockingOptr = true;
  pOperator->completed    = false;
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
  pInfo->pRes = destroyOutputBuf(pInfo->pRes);
}

static void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput) {
  SHashGroupbyOperatorInfo* pInfo = (SHashGroupbyOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

static void destroyArithOperatorInfo(void* param, int32_t numOfOutput) {
  SArithOperatorInfo* pInfo = (SArithOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

SOperatorInfo* createStableAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SAggOperatorInfo* pInfo = calloc(1, sizeof(SAggOperatorInfo));

  pInfo->pRes = createOutputBuf(pExpr, numOfOutput);
  pInfo->pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->rowCellInfoOffset);
  initResultRowInfo(&pInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "STableAggregate";
  pOperator->blockingOptr = true;
  pOperator->completed    = false;
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

  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput);
  pInfo->binfo.pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8, TSDB_DATA_TYPE_INT);
  setDefaultOutputBuf(pRuntimeEnv, pInfo->binfo.pCtx, &pInfo->binfo.resultRowInfo, pInfo->binfo.pRes, pInfo->binfo.rowCellInfoOffset);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "ArithmeticOp";
  pOperator->blockingOptr = false;
  pOperator->completed    = false;
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

  pOperator->name         = "LimitOp";
  pOperator->blockingOptr = false;
  pOperator->completed    = false;
  pOperator->upstream     = upstream;
  pOperator->exec         = doLimit;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  return pOperator;
}

SOperatorInfo* createOffsetOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream) {
  SOffsetOperatorInfo* pInfo = calloc(1, sizeof(SOffsetOperatorInfo));

  pInfo->offset = pRuntimeEnv->pQuery->limit.offset;
  pInfo->currentOffset = pInfo->offset;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "OffsetOp";
  pOperator->blockingOptr = false;
  pOperator->completed    = false;
  pOperator->upstream     = upstream;
  pOperator->exec         = doOffset;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  return pOperator;
}

SOperatorInfo* createIntervalAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SHashIntervalOperatorInfo* pInfo = calloc(1, sizeof(SHashIntervalOperatorInfo));

  pInfo->pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->rowCellInfoOffset);
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput);
  initResultRowInfo(&pInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "HashIntervalAgg";
  pOperator->blockingOptr = true;
  pOperator->completed    = false;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doHashIntervalAgg;
  pOperator->cleanup      = destroyBasicOperatorInfo;

  return pOperator;
}

SOperatorInfo* createStableIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SHashIntervalOperatorInfo* pInfo = calloc(1, sizeof(SHashIntervalOperatorInfo));

  pInfo->pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->rowCellInfoOffset);
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput);
  initResultRowInfo(&pInfo->resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "STableIntervalAggOp";
  pOperator->blockingOptr = true;
  pOperator->completed    = false;
  pOperator->upstream     = upstream;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->exec         = doSTableIntervalAgg;
  pOperator->cleanup      = destroyBasicOperatorInfo;

  return pOperator;
}

SOperatorInfo* createHashGroupbyOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SHashGroupbyOperatorInfo* pInfo = calloc(1, sizeof(SHashGroupbyOperatorInfo));
  pInfo->colIndex = -1;  // group by column index

  pInfo->binfo.pCtx = createSQLFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8, TSDB_DATA_TYPE_INT);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "HashGroupbyAgg";
  pOperator->blockingOptr = true;
  pOperator->completed    = false;
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
  pInfo->pRes        = createOutputBuf(pExpr, numOfOutput);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "FillOp";
  pOperator->blockingOptr = false;
  pOperator->completed    = false;
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

  STagScanInfo   *pTagScanInfo = pOperator->info;
  SQueryRuntimeEnv *pRuntimeEnv = pOperator->pRuntimeEnv;
  SQuery* pQuery = pRuntimeEnv->pQuery;

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  assert(numOfGroup == 0 || numOfGroup == 1);

  if (numOfGroup == 0) {
    return NULL;
  }

  SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);

  size_t num = taosArrayGetSize(pa);
  assert(num == pRuntimeEnv->tableqinfoGroupInfo.numOfTables);

  if (pTagScanInfo->pRes == NULL) {
    pTagScanInfo->pRes = createOutputBuf(pOperator->pExpr, pOperator->numOfOutput);
  }

  int32_t count = 0;
//  int32_t functionId = pOperator->pExpr[0].base.functionId;
  /*if (functionId == TSDB_FUNC_TID_TAG) { // return the tags & table Id
    assert(pQuery->numOfOutput == 1);

    SExprInfo* pExprInfo = &pOperator->pExpr[0];
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

    while(pRuntimeEnv->tableIndex < num && count < pQuery->rec.capacity) {
      int32_t i = pRuntimeEnv->tableIndex++;
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

    qDebug("QInfo:%p create (tableId, tag) info completed, rows:%d", pRuntimeEnv->qinfo, count);

  } else if (functionId == TSDB_FUNC_COUNT) {// handle the "count(tbname)" query
    *(int64_t*) pQuery->sdata[0]->data = num;

    count = 1;
    SET_STABLE_QUERY_OVER(pRuntimeEnv);
    qDebug("QInfo:%p create count(tbname) query, res:%d rows:1", pRuntimeEnv->qinfo, count);
  } else*/ {  // return only the tags|table name etc.
    count = 0;
    SSchema* tbnameSchema = tGetTbnameColumnSchema();

    int32_t maxNumOfTables = (int32_t)pQuery->rec.capacity;
//    if (pQuery->limit.limit >= 0 && pQuery->limit.limit < pQuery->rec.capacity) {
//      maxNumOfTables = (int32_t)pQuery->limit.limit;
//    }

    while(pRuntimeEnv->tableIndex < num && count < maxNumOfTables) {
      int32_t i = pRuntimeEnv->tableIndex++;

      SExprInfo* pExprInfo = pOperator->pExpr;
      STableQueryInfo* item = taosArrayGetP(pa, i);

      char *data = NULL, *dst = NULL;
      int16_t type = 0, bytes = 0;
      for(int32_t j = 0; j < pOperator->numOfOutput; ++j) {
        // not assign value in case of user defined constant output column
        if (TSDB_COL_IS_UD_COL(pExprInfo[j].base.colInfo.flag)) {
          continue;
        }

        SColumnInfoData* pColInfo = taosArrayGet(pTagScanInfo->pRes->pDataBlock, j);

        if (pExprInfo[j].base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
          bytes = tbnameSchema->bytes;
          type = tbnameSchema->type;

          data = tsdbGetTableName(item->pTable);
          dst = pColInfo->pData + count * tbnameSchema->bytes;
        } else {
          type = pExprInfo[j].type;
          bytes = pExprInfo[j].bytes;

          data = tsdbGetTableTagVal(item->pTable, pExprInfo[j].base.colInfo.colId, type, bytes);
          dst = pColInfo->pData + count * pExprInfo[j].bytes;
        }

        doSetTagValueToResultBuf(dst, data, type, bytes);
      }
      count += 1;
    }

    pTagScanInfo->pRes->info.rows = count;
    qDebug("QInfo:%p create tag values results completed, rows:%d", pRuntimeEnv->qinfo, count);
  }

  return pTagScanInfo->pRes;
}

SOperatorInfo* createTagScanOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv) {
  STagScanInfo* pInfo = calloc(1, sizeof(STagScanInfo));

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name = "SeqTagScanOp";
  pOperator->blockingOptr = false;
  pOperator->completed    = false;
  pOperator->info         = pInfo;
  pOperator->exec         = doTagScan;
  pOperator->pExpr        = pRuntimeEnv->pQuery->pExpr1;
  pOperator->numOfOutput  = pRuntimeEnv->pQuery->numOfOutput;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  return pOperator;
}

/*
 * in each query, this function will be called only once, no retry for further result.
 *
 * select count(*)/top(field,k)/avg(field name) from table_name [where ts>now-1a];
 * select count(*) from table_name group by status_column;
 */
void tableAggregationProcess(SQInfo *pQInfo, STableQueryInfo* pTableInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (!pQuery->topBotQuery && pQuery->limit.offset > 0) {  // no need to execute, since the output will be ignore.
    return;
  }

  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);
  pQuery->rec.rows = (pRuntimeEnv->outputBuf != NULL)? pRuntimeEnv->outputBuf->info.rows:0;
}


void tableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

#if 0
  if (hasNotReturnedResults(pRuntimeEnv, &pRuntimeEnv->groupResInfo)) {
    if (pQuery->fillType != TSDB_FILL_NONE && !isPointInterpoQuery(pQuery)) {
      /*
       * There are remain results that are not returned due to result interpolation
       * So, we do keep in this procedure instead of launching retrieve procedure for next results.
       */
//      pQuery->rec.rows = doFillGapsInResults(pRuntimeEnv, (tFilePage **)pQuery->sdata);
      if (pQuery->rec.rows > 0) {
        limitOperator(pQuery, pQInfo);
        qDebug("QInfo:%p current:%" PRId64 " returned, total:%" PRId64, pQInfo, pQuery->rec.rows, pQuery->rec.total);
      } else {
        return copyAndFillResult(pQInfo);
      }

    } else {
      pQuery->rec.rows = 0;
      assert(pRuntimeEnv->resultRowInfo.size > 0);
      copyToOutputBuf(pRuntimeEnv, &pRuntimeEnv->resultRowInfo);
      doSecondaryArithmeticProcess(pQuery);

      if (pQuery->rec.rows > 0) {
        limitOperator(pQuery, pQInfo);
      }

      if (pQuery->rec.rows > 0) {
        qDebug("QInfo:%p %" PRId64 " rows returned from group results, total:%" PRId64 "", pQInfo, pQuery->rec.rows,
               pQuery->rec.total);
      } else {
        qDebug("QInfo:%p query over, %" PRId64 " rows are returned", pQInfo, pQuery->rec.total);
      }
    }

    return;
  }
#endif

  // number of points returned during this query
  pQuery->rec.rows = 0;
  int64_t st = taosGetTimestampUs();

  assert(pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 1);
  SArray* g = GET_TABLEGROUP(pRuntimeEnv, 0);

  STableQueryInfo* item = taosArrayGetP(g, 0);
  pQuery->current = item;

  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);
  pQuery->rec.rows = (pRuntimeEnv->outputBuf != NULL)? pRuntimeEnv->outputBuf->info.rows:0;

  // record the total elapsed time
  pQInfo->summary.elapsedTime += (taosGetTimestampUs() - st);
  assert(pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 1);
}

void buildTableBlockDistResult(SQInfo *pQInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->pos = 0;

  STableBlockDist *pTableBlockDist  = calloc(1, sizeof(STableBlockDist)); 
  pTableBlockDist->dataBlockInfos   = taosArrayInit(512, sizeof(SDataBlockInfo));
  pTableBlockDist->result           = (char *)malloc(512);

  TsdbQueryHandleT pQueryHandle = pRuntimeEnv->pQueryHandle;
  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
  SSchema blockDistSchema = tGetBlockDistColumnSchema();

  int64_t startTime = taosGetTimestampUs();
  while (tsdbNextDataBlockWithoutMerge(pQueryHandle)) {
    if (isQueryKilled(GET_QINFO_ADDR(pRuntimeEnv))) {
      freeTableBlockDist(pTableBlockDist);
      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }
    if (pTableBlockDist->firstSeekTimeUs == 0) {
       pTableBlockDist->firstSeekTimeUs = taosGetTimestampUs() - startTime;
    }   
   
    tsdbRetrieveDataBlockInfo(pQueryHandle, &blockInfo);
    taosArrayPush(pTableBlockDist->dataBlockInfos, &blockInfo); 
  }
  if (terrno != TSDB_CODE_SUCCESS) {
    freeTableBlockDist(pTableBlockDist);
    longjmp(pRuntimeEnv->env, terrno);
  }

  pTableBlockDist->numOfRowsInMemTable = tsdbGetNumOfRowsInMemTable(pQueryHandle); 
  
  generateBlockDistResult(pTableBlockDist); 

  int type = -1;
  assert(pQuery->numOfOutput == 1);
  SExprInfo* pExprInfo = pQuery->pExpr1;
  for (int32_t j = 0; j < pQuery->numOfOutput; j++) {
    if (pExprInfo[j].base.colInfo.colId == TSDB_BLOCK_DIST_COLUMN_INDEX) {
      type = blockDistSchema.type;
    }
    assert(type == TSDB_DATA_TYPE_BINARY);
    STR_WITH_SIZE_TO_VARSTR(pQuery->sdata[j]->data, pTableBlockDist->result, (VarDataLenT)strlen(pTableBlockDist->result)); 
  }

  freeTableBlockDist(pTableBlockDist);

  pQuery->rec.rows = 1;
  setQueryStatus(pQuery, QUERY_COMPLETED);
}

void stableQueryImpl(SQInfo *pQInfo) {
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->rec.rows = 0;

  int64_t st = taosGetTimestampUs();

//  if (QUERY_IS_INTERVAL_QUERY(pQuery) ||
//      (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)) && (!pQuery->groupbyColumn))) {
    //multiTableQueryProcess(pQInfo);
    pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);
    pQuery->rec.rows = pRuntimeEnv->outputBuf != NULL? pRuntimeEnv->outputBuf->info.rows:0;
//  } else {
//    assert(pQuery->checkResultBuf == 1 || isPointInterpoQuery(pQuery) || pQuery->groupbyColumn);
//    sequentialTableProcess(pQInfo);
//  }

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
          (pFuncMsg->functionId == TSDB_FUNC_COUNT && pFuncMsg->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX)) {
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
      pExprMsg->colInfo.flag = htons(pExprMsg->colInfo.flag);
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

int32_t createQueryFuncExprFromMsg(SQueryTableMsg *pQueryMsg, int32_t numOfOutput, SExprInfo **pExprInfo, SSqlFuncMsg **pExprMsg,
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
        return TSDB_CODE_QRY_INVALID_MSG;
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

//  // TODO refactor
//  for (int32_t i = 0; i < numOfOutput; ++i) {
//    pExprs[i].base = *pExprMsg[i];
//    int16_t functId = pExprs[i].base.functionId;
//
//    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
//      int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].base, pTagCols);
//      if (j < 0 || j >= pQueryMsg->numOfCols) {
//        return TSDB_CODE_QRY_INVALID_MSG;
//      } else {
//        SColumnInfo *pCol = &pQueryMsg->colList[j];
//        int32_t ret =
//            getResultDataInfo(pCol->type, pCol->bytes, functId, (int32_t)pExprs[i].base.arg[0].argValue.i64,
//                              &pExprs[i].type, &pExprs[i].bytes, &pExprs[i].interBytes, tagLen, isSuperTable);
//        assert(ret == TSDB_CODE_SUCCESS);
//      }
//    }
//  }

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

static void calResultBufSize(SQuery* pQuery) {
  const int32_t RESULT_MSG_MIN_SIZE  = 1024 * (1024 + 512);  // bytes
  const int32_t RESULT_MSG_MIN_ROWS  = 8192;
  const float RESULT_THRESHOLD_RATIO = 0.85f;

  if (isProjQuery(pQuery)) {
    int32_t numOfRes = RESULT_MSG_MIN_SIZE / pQuery->resultRowSize;
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

SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SSqlGroupbyExpr *pGroupbyExpr, SExprInfo *pExprs,
                               SExprInfo *pSecExprs, STableGroupInfo *pTableGroupInfo, SColumnInfo* pTagCols, bool stableQuery, char* sql) {
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
    if (pQuery->pExpr2 == NULL || col >= pQuery->numOfExpr2) {
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

  pQuery->pos = -1;
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

  colIdCheck(pQuery);

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
    setQueryStatus(pQuery, QUERY_COMPLETED);
    pRuntimeEnv->tableqinfoGroupInfo.numOfTables = 0;
    // todo free memory
    return TSDB_CODE_SUCCESS;
  }

  if (pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:%p no table qualified for tag filter, abort query", pQInfo);
    setQueryStatus(pQuery, QUERY_COMPLETED);
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

size_t getResultSize(SQInfo *pQInfo, int64_t *numOfRows) {
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  /*
   * get the file size and set the numOfRows to be the file size, since for tsComp query,
   * the returned row size is equalled to 1
   * TODO handle the case that the file is too large to send back one time
   */
  if (isTsCompQuery(pQuery) && (*numOfRows) > 0) {
    struct stat fStat;
    FILE *f = *(FILE **)pQuery->sdata[0]->data;
    if ((f != NULL) && (fstat(fileno(f), &fStat) == 0)) {
      *numOfRows = fStat.st_size;
      return fStat.st_size;
    } else {
      qError("QInfo:%p failed to get file info, file:%p, reason:%s", pQInfo, f, strerror(errno));
      return 0;
    }
  } else {
    return (size_t)(pQuery->resultRowSize * (*numOfRows));
  }
}

int32_t doDumpQueryResult(SQInfo *pQInfo, char *data) {
  // the remained number of retrieved rows, not the interpolated result
  SQuery *pQuery = pQInfo->runtimeEnv.pQuery;

  // load data from file to msg buffer
  if (isTsCompQuery(pQuery)) {

    FILE *f = *(FILE **)pQuery->sdata[0]->data;  // TODO refactor

    // make sure file exist
    if (f) {
      off_t s = lseek(fileno(f), 0, SEEK_END);

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

      if (s <= (sizeof(STSBufFileHeader) + sizeof(STSGroupBlockInfo) + 6 * sizeof(int32_t))) {
        qDump(data, s);
        
        assert(0);
      }

      fclose(f);
      *(FILE **)pQuery->sdata[0]->data = NULL;
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
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  assert(numOfGroup == 0 || numOfGroup == 1);

  if (numOfGroup == 0) {
    return;
  }

  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot);
  pQuery->rec.rows = pRuntimeEnv->outputBuf->info.rows;
  return;

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

    while(pRuntimeEnv->tableIndex < num && count < pQuery->rec.capacity) {
      int32_t i = pRuntimeEnv->tableIndex++;
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
    *(int64_t*) pQuery->sdata[0]->data = num;

    count = 1;
    SET_STABLE_QUERY_OVER(pRuntimeEnv);
    qDebug("QInfo:%p create count(tbname) query, res:%d rows:1", pQInfo, count);
  } else {  // return only the tags|table name etc.
    count = 0;
    SSchema* tbnameSchema = tGetTbnameColumnSchema();

    int32_t maxNumOfTables = (int32_t)pQuery->rec.capacity;
    if (pQuery->limit.limit >= 0 && pQuery->limit.limit < pQuery->rec.capacity) {
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
          dst = pQuery->sdata[j]->data + count * tbnameSchema->bytes;
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
