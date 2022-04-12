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

#include "filter.h"
#include "functionMgt.h"
#include "function.h"
#include "querynodes.h"
#include "tname.h"
#include "os.h"

#include "parser.h"
#include "tdatablock.h"
#include "texception.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tsort.h"
#include "ttime.h"

#include "executorimpl.h"
#include "function.h"
#include "query.h"
#include "tcompare.h"
#include "tcompression.h"
#include "thash.h"
#include "vnode.h"
#include "ttypes.h"
#include "vnode.h"

#define IS_MAIN_SCAN(runtime)          ((runtime)->scanFlag == MAIN_SCAN)
#define IS_REVERSE_SCAN(runtime)       ((runtime)->scanFlag == REVERSE_SCAN)
#define IS_REPEAT_SCAN(runtime)        ((runtime)->scanFlag == REPEAT_SCAN)
#define SET_MAIN_SCAN_FLAG(runtime)    ((runtime)->scanFlag = MAIN_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define TSWINDOW_IS_EQUAL(t1, t2) (((t1).skey == (t2).skey) && ((t1).ekey == (t2).ekey))

#define SDATA_BLOCK_INITIALIZER \
  (SDataBlockInfo) { {0}, 0 }

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

enum {
  TS_JOIN_TS_EQUAL = 0,
  TS_JOIN_TS_NOT_EQUALS = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP = 2,
} SResultTsInterpType;


#if 0
static UNUSED_FUNC void *u_malloc (size_t __size) {
  uint32_t v = taosRand();

  if (v % 1000 <= 0) {
    return NULL;
  } else {
    return taosMemoryMalloc(__size);
  }
}

static UNUSED_FUNC void* u_calloc(size_t num, size_t __size) {
  uint32_t v = taosRand();
  if (v % 1000 <= 0) {
    return NULL;
  } else {
    return taosMemoryCalloc(num, __size);
  }
}

static UNUSED_FUNC void* u_realloc(void* p, size_t __size) {
  uint32_t v = taosRand();
  if (v % 5 <= 1) {
    return NULL;
  } else {
    return taosMemoryRealloc(p, __size);
  }
}

#define calloc  u_calloc
#define malloc  u_malloc
#define realloc u_realloc
#endif

#define CLEAR_QUERY_STATUS(q, st)   ((q)->status &= (~(st)))
#define GET_NUM_OF_TABLEGROUP(q)    taosArrayGetSize((q)->tableqinfoGroupInfo.pGroupList)
#define QUERY_IS_INTERVAL_QUERY(_q) ((_q)->interval.interval > 0)

#define TSKEY_MAX_ADD(a, b)                      \
  do {                                           \
    if (a < 0) {                                 \
      a = a + b;                                 \
      break;                                     \
    }                                            \
    if (sizeof(a) == sizeof(int32_t)) {          \
      if ((b) > 0 && ((b) >= INT32_MAX - (a))) { \
        a = INT32_MAX;                           \
      } else {                                   \
        a = a + b;                               \
      }                                          \
    } else {                                     \
      if ((b) > 0 && ((b) >= INT64_MAX - (a))) { \
        a = INT64_MAX;                           \
      } else {                                   \
        a = a + b;                               \
      }                                          \
    }                                            \
  } while (0)

#define TSKEY_MIN_SUB(a, b)                      \
  do {                                           \
    if (a >= 0) {                                \
      a = a + b;                                 \
      break;                                     \
    }                                            \
    if (sizeof(a) == sizeof(int32_t)) {          \
      if ((b) < 0 && ((b) <= INT32_MIN - (a))) { \
        a = INT32_MIN;                           \
      } else {                                   \
        a = a + b;                               \
      }                                          \
    } else {                                     \
      if ((b) < 0 && ((b) <= INT64_MIN - (a))) { \
        a = INT64_MIN;                           \
      } else {                                   \
        a = a + b;                               \
      }                                          \
    }                                            \
  } while (0)

int32_t getMaximumIdleDurationSec() { return tsShellActivityTimer * 2; }

static int32_t getExprFunctionId(SExprInfo* pExprInfo) {
  assert(pExprInfo != NULL && pExprInfo->pExpr != NULL && pExprInfo->pExpr->nodeType == TEXPR_UNARYEXPR_NODE);
  return 0;
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
  tw->skey = convertTimePrecision((int64_t)mktime(&tm) * 1000L, TSDB_TIME_PRECISION_MILLI, precision);

  mon = (int)(mon + interval);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->ekey = convertTimePrecision((int64_t)mktime(&tm) * 1000L, TSDB_TIME_PRECISION_MILLI, precision);

  tw->ekey -= 1;
}

static void doSetTagValueToResultBuf(char* output, const char* val, int16_t type, int16_t bytes);
static bool functionNeedToExecute(SqlFunctionCtx* pCtx);

static void setBlockStatisInfo(SqlFunctionCtx* pCtx, SSDataBlock* pSDataBlock, SColumn* pColumn);

static void destroyTableQueryInfoImpl(STableQueryInfo* pTableQueryInfo);
static bool hasMainOutput(STaskAttr* pQueryAttr);

static SColumnInfo* extractColumnFilterInfo(SExprInfo* pExpr, int32_t numOfOutput, int32_t* numOfFilterCols);

static int32_t setTimestampListJoinInfo(STaskRuntimeEnv* pRuntimeEnv, SVariant* pTag, STableQueryInfo* pTableQueryInfo);
static void    releaseQueryBuf(size_t numOfTables);
static int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
// static STsdbQueryCond createTsdbQueryCond(STaskAttr* pQueryAttr, STimeWindow* win);
static STableIdInfo createTableIdInfo(STableQueryInfo* pTableQueryInfo);

static int32_t getNumOfScanTimes(STaskAttr* pQueryAttr);

static void destroyBasicOperatorInfo(void* param, int32_t numOfOutput);
static void destroySFillOperatorInfo(void* param, int32_t numOfOutput);
static void destroyProjectOperatorInfo(void* param, int32_t numOfOutput);
static void destroyTagScanOperatorInfo(void* param, int32_t numOfOutput);
static void destroyOrderOperatorInfo(void* param, int32_t numOfOutput);
static void destroySWindowOperatorInfo(void* param, int32_t numOfOutput);
static void destroyStateWindowOperatorInfo(void* param, int32_t numOfOutput);
static void destroyAggOperatorInfo(void* param, int32_t numOfOutput);

static void destroyIntervalOperatorInfo(void* param, int32_t numOfOutput);
static void destroyExchangeOperatorInfo(void* param, int32_t numOfOutput);
static void destroyConditionOperatorInfo(void* param, int32_t numOfOutput);

static void destroyOperatorInfo(SOperatorInfo* pOperator);
static void destroySysTableScannerOperatorInfo(void* param, int32_t numOfOutput);

void doSetOperatorCompleted(SOperatorInfo* pOperator) {
  pOperator->status = OP_EXEC_DONE;
  if (pOperator->pTaskInfo != NULL) {
    setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  }
}

#define OPTR_IS_OPENED(_optr)  (((_optr)->status & OP_OPENED) == OP_OPENED)
#define OPTR_SET_OPENED(_optr) ((_optr)->status |= OP_OPENED)

int32_t operatorDummyOpenFn(SOperatorInfo* pOperator) {
  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

void operatorDummyCloseFn(void* param, int32_t numOfCols) {}

static int32_t doCopyToSDataBlock(SSDataBlock* pBlock, int32_t rowCapacity, SExprInfo* pExprInfo, SDiskbasedBuf* pBuf, SGroupResInfo* pGroupResInfo,
                                  int32_t orderType, int32_t* rowCellOffset);
static void    initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void    getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, int64_t keyFirst,
                                       int64_t keyLast, STimeWindow* win);

static void setResultBufSize(STaskAttr* pQueryAttr, SResultInfo* pResultInfo);
static void setCtxTagForJoin(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, void* pTable);
static void doSetTableGroupOutputBuf(SAggOperatorInfo* pAggInfo, int32_t numOfOutput, int32_t tableGroupId,
                                     SExecTaskInfo* pTaskInfo);

SArray* getOrderCheckColumns(STaskAttr* pQuery);

typedef struct SRowCompSupporter {
  STaskRuntimeEnv* pRuntimeEnv;
  int16_t          dataOffset;
  __compar_fn_t    comFunc;
} SRowCompSupporter;

static int compareRowData(const void* a, const void* b, const void* userData) {
  const SResultRow* pRow1 = (const SResultRow*)a;
  const SResultRow* pRow2 = (const SResultRow*)b;

  SRowCompSupporter* supporter = (SRowCompSupporter*)userData;
  STaskRuntimeEnv*   pRuntimeEnv = supporter->pRuntimeEnv;

  SFilePage* page1 = getBufPage(pRuntimeEnv->pResultBuf, pRow1->pageId);
  SFilePage* page2 = getBufPage(pRuntimeEnv->pResultBuf, pRow2->pageId);

  int16_t offset = supporter->dataOffset;
  char*   in1 = getPosInResultPage(pRuntimeEnv->pQueryAttr, page1, pRow1->offset, offset);
  char*   in2 = getPosInResultPage(pRuntimeEnv->pQueryAttr, page2, pRow2->offset, offset);

  return (in1 != NULL && in2 != NULL) ? supporter->comFunc(in1, in2) : 0;
}

// setup the output buffer for each operator
SSDataBlock* createOutputBuf_rv1(SDataBlockDescNode* pNode) {
  int32_t numOfCols = LIST_LENGTH(pNode->pSlots);

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.numOfCols = numOfCols;
  pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  pBlock->info.blockId = pNode->dataBlockId;
  pBlock->info.rowSize = pNode->totalRowSize;   // todo ??

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData idata = {{0}};
    SSlotDescNode*  pDescNode = nodesListGetNode(pNode->pSlots, i);
    if (!pDescNode->output) {
      continue;
    }

    idata.info.type  = pDescNode->dataType.type;
    idata.info.bytes = pDescNode->dataType.bytes;
    idata.info.scale = pDescNode->dataType.scale;
    idata.info.slotId = pDescNode->slotId;
    idata.info.precision = pDescNode->dataType.precision;

    if (IS_VAR_DATA_TYPE(idata.info.type)) {
      pBlock->info.hasVarCol = true;
    }

    taosArrayPush(pBlock->pDataBlock, &idata);
  }

  return pBlock;
}

static bool isSelectivityWithTagsQuery(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  return true;
  //  bool    hasTags = false;
  //  int32_t numOfSelectivity = 0;
  //
  //  for (int32_t i = 0; i < numOfOutput; ++i) {
  //    int32_t functId = pCtx[i].functionId;
  //    if (functId == FUNCTION_TAG_DUMMY || functId == FUNCTION_TS_DUMMY) {
  //      hasTags = true;
  //      continue;
  //    }
  //
  //    if ((aAggs[functId].status & FUNCSTATE_SELECTIVITY) != 0) {
  //      numOfSelectivity++;
  //    }
  //  }
  //
  //  return (numOfSelectivity > 0 && hasTags);
}

static bool hasNull(SColumn* pColumn, SColumnDataAgg* pStatis) {
  if (TSDB_COL_IS_TAG(pColumn->flag) || TSDB_COL_IS_UD_COL(pColumn->flag) ||
      pColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    return false;
  }

  if (pStatis != NULL && pStatis->numOfNull == 0) {
    return false;
  }

  return true;
}

static void prepareResultListBuffer(SResultRowInfo* pResultRowInfo, jmp_buf env) {
  int64_t newCapacity = 0;

  // more than the capacity, reallocate the resources
  if (pResultRowInfo->size < pResultRowInfo->capacity) {
    return;
  }

  if (pResultRowInfo->capacity > 10000) {
    newCapacity = (int64_t)(pResultRowInfo->capacity * 1.25);
  } else {
    newCapacity = (int64_t)(pResultRowInfo->capacity * 1.5);
  }

  if (newCapacity == pResultRowInfo->capacity) {
    newCapacity += 4;
  }

  pResultRowInfo->pPosition = taosMemoryRealloc(pResultRowInfo->pPosition, newCapacity * sizeof(SResultRowPosition));

  int32_t inc = (int32_t)newCapacity - pResultRowInfo->capacity;
  memset(&pResultRowInfo->pPosition[pResultRowInfo->capacity], 0, sizeof(SResultRowPosition) * inc);
  pResultRowInfo->capacity = (int32_t)newCapacity;
}

static bool chkResultRowFromKey(STaskRuntimeEnv* pRuntimeEnv, SResultRowInfo* pResultRowInfo, char* pData,
                                int16_t bytes, bool masterscan, uint64_t uid) {
  bool existed = false;
  SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, uid);

  SResultRow** p1 =
      (SResultRow**)taosHashGet(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  // in case of repeat scan/reverse scan, no new time window added.
  if (QUERY_IS_INTERVAL_QUERY(pRuntimeEnv->pQueryAttr)) {
    if (!masterscan) {  // the *p1 may be NULL in case of sliding+offset exists.
      return p1 != NULL;
    }

    if (p1 != NULL) {
      if (pResultRowInfo->size == 0) {
        existed = false;
      } else if (pResultRowInfo->size == 1) {
//        existed = (pResultRowInfo->pResult[0] == (*p1));
      } else {  // check if current pResultRowInfo contains the existed pResultRow
        SET_RES_EXT_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, uid, pResultRowInfo);
        int64_t* index =
            taosHashGet(pRuntimeEnv->pResultRowListSet, pRuntimeEnv->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes));
        if (index != NULL) {
          existed = true;
        } else {
          existed = false;
        }
      }
    }

    return existed;
  }

  return p1 != NULL;
}

SResultRow* getNewResultRow_rv(SDiskbasedBuf* pResultBuf, int64_t tableGroupId, int32_t interBufSize) {
  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SIDList list = getDataBufPagesIdList(pResultBuf, tableGroupId);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewBufPage(pResultBuf, tableGroupId, &pageId);
    pData->num = sizeof(SFilePage);
  } else {
    SPageInfo* pi = getLastPageInfo(list);
    pData = getBufPage(pResultBuf, getPageId(pi));
    pageId = getPageId(pi);

    if (pData->num + interBufSize > getBufPageSize(pResultBuf)) {
      // release current page first, and prepare the next one
      releaseBufPageInfo(pResultBuf, pi);

      pData = getNewBufPage(pResultBuf, tableGroupId, &pageId);
      if (pData != NULL) {
        pData->num = sizeof(SFilePage);
      }
    }
  }

  if (pData == NULL) {
    return NULL;
  }

  // set the number of rows in current disk page
  SResultRow* pResultRow = (SResultRow*)((char*)pData + pData->num);
  pResultRow->pageId = pageId;
  pResultRow->offset = (int32_t)pData->num;

  pData->num += interBufSize;

  return pResultRow;
}

static SResultRow* doSetResultOutBufByKey_rv(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo, int64_t tid,
                                             char* pData, int16_t bytes, bool masterscan, uint64_t tableGroupId,
                                             SExecTaskInfo* pTaskInfo, bool isIntervalQuery, SAggSupporter* pSup) {
  bool existInCurrentResusltRowInfo = false;
  SET_RES_WINDOW_KEY(pSup->keyBuf, pData, bytes, tableGroupId);

  SResultRowPosition* p1 = (SResultRowPosition*)taosHashGet(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  // in case of repeat scan/reverse scan, no new time window added.
  if (isIntervalQuery) {
    if (!masterscan) {  // the *p1 may be NULL in case of sliding+offset exists.
      if (p1 != NULL) {
        return getResultRowByPos(pResultBuf, p1);
      } else {
        return NULL;
      }
    }

    if (p1 != NULL) {
      if (pResultRowInfo->size == 0) {
        existInCurrentResusltRowInfo = false;  // this time window created by other timestamp that does not belongs to current table.
        assert(pResultRowInfo->curPos == -1);
      } else if (pResultRowInfo->size == 1) {
        // ASSERT(0);
        SResultRowPosition* p = &pResultRowInfo->pPosition[0];
        existInCurrentResusltRowInfo = (p->pageId == p1->pageId && p->offset == p1->offset);
      } else {  // check if current pResultRowInfo contains the existInCurrentResusltRowInfo pResultRow
        SET_RES_EXT_WINDOW_KEY(pSup->keyBuf, pData, bytes, tid, pResultRowInfo);
        int64_t* index = taosHashGet(pSup->pResultRowListSet, pSup->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes));
        if (index != NULL) {
          // TODO check the scan order for current opened time window
//          pResultRowInfo->curPos = (int32_t)*index;
          existInCurrentResusltRowInfo = true;
        } else {
          existInCurrentResusltRowInfo = false;
        }
      }
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existInCurrentResusltRowInfo in the pResultRowInfo object.
    if (p1 != NULL) {
      return getResultRowByPos(pResultBuf, p1);
    }
  }

  SResultRow* pResult = NULL;
  if (!existInCurrentResusltRowInfo) {
    // 1. close current opened time window
    if (pResultRowInfo->curPos != -1) {  // todo extract function
      SResultRowPosition* pos = &pResultRowInfo->pPosition[pResultRowInfo->curPos];
      SFilePage* pPage = getBufPage(pResultBuf, pos->pageId);
      SResultRow* pRow = (SResultRow*)((char*)pPage + pos->offset);
      closeResultRow(pRow);
      releaseBufPage(pResultBuf, pPage);
    }

    prepareResultListBuffer(pResultRowInfo, pTaskInfo->env);
    if (p1 == NULL) {
      pResult = getNewResultRow_rv(pResultBuf, tableGroupId, pSup->resultRowSize);
      initResultRow(pResult);

      // add a new result set for a new group
      SResultRowPosition pos = {.pageId = pResult->pageId, .offset = pResult->offset};
      taosHashPut(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pos, sizeof(SResultRowPosition));
      SResultRowCell cell = {.groupId = tableGroupId, .pos = pos};
      taosArrayPush(pSup->pResultRowArrayList, &cell);
    } else {
      pResult = getResultRowByPos(pResultBuf, p1);
    }

    // 2. set the new time window to be the new active time window
    pResultRowInfo->curPos = pResultRowInfo->size;
    pResultRowInfo->pPosition[pResultRowInfo->size++] = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};

    int64_t index = pResultRowInfo->curPos;
    SET_RES_EXT_WINDOW_KEY(pSup->keyBuf, pData, bytes, tid, pResultRowInfo);
    taosHashPut(pSup->pResultRowListSet, pSup->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes), &index, POINTER_BYTES);
  } else {
    pResult = getResultRowByPos(pResultBuf, p1);
  }

  // too many time window in query
  if (pResultRowInfo->size > MAX_INTERVAL_TIME_WINDOW) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  return pResult;
}

static void getInitialStartTimeWindow(SInterval* pInterval, int32_t precision, TSKEY ts, STimeWindow* w, TSKEY ekey,
                                      bool ascQuery) {
  if (ascQuery) {
    getAlignQueryTimeWindow(pInterval, precision, ts, ts, ekey, w);
  } else {
    // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
    getAlignQueryTimeWindow(pInterval, precision, ts, ekey, ts, w);

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
static STimeWindow getActiveTimeWindow(SDiskbasedBuf * pBuf, SResultRowInfo* pResultRowInfo, int64_t ts, SInterval* pInterval,
                                       int32_t precision, STimeWindow* win) {
  STimeWindow w = {0};

  if (pResultRowInfo->curPos == -1) {  // the first window, from the previous stored value
    getInitialStartTimeWindow(pInterval, precision, ts, &w, win->ekey, true);
    w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
  } else {
    w = getResultRow(pBuf, pResultRowInfo, pResultRowInfo->curPos)->win;
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

// get the correct time window according to the handled timestamp
static STimeWindow getCurrentActiveTimeWindow(SResultRowInfo* pResultRowInfo, int64_t ts, STaskAttr* pQueryAttr) {
  STimeWindow w = {0};
#if 0
  if (pResultRowInfo->curPos == -1) {  // the first window, from the previous stored value
                                       //    getInitialStartTimeWindow(pQueryAttr, ts, &w);

    if (pQueryAttr->interval.intervalUnit == 'n' || pQueryAttr->interval.intervalUnit == 'y') {
      w.ekey =
          taosTimeAdd(w.skey, pQueryAttr->interval.interval, pQueryAttr->interval.intervalUnit, pQueryAttr->precision) -
          1;
    } else {
      w.ekey = w.skey + pQueryAttr->interval.interval - 1;
    }
  } else {
    w = pRow->win;
  }

  /*
   * query border check, skey should not be bounded by the query time range, since the value skey will
   * be used as the time window index value. So we only change ekey of time window accordingly.
   */
  if (w.ekey > pQueryAttr->window.ekey && QUERY_IS_ASC_QUERY(pQueryAttr)) {
    w.ekey = pQueryAttr->window.ekey;
  }
#endif

  return w;
}

// a new buffer page for each table. Needs to opt this design
static int32_t addNewWindowResultBuf(SResultRow* pWindowRes, SDiskbasedBuf* pResultBuf, int32_t tid, uint32_t size) {
  if (pWindowRes->pageId != -1) {
    return 0;
  }

  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SIDList list = getDataBufPagesIdList(pResultBuf, tid);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewBufPage(pResultBuf, tid, &pageId);
    pData->num = sizeof(SFilePage);
  } else {
    SPageInfo* pi = getLastPageInfo(list);
    pData = getBufPage(pResultBuf, getPageId(pi));
    pageId = getPageId(pi);

    if (pData->num + size > getBufPageSize(pResultBuf)) {
      // release current page first, and prepare the next one
      releaseBufPageInfo(pResultBuf, pi);

      pData = getNewBufPage(pResultBuf, tid, &pageId);
      if (pData != NULL) {
        pData->num = sizeof(SFilePage);
      }
    }
  }

  if (pData == NULL) {
    return -1;
  }

  // set the number of rows in current disk page
  if (pWindowRes->pageId == -1) {  // not allocated yet, allocate new buffer
    pWindowRes->pageId = pageId;
    pWindowRes->offset = (int32_t)pData->num;

    pData->num += size;
    assert(pWindowRes->pageId >= 0);
  }

  return 0;
}

static bool chkWindowOutputBufByKey(STaskRuntimeEnv* pRuntimeEnv, SResultRowInfo* pResultRowInfo, STimeWindow* win,
                                    bool masterscan, SResultRow** pResult, int64_t groupId, SqlFunctionCtx* pCtx,
                                    int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  assert(win->skey <= win->ekey);
  return chkResultRowFromKey(pRuntimeEnv, pResultRowInfo, (char*)&win->skey, TSDB_KEYSIZE, masterscan, groupId);
}

static void setResultRowOutputBufInitCtx_rv(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset);

static int32_t setResultOutputBufByKey_rv(SResultRowInfo* pResultRowInfo, int64_t id, STimeWindow* win, bool masterscan,
                                          SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                          int32_t numOfOutput, int32_t* rowCellInfoOffset, SAggSupporter* pAggSup,
                                          SExecTaskInfo* pTaskInfo) {
  assert(win->skey <= win->ekey);
  SResultRow* pResultRow = doSetResultOutBufByKey_rv(pAggSup->pResultBuf, pResultRowInfo, id, (char*)&win->skey,
                                                     TSDB_KEYSIZE, masterscan, tableGroupId, pTaskInfo, true, pAggSup);

  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // set time window for current result
  pResultRow->win = (*win);
  *pResult = pResultRow;
  setResultRowOutputBufInitCtx_rv(pResultRow, pCtx, numOfOutput, rowCellInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static void setResultRowInterpo(SResultRow* pResult, SResultTsInterpType type) {
  assert(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
  if (type == RESULT_ROW_START_INTERP) {
    pResult->startInterp = true;
  } else {
    pResult->endInterp = true;
  }
}

static bool resultRowInterpolated(SResultRow* pResult, SResultTsInterpType type) {
  assert(pResult != NULL && (type == RESULT_ROW_START_INTERP || type == RESULT_ROW_END_INTERP));
  if (type == RESULT_ROW_START_INTERP) {
    return pResult->startInterp == true;
  } else {
    return pResult->endInterp == true;
  }
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
    int32_t end = searchFn((char*)pData, pos + 1, ekey, order);
    if (end >= 0) {
      forwardStep = pos - end;

      if (pData[end] == ekey) {
        forwardStep += 1;
      }
    }
  }

  assert(forwardStep >= 0);
  return forwardStep;
}

static void doUpdateResultRowIndex(SResultRowInfo* pResultRowInfo, TSKEY lastKey, bool ascQuery, bool timeWindowInterpo) {
  int64_t skey = TSKEY_INITIAL_VAL;
#if 0
  int32_t i = 0;
  for (i = pResultRowInfo->size - 1; i >= 0; --i) {
    SResultRow* pResult = pResultRowInfo->pResult[i];
    if (pResult->closed) {
      break;
    }

    // new closed result rows
    if (timeWindowInterpo) {
      if (pResult->endInterp &&
          ((pResult->win.skey <= lastKey && ascQuery) || (pResult->win.skey >= lastKey && !ascQuery))) {
        if (i > 0) {  // the first time window, the startInterp is false.
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
    if (pResultRowInfo->size == 0) {
      //      assert(pResultRowInfo->current == NULL);
      assert(pResultRowInfo->curPos == -1);
      pResultRowInfo->curPos = -1;
    } else {
      pResultRowInfo->curPos = pResultRowInfo->size - 1;
    }
  } else {
    for (i = pResultRowInfo->size - 1; i >= 0; --i) {
      SResultRow* pResult = pResultRowInfo->pResult[i];
      if (pResult->closed) {
        break;
      }
    }

    if (i == pResultRowInfo->size - 1) {
      pResultRowInfo->curPos = i;
    } else {
      pResultRowInfo->curPos = i + 1;  // current not closed result object
    }
  }
#endif
}

static void updateResultRowInfoActiveIndex(SResultRowInfo* pResultRowInfo, const STimeWindow* pWin, TSKEY lastKey,
                                           bool ascQuery, bool interp) {
  if ((lastKey > pWin->ekey && ascQuery) || (lastKey < pWin->ekey && (!ascQuery))) {
    closeAllResultRows(pResultRowInfo);
    pResultRowInfo->curPos = pResultRowInfo->size - 1;
  } else {
    int32_t step = ascQuery ? 1 : -1;
    doUpdateResultRowIndex(pResultRowInfo, lastKey - step, ascQuery, interp);
  }
}

static int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos,
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
        item->lastKey = pPrimaryColumn[startPos - (num - 1)] + step;
      }
    } else {
      num = startPos + 1;
      if (item != NULL) {
        item->lastKey = pDataBlockInfo->window.skey + step;
      }
    }
  }

  assert(num >= 0);
  return num;
}

//  query_range_start, query_range_end, window_duration, window_start, window_end
static void initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow) {
  pColData->info.type = TSDB_DATA_TYPE_TIMESTAMP;
  pColData->info.bytes = sizeof(int64_t);

  colInfoDataEnsureCapacity(pColData, 5);
  colDataAppendInt64(pColData, 0, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 1, &pQueryWindow->ekey);

  int64_t interval = 0;
  colDataAppendInt64(pColData, 2, &interval);  // this value may be variable in case of 'n' and 'y'.
  colDataAppendInt64(pColData, 3, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 4, &pQueryWindow->ekey);
}

static void updateTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pWin, bool includeEndpoint) {
  int64_t* ts = (int64_t*)pColData->pData;
  int32_t delta = includeEndpoint? 1:0;

  int64_t duration = pWin->ekey - pWin->skey + delta;
  ts[2] = duration;       // set the duration
  ts[3] = pWin->skey;     // window start key
  ts[4] = pWin->ekey + delta; // window end key
}

void doApplyFunctions(SqlFunctionCtx* pCtx, STimeWindow* pWin, SColumnInfoData* pTimeWindowData, int32_t offset, int32_t forwardStep, TSKEY* tsCol,
                             int32_t numOfTotal, int32_t numOfOutput, int32_t order) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    pCtx[k].startTs = pWin->skey;

    // keep it temporarily
    bool    hasAgg      = pCtx[k].input.colDataAggIsSet;
    int32_t numOfRows   = pCtx[k].input.numOfRows;
    int32_t startOffset = pCtx[k].input.startRowIndex;

    int32_t pos = (order == TSDB_ORDER_ASC) ? offset : offset - (forwardStep - 1);
    pCtx[k].input.startRowIndex = pos;
    pCtx[k].input.numOfRows = forwardStep;

    if (tsCol != NULL) {
      pCtx[k].ptsList = tsCol;
    }

    // not a whole block involved in query processing, statistics data can not be used
    // NOTE: the original value of isSet have been changed here
    if (pCtx[k].isAggSet && forwardStep < numOfTotal) {
      pCtx[k].isAggSet = false;
    }

    if (fmIsWindowPseudoColumnFunc(pCtx[k].functionId)) {
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[k]);
      char* p = GET_ROWCELL_INTERBUF(pEntryInfo);

      SColumnInfoData idata = {0};
      idata.info.type  = TSDB_DATA_TYPE_BIGINT;
      idata.info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      idata.pData      = p;

      SScalarParam out = {.columnData = &idata};
      SScalarParam tw = {.numOfRows = 5, .columnData = pTimeWindowData};
      pCtx[k].sfp.process(&tw, 1, &out);
      pEntryInfo->numOfRes = 1;
      continue;
    }

    if (functionNeedToExecute(&pCtx[k])) {
      pCtx[k].fpSet.process(&pCtx[k]);
    }

    // restore it
    pCtx[k].input.colDataAggIsSet = hasAgg;
    pCtx[k].input.startRowIndex = startOffset;
    pCtx[k].input.numOfRows = numOfRows;
  }
}

static int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                      TSKEY* primaryKeys, int32_t prevPosition, STableIntervalOperatorInfo* pInfo) {
  int32_t order = pInfo->order;
  bool    ascQuery = (order == TSDB_ORDER_ASC);

  int32_t precision = pInterval->precision;
  getNextTimeWindow(pInterval, precision, order, pNext);

  // next time window is not in current block
  if ((pNext->skey > pDataBlockInfo->window.ekey && order == TSDB_ORDER_ASC) ||
      (pNext->ekey < pDataBlockInfo->window.skey && order == TSDB_ORDER_DESC)) {
    return -1;
  }

  TSKEY   startKey = ascQuery ? pNext->skey : pNext->ekey;
  int32_t startPos = 0;

  // tumbling time window query, a special case of sliding time window query
  if (pInterval->sliding == pInterval->interval && prevPosition != -1) {
    int32_t factor = GET_FORWARD_DIRECTION_FACTOR(order);
    startPos = prevPosition + factor;
  } else {
    if (startKey <= pDataBlockInfo->window.skey && ascQuery) {
      startPos = 0;
    } else if (startKey >= pDataBlockInfo->window.ekey && !ascQuery) {
      startPos = pDataBlockInfo->rows - 1;
    } else {
      startPos = binarySearchForKey((char*)primaryKeys, pDataBlockInfo->rows, startKey, order);
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

static FORCE_INLINE TSKEY reviseWindowEkey(STaskAttr* pQueryAttr, STimeWindow* pWindow) {
  TSKEY ekey = -1;
  if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
    ekey = pWindow->ekey;
    if (ekey > pQueryAttr->window.ekey) {
      ekey = pQueryAttr->window.ekey;
    }
  } else {
    ekey = pWindow->skey;
    if (ekey < pQueryAttr->window.ekey) {
      ekey = pQueryAttr->window.ekey;
    }
  }

  return ekey;
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

static void saveDataBlockLastRow(char** pRow, SArray* pDataBlock, int32_t rowIndex, int32_t numOfCols) {
  if (pDataBlock == NULL) {
    return;
  }

  for (int32_t k = 0; k < numOfCols; ++k) {
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, k);
    memcpy(pRow[k], ((char*)pColInfo->pData) + (pColInfo->info.bytes * rowIndex), pColInfo->info.bytes);
  }
}

static TSKEY getStartTsKey(STimeWindow* win, const TSKEY* tsCols, int32_t rows, bool ascQuery) {
  TSKEY ts = TSKEY_INITIAL_VAL;
  if (tsCols == NULL) {
    ts = ascQuery ? win->skey : win->ekey;
  } else {
    int32_t offset = ascQuery ? 0 : rows - 1;
    ts = tsCols[offset];
  }

  return ts;
}

static void doSetInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order);

static void doSetInputDataBlockInfo(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock,
                                    int32_t order) {
  for (int32_t i = 0; i < pOperator->numOfOutput; ++i) {
    pCtx[i].order = order;
    pCtx[i].size = pBlock->info.rows;
    pCtx[i].currentStage = (uint8_t)pOperator->pRuntimeEnv->scanFlag;

    setBlockStatisInfo(&pCtx[i], pBlock, NULL /*&pOperator->pExpr[i].base.colInfo*/);
  }
}

void setInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order) {
  if (pBlock->pDataBlock != NULL) {
    doSetInputDataBlock(pOperator, pCtx, pBlock, order);
  } else {
    doSetInputDataBlockInfo(pOperator, pCtx, pBlock, order);
  }
}

static void doSetInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order) {
  for (int32_t i = 0; i < pOperator->numOfOutput; ++i) {
    pCtx[i].order = order;
    pCtx[i].size = pBlock->info.rows;
    pCtx[i].currentStage = MAIN_SCAN;

    SExprInfo* pOneExpr = &pOperator->pExpr[i];
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam *pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        pCtx[i].input.pData[j]  = taosArrayGet(pBlock->pDataBlock, slotId);
        pCtx[i].input.totalRows = pBlock->info.rows;
        pCtx[i].input.numOfRows = pBlock->info.rows;
        pCtx[i].input.startRowIndex = 0;
        ASSERT(pCtx[i].input.pData[j] != NULL);
      }
    }

    //    setBlockStatisInfo(&pCtx[i], pBlock, pOperator->pExpr[i].base.pColumns);
    //      uint32_t flag = pOperator->pExpr[i].base.pParam[0].pCol->flag;
    //      if (TSDB_COL_IS_NORMAL_COL(flag) /*|| (pCtx[i].functionId == FUNCTION_BLKINFO) ||
    //          (TSDB_COL_IS_TAG(flag) && pOperator->pRuntimeEnv->scanFlag == MERGE_STAGE)*/) {

    //        SColumn* pCol = pOperator->pExpr[i].base.pParam[0].pCol;
    //        if (pCtx[i].columnIndex == -1) {
    //          for(int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
    //            SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, j);
    //            if (pColData->info.colId == pCol->colId) {
    //              pCtx[i].columnIndex = j;
    //              break;
    //            }
    //          }
    //        }

    // in case of the block distribution query, the inputBytes is not a constant value.
    //pCtx[i].input.pData[0]  = taosArrayGet(pBlock->pDataBlock, slotId);
    //pCtx[i].input.totalRows = pBlock->info.rows;
    //pCtx[i].input.numOfRows = pBlock->info.rows;
    //pCtx[i].input.startRowIndex = 0;


    //        uint32_t status = aAggs[pCtx[i].functionId].status;
    //        if ((status & (FUNCSTATE_SELECTIVITY | FUNCSTATE_NEED_TS)) != 0) {
    //          SColumnInfoData* tsInfo = taosArrayGet(pBlock->pDataBlock, 0);
    // In case of the top/bottom query again the nest query result, which has no timestamp column
    // don't set the ptsList attribute.
    //          if (tsInfo->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    //            pCtx[i].ptsList = (int64_t*) tsInfo->pData;
    //          } else {
    //            pCtx[i].ptsList = NULL;
    //          }
    //        }
    //      } else if (TSDB_COL_IS_UD_COL(pCol->flag) && (pOperator->pRuntimeEnv->scanFlag == MERGE_STAGE)) {
    //        SColIndex*       pColIndex = &pOperator->pExpr[i].base.colInfo;
    //        SColumnInfoData* p = taosArrayGet(pBlock->pDataBlock, pColIndex->colIndex);
    //
    //        pCtx[i].pInput = p->pData;
    //        assert(p->info.colId == pColIndex->info.colId && pCtx[i].inputType == p->info.type);
    //        for(int32_t j = 0; j < pBlock->info.rows; ++j) {
    //          char* dst = p->pData + j * p->info.bytes;
    //          taosVariantDump(&pOperator->pExpr[i].base.param[1], dst, p->info.type, true);
    //        }
    //      }
  }
}

static void doAggregateImpl(SOperatorInfo* pOperator, TSKEY startTs, SqlFunctionCtx* pCtx) {
  for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
    if (functionNeedToExecute(&pCtx[k])) {
      pCtx[k].startTs = startTs;  // this can be set during create the struct
      pCtx[k].fpSet.process(&pCtx[k]);
    }
  }
}

static void setPseudoOutputColInfo(SSDataBlock* pResult, SqlFunctionCtx* pCtx, SArray* pPseudoList) {
  size_t num = (pPseudoList != NULL)? taosArrayGetSize(pPseudoList):0;
  for (int32_t i = 0; i < num; ++i) {
    pCtx[i].pOutput = taosArrayGet(pResult->pDataBlock, i);
  }
}

void projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList) {
  setPseudoOutputColInfo(pResult, pCtx, pPseudoList);
  pResult->info.groupId = pSrcBlock->info.groupId;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx* pfCtx = &pCtx[k];

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_COLUMN) {  // it is a project query
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      colDataAssign(pColInfoData, pfCtx->input.pData[0], pfCtx->input.numOfRows);

      pResult->info.rows = pSrcBlock->info.rows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
        colDataAppend(pColInfoData, i, taosVariantGet(&pExpr[k].base.pParam[0].param, pExpr[k].base.pParam[0].param.nType), TSDB_DATA_TYPE_NULL == pExpr[k].base.pParam[0].param.nType);
      }
      pResult->info.rows = pSrcBlock->info.rows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_OPERATOR) {
      SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
      taosArrayPush(pBlockList, &pSrcBlock);

      SScalarParam dest = {0};
      dest.columnData = taosArrayGet(pResult->pDataBlock, outputSlotId);

      scalarCalculate(pExpr[k].pExpr->_optrRoot.pRootNode, pBlockList, &dest);
      pResult->info.rows = dest.numOfRows;

      taosArrayDestroy(pBlockList);
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      ASSERT(!fmIsAggFunc(pfCtx->functionId));

      if (fmIsPseudoColumnFunc(pfCtx->functionId)) {
        // do nothing
      } else if (fmIsNonstandardSQLFunc(pfCtx->functionId)) {
        // todo set the correct timestamp column
        pfCtx->input.pPTS = taosArrayGet(pSrcBlock->pDataBlock, 1);

        SResultRowEntryInfo *pResInfo = GET_RES_INFO(&pCtx[k]);
        pfCtx->fpSet.init(&pCtx[k], pResInfo);

        pfCtx->pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        pfCtx->offset  = pResult->info.rows;  // set the start offset

        if (taosArrayGetSize(pPseudoList) > 0) {
          int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
          pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
        }

        int32_t numOfRows = pfCtx->fpSet.process(pfCtx);
        pResult->info.rows += numOfRows;
      } else {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        taosArrayPush(pBlockList, &pSrcBlock);

        SScalarParam dest = {0};
        dest.columnData = taosArrayGet(pResult->pDataBlock, outputSlotId);

        scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest);
        pResult->info.rows = dest.numOfRows;
        taosArrayDestroy(pBlockList);
      }
    } else {
      ASSERT(0);
    }
  }
}

void doTimeWindowInterpolation(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SArray* pDataBlock, TSKEY prevTs,
                               int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey, int32_t type) {
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  SExprInfo*       pExpr = pOperator->pExpr;

  SqlFunctionCtx* pCtx = pInfo->pCtx;

  for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
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
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char*)pRuntimeEnv->prevRow[index]);
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
            pCtx[k].start.ptr = (char*)pRuntimeEnv->prevRow[index];
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

static bool setTimeWindowInterpolationStartTs(SOperatorInfo* pOperatorInfo, SqlFunctionCtx* pCtx, int32_t pos,
                                              int32_t numOfRows, SArray* pDataBlock, const TSKEY* tsCols, STimeWindow* win) {
  bool  ascQuery = true;
  TSKEY curTs = tsCols[pos];
  TSKEY lastTs = 0;//*(TSKEY*)pRuntimeEnv->prevRow[0];

  // lastTs == INT64_MIN and pos == 0 means this is the first time window, interpolation is not needed.
  // start exactly from this point, no need to do interpolation
  TSKEY key = ascQuery ? win->skey : win->ekey;
  if (key == curTs) {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);
    return true;
  }

  if (lastTs == INT64_MIN && ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery))) {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);
    return true;
  }

  int32_t step = 1;//GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
  TSKEY   prevTs = ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery)) ? lastTs : tsCols[pos - step];

  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, prevTs, pos - step, curTs, pos, key, RESULT_ROW_START_INTERP);
  return true;
}

static bool setTimeWindowInterpolationEndTs(SOperatorInfo* pOperatorInfo, SqlFunctionCtx* pCtx, int32_t endRowIndex,
                                            SArray* pDataBlock, const TSKEY* tsCols, TSKEY blockEkey,
                                            STimeWindow* win) {
  int32_t order = TSDB_ORDER_ASC;
  int32_t numOfOutput = pOperatorInfo->numOfOutput;

  TSKEY actualEndKey = tsCols[endRowIndex];
  TSKEY key = order ? win->ekey : win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey /*&& QUERY_IS_ASC_QUERY(pQueryAttr)*/) || (key < blockEkey /*&& !QUERY_IS_ASC_QUERY(pQueryAttr)*/)) {
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
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);
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
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_END_INTERP);
  }
}

static SArray* hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pSDataBlock, int32_t tableGroupId) {
  STableIntervalOperatorInfo* pInfo = (STableIntervalOperatorInfo*)pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  int32_t        numOfOutput = pOperatorInfo->numOfOutput;

  SArray* pUpdated = NULL;
  if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    pUpdated = taosArrayInit(4, sizeof(SResultRowPosition));
  }

  int32_t step = 1;
  bool    ascScan = true;

  int32_t prevIndex = pResultRowInfo->curPos;

  TSKEY* tsCols = NULL;
  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
//    assert(tsCols[0] == pSDataBlock->info.window.skey && tsCols[pSDataBlock->info.rows - 1] ==
//           pSDataBlock->info.window.ekey);
  }

  int32_t startPos = ascScan? 0 : (pSDataBlock->info.rows - 1);
  TSKEY   ts = getStartTsKey(&pSDataBlock->info.window, tsCols, pSDataBlock->info.rows, ascScan);

  STimeWindow win = getActiveTimeWindow(pInfo->aggSup.pResultBuf, pResultRowInfo, ts, &pInfo->interval, pInfo->interval.precision, &pInfo->win);
  bool        masterScan = true;

  SResultRow* pResult = NULL;
  int32_t     ret = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult,
                                               tableGroupId, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset,
                                               &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    SResultRowPosition pos = {.pageId = pResult->pageId, .offset = pResult->offset};
    taosArrayPush(pUpdated, &pos);
  }

  int32_t forwardStep = 0;
  TSKEY   ekey = win.ekey;
  forwardStep =
      getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);

  // prev time window not interpolation yet.
  int32_t curIndex = pResultRowInfo->curPos;

#if 0
  if (prevIndex != -1 && prevIndex < curIndex && pInfo->timeWindowInterpo) {
    for (int32_t j = prevIndex; j < curIndex; ++j) {  // previous time window may be all closed already.
      SResultRow* pRes = getResultRow(pResultRowInfo, j);
      if (pRes->closed) {
        assert(resultRowInterpolated(pRes, RESULT_ROW_START_INTERP) && resultRowInterpolated(pRes, RESULT_ROW_END_INTERP));
        continue;
      }

      STimeWindow w = pRes->win;
      ret = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &w, masterScan, &pResult, tableGroupId,
                                       pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup,
                                       pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      assert(!resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));
      doTimeWindowInterpolation(pOperatorInfo, &pInfo->binfo, pSDataBlock->pDataBlock, *(TSKEY*)pInfo->pRow[0], -1,
                                tsCols[startPos], startPos, w.ekey, RESULT_ROW_END_INTERP);

      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
      setNotInterpoWindowKey(pInfo->binfo.pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);

      doApplyFunctions(pInfo->binfo.pCtx, &w, &pInfo->timeWindowData, startPos, 0, tsCols, pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
    }

    // restore current time window
    ret = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult, tableGroupId,
                                     pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup,
                                     pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }
#endif

  // window start key interpolation
  doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &win, startPos, forwardStep, pInfo->order, false);

  updateTimeWindowInfo(&pInfo->timeWindowData, &win, true);
  doApplyFunctions(pInfo->binfo.pCtx, &win, &pInfo->timeWindowData, startPos, forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, pInfo);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    int32_t code = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &nextWin, masterScan, &pResult,
                                              tableGroupId, pInfo->binfo.pCtx, numOfOutput,
                                              pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
      SResultRowPosition pos = {.pageId = pResult->pageId, .offset = pResult->offset};
      taosArrayPush(pUpdated, &pos);
    }

    ekey = nextWin.ekey;  // reviseWindowEkey(pQueryAttr, &nextWin);
    forwardStep =
        getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);

    // window start(end) key interpolation
    doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &nextWin, startPos, forwardStep,
                                pInfo->order, false);

    updateTimeWindowInfo(&pInfo->timeWindowData, &nextWin, true);
    doApplyFunctions(pInfo->binfo.pCtx, &nextWin, &pInfo->timeWindowData, startPos, forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
  }

  if (pInfo->timeWindowInterpo) {
    int32_t rowIndex = ascScan ? (pSDataBlock->info.rows - 1) : 0;
    saveDataBlockLastRow(pInfo->pRow, pSDataBlock->pDataBlock, rowIndex, pSDataBlock->info.numOfCols);
  }

  return pUpdated;
  //  updateResultRowInfoActiveIndex(pResultRowInfo, &pInfo->win, pRuntimeEnv->current->lastKey, true, false);
}

static void hashAllIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pSDataBlock,
                               int32_t tableGroupId) {
  STableIntervalOperatorInfo* pInfo = (STableIntervalOperatorInfo*)pOperatorInfo->info;

  STaskRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  int32_t          numOfOutput = pOperatorInfo->numOfOutput;

  int32_t step = 1;//GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
  bool    ascQuery = true;

  TSKEY* tsCols = NULL;
  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, 0);
    tsCols = (int64_t*)pColDataInfo->pData;
    assert(tsCols[0] == pSDataBlock->info.window.skey &&
           tsCols[pSDataBlock->info.rows - 1] == pSDataBlock->info.window.ekey);
  }

  int32_t startPos = ascQuery ? 0 : (pSDataBlock->info.rows - 1);
  TSKEY   ts = getStartTsKey(&pSDataBlock->info.window, tsCols, pSDataBlock->info.rows, ascQuery);

  STimeWindow win = {0};//getCurrentActiveTimeWindow(pResultRowInfo, ts, pQueryAttr);
  bool        masterScan = IS_MAIN_SCAN(pRuntimeEnv);

  SResultRow* pResult = NULL;
  int32_t     forwardStep = 0;
  int32_t     ret = 0;
  STimeWindow preWin = win;

  while (1) {
    // null data, failed to allocate more memory buffer
//    ret = setResultOutputBufByKey(pRuntimeEnv, pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult,
//                                  tableGroupId, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    TSKEY ekey = 0;//reviseWindowEkey(pQueryAttr, &win);
    //    forwardStep = getNumOfRowsInTimeWindow(pRuntimeEnv, &pSDataBlock->info, tsCols, startPos, ekey,
    //    binarySearchForKey, true);

    // window start(end) key interpolation
    //    doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &win, startPos,
    //    forwardStep); doApplyFunctions(pRuntimeEnv, pInfo->binfo.pCtx, ascQuery ? &win : &preWin, startPos,
    //    forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput);
    preWin = win;

    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
    //    startPos = getNextQualifiedWindow(pQueryAttr, &win, &pSDataBlock->info, tsCols, binarySearchForKey,
    //    prevEndPos);
    if (startPos < 0) {
//      if ((ascQuery && win.skey <= pQueryAttr->window.ekey) || ((!ascQuery) && win.ekey >= pQueryAttr->window.ekey)) {
//        int32_t code =
//            setResultOutputBufByKey(pRuntimeEnv, pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult,
//                                    tableGroupId, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset);
//        if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
//          longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
//        }
//
//        startPos = pSDataBlock->info.rows - 1;

        // window start(end) key interpolation
        //        doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &win, startPos,
        //        forwardStep); doApplyFunctions(pRuntimeEnv, pInfo->binfo.pCtx, ascQuery ? &win : &preWin, startPos,
        //        forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput);
//      }

      break;
    }
    setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
  }

//  if (pQueryAttr->timeWindowInterpo) {
//    int32_t rowIndex = ascQuery ? (pSDataBlock->info.rows - 1) : 0;
    //    saveDataBlockLastRow(pRuntimeEnv, &pSDataBlock->info, pSDataBlock->pDataBlock, rowIndex);
//  }

  //  updateResultRowInfoActiveIndex(pResultRowInfo, pQueryAttr, pRuntimeEnv->current->lastKey);
}

static void doKeepTuple(SWindowRowsSup* pRowSup, int64_t ts) {
  pRowSup->win.ekey  = ts;
  pRowSup->prevTs    = ts;
  pRowSup->numOfRows += 1;
}

static void doKeepNewWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, int32_t rowIndex) {
  pRowSup->startRowIndex = rowIndex;
  pRowSup->numOfRows     = 0;
  pRowSup->win.skey      = tsList[rowIndex];
}

// todo handle multiple tables cases.
static void doSessionWindowAggImpl(SOperatorInfo* pOperator, SSessionAggOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  // todo find the correct time stamp column slot
  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, 0);

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->numOfOutput;
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
      int32_t ret = setResultOutputBufByKey_rv(&pInfo->binfo.resultRowInfo, pBlock->info.uid, &window, masterScan,
                                               &pResult, gid, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      // pInfo->numOfRows data belong to the current session window
      updateTimeWindowInfo(&pInfo->timeWindowData, &window, false);
      doApplyFunctions(pInfo->binfo.pCtx, &window, &pInfo->timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = setResultOutputBufByKey_rv(&pInfo->binfo.resultRowInfo, pBlock->info.uid, &pRowSup->win, masterScan, &pResult,
                                           gid, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pInfo->binfo.pCtx, &pRowSup->win, &pInfo->timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
}

static void setResultRowKey(SResultRow* pResultRow, char* pData, int16_t type) {
  if (IS_VAR_DATA_TYPE(type)) {
    // todo disable this
//    if (pResultRow->key == NULL) {
//      pResultRow->key = taosMemoryMalloc(varDataTLen(pData));
//      varDataCopy(pResultRow->key, pData);
//    } else {
//      ASSERT(memcmp(pResultRow->key, pData, varDataTLen(pData)) == 0);
//    }
  } else {
    int64_t v = -1;
    GET_TYPED_DATA(v, int64_t, type, pData);

    pResultRow->win.skey = v;
    pResultRow->win.ekey = v;
  }
}

int32_t setGroupResultOutputBuf_rv(SOptrBasicInfo* binfo, int32_t numOfCols, char* pData, int16_t type,
                                          int16_t bytes, int32_t groupId, SDiskbasedBuf* pBuf, SExecTaskInfo* pTaskInfo,
                                          SAggSupporter* pAggSup) {
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = binfo->pCtx;

  SResultRow* pResultRow = doSetResultOutBufByKey_rv(pBuf, pResultRowInfo, groupId, (char*)pData, bytes, true, groupId,
                                                     pTaskInfo, false, pAggSup);
  assert(pResultRow != NULL);

  setResultRowKey(pResultRow, pData, type);
  setResultRowOutputBufInitCtx_rv(pResultRow, pCtx, numOfCols, binfo->rowCellInfoOffset);
  return TSDB_CODE_SUCCESS;
}

static bool functionNeedToExecute(SqlFunctionCtx* pCtx) {
  struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  // in case of timestamp column, always generated results.
  int32_t functionId = pCtx->functionId;
  if (functionId == -1) {
    return false;
  }

  if (isRowEntryCompleted(pResInfo)) {
    return false;
  }

  if (functionId == FUNCTION_FIRST_DST || functionId == FUNCTION_FIRST) {
    //    return QUERY_IS_ASC_QUERY(pQueryAttr);
  }

  // denote the order type
  if ((functionId == FUNCTION_LAST_DST || functionId == FUNCTION_LAST)) {
    //    return pCtx->param[0].i == pQueryAttr->order.order;
  }

  // in the reverse table scan, only the following functions need to be executed
  //  if (IS_REVERSE_SCAN(pRuntimeEnv) ||
  //      (pRuntimeEnv->scanFlag == REPEAT_SCAN && functionId != FUNCTION_STDDEV && functionId != FUNCTION_PERCT)) {
  //    return false;
  //  }

  return true;
}

void setBlockStatisInfo(SqlFunctionCtx* pCtx, SSDataBlock* pSDataBlock, SColumn* pColumn) {
  SColumnDataAgg* pAgg = NULL;

  if (pSDataBlock->pBlockAgg != NULL && TSDB_COL_IS_NORMAL_COL(pColumn->flag)) {
    pAgg = &pSDataBlock->pBlockAgg[pCtx->columnIndex];

    pCtx->agg = *pAgg;
    pCtx->isAggSet = true;
    assert(pCtx->agg.numOfNull <= pSDataBlock->info.rows);
  } else {
    pCtx->isAggSet = false;
  }

  pCtx->hasNull = hasNull(pColumn, pAgg);

  // set the statistics data for primary time stamp column
  if (pCtx->functionId == FUNCTION_SPREAD && pColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pCtx->isAggSet = true;
    pCtx->agg.min = pSDataBlock->info.window.skey;
    pCtx->agg.max = pSDataBlock->info.window.ekey;
  }
}

// set the output buffer for the selectivity + tag query
static int32_t setCtxTagColumnInfo(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  if (!isSelectivityWithTagsQuery(pCtx, numOfOutput)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t num = 0;
  int16_t tagLen = 0;

  SqlFunctionCtx*  p = NULL;
  SqlFunctionCtx** pTagCtx = taosMemoryCalloc(numOfOutput, POINTER_BYTES);
  if (pTagCtx == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    int32_t functionId = pCtx[i].functionId;

    if (functionId == FUNCTION_TAG_DUMMY || functionId == FUNCTION_TS_DUMMY) {
      tagLen += pCtx[i].resDataInfo.bytes;
      pTagCtx[num++] = &pCtx[i];
    } else if (1 /*(aAggs[functionId].status & FUNCSTATE_SELECTIVITY) != 0*/) {
      p = &pCtx[i];
    } else if (functionId == FUNCTION_TS || functionId == FUNCTION_TAG) {
      // tag function may be the group by tag column
      // ts may be the required primary timestamp column
      continue;
    } else {
      // the column may be the normal column, group by normal_column, the functionId is FUNCTION_PRJ
    }
  }
  if (p != NULL) {
    p->subsidiaryRes.pCtx = pTagCtx;
    p->subsidiaryRes.numOfCols = num;
    p->subsidiaryRes.bufLen = tagLen;
  } else {
    taosMemoryFreeClear(pTagCtx);
  }

  return TSDB_CODE_SUCCESS;
}

SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowCellInfoOffset) {
  SqlFunctionCtx* pFuncCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pFuncCtx == NULL) {
    return NULL;
  }

  *rowCellInfoOffset = taosMemoryCalloc(numOfOutput, sizeof(int32_t));
  if (*rowCellInfoOffset == 0) {
    taosMemoryFreeClear(pFuncCtx);
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = &pExprInfo[i];

    SExprBasicInfo* pFunct = &pExpr->base;
    SqlFunctionCtx* pCtx = &pFuncCtx[i];

    pCtx->functionId = -1;
    if (pExpr->pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SFuncExecEnv env = {0};
      pCtx->functionId = pExpr->pExpr->_function.pFunctNode->funcId;

      if (fmIsAggFunc(pCtx->functionId) || fmIsNonstandardSQLFunc(pCtx->functionId)) {
        fmGetFuncExecFuncs(pCtx->functionId, &pCtx->fpSet);
        pCtx->fpSet.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
      } else {
        fmGetScalarFuncExecFuncs(pCtx->functionId, &pCtx->sfp);
        if (pCtx->sfp.getEnv != NULL) {
          pCtx->sfp.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
        }
      }
      pCtx->resDataInfo.interBufSize = env.calcMemSize;
    } else if (pExpr->pExpr->nodeType == QUERY_NODE_COLUMN || pExpr->pExpr->nodeType == QUERY_NODE_OPERATOR || pExpr->pExpr->nodeType == QUERY_NODE_VALUE) {
      pCtx->resDataInfo.interBufSize = pFunct->resSchema.bytes; // for simple column, the intermediate buffer needs to hold one element.
    }

    pCtx->input.numOfInputCols = pFunct->numOfParams;
    pCtx->input.pData = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);
    pCtx->input.pColumnDataAgg = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);

    pCtx->pTsOutput         = NULL;
    pCtx->resDataInfo.bytes = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type  = pFunct->resSchema.type;
    pCtx->order             = TSDB_ORDER_ASC;
    pCtx->start.key         = INT64_MIN;
    pCtx->end.key           = INT64_MIN;
#if 0
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
//      int16_t type = pFunct->param[j].nType;
//      int16_t bytes = pFunct->param[j].nLen;

//      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
//        taosVariantCreateFromBinary(&pCtx->param[j], pFunct->param[j].pz, bytes, type);
//      } else {
//        taosVariantCreateFromBinary(&pCtx->param[j], (char *)&pFunct->param[j].i, bytes, type);
//      }
    }

    // set the order information for top/bottom query
    int32_t functionId = pCtx->functionId;
    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF) {
      int32_t f = getExprFunctionId(&pExpr[0]);
      assert(f == FUNCTION_TS || f == FUNCTION_TS_DUMMY);

//      pCtx->param[2].i = pQueryAttr->order.order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

//      pCtx->param[1].i = pQueryAttr->order.col.info.colId;
    } else if (functionId == FUNCTION_INTERP) {
//      pCtx->param[2].i = (int8_t)pQueryAttr->fillType;
//      if (pQueryAttr->fillVal != NULL) {
//        if (isNull((const char *)&pQueryAttr->fillVal[i], pCtx->inputType)) {
//          pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
//        } else {  // todo refactor, taosVariantCreateFromBinary should handle the NULL value
//          if (pCtx->inputType != TSDB_DATA_TYPE_BINARY && pCtx->inputType != TSDB_DATA_TYPE_NCHAR) {
//            taosVariantCreateFromBinary(&pCtx->param[1], (char *)&pQueryAttr->fillVal[i], pCtx->inputBytes, pCtx->inputType);
//          }
//        }
//      }
    } else if (functionId == FUNCTION_TS_COMP) {
//      pCtx->param[0].i = pQueryAttr->vgId;  //TODO this should be the parameter from client
      pCtx->param[0].nType = TSDB_DATA_TYPE_BIGINT;
    } else if (functionId == FUNCTION_TWA) {
//      pCtx->param[1].i = pQueryAttr->window.skey;
      pCtx->param[1].nType = TSDB_DATA_TYPE_BIGINT;
//      pCtx->param[2].i = pQueryAttr->window.ekey;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
    } else if (functionId == FUNCTION_ARITHM) {
//      pCtx->param[1].pz = (char*) getScalarFuncSupport(pRuntimeEnv->scalarSup, i);
    }
#endif
  }

  for (int32_t i = 1; i < numOfOutput; ++i) {
    (*rowCellInfoOffset)[i] =
        (int32_t)((*rowCellInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) + pFuncCtx[i - 1].resDataInfo.interBufSize);
  }

  setCtxTagColumnInfo(pFuncCtx, numOfOutput);
  return pFuncCtx;
}

static void* destroySqlFunctionCtx(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  if (pCtx == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    for (int32_t j = 0; j < pCtx[i].numOfParams; ++j) {
      taosVariantDestroy(&pCtx[i].param[j]);
    }

    taosVariantDestroy(&pCtx[i].tag);
    taosMemoryFreeClear(pCtx[i].subsidiaryRes.pCtx);
  }

  taosMemoryFreeClear(pCtx);
  return NULL;
}

bool isTaskKilled(SExecTaskInfo* pTaskInfo) {
  // query has been executed more than tsShellActivityTimer, and the retrieve has not arrived
  // abort current query execution.
  if (pTaskInfo->owner != 0 &&
      ((taosGetTimestampSec() - pTaskInfo->cost.start / 1000) > 10 * getMaximumIdleDurationSec())
      /*(!needBuildResAfterQueryComplete(pTaskInfo))*/) {
    assert(pTaskInfo->cost.start != 0);
    //    qDebug("QInfo:%" PRIu64 " retrieve not arrive beyond %d ms, abort current query execution, start:%" PRId64
    //           ", current:%d", pQInfo->qId, 1, pQInfo->startExecTs, taosGetTimestampSec());
    //    return true;
  }

  return false;
}

void setTaskKilled(SExecTaskInfo* pTaskInfo) { pTaskInfo->code = TSDB_CODE_TSC_QUERY_CANCELLED; }

static bool isCachedLastQuery(STaskAttr* pQueryAttr) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functionId = getExprFunctionId(&pQueryAttr->pExpr1[i]);
    if (functionId == FUNCTION_LAST || functionId == FUNCTION_LAST_DST) {
      continue;
    }

    return false;
  }

  if (pQueryAttr->order.order != TSDB_ORDER_DESC || !TSWINDOW_IS_EQUAL(pQueryAttr->window, TSWINDOW_DESC_INITIALIZER)) {
    return false;
  }

  if (pQueryAttr->groupbyColumn) {
    return false;
  }

  if (pQueryAttr->interval.interval > 0) {
    return false;
  }

  if (pQueryAttr->numOfFilterCols > 0 || pQueryAttr->havingNum > 0) {
    return false;
  }

  return true;
}

/////////////////////////////////////////////////////////////////////////////////////////////
// todo refactor : return window
void getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, int64_t keyFirst, int64_t keyLast,
                             STimeWindow* win) {
  ASSERT(key >= keyFirst && key <= keyLast);
  win->skey = taosTimeTruncate(key, pInterval, precision);

  /*
   * if the realSkey > INT64_MAX - pInterval->interval, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  if (keyFirst > (INT64_MAX - pInterval->interval)) {
    assert(keyLast - keyFirst < pInterval->interval);
    win->ekey = INT64_MAX;
  } else {
    win->ekey = taosTimeAdd(win->skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
  }
}

static int32_t updateBlockLoadStatus(STaskAttr* pQuery, int32_t status) {
  bool hasFirstLastFunc = false;
  bool hasOtherFunc = false;

  if (status == BLK_DATA_ALL_NEEDED || status == BLK_DATA_DISCARD) {
    return status;
  }

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    int32_t functionId = getExprFunctionId(&pQuery->pExpr1[i]);

    if (functionId == FUNCTION_TS || functionId == FUNCTION_TS_DUMMY || functionId == FUNCTION_TAG ||
        functionId == FUNCTION_TAG_DUMMY) {
      continue;
    }

    if (functionId == FUNCTION_FIRST_DST || functionId == FUNCTION_LAST_DST) {
      hasFirstLastFunc = true;
    } else {
      hasOtherFunc = true;
    }
  }

  if (hasFirstLastFunc && status == BLK_DATA_NO_NEEDED) {
    if (!hasOtherFunc) {
      return BLK_DATA_DISCARD;
    } else {
      return BLK_DATA_ALL_NEEDED;
    }
  }

  return status;
}

static void doUpdateLastKey(STaskAttr* pQueryAttr) {
  STimeWindow* win = &pQueryAttr->window;

  size_t num = taosArrayGetSize(pQueryAttr->tableGroupInfo.pGroupList);
  for (int32_t i = 0; i < num; ++i) {
    SArray* p1 = taosArrayGetP(pQueryAttr->tableGroupInfo.pGroupList, i);

    size_t len = taosArrayGetSize(p1);
    for (int32_t j = 0; j < len; ++j) {
      //      STableKeyInfo* pInfo = taosArrayGet(p1, j);
      //
      //      // update the new lastkey if it is equalled to the value of the old skey
      //      if (pInfo->lastKey == win->ekey) {
      //        pInfo->lastKey = win->skey;
      //      }
    }
  }
}

// static void updateDataCheckOrder(SQInfo *pQInfo, SQueryTableReq* pQueryMsg, bool stableQuery) {
//   STaskAttr* pQueryAttr = pQInfo->runtimeEnv.pQueryAttr;
//
//   // in case of point-interpolation query, use asc order scan
//   char msg[] = "QInfo:0x%"PRIx64" scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%"
//   PRId64
//                "-%" PRId64 ", new qrange:%" PRId64 "-%" PRId64;
//
//   // todo handle the case the the order irrelevant query type mixed up with order critical query type
//   // descending order query for last_row query
//   if (isFirstLastRowQuery(pQueryAttr)) {
//     //qDebug("QInfo:0x%"PRIx64" scan order changed for last_row query, old:%d, new:%d", pQInfo->qId,
//     pQueryAttr->order.order, TSDB_ORDER_ASC);
//
//     pQueryAttr->order.order = TSDB_ORDER_ASC;
//     if (pQueryAttr->window.skey > pQueryAttr->window.ekey) {
//       TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//     }
//
//     pQueryAttr->needReverseScan = false;
//     return;
//   }
//
//   if (pQueryAttr->groupbyColumn && pQueryAttr->order.order == TSDB_ORDER_DESC) {
//     pQueryAttr->order.order = TSDB_ORDER_ASC;
//     if (pQueryAttr->window.skey > pQueryAttr->window.ekey) {
//       TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//     }
//
//     pQueryAttr->needReverseScan = false;
//     doUpdateLastKey(pQueryAttr);
//     return;
//   }
//
//   if (pQueryAttr->pointInterpQuery && pQueryAttr->interval.interval == 0) {
//     if (!QUERY_IS_ASC_QUERY(pQueryAttr)) {
//       //qDebug(msg, pQInfo->qId, "interp", pQueryAttr->order.order, TSDB_ORDER_ASC, pQueryAttr->window.skey,
//       pQueryAttr->window.ekey, pQueryAttr->window.ekey, pQueryAttr->window.skey); TSWAP(pQueryAttr->window.skey,
//       pQueryAttr->window.ekey, TSKEY);
//     }
//
//     pQueryAttr->order.order = TSDB_ORDER_ASC;
//     return;
//   }
//
//   if (pQueryAttr->interval.interval == 0) {
//     if (onlyFirstQuery(pQueryAttr)) {
//       if (!QUERY_IS_ASC_QUERY(pQueryAttr)) {
//         //qDebug(msg, pQInfo->qId, "only-first", pQueryAttr->order.order, TSDB_ORDER_ASC, pQueryAttr->window.skey,
////               pQueryAttr->window.ekey, pQueryAttr->window.ekey, pQueryAttr->window.skey);
//
//        TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//        doUpdateLastKey(pQueryAttr);
//      }
//
//      pQueryAttr->order.order = TSDB_ORDER_ASC;
//      pQueryAttr->needReverseScan = false;
//    } else if (onlyLastQuery(pQueryAttr) && notContainSessionOrStateWindow(pQueryAttr)) {
//      if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//        //qDebug(msg, pQInfo->qId, "only-last", pQueryAttr->order.order, TSDB_ORDER_DESC, pQueryAttr->window.skey,
////               pQueryAttr->window.ekey, pQueryAttr->window.ekey, pQueryAttr->window.skey);
//
//        TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//        doUpdateLastKey(pQueryAttr);
//      }
//
//      pQueryAttr->order.order = TSDB_ORDER_DESC;
//      pQueryAttr->needReverseScan = false;
//    }
//
//  } else {  // interval query
//    if (stableQuery) {
//      if (onlyFirstQuery(pQueryAttr)) {
//        if (!QUERY_IS_ASC_QUERY(pQueryAttr)) {
//          //qDebug(msg, pQInfo->qId, "only-first stable", pQueryAttr->order.order, TSDB_ORDER_ASC,
////                 pQueryAttr->window.skey, pQueryAttr->window.ekey, pQueryAttr->window.ekey,
/// pQueryAttr->window.skey);
//
//          TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//          doUpdateLastKey(pQueryAttr);
//        }
//
//        pQueryAttr->order.order = TSDB_ORDER_ASC;
//        pQueryAttr->needReverseScan = false;
//      } else if (onlyLastQuery(pQueryAttr)) {
//        if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//          //qDebug(msg, pQInfo->qId, "only-last stable", pQueryAttr->order.order, TSDB_ORDER_DESC,
////                 pQueryAttr->window.skey, pQueryAttr->window.ekey, pQueryAttr->window.ekey,
/// pQueryAttr->window.skey);
//
//          TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//          doUpdateLastKey(pQueryAttr);
//        }
//
//        pQueryAttr->order.order = TSDB_ORDER_DESC;
//        pQueryAttr->needReverseScan = false;
//      }
//    }
//  }
//}

static void getIntermediateBufInfo(STaskRuntimeEnv* pRuntimeEnv, int32_t* ps, int32_t* rowsize) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t    MIN_ROWS_PER_PAGE = 4;

  *rowsize = (int32_t)(pQueryAttr->resultRowSize *
                       getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery));
  int32_t overhead = sizeof(SFilePage);

  // one page contains at least two rows
  *ps = DEFAULT_INTERN_BUF_PAGE_SIZE;
  while (((*rowsize) * MIN_ROWS_PER_PAGE) > (*ps) - overhead) {
    *ps = ((*ps) << 1u);
  }
}

#define IS_PREFILTER_TYPE(_t) ((_t) != TSDB_DATA_TYPE_BINARY && (_t) != TSDB_DATA_TYPE_NCHAR)

// static FORCE_INLINE bool doFilterByBlockStatistics(STaskRuntimeEnv* pRuntimeEnv, SDataStatis *pDataStatis,
// SqlFunctionCtx *pCtx, int32_t numOfRows) {
//   STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
//
//   if (pDataStatis == NULL || pQueryAttr->pFilters == NULL) {
//     return true;
//   }
//
//   return filterRangeExecute(pQueryAttr->pFilters, pDataStatis, pQueryAttr->numOfCols, numOfRows);
// }

static bool overlapWithTimeWindow(STaskAttr* pQueryAttr, SDataBlockInfo* pBlockInfo) {
  STimeWindow w = {0};

  TSKEY sk = TMIN(pQueryAttr->window.skey, pQueryAttr->window.ekey);
  TSKEY ek = TMAX(pQueryAttr->window.skey, pQueryAttr->window.ekey);

  if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
    //    getAlignQueryTimeWindow(pQueryAttr, pBlockInfo->window.skey, sk, ek, &w);
    assert(w.ekey >= pBlockInfo->window.skey);

    if (w.ekey < pBlockInfo->window.ekey) {
      return true;
    }

    while (1) {
      //      getNextTimeWindow(pQueryAttr, &w);
      if (w.skey > pBlockInfo->window.ekey) {
        break;
      }

      assert(w.ekey > pBlockInfo->window.ekey);
      if (w.skey <= pBlockInfo->window.ekey && w.skey > pBlockInfo->window.skey) {
        return true;
      }
    }
  } else {
    //    getAlignQueryTimeWindow(pQueryAttr, pBlockInfo->window.ekey, sk, ek, &w);
    assert(w.skey <= pBlockInfo->window.ekey);

    if (w.skey > pBlockInfo->window.skey) {
      return true;
    }

    while (1) {
      //      getNextTimeWindow(pQueryAttr, &w);
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

static int32_t doTSJoinFilter(STaskRuntimeEnv* pRuntimeEnv, TSKEY key, bool ascQuery) {
  STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);

#if defined(_DEBUG_VIEW)
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64 ", tag:%" PRIu64
         ", query order:%d, ts order:%d, traverse:%d, index:%d\n",
         elem.ts, key, elem.tag.i, pQueryAttr->order.order, pRuntimeEnv->pTsBuf->tsOrder,
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

bool doFilterDataBlock(SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols, int32_t numOfRows, int8_t* p) {
  bool all = true;

  for (int32_t i = 0; i < numOfRows; ++i) {
    bool qualified = false;

    for (int32_t k = 0; k < numOfFilterCols; ++k) {
      char* pElem = (char*)pFilterInfo[k].pData + pFilterInfo[k].info.bytes * i;

      qualified = false;
      for (int32_t j = 0; j < pFilterInfo[k].numOfFilters; ++j) {
        SColumnFilterElem* pFilterElem = NULL;
        //        SColumnFilterElem* pFilterElem = &pFilterInfo[k].pFilters[j];

        bool isnull = isNull(pElem, pFilterInfo[k].info.type);
        if (isnull) {
          //          if (pFilterElem->fp == isNullOperator) {
          //            qualified = true;
          //            break;
          //          } else {
          //            continue;
          //          }
        } else {
          //          if (pFilterElem->fp == notNullOperator) {
          //            qualified = true;
          //            break;
          //          } else if (pFilterElem->fp == isNullOperator) {
          //            continue;
          //          }
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

  return all;
}

void doCompactSDataBlock(SSDataBlock* pBlock, int32_t numOfRows, int8_t* p) {
  int32_t len = 0;
  int32_t start = 0;
  for (int32_t j = 0; j < numOfRows; ++j) {
    if (p[j] == 1) {
      len++;
    } else {
      if (len > 0) {
        int32_t cstart = j - len;
        for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
          SColumnInfoData* pColumnInfoData = taosArrayGet(pBlock->pDataBlock, i);

          int16_t bytes = pColumnInfoData->info.bytes;
          memmove(((char*)pColumnInfoData->pData) + start * bytes, pColumnInfoData->pData + cstart * bytes,
                  len * bytes);
        }

        start += len;
        len = 0;
      }
    }
  }

  if (len > 0) {
    int32_t cstart = numOfRows - len;
    for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
      SColumnInfoData* pColumnInfoData = taosArrayGet(pBlock->pDataBlock, i);

      int16_t bytes = pColumnInfoData->info.bytes;
      memmove(pColumnInfoData->pData + start * bytes, pColumnInfoData->pData + cstart * bytes, len * bytes);
    }

    start += len;
    len = 0;
  }

  pBlock->info.rows = start;
  pBlock->pBlockAgg = NULL;  // clean the block statistics info

  if (start > 0) {
    SColumnInfoData* pColumnInfoData = taosArrayGet(pBlock->pDataBlock, 0);
    if (pColumnInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP &&
        pColumnInfoData->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pBlock->info.window.skey = *(int64_t*)pColumnInfoData->pData;
      pBlock->info.window.ekey = *(int64_t*)(pColumnInfoData->pData + TSDB_KEYSIZE * (start - 1));
    }
  }
}

void filterRowsInDataBlock(STaskRuntimeEnv* pRuntimeEnv, SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols,
                           SSDataBlock* pBlock, bool ascQuery) {
  int32_t numOfRows = pBlock->info.rows;

  int8_t* p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  bool    all = true;
#if 0
  if (pRuntimeEnv->pTsBuf != NULL) {
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

    // save the cursor status
    pRuntimeEnv->current->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
  } else {
    all = doFilterDataBlock(pFilterInfo, numOfFilterCols, numOfRows, p);
  }
#endif

  if (!all) {
    doCompactSDataBlock(pBlock, numOfRows, p);
  }

  taosMemoryFreeClear(p);
}

void filterColRowsInDataBlock(STaskRuntimeEnv* pRuntimeEnv, SSDataBlock* pBlock, bool ascQuery) {
  int32_t numOfRows = pBlock->info.rows;

  int8_t* p = NULL;
  bool    all = true;

  if (pRuntimeEnv->pTsBuf != NULL) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, 0);
    p = taosMemoryCalloc(numOfRows, sizeof(int8_t));

    TSKEY* k = (TSKEY*)pColInfoData->pData;
    for (int32_t i = 0; i < numOfRows; ++i) {
      int32_t offset = ascQuery ? i : (numOfRows - i - 1);
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

    // save the cursor status
    //   pRuntimeEnv->current->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
  } else {
    //   all = filterExecute(pRuntimeEnv->pQueryAttr->pFilters, numOfRows, &p, pBlock->pBlockAgg,
    //   pRuntimeEnv->pQueryAttr->numOfCols);
  }

  if (!all) {
    if (p) {
      doCompactSDataBlock(pBlock, numOfRows, p);
    } else {
      pBlock->info.rows = 0;
      pBlock->pBlockAgg = NULL;  // clean the block statistics info
    }
  }

  taosMemoryFreeClear(p);
}

static SColumnInfo* doGetTagColumnInfoById(SColumnInfo* pTagColList, int32_t numOfTags, int16_t colId);
static void         doSetTagValueInParam(void* pTable, int32_t tagColId, SVariant* tag, int16_t type, int16_t bytes);

static uint32_t doFilterByBlockTimeWindow(STableScanInfo* pTableScanInfo, SSDataBlock* pBlock) {
  SqlFunctionCtx* pCtx = pTableScanInfo->pCtx;
  uint32_t        status = BLK_DATA_NO_NEEDED;

  int32_t numOfOutput = pTableScanInfo->numOfOutput;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    int32_t functionId = pCtx[i].functionId;
    int32_t colId = pTableScanInfo->pExpr[i].base.pParam[0].pCol->colId;

    // group by + first/last should not apply the first/last block filter
    if (functionId < 0) {
      status |= BLK_DATA_ALL_NEEDED;
      return status;
    } else {
      //      status |= aAggs[functionId].dataReqFunc(&pTableScanInfo->pCtx[i], &pBlock->info.window, colId);
      //      if ((status & BLK_DATA_ALL_NEEDED) == BLK_DATA_ALL_NEEDED) {
      //        return status;
      //      }
    }
  }

  return status;
}

int32_t loadDataBlockOnDemand(SExecTaskInfo* pTaskInfo, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock,
                              uint32_t* status) {
  *status = BLK_DATA_NO_NEEDED;

  pBlock->pDataBlock = NULL;
  pBlock->pBlockAgg = NULL;

  //  int64_t groupId = pRuntimeEnv->current->groupIndex;
  //  bool    ascQuery = QUERY_IS_ASC_QUERY(pQueryAttr);

  STaskCostInfo* pCost = &pTaskInfo->cost;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;
#if 0
  if (pRuntimeEnv->pTsBuf != NULL) {
    (*status) = BLK_DATA_ALL_NEEDED;

    if (pQueryAttr->stableQuery) {  // todo refactor
      SExprInfo*   pExprInfo = &pTableScanInfo->pExpr[0];
      int16_t      tagId = (int16_t)pExprInfo->base.param[0].i;
      SColumnInfo* pColInfo = doGetTagColumnInfoById(pQueryAttr->tagColList, pQueryAttr->numOfTags, tagId);

      // compare tag first
      SVariant t = {0};
      doSetTagValueInParam(pRuntimeEnv->current->pTable, tagId, &t, pColInfo->type, pColInfo->bytes);
      setTimestampListJoinInfo(pRuntimeEnv, &t, pRuntimeEnv->current);

      STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);
      if (!tsBufIsValidElem(&elem) || (tsBufIsValidElem(&elem) && (taosVariantCompare(&t, elem.tag) != 0))) {
        (*status) = BLK_DATA_DISCARD;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  // Calculate all time windows that are overlapping or contain current data block.
  // If current data block is contained by all possible time window, do not load current data block.
  if (/*pQueryAttr->pFilters || */pQueryAttr->groupbyColumn || pQueryAttr->sw.gap > 0 ||
      (QUERY_IS_INTERVAL_QUERY(pQueryAttr) && overlapWithTimeWindow(pTaskInfo, &pBlock->info))) {
    (*status) = BLK_DATA_ALL_NEEDED;
  }

  // check if this data block is required to load
  if ((*status) != BLK_DATA_ALL_NEEDED) {
    bool needFilter = true;

    // the pCtx[i] result is belonged to previous time window since the outputBuf has not been set yet,
    // the filter result may be incorrect. So in case of interval query, we need to set the correct time output buffer
    if (QUERY_IS_INTERVAL_QUERY(pQueryAttr)) {
      SResultRow* pResult = NULL;

      bool  masterScan = IS_MAIN_SCAN(pRuntimeEnv);
      TSKEY k = ascQuery? pBlock->info.window.skey : pBlock->info.window.ekey;

      STimeWindow win = getActiveTimeWindow(pTableScanInfo->pResultRowInfo, k, pQueryAttr);
      if (pQueryAttr->pointInterpQuery) {
        needFilter = chkWindowOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, &win, masterScan, &pResult, groupId,
                                    pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                    pTableScanInfo->rowCellInfoOffset);
      } else {
        if (setResultOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pBlock->info.uid, &win, masterScan, &pResult, groupId,
                                    pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                    pTableScanInfo->rowCellInfoOffset) != TSDB_CODE_SUCCESS) {
          longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }
    } else if (pQueryAttr->stableQuery && (!pQueryAttr->tsCompQuery) && (!pQueryAttr->diffQuery)) { // stable aggregate, not interval aggregate or normal column aggregate
      doSetTableGroupOutputBuf(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pTableScanInfo->pCtx,
                               pTableScanInfo->rowCellInfoOffset, pTableScanInfo->numOfOutput,
                               pRuntimeEnv->current->groupIndex);
    }

    if (needFilter) {
      (*status) = doFilterByBlockTimeWindow(pTableScanInfo, pBlock);
    } else {
      (*status) = BLK_DATA_ALL_NEEDED;
    }
  }

  SDataBlockInfo* pBlockInfo = &pBlock->info;
//  *status = updateBlockLoadStatus(pRuntimeEnv->pQueryAttr, *status);

  if ((*status) == BLK_DATA_NO_NEEDED || (*status) == BLK_DATA_DISCARD) {
    //qDebug("QInfo:0x%"PRIx64" data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo->qId, pBlockInfo->window.skey,
//           pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->discardBlocks += 1;
  } else if ((*status) == BLK_DATA_STATIS_NEEDED) {
    // this function never returns error?
    pCost->loadBlockStatis += 1;
//    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->pTsdbReadHandle, &pBlock->pBlockAgg);

    if (pBlock->pBlockAgg == NULL) {  // data block statistics does not exist, load data block
//      pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pTsdbReadHandle, NULL);
      pCost->totalCheckedRows += pBlock->info.rows;
    }
  } else {
    assert((*status) == BLK_DATA_ALL_NEEDED);

    // load the data block statistics to perform further filter
    pCost->loadBlockStatis += 1;
//    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->pTsdbReadHandle, &pBlock->pBlockAgg);

    if (pQueryAttr->topBotQuery && pBlock->pBlockAgg != NULL) {
      { // set previous window
        if (QUERY_IS_INTERVAL_QUERY(pQueryAttr)) {
          SResultRow* pResult = NULL;

          bool  masterScan = IS_MAIN_SCAN(pRuntimeEnv);
          TSKEY k = ascQuery? pBlock->info.window.skey : pBlock->info.window.ekey;

          STimeWindow win = getActiveTimeWindow(pTableScanInfo->pResultRowInfo, k, pQueryAttr);
          if (setResultOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pBlock->info.uid, &win, masterScan, &pResult, groupId,
                                      pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                      pTableScanInfo->rowCellInfoOffset) != TSDB_CODE_SUCCESS) {
            longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
          }
        }
      }
      bool load = false;
      for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
        int32_t functionId = pTableScanInfo->pCtx[i].functionId;
        if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM) {
//          load = topbot_datablock_filter(&pTableScanInfo->pCtx[i], (char*)&(pBlock->pBlockAgg[i].min),
//                                         (char*)&(pBlock->pBlockAgg[i].max));
          if (!load) { // current block has been discard due to filter applied
            pCost->discardBlocks += 1;
            //qDebug("QInfo:0x%"PRIx64" data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo->qId,
//                   pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
            (*status) = BLK_DATA_DISCARD;
            return TSDB_CODE_SUCCESS;
          }
        }
      }
    }

    // current block has been discard due to filter applied
//    if (!doFilterByBlockStatistics(pRuntimeEnv, pBlock->pBlockAgg, pTableScanInfo->pCtx, pBlockInfo->rows)) {
//      pCost->discardBlocks += 1;
//      qDebug("QInfo:0x%"PRIx64" data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo->qId, pBlockInfo->window.skey,
//             pBlockInfo->window.ekey, pBlockInfo->rows);
//      (*status) = BLK_DATA_DISCARD;
//      return TSDB_CODE_SUCCESS;
//    }

    pCost->totalCheckedRows += pBlockInfo->rows;
    pCost->loadBlocks += 1;
//    pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pTsdbReadHandle, NULL);
//    if (pBlock->pDataBlock == NULL) {
//      return terrno;
//    }

//    if (pQueryAttr->pFilters != NULL) {
//      filterSetColFieldData(pQueryAttr->pFilters, pBlock->info.numOfCols, pBlock->pDataBlock);
//    }
    
//    if (pQueryAttr->pFilters != NULL || pRuntimeEnv->pTsBuf != NULL) {
//      filterColRowsInDataBlock(pRuntimeEnv, pBlock, ascQuery);
//    }
  }
#endif
  return TSDB_CODE_SUCCESS;
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
 * set tag value in SqlFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(void* pTable, int32_t tagColId, SVariant* tag, int16_t type, int16_t bytes) {
  taosVariantDestroy(tag);

  char* val = NULL;
  //  if (tagColId == TSDB_TBNAME_COLUMN_INDEX) {
  //    val = tsdbGetTableName(pTable);
  //    assert(val != NULL);
  //  } else {
  //    val = tsdbGetTableTagVal(pTable, tagColId, type, bytes);
  //  }

  if (val == NULL || isNull(val, type)) {
    tag->nType = TSDB_DATA_TYPE_NULL;
    return;
  }

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    int32_t maxLen = bytes - VARSTR_HEADER_SIZE;
    int32_t len = (varDataLen(val) > maxLen) ? maxLen : varDataLen(val);
    taosVariantCreateFromBinary(tag, varDataVal(val), len, type);
    // taosVariantCreateFromBinary(tag, varDataVal(val), varDataLen(val), type);
  } else {
    taosVariantCreateFromBinary(tag, val, bytes, type);
  }
}

static SColumnInfo* doGetTagColumnInfoById(SColumnInfo* pTagColList, int32_t numOfTags, int16_t colId) {
  assert(pTagColList != NULL && numOfTags > 0);

  for (int32_t i = 0; i < numOfTags; ++i) {
    if (pTagColList[i].colId == colId) {
      return &pTagColList[i];
    }
  }

  return NULL;
}

void setTagValue(SOperatorInfo* pOperatorInfo, void* pTable, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  SExprInfo* pExpr = pOperatorInfo->pExpr;
  SExprInfo* pExprInfo = &pExpr[0];
  int32_t    functionId = getExprFunctionId(pExprInfo);
#if 0
  if (pQueryAttr->numOfOutput == 1 && functionId == FUNCTION_TS_COMP && pQueryAttr->stableQuery) {
    assert(pExprInfo->base.numOfParams == 1);

    //    int16_t      tagColId = (int16_t)pExprInfo->base.param[0].i;
    int16_t      tagColId = -1;
    SColumnInfo* pColInfo = doGetTagColumnInfoById(pQueryAttr->tagColList, pQueryAttr->numOfTags, tagColId);

    doSetTagValueInParam(pTable, tagColId, &pCtx[0].tag, pColInfo->type, pColInfo->bytes);

  } else {
    // set tag value, by which the results are aggregated.
    int32_t offset = 0;
    memset(pRuntimeEnv->tagVal, 0, pQueryAttr->tagLen);

    for (int32_t idx = 0; idx < numOfOutput; ++idx) {
      SExprInfo* pLocalExprInfo = &pExpr[idx];

      // ts_comp column required the tag value for join filter
      if (!TSDB_COL_IS_TAG(pLocalExprInfo->base.pParam[0].pCol->flag)) {
        continue;
      }

      // todo use tag column index to optimize performance
      doSetTagValueInParam(pTable, pLocalExprInfo->base.pParam[0].pCol->colId, &pCtx[idx].tag,
                           pLocalExprInfo->base.resSchema.type, pLocalExprInfo->base.resSchema.bytes);

      if (IS_NUMERIC_TYPE(pLocalExprInfo->base.resSchema.type) ||
          pLocalExprInfo->base.resSchema.type == TSDB_DATA_TYPE_BOOL ||
          pLocalExprInfo->base.resSchema.type == TSDB_DATA_TYPE_TIMESTAMP) {
        memcpy(pRuntimeEnv->tagVal + offset, &pCtx[idx].tag.i, pLocalExprInfo->base.resSchema.bytes);
      } else {
        if (pCtx[idx].tag.pz != NULL) {
          memcpy(pRuntimeEnv->tagVal + offset, pCtx[idx].tag.pz, pCtx[idx].tag.nLen);
        }
      }

      offset += pLocalExprInfo->base.resSchema.bytes;
    }
  }

  // set the tsBuf start position before check each data block
  if (pRuntimeEnv->pTsBuf != NULL) {
    setCtxTagForJoin(pRuntimeEnv, &pCtx[0], pExprInfo, pTable);
  }
#endif

}

void copyToSDataBlock(SSDataBlock* pBlock, int32_t* offset, SGroupResInfo* pGroupResInfo, SDiskbasedBuf* pResBuf) {
  pBlock->info.rows = 0;

  int32_t code = TSDB_CODE_SUCCESS;
  while (pGroupResInfo->currentGroup < pGroupResInfo->totalGroup) {
    // all results in current group have been returned to client, try next group
    if ((pGroupResInfo->pRows == NULL) || taosArrayGetSize(pGroupResInfo->pRows) == 0) {
      assert(pGroupResInfo->index == 0);
      //      if ((code = mergeIntoGroupResult(&pGroupResInfo, pRuntimeEnv, offset)) != TSDB_CODE_SUCCESS) {
      return;
      //      }
    }

    //    doCopyToSDataBlock(pResBuf, pGroupResInfo, TSDB_ORDER_ASC, pBlock, );

    // current data are all dumped to result buffer, clear it
    if (!hasRemainDataInCurrentGroup(pGroupResInfo)) {
      cleanupGroupResInfo(pGroupResInfo);
      if (!incNextGroup(pGroupResInfo)) {
        break;
      }
    }

    // enough results in data buffer, return
    //    if (pBlock->info.rows >= threshold) {
    //      break;
    //    }
  }
}

static void updateTableQueryInfoForReverseScan(STableQueryInfo* pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  //  TSWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey, TSKEY);
  //  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;

  //  SWITCH_ORDER(pTableQueryInfo->cur.order);
  //  pTableQueryInfo->cur.vgroupIndex = -1;

  // set the index to be the end slot of result rows array
  SResultRowInfo* pResultRowInfo = &pTableQueryInfo->resInfo;
  if (pResultRowInfo->size > 0) {
    pResultRowInfo->curPos = pResultRowInfo->size - 1;
  } else {
    pResultRowInfo->curPos = -1;
  }
}

void initResultRow(SResultRow* pResultRow) {
  pResultRow->pEntryInfo = (struct SResultRowEntryInfo*)((char*)pResultRow + sizeof(SResultRow));
}

/*
 * The start of each column SResultRowEntryInfo is denote by RowCellInfoOffset.
 * Note that in case of top/bottom query, the whole multiple rows of result is treated as only one row of results.
 * +------------+-----------------result column 1------------+------------------result column 2-----------+
 * | SResultRow | SResultRowEntryInfo | intermediate buffer1 | SResultRowEntryInfo | intermediate buffer 2|
 * +------------+--------------------------------------------+--------------------------------------------+
 *           offset[0]                                  offset[1]                                   offset[2]
 */
// TODO refactor: some function move away
void setFunctionResultOutput(SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage, SExecTaskInfo* pTaskInfo) {
  SqlFunctionCtx* pCtx = pInfo->pCtx;
  SSDataBlock*    pDataBlock = pInfo->pRes;
  int32_t*        rowCellInfoOffset = pInfo->rowCellInfoOffset;

  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;
  initResultRowInfo(pResultRowInfo, 16);

  int64_t     tid = 0;
  int64_t     groupId = 0;
  SResultRow* pRow = doSetResultOutBufByKey_rv(pSup->pResultBuf, pResultRowInfo, tid, (char*)&tid, sizeof(tid), true,
                                               groupId, pTaskInfo, false, pSup);

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    struct SResultRowEntryInfo* pEntry = getResultCell(pRow, i, rowCellInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo = pEntry;
    pCtx[i].currentStage = stage;

    // set the timestamp output buffer for top/bottom/diff query
    //    int32_t fid = pCtx[i].functionId;
    //    if (fid == FUNCTION_TOP || fid == FUNCTION_BOTTOM || fid == FUNCTION_DIFF || fid == FUNCTION_DERIVATIVE) {
    //      if (i > 0) pCtx[i].pTsOutput = pCtx[i-1].pOutput;
    //    }
  }

  initCtxOutputBuffer(pCtx, pDataBlock->info.numOfCols);
}

void updateOutputBuf(SOptrBasicInfo* pBInfo, int32_t* bufCapacity, int32_t numOfInputRows) {
  SSDataBlock* pDataBlock = pBInfo->pRes;

  int32_t newSize = pDataBlock->info.rows + numOfInputRows + 5;  // extra output buffer
  if ((*bufCapacity) < newSize) {
    for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);

      char* p = taosMemoryRealloc(pColInfo->pData, newSize * pColInfo->info.bytes);
      if (p != NULL) {
        pColInfo->pData = p;

        // it starts from the tail of the previously generated results.
        pBInfo->pCtx[i].pOutput = pColInfo->pData;
        (*bufCapacity) = newSize;
      } else {
        // longjmp
      }
    }
  }

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
    pBInfo->pCtx[i].pOutput = pColInfo->pData + pColInfo->info.bytes * pDataBlock->info.rows;

    // set the correct pointer after the memory buffer reallocated.
    int32_t functionId = pBInfo->pCtx[i].functionId;

    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF ||
        functionId == FUNCTION_DERIVATIVE) {
//      if (i > 0) pBInfo->pCtx[i].pTsOutput = pBInfo->pCtx[i - 1].pOutput;
    }
  }
}

void copyTsColoum(SSDataBlock* pRes, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  bool    needCopyTs = false;
  int32_t tsNum = 0;
  char*   src = NULL;
  for (int32_t i = 0; i < numOfOutput; i++) {
    int32_t functionId = pCtx[i].functionId;
    if (functionId == FUNCTION_DIFF || functionId == FUNCTION_DERIVATIVE) {
      needCopyTs = true;
      if (i > 0 && pCtx[i - 1].functionId == FUNCTION_TS_DUMMY) {
        SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, i - 1);  // find ts data
        src = pColRes->pData;
      }
    } else if (functionId == FUNCTION_TS_DUMMY) {
      tsNum++;
    }
  }

  if (!needCopyTs) return;
  if (tsNum < 2) return;
  if (src == NULL) return;

  for (int32_t i = 0; i < numOfOutput; i++) {
    int32_t functionId = pCtx[i].functionId;
    if (functionId == FUNCTION_TS_DUMMY) {
      SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, i);
      memcpy(pColRes->pData, src, pColRes->info.bytes * pRes->info.rows);
    }
  }
}

void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size) {
  for (int32_t j = 0; j < size; ++j) {
    struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (isRowEntryInitialized(pResInfo) || fmIsPseudoColumnFunc(pCtx[j].functionId) || pCtx[j].functionId == -1 || fmIsScalarFunc(pCtx[j].functionId)) {
      continue;
    }

    pCtx[j].fpSet.init(&pCtx[j], pCtx[j].resultInfo);
  }
}

void setTaskStatus(SExecTaskInfo* pTaskInfo, int8_t status) {
  if (status == TASK_NOT_COMPLETED) {
    pTaskInfo->status = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    CLEAR_QUERY_STATUS(pTaskInfo, TASK_NOT_COMPLETED);
    pTaskInfo->status |= status;
  }
}

void finalizeQueryResult(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t j = 0; j < numOfOutput; ++j) {
    if (pCtx[j].functionId == -1) {
      continue;
    }

    pCtx[j].fpSet.finalize(&pCtx[j]);
  }
}

void finalizeMultiTupleQueryResult(SqlFunctionCtx* pCtx, int32_t numOfOutput, SDiskbasedBuf* pBuf,
                                   SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset) {
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRowPosition* pPos = &pResultRowInfo->pPosition[i];

    SFilePage*  bufPage = getBufPage(pBuf, pPos->pageId);
    SResultRow* pRow = (SResultRow*)((char*)bufPage + pPos->offset);

    // TODO ignore the close status anyway.
//    if (!isResultRowClosed(pRow)) {
//      continue;
//    }

    for (int32_t j = 0; j < numOfOutput; ++j) {
      pCtx[j].resultInfo = getResultCell(pRow, j, rowCellInfoOffset);

      struct SResultRowEntryInfo* pResInfo = pCtx[j].resultInfo;
      if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
        continue;
      }

      if (pCtx[j].fpSet.process) {  // TODO set the dummy function, to avoid the check for null ptr.
        pCtx[j].fpSet.finalize(&pCtx[j]);
      }

      if (pRow->numOfRows < pResInfo->numOfRes) {
        pRow->numOfRows = pResInfo->numOfRes;
      }
    }

    releaseBufPage(pBuf, bufPage);
  }
}

void finalizeUpdatedResult(SqlFunctionCtx* pCtx, int32_t numOfOutput, SDiskbasedBuf* pBuf, SArray* pUpdateList,
                           int32_t* rowCellInfoOffset) {
  size_t num = taosArrayGetSize(pUpdateList);

  for (int32_t i = 0; i < num; ++i) {
    SResultRowPosition* pPos = taosArrayGet(pUpdateList, i);

    SFilePage*  bufPage = getBufPage(pBuf, pPos->pageId);
    SResultRow* pRow = (SResultRow*)((char*)bufPage + pPos->offset);

    for (int32_t j = 0; j < numOfOutput; ++j) {
      pCtx[j].resultInfo = getResultCell(pRow, j, rowCellInfoOffset);

      struct SResultRowEntryInfo* pResInfo = pCtx[j].resultInfo;
      if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
        continue;
      }

      if (pCtx[j].fpSet.process) {  // TODO set the dummy function.
        pCtx[j].fpSet.finalize(&pCtx[j]);
        pResInfo->initialized = true;
      }

      if (pRow->numOfRows < pResInfo->numOfRes) {
        pRow->numOfRows = pResInfo->numOfRes;
      }
    }

    releaseBufPage(pBuf, bufPage);
    /*
     * set the number of output results for group by normal columns, the number of output rows usually is 1 except
     * the top and bottom query
     */
    //    buf->numOfRows = (uint16_t)getNumOfResult(pCtx, numOfOutput);
  }
}

static bool hasMainOutput(STaskAttr* pQueryAttr) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functionId = getExprFunctionId(&pQueryAttr->pExpr1[i]);

    if (functionId != FUNCTION_TS && functionId != FUNCTION_TAG && functionId != FUNCTION_TAGPRJ) {
      return true;
    }
  }

  return false;
}

STableQueryInfo* createTableQueryInfo(void* buf, bool groupbyColumn, STimeWindow win) {
  STableQueryInfo* pTableQueryInfo = buf;
  pTableQueryInfo->lastKey = win.skey;

  // set more initial size of interval/groupby query
  //  if (/*QUERY_IS_INTERVAL_QUERY(pQueryAttr) || */groupbyColumn) {
  int32_t initialSize = 128;
  int32_t code = initResultRowInfo(&pTableQueryInfo->resInfo, initialSize);
  if (code != TSDB_CODE_SUCCESS) {
    return NULL;
  }
  //  } else { // in other aggregate query, do not initialize the windowResInfo
  //  }

  return pTableQueryInfo;
}

void destroyTableQueryInfoImpl(STableQueryInfo* pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  //  taosVariantDestroy(&pTableQueryInfo->tag);
  cleanupResultRowInfo(&pTableQueryInfo->resInfo);
}

void setResultRowOutputBufInitCtx_rv(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);

    struct SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
      continue;
    }

    if (fmIsWindowPseudoColumnFunc(pCtx[i].functionId)) {
      continue;
    }

    if (!pResInfo->initialized && pCtx[i].functionId != -1) {
      pCtx[i].fpSet.init(&pCtx[i], pResInfo);
    }
  }
}

void doFilter(const SNode* pFilterNode, SSDataBlock* pBlock) {
  if (pFilterNode == NULL) {
    return;
  }

  SFilterInfo* filter = NULL;

  // todo move to the initialization function
  int32_t code = filterInitFromNode((SNode*)pFilterNode, &filter, 0);

  SFilterColumnParam param1 = {.numOfCols = pBlock->info.numOfCols, .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param1);

  int8_t* rowRes = NULL;
  bool    keep = filterExecute(filter, pBlock, &rowRes, NULL, param1.numOfCols);
  filterFreeInfo(filter);

  SSDataBlock* px = createOneDataBlock(pBlock);
  blockDataEnsureCapacity(px, pBlock->info.rows);

  // todo extract method
  int32_t numOfRow = 0;
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(px->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, i);
    if (keep) {
      colDataAssign(pDst, pSrc, pBlock->info.rows);
      numOfRow = pBlock->info.rows;
    } else if (NULL != rowRes) {
      numOfRow = 0;
      for (int32_t j = 0; j < pBlock->info.rows; ++j) {
        if (rowRes[j] == 0) {
          continue;
        }

        if (colDataIsNull_s(pSrc, j)) {
          colDataAppendNULL(pDst, numOfRow);
        } else {
          colDataAppend(pDst, numOfRow, colDataGetData(pSrc, j), false);
        }
        numOfRow += 1;
      }
    } else {
      numOfRow = 0;
    }

    *pSrc = *pDst;
  }

  pBlock->info.rows = numOfRow;
  blockDataUpdateTsWindow(pBlock);
}

void doSetTableGroupOutputBuf(SAggOperatorInfo* pAggInfo, int32_t numOfOutput, int32_t tableGroupId, SExecTaskInfo* pTaskInfo) {
  // for simple group by query without interval, all the tables belong to one group result.
  int64_t uid = 0;
  int64_t tid = 0;

  SResultRowInfo* pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
  SqlFunctionCtx* pCtx = pAggInfo->binfo.pCtx;
  int32_t*        rowCellInfoOffset = pAggInfo->binfo.rowCellInfoOffset;

  SResultRow* pResultRow =
      doSetResultOutBufByKey_rv(pAggInfo->pResultBuf, pResultRowInfo, tid, (char*)&tableGroupId, sizeof(tableGroupId),
                                true, uid, pTaskInfo, false, &pAggInfo->aggSup);
  assert(pResultRow != NULL);

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pResultRow->pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pResultRow, pAggInfo->pResultBuf, tableGroupId, pAggInfo->binfo.pRes->info.rowSize);
    if (ret != TSDB_CODE_SUCCESS) {
      return;
    }
  }

  setResultRowOutputBufInitCtx_rv(pResultRow, pCtx, numOfOutput, rowCellInfoOffset);
}

void setExecutionContext(int32_t numOfOutput, int32_t tableGroupId, TSKEY nextKey, SExecTaskInfo* pTaskInfo,
                         STableQueryInfo* pTableQueryInfo, SAggOperatorInfo* pAggInfo) {
  // lastKey needs to be updated
  pTableQueryInfo->lastKey = nextKey;
  if (pAggInfo->groupId != INT32_MIN && pAggInfo->groupId == tableGroupId) {
    return;
  }

  doSetTableGroupOutputBuf(pAggInfo, numOfOutput, tableGroupId, pTaskInfo);

  // record the current active group id
  pAggInfo->groupId = tableGroupId;
}

void setCtxTagForJoin(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, void* pTable) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

  SExprBasicInfo* pExpr = &pExprInfo->base;
  //  if (pQueryAttr->stableQuery && (pRuntimeEnv->pTsBuf != NULL) &&
  //      (pExpr->functionId == FUNCTION_TS || pExpr->functionId == FUNCTION_PRJ) &&
  //      (pExpr->colInfo.colIndex == PRIMARYKEY_TIMESTAMP_COL_ID)) {
  //    assert(pExpr->numOfParams == 1);
  //
  //    int16_t      tagColId = (int16_t)pExprInfo->base.param[0].i;
  //    SColumnInfo* pColInfo = doGetTagColumnInfoById(pQueryAttr->tagColList, pQueryAttr->numOfTags, tagColId);
  //
  //    doSetTagValueInParam(pTable, tagColId, &pCtx->tag, pColInfo->type, pColInfo->bytes);
  //
  //    int16_t tagType = pCtx[0].tag.nType;
  //    if (tagType == TSDB_DATA_TYPE_BINARY || tagType == TSDB_DATA_TYPE_NCHAR) {
  //      //qDebug("QInfo:0x%"PRIx64" set tag value for join comparison, colId:%" PRId64 ", val:%s",
  //      GET_TASKID(pRuntimeEnv),
  ////             pExprInfo->base.param[0].i, pCtx[0].tag.pz);
  //    } else {
  //      //qDebug("QInfo:0x%"PRIx64" set tag value for join comparison, colId:%" PRId64 ", val:%" PRId64,
  //      GET_TASKID(pRuntimeEnv),
  ////             pExprInfo->base.param[0].i, pCtx[0].tag.i);
  //    }
  //  }
}

int32_t setTimestampListJoinInfo(STaskRuntimeEnv* pRuntimeEnv, SVariant* pTag, STableQueryInfo* pTableQueryInfo) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

  assert(pRuntimeEnv->pTsBuf != NULL);
#if 0
  // both the master and supplement scan needs to set the correct ts comp start position
  if (pTableQueryInfo->cur.vgroupIndex == -1) {
    taosVariantAssign(&pTableQueryInfo->tag, pTag);

    STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTsBuf, pQueryAttr->vgId, &pTableQueryInfo->tag);

    // failed to find data with the specified tag value and vnodeId
    if (!tsBufIsValidElem(&elem)) {
      if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
        //qError("QInfo:0x%"PRIx64" failed to find tag:%s in ts_comp", GET_TASKID(pRuntimeEnv), pTag->pz);
      } else {
        //qError("QInfo:0x%"PRIx64" failed to find tag:%" PRId64 " in ts_comp", GET_TASKID(pRuntimeEnv), pTag->i);
      }

      return -1;
    }

    // Keep the cursor info of current table
    pTableQueryInfo->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
    if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
      //qDebug("QInfo:0x%"PRIx64" find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", GET_TASKID(pRuntimeEnv), pTag->pz, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
    } else {
      //qDebug("QInfo:0x%"PRIx64" find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", GET_TASKID(pRuntimeEnv), pTag->i, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
    }

  } else {
    tsBufSetCursor(pRuntimeEnv->pTsBuf, &pTableQueryInfo->cur);
    if (pTag->nType == TSDB_DATA_TYPE_BINARY || pTag->nType == TSDB_DATA_TYPE_NCHAR) {
      //qDebug("QInfo:0x%"PRIx64" find tag:%s start pos in ts_comp, blockIndex:%d, tsIndex:%d", GET_TASKID(pRuntimeEnv), pTag->pz, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
    } else {
      //qDebug("QInfo:0x%"PRIx64" find tag:%"PRId64" start pos in ts_comp, blockIndex:%d, tsIndex:%d", GET_TASKID(pRuntimeEnv), pTag->i, pTableQueryInfo->cur.blockIndex, pTableQueryInfo->cur.tsIndex);
    }
  }
#endif
  return 0;
}

/*
 * There are two cases to handle:
 *
 * 1. Query range is not set yet (queryRangeSet = 0). we need to set the query range info, including
 * pQueryAttr->lastKey, pQueryAttr->window.skey, and pQueryAttr->eKey.
 * 2. Query range is set and query is in progress. There may be another result with the same query ranges to be
 *    merged during merge stage. In this case, we need the pTableQueryInfo->lastResRows to decide if there
 *    is a previous result generated or not.
 */
void setIntervalQueryRange(STaskRuntimeEnv* pRuntimeEnv, TSKEY key) {
  STaskAttr*       pQueryAttr = pRuntimeEnv->pQueryAttr;
  STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
  SResultRowInfo*  pResultRowInfo = &pTableQueryInfo->resInfo;

  if (pResultRowInfo->curPos != -1) {
    return;
  }

  //  pTableQueryInfo->win.skey = key;
  STimeWindow win = {.skey = key, .ekey = pQueryAttr->window.ekey};

  /**
   * In handling the both ascending and descending order super table query, we need to find the first qualified
   * timestamp of this table, and then set the first qualified start timestamp.
   * In ascending query, the key is the first qualified timestamp. However, in the descending order query, additional
   * operations involve.
   */
  STimeWindow w = TSWINDOW_INITIALIZER;

  TSKEY sk = TMIN(win.skey, win.ekey);
  TSKEY ek = TMAX(win.skey, win.ekey);
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

/**
 * copyToOutputBuf support copy data in ascending/descending order
 * For interval query of both super table and table, copy the data in ascending order, since the output results are
 * ordered in SWindowResutl already. While handling the group by query for both table and super table,
 * all group result are completed already.
 *
 * @param pQInfo
 * @param result
 */
int32_t doCopyToSDataBlock(SSDataBlock* pBlock, int32_t rowCapacity, SExprInfo* pExprInfo, SDiskbasedBuf* pBuf, SGroupResInfo* pGroupResInfo, int32_t orderType, int32_t* rowCellOffset) {
  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
  int32_t numOfResult = pBlock->info.rows;  // there are already exists result rows

  int32_t start = 0;
  int32_t step = -1;

  // qDebug("QInfo:0x%"PRIx64" start to copy data from windowResInfo to output buf", GET_TASKID(pRuntimeEnv));
  assert(orderType == TSDB_ORDER_ASC || orderType == TSDB_ORDER_DESC);

  if (orderType == TSDB_ORDER_ASC) {
    start = pGroupResInfo->index;
    step = 1;
  } else {  // desc order copy all data
    start = numOfRows - pGroupResInfo->index - 1;
    step = -1;
  }

  int32_t nrows = pBlock->info.rows;

  for (int32_t i = start; (i < numOfRows) && (i >= 0); i += step) {
    SResultRowPosition* pPos = taosArrayGet(pGroupResInfo->pRows, i);
    SFilePage*          page = getBufPage(pBuf, pPos->pageId);

    SResultRow* pRow = (SResultRow*)((char*)page + pPos->offset);
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }

    // TODO copy multiple rows?
    int32_t numOfRowsToCopy = pRow->numOfRows;
    if (numOfResult + numOfRowsToCopy >= rowCapacity) {
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      SColumnInfoData*     pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
      SResultRowEntryInfo* pEntryInfo = getResultCell(pRow, j, rowCellOffset);

      char* in = GET_ROWCELL_INTERBUF(pEntryInfo);
      colDataAppend(pColInfoData, nrows, in, pEntryInfo->isNullRes);
    }

    releaseBufPage(pBuf, page);
    nrows += 1;

    numOfResult += numOfRowsToCopy;
    if (numOfResult == rowCapacity) {  // output buffer is full
      break;
    }
  }

  // qDebug("QInfo:0x%"PRIx64" copy data to query buf completed", GET_TASKID(pRuntimeEnv));
  pBlock->info.rows = numOfResult;
  return 0;
}

void toSDatablock(SSDataBlock* pBlock, int32_t rowCapacity, SGroupResInfo* pGroupResInfo, SExprInfo* pExprInfo, SDiskbasedBuf* pBuf,
                  int32_t* rowCellOffset) {
  assert(pGroupResInfo->currentGroup <= pGroupResInfo->totalGroup);

  blockDataCleanup(pBlock);
  if (!hasRemainDataInCurrentGroup(pGroupResInfo)) {
    return;
  }

  int32_t orderType = TSDB_ORDER_ASC;
  doCopyToSDataBlock(pBlock, rowCapacity, pExprInfo, pBuf, pGroupResInfo, orderType, rowCellOffset);

  // add condition (pBlock->info.rows >= 1) just to runtime happy
  blockDataUpdateTsWindow(pBlock);
}

static void updateNumOfRowsInResultRows(SqlFunctionCtx* pCtx, int32_t numOfOutput, SResultRowInfo* pResultRowInfo,
                                        int32_t* rowCellInfoOffset) {
  // update the number of result for each, only update the number of rows for the corresponding window result.
  //  if (QUERY_IS_INTERVAL_QUERY(pQueryAttr)) {
  //    return;
  //  }
#if 0
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRow* pResult = pResultRowInfo->pResult[i];

    for (int32_t j = 0; j < numOfOutput; ++j) {
      int32_t functionId = pCtx[j].functionId;
      if (functionId == FUNCTION_TS || functionId == FUNCTION_TAG || functionId == FUNCTION_TAGPRJ) {
        continue;
      }

      SResultRowEntryInfo* pCell = getResultCell(pResult, j, rowCellInfoOffset);
      pResult->numOfRows = (uint16_t)(TMAX(pResult->numOfRows, pCell->numOfRes));
    }
  }
#endif

}

static int32_t compressQueryColData(SColumnInfoData* pColRes, int32_t numOfRows, char* data, int8_t compressed) {
  int32_t colSize = pColRes->info.bytes * numOfRows;
  return (*(tDataTypes[pColRes->info.type].compFunc))(pColRes->pData, colSize, numOfRows, data,
                                                      colSize + COMP_OVERFLOW_BYTES, compressed, NULL, 0);
}

int32_t doFillTimeIntervalGapsInResults(struct SFillInfo* pFillInfo, SSDataBlock* pOutput, int32_t capacity, void** p) {
  //  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
  //    SColumnInfoData* pColInfoData = taosArrayGet(pOutput->pDataBlock, i);
  //    p[i] = pColInfoData->pData + (pColInfoData->info.bytes * pOutput->info.rows);
  //  }

  int32_t numOfRows = (int32_t)taosFillResultDataBlock(pFillInfo, p, capacity - pOutput->info.rows);
  pOutput->info.rows += numOfRows;

  return pOutput->info.rows;
}

void publishOperatorProfEvent(SOperatorInfo* operatorInfo, EQueryProfEventType eventType) {
  SQueryProfEvent event = {0};

  event.eventType = eventType;
  event.eventTime = taosGetTimestampUs();
  event.operatorType = operatorInfo->operatorType;

  if (operatorInfo->pRuntimeEnv) {
    //    SQInfo* pQInfo = operatorInfo->pRuntimeEnv->qinfo;
    //    if (pQInfo->summary.queryProfEvents) {
    //      taosArrayPush(pQInfo->summary.queryProfEvents, &event);
    //    }
  }
}

void publishQueryAbortEvent(SExecTaskInfo* pTaskInfo, int32_t code) {
  SQueryProfEvent event;
  event.eventType = QUERY_PROF_QUERY_ABORT;
  event.eventTime = taosGetTimestampUs();
  event.abortCode = code;

  if (pTaskInfo->cost.queryProfEvents) {
    taosArrayPush(pTaskInfo->cost.queryProfEvents, &event);
  }
}

typedef struct {
  uint8_t operatorType;
  int64_t beginTime;
  int64_t endTime;
  int64_t selfTime;
  int64_t descendantsTime;
} SOperatorStackItem;

static void doOperatorExecProfOnce(SOperatorStackItem* item, SQueryProfEvent* event, SArray* opStack,
                                   SHashObj* profResults) {
  item->endTime = event->eventTime;
  item->selfTime = (item->endTime - item->beginTime) - (item->descendantsTime);

  for (int32_t j = 0; j < taosArrayGetSize(opStack); ++j) {
    SOperatorStackItem* ancestor = taosArrayGet(opStack, j);
    ancestor->descendantsTime += item->selfTime;
  }

  uint8_t              operatorType = item->operatorType;
  SOperatorProfResult* result = taosHashGet(profResults, &operatorType, sizeof(operatorType));
  if (result != NULL) {
    result->sumRunTimes++;
    result->sumSelfTime += item->selfTime;
  } else {
    SOperatorProfResult opResult;
    opResult.operatorType = operatorType;
    opResult.sumSelfTime = item->selfTime;
    opResult.sumRunTimes = 1;
    taosHashPut(profResults, &(operatorType), sizeof(operatorType), &opResult, sizeof(opResult));
  }
}

void calculateOperatorProfResults(SQInfo* pQInfo) {
  if (pQInfo->summary.queryProfEvents == NULL) {
    // qDebug("QInfo:0x%"PRIx64" query prof events array is null", pQInfo->qId);
    return;
  }

  if (pQInfo->summary.operatorProfResults == NULL) {
    // qDebug("QInfo:0x%"PRIx64" operator prof results hash is null", pQInfo->qId);
    return;
  }

  SArray* opStack = taosArrayInit(32, sizeof(SOperatorStackItem));
  if (opStack == NULL) {
    return;
  }

  size_t    size = taosArrayGetSize(pQInfo->summary.queryProfEvents);
  SHashObj* profResults = pQInfo->summary.operatorProfResults;

  for (int i = 0; i < size; ++i) {
    SQueryProfEvent* event = taosArrayGet(pQInfo->summary.queryProfEvents, i);
    if (event->eventType == QUERY_PROF_BEFORE_OPERATOR_EXEC) {
      SOperatorStackItem opItem;
      opItem.operatorType = event->operatorType;
      opItem.beginTime = event->eventTime;
      opItem.descendantsTime = 0;
      taosArrayPush(opStack, &opItem);
    } else if (event->eventType == QUERY_PROF_AFTER_OPERATOR_EXEC) {
      SOperatorStackItem* item = taosArrayPop(opStack);
      assert(item->operatorType == event->operatorType);
      doOperatorExecProfOnce(item, event, opStack, profResults);
    } else if (event->eventType == QUERY_PROF_QUERY_ABORT) {
      SOperatorStackItem* item;
      while ((item = taosArrayPop(opStack)) != NULL) {
        doOperatorExecProfOnce(item, event, opStack, profResults);
      }
    }
  }

  taosArrayDestroy(opStack);
}

void queryCostStatis(SExecTaskInfo* pTaskInfo) {
  STaskCostInfo* pSummary = &pTaskInfo->cost;

  //  uint64_t hashSize = taosHashGetMemSize(pQInfo->runtimeEnv.pResultRowHashTable);
  //  hashSize += taosHashGetMemSize(pRuntimeEnv->tableqinfoGroupInfo.map);
  //  pSummary->hashSize = hashSize;

  // add the merge time
  pSummary->elapsedTime += pSummary->firstStageMergeTime;

  //  SResultRowPool* p = pTaskInfo->pool;
  //  if (p != NULL) {
  //    pSummary->winInfoSize = getResultRowPoolMemSize(p);
  //    pSummary->numOfTimeWindows = getNumOfAllocatedResultRows(p);
  //  } else {
  //    pSummary->winInfoSize = 0;
  //    pSummary->numOfTimeWindows = 0;
  //  }
  //
  //  calculateOperatorProfResults(pQInfo);

  qDebug("%s :cost summary: elapsed time:%" PRId64 " us, first merge:%" PRId64
         " us, total blocks:%d, "
         "load block statis:%d, load data block:%d, total rows:%" PRId64 ", check rows:%" PRId64,
         GET_TASKID(pTaskInfo), pSummary->elapsedTime, pSummary->firstStageMergeTime, pSummary->totalBlocks,
         pSummary->loadBlockStatis, pSummary->loadBlocks, pSummary->totalRows, pSummary->totalCheckedRows);
  //
  // qDebug("QInfo:0x%"PRIx64" :cost summary: winResPool size:%.2f Kb, numOfWin:%"PRId64", tableInfoSize:%.2f Kb,
  // hashTable:%.2f Kb", pQInfo->qId, pSummary->winInfoSize/1024.0,
  //      pSummary->numOfTimeWindows, pSummary->tableInfoSize/1024.0, pSummary->hashSize/1024.0);

  if (pSummary->operatorProfResults) {
    SOperatorProfResult* opRes = taosHashIterate(pSummary->operatorProfResults, NULL);
    while (opRes != NULL) {
      // qDebug("QInfo:0x%" PRIx64 " :cost summary: operator : %d, exec times: %" PRId64 ", self time: %" PRId64,
      //             pQInfo->qId, opRes->operatorType, opRes->sumRunTimes, opRes->sumSelfTime);
      opRes = taosHashIterate(pSummary->operatorProfResults, opRes);
    }
  }
}

// static void updateOffsetVal(STaskRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pBlockInfo) {
//   STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//   STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
//
//   int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
//
//   if (pQueryAttr->limit.offset == pBlockInfo->rows) {  // current block will ignore completed
//     pTableQueryInfo->lastKey = QUERY_IS_ASC_QUERY(pQueryAttr) ? pBlockInfo->window.ekey + step :
//     pBlockInfo->window.skey + step; pQueryAttr->limit.offset = 0; return;
//   }
//
//   if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//     pQueryAttr->pos = (int32_t)pQueryAttr->limit.offset;
//   } else {
//     pQueryAttr->pos = pBlockInfo->rows - (int32_t)pQueryAttr->limit.offset - 1;
//   }
//
//   assert(pQueryAttr->pos >= 0 && pQueryAttr->pos <= pBlockInfo->rows - 1);
//
//   SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pTsdbReadHandle, NULL);
//   SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//   // update the pQueryAttr->limit.offset value, and pQueryAttr->pos value
//   TSKEY *keys = (TSKEY *) pColInfoData->pData;
//
//   // update the offset value
//   pTableQueryInfo->lastKey = keys[pQueryAttr->pos];
//   pQueryAttr->limit.offset = 0;
//
//   int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
//
//   //qDebug("QInfo:0x%"PRIx64" check data block, brange:%" PRId64 "-%" PRId64 ", numBlocksOfStep:%d, numOfRes:%d,
//   lastKey:%"PRId64, GET_TASKID(pRuntimeEnv),
//          pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes, pQuery->current->lastKey);
// }

// void skipBlocks(STaskRuntimeEnv *pRuntimeEnv) {
//   STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//
//   if (pQueryAttr->limit.offset <= 0 || pQueryAttr->numOfFilterCols > 0) {
//     return;
//   }
//
//   pQueryAttr->pos = 0;
//   int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
//
//   STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
//   TsdbQueryHandleT pTsdbReadHandle = pRuntimeEnv->pTsdbReadHandle;
//
//   SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
//   while (tsdbNextDataBlock(pTsdbReadHandle)) {
//     if (isTaskKilled(pRuntimeEnv->qinfo)) {
//       longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
//     }
//
//     tsdbRetrieveDataBlockInfo(pTsdbReadHandle, &blockInfo);
//
//     if (pQueryAttr->limit.offset > blockInfo.rows) {
//       pQueryAttr->limit.offset -= blockInfo.rows;
//       pTableQueryInfo->lastKey = (QUERY_IS_ASC_QUERY(pQueryAttr)) ? blockInfo.window.ekey : blockInfo.window.skey;
//       pTableQueryInfo->lastKey += step;
//
//       //qDebug("QInfo:0x%"PRIx64" skip rows:%d, offset:%" PRId64, GET_TASKID(pRuntimeEnv), blockInfo.rows,
//              pQuery->limit.offset);
//     } else {  // find the appropriated start position in current block
//       updateOffsetVal(pRuntimeEnv, &blockInfo);
//       break;
//     }
//   }
//
//   if (terrno != TSDB_CODE_SUCCESS) {
//     longjmp(pRuntimeEnv->env, terrno);
//   }
// }

// static TSKEY doSkipIntervalProcess(STaskRuntimeEnv* pRuntimeEnv, STimeWindow* win, SDataBlockInfo* pBlockInfo,
// STableQueryInfo* pTableQueryInfo) {
//   STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//   SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
//
//   assert(pQueryAttr->limit.offset == 0);
//   STimeWindow tw = *win;
//   getNextTimeWindow(pQueryAttr, &tw);
//
//   if ((tw.skey <= pBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQueryAttr)) ||
//       (tw.ekey >= pBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQueryAttr))) {
//
//     // load the data block and check data remaining in current data block
//     // TODO optimize performance
//     SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pTsdbReadHandle, NULL);
//     SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//     tw = *win;
//     int32_t startPos =
//         getNextQualifiedWindow(pQueryAttr, &tw, pBlockInfo, pColInfoData->pData, binarySearchForKey, -1);
//     assert(startPos >= 0);
//
//     // set the abort info
//     pQueryAttr->pos = startPos;
//
//     // reset the query start timestamp
//     pTableQueryInfo->win.skey = ((TSKEY *)pColInfoData->pData)[startPos];
//     pQueryAttr->window.skey = pTableQueryInfo->win.skey;
//     TSKEY key = pTableQueryInfo->win.skey;
//
//     pWindowResInfo->prevSKey = tw.skey;
//     int32_t index = pRuntimeEnv->resultRowInfo.curIndex;
//
//     int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
//     pRuntimeEnv->resultRowInfo.curIndex = index;  // restore the window index
//
//     //qDebug("QInfo:0x%"PRIx64" check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d,
//     lastKey:%" PRId64,
//            GET_TASKID(pRuntimeEnv), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes,
//            pQueryAttr->current->lastKey);
//
//     return key;
//   } else {  // do nothing
//     pQueryAttr->window.skey      = tw.skey;
//     pWindowResInfo->prevSKey = tw.skey;
//     pTableQueryInfo->lastKey = tw.skey;
//
//     return tw.skey;
//   }
//
//   return true;
// }

// static bool skipTimeInterval(STaskRuntimeEnv *pRuntimeEnv, TSKEY* start) {
//   STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//   if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//     assert(*start <= pRuntimeEnv->current->lastKey);
//   } else {
//     assert(*start >= pRuntimeEnv->current->lastKey);
//   }
//
//   // if queried with value filter, do NOT forward query start position
//   if (pQueryAttr->limit.offset <= 0 || pQueryAttr->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL ||
//   pRuntimeEnv->pFillInfo != NULL) {
//     return true;
//   }
//
//   /*
//    * 1. for interval without interpolation query we forward pQueryAttr->interval.interval at a time for
//    *    pQueryAttr->limit.offset times. Since hole exists, pQueryAttr->interval.interval*pQueryAttr->limit.offset
//    value is
//    *    not valid. otherwise, we only forward pQueryAttr->limit.offset number of points
//    */
//   assert(pRuntimeEnv->resultRowInfo.prevSKey == TSKEY_INITIAL_VAL);
//
//   STimeWindow w = TSWINDOW_INITIALIZER;
//   bool ascQuery = QUERY_IS_ASC_QUERY(pQueryAttr);
//
//   SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
//   STableQueryInfo *pTableQueryInfo = pRuntimeEnv->current;
//
//   SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
//   while (tsdbNextDataBlock(pRuntimeEnv->pTsdbReadHandle)) {
//     tsdbRetrieveDataBlockInfo(pRuntimeEnv->pTsdbReadHandle, &blockInfo);
//
//     if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//       if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
//         getAlignQueryTimeWindow(pQueryAttr, blockInfo.window.skey, blockInfo.window.skey, pQueryAttr->window.ekey,
//         &w); pWindowResInfo->prevSKey = w.skey;
//       }
//     } else {
//       getAlignQueryTimeWindow(pQueryAttr, blockInfo.window.ekey, pQueryAttr->window.ekey, blockInfo.window.ekey, &w);
//       pWindowResInfo->prevSKey = w.skey;
//     }
//
//     // the first time window
//     STimeWindow win = getActiveTimeWindow(pWindowResInfo, pWindowResInfo->prevSKey, pQueryAttr);
//
//     while (pQueryAttr->limit.offset > 0) {
//       STimeWindow tw = win;
//
//       if ((win.ekey <= blockInfo.window.ekey && ascQuery) || (win.ekey >= blockInfo.window.skey && !ascQuery)) {
//         pQueryAttr->limit.offset -= 1;
//         pWindowResInfo->prevSKey = win.skey;
//
//         // current time window is aligned with blockInfo.window.ekey
//         // restart it from next data block by set prevSKey to be TSKEY_INITIAL_VAL;
//         if ((win.ekey == blockInfo.window.ekey && ascQuery) || (win.ekey == blockInfo.window.skey && !ascQuery)) {
//           pWindowResInfo->prevSKey = TSKEY_INITIAL_VAL;
//         }
//       }
//
//       if (pQueryAttr->limit.offset == 0) {
//         *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
//         return true;
//       }
//
//       // current window does not ended in current data block, try next data block
//       getNextTimeWindow(pQueryAttr, &tw);
//
//       /*
//        * If the next time window still starts from current data block,
//        * load the primary timestamp column first, and then find the start position for the next queried time window.
//        * Note that only the primary timestamp column is required.
//        * TODO: Optimize for this cases. All data blocks are not needed to be loaded, only if the first actually
//        required
//        * time window resides in current data block.
//        */
//       if ((tw.skey <= blockInfo.window.ekey && ascQuery) || (tw.ekey >= blockInfo.window.skey && !ascQuery)) {
//
//         SArray *pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pTsdbReadHandle, NULL);
//         SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//         if ((win.ekey > blockInfo.window.ekey && ascQuery) || (win.ekey < blockInfo.window.skey && !ascQuery)) {
//           pQueryAttr->limit.offset -= 1;
//         }
//
//         if (pQueryAttr->limit.offset == 0) {
//           *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
//           return true;
//         } else {
//           tw = win;
//           int32_t startPos =
//               getNextQualifiedWindow(pQueryAttr, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey, -1);
//           assert(startPos >= 0);
//
//           // set the abort info
//           pQueryAttr->pos = startPos;
//           pTableQueryInfo->lastKey = ((TSKEY *)pColInfoData->pData)[startPos];
//           pWindowResInfo->prevSKey = tw.skey;
//           win = tw;
//         }
//       } else {
//         break;  // offset is not 0, and next time window begins or ends in the next block.
//       }
//     }
//   }
//
//   // check for error
//   if (terrno != TSDB_CODE_SUCCESS) {
//     longjmp(pRuntimeEnv->env, terrno);
//   }
//
//   return true;
// }

int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num) {
  if (p->pDownstream == NULL) {
    assert(p->numOfDownstream == 0);
  }

  p->pDownstream = taosMemoryCalloc(1, num * POINTER_BYTES);
  if (p->pDownstream == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(p->pDownstream, pDownstream, num * POINTER_BYTES);
  p->numOfDownstream = num;
  return TSDB_CODE_SUCCESS;
}

static void doDestroyTableQueryInfo(STableGroupInfo* pTableqinfoGroupInfo);

static void doTableQueryInfoTimeWindowCheck(SExecTaskInfo* pTaskInfo, STableQueryInfo* pTableQueryInfo, int32_t order) {
#if 0
    if (order == TSDB_ORDER_ASC) {
    assert(
        (pTableQueryInfo->win.skey <= pTableQueryInfo->win.ekey) &&
        (pTableQueryInfo->lastKey >= pTaskInfo->window.skey) &&
        (pTableQueryInfo->win.skey >= pTaskInfo->window.skey && pTableQueryInfo->win.ekey <= pTaskInfo->window.ekey));
  } else {
    assert(
        (pTableQueryInfo->win.skey >= pTableQueryInfo->win.ekey) &&
        (pTableQueryInfo->lastKey <= pTaskInfo->window.skey) &&
        (pTableQueryInfo->win.skey <= pTaskInfo->window.skey && pTableQueryInfo->win.ekey >= pTaskInfo->window.ekey));
  }
#endif
}

// STsdbQueryCond createTsdbQueryCond(STaskAttr* pQueryAttr, STimeWindow* win) {
//   STsdbQueryCond cond = {
//       .colList   = pQueryAttr->tableCols,
//       .order     = pQueryAttr->order.order,
//       .numOfCols = pQueryAttr->numOfCols,
//       .type      = BLOCK_LOAD_OFFSET_SEQ_ORDER,
//       .loadExternalRows = false,
//   };
//
//   TIME_WINDOW_COPY(cond.twindow, *win);
//   return cond;
// }

static STableIdInfo createTableIdInfo(STableQueryInfo* pTableQueryInfo) {
  STableIdInfo tidInfo;
  //  STableId* id = TSDB_TABLEID(pTableQueryInfo->pTable);
  //
  //  tidInfo.uid = id->uid;
  //  tidInfo.tid = id->tid;
  //  tidInfo.key = pTableQueryInfo->lastKey;

  return tidInfo;
}

// static void updateTableIdInfo(STableQueryInfo* pTableQueryInfo, SSDataBlock* pBlock, SHashObj* pTableIdInfo, int32_t
// order) {
//   int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);
//   pTableQueryInfo->lastKey = ((order == TSDB_ORDER_ASC)? pBlock->info.window.ekey:pBlock->info.window.skey) + step;
//
//   if (pTableQueryInfo->pTable == NULL) {
//     return;
//   }
//
//   STableIdInfo tidInfo = createTableIdInfo(pTableQueryInfo);
//   STableIdInfo *idinfo = taosHashGet(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid));
//   if (idinfo != NULL) {
//     assert(idinfo->tid == tidInfo.tid && idinfo->uid == tidInfo.uid);
//     idinfo->key = tidInfo.key;
//   } else {
//     taosHashPut(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid), &tidInfo, sizeof(STableIdInfo));
//   }
// }

static void doCloseAllTimeWindow(STaskRuntimeEnv* pRuntimeEnv) {
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

int32_t loadRemoteDataCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  SSourceDataInfo* pSourceDataInfo = (SSourceDataInfo*)param;
  if (code == TSDB_CODE_SUCCESS) {
    pSourceDataInfo->pRsp = pMsg->pData;

    SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->compLen   = htonl(pRsp->compLen);
    pRsp->useconds  = htobe64(pRsp->useconds);
  } else {
    pSourceDataInfo->code = code;
  }

  pSourceDataInfo->status = EX_SOURCE_DATA_READY;
  tsem_post(&pSourceDataInfo->pEx->ready);
  return TSDB_CODE_SUCCESS;
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  taosMemoryFreeClear(pMsgBody->msgInfo.pData);
  taosMemoryFreeClear(pMsgBody);
}

void qProcessFetchRsp(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->ahandle;
  assert(pMsg->ahandle != NULL);

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL};

  if (pMsg->contLen > 0) {
    buf.pData = taosMemoryCalloc(1, pMsg->contLen);
    if (buf.pData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
    }
  }

  pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);
  rpcFreeCont(pMsg->pCont);
  destroySendMsgInfo(pSendInfo);
}

static int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex) {
  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SResFetchReq* pMsg = taosMemoryCalloc(1, sizeof(SResFetchReq));
  if (NULL == pMsg) {
    pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return pTaskInfo->code;
  }

  SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, sourceIndex);
  SSourceDataInfo*       pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);

  qDebug("%s build fetch msg and send to vgId:%d, ep:%s, taskId:0x%" PRIx64 ", %d/%" PRIzu, GET_TASKID(pTaskInfo),
         pSource->addr.nodeId, pSource->addr.epSet.eps[0].fqdn, pSource->taskId, sourceIndex, totalSources);

  pMsg->header.vgId = htonl(pSource->addr.nodeId);
  pMsg->sId = htobe64(pSource->schedId);
  pMsg->taskId = htobe64(pSource->taskId);
  pMsg->queryId = htobe64(pTaskInfo->id.queryId);

  // send the fetch remote task result reques
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    taosMemoryFreeClear(pMsg);
    qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
    pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return pTaskInfo->code;
  }

  pMsgSendInfo->param = pDataInfo;
  pMsgSendInfo->msgInfo.pData = pMsg;
  pMsgSendInfo->msgInfo.len = sizeof(SResFetchReq);
  pMsgSendInfo->msgType = TDMT_VND_FETCH;
  pMsgSendInfo->fp = loadRemoteDataCallback;

  int64_t transporterId = 0;
  int32_t code = asyncSendMsgToServer(pExchangeInfo->pTransporter, &pSource->addr.epSet, &transporterId, pMsgSendInfo);
  return TSDB_CODE_SUCCESS;
}

// TODO if only one or two columnss required, how to extract data?
int32_t setSDataBlockFromFetchRsp(SSDataBlock* pRes, SLoadRemoteDataInfo* pLoadInfo, int32_t numOfRows,
                                         char* pData, int32_t compLen, int32_t numOfOutput, int64_t startTs,
                                         uint64_t* total, SArray* pColList) {
  blockDataEnsureCapacity(pRes, numOfRows);

  if (pColList == NULL) {  // data from other sources
    int32_t* colLen = (int32_t*)pData;
    char*    pStart = pData + sizeof(int32_t) * numOfOutput;

    for (int32_t i = 0; i < numOfOutput; ++i) {
      colLen[i] = htonl(colLen[i]);
      ASSERT(colLen[i] > 0);

      SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, i);
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        pColInfoData->varmeta.length = colLen[i];
        pColInfoData->varmeta.allocLen = colLen[i];

        memcpy(pColInfoData->varmeta.offset, pStart, sizeof(int32_t) * numOfRows);
        pStart += sizeof(int32_t) * numOfRows;

        pColInfoData->pData = taosMemoryMalloc(colLen[i]);
      } else {
        memcpy(pColInfoData->nullbitmap, pStart, BitmapLen(numOfRows));
        pStart += BitmapLen(numOfRows);
      }

      memcpy(pColInfoData->pData, pStart, colLen[i]);
      pStart += colLen[i];
    }
  } else {  // extract data according to pColList
    ASSERT(numOfOutput == taosArrayGetSize(pColList));

    // data from mnode
    for (int32_t i = 0; i < numOfOutput; ++i) {
      for (int32_t j = 0; j < numOfOutput; ++j) {
        int16_t colIndex = *(int16_t*)taosArrayGet(pColList, j);
        if (colIndex - 1 == i) {
          SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, j);

          for (int32_t k = 0; k < numOfRows; ++k) {
            colDataAppend(pColInfoData, k, pData, false);
            pData += pColInfoData->info.bytes;
          }
          break;
        }
      }
    }
  }

  pRes->info.rows = numOfRows;

  int64_t el = taosGetTimestampUs() - startTs;

  pLoadInfo->totalRows += numOfRows;
  pLoadInfo->totalSize += compLen;

  if (total != NULL) {
    *total += numOfRows;
  }

  pLoadInfo->totalElapsed += el;
  return TSDB_CODE_SUCCESS;
}

static void* setAllSourcesCompleted(SOperatorInfo* pOperator, int64_t startTs) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int64_t              el = taosGetTimestampUs() - startTs;
  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  pLoadInfo->totalElapsed += el;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  qDebug("%s all %" PRIzu " sources are exhausted, total rows: %" PRIu64 " bytes:%" PRIu64 ", elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize,
         pLoadInfo->totalElapsed / 1000.0);

  doSetOperatorCompleted(pOperator);
  return NULL;
}

static SSDataBlock* concurrentlyLoadRemoteDataImpl(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                                   SExecTaskInfo* pTaskInfo) {
  int32_t code = 0;
  int64_t startTs = taosGetTimestampUs();
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  while (1) {
    int32_t completed = 0;
    for (int32_t i = 0; i < totalSources; ++i) {
      SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);

      if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
        completed += 1;
        continue;
      }

      if (pDataInfo->status != EX_SOURCE_DATA_READY) {
        continue;
      }

      SRetrieveTableRsp*     pRsp = pDataInfo->pRsp;
      SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, i);

      SSDataBlock*         pRes = pExchangeInfo->pResult;
      SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
      if (pRsp->numOfRows == 0) {
        qDebug("%s vgId:%d, taskID:0x%" PRIx64 " index:%d completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64
               " try next",
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, i + 1, pDataInfo->totalRows,
               pExchangeInfo->loadInfo.totalRows);
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
        completed += 1;
        continue;
      }

      SRetrieveTableRsp* pTableRsp = pDataInfo->pRsp;
      code =
          setSDataBlockFromFetchRsp(pExchangeInfo->pResult, pLoadInfo, pTableRsp->numOfRows, pTableRsp->data,
                                    pTableRsp->compLen, pOperator->numOfOutput, startTs, &pDataInfo->totalRows, NULL);
      if (code != 0) {
        goto _error;
      }

      if (pRsp->completed == 1) {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, rowsOfSource:%" PRIu64
               ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows, pDataInfo->totalRows,
               pLoadInfo->totalRows, pLoadInfo->totalSize, i + 1, totalSources);
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      } else {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows, pLoadInfo->totalRows,
               pLoadInfo->totalSize);
      }

      if (pDataInfo->status != EX_SOURCE_DATA_EXHAUSTED) {
        pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
        if (code != TSDB_CODE_SUCCESS) {
          goto _error;
        }
      }

      return pExchangeInfo->pResult;
    }

    if (completed == totalSources) {
      return setAllSourcesCompleted(pOperator, startTs);
    }
  }

_error:
  pTaskInfo->code = code;
  return NULL;
}

static SSDataBlock* concurrentlyLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    return concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
  }

  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  // Asynchronously send all fetch requests to all sources.
  for (int32_t i = 0; i < totalSources; ++i) {
    int32_t code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
    if (code != TSDB_CODE_SUCCESS) {
      return NULL;
    }
  }

  int64_t endTs = taosGetTimestampUs();
  qDebug("%s send all fetch request to %" PRIzu " sources completed, elapsed:%" PRId64, GET_TASKID(pTaskInfo),
         totalSources, endTs - startTs);

  tsem_wait(&pExchangeInfo->ready);
  pOperator->status = OP_RES_TO_RETURN;
  return concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
}

static int32_t prepareConcurrentlyLoad(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  // Asynchronously send all fetch requests to all sources.
  for (int32_t i = 0; i < totalSources; ++i) {
    int32_t code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      return code;
    }
  }

  int64_t endTs = taosGetTimestampUs();
  qDebug("%s send all fetch request to %" PRIzu " sources completed, elapsed:%" PRId64, GET_TASKID(pTaskInfo),
         totalSources, endTs - startTs);

  tsem_wait(&pExchangeInfo->ready);
  pOperator->cost.openCost = taosGetTimestampUs() - startTs;

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* seqLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  while (1) {
    if (pExchangeInfo->current >= totalSources) {
      return setAllSourcesCompleted(pOperator, startTs);
    }

    doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    tsem_wait(&pExchangeInfo->ready);

    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, taskID:0x%" PRIx64 " error happens, code:%s",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, tstrerror(pDataInfo->code));
      pOperator->pTaskInfo->code = pDataInfo->code;
      return NULL;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
    if (pRsp->numOfRows == 0) {
      qDebug("%s vgId:%d, taskID:0x%" PRIx64 " %d of total completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 " try next",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pExchangeInfo->current + 1,
             pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
      continue;
    }

    SSDataBlock*       pRes = pExchangeInfo->pResult;
    SRetrieveTableRsp* pTableRsp = pDataInfo->pRsp;
    int32_t            code =
        setSDataBlockFromFetchRsp(pExchangeInfo->pResult, pLoadInfo, pTableRsp->numOfRows, pTableRsp->data,
                                  pTableRsp->compLen, pOperator->numOfOutput, startTs, &pDataInfo->totalRows, NULL);

    if (pRsp->completed == 1) {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, rowsOfSource:%" PRIu64
             ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows, pDataInfo->totalRows,
             pLoadInfo->totalRows, pLoadInfo->totalSize, pExchangeInfo->current + 1, totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, totalRows:%" PRIu64
             ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows, pLoadInfo->totalRows,
             pLoadInfo->totalSize);
    }

    return pExchangeInfo->pResult;
  }
}

static int32_t prepareLoadRemoteData(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SExchangeInfo* pExchangeInfo = pOperator->info;
  if (pExchangeInfo->seqLoadData) {
    // do nothing for sequentially load data
  } else {
    int32_t code = prepareConcurrentlyLoad(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doLoadRemoteData(SOperatorInfo* pOperator, bool* newgroup) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  size_t               totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    qDebug("%s all %" PRIzu " source(s) are exhausted, total rows:%" PRIu64 " bytes:%" PRIu64 ", elapsed:%.2f ms",
           GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize,
           pLoadInfo->totalElapsed / 1000.0);
    return NULL;
  }

  *newgroup = false;

  if (pExchangeInfo->seqLoadData) {
    return seqLoadRemoteData(pOperator);
  } else {
    return concurrentlyLoadRemoteData(pOperator);
  }

#if 0
  _error:
  taosMemoryFreeClear(pMsg);
  taosMemoryFreeClear(pMsgSendInfo);

  terrno = pTaskInfo->code;
  return NULL;
#endif
}

static int32_t initDataSource(int32_t numOfSources, SExchangeInfo* pInfo) {
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.pEx = pInfo;
    dataInfo.index = i;

    void* ret = taosArrayPush(pInfo->pSourceDataInfo, &dataInfo);
    if (ret == NULL) {
      taosArrayDestroy(pInfo->pSourceDataInfo);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createExchangeOperatorInfo(const SNodeList* pSources, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  SExchangeInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  size_t numOfSources = LIST_LENGTH(pSources);
  pInfo->pSources = taosArrayInit(numOfSources, sizeof(SDownstreamSourceNode));
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL || pInfo->pSources == NULL) {
    goto _error;
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SNodeListNode* pNode = nodesListGetNode((SNodeList*)pSources, i);
    taosArrayPush(pInfo->pSources, pNode);
  }

  int32_t code = initDataSource(numOfSources, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pResult     = pBlock;
  pInfo->seqLoadData = true;

  tsem_init(&pInfo->ready, 0, 0);

  pOperator->name         = "ExchangeOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = pBlock->info.numOfCols;
  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->_openFn      = prepareLoadRemoteData;  // assign a dummy function.
  pOperator->getNextFn    = doLoadRemoteData;
  pOperator->closeFn      = destroyExchangeOperatorInfo;

#if 1
  {  // todo refactor
    SRpcInit rpcInit;
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = "EX";
    rpcInit.numOfThreads = 1;
    rpcInit.cfp = qProcessFetchRsp;
    rpcInit.sessions = tsMaxConnections;
    rpcInit.connType = TAOS_CONN_CLIENT;
    rpcInit.user = (char*)"root";
    rpcInit.idleTime = tsShellActivityTimer * 1000;
    rpcInit.ckey = "key";
    rpcInit.spi = 1;
    rpcInit.secret = (char*)"dcc5bed04851fec854c035b2e40263b6";

    pInfo->pTransporter = rpcOpen(&rpcInit);
    if (pInfo->pTransporter == NULL) {
      return NULL;  // todo
    }
  }
#endif

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyExchangeOperatorInfo(pInfo, numOfSources);
  }

  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

static int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize, const char* pKey);
static void    cleanupAggSup(SAggSupporter* pAggSup);

static void destroySortedMergeOperatorInfo(void* param, int32_t numOfOutput) {
  SSortedMergeOperatorInfo* pInfo = (SSortedMergeOperatorInfo*)param;
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->groupInfo);

  if (pInfo->pSortHandle != NULL) {
    tsortDestroySortHandle(pInfo->pSortHandle);
  }

  blockDataDestroy(pInfo->binfo.pRes);
  cleanupAggSup(&pInfo->aggSup);
}

static void destroySlimitOperatorInfo(void* param, int32_t numOfOutput) {
  SSLimitOperatorInfo* pInfo = (SSLimitOperatorInfo*)param;
  taosArrayDestroy(pInfo->orderColumnList);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  taosMemoryFreeClear(pInfo->prevRow);
}

static void assignExprInfo(SExprInfo* dst, const SExprInfo* src) {
  assert(dst != NULL && src != NULL);

  *dst = *src;

  dst->pExpr = exprdup(src->pExpr);
  dst->base.pParam = taosMemoryCalloc(src->base.numOfParams, sizeof(SColumn));
  memcpy(dst->base.pParam, src->base.pParam, sizeof(SColumn) * src->base.numOfParams);

  //  memset(dst->base.param, 0, sizeof(SVariant) * tListLen(dst->base.param));
  //  for (int32_t j = 0; j < src->base.numOfParams; ++j) {
  //    taosVariantAssign(&dst->base.param[j], &src->base.param[j]);
  //  }
}

static SExprInfo* exprArrayDup(SArray* pExprList) {
  size_t numOfOutput = taosArrayGetSize(pExprList);

  SExprInfo* p = taosMemoryCalloc(numOfOutput, sizeof(SExprInfo));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprList, i);
    assignExprInfo(&p[i], pExpr);
  }

  return p;
}

// TODO merge aggregate super table
static void appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    bool isNull = tsortIsNullVal(pTupleHandle, i);
    if (isNull) {
      colDataAppend(pColInfo, pBlock->info.rows, NULL, true);
    } else {
      char* pData = tsortGetValue(pTupleHandle, i);
      colDataAppend(pColInfo, pBlock->info.rows, pData, false);
    }
  }

  pBlock->info.rows += 1;
}

SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity) {
  blockDataCleanup(pDataBlock);
  blockDataEnsureCapacity(pDataBlock, capacity);

  blockDataEnsureCapacity(pDataBlock, capacity);

  while (1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(pDataBlock, pTupleHandle);
    if (pDataBlock->info.rows >= capacity) {
      return pDataBlock;
    }
  }

  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

SSDataBlock* loadNextDataBlock(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  bool           newgroup = false;
  return pOperator->getNextFn(pOperator, &newgroup);
}

static bool needToMerge(SSDataBlock* pBlock, SArray* groupInfo, char** buf, int32_t rowIndex) {
  size_t size = taosArrayGetSize(groupInfo);
  if (size == 0) {
    return true;
  }

  for (int32_t i = 0; i < size; ++i) {
    int32_t* index = taosArrayGet(groupInfo, i);

    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, *index);
    bool             isNull = colDataIsNull(pColInfo, rowIndex, pBlock->info.rows, NULL);

    if ((isNull && buf[i] != NULL) || (!isNull && buf[i] == NULL)) {
      return false;
    }

    char* pCell = colDataGetData(pColInfo, rowIndex);
    if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
      if (varDataLen(pCell) != varDataLen(buf[i])) {
        return false;
      } else {
        if (memcmp(varDataVal(pCell), varDataVal(buf[i]), varDataLen(pCell)) != 0) {
          return false;
        }
      }
    } else {
      if (memcmp(pCell, buf[i], pColInfo->info.bytes) != 0) {
        return false;
      }
    }
  }

  return 0;
}

static void doMergeResultImpl(SSortedMergeOperatorInfo* pInfo, SqlFunctionCtx* pCtx, int32_t numOfExpr,
                              int32_t rowIndex) {
  for (int32_t j = 0; j < numOfExpr; ++j) {  // TODO set row index
    pCtx[j].startRow = rowIndex;
  }

  for (int32_t j = 0; j < numOfExpr; ++j) {
    int32_t functionId = pCtx[j].functionId;
    //    pCtx[j].fpSet->addInput(&pCtx[j]);

    //    if (functionId < 0) {
    //      SUdfInfo* pUdfInfo = taosArrayGet(pInfo->udfInfo, -1 * functionId - 1);
    //      doInvokeUdf(pUdfInfo, &pCtx[j], 0, TSDB_UDF_FUNC_MERGE);
    //    } else {
    //      assert(!TSDB_FUNC_IS_SCALAR(functionId));
    //      aAggs[functionId].mergeFunc(&pCtx[j]);
    //    }
  }
}

static void doFinalizeResultImpl(SqlFunctionCtx* pCtx, int32_t numOfExpr) {
  for (int32_t j = 0; j < numOfExpr; ++j) {
    int32_t functionId = pCtx[j].functionId;
    //    if (functionId == FUNC_TAG_DUMMY || functionId == FUNC_TS_DUMMY) {
    //      continue;
    //    }

    //    if (functionId < 0) {
    //      SUdfInfo* pUdfInfo = taosArrayGet(pInfo->udfInfo, -1 * functionId - 1);
    //      doInvokeUdf(pUdfInfo, &pCtx[j], 0, TSDB_UDF_FUNC_FINALIZE);
    //    } else {
    pCtx[j].fpSet.finalize(&pCtx[j]);
  }
}

static bool saveCurrentTuple(char** rowColData, SArray* pColumnList, SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t size = (int32_t)taosArrayGetSize(pColumnList);

  for (int32_t i = 0; i < size; ++i) {
    int32_t*         index = taosArrayGet(pColumnList, i);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, *index);

    char* data = colDataGetData(pColInfo, rowIndex);
    memcpy(rowColData[i], data, colDataGetLength(pColInfo, rowIndex));
  }

  return true;
}

static void doMergeImpl(SOperatorInfo* pOperator, int32_t numOfExpr, SSDataBlock* pBlock) {
  SSortedMergeOperatorInfo* pInfo = pOperator->info;

  SqlFunctionCtx* pCtx = pInfo->binfo.pCtx;
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    pCtx[i].size = 1;
  }

  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (!pInfo->hasGroupVal) {
      ASSERT(i == 0);
      doMergeResultImpl(pInfo, pCtx, numOfExpr, i);
      pInfo->hasGroupVal = saveCurrentTuple(pInfo->groupVal, pInfo->groupInfo, pBlock, i);
    } else {
      if (needToMerge(pBlock, pInfo->groupInfo, pInfo->groupVal, i)) {
        doMergeResultImpl(pInfo, pCtx, numOfExpr, i);
      } else {
        doFinalizeResultImpl(pCtx, numOfExpr);
        int32_t numOfRows = getNumOfResult(pInfo->binfo.pCtx, pOperator->numOfOutput, NULL);
        //        setTagValueForMultipleRows(pCtx, pOperator->numOfOutput, numOfRows);

        // TODO check for available buffer;

        // next group info data
        pInfo->binfo.pRes->info.rows += numOfRows;
        for (int32_t j = 0; j < numOfExpr; ++j) {
          if (pCtx[j].functionId < 0) {
            continue;
          }

          pCtx[j].fpSet.process(&pCtx[j]);
        }

        doMergeResultImpl(pInfo, pCtx, numOfExpr, i);
        pInfo->hasGroupVal = saveCurrentTuple(pInfo->groupVal, pInfo->groupInfo, pBlock, i);
      }
    }
  }
}

static SSDataBlock* doMerge(SOperatorInfo* pOperator) {
  SSortedMergeOperatorInfo* pInfo = pOperator->info;
  SSortHandle*              pHandle = pInfo->pSortHandle;

  SSDataBlock* pDataBlock = createOneDataBlock(pInfo->binfo.pRes);
  blockDataEnsureCapacity(pDataBlock, pInfo->binfo.capacity);

  while (1) {
    blockDataCleanup(pDataBlock);
    while (1) {
      STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
      if (pTupleHandle == NULL) {
        break;
      }

      // build datablock for merge for one group
      appendOneRowToDataBlock(pDataBlock, pTupleHandle);
      if (pDataBlock->info.rows >= pInfo->binfo.capacity) {
        break;
      }
    }

    if (pDataBlock->info.rows == 0) {
      break;
    }

    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pDataBlock, TSDB_ORDER_ASC);
    //  updateOutputBuf(&pInfo->binfo, &pAggInfo->bufCapacity, pBlock->info.rows * pAggInfo->resultRowFactor,
    //  pOperator->pRuntimeEnv, true);
    doMergeImpl(pOperator, pOperator->numOfOutput, pDataBlock);
    // flush to tuple store, and after all data have been handled, return to upstream node or sink node
  }

  doFinalizeResultImpl(pInfo->binfo.pCtx, pOperator->numOfOutput);
  int32_t numOfRows = getNumOfResult(pInfo->binfo.pCtx, pOperator->numOfOutput, NULL);
  //        setTagValueForMultipleRows(pCtx, pOperator->numOfOutput, numOfRows);

  // TODO check for available buffer;

  // next group info data
  pInfo->binfo.pRes->info.rows += numOfRows;
  return (pInfo->binfo.pRes->info.rows > 0) ? pInfo->binfo.pRes : NULL;
}

static SSDataBlock* doSortedMerge(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SSortedMergeOperatorInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    return getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pInfo->binfo.capacity);
  }

  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, NULL, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->binfo.pRes, "GET_TASKID(pTaskInfo)");

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    ps->param = pOperator->pDownstream[i];
    tsortAddSource(pInfo->pSortHandle, ps);
  }

  int32_t code = tsortOpen(pInfo->pSortHandle);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
  }

  pOperator->status = OP_RES_TO_RETURN;
  return doMerge(pOperator);
}

static int32_t initGroupCol(SExprInfo* pExprInfo, int32_t numOfCols, SArray* pGroupInfo,
                            SSortedMergeOperatorInfo* pInfo) {
  if (pGroupInfo == NULL || taosArrayGetSize(pGroupInfo) == 0) {
    return 0;
  }

  int32_t len = 0;
  SArray* plist = taosArrayInit(3, sizeof(SColumn));
  pInfo->groupInfo = taosArrayInit(3, sizeof(int32_t));

  if (plist == NULL || pInfo->groupInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  size_t numOfGroupCol = taosArrayGetSize(pInfo->groupInfo);
  for (int32_t i = 0; i < numOfGroupCol; ++i) {
    SColumn* pCol = taosArrayGet(pGroupInfo, i);
    for (int32_t j = 0; j < numOfCols; ++j) {
      SExprInfo* pe = &pExprInfo[j];
      if (pe->base.resSchema.slotId == pCol->colId) {
        taosArrayPush(plist, pCol);
        taosArrayPush(pInfo->groupInfo, &j);
        len += pCol->bytes;
        break;
      }
    }
  }

  ASSERT(taosArrayGetSize(pGroupInfo) == taosArrayGetSize(plist));

  pInfo->groupVal = taosMemoryCalloc(1, (POINTER_BYTES * numOfGroupCol + len));
  if (pInfo->groupVal == NULL) {
    taosArrayDestroy(plist);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t offset = 0;
  char*   start = (char*)(pInfo->groupVal + (POINTER_BYTES * numOfGroupCol));
  for (int32_t i = 0; i < numOfGroupCol; ++i) {
    pInfo->groupVal[i] = start + offset;
    SColumn* pCol = taosArrayGet(plist, i);
    offset += pCol->bytes;
  }

  taosArrayDestroy(plist);

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createSortedMergeOperatorInfo(SOperatorInfo** downstream, int32_t numOfDownstream, SExprInfo* pExprInfo,
                                             int32_t num, SArray* pSortInfo, SArray* pGroupInfo,
                                             SExecTaskInfo* pTaskInfo) {
  SSortedMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortedMergeOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->binfo.pCtx = createSqlFunctionCtx(pExprInfo, num, &pInfo->binfo.rowCellInfoOffset);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)1);

  if (pInfo->binfo.pCtx == NULL || pInfo->binfo.pRes == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t code = doInitAggInfoSup(&pInfo->aggSup, pInfo->binfo.pCtx, num, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setFunctionResultOutput(&pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, pTaskInfo);
  code = initGroupCol(pExprInfo, num, pGroupInfo, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  //  pInfo->resultRowFactor = (int32_t)(getRowNumForMultioutput(pRuntimeEnv->pQueryAttr,
  //      pRuntimeEnv->pQueryAttr->topBotQuery, false));
  pInfo->sortBufSize = 1024 * 16;  // 1MB
  pInfo->bufPageSize = 1024;
  pInfo->pSortInfo = pSortInfo;

  pInfo->binfo.capacity = blockDataGetCapacityInRow(pInfo->binfo.pRes, pInfo->bufPageSize);

  pOperator->name = "SortedMerge";
  // pOperator->operatorType = OP_SortedMerge;
  pOperator->blockingOptr = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfOutput = num;
  pOperator->pExpr = pExprInfo;

  pOperator->pTaskInfo = pTaskInfo;
  pOperator->getNextFn = doSortedMerge;
  pOperator->closeFn = destroySortedMergeOperatorInfo;

  code = appendDownstream(pOperator, downstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroySortedMergeOperatorInfo(pInfo, num);
  }

  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
  return NULL;
}

static SSDataBlock* doSort(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSortOperatorInfo* pInfo = pOperator->info;

  if (pOperator->status == OP_RES_TO_RETURN) {
    return getSortedBlockData(pInfo->pSortHandle, pInfo->pDataBlock, pInfo->numOfRowsInRes);
  }

  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, pInfo->inputSlotMap, SORT_SINGLESOURCE_SORT, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->pDataBlock, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock);

  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  ps->param = pOperator->pDownstream[0];
  tsortAddSource(pInfo->pSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pSortHandle);
  taosMemoryFreeClear(ps);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
  }

  pOperator->status = OP_RES_TO_RETURN;
  return getSortedBlockData(pInfo->pSortHandle, pInfo->pDataBlock, pInfo->numOfRowsInRes);
}

SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSDataBlock* pResBlock, SArray* pSortInfo, SArray* pIndexMap, SExecTaskInfo* pTaskInfo) {
  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  int32_t rowSize = pResBlock->info.rowSize;

  if (pInfo == NULL || pOperator == NULL || rowSize > 100 * 1024 * 1024) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->bufPageSize = rowSize < 1024 ? 1024*2 : rowSize*2; // there are headers, so pageSize = rowSize + header

  pInfo->sortBufSize = pInfo->bufPageSize * 16;  // TODO dynamic set the available sort buffer
  pInfo->numOfRowsInRes = 1024;
  pInfo->pDataBlock = pResBlock;
  pInfo->pSortInfo = pSortInfo;
  pInfo->inputSlotMap = pIndexMap;

  pOperator->name         = "SortOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SORT;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;

  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->getNextFn    = doSort;
  pOperator->closeFn      = destroyOrderOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

static int32_t getTableScanOrder(STableScanInfo* pTableScanInfo) { return pTableScanInfo->order; }

// this is a blocking operator
static int32_t doOpenAggregateOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo*   pInfo = &pAggInfo->binfo;

  int32_t        order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  bool newgroup = true;
  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, &newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }
    //    if (pAggInfo->current != NULL) {
    //      setTagValue(pOperator, pAggInfo->current->pTable, pInfo->pCtx, pOperator->numOfOutput);
    //    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, order);
    doAggregateImpl(pOperator, 0, pInfo->pCtx);

#if 0   // test for encode/decode result info
    if(pOperator->encodeResultRow){
      char *result = NULL;
      int32_t length = 0;
      SAggSupporter   *pSup = &pAggInfo->aggSup;
      pOperator->encodeResultRow(pOperator, pSup, pInfo, &result, &length);
      taosHashClear(pSup->pResultRowHashTable);
      pInfo->resultRowInfo.size = 0;
      pOperator->decodeResultRow(pOperator, pSup, pInfo, result, length);
      if(result){
        taosMemoryFree(result);
      }
    }
#endif
  }

  finalizeQueryResult(pInfo->pCtx, pOperator->numOfOutput);

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* getAggregateResult(SOperatorInfo* pOperator, bool* newgroup) {
  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo*   pInfo = &pAggInfo->binfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  getNumOfResult(pInfo->pCtx, pOperator->numOfOutput, pInfo->pRes);
  doSetOperatorCompleted(pOperator);

  return (blockDataGetNumOfRows(pInfo->pRes) != 0) ? pInfo->pRes : NULL;
}

void aggEncodeResultRow(SOperatorInfo* pOperator, SAggSupporter *pSup, SOptrBasicInfo *pInfo, char **result, int32_t *length) {
  int32_t size = taosHashGetSize(pSup->pResultRowHashTable);
  size_t  keyLen = sizeof(uint64_t) * 2;  // estimate the key length
  int32_t totalSize = sizeof(int32_t) + size * (sizeof(int32_t) + keyLen + sizeof(int32_t) + pSup->resultRowSize);
  *result = taosMemoryCalloc(1, totalSize);
  if (*result == NULL) {
    longjmp(pOperator->pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
  }
  *(int32_t*)(*result) = size;
  int32_t offset = sizeof(int32_t);

  // prepare memory
  SResultRowPosition* pos = &pInfo->resultRowInfo.pPosition[pInfo->resultRowInfo.curPos];
  void* pPage = getBufPage(pSup->pResultBuf, pos->pageId);
  SResultRow* pRow = (SResultRow*)((char*)pPage + pos->offset);
  setBufPageDirty(pPage, true);
  releaseBufPage(pSup->pResultBuf, pPage);

  void*   pIter = taosHashIterate(pSup->pResultRowHashTable, NULL);
  while (pIter) {
    void*        key = taosHashGetKey(pIter, &keyLen);
    SResultRowPosition* p1 = (SResultRowPosition*)pIter;

    pPage = (SFilePage*) getBufPage(pSup->pResultBuf, p1->pageId);
    pRow = (SResultRow*)((char*)pPage + p1->offset);
    setBufPageDirty(pPage, true);
    releaseBufPage(pSup->pResultBuf, pPage);

    // recalculate the result size
    int32_t realTotalSize = offset + sizeof(int32_t) + keyLen + sizeof(int32_t) + pSup->resultRowSize;
    if (realTotalSize > totalSize) {
      char* tmp = taosMemoryRealloc(*result, realTotalSize);
      if (tmp == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        taosMemoryFree(*result);
        *result = NULL;
        longjmp(pOperator->pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
      } else {
        *result = tmp;
      }
    }
    // save key
    *(int32_t*)(*result + offset) = keyLen;
    offset += sizeof(int32_t);
    memcpy(*result + offset, key, keyLen);
    offset += keyLen;

    // save value
    *(int32_t*)(*result + offset) = pSup->resultRowSize;
    offset += sizeof(int32_t);
    memcpy(*result + offset, pRow, pSup->resultRowSize);
    offset += pSup->resultRowSize;

    pIter = taosHashIterate(pSup->pResultRowHashTable, pIter);
  }

  if (length) {
    *length = offset;
  }
  return;
}

bool aggDecodeResultRow(SOperatorInfo* pOperator, SAggSupporter *pSup, SOptrBasicInfo *pInfo, char* result, int32_t length) {
  if (!result || length <= 0) {
    return false;
  }

  //  int32_t size = taosHashGetSize(pSup->pResultRowHashTable);
  int32_t count = *(int32_t*)(result);

  int32_t offset = sizeof(int32_t);
  while (count-- > 0 && length > offset) {
    int32_t keyLen = *(int32_t*)(result + offset);
    offset += sizeof(int32_t);

    uint64_t    tableGroupId = *(uint64_t*)(result + offset);
    SResultRow* resultRow = getNewResultRow_rv(pSup->pResultBuf, tableGroupId, pSup->resultRowSize);
    if (!resultRow) {
      longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_INVALID_INPUT);
    }
    // add a new result set for a new group
    SResultRowPosition pos = {.pageId = resultRow->pageId, .offset = resultRow->offset};
    taosHashPut(pSup->pResultRowHashTable, result + offset, keyLen, &pos, sizeof(SResultRowPosition));

    offset += keyLen;
    int32_t valueLen = *(int32_t*)(result + offset);
    if (valueLen != pSup->resultRowSize) {
      longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_INVALID_INPUT);
    }
    offset += sizeof(int32_t);
    int32_t pageId = resultRow->pageId;
    int32_t pOffset = resultRow->offset;
    memcpy(resultRow, result + offset, valueLen);
    resultRow->pageId = pageId;
    resultRow->offset = pOffset;
    offset += valueLen;

    initResultRow(resultRow);
    prepareResultListBuffer(&pInfo->resultRowInfo, pOperator->pTaskInfo->env);
    pInfo->resultRowInfo.curPos = pInfo->resultRowInfo.size;
    pInfo->resultRowInfo.pPosition[pInfo->resultRowInfo.size++] = (SResultRowPosition) {.pageId = resultRow->pageId, .offset = resultRow->offset};
  }

  if (offset != length) {
    longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_INVALID_INPUT);
  }
  return true;
}

static SSDataBlock* doMultiTableAggregate(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo*   pInfo = &pAggInfo->binfo;
  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSDatablock(pInfo->pRes, pAggInfo->binfo.capacity, &pAggInfo->groupResInfo, pOperator->pExpr, pAggInfo->pResultBuf, pAggInfo->binfo.rowCellInfoOffset);
    if (pInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pAggInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pInfo->pRes;
  }

  // table scan order
  int32_t        order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    //    setTagValue(pOperator, pRuntimeEnv->current->pTable, pInfo->pCtx, pOperator->numOfOutput);
    //    if (downstream->operatorType == OP_TableScan) {
    //      STableScanInfo* pScanInfo = downstream->info;
    //      order = getTableScanOrder(pScanInfo);
    //    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, order);

    TSKEY key = 0;
    if (order == TSDB_ORDER_ASC) {
      key = pBlock->info.window.ekey;
      TSKEY_MAX_ADD(key, 1);
    } else {
      key = pBlock->info.window.skey;
      TSKEY_MIN_SUB(key, -1);
    }

    setExecutionContext(pOperator->numOfOutput, pAggInfo->current->groupIndex, key, pTaskInfo, pAggInfo->current,
                        pAggInfo);
    doAggregateImpl(pOperator, 0, pInfo->pCtx);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->resultRowInfo);
  updateNumOfRowsInResultRows(pInfo->pCtx, pOperator->numOfOutput, &pInfo->resultRowInfo, pInfo->rowCellInfoOffset);

  initGroupResInfo(&pAggInfo->groupResInfo, &pInfo->resultRowInfo);
  toSDatablock(pInfo->pRes, pAggInfo->binfo.capacity, &pAggInfo->groupResInfo, pOperator->pExpr, pAggInfo->pResultBuf, pAggInfo->binfo.rowCellInfoOffset);

  if (pInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pAggInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  return pInfo->pRes;
}

static SSDataBlock* doProjectOperation(SOperatorInfo* pOperator, bool* newgroup) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;

  SSDataBlock* pRes = pInfo->pRes;
  blockDataCleanup(pRes);
  
#if 0
  if (pProjectInfo->existDataBlock) {  // TODO refactor
    SSDataBlock* pBlock = pProjectInfo->existDataBlock;
    pProjectInfo->existDataBlock = NULL;
    *newgroup = true;

    // todo dynamic set tags
    //    if (pTableQueryInfo != NULL) {
    //      setTagValue(pOperator, pTableQueryInfo->pTable, pInfo->pCtx, pOperator->numOfOutput);
    //    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, TSDB_ORDER_ASC);

    blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
    projectApplyFunctions(pOperator->pExpr, pInfo->pRes, pBlock, pInfo->pCtx, pOperator->numOfOutput);
    if (pRes->info.rows >= pProjectInfo->binfo.capacity * 0.8) {
      copyTsColoum(pRes, pInfo->pCtx, pOperator->numOfOutput);
      resetResultRowEntryResult(pInfo->pCtx, pOperator->numOfOutput);
      return pRes;
    }
  }
#endif

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    bool prevVal = *newgroup;

    // The downstream exec may change the value of the newgroup, so use a local variable instead.
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      *newgroup = prevVal;
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      break;
    }

    // Return result of the previous group in the firstly.
    if (*newgroup) {
      if (pRes->info.rows > 0) {
        pProjectInfo->existDataBlock = pBlock;
        break;
      } else {  // init output buffer for a new group data
        initCtxOutputBuffer(pInfo->pCtx, pOperator->numOfOutput);
      }
    }

    // todo dynamic set tags
    //    STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
    //    if (pTableQueryInfo != NULL) {
    //      setTagValue(pOperator, pTableQueryInfo->pTable, pInfo->pCtx, pOperator->numOfOutput);
    //    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, TSDB_ORDER_ASC);
    blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);

    projectApplyFunctions(pOperator->pExpr, pInfo->pRes, pBlock, pInfo->pCtx, pOperator->numOfOutput, pProjectInfo->pPseudoColInfo);

    if (pProjectInfo->curSOffset > 0) {
      if (pProjectInfo->groupId == 0) {  // it is the first group
        pProjectInfo->groupId = pBlock->info.groupId;
        blockDataCleanup(pInfo->pRes);
        continue;
      } else if (pProjectInfo->groupId != pBlock->info.groupId) {
        pProjectInfo->curSOffset -= 1;

        // ignore data block in current group
        if (pProjectInfo->curSOffset > 0) {
          blockDataCleanup(pInfo->pRes);
          continue;
        }
      }

      pProjectInfo->groupId = pBlock->info.groupId;
    }

    if (pProjectInfo->groupId != 0 && pProjectInfo->groupId != pBlock->info.groupId) {
      pProjectInfo->curGroupOutput += 1;
      if ((pProjectInfo->slimit.limit > 0) && (pProjectInfo->slimit.limit <= pProjectInfo->curGroupOutput)) {
        pOperator->status = OP_EXEC_DONE;
        return NULL;
      }

      // reset the value for a new group data
      pProjectInfo->curOffset = 0;
      pProjectInfo->curOutput = 0;
    }

    pProjectInfo->groupId = pBlock->info.groupId;

    // todo extract method
    if (pProjectInfo->curOffset < pInfo->pRes->info.rows && pProjectInfo->curOffset > 0) {
      blockDataTrimFirstNRows(pInfo->pRes, pProjectInfo->curOffset);
      pProjectInfo->curOffset = 0;
    } else if (pProjectInfo->curOffset >= pInfo->pRes->info.rows) {
      pProjectInfo->curOffset -= pInfo->pRes->info.rows;
      blockDataCleanup(pInfo->pRes);
      continue;
    }

    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
  
  if (pProjectInfo->limit.limit > 0 && pProjectInfo->curOutput + pInfo->pRes->info.rows >= pProjectInfo->limit.limit) {
    pInfo->pRes->info.rows = (int32_t)(pProjectInfo->limit.limit - pProjectInfo->curOutput);
  }

  pProjectInfo->curOutput += pInfo->pRes->info.rows;

  //  copyTsColoum(pRes, pInfo->pCtx, pOperator->numOfOutput);
  return (pInfo->pRes->info.rows > 0) ? pInfo->pRes : NULL;
}

static int32_t doOpenIntervalAgg(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  STableIntervalOperatorInfo* pInfo = pOperator->info;

  int32_t order = TSDB_ORDER_ASC;
  //  STimeWindow win = {0};
  bool newgroup = false;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, &newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    //    setTagValue(pOperator, pRuntimeEnv->current->pTable, pInfo->pCtx, pOperator->numOfOutput);
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, order);
    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, 0);

#if 0   // test for encode/decode result info
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
  finalizeMultiTupleQueryResult(pInfo->binfo.pCtx, pOperator->numOfOutput, pInfo->aggSup.pResultBuf,
                                &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);

  initGroupResInfo(&pInfo->groupResInfo, &pInfo->binfo.resultRowInfo);
  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doBuildIntervalResult(SOperatorInfo* pOperator, bool* newgroup) {
  STableIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    return pOperator->getStreamResFn(pOperator, newgroup);
  }

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  blockDataEnsureCapacity(pInfo->binfo.pRes, pInfo->binfo.capacity);
  toSDatablock(pInfo->binfo.pRes, pInfo->binfo.capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pInfo->binfo.rowCellInfoOffset);

  if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
}

static SSDataBlock* doStreamIntervalAgg(SOperatorInfo *pOperator, bool* newgroup) {
  STableIntervalOperatorInfo* pInfo = pOperator->info;
  int32_t order = TSDB_ORDER_ASC;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSDatablock(pInfo->binfo.pRes, pInfo->binfo.capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pInfo->binfo.rowCellInfoOffset);
    if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }
    return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
  }

  //  STimeWindow win = {0};
  *newgroup = false;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  SArray* pUpdated = NULL;

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    // The timewindows that overlaps the timestamps of the input pBlock need to be recalculated and return to the caller.
    // Note that all the time window are not close till now.

    //    setTagValue(pOperator, pRuntimeEnv->current->pTable, pInfo->pCtx, pOperator->numOfOutput);
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, order);
    pUpdated = hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, 0);
  }

  finalizeUpdatedResult(pInfo->binfo.pCtx, pOperator->numOfOutput, pInfo->aggSup.pResultBuf, pUpdated, pInfo->binfo.rowCellInfoOffset);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pUpdated);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pInfo->binfo.capacity);
  toSDatablock(pInfo->binfo.pRes, pInfo->binfo.capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pInfo->binfo.rowCellInfoOffset);

  ASSERT(pInfo->binfo.pRes->info.rows > 0);
  pOperator->status = OP_RES_TO_RETURN;

  return pInfo->binfo.pRes->info.rows == 0 ? NULL : pInfo->binfo.pRes;
}

static SSDataBlock* doAllIntervalAgg(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    //    toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);
    if (pSliceInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pSliceInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }

    return pSliceInfo->binfo.pRes;
  }

  int32_t     order = TSDB_ORDER_ASC;
//  STimeWindow win = pQueryAttr->window;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);
    if (pBlock == NULL) {
      break;
    }

    //    setTagValue(pOperator, pRuntimeEnv->current->pTable, pIntervalInfo->pCtx, pOperator->numOfOutput);
    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pSliceInfo->binfo.pCtx, pBlock, order);
    hashAllIntervalAgg(pOperator, &pSliceInfo->binfo.resultRowInfo, pBlock, 0);
  }

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pSliceInfo->binfo.resultRowInfo);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  finalizeQueryResult(pSliceInfo->binfo.pCtx, pOperator->numOfOutput);

  initGroupResInfo(&pSliceInfo->groupResInfo, &pSliceInfo->binfo.resultRowInfo);
  //  toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pSliceInfo->pRes);

  if (pSliceInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pSliceInfo->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pSliceInfo->binfo.pRes->info.rows == 0 ? NULL : pSliceInfo->binfo.pRes;
}

static SSDataBlock* doSTableIntervalAgg(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;
  STaskRuntimeEnv*            pRuntimeEnv = pOperator->pRuntimeEnv;

  if (pOperator->status == OP_RES_TO_RETURN) {
    int64_t st = taosGetTimestampUs();

    //    copyToSDataBlock(NULL, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
    if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }
    return pIntervalInfo->binfo.pRes;
  }

  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t    order = pQueryAttr->order.order;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;

    //    setTagValue(pOperator, pTableQueryInfo->pTable, pIntervalInfo->pCtx, pOperator->numOfOutput);
    setInputDataBlock(pOperator, pIntervalInfo->binfo.pCtx, pBlock, pQueryAttr->order.order);
    setIntervalQueryRange(pRuntimeEnv, pBlock->info.window.skey);

    hashIntervalAgg(pOperator, &pTableQueryInfo->resInfo, pBlock, pTableQueryInfo->groupIndex);
  }

  pOperator->status = OP_RES_TO_RETURN;
  pQueryAttr->order.order = order;  // TODO : restore the order
  doCloseAllTimeWindow(pRuntimeEnv);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);

  //  copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
  if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pIntervalInfo->binfo.pRes;
}

static SSDataBlock* doAllSTableIntervalAgg(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;
  STaskRuntimeEnv*            pRuntimeEnv = pOperator->pRuntimeEnv;

  if (pOperator->status == OP_RES_TO_RETURN) {
    //    copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
    if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pIntervalInfo->binfo.pRes;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;

    //    setTagValue(pOperator, pTableQueryInfo->pTable, pIntervalInfo->pCtx, pOperator->numOfOutput);
//    setInputDataBlock(pOperator, pIntervalInfo->binfo.pCtx, pBlock, pQueryAttr->order.order);
    setIntervalQueryRange(pRuntimeEnv, pBlock->info.window.skey);

    hashAllIntervalAgg(pOperator, &pTableQueryInfo->resInfo, pBlock, pTableQueryInfo->groupIndex);
  }

  pOperator->status = OP_RES_TO_RETURN;
//  pQueryAttr->order.order = order;  // TODO : restore the order
  doCloseAllTimeWindow(pRuntimeEnv);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);

  int64_t st = taosGetTimestampUs();
  //  copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
  if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pIntervalInfo->binfo.pRes;
}

static void doStateWindowAggImpl(SOperatorInfo* pOperator, SStateWindowOperatorInfo* pInfo, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo* pBInfo = &pInfo->binfo;

  SColumnInfoData* pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->colIndex);
  int64_t gid = pBlock->info.groupId;

  bool    masterScan = true;
  int32_t numOfOutput = pOperator->numOfOutput;

  int16_t bytes = pStateColInfoData->info.bytes;
  int16_t type = pStateColInfoData->info.type;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, 0);
  TSKEY*           tsList = (TSKEY*)pColInfoData->pData;

  SWindowRowsSup* pRowSup = &pInfo->winSup;
  pRowSup->numOfRows = 0;
  
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    if (colDataIsNull(pStateColInfoData, pBlock->info.rows, j, pBlock->pBlockAgg)) {
      continue;
    }

    char* val = colDataGetData(pStateColInfoData, j);

    if (!pInfo->hasKey) {
      memcpy(pInfo->stateKey.pData, val, bytes);
      pInfo->hasKey = true;

      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    } else if (memcmp(pInfo->stateKey.pData, val, bytes) == 0) {
      doKeepTuple(pRowSup, tsList[j]);
      if (j == 0 && pRowSup->startRowIndex != 0) {
        pRowSup->startRowIndex = 0;
      }
    } else {  // a new state window started
      SResultRow* pResult = NULL;

      // keep the time window for the closed time window.
      STimeWindow window = pRowSup->win;

      pRowSup->win.ekey = pRowSup->win.skey;
      int32_t ret = setResultOutputBufByKey_rv(&pInfo->binfo.resultRowInfo, pBlock->info.uid, &window, masterScan,
                                               &pResult, gid, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
      }

      updateTimeWindowInfo(&pInfo->timeWindowData, &window, false);
      doApplyFunctions(pInfo->binfo.pCtx, &window, &pInfo->timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

      // here we start a new session window
      doKeepNewWindowStartInfo(pRowSup, tsList, j);
      doKeepTuple(pRowSup, tsList[j]);
    }
  }

  SResultRow* pResult = NULL;
  pRowSup->win.ekey = tsList[pBlock->info.rows - 1];
  int32_t ret = setResultOutputBufByKey_rv(&pInfo->binfo.resultRowInfo, pBlock->info.uid, &pRowSup->win, masterScan, &pResult,
                                           gid, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
  }

  updateTimeWindowInfo(&pInfo->timeWindowData, &pRowSup->win, false);
  doApplyFunctions(pInfo->binfo.pCtx, &pRowSup->win, &pInfo->timeWindowData, pRowSup->startRowIndex, pRowSup->numOfRows, NULL, pBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
}

static SSDataBlock* doStateWindowAgg(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SStateWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo* pBInfo    = &pInfo->binfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSDatablock(pBInfo->pRes, pBInfo->capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pBInfo->rowCellInfoOffset);
    if (pBInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    return pBInfo->pRes;
  }

  int32_t        order = TSDB_ORDER_ASC;
  STimeWindow    win = pTaskInfo->window;

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, order);
    doStateWindowAggImpl(pOperator, pInfo, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pBInfo->resultRowInfo);
  finalizeMultiTupleQueryResult(pBInfo->pCtx, pOperator->numOfOutput, pInfo->aggSup.pResultBuf, &pBInfo->resultRowInfo, pBInfo->rowCellInfoOffset);

  initGroupResInfo(&pInfo->groupResInfo, &pBInfo->resultRowInfo);
  blockDataEnsureCapacity(pBInfo->pRes, pBInfo->capacity);
  toSDatablock(pBInfo->pRes, pBInfo->capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pBInfo->rowCellInfoOffset);
  if (pBInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
}

static SSDataBlock* doSessionWindowAgg(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSessionAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*          pBInfo = &pInfo->binfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSDatablock(pBInfo->pRes, pBInfo->capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pBInfo->rowCellInfoOffset);
    if (pBInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    return pBInfo->pRes;
  }

  int32_t        order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, order);
    doSessionWindowAggImpl(pOperator, pInfo, pBlock);
  }

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pBInfo->resultRowInfo);
  finalizeMultiTupleQueryResult(pBInfo->pCtx, pOperator->numOfOutput, pInfo->aggSup.pResultBuf, &pBInfo->resultRowInfo, pBInfo->rowCellInfoOffset);

  initGroupResInfo(&pInfo->groupResInfo, &pBInfo->resultRowInfo);
  blockDataEnsureCapacity(pBInfo->pRes, pBInfo->capacity);
  toSDatablock(pBInfo->pRes, pBInfo->capacity, &pInfo->groupResInfo, pOperator->pExpr, pInfo->aggSup.pResultBuf, pBInfo->rowCellInfoOffset);
  if (pBInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  return pBInfo->pRes->info.rows == 0 ? NULL : pBInfo->pRes;
}

static void doHandleRemainBlockForNewGroupImpl(SFillOperatorInfo* pInfo, SResultInfo* pResultInfo, bool* newgroup,
                                               SExecTaskInfo* pTaskInfo) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;

  int64_t ekey = Q_STATUS_EQUAL(pTaskInfo->status, TASK_COMPLETED) ? pTaskInfo->window.ekey
                                                                   : pInfo->existNewGroupBlock->info.window.ekey;
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->existNewGroupBlock->info.rows, ekey);
  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->existNewGroupBlock);

  doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pResultInfo->capacity, pInfo->p);
  pInfo->existNewGroupBlock = NULL;
  *newgroup = true;
}

static void doHandleRemainBlockFromNewGroup(SFillOperatorInfo* pInfo, SResultInfo* pResultInfo, bool* newgroup,
                                            SExecTaskInfo* pTaskInfo) {
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    *newgroup = false;
    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, (int32_t)pResultInfo->capacity, pInfo->p);
    if (pInfo->pRes->info.rows > pResultInfo->threshold || (!pInfo->multigroupResult)) {
      return;
    }
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pInfo, pResultInfo, newgroup, pTaskInfo);
  }
}

static SSDataBlock* doFill(SOperatorInfo* pOperator, bool* newgroup) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  blockDataCleanup(pInfo->pRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  doHandleRemainBlockFromNewGroup(pInfo, pResultInfo, newgroup, pTaskInfo);
  if (pInfo->pRes->info.rows > pResultInfo->threshold || (!pInfo->multigroupResult && pInfo->pRes->info.rows > 0)) {
    return pInfo->pRes;
  }

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  while (1) {
    publishOperatorProfEvent(pDownstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = pDownstream->getNextFn(pDownstream, newgroup);
    publishOperatorProfEvent(pDownstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (*newgroup) {
      assert(pBlock != NULL);
    }

    if (*newgroup && pInfo->totalInputRows > 0) {  // there are already processed current group data block
      pInfo->existNewGroupBlock = pBlock;
      *newgroup = false;

      // Fill the previous group data block, before handle the data block of new group.
      // Close the fill operation for previous group data block
      taosFillSetStartInfo(pInfo->pFillInfo, 0, pTaskInfo->window.ekey);
    } else {
      if (pBlock == NULL) {
        if (pInfo->totalInputRows == 0) {
          pOperator->status = OP_EXEC_DONE;
          return NULL;
        }

        taosFillSetStartInfo(pInfo->pFillInfo, 0, pTaskInfo->window.ekey);
      } else {
        pInfo->totalInputRows += pBlock->info.rows;
        taosFillSetStartInfo(pInfo->pFillInfo, pBlock->info.rows, pBlock->info.window.ekey);
        taosFillSetInputDataBlock(pInfo->pFillInfo, pBlock);
      }
    }

    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pInfo->capacity, pInfo->p);

    // current group has no more result to return
    if (pInfo->pRes->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pInfo->pRes->info.rows > pResultInfo->threshold || pBlock == NULL || (!pInfo->multigroupResult)) {
        return pInfo->pRes;
      }

      doHandleRemainBlockFromNewGroup(pInfo, pResultInfo, newgroup, pTaskInfo);
      if (pInfo->pRes->info.rows > pOperator->resultInfo.threshold || pBlock == NULL) {
        return pInfo->pRes;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      assert(pBlock != NULL);
      doHandleRemainBlockForNewGroupImpl(pInfo, pResultInfo, newgroup, pTaskInfo);
      if (pInfo->pRes->info.rows > pResultInfo->threshold) {
        return pInfo->pRes;
      }
    } else {
      return NULL;
    }
  }
}

// todo set the attribute of query scan count
static int32_t getNumOfScanTimes(STaskAttr* pQueryAttr) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functionId = getExprFunctionId(&pQueryAttr->pExpr1[i]);
    if (functionId == FUNCTION_STDDEV || functionId == FUNCTION_PERCT) {
      return 2;
    }
  }

  return 1;
}

static void destroyOperatorInfo(SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return;
  }

  if (pOperator->closeFn != NULL) {
    pOperator->closeFn(pOperator->info, pOperator->numOfOutput);
  }

  if (pOperator->pDownstream != NULL) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      destroyOperatorInfo(pOperator->pDownstream[i]);
    }

    taosMemoryFreeClear(pOperator->pDownstream);
    pOperator->numOfDownstream = 0;
  }

  taosMemoryFreeClear(pOperator->info);
  taosMemoryFreeClear(pOperator);
}

int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize, const char* pKey) {
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  pAggSup->resultRowSize       = getResultRowSize(pCtx, numOfOutput);
  pAggSup->keyBuf              = taosMemoryCalloc(1, keyBufSize);
  pAggSup->pResultRowHashTable = taosHashInit(10, hashFn, true, HASH_NO_LOCK);
  pAggSup->pResultRowListSet   = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  pAggSup->pResultRowArrayList = taosArrayInit(10, sizeof(SResultRowCell));

  if (pAggSup->keyBuf == NULL || pAggSup->pResultRowArrayList == NULL || pAggSup->pResultRowListSet == NULL ||
      pAggSup->pResultRowHashTable == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = createDiskbasedBuf(&pAggSup->pResultBuf, 4096, 4096 * 256, pKey, "/tmp/");
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static void cleanupAggSup(SAggSupporter* pAggSup) {
  taosMemoryFreeClear(pAggSup->keyBuf);
  taosHashCleanup(pAggSup->pResultRowHashTable);
  taosHashCleanup(pAggSup->pResultRowListSet);
  taosArrayDestroy(pAggSup->pResultRowArrayList);
  destroyDiskbasedBuf(pAggSup->pResultBuf);
}

int32_t initAggInfo(SOptrBasicInfo* pBasicInfo, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols,
                           int32_t numOfRows, SSDataBlock* pResultBlock, size_t keyBufSize, const char* pkey) {
  pBasicInfo->pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pBasicInfo->rowCellInfoOffset);
  pBasicInfo->pRes = pResultBlock;
  pBasicInfo->capacity = numOfRows;

  doInitAggInfoSup(pAggSup, pBasicInfo->pCtx, numOfCols, keyBufSize, pkey);
  return TSDB_CODE_SUCCESS;
}

static STableQueryInfo* initTableQueryInfo(const STableGroupInfo* pTableGroupInfo) {
  STableQueryInfo* pTableQueryInfo = taosMemoryCalloc(pTableGroupInfo->numOfTables, sizeof(STableQueryInfo));
  if (pTableQueryInfo == NULL) {
    return NULL;
  }

  int32_t index = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pTableGroupInfo->pGroupList); ++i) {
    SArray* pa = taosArrayGetP(pTableGroupInfo->pGroupList, i);
    for (int32_t j = 0; j < taosArrayGetSize(pa); ++j) {
      STableKeyInfo* pk = taosArrayGet(pa, j);

      STableQueryInfo* pTQueryInfo = &pTableQueryInfo[index++];
      pTQueryInfo->uid = pk->uid;
      pTQueryInfo->lastKey = pk->lastKey;
      pTQueryInfo->groupIndex = i;
    }
  }

  STimeWindow win = {0, INT64_MAX};
  createTableQueryInfo(pTableQueryInfo, false, win);
  return pTableQueryInfo;
}

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, SExecTaskInfo* pTaskInfo,
                                           const STableGroupInfo* pTableGroupInfo) {
  SAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAggOperatorInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  //(int32_t)(getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery));
  int32_t numOfRows = 1;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t code = initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResultBlock, keyBufSize, pTaskInfo->id.str);
  pInfo->pTableQueryInfo = initTableQueryInfo(pTableGroupInfo);
  if (code != TSDB_CODE_SUCCESS || pInfo->pTableQueryInfo == NULL) {
    goto _error;
  }

  setFunctionResultOutput(&pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, pTaskInfo);

  pOperator->name         = "TableAggregate";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_AGG;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;

  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->_openFn      = doOpenAggregateOptr;
  pOperator->getNextFn    = getAggregateResult;
  pOperator->closeFn      = destroyAggOperatorInfo;
  pOperator->encodeResultRow = aggEncodeResultRow;
  pOperator->decodeResultRow = aggDecodeResultRow;

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;
_error:
  destroyAggOperatorInfo(pInfo, numOfCols);
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

void doDestroyBasicInfo(SOptrBasicInfo* pInfo, int32_t numOfOutput) {
  assert(pInfo != NULL);

  destroySqlFunctionCtx(pInfo->pCtx, numOfOutput);
  taosMemoryFreeClear(pInfo->rowCellInfoOffset);

  cleanupResultRowInfo(&pInfo->resultRowInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

void destroyBasicOperatorInfo(void* param, int32_t numOfOutput) {
  SOptrBasicInfo* pInfo = (SOptrBasicInfo*)param;
  doDestroyBasicInfo(pInfo, numOfOutput);
}

void destroyStateWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  taosMemoryFreeClear(pInfo->stateKey.pData);
}

void destroyAggOperatorInfo(void* param, int32_t numOfOutput) {
  SAggOperatorInfo* pInfo = (SAggOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

void destroyIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = (STableIntervalOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  cleanupAggSup(&pInfo->aggSup);
}

void destroySWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SSessionAggOperatorInfo* pInfo = (SSessionAggOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

void destroySFillOperatorInfo(void* param, int32_t numOfOutput) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*)param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  taosMemoryFreeClear(pInfo->p);
}

static void destroyProjectOperatorInfo(void* param, int32_t numOfOutput) {
  SProjectOperatorInfo* pInfo = (SProjectOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

static void destroyTagScanOperatorInfo(void* param, int32_t numOfOutput) {
  STagScanInfo* pInfo = (STagScanInfo*)param;
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

static void destroyOrderOperatorInfo(void* param, int32_t numOfOutput) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  pInfo->pDataBlock = blockDataDestroy(pInfo->pDataBlock);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->inputSlotMap);
}

void destroyExchangeOperatorInfo(void* param, int32_t numOfOutput) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  taosArrayDestroy(pExInfo->pSources);
  taosArrayDestroy(pExInfo->pSourceDataInfo);
  if (pExInfo->pResult != NULL) {
    blockDataDestroy(pExInfo->pResult);
  }

  tsem_destroy(&pExInfo->ready);
}

SOperatorInfo* createMultiTableAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                               SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo,
                                               const STableGroupInfo* pTableGroupInfo) {
  SAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAggOperatorInfo));

  int32_t numOfRows = 1;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t code =
      initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResBlock, keyBufSize, pTaskInfo->id.str);
  pInfo->pTableQueryInfo = initTableQueryInfo(pTableGroupInfo);
  if (code != TSDB_CODE_SUCCESS || pInfo->pTableQueryInfo == NULL) {
    goto _error;
  }

  size_t tableGroup = taosArrayGetSize(pTableGroupInfo->pGroupList);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)tableGroup);

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->name = "MultiTableAggregate";
  // pOperator->operatorType = OP_MultiTableAggregate;
  pOperator->blockingOptr = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pExpr = pExprInfo;
  pOperator->numOfOutput = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->getNextFn = doMultiTableAggregate;
  pOperator->closeFn = destroyAggOperatorInfo;
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  return NULL;
}

static SArray* setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols) {
  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  for(int32_t i = 0; i < numOfCols; ++i) {
    if (fmIsPseudoColumnFunc(pCtx[i].functionId)) {
      taosArrayPush(pList, &i);
    }
  }

  return pList;
}

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t num,
                                         SSDataBlock* pResBlock, SLimit* pLimit, SLimit* pSlimit, SExecTaskInfo* pTaskInfo) {
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->limit      = *pLimit;
  pInfo->slimit     = *pSlimit;
  pInfo->curOffset  = pLimit->offset;
  pInfo->curSOffset = pSlimit->offset;

  pInfo->binfo.pRes = pResBlock;

  int32_t numOfCols = num;
  int32_t numOfRows = 4096;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResBlock, keyBufSize, pTaskInfo->id.str);
  setFunctionResultOutput(&pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, pTaskInfo);
  pInfo->pPseudoColInfo = setRowTsColumnOutputInfo(pInfo->binfo.pCtx, numOfCols);

  pOperator->name         = "ProjectOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_PROJECT;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = num;
  pOperator->_openFn      = operatorDummyOpenFn;
  pOperator->getNextFn    = doProjectOperation;
  pOperator->closeFn      = destroyProjectOperatorInfo;

  pOperator->pTaskInfo = pTaskInfo;
  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlot,
                                          const STableGroupInfo* pTableGroupInfo, SExecTaskInfo* pTaskInfo) {
  STableIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STableIntervalOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->order     = TSDB_ORDER_ASC;
  pInfo->interval  = *pInterval;
  pInfo->execModel = pTaskInfo->execModel;

  pInfo->win       = pTaskInfo->window;
  pInfo->win.skey  = 0;
  pInfo->win.ekey  = INT64_MAX;

  pInfo->primaryTsIndex = primaryTsSlot;

  int32_t numOfRows = 4096;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t code = initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResBlock, keyBufSize, pTaskInfo->id.str);
  initExecTimeWindowInfo(&pInfo->timeWindowData, &pInfo->win);

  //  pInfo->pTableQueryInfo = initTableQueryInfo(pTableGroupInfo);
  if (code != TSDB_CODE_SUCCESS /* || pInfo->pTableQueryInfo == NULL*/) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)1);

  pOperator->name         = "TimeIntervalAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_INTERVAL;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExprInfo;
  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->numOfOutput  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->_openFn      = doOpenIntervalAgg;
  pOperator->getNextFn    = doBuildIntervalResult;
  pOperator->getStreamResFn= doStreamIntervalAgg;
  pOperator->closeFn      = destroyIntervalOperatorInfo;
  pOperator->encodeResultRow = aggEncodeResultRow;
  pOperator->decodeResultRow = aggDecodeResultRow;

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

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SExecTaskInfo* pTaskInfo) {
  STimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  pOperator->name         = "TimeSliceOperator";
  //  pOperator->operatorType = OP_AllTimeWindow;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->getNextFn    = doAllIntervalAgg;
  pOperator->closeFn      = destroyBasicOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo) {
  SStateWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStateWindowOperatorInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->colIndex = -1;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExpr, numOfCols, 4096, pResBlock, keyBufSize, pTaskInfo->id.str);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  pOperator->name         = "StateWindowOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STATE_WINDOW;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfCols;

  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->info         = pInfo;
  pOperator->getNextFn    = doStateWindowAgg;
  pOperator->closeFn      = destroyStateWindowOperatorInfo;
  pOperator->encodeResultRow = aggEncodeResultRow;
  pOperator->decodeResultRow = aggDecodeResultRow;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_SUCCESS;
  return NULL;
}

SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                            SSDataBlock* pResBlock, int64_t gap, SExecTaskInfo* pTaskInfo) {
  SSessionAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSessionAggOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t numOfRows = 4096;
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t code = initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResBlock, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);
  initExecTimeWindowInfo(&pInfo->timeWindowData, &pTaskInfo->window);

  pInfo->gap              = gap;
  pInfo->binfo.pRes       = pResBlock;
  pInfo->winSup.prevTs    = INT64_MIN;
  pInfo->reptScan         = false;
  pOperator->name         = "SessionWindowAggOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->getNextFn    = doSessionWindowAgg;
  pOperator->closeFn      = destroySWindowOperatorInfo;
  pOperator->encodeResultRow = aggEncodeResultRow;
  pOperator->decodeResultRow = aggDecodeResultRow;
  pOperator->pTaskInfo    = pTaskInfo;

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

SOperatorInfo* createMultiTableTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream,
                                                        SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STableIntervalOperatorInfo));

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->name = "MultiTableTimeIntervalOperator";
  //  pOperator->operatorType = OP_MultiTableTimeInterval;
  pOperator->blockingOptr = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExpr;
  pOperator->numOfOutput = numOfOutput;
  pOperator->info = pInfo;

  pOperator->getNextFn = doSTableIntervalAgg;
  pOperator->closeFn = destroyBasicOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

SOperatorInfo* createAllMultiTableTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream,
                                                           SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STableIntervalOperatorInfo));

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->name = "AllMultiTableTimeIntervalOperator";
  //  pOperator->operatorType = OP_AllMultiTableTimeInterval;
  pOperator->blockingOptr = true;

  pOperator->status = OP_NOT_OPENED;
  pOperator->pExpr = pExpr;
  pOperator->numOfOutput = numOfOutput;
  pOperator->info = pInfo;

  pOperator->getNextFn = doAllSTableIntervalAgg;
  pOperator->closeFn = destroyBasicOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);

  return pOperator;
}

static int32_t initFillInfo(SFillOperatorInfo* pInfo, SExprInfo* pExpr, int32_t numOfCols, int64_t* fillVal,
                            STimeWindow win, int32_t capacity, const char* id, SInterval* pInterval, int32_t fillType) {
  struct SFillColInfo* pColInfo = createFillColInfo(pExpr, numOfCols, (int64_t*)fillVal);

  TSKEY sk = TMIN(win.skey, win.ekey);
  TSKEY ek = TMAX(win.skey, win.ekey);

  // TODO set correct time precision
  STimeWindow w = TSWINDOW_INITIALIZER;
  getAlignQueryTimeWindow(pInterval, TSDB_TIME_PRECISION_MILLI, win.skey, sk, ek, &w);

  int32_t order = TSDB_ORDER_ASC;
  pInfo->pFillInfo = taosCreateFillInfo(order, w.skey, 0, capacity, numOfCols, pInterval->sliding,
                                        pInterval->slidingUnit, (int8_t)pInterval->precision, fillType, pColInfo, id);

  pInfo->p = taosMemoryCalloc(numOfCols, POINTER_BYTES);

  if (pInfo->pFillInfo == NULL || pInfo->p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                      SInterval* pInterval, SSDataBlock* pResBlock, int32_t fillType, char* fillVal,
                                      bool multigroupResult, SExecTaskInfo* pTaskInfo) {
  SFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SFillOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  pInfo->pRes = pResBlock;
  pInfo->multigroupResult = multigroupResult;
  pInfo->intervalInfo = *pInterval;

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  int32_t      code = initFillInfo(pInfo, pExpr, numOfCols, (int64_t*)fillVal, pTaskInfo->window, pResultInfo->capacity,
                                   pTaskInfo->id.str, pInterval, fillType);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name = "FillOperator";
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  //  pOperator->operatorType = OP_Fill;
  pOperator->pExpr = pExpr;
  pOperator->numOfOutput = numOfCols;
  pOperator->info = pInfo;
  pOperator->_openFn = operatorDummyOpenFn;
  pOperator->getNextFn = doFill;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->closeFn = destroySFillOperatorInfo;

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  taosMemoryFreeClear(pOperator);
  taosMemoryFreeClear(pInfo);
  return NULL;
}

static SSDataBlock* doTagScan(SOperatorInfo* pOperator, bool* newgroup) {
#if 0
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  int32_t maxNumOfTables = (int32_t)pResultInfo->capacity;

  STagScanInfo *pInfo = pOperator->info;
  SSDataBlock  *pRes = pInfo->pRes;
  *newgroup = false;

  int32_t count = 0;
  SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);

  int32_t functionId = getExprFunctionId(&pOperator->pExpr[0]);
  if (functionId == FUNCTION_TID_TAG) { // return the tags & table Id
    assert(pQueryAttr->numOfOutput == 1);

    SExprInfo* pExprInfo = &pOperator->pExpr[0];
    int32_t rsize = pExprInfo->base.resSchema.bytes;

    count = 0;

    int16_t bytes = pExprInfo->base.resSchema.bytes;
    int16_t type  = pExprInfo->base.resSchema.type;

    for(int32_t i = 0; i < pQueryAttr->numOfTags; ++i) {
      if (pQueryAttr->tagColList[i].colId == pExprInfo->base.pColumns->info.colId) {
        bytes = pQueryAttr->tagColList[i].bytes;
        type = pQueryAttr->tagColList[i].type;
        break;
      }
    }

    SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, 0);

    while(pInfo->curPos < pInfo->totalTables && count < maxNumOfTables) {
      int32_t i = pInfo->curPos++;
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

      *(int32_t *)output = pQueryAttr->vgId;
      output += sizeof(pQueryAttr->vgId);

      char* data = NULL;
      if (pExprInfo->base.pColumns->info.colId == TSDB_TBNAME_COLUMN_INDEX) {
        data = tsdbGetTableName(item->pTable);
      } else {
        data = tsdbGetTableTagVal(item->pTable, pExprInfo->base.pColumns->info.colId, type, bytes);
      }

      doSetTagValueToResultBuf(output, data, type, bytes);
      count += 1;
    }

    //qDebug("QInfo:0x%"PRIx64" create (tableId, tag) info completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  } else if (functionId == FUNCTION_COUNT) {// handle the "count(tbname)" query
    SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, 0);
    *(int64_t*)pColInfo->pData = pInfo->totalTables;
    count = 1;

    pOperator->status = OP_EXEC_DONE;
    //qDebug("QInfo:0x%"PRIx64" create count(tbname) query, res:%d rows:1", GET_TASKID(pRuntimeEnv), count);
  } else {  // return only the tags|table name etc.
    SExprInfo* pExprInfo = &pOperator->pExpr[0];  // todo use the column list instead of exprinfo

    count = 0;
    while(pInfo->curPos < pInfo->totalTables && count < maxNumOfTables) {
      int32_t i = pInfo->curPos++;

      STableQueryInfo* item = taosArrayGetP(pa, i);

      char *data = NULL, *dst = NULL;
      int16_t type = 0, bytes = 0;
      for(int32_t j = 0; j < pOperator->numOfOutput; ++j) {
        // not assign value in case of user defined constant output column
        if (TSDB_COL_IS_UD_COL(pExprInfo[j].base.pColumns->flag)) {
          continue;
        }

        SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, j);
        type  = pExprInfo[j].base.resSchema.type;
        bytes = pExprInfo[j].base.resSchema.bytes;

        if (pExprInfo[j].base.pColumns->info.colId == TSDB_TBNAME_COLUMN_INDEX) {
          data = tsdbGetTableName(item->pTable);
        } else {
          data = tsdbGetTableTagVal(item->pTable, pExprInfo[j].base.pColumns->info.colId, type, bytes);
        }

        dst  = pColInfo->pData + count * pExprInfo[j].base.resSchema.bytes;
        doSetTagValueToResultBuf(dst, data, type, bytes);
      }

      count += 1;
    }

    if (pInfo->curPos >= pInfo->totalTables) {
      pOperator->status = OP_EXEC_DONE;
    }

    //qDebug("QInfo:0x%"PRIx64" create tag values results completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  }

  if (pOperator->status == OP_EXEC_DONE) {
    setTaskStatus(pOperator->pRuntimeEnv, TASK_COMPLETED);
  }

  pRes->info.rows = count;
  return (pRes->info.rows == 0)? NULL:pInfo->pRes;

#endif
  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createTagScanOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput) {
  STagScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STagScanInfo));
  //  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pResultInfo->capacity);

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  assert(numOfGroup == 0 || numOfGroup == 1);

  pInfo->curPos = 0;

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->name = "SeqTableTagScan";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->getNextFn = doTagScan;
  pOperator->pExpr = pExpr;
  pOperator->numOfOutput = numOfOutput;
  pOperator->closeFn = destroyTagScanOperatorInfo;

  return pOperator;
}


static int32_t getColumnIndexInSource(SQueriedTableInfo* pTableInfo, SExprBasicInfo* pExpr, SColumnInfo* pTagCols) {
  int32_t j = 0;

  if (TSDB_COL_IS_TAG(pExpr->pParam[0].pCol->type)) {
    if (pExpr->pParam[0].pCol->colId == TSDB_TBNAME_COLUMN_INDEX) {
      return TSDB_TBNAME_COLUMN_INDEX;
    }

    while (j < pTableInfo->numOfTags) {
      if (pExpr->pParam[0].pCol->colId == pTagCols[j].colId) {
        return j;
      }

      j += 1;
    }

  } /*else if (TSDB_COL_IS_UD_COL(pExpr->colInfo.flag)) {  // user specified column data
    return TSDB_UD_COLUMN_INDEX;
  } else {
    while (j < pTableInfo->numOfCols) {
      if (pExpr->colInfo.colId == pTableInfo->colList[j].colId) {
        return j;
      }

      j += 1;
    }
  }*/

  return INT32_MIN;  // return a less than TSDB_TBNAME_COLUMN_INDEX value
}

bool validateExprColumnInfo(SQueriedTableInfo* pTableInfo, SExprBasicInfo* pExpr, SColumnInfo* pTagCols) {
  int32_t j = getColumnIndexInSource(pTableInfo, pExpr, pTagCols);
  return j != INT32_MIN;
}

static SResSchema createResSchema(int32_t type, int32_t bytes, int32_t slotId, int32_t scale, int32_t precision,
                                  const char* name) {
  SResSchema s = {0};
  s.scale  = scale;
  s.type   = type;
  s.bytes  = bytes;
  s.slotId = slotId;
  s.precision = precision;
  strncpy(s.name, name, tListLen(s.name));

  return s;
}

static SColumn* createColumn(int32_t blockId, int32_t slotId, SDataType* pType) {
  SColumn* pCol = taosMemoryCalloc(1, sizeof(SColumn));
  if (pCol == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCol->slotId      = slotId;
  pCol->bytes       = pType->bytes;
  pCol->type        = pType->type;
  pCol->scale       = pType->scale;
  pCol->precision   = pType->precision;
  pCol->dataBlockId = blockId;

  return pCol;
}

SExprInfo* createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, int32_t* numOfExprs) {
  int32_t numOfFuncs = LIST_LENGTH(pNodeList);
  int32_t numOfGroupKeys = 0;
  if (pGroupKeys != NULL) {
    numOfGroupKeys = LIST_LENGTH(pGroupKeys);
  }

  *numOfExprs = numOfFuncs + numOfGroupKeys;
  SExprInfo* pExprs = taosMemoryCalloc(*numOfExprs, sizeof(SExprInfo));

  for (int32_t i = 0; i < (*numOfExprs); ++i) {
    STargetNode* pTargetNode = NULL;
    if (i < numOfFuncs) {
      pTargetNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    } else {
      pTargetNode = (STargetNode*)nodesListGetNode(pGroupKeys, i - numOfFuncs);
    }

    SExprInfo* pExp = &pExprs[i];

    pExp->pExpr = taosMemoryCalloc(1, sizeof(tExprNode));
    pExp->pExpr->_function.num = 1;
    pExp->pExpr->_function.functionId = -1;

    // it is a project query, or group by column
    if (nodeType(pTargetNode->pExpr) == QUERY_NODE_COLUMN) {
      pExp->pExpr->nodeType = QUERY_NODE_COLUMN;
      SColumnNode* pColNode = (SColumnNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pColNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale, pType->precision, pColNode->colName);
      pExp->base.pParam[0].pCol = createColumn(pColNode->dataBlockId, pColNode->slotId, pType);
      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_COLUMN;
    } else if (nodeType(pTargetNode->pExpr) == QUERY_NODE_VALUE) {
      pExp->pExpr->nodeType = QUERY_NODE_VALUE;
      SValueNode* pValNode = (SValueNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pValNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale, pType->precision, pValNode->node.aliasName);
      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_VALUE;
      valueNodeToVariant(pValNode, &pExp->base.pParam[0].param);
    } else if (nodeType(pTargetNode->pExpr) == QUERY_NODE_FUNCTION) {
      pExp->pExpr->nodeType = QUERY_NODE_FUNCTION;
      SFunctionNode* pFuncNode = (SFunctionNode*)pTargetNode->pExpr;

      SDataType* pType = &pFuncNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pFuncNode->node.aliasName);

      pExp->pExpr->_function.functionId = pFuncNode->funcId;
      pExp->pExpr->_function.pFunctNode = pFuncNode;
      strncpy(pExp->pExpr->_function.functionName, pFuncNode->functionName, tListLen(pExp->pExpr->_function.functionName));

      // TODO: value parameter needs to be handled
      int32_t numOfParam = LIST_LENGTH(pFuncNode->pParameterList);

      pExp->base.pParam = taosMemoryCalloc(numOfParam, sizeof(SFunctParam));
      pExp->base.numOfParams = numOfParam;

      for (int32_t j = 0; j < numOfParam; ++j) {
        SNode*       p1 = nodesListGetNode(pFuncNode->pParameterList, j);
        if (p1->type == QUERY_NODE_COLUMN) {
          SColumnNode* pcn = (SColumnNode*) p1;

          pExp->base.pParam[j].type = FUNC_PARAM_TYPE_COLUMN;
          pExp->base.pParam[j].pCol = createColumn(pcn->dataBlockId, pcn->slotId, &pcn->node.resType);
        } else if (p1->type == QUERY_NODE_VALUE) {
          SValueNode* pvn = (SValueNode*)p1;
          pExp->base.pParam[j].type = FUNC_PARAM_TYPE_VALUE;
        }
      }
    } else if (nodeType(pTargetNode->pExpr) == QUERY_NODE_OPERATOR) {
      pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
      SOperatorNode* pNode = (SOperatorNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale, pType->precision, pNode->node.aliasName);

      pExp->pExpr->_optrRoot.pRootNode = pTargetNode->pExpr;

//      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_COLUMN;
//      pExp->base.pParam[0].pCol = createColumn(pTargetNode->dataBlockId, pTargetNode->slotId, pType);
    } else {
      ASSERT(0);
    }
  }

  return pExprs;
}

static SExecTaskInfo* createExecTaskInfo(uint64_t queryId, uint64_t taskId, EOPTR_EXEC_MODEL model) {
  SExecTaskInfo* pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);

  pTaskInfo->cost.created = taosGetTimestampMs();
  pTaskInfo->id.queryId = queryId;
  pTaskInfo->execModel  = model;

  char* p = taosMemoryCalloc(1, 128);
  snprintf(p, 128, "TID:0x%" PRIx64 " QID:0x%" PRIx64, taskId, queryId);
  pTaskInfo->id.str = strdup(p);

  return pTaskInfo;
}

static tsdbReaderT doCreateDataReader(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle,
                                      STableGroupInfo* pTableGroupInfo, uint64_t queryId, uint64_t taskId);

static int32_t doCreateTableGroup(void* metaHandle, int32_t tableType, uint64_t tableUid, STableGroupInfo* pGroupInfo,
                                  uint64_t queryId, uint64_t taskId);
static SArray* extractTableIdList(const STableGroupInfo* pTableGroupInfo);
static SArray* extractScanColumnId(SNodeList* pNodeList);
static SArray* extractColumnInfo(SNodeList* pNodeList);
static SArray* extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols);

static SArray* createSortInfo(SNodeList* pNodeList, SNodeList* pNodeListTarget);
static SArray* createIndexMap(SNodeList* pNodeList);
static SArray* extractPartitionColInfo(SNodeList* pNodeList);

SOperatorInfo* createOperatorTree(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle,
                                        uint64_t queryId, uint64_t taskId, STableGroupInfo* pTableGroupInfo) {
  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    int32_t type = nodeType(pPhyNode);
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type) {
      SScanPhysiNode* pScanPhyNode = (SScanPhysiNode*)pPhyNode;

      int32_t     numOfCols = 0;
      tsdbReaderT pDataReader = doCreateDataReader((STableScanPhysiNode*)pPhyNode, pHandle, pTableGroupInfo, (uint64_t)queryId, taskId);
      SArray* pColList = extractColMatchInfo(pScanPhyNode->pScanCols, pScanPhyNode->node.pOutputDataBlockDesc, &numOfCols);
      SSDataBlock* pResBlock = createOutputBuf_rv1(pScanPhyNode->node.pOutputDataBlockDesc);

      return createTableScanOperatorInfo(pDataReader, pScanPhyNode->order, numOfCols, pScanPhyNode->count,
                                         pScanPhyNode->reverse, pColList, pResBlock, pScanPhyNode->node.pConditions, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == type) {
      SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pPhyNode;
      SSDataBlock*        pResBlock = createOutputBuf_rv1(pExchange->node.pOutputDataBlockDesc);
      return createExchangeOperatorInfo(pExchange->pSrcEndPoints, pResBlock, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
      SScanPhysiNode* pScanPhyNode = (SScanPhysiNode*)pPhyNode;  // simple child table.

      int32_t code = doCreateTableGroup(pHandle->meta, pScanPhyNode->tableType, pScanPhyNode->uid, pTableGroupInfo, queryId, taskId);
      SArray* tableIdList = extractTableIdList(pTableGroupInfo);

      SSDataBlock* pResBlock = createOutputBuf_rv1(pScanPhyNode->node.pOutputDataBlockDesc);

      int32_t numOfCols = 0;
      SArray* pColList = extractColMatchInfo(pScanPhyNode->pScanCols, pScanPhyNode->node.pOutputDataBlockDesc, &numOfCols);
      SOperatorInfo* pOperator = createStreamScanOperatorInfo(pHandle->reader, pResBlock, pColList, tableIdList, pTaskInfo);
      taosArrayDestroy(tableIdList);
      return pOperator;
    } else if (QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == type) {
      SSystemTableScanPhysiNode* pSysScanPhyNode = (SSystemTableScanPhysiNode*)pPhyNode;
      SSDataBlock*               pResBlock = createOutputBuf_rv1(pSysScanPhyNode->scan.node.pOutputDataBlockDesc);

      struct SScanPhysiNode* pScanNode = &pSysScanPhyNode->scan;
      SArray*                colList = extractScanColumnId(pScanNode->pScanCols);

      SOperatorInfo* pOperator = createSysTableScanOperatorInfo(
          pHandle->meta, pResBlock, &pScanNode->tableName, pScanNode->node.pConditions, pSysScanPhyNode->mgmtEpSet,
          colList, pTaskInfo, pSysScanPhyNode->showRewrite, pSysScanPhyNode->accountId);
      return pOperator;
    } else {
      ASSERT(0);
    }
  }

  int32_t type = nodeType(pPhyNode);
  size_t size = LIST_LENGTH(pPhyNode->pChildren);
  ASSERT(size == 1);

  SPhysiNode*    pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
  SOperatorInfo* op = createOperatorTree(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableGroupInfo);
  int32_t        num = 0;

  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
    SProjectPhysiNode* pProjPhyNode = (SProjectPhysiNode*) pPhyNode;
    SExprInfo*   pExprInfo = createExprInfo(pProjPhyNode->pProjections, NULL, &num);

    SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);
    SLimit limit = {.limit = pProjPhyNode->limit, .offset = pProjPhyNode->offset};
    SLimit slimit = {.limit = pProjPhyNode->slimit, .offset = pProjPhyNode->soffset};
    return createProjectOperatorInfo(op, pExprInfo, num, pResBlock, &limit, &slimit, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_AGG == type) {
    SAggPhysiNode* pAggNode = (SAggPhysiNode*)pPhyNode;
    SExprInfo*     pExprInfo = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &num);
    SSDataBlock*   pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);

    SExprInfo* pScalarExprInfo = NULL;
    int32_t numOfScalarExpr = 0;
    if (pAggNode->pGroupKeys != NULL) {
      SArray* pColList = extractColumnInfo(pAggNode->pGroupKeys);
      if (pAggNode->pExprs != NULL) {
        pScalarExprInfo = createExprInfo(pAggNode->pExprs, NULL, &numOfScalarExpr);
      }

      return createGroupOperatorInfo(op, pExprInfo, num, pResBlock, pColList, pAggNode->node.pConditions, pScalarExprInfo, numOfScalarExpr, pTaskInfo, NULL);
    } else {
      return createAggregateOperatorInfo(op, pExprInfo, num, pResBlock, pTaskInfo, pTableGroupInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_INTERVAL == type) {
    SIntervalPhysiNode* pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;

    SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);

    SInterval interval = {
        .interval     = pIntervalPhyNode->interval,
        .sliding      = pIntervalPhyNode->sliding,
        .intervalUnit = pIntervalPhyNode->intervalUnit,
        .slidingUnit  = pIntervalPhyNode->slidingUnit,
        .offset       = pIntervalPhyNode->offset,
        .precision    = pIntervalPhyNode->precision
    };

    int32_t primaryTsSlotId = ((SColumnNode*) pIntervalPhyNode->window.pTspk)->slotId;
    return createIntervalOperatorInfo(op, pExprInfo, num, pResBlock, &interval, primaryTsSlotId, pTableGroupInfo, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_SORT == type) {
    SSortPhysiNode* pSortPhyNode = (SSortPhysiNode*)pPhyNode;

    SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);
    SArray*      info = createSortInfo(pSortPhyNode->pSortKeys, pSortPhyNode->pTargets);
    SArray*      slotMap = createIndexMap(pSortPhyNode->pTargets);
    return createSortOperatorInfo(op, pResBlock, info, slotMap, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW == type) {
    SSessionWinodwPhysiNode* pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;

    SExprInfo*   pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);
    return createSessionAggOperatorInfo(op, pExprInfo, num, pResBlock, pSessionNode->gap, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_PARTITION == type) {
    SPartitionPhysiNode* pPartNode = (SPartitionPhysiNode*) pPhyNode;
    SArray* pColList = extractPartitionColInfo(pPartNode->pPartitionKeys);
    SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);

    SExprInfo* pExprInfo = createExprInfo(pPartNode->pTargets, NULL, &num);
    return createPartitionOperatorInfo(op, pExprInfo, num, pResBlock, pColList, pTaskInfo, NULL);
  } else if (QUERY_NODE_STATE_WINDOW == type) {
    SStateWinodwPhysiNode* pStateNode = (SStateWinodwPhysiNode*) pPhyNode;

    SExprInfo* pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);
    return createStatewindowOperatorInfo(op, pExprInfo, num, pResBlock, pTaskInfo);
  } else {
    ASSERT(0);
  } /*else if (pPhyNode->info.type == OP_MultiTableAggregate) {
     size_t size = taosArrayGetSize(pPhyNode->pChildren);
     assert(size == 1);

     for (int32_t i = 0; i < size; ++i) {
       SPhysiNode*      pChildNode = taosArrayGetP(pPhyNode->pChildren, i);
       SOperatorInfo* op = createOperatorTree(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableGroupInfo);
       return createMultiTableAggOperatorInfo(op, pPhyNode->pTargets, pTaskInfo, pTableGroupInfo);
     }
   }*/
  return NULL;
}

static tsdbReaderT createDataReaderImpl(STableScanPhysiNode* pTableScanNode, STableGroupInfo* pGroupInfo,
                                        void* readHandle, uint64_t queryId, uint64_t taskId) {
  STsdbQueryCond cond = {.loadExternalRows = false};

  cond.order = pTableScanNode->scan.order;
  cond.numOfCols = LIST_LENGTH(pTableScanNode->scan.pScanCols);
  cond.colList = taosMemoryCalloc(cond.numOfCols, sizeof(SColumnInfo));
  if (cond.colList == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  cond.twindow = pTableScanNode->scanRange;
  cond.type = BLOCK_LOAD_OFFSET_SEQ_ORDER;
  //  cond.type = pTableScanNode->scanFlag;

  int32_t j = 0;
  for (int32_t i = 0; i < cond.numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pTableScanNode->scan.pScanCols, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
    if (pColNode->colType == COLUMN_TYPE_TAG) {
      continue;
    }

    cond.colList[j].type  = pColNode->node.resType.type;
    cond.colList[j].bytes = pColNode->node.resType.bytes;
    cond.colList[j].colId = pColNode->colId;
    j += 1;
  }

  cond.numOfCols = j;
  return tsdbQueryTables(readHandle, &cond, pGroupInfo, queryId, taskId);
}

SArray* extractScanColumnId(SNodeList* pNodeList) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(int16_t));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    for (int32_t j = 0; j < numOfCols; ++j) {
      STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, j);
      if (pNode->slotId == i) {
        SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
        taosArrayPush(pList, &pColNode->colId);
        break;
      }
    }
  }

  return pList;
}

SArray* extractColumnInfo(SNodeList* pNodeList) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

    // todo extract method
    SColumn c = {0};
    c.slotId = pColNode->slotId;
    c.colId  = pColNode->colId;
    c.type   = pColNode->node.resType.type;
    c.bytes  = pColNode->node.resType.bytes;
    c.precision = pColNode->node.resType.precision;
    c.scale = pColNode->node.resType.scale;

    taosArrayPush(pList, &c);
  }

  return pList;
}

SArray* extractPartitionColInfo(SNodeList* pNodeList) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pNodeList, i);

    // todo extract method
    SColumn c = {0};
    c.slotId = pColNode->slotId;
    c.colId  = pColNode->colId;
    c.type   = pColNode->node.resType.type;
    c.bytes  = pColNode->node.resType.bytes;
    c.precision = pColNode->node.resType.precision;
    c.scale = pColNode->node.resType.scale;

    taosArrayPush(pList, &c);
  }

  return pList;
}

SArray* createSortInfo(SNodeList* pNodeList, SNodeList* pNodeListTarget) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SBlockOrderInfo));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return pList;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    
    SOrderByExprNode* pSortKey = (SOrderByExprNode*)nodesListGetNode(pNodeList, i);
    SBlockOrderInfo   bi = {0};
    bi.order = (pSortKey->order == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    bi.nullFirst = (pSortKey->nullOrder == NULL_ORDER_FIRST);

    SColumnNode* pColNode = (SColumnNode*)pSortKey->pExpr;

    bool found = false;
    for (int32_t j = 0; j < LIST_LENGTH(pNodeListTarget); ++j) {
      STargetNode* pTarget = (STargetNode*)nodesListGetNode(pNodeListTarget, j);

      SColumnNode* pColNodeT = (SColumnNode*)pTarget->pExpr;
      if(pColNode->slotId == pColNodeT->slotId){    // to find slotId in PhysiSort OutputDataBlockDesc
        bi.slotId = pTarget->slotId;
        found = true;
        break;
      }
    }

    if(!found){
      qError("sort slot id does not found");
    }
    taosArrayPush(pList, &bi);
  }

  return pList;
}

SArray* createIndexMap(SNodeList* pNodeList) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(int32_t));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return pList;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pTarget = (STargetNode*)nodesListGetNode(pNodeList, i);

    SColumnNode* pColNode = (SColumnNode*)pTarget->pExpr;
    taosArrayPush(pList, &pColNode->slotId);
  }

  return pList;
}

SArray* extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColMatchInfo));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

    SColMatchInfo c = {0};
    c.colId = pColNode->colId;
    c.targetSlotId = pNode->slotId;
    c.output = true;
    taosArrayPush(pList, &c);
  }

  *numOfOutputCols = 0;
  int32_t num = LIST_LENGTH(pOutputNodeList->pSlots);
  for (int32_t i = 0; i < num; ++i) {
    SSlotDescNode* pNode = (SSlotDescNode*)nodesListGetNode(pOutputNodeList->pSlots, i);
    // todo: add reserve flag check
    if (pNode->slotId >= numOfCols) { // it is a column reserved for the arithmetic expression calculation
      (*numOfOutputCols) += 1;
      continue;
    }

    SColMatchInfo* info = taosArrayGet(pList, pNode->slotId);
    if (pNode->output) {
      (*numOfOutputCols) += 1;
    } else {
      info->output = false;
    }
  }

  return pList;
}

int32_t doCreateTableGroup(void* metaHandle, int32_t tableType, uint64_t tableUid, STableGroupInfo* pGroupInfo, uint64_t queryId, uint64_t taskId) {
  int32_t code = 0;
  if (tableType == TSDB_SUPER_TABLE) {
    code = tsdbQuerySTableByTagCond(metaHandle, tableUid, 0, NULL, 0, 0, NULL, pGroupInfo, NULL, 0, queryId, taskId);
  } else {  // Create one table group.
    code = tsdbGetOneTableGroup(metaHandle, tableUid, 0, pGroupInfo);
  }

  return code;
}

SArray* extractTableIdList(const STableGroupInfo* pTableGroupInfo) {
  SArray* tableIdList = taosArrayInit(4, sizeof(uint64_t));

  if (pTableGroupInfo->numOfTables > 0) {
    SArray* pa = taosArrayGetP(pTableGroupInfo->pGroupList, 0);
    ASSERT(taosArrayGetSize(pTableGroupInfo->pGroupList) == 1);

    // Transfer the Array of STableKeyInfo into uid list.
    size_t numOfTables = taosArrayGetSize(pa);
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pkeyInfo = taosArrayGet(pa, i);
      taosArrayPush(tableIdList, &pkeyInfo->uid);
    }
  }

  return tableIdList;
}

tsdbReaderT doCreateDataReader(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle,
                               STableGroupInfo* pTableGroupInfo, uint64_t queryId, uint64_t taskId) {
  uint64_t uid = pTableScanNode->scan.uid;
  int32_t  code =
      doCreateTableGroup(pHandle->meta, pTableScanNode->scan.tableType, uid, pTableGroupInfo, queryId, taskId);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pTableGroupInfo->numOfTables == 0) {
    code = 0;
    qDebug("no table qualified for query, TID:0x%" PRIx64 ", QID:0x%" PRIx64, taskId, queryId);
    goto _error;
  }

  return createDataReaderImpl(pTableScanNode, pTableGroupInfo, pHandle->reader, queryId, taskId);

_error:
  terrno = code;
  return NULL;
}

int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId, EOPTR_EXEC_MODEL model) {
  uint64_t queryId = pPlan->id.queryId;

  int32_t code = TSDB_CODE_SUCCESS;
  *pTaskInfo = createExecTaskInfo(queryId, taskId, model);
  if (*pTaskInfo == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _complete;
  }

  STableGroupInfo group = {0};
  (*pTaskInfo)->pRoot = createOperatorTree(pPlan->pNode, *pTaskInfo, pHandle, queryId, taskId, &group);
  if (NULL == (*pTaskInfo)->pRoot) {
    code = terrno;
    goto _complete;
  }

  if ((*pTaskInfo)->pRoot == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _complete;
  }

  return code;

_complete:
  taosMemoryFreeClear(*pTaskInfo);

  terrno = code;
  return code;
}

static int32_t updateOutputBufForTopBotQuery(SQueriedTableInfo* pTableInfo, SColumnInfo* pTagCols, SExprInfo* pExprs,
                                             int32_t numOfOutput, int32_t tagLen, bool superTable) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    int16_t functId = getExprFunctionId(&pExprs[i]);

    if (functId == FUNCTION_TOP || functId == FUNCTION_BOTTOM) {
      int32_t j = getColumnIndexInSource(pTableInfo, &pExprs[i].base, pTagCols);
      if (j < 0 || j >= pTableInfo->numOfCols) {
        return TSDB_CODE_QRY_INVALID_MSG;
      } else {
        SColumnInfo* pCol = &pTableInfo->colList[j];
        //        int32_t ret = getResultDataInfo(pCol->type, pCol->bytes, functId, (int32_t)pExprs[i].base.param[0].i,
        //                                        &pExprs[i].base.resSchema.type, &pExprs[i].base.resSchema.bytes,
        //                                        &pExprs[i].base.interBytes, tagLen, superTable, NULL);
        //        assert(ret == TSDB_CODE_SUCCESS);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

void setResultBufSize(STaskAttr* pQueryAttr, SResultInfo* pResultInfo) {
  const int32_t DEFAULT_RESULT_MSG_SIZE = 1024 * (1024 + 512);

  // the minimum number of rows for projection query
  const int32_t MIN_ROWS_FOR_PRJ_QUERY = 8192;
  const int32_t DEFAULT_MIN_ROWS = 4096;

  const float THRESHOLD_RATIO = 0.85f;

//  if (isProjQuery(pQueryAttr)) {
//    int32_t numOfRes = DEFAULT_RESULT_MSG_SIZE / pQueryAttr->resultRowSize;
//    if (numOfRes < MIN_ROWS_FOR_PRJ_QUERY) {
//      numOfRes = MIN_ROWS_FOR_PRJ_QUERY;
//    }
//
//    pResultInfo->capacity = numOfRes;
//  } else {  // in case of non-prj query, a smaller output buffer will be used.
//    pResultInfo->capacity = DEFAULT_MIN_ROWS;
//  }

  pResultInfo->threshold = (int32_t)(pResultInfo->capacity * THRESHOLD_RATIO);
  pResultInfo->totalRows = 0;
}

// TODO refactor
void freeColumnFilterInfo(SColumnFilterInfo* pFilter, int32_t numOfFilters) {
  if (pFilter == NULL || numOfFilters == 0) {
    return;
  }

  for (int32_t i = 0; i < numOfFilters; i++) {
    if (pFilter[i].filterstr && pFilter[i].pz) {
      taosMemoryFree((void*)(pFilter[i].pz));
    }
  }

  taosMemoryFree(pFilter);
}

static void doDestroyTableQueryInfo(STableGroupInfo* pTableqinfoGroupInfo) {
  if (pTableqinfoGroupInfo->pGroupList != NULL) {
    int32_t numOfGroups = (int32_t)taosArrayGetSize(pTableqinfoGroupInfo->pGroupList);
    for (int32_t i = 0; i < numOfGroups; ++i) {
      SArray* p = taosArrayGetP(pTableqinfoGroupInfo->pGroupList, i);

      size_t num = taosArrayGetSize(p);
      for (int32_t j = 0; j < num; ++j) {
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

void doDestroyTask(SExecTaskInfo* pTaskInfo) {
  qDebug("%s execTask is freed", GET_TASKID(pTaskInfo));

  doDestroyTableQueryInfo(&pTaskInfo->tableqinfoGroupInfo);
  //  taosArrayDestroy(pTaskInfo->summary.queryProfEvents);
  //  taosHashCleanup(pTaskInfo->summary.operatorProfResults);

  taosMemoryFreeClear(pTaskInfo->sql);
  taosMemoryFreeClear(pTaskInfo->id.str);
  taosMemoryFreeClear(pTaskInfo);
}

static void doSetTagValueToResultBuf(char* output, const char* val, int16_t type, int16_t bytes) {
  if (val == NULL) {
    setNull(output, type, bytes);
    return;
  }

  if (IS_VAR_DATA_TYPE(type)) {
    // Binary data overflows for sort of unknown reasons. Let trim the overflow data
    if (varDataTLen(val) > bytes) {
      int32_t maxLen = bytes - VARSTR_HEADER_SIZE;
      int32_t len = (varDataLen(val) > maxLen) ? maxLen : varDataLen(val);
      memcpy(varDataVal(output), varDataVal(val), len);
      varDataSetLen(output, len);
    } else {
      varDataCopy(output, val);
    }
  } else {
    memcpy(output, val, bytes);
  }
}

static int64_t getQuerySupportBufSize(size_t numOfTables) {
  size_t s1 = sizeof(STableQueryInfo);
  //  size_t s3 = sizeof(STableCheckInfo);  buffer consumption in tsdb
  return (int64_t)(s1 * 1.5 * numOfTables);
}

int32_t checkForQueryBuf(size_t numOfTables) {
  int64_t t = getQuerySupportBufSize(numOfTables);
  if (tsQueryBufferSizeBytes < 0) {
    return TSDB_CODE_SUCCESS;
  } else if (tsQueryBufferSizeBytes > 0) {
    while (1) {
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

int32_t getOperatorExplainExecInfo(SOperatorInfo *operatorInfo, SExplainExecInfo **pRes, int32_t *capacity, int32_t *resNum) {
  if (*resNum >= *capacity) {
    *capacity += 10;
    
    *pRes = taosMemoryRealloc(*pRes, (*capacity) * sizeof(SExplainExecInfo));
    if (NULL == *pRes) {
      qError("malloc %d failed", (*capacity) * (int32_t)sizeof(SExplainExecInfo));
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
  }

  (*pRes)[*resNum].numOfRows = operatorInfo->resultInfo.totalRows;
  (*pRes)[*resNum].startupCost = operatorInfo->cost.openCost;
  (*pRes)[*resNum].totalCost = operatorInfo->cost.totalCost;

  if (operatorInfo->getExplainFn) {
    int32_t code = (*operatorInfo->getExplainFn)(operatorInfo, &(*pRes)->verboseInfo);
    if (code) {
      qError("operator getExplainFn failed, error:%s", tstrerror(code));
      return code;
    }
  }
  
  ++(*resNum);
  
  int32_t code = 0;
  for (int32_t i = 0; i < operatorInfo->numOfDownstream; ++i) {
    code = getOperatorExplainExecInfo(operatorInfo->pDownstream[i], pRes, capacity, resNum);
    if (code) {
      taosMemoryFreeClear(*pRes);
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}


