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

#include <function.h>
#include <functionMgt.h>
#include <tname.h>
#include "os.h"

#include "parser.h"
#include "tdatablock.h"
#include "texception.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tsort.h"
#include "ttime.h"

#include "../../function/inc/taggfunction.h"
#include "executorimpl.h"
#include "function.h"
#include "query.h"
#include "tcompare.h"
#include "tcompression.h"
#include "thash.h"
#include "tsdb.h"
#include "ttypes.h"
#include "vnode.h"

#define IS_MAIN_SCAN(runtime)          ((runtime)->scanFlag == MAIN_SCAN)
#define IS_REVERSE_SCAN(runtime)       ((runtime)->scanFlag == REVERSE_SCAN)
#define IS_REPEAT_SCAN(runtime)        ((runtime)->scanFlag == REPEAT_SCAN)
#define SET_MAIN_SCAN_FLAG(runtime)  ((runtime)->scanFlag = MAIN_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define TSWINDOW_IS_EQUAL(t1, t2) (((t1).skey == (t2).skey) && ((t1).ekey == (t2).ekey))
#define SWITCH_ORDER(n) (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

#define SDATA_BLOCK_INITIALIZER (SDataBlockInfo) {{0}, 0}

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#define MULTI_KEY_DELIM  "-"

enum {
  TS_JOIN_TS_EQUAL       = 0,
  TS_JOIN_TS_NOT_EQUALS  = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

typedef enum SResultTsInterpType {
  RESULT_ROW_START_INTERP = 1,
  RESULT_ROW_END_INTERP   = 2,
} SResultTsInterpType;

#if 0
static UNUSED_FUNC void *u_malloc (size_t __size) {
  uint32_t v = taosRand();

  if (v % 1000 <= 0) {
    return NULL;
  } else {
    return malloc(__size);
  }
}

static UNUSED_FUNC void* u_calloc(size_t num, size_t __size) {
  uint32_t v = taosRand();
  if (v % 1000 <= 0) {
    return NULL;
  } else {
    return calloc(num, __size);
  }
}

static UNUSED_FUNC void* u_realloc(void* p, size_t __size) {
  uint32_t v = taosRand();
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

#define TSKEY_MAX_ADD(a,b)                 \
do {                                       \
  if (a < 0) { a = a + b; break;}          \
  if (sizeof(a) == sizeof(int32_t)) {      \
   if((b) > 0 && ((b) >= INT32_MAX - (a))){\
     a = INT32_MAX;                        \
   } else {                                \
     a = a + b;                            \
   }                                       \
  } else {                                 \
   if((b) > 0 && ((b) >= INT64_MAX - (a))){\
     a = INT64_MAX;                        \
   } else {                                \
     a = a + b;                            \
   }                                       \
  }                                        \
} while(0)                                 

#define TSKEY_MIN_SUB(a,b)                 \
do {                                       \
  if (a >= 0) { a = a + b; break;}         \
  if (sizeof(a) == sizeof(int32_t)){       \
   if((b) < 0 && ((b) <= INT32_MIN - (a))){\
     a = INT32_MIN;                        \
   } else {                                \
     a = a + b;                            \
   }                                       \
  } else {                                 \
    if((b) < 0 && ((b) <= INT64_MIN-(a))) {\
     a = INT64_MIN;                        \
    } else {                               \
     a = a + b;                            \
    }                                      \
  }                                        \
} while (0)

int32_t getMaximumIdleDurationSec() {
  return tsShellActivityTimer * 2;
}

static int32_t getExprFunctionId(SExprInfo *pExprInfo) {
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
  //convert key to second
  key = convertTimePrecision(key, precision, TSDB_TIME_PRECISION_MILLI) / 1000;

  if (pInterval->intervalUnit == 'y') {
    interval *= 12;
  }

  struct tm tm;
  time_t t = (time_t)key;
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
static void setResultOutputBuf(STaskRuntimeEnv* pRuntimeEnv, SResultRow* pResult, SqlFunctionCtx* pCtx,
                               int32_t numOfCols, int32_t* rowCellInfoOffset);

void setResultRowOutputBufInitCtx(STaskRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset);
static bool functionNeedToExecute(SqlFunctionCtx *pCtx);

static void setBlockStatisInfo(SqlFunctionCtx *pCtx, SSDataBlock* pSDataBlock, SColumn* pColumn);

static void destroyTableQueryInfoImpl(STableQueryInfo *pTableQueryInfo);
static bool hasMainOutput(STaskAttr *pQueryAttr);

static SColumnInfo* extractColumnFilterInfo(SExprInfo* pExpr, int32_t numOfOutput, int32_t* numOfFilterCols);

static int32_t setTimestampListJoinInfo(STaskRuntimeEnv* pRuntimeEnv, SVariant* pTag, STableQueryInfo *pTableQueryInfo);
static void releaseQueryBuf(size_t numOfTables);
static int32_t binarySearchForKey(char *pValue, int num, TSKEY key, int order);
//static STsdbQueryCond createTsdbQueryCond(STaskAttr* pQueryAttr, STimeWindow* win);
static STableIdInfo createTableIdInfo(STableQueryInfo* pTableQueryInfo);

static void setTableScanFilterOperatorInfo(STableScanInfo* pTableScanInfo, SOperatorInfo* pDownstream);

static int32_t getNumOfScanTimes(STaskAttr* pQueryAttr);

static void destroyBasicOperatorInfo(void* param, int32_t numOfOutput);
static void destroySFillOperatorInfo(void* param, int32_t numOfOutput);
static void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput);
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

static void doSetOperatorCompleted(SOperatorInfo* pOperator) {
  pOperator->status = OP_EXEC_DONE;
  if (pOperator->pTaskInfo != NULL) {
    setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  }
}

#define OPTR_IS_OPENED(_optr)  (((_optr)->status & OP_OPENED) == OP_OPENED)
#define OPTR_SET_OPENED(_optr) ((_optr)->status |= OP_OPENED)

static int32_t operatorDummyOpenFn(SOperatorInfo *pOperator) {
  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static void operatorDummyCloseFn(void* param, int32_t numOfCols) {}

static int32_t doCopyToSDataBlock(SDiskbasedBuf *pBuf, SGroupResInfo* pGroupResInfo, int32_t orderType, SSDataBlock* pBlock, int32_t rowCapacity, int32_t* rowCellOffset);

static int32_t getGroupbyColumnIndex(SGroupbyExpr *pGroupbyExpr, SSDataBlock* pDataBlock);
static int32_t setGroupResultOutputBuf(STaskRuntimeEnv *pRuntimeEnv, SOptrBasicInfo *binf, int32_t numOfCols, char *pData, int16_t type, int16_t bytes, int32_t groupIndex);

static void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, int64_t keyFirst, int64_t keyLast, STimeWindow *win);

static void setResultBufSize(STaskAttr* pQueryAttr, SRspResultInfo* pResultInfo);
static void setCtxTagForJoin(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, void* pTable);
static void setParamForStableStddev(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, int32_t numOfOutput, SExprInfo* pExpr);
static void setParamForStableStddevByColData(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, int32_t numOfOutput, SExprInfo* pExpr, char* val, int16_t bytes);
static void doSetTableGroupOutputBuf(SAggOperatorInfo* pAggInfo, int32_t numOfOutput, int32_t tableGroupId, SExecTaskInfo* pTaskInfo);

SArray* getOrderCheckColumns(STaskAttr* pQuery);

typedef struct SRowCompSupporter {
  STaskRuntimeEnv *pRuntimeEnv;
  int16_t           dataOffset;
  __compar_fn_t     comFunc;
} SRowCompSupporter;

static int compareRowData(const void *a, const void *b, const void *userData) {
  const SResultRow *pRow1 = (const SResultRow *)a;
  const SResultRow *pRow2 = (const SResultRow *)b;

  SRowCompSupporter *supporter  = (SRowCompSupporter *)userData;
  STaskRuntimeEnv* pRuntimeEnv =  supporter->pRuntimeEnv;

  SFilePage *page1 = getBufPage(pRuntimeEnv->pResultBuf, pRow1->pageId);
  SFilePage *page2 = getBufPage(pRuntimeEnv->pResultBuf, pRow2->pageId);

  int16_t offset = supporter->dataOffset;
  char *in1  = getPosInResultPage(pRuntimeEnv->pQueryAttr, page1, pRow1->offset, offset);
  char *in2  = getPosInResultPage(pRuntimeEnv->pQueryAttr, page2, pRow2->offset, offset);

  return (in1 != NULL && in2 != NULL) ? supporter->comFunc(in1, in2) : 0;
}

static void sortGroupResByOrderList(SGroupResInfo *pGroupResInfo, STaskRuntimeEnv *pRuntimeEnv, SSDataBlock* pDataBlock) {
  SArray *columnOrderList = getOrderCheckColumns(pRuntimeEnv->pQueryAttr);
  size_t size = taosArrayGetSize(columnOrderList);
  taosArrayDestroy(columnOrderList);

  if (size <= 0) {
    return;
  }

  int32_t orderId = pRuntimeEnv->pQueryAttr->order.col.colId;
  if (orderId <= 0) {
    return;
  }

  bool found = false;
  int16_t dataOffset = 0;

  for (int32_t j = 0; j < pDataBlock->info.numOfCols; ++j) {
    SColumnInfoData* pColInfoData = (SColumnInfoData *)taosArrayGet(pDataBlock->pDataBlock, j);
    if (orderId == j) {
      found = true;
      break;
    }

    dataOffset += pColInfoData->info.bytes;
  }

  if (found == false) {
    return;
  }

  int16_t type = pRuntimeEnv->pQueryAttr->pExpr1[orderId].base.resSchema.type;

  SRowCompSupporter support = {.pRuntimeEnv = pRuntimeEnv, .dataOffset = dataOffset, .comFunc = getComparFunc(type, 0)};
  taosArraySortPWithExt(pGroupResInfo->pRows, compareRowData, &support);
}

//setup the output buffer for each operator
SSDataBlock* createOutputBuf(SExprInfo* pExpr, int32_t numOfOutput, int32_t numOfRows) {
  const static int32_t minSize = 8;

  SSDataBlock *res = calloc(1, sizeof(SSDataBlock));
  res->info.numOfCols = numOfOutput;
  res->pDataBlock = taosArrayInit(numOfOutput, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData idata = {{0}};
    idata.info.type  = pExpr[i].base.resSchema.type;
    idata.info.bytes = pExpr[i].base.resSchema.bytes;
    idata.info.colId = pExpr[i].base.resSchema.colId;

    int32_t size = TMAX(idata.info.bytes * numOfRows, minSize);
    idata.pData = calloc(1, size);  // at least to hold a pointer on x64 platform
    taosArrayPush(res->pDataBlock, &idata);
  }

  return res;
}

SSDataBlock* createOutputBuf_rv(SArray* pExprInfo, int32_t numOfRows) {
  size_t numOfOutput = taosArrayGetSize(pExprInfo);

  SSDataBlock *res = calloc(1, sizeof(SSDataBlock));
  res->info.numOfCols = numOfOutput;
  res->pDataBlock = taosArrayInit(numOfOutput, sizeof(SColumnInfoData));

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData idata = {{0}};
    SExprInfo* pExpr = taosArrayGetP(pExprInfo, i);

    idata.info.type  = pExpr->base.resSchema.type;
    idata.info.bytes = pExpr->base.resSchema.bytes;
    idata.info.colId = pExpr->base.resSchema.colId;
    taosArrayPush(res->pDataBlock, &idata);
  }

  blockDataEnsureCapacity(res, numOfRows);
  return res;
}

SSDataBlock* createOutputBuf_rv1(SDataBlockDescNode* pNode) {
  int32_t numOfCols = LIST_LENGTH(pNode->pSlots);

  SSDataBlock* pBlock = calloc(1, sizeof(SSDataBlock));
  pBlock->info.numOfCols = numOfCols;
  pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  pBlock->info.blockId = pNode->dataBlockId;
  pBlock->info.rowSize = pNode->resultRowSize;

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData idata = {{0}};
    SSlotDescNode* pDescNode = nodesListGetNode(pNode->pSlots, i);
    idata.info.type   = pDescNode->dataType.type;
    idata.info.bytes  = pDescNode->dataType.bytes;
    idata.info.scale  = pDescNode->dataType.scale;
    idata.info.slotId = pDescNode->slotId;
    idata.info.precision = pDescNode->dataType.precision;

    taosArrayPush(pBlock->pDataBlock, &idata);
  }

  return pBlock;
}

static bool isSelectivityWithTagsQuery(SqlFunctionCtx *pCtx, int32_t numOfOutput) {
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

static bool isProjQuery(STaskAttr *pQueryAttr) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functId = getExprFunctionId(&pQueryAttr->pExpr1[i]);
    if (functId != FUNCTION_PRJ && functId != FUNCTION_TAGPRJ) {
      return false;
    }
  }

  return true;
}

static bool hasNull(SColumn* pColumn, SColumnDataAgg *pStatis) {
  if (TSDB_COL_IS_TAG(pColumn->flag) || TSDB_COL_IS_UD_COL(pColumn->flag) || pColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
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

  char *t = realloc(pResultRowInfo->pResult, (size_t)(newCapacity * POINTER_BYTES));
  if (t == NULL) {
    longjmp(env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pResultRowInfo->pPosition = realloc(pResultRowInfo->pPosition, newCapacity * sizeof(SResultRowPosition));
  pResultRowInfo->pResult = (SResultRow **)t;

  int32_t inc = (int32_t)newCapacity - pResultRowInfo->capacity;
  memset(&pResultRowInfo->pResult[pResultRowInfo->capacity], 0, POINTER_BYTES * inc);
  memset(&pResultRowInfo->pPosition[pResultRowInfo->capacity], 0, sizeof(SResultRowPosition));

  pResultRowInfo->capacity = (int32_t)newCapacity;
}

static bool chkResultRowFromKey(STaskRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, char *pData,
                                             int16_t bytes, bool masterscan, uint64_t uid) {
  bool existed = false;
  SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, uid);

  SResultRow **p1 =
      (SResultRow **)taosHashGet(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  // in case of repeat scan/reverse scan, no new time window added.
  if (QUERY_IS_INTERVAL_QUERY(pRuntimeEnv->pQueryAttr)) {
    if (!masterscan) {  // the *p1 may be NULL in case of sliding+offset exists.
      return p1 != NULL;
    }

    if (p1 != NULL) {
      if (pResultRowInfo->size == 0) {
        existed = false;
        assert(pResultRowInfo->curPos == -1);
      } else if (pResultRowInfo->size == 1) {
        existed = (pResultRowInfo->pResult[0] == (*p1));
      } else {  // check if current pResultRowInfo contains the existed pResultRow
        SET_RES_EXT_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, uid, pResultRowInfo);
        int64_t* index = taosHashGet(pRuntimeEnv->pResultRowListSet, pRuntimeEnv->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes));
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


static SResultRow* doSetResultOutBufByKey(STaskRuntimeEnv* pRuntimeEnv, SResultRowInfo* pResultRowInfo, int64_t tid,
                                          char* pData, int16_t bytes, bool masterscan, uint64_t tableGroupId) {
  bool existed = false;
  SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, tableGroupId);

  SResultRow **p1 =
      (SResultRow **)taosHashGet(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  // in case of repeat scan/reverse scan, no new time window added.
  if (QUERY_IS_INTERVAL_QUERY(pRuntimeEnv->pQueryAttr)) {
    if (!masterscan) {  // the *p1 may be NULL in case of sliding+offset exists.
      return (p1 != NULL)? *p1:NULL;
    }

    if (p1 != NULL) {
      if (pResultRowInfo->size == 0) {
        existed = false;
        assert(pResultRowInfo->curPos == -1);
      } else if (pResultRowInfo->size == 1) {
        existed = (pResultRowInfo->pResult[0] == (*p1));
        pResultRowInfo->curPos = 0;
      } else {  // check if current pResultRowInfo contains the existed pResultRow
        SET_RES_EXT_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, tid, pResultRowInfo);
        int64_t* index = taosHashGet(pRuntimeEnv->pResultRowListSet, pRuntimeEnv->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes));
        if (index != NULL) {
          pResultRowInfo->curPos = (int32_t) *index;
          existed = true;
        } else {
          existed = false;
        }
      }
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existed in the pResultRowInfo object.
    if (p1 != NULL) {
      return *p1;
    }
  }

  if (!existed) {
//    prepareResultListBuffer(pResultRowInfo, pRuntimeEnv);

    SResultRow *pResult = NULL;
    if (p1 == NULL) {
      pResult = getNewResultRow(pRuntimeEnv->pool);
      int32_t ret = initResultRow(pResult);
      if (ret != TSDB_CODE_SUCCESS) {
        longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      // add a new result set for a new group
      taosHashPut(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pResult, POINTER_BYTES);
      SResultRowCell cell = {.groupId = tableGroupId, .pRow = pResult};
      taosArrayPush(pRuntimeEnv->pResultRowArrayList, &cell);
    } else {
      pResult = *p1;
    }

    pResultRowInfo->curPos = pResultRowInfo->size;
    pResultRowInfo->pResult[pResultRowInfo->size++] = pResult;

    int64_t index = pResultRowInfo->curPos;
    SET_RES_EXT_WINDOW_KEY(pRuntimeEnv->keyBuf, pData, bytes, tid, pResultRowInfo);
    taosHashPut(pRuntimeEnv->pResultRowListSet, pRuntimeEnv->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes), &index, POINTER_BYTES);
  }

  // too many time window in query
  if (pResultRowInfo->size > MAX_INTERVAL_TIME_WINDOW) {
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  return pResultRowInfo->pResult[pResultRowInfo->curPos];
}

SResultRow* getNewResultRow_rv(SDiskbasedBuf* pResultBuf, int64_t tableGroupId, int32_t interBufSize) {
  SFilePage *pData = NULL;

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

    if (pData->num + interBufSize + sizeof(SResultRow) > getBufPageSize(pResultBuf)) {
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

  pData->num += interBufSize + sizeof(SResultRow);

  return pResultRow;
}

static SResultRow* doSetResultOutBufByKey_rv(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo, int64_t tid, char* pData, int16_t bytes,
    bool masterscan, uint64_t tableGroupId, SExecTaskInfo* pTaskInfo, bool isIntervalQuery, SAggSupporter* pSup) {
  bool existed = false;
  SET_RES_WINDOW_KEY(pSup->keyBuf, pData, bytes, tableGroupId);

  SResultRow **p1 =
      (SResultRow **)taosHashGet(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  // in case of repeat scan/reverse scan, no new time window added.
  if (isIntervalQuery) {
    if (!masterscan) {  // the *p1 may be NULL in case of sliding+offset exists.
      return (p1 != NULL)? *p1:NULL;
    }

    if (p1 != NULL) {
      if (pResultRowInfo->size == 0) {
        existed = false;
        assert(pResultRowInfo->curPos == -1);
      } else if (pResultRowInfo->size == 1) {
        existed = (pResultRowInfo->pResult[0] == (*p1));
        pResultRowInfo->curPos = 0;
      } else {  // check if current pResultRowInfo contains the existed pResultRow
        SET_RES_EXT_WINDOW_KEY(pSup->keyBuf, pData, bytes, tid, pResultRowInfo);
        int64_t* index = taosHashGet(pSup->pResultRowListSet, pSup->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes));
        if (index != NULL) {
          pResultRowInfo->curPos = (int32_t) *index;
          existed = true;
        } else {
          existed = false;
        }
      }
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existed in the pResultRowInfo object.
    if (p1 != NULL) {
      return *p1;
    }
  }

  if (!existed) {
    prepareResultListBuffer(pResultRowInfo, pTaskInfo->env);

    SResultRow *pResult = NULL;
    if (p1 == NULL) {
      pResult = getNewResultRow_rv(pResultBuf, tableGroupId, pSup->resultRowSize);
      int32_t ret = initResultRow(pResult);
      if (ret != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      // add a new result set for a new group
      taosHashPut(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pResult, POINTER_BYTES);
      SResultRowCell cell = {.groupId = tableGroupId, .pRow = pResult};
      taosArrayPush(pSup->pResultRowArrayList, &cell);
    } else {
      pResult = *p1;
    }

    pResultRowInfo->curPos = pResultRowInfo->size;
    pResultRowInfo->pPosition[pResultRowInfo->size] = (SResultRowPosition) {.pageId = pResult->pageId, .offset = pResult->offset};
    pResultRowInfo->pResult[pResultRowInfo->size++] = pResult;

    int64_t index = pResultRowInfo->curPos;
    SET_RES_EXT_WINDOW_KEY(pSup->keyBuf, pData, bytes, tid, pResultRowInfo);
    taosHashPut(pSup->pResultRowListSet, pSup->keyBuf, GET_RES_EXT_WINDOW_KEY_LEN(bytes), &index, POINTER_BYTES);
  }

  // too many time window in query
  if (pResultRowInfo->size > MAX_INTERVAL_TIME_WINDOW) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  return pResultRowInfo->pResult[pResultRowInfo->curPos];
}

static void getInitialStartTimeWindow(SInterval* pInterval, int32_t precision, TSKEY ts, STimeWindow* w, TSKEY ekey, bool ascQuery) {
  if (ascQuery) {
    getAlignQueryTimeWindow(pInterval, precision, ts, ts, ekey, w);
  } else {
    // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
    getAlignQueryTimeWindow(pInterval, precision, ts, ekey, ts, w);

    int64_t key = w->skey;
    while(key < ts) { // moving towards end
      if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
        key = taosTimeAdd(key, pInterval->sliding, pInterval->slidingUnit, precision);
      } else {
        key += pInterval->sliding;
      }

      if (key >= ts) {
        break;
      }

      w->skey = key;
    }
  }
}

// get the correct time window according to the handled timestamp
static STimeWindow getActiveTimeWindow(SResultRowInfo * pResultRowInfo, int64_t ts, SInterval* pInterval, int32_t precision, STimeWindow* win) {
  STimeWindow w = {0};

 if (pResultRowInfo->curPos == -1) {  // the first window, from the previous stored value
    getInitialStartTimeWindow(pInterval, precision, ts, &w, win->ekey, true);

    if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
      w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
    } else {
      w.ekey = w.skey + pInterval->interval - 1;
    }
  } else {
    w = getResultRow(pResultRowInfo, pResultRowInfo->curPos)->win;
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
      w.ekey = w.skey + pInterval->interval - 1;
    }
  }
  return w;
}

// get the correct time window according to the handled timestamp
static STimeWindow getCurrentActiveTimeWindow(SResultRowInfo * pResultRowInfo, int64_t ts, STaskAttr *pQueryAttr) {
  STimeWindow w = {0};

 if (pResultRowInfo->curPos == -1) {  // the first window, from the previous stored value
//    getInitialStartTimeWindow(pQueryAttr, ts, &w);

    if (pQueryAttr->interval.intervalUnit == 'n' || pQueryAttr->interval.intervalUnit == 'y') {
      w.ekey = taosTimeAdd(w.skey, pQueryAttr->interval.interval, pQueryAttr->interval.intervalUnit, pQueryAttr->precision) - 1;
    } else {
      w.ekey = w.skey + pQueryAttr->interval.interval - 1;
    }
  } else {
    w = getResultRow(pResultRowInfo, pResultRowInfo->curPos)->win;
  }

  /*
   * query border check, skey should not be bounded by the query time range, since the value skey will
   * be used as the time window index value. So we only change ekey of time window accordingly.
   */
  if (w.ekey > pQueryAttr->window.ekey && QUERY_IS_ASC_QUERY(pQueryAttr)) {
    w.ekey = pQueryAttr->window.ekey;
  }

  return w;
}

// a new buffer page for each table. Needs to opt this design
static int32_t addNewWindowResultBuf(SResultRow *pWindowRes, SDiskbasedBuf *pResultBuf, int32_t tid, uint32_t size) {
  if (pWindowRes->pageId != -1) {
    return 0;
  }

  SFilePage *pData = NULL;

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

static bool chkWindowOutputBufByKey(STaskRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, STimeWindow *win,
                                       bool masterscan, SResultRow **pResult, int64_t groupId, SqlFunctionCtx* pCtx,
                                       int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  assert(win->skey <= win->ekey);
  return chkResultRowFromKey(pRuntimeEnv, pResultRowInfo, (char *)&win->skey, TSDB_KEYSIZE, masterscan, groupId);
}

static int32_t setResultOutputBufByKey(STaskRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo, int64_t tid, STimeWindow *win,
                                       bool masterscan, SResultRow **pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                       int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  assert(win->skey <= win->ekey);
  SDiskbasedBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SResultRow *pResultRow = doSetResultOutBufByKey(pRuntimeEnv, pResultRowInfo, tid, (char *)&win->skey, TSDB_KEYSIZE, masterscan, tableGroupId);
  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // not assign result buffer yet, add new result buffer
  if (pResultRow->pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, (int32_t) tableGroupId, pRuntimeEnv->pQueryAttr->intermediateResultRowSize);
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

static void setResultRowOutputBufInitCtx_rv(SDiskbasedBuf * pBuf, SResultRow *pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset);

static int32_t setResultOutputBufByKey_rv(SResultRowInfo *pResultRowInfo, int64_t id, STimeWindow *win,
                                       bool masterscan, SResultRow **pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                                       int32_t numOfOutput, int32_t* rowCellInfoOffset, SAggSupporter *pAggSup, SExecTaskInfo* pTaskInfo) {
  assert(win->skey <= win->ekey);
  SResultRow *pResultRow = doSetResultOutBufByKey_rv(pAggSup->pResultBuf, pResultRowInfo, id, (char *)&win->skey, TSDB_KEYSIZE, masterscan, tableGroupId,
       pTaskInfo, true, pAggSup);

  if (pResultRow == NULL) {
    *pResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // set time window for current result
  pResultRow->win = (*win);
  *pResult = pResultRow;
  setResultRowOutputBufInitCtx_rv(pAggSup->pResultBuf, pResultRow, pCtx, numOfOutput, rowCellInfoOffset);
  return TSDB_CODE_SUCCESS;
}


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

  assert(forwardStep >= 0);
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
    if (pResultRowInfo->size == 0) {
//      assert(pResultRowInfo->current == NULL);
      assert(pResultRowInfo->curPos == -1);
      pResultRowInfo->curPos = -1;
    } else {
      pResultRowInfo->curPos = pResultRowInfo->size - 1;
    }
  } else {

    for (i = pResultRowInfo->size - 1; i >= 0; --i) {
      SResultRow *pResult = pResultRowInfo->pResult[i];
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
}

static void updateResultRowInfoActiveIndex(SResultRowInfo* pResultRowInfo, const STimeWindow* pWin, TSKEY lastKey, bool ascQuery, bool interp) {
  if ((lastKey > pWin->ekey && ascQuery) || (lastKey < pWin->ekey && (!ascQuery))) {
    closeAllResultRows(pResultRowInfo);
    pResultRowInfo->curPos = pResultRowInfo->size - 1;
  } else {
    int32_t step = ascQuery ? 1 : -1;
    doUpdateResultRowIndex(pResultRowInfo, lastKey - step, ascQuery, interp);
  }
}

static int32_t getNumOfRowsInTimeWindow(SDataBlockInfo *pDataBlockInfo, TSKEY *pPrimaryColumn,
                                        int32_t startPos, TSKEY ekey, __block_search_fn_t searchFn, STableQueryInfo* item, int32_t order) {
  assert(startPos >= 0 && startPos < pDataBlockInfo->rows);

  int32_t num   = -1;
  int32_t step  = GET_FORWARD_DIRECTION_FACTOR(order);

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

static void doApplyFunctions(SqlFunctionCtx* pCtx, STimeWindow* pWin, int32_t offset, int32_t forwardStep, TSKEY* tsCol,
                             int32_t numOfTotal, int32_t numOfOutput, int32_t order) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    pCtx[k].startTs = pWin->skey;

    // keep it temporarialy
    int32_t startOffset = pCtx[k].input.startRowIndex;
    bool    hasAgg      = pCtx[k].input.colDataAggIsSet;
    int32_t numOfRows   = pCtx[k].input.numOfRows;

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
  bool    ascQuery  = (order == TSDB_ORDER_ASC);

  int32_t precision = pInfo->precision;
  getNextTimeWindow(pInterval, precision, order, pNext);

  // next time window is not in current block
  if ((pNext->skey > pDataBlockInfo->window.ekey && order == TSDB_ORDER_ASC) ||
      (pNext->ekey < pDataBlockInfo->window.skey && order == TSDB_ORDER_DESC)) {
    return -1;
  }

  TSKEY startKey = ascQuery? pNext->skey:pNext->ekey;
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
      startPos = binarySearchForKey((char *)primaryKeys, pDataBlockInfo->rows, startKey, order);
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
        pNext->ekey += ((next - pNext->ekey + pInterval->sliding - 1)/pInterval->sliding) * pInterval->sliding;
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

static FORCE_INLINE TSKEY reviseWindowEkey(STaskAttr *pQueryAttr, STimeWindow *pWindow) {
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
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, k);
    memcpy(pRow[k], ((char*)pColInfo->pData) + (pColInfo->info.bytes * rowIndex), pColInfo->info.bytes);
  }
}

static TSKEY getStartTsKey(STimeWindow* win, const TSKEY* tsCols, int32_t rows, bool ascQuery) {
  TSKEY ts = TSKEY_INITIAL_VAL;
  if (tsCols == NULL) {
    ts = ascQuery? win->skey : win->ekey;
  } else {
    int32_t offset = ascQuery? 0:rows-1;
    ts = tsCols[offset];
  }

  return ts;
}

static void doSetInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order);

static void doSetInputDataBlockInfo(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order) {
  for (int32_t i = 0; i < pOperator->numOfOutput; ++i) {
    pCtx[i].order = order;
    pCtx[i].size  = pBlock->info.rows;
    pCtx[i].currentStage = (uint8_t)pOperator->pRuntimeEnv->scanFlag;

    setBlockStatisInfo(&pCtx[i], pBlock, NULL/*&pOperator->pExpr[i].base.colInfo*/);
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
    pCtx[i].size  = pBlock->info.rows;
    pCtx[i].currentStage = MAIN_SCAN;

    //    setBlockStatisInfo(&pCtx[i], pBlock, pOperator->pExpr[i].base.pColumns);
    int32_t slotId = pOperator->pExpr[i].base.pParam[0].pCol->slotId;

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
    pCtx[i].input.pData[0]  = taosArrayGet(pBlock->pDataBlock, slotId);
    pCtx[i].input.totalRows = pBlock->info.rows;
    pCtx[i].input.numOfRows = pBlock->info.rows;
    pCtx[i].input.startRowIndex = 0;

    ASSERT(pCtx[i].input.pData[0] != NULL);

    //        if (pCtx[i].functionId < 0) {
    //          SColumnInfoData* tsInfo = taosArrayGet(pBlock->pDataBlock, 0);
    //          pCtx[i].ptsList = (int64_t*) tsInfo->pData;

    //          continue;
    //        }

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
      pCtx[k].startTs = startTs;// this can be set during create the struct
      pCtx[k].fpSet.process(&pCtx[k]);
    }
  }
}

static void projectApplyFunctions(SqlFunctionCtx *pCtx, int32_t numOfOutput) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    // Always set the asc order for merge stage process
    if (pCtx[k].currentStage == MERGE_STAGE) {
      pCtx[k].order = TSDB_ORDER_ASC;
    }
//    pCtx[k].fpSet.process(&pCtx[k]);
//      aAggs[pCtx[k].functionId].xFunction(&pCtx[k]);
//    }
  }
}

void doTimeWindowInterpolation(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SArray* pDataBlock, TSKEY prevTs,
                               int32_t prevRowIndex, TSKEY curTs, int32_t curRowIndex, TSKEY windowKey, int32_t type) {
  STaskRuntimeEnv *pRuntimeEnv = pOperator->pRuntimeEnv;
  SExprInfo* pExpr = pOperator->pExpr;

  SqlFunctionCtx* pCtx = pInfo->pCtx;

  for (int32_t k = 0; k < pOperator->numOfOutput; ++k) {
    int32_t functionId = pCtx[k].functionId;
    if (functionId != FUNCTION_TWA && functionId != FUNCTION_INTERP) {
      pCtx[k].start.key = INT64_MIN;
      continue;
    }

    SColIndex *      pColIndex = NULL/*&pExpr[k].base.colInfo*/;
    int16_t          index = pColIndex->colIndex;
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, index);

//    assert(pColInfo->info.colId == pColIndex->info.colId && curTs != windowKey);
    double v1 = 0, v2 = 0, v = 0;

    if (prevRowIndex == -1) {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char *)pRuntimeEnv->prevRow[index]);
    } else {
      GET_TYPED_DATA(v1, double, pColInfo->info.type, (char *)pColInfo->pData + prevRowIndex * pColInfo->info.bytes);
    }

    GET_TYPED_DATA(v2, double, pColInfo->info.type, (char *)pColInfo->pData + curRowIndex * pColInfo->info.bytes);

    if (functionId == FUNCTION_INTERP) {
      if (type == RESULT_ROW_START_INTERP) {
        pCtx[k].start.key = prevTs;
        pCtx[k].start.val = v1;

        pCtx[k].end.key = curTs;
        pCtx[k].end.val = v2;

        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
          if (prevRowIndex == -1) {
            pCtx[k].start.ptr = (char *)pRuntimeEnv->prevRow[index];
          } else {
            pCtx[k].start.ptr = (char *)pColInfo->pData + prevRowIndex * pColInfo->info.bytes;
          }

          pCtx[k].end.ptr = (char *)pColInfo->pData + curRowIndex * pColInfo->info.bytes;
        }
      }
    } else if (functionId == FUNCTION_TWA) {
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

static bool setTimeWindowInterpolationStartTs(SOperatorInfo* pOperatorInfo, SqlFunctionCtx* pCtx, int32_t pos,
                                              int32_t numOfRows, SArray* pDataBlock, const TSKEY* tsCols, STimeWindow* win) {
  STaskRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

  bool ascQuery = QUERY_IS_ASC_QUERY(pQueryAttr);

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

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
  TSKEY   prevTs = ((pos == 0 && ascQuery) || (pos == (numOfRows - 1) && !ascQuery))? lastTs:tsCols[pos - step];

  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, prevTs, pos - step, curTs, pos,
      key, RESULT_ROW_START_INTERP);
  return true;
}

static bool setTimeWindowInterpolationEndTs(SOperatorInfo* pOperatorInfo, SqlFunctionCtx* pCtx,
    int32_t endRowIndex, SArray* pDataBlock, const TSKEY* tsCols, TSKEY blockEkey, STimeWindow* win) {
  STaskRuntimeEnv *pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t numOfOutput = pOperatorInfo->numOfOutput;

  TSKEY   actualEndKey = tsCols[endRowIndex];

  TSKEY key = QUERY_IS_ASC_QUERY(pQueryAttr)? win->ekey:win->skey;

  // not ended in current data block, do not invoke interpolation
  if ((key > blockEkey && QUERY_IS_ASC_QUERY(pQueryAttr)) || (key < blockEkey && !QUERY_IS_ASC_QUERY(pQueryAttr))) {
    setNotInterpoWindowKey(pCtx, numOfOutput, RESULT_ROW_END_INTERP);
    return false;
  }

  // there is actual end point of current time window, no interpolation need
  if (key == actualEndKey) {
    setNotInterpoWindowKey(pCtx, numOfOutput, RESULT_ROW_END_INTERP);
    return true;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
  int32_t nextRowIndex = endRowIndex + step;
  assert(nextRowIndex >= 0);

  TSKEY nextKey = tsCols[nextRowIndex];
  doTimeWindowInterpolation(pOperatorInfo, pOperatorInfo->info, pDataBlock, actualEndKey, endRowIndex, nextKey,
      nextRowIndex, key, RESULT_ROW_END_INTERP);
  return true;
}

static void doWindowBorderInterpolation(SOperatorInfo* pOperatorInfo, SSDataBlock* pBlock, SqlFunctionCtx* pCtx,
    SResultRow* pResult, STimeWindow* win, int32_t startPos, int32_t forwardStep, int32_t order, bool timeWindowInterpo) {
  if (!timeWindowInterpo) {
    return;
  }

  assert(pBlock != NULL);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (pBlock->pDataBlock == NULL){
//    tscError("pBlock->pDataBlock == NULL");
    return;
  }

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

    TSKEY endKey = (order == TSDB_ORDER_ASC)? pBlock->info.window.ekey:pBlock->info.window.skey;
    bool  interp = setTimeWindowInterpolationEndTs(pOperatorInfo, pCtx, endRowIndex, pBlock->pDataBlock, tsCols, endKey, win);
    if (interp) {
      setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
    }
  } else {
    setNotInterpoWindowKey(pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_END_INTERP);
  }
}

static void hashIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pSDataBlock, int32_t tableGroupId) {
  STableIntervalOperatorInfo* pInfo = (STableIntervalOperatorInfo*) pOperatorInfo->info;

  SExecTaskInfo* pTaskInfo = pOperatorInfo->pTaskInfo;
  int32_t numOfOutput = pOperatorInfo->numOfOutput;

  int32_t step = 1;
  bool ascQuery = true;

  int32_t prevIndex = pResultRowInfo->curPos;

  TSKEY* tsCols = NULL;
  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, 0);
    tsCols = (int64_t*) pColDataInfo->pData;
    assert(tsCols[0] == pSDataBlock->info.window.skey && tsCols[pSDataBlock->info.rows - 1] == pSDataBlock->info.window.ekey);
  }

  int32_t startPos = ascQuery? 0 : (pSDataBlock->info.rows - 1);
  TSKEY ts = getStartTsKey(&pSDataBlock->info.window, tsCols, pSDataBlock->info.rows, ascQuery);

  STimeWindow win = getActiveTimeWindow(pResultRowInfo, ts, &pInfo->interval, pInfo->precision, &pInfo->win);
  bool masterScan = true;

  SResultRow* pResult = NULL;
  int32_t ret = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult, tableGroupId, pInfo->binfo.pCtx,
                                        numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
  if (ret != TSDB_CODE_SUCCESS || pResult == NULL) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  int32_t forwardStep = 0;
  TSKEY   ekey = win.ekey;
  forwardStep =
      getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);

  // prev time window not interpolation yet.
  int32_t curIndex = pResultRowInfo->curPos;
  if (prevIndex != -1 && prevIndex < curIndex && pInfo->timeWindowInterpo) {
    for (int32_t j = prevIndex; j < curIndex; ++j) {  // previous time window may be all closed already.
      SResultRow* pRes = getResultRow(pResultRowInfo, j);
      if (pRes->closed) {
        assert(resultRowInterpolated(pRes, RESULT_ROW_START_INTERP) && resultRowInterpolated(pRes, RESULT_ROW_END_INTERP));
        continue;
      }

        STimeWindow w = pRes->win;
        ret = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &w, masterScan, &pResult,
                                      tableGroupId, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
        if (ret != TSDB_CODE_SUCCESS) {
          longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        }

        assert(!resultRowInterpolated(pResult, RESULT_ROW_END_INTERP));

        doTimeWindowInterpolation(pOperatorInfo, &pInfo->binfo, pSDataBlock->pDataBlock, *(TSKEY*)pInfo->pRow[0], -1,
                                  tsCols[startPos], startPos, w.ekey, RESULT_ROW_END_INTERP);

        setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
        setNotInterpoWindowKey(pInfo->binfo.pCtx, pOperatorInfo->numOfOutput, RESULT_ROW_START_INTERP);

        doApplyFunctions(pInfo->binfo.pCtx, &w, startPos, 0, tsCols, pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
      }

    // restore current time window
    ret = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult, tableGroupId, pInfo->binfo.pCtx,
                                  numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  // window start key interpolation
  doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &win, startPos, forwardStep, pInfo->order, false);
  doApplyFunctions(pInfo->binfo.pCtx, &win, startPos, forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);

  STimeWindow nextWin = win;
  while (1) {
    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
    startPos = getNextQualifiedWindow(&pInfo->interval, &nextWin, &pSDataBlock->info, tsCols, prevEndPos, pInfo);
    if (startPos < 0) {
      break;
    }

    // null data, failed to allocate more memory buffer
    int32_t code = setResultOutputBufByKey_rv(pResultRowInfo, pSDataBlock->info.uid, &nextWin, masterScan, &pResult, tableGroupId,
                                           pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset, &pInfo->aggSup, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    ekey = nextWin.ekey;//reviseWindowEkey(pQueryAttr, &nextWin);
    forwardStep = getNumOfRowsInTimeWindow(&pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, NULL, TSDB_ORDER_ASC);

    // window start(end) key interpolation
    doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &nextWin, startPos, forwardStep, pInfo->order, false);
    doApplyFunctions(pInfo->binfo.pCtx, &nextWin, startPos, forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput, TSDB_ORDER_ASC);
  }

  if (pInfo->timeWindowInterpo) {
    int32_t rowIndex = ascQuery? (pSDataBlock->info.rows-1):0;
    saveDataBlockLastRow(pInfo->pRow, pSDataBlock->pDataBlock, rowIndex, pSDataBlock->info.numOfCols);
  }

//  updateResultRowInfoActiveIndex(pResultRowInfo, &pInfo->win, pRuntimeEnv->current->lastKey, true, false);
}

static void hashAllIntervalAgg(SOperatorInfo* pOperatorInfo, SResultRowInfo* pResultRowInfo, SSDataBlock* pSDataBlock, int32_t tableGroupId) {
  STableIntervalOperatorInfo* pInfo = (STableIntervalOperatorInfo*) pOperatorInfo->info;

  STaskRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;
  int32_t           numOfOutput = pOperatorInfo->numOfOutput;
  STaskAttr*       pQueryAttr = pRuntimeEnv->pQueryAttr;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
  bool ascQuery = QUERY_IS_ASC_QUERY(pQueryAttr);

  TSKEY* tsCols = NULL;
  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, 0);
    tsCols = (int64_t*) pColDataInfo->pData;
    assert(tsCols[0] == pSDataBlock->info.window.skey &&
           tsCols[pSDataBlock->info.rows - 1] == pSDataBlock->info.window.ekey);
  }

  int32_t startPos = ascQuery? 0 : (pSDataBlock->info.rows - 1);
  TSKEY ts = getStartTsKey(&pSDataBlock->info.window, tsCols, pSDataBlock->info.rows, ascQuery);

  STimeWindow win = getCurrentActiveTimeWindow(pResultRowInfo, ts, pQueryAttr);
  bool masterScan = IS_MAIN_SCAN(pRuntimeEnv);

  SResultRow* pResult = NULL;
  int32_t forwardStep = 0;
  int32_t ret = 0;
  STimeWindow preWin = win;

  while (1) {
    // null data, failed to allocate more memory buffer
    ret = setResultOutputBufByKey(pRuntimeEnv, pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult,
                                  tableGroupId, pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    TSKEY   ekey = reviseWindowEkey(pQueryAttr, &win);
//    forwardStep = getNumOfRowsInTimeWindow(pRuntimeEnv, &pSDataBlock->info, tsCols, startPos, ekey, binarySearchForKey, true);

    // window start(end) key interpolation
//    doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &win, startPos, forwardStep);
//    doApplyFunctions(pRuntimeEnv, pInfo->binfo.pCtx, ascQuery ? &win : &preWin, startPos, forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput);
    preWin = win;

    int32_t prevEndPos = (forwardStep - 1) * step + startPos;
//    startPos = getNextQualifiedWindow(pQueryAttr, &win, &pSDataBlock->info, tsCols, binarySearchForKey, prevEndPos);
    if (startPos < 0) {
      if ((ascQuery && win.skey <= pQueryAttr->window.ekey) || ((!ascQuery) && win.ekey >= pQueryAttr->window.ekey)) {
        int32_t code = setResultOutputBufByKey(pRuntimeEnv, pResultRowInfo, pSDataBlock->info.uid, &win, masterScan, &pResult, tableGroupId,
                                               pInfo->binfo.pCtx, numOfOutput, pInfo->binfo.rowCellInfoOffset);
        if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
          longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
        }

        startPos = pSDataBlock->info.rows - 1;

        // window start(end) key interpolation
//        doWindowBorderInterpolation(pOperatorInfo, pSDataBlock, pInfo->binfo.pCtx, pResult, &win, startPos, forwardStep);
//        doApplyFunctions(pRuntimeEnv, pInfo->binfo.pCtx, ascQuery ? &win : &preWin, startPos, forwardStep, tsCols, pSDataBlock->info.rows, numOfOutput);
      }

      break;
    }
    setResultRowInterpo(pResult, RESULT_ROW_END_INTERP);
  }

  if (pQueryAttr->timeWindowInterpo) {
    int32_t rowIndex = ascQuery? (pSDataBlock->info.rows-1):0;
//    saveDataBlockLastRow(pRuntimeEnv, &pSDataBlock->info, pSDataBlock->pDataBlock, rowIndex);
  }

//  updateResultRowInfoActiveIndex(pResultRowInfo, pQueryAttr, pRuntimeEnv->current->lastKey);
}



static void doHashGroupbyAgg(SOperatorInfo* pOperator, SGroupbyOperatorInfo *pInfo, SSDataBlock *pSDataBlock) {
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  STableQueryInfo*  item = pRuntimeEnv->current;

  SColumnInfoData* pColInfoData = taosArrayGet(pSDataBlock->pDataBlock, pInfo->colIndex);

  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int16_t     bytes = pColInfoData->info.bytes;
  int16_t     type = pColInfoData->info.type;

  if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
    //qError("QInfo:0x%"PRIx64" group by not supported on double/float columns, abort", GET_TASKID(pRuntimeEnv));
    return;
  }

  SColumnInfoData* pFirstColData = taosArrayGet(pSDataBlock->pDataBlock, 0);
  int64_t* tsList = (pFirstColData->info.type == TSDB_DATA_TYPE_TIMESTAMP)? (int64_t*) pFirstColData->pData:NULL;

  STimeWindow w = TSWINDOW_INITIALIZER;

  int32_t num = 0;
  for (int32_t j = 0; j < pSDataBlock->info.rows; ++j) {
    char* val = ((char*)pColInfoData->pData) + bytes * j;
    if (isNull(val, type)) {
      continue;
    }

    // Compare with the previous row of this column, and do not set the output buffer again if they are identical.
    if (pInfo->prevData == NULL) {
      pInfo->prevData = malloc(bytes);
      memcpy(pInfo->prevData, val, bytes);
      num++;
      continue;
    }

    if (IS_VAR_DATA_TYPE(type)) {
      int32_t len = varDataLen(val);
      if(len == varDataLen(pInfo->prevData) && memcmp(varDataVal(pInfo->prevData), varDataVal(val), len) == 0) {
        num++;
        continue;
      }
    } else {
      if (memcmp(pInfo->prevData, val, bytes) == 0) {
        num++;
        continue;
      }
    }

    if (pQueryAttr->stableQuery && pQueryAttr->stabledev && (pRuntimeEnv->prevResult != NULL)) {
      setParamForStableStddevByColData(pRuntimeEnv, pInfo->binfo.pCtx, pOperator->numOfOutput, pOperator->pExpr, pInfo->prevData, bytes);
    }

    int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, &(pInfo->binfo), pOperator->numOfOutput, pInfo->prevData, type, bytes, item->groupIndex);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
    }

//    doApplyFunctions(pRuntimeEnv, pInfo->binfo.pCtx, &w, j - num, num, tsList, pSDataBlock->info.rows, pOperator->numOfOutput);

    num = 1;
    memcpy(pInfo->prevData, val, bytes);
  }

  if (num > 0) {
    char* val = ((char*)pColInfoData->pData) + bytes * (pSDataBlock->info.rows - num);
    memcpy(pInfo->prevData, val, bytes);

    if (pQueryAttr->stableQuery && pQueryAttr->stabledev && (pRuntimeEnv->prevResult != NULL)) {
      setParamForStableStddevByColData(pRuntimeEnv, pInfo->binfo.pCtx, pOperator->numOfOutput, pOperator->pExpr, val, bytes);
    }

    int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, &(pInfo->binfo), pOperator->numOfOutput, val, type, bytes, item->groupIndex);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
    }

//    doApplyFunctions(pRuntimeEnv, pInfo->binfo.pCtx, &w, pSDataBlock->info.rows - num, num, tsList, pSDataBlock->info.rows, pOperator->numOfOutput);
  }

  tfree(pInfo->prevData);
}

static void doSessionWindowAggImpl(SOperatorInfo* pOperator, SSessionAggOperatorInfo *pInfo, SSDataBlock *pSDataBlock) {
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  STableQueryInfo*  item = pRuntimeEnv->current;

  // primary timestamp column
  SColumnInfoData* pColInfoData = taosArrayGet(pSDataBlock->pDataBlock, 0);

  bool    masterScan = IS_MAIN_SCAN(pRuntimeEnv);
  SOptrBasicInfo* pBInfo = &pInfo->binfo;

  int64_t gap = pOperator->pRuntimeEnv->pQueryAttr->sw.gap;
  pInfo->numOfRows = 0;
  if (IS_REPEAT_SCAN(pRuntimeEnv) && !pInfo->reptScan) {
    pInfo->reptScan = true;
    pInfo->prevTs = INT64_MIN;
  }

  TSKEY* tsList = (TSKEY*)pColInfoData->pData;
  for (int32_t j = 0; j < pSDataBlock->info.rows; ++j) {
    if (pInfo->prevTs == INT64_MIN) {
      pInfo->curWindow.skey = tsList[j];
      pInfo->curWindow.ekey = tsList[j];
      pInfo->prevTs = tsList[j];
      pInfo->numOfRows = 1;
      pInfo->start = j;
    } else if (tsList[j] - pInfo->prevTs <= gap && (tsList[j] - pInfo->prevTs) >= 0) {
      pInfo->curWindow.ekey = tsList[j];
      pInfo->prevTs = tsList[j];
      pInfo->numOfRows += 1;
      if (j == 0 && pInfo->start != 0) {
        pInfo->numOfRows = 1;
        pInfo->start = 0;
      }
    } else {  // start a new session window
      SResultRow* pResult = NULL;

      pInfo->curWindow.ekey = pInfo->curWindow.skey;
      int32_t ret = setResultOutputBufByKey(pRuntimeEnv, &pBInfo->resultRowInfo, pSDataBlock->info.uid, &pInfo->curWindow, masterScan,
                                            &pResult, item->groupIndex, pBInfo->pCtx, pOperator->numOfOutput,
                                            pBInfo->rowCellInfoOffset);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
      }

//      doApplyFunctions(pRuntimeEnv, pBInfo->pCtx, &pInfo->curWindow, pInfo->start, pInfo->numOfRows, tsList,
//                       pSDataBlock->info.rows, pOperator->numOfOutput);

      pInfo->curWindow.skey = tsList[j];
      pInfo->curWindow.ekey = tsList[j];
      pInfo->prevTs = tsList[j];
      pInfo->numOfRows = 1;
      pInfo->start = j;
    }
  }

  SResultRow* pResult = NULL;

  pInfo->curWindow.ekey = pInfo->curWindow.skey;
  int32_t ret = setResultOutputBufByKey(pRuntimeEnv, &pBInfo->resultRowInfo, pSDataBlock->info.uid, &pInfo->curWindow, masterScan,
                                        &pResult, item->groupIndex, pBInfo->pCtx, pOperator->numOfOutput,
                                        pBInfo->rowCellInfoOffset);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
  }

//  doApplyFunctions(pRuntimeEnv, pBInfo->pCtx, &pInfo->curWindow, pInfo->start, pInfo->numOfRows, tsList,
//                   pSDataBlock->info.rows, pOperator->numOfOutput);
}

static void setResultRowKey(SResultRow* pResultRow, char* pData, int16_t type) {
  if (IS_VAR_DATA_TYPE(type)) {
    if (pResultRow->key == NULL) {
      pResultRow->key = malloc(varDataTLen(pData));
      varDataCopy(pResultRow->key, pData);
    } else {
      assert(memcmp(pResultRow->key, pData, varDataTLen(pData)) == 0);
    }
  } else {
    int64_t v = -1;
    GET_TYPED_DATA(v, int64_t, type, pData);

    pResultRow->win.skey = v;
    pResultRow->win.ekey = v;
  }
}

static int32_t setGroupResultOutputBuf(STaskRuntimeEnv *pRuntimeEnv, SOptrBasicInfo *binfo, int32_t numOfCols, char *pData, int16_t type, int16_t bytes, int32_t groupIndex) {
  SDiskbasedBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  int32_t        *rowCellInfoOffset = binfo->rowCellInfoOffset;
  SResultRowInfo *pResultRowInfo    = &binfo->resultRowInfo;
  SqlFunctionCtx *pCtx              = binfo->pCtx;

  // not assign result buffer yet, add new result buffer, TODO remove it
  char* d = pData;
  int16_t len = bytes;
  if (IS_VAR_DATA_TYPE(type)) {
    d = varDataVal(pData);
    len = varDataLen(pData);
  }

  int64_t tid = 0;
  SResultRow *pResultRow = doSetResultOutBufByKey(pRuntimeEnv, pResultRowInfo, tid, d, len, true, groupIndex);
  assert (pResultRow != NULL);

  setResultRowKey(pResultRow, pData, type);
  if (pResultRow->pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pResultRow, pResultBuf, groupIndex, pRuntimeEnv->pQueryAttr->resultRowSize);
    if (ret != 0) {
      return -1;
    }
  }

  setResultOutputBuf(pRuntimeEnv, pResultRow, pCtx, numOfCols, rowCellInfoOffset);
  initCtxOutputBuffer(pCtx, numOfCols);
  return TSDB_CODE_SUCCESS;
}

static int32_t getGroupbyColumnIndex(SGroupbyExpr *pGroupbyExpr, SSDataBlock* pDataBlock) {
  size_t num = taosArrayGetSize(pGroupbyExpr->columnInfo);
  for (int32_t k = 0; k < num; ++k) {
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
  return -1;
}

static bool functionNeedToExecute(SqlFunctionCtx *pCtx) {
  struct SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);

  // in case of timestamp column, always generated results.
  int32_t functionId = pCtx->functionId;
  if (functionId == FUNCTION_TS) {
    return true;
  }

  if (isRowEntryCompleted(pResInfo) || functionId == FUNCTION_TAG_DUMMY || functionId == FUNCTION_TS_DUMMY) {
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

void setBlockStatisInfo(SqlFunctionCtx *pCtx, SSDataBlock* pSDataBlock, SColumn* pColumn) {
  SColumnDataAgg *pAgg = NULL;

  if (pSDataBlock->pBlockAgg != NULL && TSDB_COL_IS_NORMAL_COL(pColumn->flag)) {
    pAgg = &pSDataBlock->pBlockAgg[pCtx->columnIndex];

    pCtx->agg = *pAgg;
    pCtx->isAggSet  = true;
    assert(pCtx->agg.numOfNull <= pSDataBlock->info.rows);
  } else {
    pCtx->isAggSet = false;
  }

  pCtx->hasNull = hasNull(pColumn, pAgg);

  // set the statistics data for primary time stamp column
  if (pCtx->functionId == FUNCTION_SPREAD && pColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pCtx->isAggSet  = true;
    pCtx->agg.min = pSDataBlock->info.window.skey;
    pCtx->agg.max = pSDataBlock->info.window.ekey;
  }
}

// set the output buffer for the selectivity + tag query
static int32_t setCtxTagColumnInfo(SqlFunctionCtx *pCtx, int32_t numOfOutput) {
  if (!isSelectivityWithTagsQuery(pCtx, numOfOutput)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t num = 0;
  int16_t tagLen = 0;

  SqlFunctionCtx*  p = NULL;
  SqlFunctionCtx** pTagCtx = calloc(numOfOutput, POINTER_BYTES);
  if (pTagCtx == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    int32_t functionId = pCtx[i].functionId;

    if (functionId == FUNCTION_TAG_DUMMY || functionId == FUNCTION_TS_DUMMY) {
      tagLen += pCtx[i].resDataInfo.bytes;
      pTagCtx[num++] = &pCtx[i];
    } else if (1/*(aAggs[functionId].status & FUNCSTATE_SELECTIVITY) != 0*/) {
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
    tfree(pTagCtx);
  }

  return TSDB_CODE_SUCCESS;
}

static SqlFunctionCtx* createSqlFunctionCtx(STaskRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput,
                                            int32_t** rowCellInfoOffset) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

  SqlFunctionCtx * pFuncCtx = (SqlFunctionCtx *)calloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pFuncCtx == NULL) {
    return NULL;
  }

  *rowCellInfoOffset = calloc(numOfOutput, sizeof(int32_t));
  if (*rowCellInfoOffset == 0) {
    tfree(pFuncCtx);
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprBasicInfo *pFunct = &pExpr[i].base;
    SqlFunctionCtx* pCtx = &pFuncCtx[i];
#if 0
    SColIndex *pIndex = &pFunct->colInfo;

    if (TSDB_COL_REQ_NULL(pIndex->flag)) {
      pCtx->requireNull = true;
      pIndex->flag &= ~(TSDB_COL_NULL);
    } else {
      pCtx->requireNull = false;
    }
#endif
//    pCtx->inputBytes = pFunct->colBytes;
//    pCtx->inputType  = pFunct->colType;

    pCtx->ptsOutputBuf = NULL;

    pCtx->resDataInfo.bytes  = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type   = pFunct->resSchema.type;

    pCtx->order        = pQueryAttr->order.order;
//    pCtx->functionId   = pFunct->functionId;
    pCtx->stableQuery  = pQueryAttr->stableQuery;
//    pCtx->resDataInfo.interBufSize = pFunct->interBytes;
    pCtx->start.key    = INT64_MIN;
    pCtx->end.key      = INT64_MIN;

    pCtx->numOfParams  = pFunct->numOfParams;
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
      int16_t type = pFunct->pParam[j].param.nType;
      int16_t bytes = pFunct->pParam[j].param.nType;

      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
//        taosVariantCreateFromBinary(&pCtx->param[j], pFunct->param[j].pz, bytes, type);
      } else {
//        taosVariantCreateFromBinary(&pCtx->param[j], (char *)&pFunct->param[j].i, bytes, type);
      }
    }

    // set the order information for top/bottom query
    int32_t functionId = pCtx->functionId;

    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF) {
      int32_t f = getExprFunctionId(&pExpr[0]);
      assert(f == FUNCTION_TS || f == FUNCTION_TS_DUMMY);

      pCtx->param[2].i = pQueryAttr->order.order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i = pQueryAttr->order.col.colId;
    } else if (functionId == FUNCTION_INTERP) {
      pCtx->param[2].i = (int8_t)pQueryAttr->fillType;
      if (pQueryAttr->fillVal != NULL) {
        if (isNull((const char *)&pQueryAttr->fillVal[i], pCtx->inputType)) {
          pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
        } else {  // todo refactor, taosVariantCreateFromBinary should handle the NULL value
          if (pCtx->inputType != TSDB_DATA_TYPE_BINARY && pCtx->inputType != TSDB_DATA_TYPE_NCHAR) {
            taosVariantCreateFromBinary(&pCtx->param[1], (char *)&pQueryAttr->fillVal[i], pCtx->inputBytes, pCtx->inputType);
          }
        }
      }
    } else if (functionId == FUNCTION_TS_COMP) {
      pCtx->param[0].i = pQueryAttr->vgId;  //TODO this should be the parameter from client
      pCtx->param[0].nType = TSDB_DATA_TYPE_BIGINT;
    } else if (functionId == FUNCTION_TWA) {
      pCtx->param[1].i = pQueryAttr->window.skey;
      pCtx->param[1].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[2].i = pQueryAttr->window.ekey;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
    } else if (functionId == FUNCTION_ARITHM) {
//      pCtx->param[1].pz = (char*) getScalarFuncSupport(pRuntimeEnv->scalarSup, i);
    }
  }

//  for(int32_t i = 1; i < numOfOutput; ++i) {
//    (*rowCellInfoOffset)[i] = (int32_t)((*rowCellInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) + pExpr[i - 1].base.interBytes);
//  }

  setCtxTagColumnInfo(pFuncCtx, numOfOutput);

  return pFuncCtx;
}

static SqlFunctionCtx* createSqlFunctionCtx_rv(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowCellInfoOffset) {
  SqlFunctionCtx * pFuncCtx = (SqlFunctionCtx *)calloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pFuncCtx == NULL) {
    return NULL;
  }

  *rowCellInfoOffset = calloc(numOfOutput, sizeof(int32_t));
  if (*rowCellInfoOffset == 0) {
    tfree(pFuncCtx);
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = &pExprInfo[i];

    SExprBasicInfo *pFunct = &pExpr->base;
    SqlFunctionCtx* pCtx = &pFuncCtx[i];

    fmGetFuncExecFuncs(pExpr->pExpr->_function.pFunctNode->funcId, &pCtx->fpSet);
    pCtx->input.numOfInputCols = pFunct->numOfParams;

    pCtx->input.pData = calloc(pFunct->numOfParams, POINTER_BYTES);
    pCtx->input.pColumnDataAgg = calloc(pFunct->numOfParams, POINTER_BYTES);

    pCtx->ptsOutputBuf       = NULL;
    pCtx->resDataInfo.bytes  = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type   = pFunct->resSchema.type;
    pCtx->order              = TSDB_ORDER_ASC;
    pCtx->start.key          = INT64_MIN;
    pCtx->end.key            = INT64_MIN;

    SFuncExecEnv env = {0};
    pCtx->fpSet.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
    pCtx->resDataInfo.interBufSize = env.calcMemSize;
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

  for(int32_t i = 1; i < numOfOutput; ++i) {
    (*rowCellInfoOffset)[i] = (int32_t)((*rowCellInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) + pFuncCtx[i].resDataInfo.interBufSize);
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
    tfree(pCtx[i].subsidiaryRes.pCtx);
  }

  tfree(pCtx);
  return NULL;
}

static int32_t setupQueryRuntimeEnv(STaskRuntimeEnv *pRuntimeEnv, int32_t numOfTables, SArray* pOperator, void* merger) {
  //qDebug("QInfo:0x%"PRIx64" setup runtime env", GET_TASKID(pRuntimeEnv));
  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;

  pRuntimeEnv->prevGroupId = INT32_MIN;
  pRuntimeEnv->pQueryAttr = pQueryAttr;

  pRuntimeEnv->pResultRowHashTable = taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pRuntimeEnv->pResultRowListSet = taosHashInit(numOfTables * 10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  pRuntimeEnv->keyBuf  = malloc(pQueryAttr->maxTableColumnWidth + sizeof(int64_t) + POINTER_BYTES);
//  pRuntimeEnv->pool    = initResultRowPool(getResultRowSize(pRuntimeEnv));
  pRuntimeEnv->pResultRowArrayList = taosArrayInit(numOfTables, sizeof(SResultRowCell));

  pRuntimeEnv->prevRow = malloc(POINTER_BYTES * pQueryAttr->numOfCols + pQueryAttr->srcRowSize);
  pRuntimeEnv->tagVal  = malloc(pQueryAttr->tagLen);

  // NOTE: pTableCheckInfo need to update the query time range and the lastKey info
  pRuntimeEnv->pTableRetrieveTsMap = taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  //pRuntimeEnv->scalarSup = createScalarFuncSupport(pQueryAttr->numOfOutput);

  if (pRuntimeEnv->scalarSup == NULL || pRuntimeEnv->pResultRowHashTable == NULL || pRuntimeEnv->keyBuf == NULL ||
      pRuntimeEnv->prevRow == NULL  || pRuntimeEnv->tagVal == NULL) {
    goto _clean;
  }

  if (pQueryAttr->numOfCols) {
    char* start = POINTER_BYTES * pQueryAttr->numOfCols + (char*) pRuntimeEnv->prevRow;
    pRuntimeEnv->prevRow[0] = start;
    for(int32_t i = 1; i < pQueryAttr->numOfCols; ++i) {
      pRuntimeEnv->prevRow[i] = pRuntimeEnv->prevRow[i - 1] + pQueryAttr->tableCols[i-1].bytes;
    }

    if (pQueryAttr->tableCols[0].type == TSDB_DATA_TYPE_TIMESTAMP) {
      *(int64_t*) pRuntimeEnv->prevRow[0] = INT64_MIN;
    }
  }

  //qDebug("QInfo:0x%"PRIx64" init runtime environment completed", GET_TASKID(pRuntimeEnv));

  // group by normal column, sliding window query, interval query are handled by interval query processor
  // interval (down sampling operation)
  return TSDB_CODE_SUCCESS;

_clean:
  //destroyScalarFuncSupport(pRuntimeEnv->scalarSup, pRuntimeEnv->pQueryAttr->numOfOutput);
  tfree(pRuntimeEnv->pResultRowHashTable);
  tfree(pRuntimeEnv->keyBuf);
  tfree(pRuntimeEnv->prevRow);
  tfree(pRuntimeEnv->tagVal);

  return TSDB_CODE_QRY_OUT_OF_MEMORY;
}

static void doFreeQueryHandle(STaskRuntimeEnv* pRuntimeEnv) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

//  tsdbCleanupReadHandle(pRuntimeEnv->pTsdbReadHandle);
  pRuntimeEnv->pTsdbReadHandle = NULL;

//  SMemRef* pMemRef = &pQueryAttr->memRef;
//  assert(pMemRef->ref == 0 && pMemRef->snapshot.imem == NULL && pMemRef->snapshot.mem == NULL);
}

static void destroyTsComp(STaskRuntimeEnv *pRuntimeEnv, STaskAttr *pQueryAttr) {
  if (pQueryAttr->tsCompQuery && pRuntimeEnv->outputBuf && pRuntimeEnv->outputBuf->pDataBlock && taosArrayGetSize(pRuntimeEnv->outputBuf->pDataBlock) > 0) {
    SColumnInfoData* pColInfoData = taosArrayGet(pRuntimeEnv->outputBuf->pDataBlock, 0);
    if (pColInfoData) {
      TdFilePtr pFile = *(TdFilePtr *)pColInfoData->pData;  // TODO refactor
      if (pFile != NULL) {
        taosCloseFile(&pFile);
        *(TdFilePtr *)pColInfoData->pData = NULL;
      }
    }
  }
}

bool isTaskKilled(SExecTaskInfo *pTaskInfo) {
  // query has been executed more than tsShellActivityTimer, and the retrieve has not arrived
  // abort current query execution.
  if (pTaskInfo->owner != 0 && ((taosGetTimestampSec() - pTaskInfo->cost.start/1000) > 10*getMaximumIdleDurationSec())
      /*(!needBuildResAfterQueryComplete(pTaskInfo))*/) {

    assert(pTaskInfo->cost.start != 0);
//    qDebug("QInfo:%" PRIu64 " retrieve not arrive beyond %d ms, abort current query execution, start:%" PRId64
//           ", current:%d", pQInfo->qId, 1, pQInfo->startExecTs, taosGetTimestampSec());
//    return true;
  }

  return false;
}

void setTaskKilled(SExecTaskInfo *pTaskInfo) { pTaskInfo->code = TSDB_CODE_TSC_QUERY_CANCELLED;}

//static bool isFixedOutputQuery(STaskAttr* pQueryAttr) {
//  if (QUERY_IS_INTERVAL_QUERY(pQueryAttr)) {
//    return false;
//  }
//
//  // Note:top/bottom query is fixed output query
//  if (pQueryAttr->topBotQuery || pQueryAttr->groupbyColumn || pQueryAttr->tsCompQuery) {
//    return true;
//  }
//
//  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
//    SExprBasicInfo *pExpr = &pQueryAttr->pExpr1[i].base;
//
//    if (pExpr->functionId == FUNCTION_TS || pExpr->functionId == FUNCTION_TS_DUMMY) {
//      continue;
//    }
//
//    if (!IS_MULTIOUTPUT(aAggs[pExpr->functionId].status)) {
//      return true;
//    }
//  }
//
//  return false;
//}

// todo refactor with isLastRowQuery
//bool isPointInterpoQuery(STaskAttr *pQueryAttr) {
//  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
//    int32_t functionId = pQueryAttr->pExpr1[i].base.functionId;
//    if (functionId == FUNCTION_INTERP) {
//      return true;
//    }
//  }
//
//  return false;
//}

static bool isFirstLastRowQuery(STaskAttr *pQueryAttr) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functionID = getExprFunctionId(&pQueryAttr->pExpr1[i]);
    if (functionID == FUNCTION_LAST_ROW) {
      return true;
    }
  }

  return false;
}

static bool isCachedLastQuery(STaskAttr *pQueryAttr) {
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
//todo refactor : return window
void getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, int64_t keyFirst, int64_t keyLast, STimeWindow *win) {
  assert(key >= keyFirst && key <= keyLast && pInterval->sliding <= pInterval->interval);
  win->skey = taosTimeTruncate(key, pInterval, precision);

  /*
   * if the realSkey > INT64_MAX - pInterval->interval, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  if (keyFirst > (INT64_MAX - pInterval->interval)) {
    assert(keyLast - keyFirst < pInterval->interval);
    win->ekey = INT64_MAX;
  } else if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
    win->ekey = taosTimeAdd(win->skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
  } else {
    win->ekey = win->skey + pInterval->interval - 1;
  }
}

/*
 * todo add more parameters to check soon..
 */
bool colIdCheck(STaskAttr *pQueryAttr, uint64_t qId) {
  // load data column information is incorrect
  for (int32_t i = 0; i < pQueryAttr->numOfCols - 1; ++i) {
    if (pQueryAttr->tableCols[i].colId == pQueryAttr->tableCols[i + 1].colId) {
      //qError("QInfo:0x%"PRIx64" invalid data load column for query", qId);
      return false;
    }
  }

  return true;
}

// todo ignore the avg/sum/min/max/count/stddev/top/bottom functions, of which
// the scan order is not matter
static bool onlyOneQueryType(STaskAttr *pQueryAttr, int32_t functId, int32_t functIdDst) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functionId = getExprFunctionId(&pQueryAttr->pExpr1[i]);

    if (functionId == FUNCTION_TS || functionId == FUNCTION_TS_DUMMY || functionId == FUNCTION_TAG ||
        functionId == FUNCTION_TAG_DUMMY) {
      continue;
    }

    if (functionId != functId && functionId != functIdDst) {
      return false;
    }
  }

  return true;
}

static bool onlyFirstQuery(STaskAttr *pQueryAttr) { return onlyOneQueryType(pQueryAttr, FUNCTION_FIRST, FUNCTION_FIRST_DST); }

static bool onlyLastQuery(STaskAttr *pQueryAttr) { return onlyOneQueryType(pQueryAttr, FUNCTION_LAST, FUNCTION_LAST_DST); }

static bool notContainSessionOrStateWindow(STaskAttr *pQueryAttr) { return !(pQueryAttr->sw.gap > 0 || pQueryAttr->stateWindow); }

static int32_t updateBlockLoadStatus(STaskAttr *pQuery, int32_t status) {
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
    if(!hasOtherFunc) {
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
  for(int32_t i = 0; i < num; ++i) {
    SArray* p1 = taosArrayGetP(pQueryAttr->tableGroupInfo.pGroupList, i);

    size_t len = taosArrayGetSize(p1);
    for(int32_t j = 0; j < len; ++j) {
//      STableKeyInfo* pInfo = taosArrayGet(p1, j);
//
//      // update the new lastkey if it is equalled to the value of the old skey
//      if (pInfo->lastKey == win->ekey) {
//        pInfo->lastKey = win->skey;
//      }
    }
  }
}

//static void updateDataCheckOrder(SQInfo *pQInfo, SQueryTableReq* pQueryMsg, bool stableQuery) {
//  STaskAttr* pQueryAttr = pQInfo->runtimeEnv.pQueryAttr;
//
//  // in case of point-interpolation query, use asc order scan
//  char msg[] = "QInfo:0x%"PRIx64" scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%" PRId64
//               "-%" PRId64 ", new qrange:%" PRId64 "-%" PRId64;
//
//  // todo handle the case the the order irrelevant query type mixed up with order critical query type
//  // descending order query for last_row query
//  if (isFirstLastRowQuery(pQueryAttr)) {
//    //qDebug("QInfo:0x%"PRIx64" scan order changed for last_row query, old:%d, new:%d", pQInfo->qId, pQueryAttr->order.order, TSDB_ORDER_ASC);
//
//    pQueryAttr->order.order = TSDB_ORDER_ASC;
//    if (pQueryAttr->window.skey > pQueryAttr->window.ekey) {
//      TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//    }
//
//    pQueryAttr->needReverseScan = false;
//    return;
//  }
//
//  if (pQueryAttr->groupbyColumn && pQueryAttr->order.order == TSDB_ORDER_DESC) {
//    pQueryAttr->order.order = TSDB_ORDER_ASC;
//    if (pQueryAttr->window.skey > pQueryAttr->window.ekey) {
//      TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//    }
//
//    pQueryAttr->needReverseScan = false;
//    doUpdateLastKey(pQueryAttr);
//    return;
//  }
//
//  if (pQueryAttr->pointInterpQuery && pQueryAttr->interval.interval == 0) {
//    if (!QUERY_IS_ASC_QUERY(pQueryAttr)) {
//      //qDebug(msg, pQInfo->qId, "interp", pQueryAttr->order.order, TSDB_ORDER_ASC, pQueryAttr->window.skey, pQueryAttr->window.ekey, pQueryAttr->window.ekey, pQueryAttr->window.skey);
//      TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey, TSKEY);
//    }
//
//    pQueryAttr->order.order = TSDB_ORDER_ASC;
//    return;
//  }
//
//  if (pQueryAttr->interval.interval == 0) {
//    if (onlyFirstQuery(pQueryAttr)) {
//      if (!QUERY_IS_ASC_QUERY(pQueryAttr)) {
//        //qDebug(msg, pQInfo->qId, "only-first", pQueryAttr->order.order, TSDB_ORDER_ASC, pQueryAttr->window.skey,
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
////                 pQueryAttr->window.skey, pQueryAttr->window.ekey, pQueryAttr->window.ekey, pQueryAttr->window.skey);
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
////                 pQueryAttr->window.skey, pQueryAttr->window.ekey, pQueryAttr->window.ekey, pQueryAttr->window.skey);
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
  int32_t MIN_ROWS_PER_PAGE = 4;

  *rowsize = (int32_t)(pQueryAttr->resultRowSize * getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery));
  int32_t overhead = sizeof(SFilePage);

  // one page contains at least two rows
  *ps = DEFAULT_INTERN_BUF_PAGE_SIZE;
  while(((*rowsize) * MIN_ROWS_PER_PAGE) > (*ps) - overhead) {
    *ps = ((*ps) << 1u);
  }
}

#define IS_PREFILTER_TYPE(_t) ((_t) != TSDB_DATA_TYPE_BINARY && (_t) != TSDB_DATA_TYPE_NCHAR)

//static FORCE_INLINE bool doFilterByBlockStatistics(STaskRuntimeEnv* pRuntimeEnv, SDataStatis *pDataStatis, SqlFunctionCtx *pCtx, int32_t numOfRows) {
//  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
//
//  if (pDataStatis == NULL || pQueryAttr->pFilters == NULL) {
//    return true;
//  }
//
//  return filterRangeExecute(pQueryAttr->pFilters, pDataStatis, pQueryAttr->numOfCols, numOfRows);
//}

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

    while(1) {
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

    while(1) {
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

static int32_t doTSJoinFilter(STaskRuntimeEnv *pRuntimeEnv, TSKEY key, bool ascQuery) {
  STSElem elem = tsBufGetElem(pRuntimeEnv->pTsBuf);

#if defined(_DEBUG_VIEW)
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64 ", tag:%"PRIu64", query order:%d, ts order:%d, traverse:%d, index:%d\n",
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

  int8_t *p = calloc(numOfRows, sizeof(int8_t));
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

  tfree(p);
}

void filterColRowsInDataBlock(STaskRuntimeEnv* pRuntimeEnv, SSDataBlock* pBlock, bool ascQuery) {
 int32_t numOfRows = pBlock->info.rows;

 int8_t *p = NULL;
 bool    all = true;

 if (pRuntimeEnv->pTsBuf != NULL) {
   SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, 0);   
   p = calloc(numOfRows, sizeof(int8_t));
   
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
//   pRuntimeEnv->current->cur = tsBufGetCursor(pRuntimeEnv->pTsBuf);
 } else {
//   all = filterExecute(pRuntimeEnv->pQueryAttr->pFilters, numOfRows, &p, pBlock->pBlockAgg, pRuntimeEnv->pQueryAttr->numOfCols);
 }

 if (!all) {
   if (p) {
     doCompactSDataBlock(pBlock, numOfRows, p);
   } else {
     pBlock->info.rows = 0;
     pBlock->pBlockAgg = NULL;  // clean the block statistics info
   }
 }

 tfree(p);
}

static SColumnInfo* doGetTagColumnInfoById(SColumnInfo* pTagColList, int32_t numOfTags, int16_t colId);
static void doSetTagValueInParam(void* pTable, int32_t tagColId, SVariant *tag, int16_t type, int16_t bytes);

static uint32_t doFilterByBlockTimeWindow(STableScanInfo* pTableScanInfo, SSDataBlock* pBlock) {
  SqlFunctionCtx* pCtx = pTableScanInfo->pCtx;
  uint32_t status = BLK_DATA_NO_NEEDED;

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

void doSetFilterColumnInfo(SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols, SSDataBlock* pBlock) {
  // set the initial static data value filter expression
  for (int32_t i = 0; i < numOfFilterCols; ++i) {
    for (int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, j);

      if (pFilterInfo[i].info.colId == pColInfo->info.colId) {
        pFilterInfo[i].pData = pColInfo->pData;
        break;
      }
    }
  }
}

int32_t loadDataBlock(SExecTaskInfo *pTaskInfo, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock, uint32_t* status) {
  STaskCostInfo* pCost = &pTaskInfo->cost;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;

  pCost->totalCheckedRows += pBlock->info.rows;
  pCost->loadBlocks += 1;

  *status = BLK_DATA_ALL_NEEDED;

  pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pTsdbReadHandle, NULL);
  if (pBlock->pDataBlock == NULL) {
    return terrno;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t loadDataBlockOnDemand(SExecTaskInfo *pTaskInfo, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock, uint32_t* status) {
  *status = BLK_DATA_NO_NEEDED;

  pBlock->pDataBlock = NULL;
  pBlock->pBlockAgg  = NULL;

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
 * set tag value in SqlFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(void* pTable, int32_t tagColId, SVariant *tag, int16_t type, int16_t bytes) {
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
    int32_t len = (varDataLen(val) > maxLen)? maxLen:varDataLen(val);
    taosVariantCreateFromBinary(tag, varDataVal(val), len, type);
    //taosVariantCreateFromBinary(tag, varDataVal(val), varDataLen(val), type);
  } else {
    taosVariantCreateFromBinary(tag, val, bytes, type);
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

void setTagValue(SOperatorInfo* pOperatorInfo, void *pTable, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  STaskRuntimeEnv* pRuntimeEnv = pOperatorInfo->pRuntimeEnv;

  SExprInfo  *pExpr      = pOperatorInfo->pExpr;
  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;

  SExprInfo* pExprInfo = &pExpr[0];
  int32_t functionId = getExprFunctionId(pExprInfo);

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
      doSetTagValueInParam(pTable, pLocalExprInfo->base.pParam[0].pCol->colId, &pCtx[idx].tag, pLocalExprInfo->base.resSchema.type,
                           pLocalExprInfo->base.resSchema.bytes);

      if (IS_NUMERIC_TYPE(pLocalExprInfo->base.resSchema.type)
          || pLocalExprInfo->base.resSchema.type == TSDB_DATA_TYPE_BOOL
          || pLocalExprInfo->base.resSchema.type == TSDB_DATA_TYPE_TIMESTAMP) {
        memcpy(pRuntimeEnv->tagVal + offset, &pCtx[idx].tag.i, pLocalExprInfo->base.resSchema.bytes);
      } else {
        if (pCtx[idx].tag.pz != NULL) {
          memcpy(pRuntimeEnv->tagVal + offset, pCtx[idx].tag.pz, pCtx[idx].tag.nLen);
        }      
      }

      offset += pLocalExprInfo->base.resSchema.bytes;
    }

    //todo : use index to avoid iterator all possible output columns
    if (pQueryAttr->stableQuery && pQueryAttr->stabledev && (pRuntimeEnv->prevResult != NULL)) {
      setParamForStableStddev(pRuntimeEnv, pCtx, numOfOutput, pExprInfo);
    }
  }

  // set the tsBuf start position before check each data block
  if (pRuntimeEnv->pTsBuf != NULL) {
    setCtxTagForJoin(pRuntimeEnv, &pCtx[0], pExprInfo, pTable);
  }
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

static void updateTableQueryInfoForReverseScan(STableQueryInfo *pTableQueryInfo) {
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

static void setupQueryRangeForReverseScan(STableScanInfo* pTableScanInfo) {
#if 0
  int32_t numOfGroups = (int32_t)(GET_NUM_OF_TABLEGROUP(pRuntimeEnv));
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray *group = GET_TABLEGROUP(pRuntimeEnv, i);
    SArray *tableKeyGroup = taosArrayGetP(pQueryAttr->tableGroupInfo.pGroupList, i);

    size_t t = taosArrayGetSize(group);
    for (int32_t j = 0; j < t; ++j) {
      STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);
      updateTableQueryInfoForReverseScan(pCheckInfo);

      // update the last key in tableKeyInfo list, the tableKeyInfo is used to build the tsdbQueryHandle and decide
      // the start check timestamp of tsdbQueryHandle
//      STableKeyInfo *pTableKeyInfo = taosArrayGet(tableKeyGroup, j);
//      pTableKeyInfo->lastKey = pCheckInfo->lastKey;
//
//      assert(pCheckInfo->pTable == pTableKeyInfo->pTable);
    }
  }
#endif

}

void switchCtxOrder(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SWITCH_ORDER(pCtx[i].order);
  }
}

// TODO fix this bug.
int32_t initResultRow(SResultRow *pResultRow) {
  pResultRow->pEntryInfo = (struct SResultRowEntryInfo*)((char*)pResultRow + sizeof(SResultRow));
  return TSDB_CODE_SUCCESS;
}

/*
 * The start of each column SResultRowEntryInfo is denote by RowCellInfoOffset.
 * Note that in case of top/bottom query, the whole multiple rows of result is treated as only one row of results.
 * +------------+-----------------result column 1------------+------------------result column 2-----------+
 * | SResultRow | SResultRowEntryInfo | intermediate buffer1 | SResultRowEntryInfo | intermediate buffer 2|
 * +------------+--------------------------------------------+--------------------------------------------+
 *           offset[0]                                  offset[1]                                   offset[2]
 */
void setDefaultOutputBuf(STaskRuntimeEnv *pRuntimeEnv, SOptrBasicInfo *pInfo, int64_t uid, int32_t stage) {
  SqlFunctionCtx* pCtx           = pInfo->pCtx;
  SSDataBlock* pDataBlock        = pInfo->pRes;
  int32_t* rowCellInfoOffset     = pInfo->rowCellInfoOffset;
  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;

  int64_t tid = 0;
  pRuntimeEnv->keyBuf = realloc(pRuntimeEnv->keyBuf, sizeof(tid) + sizeof(int64_t) + POINTER_BYTES);
  SResultRow* pRow = doSetResultOutBufByKey(pRuntimeEnv, pResultRowInfo, tid, (char *)&tid, sizeof(tid), true, uid);

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData* pData = taosArrayGet(pDataBlock->pDataBlock, i);

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT/TAGPRJ/PRJ/TAG etc.
     */
    struct SResultRowEntryInfo* pEntry = getResultCell(pRow, i, rowCellInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo   = pEntry;
    pCtx[i].pOutput      = pData->pData;
    pCtx[i].currentStage = stage;
    assert(pCtx[i].pOutput != NULL);

    // set the timestamp output buffer for top/bottom/diff query
    int32_t fid = pCtx[i].functionId;
    if (fid == FUNCTION_TOP || fid == FUNCTION_BOTTOM || fid == FUNCTION_DIFF || fid == FUNCTION_DERIVATIVE) {
      if (i > 0) pCtx[i].ptsOutputBuf = pCtx[i-1].pOutput;
    }
  }

  initCtxOutputBuffer(pCtx, pDataBlock->info.numOfCols);
}

// TODO refactor: some function move away
void setFunctionResultOutput(SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage, SExecTaskInfo* pTaskInfo) {
  SqlFunctionCtx* pCtx           = pInfo->pCtx;
  SSDataBlock* pDataBlock        = pInfo->pRes;
  int32_t* rowCellInfoOffset     = pInfo->rowCellInfoOffset;
  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;

  int64_t tid = 0;
  int64_t groupId = 0;
  SResultRow* pRow = doSetResultOutBufByKey_rv(pSup->pResultBuf, pResultRowInfo, tid, (char *)&tid, sizeof(tid), true, groupId, pTaskInfo, false, pSup);

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData* pData = taosArrayGet(pDataBlock->pDataBlock, i);

    struct SResultRowEntryInfo* pEntry = getResultCell(pRow, i, rowCellInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo   = pEntry;
    pCtx[i].currentStage = stage;

    // set the timestamp output buffer for top/bottom/diff query
//    int32_t fid = pCtx[i].functionId;
//    if (fid == FUNCTION_TOP || fid == FUNCTION_BOTTOM || fid == FUNCTION_DIFF || fid == FUNCTION_DERIVATIVE) {
//      if (i > 0) pCtx[i].ptsOutputBuf = pCtx[i-1].pOutput;
//    }
  }

  initCtxOutputBuffer(pCtx, pDataBlock->info.numOfCols);
}

void updateOutputBuf(SOptrBasicInfo* pBInfo, int32_t *bufCapacity, int32_t numOfInputRows) {
  SSDataBlock* pDataBlock = pBInfo->pRes;

  int32_t newSize = pDataBlock->info.rows + numOfInputRows + 5; // extra output buffer
  if ((*bufCapacity) < newSize) {
    for(int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
      SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);

      char* p = realloc(pColInfo->pData, newSize * pColInfo->info.bytes);
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
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);
    pBInfo->pCtx[i].pOutput = pColInfo->pData + pColInfo->info.bytes * pDataBlock->info.rows;

    // set the correct pointer after the memory buffer reallocated.
    int32_t functionId = pBInfo->pCtx[i].functionId;

    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF || functionId == FUNCTION_DERIVATIVE) {
      if (i > 0) pBInfo->pCtx[i].ptsOutputBuf = pBInfo->pCtx[i-1].pOutput;
    }
  }
}

void copyTsColoum(SSDataBlock* pRes, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  bool    needCopyTs = false;
  int32_t tsNum = 0;
  char *src = NULL;
  for (int32_t i = 0; i < numOfOutput; i++) {
    int32_t functionId = pCtx[i].functionId;
    if (functionId == FUNCTION_DIFF || functionId == FUNCTION_DERIVATIVE) {
      needCopyTs = true;
      if (i > 0  && pCtx[i-1].functionId == FUNCTION_TS_DUMMY) {
        SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, i - 1); // find ts data
        src = pColRes->pData;
      }
    } else if(functionId == FUNCTION_TS_DUMMY) {
      tsNum++;
    }
  }

  if (!needCopyTs) return;
  if (tsNum < 2) return;
  if (src == NULL) return;

  for (int32_t i = 0; i < numOfOutput; i++) {
    int32_t functionId = pCtx[i].functionId;
    if(functionId == FUNCTION_TS_DUMMY) {
      SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, i);
      memcpy(pColRes->pData, src, pColRes->info.bytes * pRes->info.rows);
    }
  }
}

void clearOutputBuf(SOptrBasicInfo* pBInfo, int32_t *bufCapacity) {
  SSDataBlock* pDataBlock = pBInfo->pRes;

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock->pDataBlock, i);

    int32_t functionId = pBInfo->pCtx[i].functionId;
    if (functionId < 0) {
      memset(pBInfo->pCtx[i].pOutput, 0, pColInfo->info.bytes * (*bufCapacity));
    }
  }
}

void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size) {
  for (int32_t j = 0; j < size; ++j) {
    struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (isRowEntryInitialized(pResInfo)) {
      continue;
    }

    pCtx[j].fpSet.init(&pCtx[j], pCtx[j].resultInfo);
  }
}

void setTaskStatus(SExecTaskInfo *pTaskInfo, int8_t status) {
  if (status == TASK_NOT_COMPLETED) {
    pTaskInfo->status = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    CLEAR_QUERY_STATUS(pTaskInfo, TASK_NOT_COMPLETED);
    pTaskInfo->status |= status;
  }
}

static void setupEnvForReverseScan(STableScanInfo *pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
//  if (pRuntimeEnv->pTsBuf) {
//    SWITCH_ORDER(pRuntimeEnv->pTsBuf->cur.order);
//    bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
//    assert(ret);
//  }

  // reverse order time range
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);
//  setTaskStatus(pTableScanInfo, QUERY_NOT_COMPLETED);

  switchCtxOrder(pCtx, numOfOutput);

  SWITCH_ORDER(pTableScanInfo->order);
  setupQueryRangeForReverseScan(pTableScanInfo);

  pTableScanInfo->times        = 1;
  pTableScanInfo->current      = 0;
  pTableScanInfo->reverseTimes = 0;
}

void finalizeQueryResult(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t j = 0; j < numOfOutput; ++j) {
    pCtx[j].fpSet.finalize(&pCtx[j]);
  }
}

void finalizeMultiTupleQueryResult(SqlFunctionCtx* pCtx, int32_t numOfOutput, SDiskbasedBuf *pBuf, SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset) {
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRowPosition* pPos = &pResultRowInfo->pPosition[i];

    SFilePage* bufPage = getBufPage(pBuf, pPos->pageId);
    SResultRow* pRow = (SResultRow*)((char*)bufPage + pPos->offset);
    if (!isResultRowClosed(pResultRowInfo, i)) {
      continue;
    }

    for (int32_t j = 0; j < numOfOutput; ++j) {
      pCtx[j].resultInfo = getResultCell(pRow, j, rowCellInfoOffset);

      struct SResultRowEntryInfo* pResInfo = pCtx[j].resultInfo;
      if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
        continue;
      }

      pCtx[j].fpSet.finalize(&pCtx[j]);

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

static bool hasMainOutput(STaskAttr *pQueryAttr) {
  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    int32_t functionId = getExprFunctionId(&pQueryAttr->pExpr1[i]);

    if (functionId != FUNCTION_TS && functionId != FUNCTION_TAG && functionId != FUNCTION_TAGPRJ) {
      return true;
    }
  }

  return false;
}

STableQueryInfo *createTableQueryInfo(void* buf, bool groupbyColumn, STimeWindow win) {
  STableQueryInfo *pTableQueryInfo = buf;
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

STableQueryInfo* createTmpTableQueryInfo(STimeWindow win) {
  STableQueryInfo* pTableQueryInfo = calloc(1, sizeof(STableQueryInfo));

//  pTableQueryInfo->win = win;
  pTableQueryInfo->lastKey = win.skey;

  // set more initial size of interval/groupby query
  int32_t initialSize = 16;
  int32_t code = initResultRowInfo(&pTableQueryInfo->resInfo, initialSize);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pTableQueryInfo);
    return NULL;
  }

  return pTableQueryInfo;
}

void destroyTableQueryInfoImpl(STableQueryInfo *pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

//  taosVariantDestroy(&pTableQueryInfo->tag);
  cleanupResultRowInfo(&pTableQueryInfo->resInfo);
}

void setResultRowOutputBufInitCtx(STaskRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SqlFunctionCtx* pCtx,
    int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  SFilePage* bufPage = getBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  int32_t offset = 0;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);

    struct SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
      offset += pCtx[i].resDataInfo.bytes;
      continue;
    }

    pCtx[i].pOutput = getPosInResultPage(pRuntimeEnv->pQueryAttr, bufPage, pResult->offset, offset);
    offset += pCtx[i].resDataInfo.bytes;

    int32_t functionId = pCtx[i].functionId;
    if (functionId < 0) {
      continue;
    }

    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF) {
      if(i > 0) pCtx[i].ptsOutputBuf = pCtx[i-1].pOutput;
    }

//    if (!pResInfo->initialized) {
//      aAggs[functionId].init(&pCtx[i], pResInfo);
//    }
  }
}

void setResultRowOutputBufInitCtx_rv(SDiskbasedBuf * pBuf, SResultRow *pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  SFilePage* bufPage = getBufPage(pBuf, pResult->pageId);

//  int32_t offset = 0;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);

    struct SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
//      offset += pCtx[i].resDataInfo.bytes;
      continue;
    }

//    offset += pCtx[i].resDataInfo.bytes;

//    int32_t functionId = pCtx[i].functionId;
//    if (functionId < 0) {
//      continue;
//    }
//    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF) {
//      if (i > 0) pCtx[i].ptsOutputBuf = pCtx[i - 1].pOutput;
//    }

    if (!pResInfo->initialized) {
      pCtx[i].fpSet.init(&pCtx[i], pResInfo);
    }
  }
}

void doSetTableGroupOutputBuf(SAggOperatorInfo* pAggInfo, int32_t numOfOutput, int32_t tableGroupId, SExecTaskInfo* pTaskInfo) {
  // for simple group by query without interval, all the tables belong to one group result.
  int64_t uid = 0;
  int64_t tid = 0;

  SResultRowInfo* pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
  SqlFunctionCtx* pCtx = pAggInfo->binfo.pCtx;
  int32_t* rowCellInfoOffset = pAggInfo->binfo.rowCellInfoOffset;

  SResultRow* pResultRow =
      doSetResultOutBufByKey_rv(pAggInfo->pResultBuf, pResultRowInfo, tid, (char*)&tableGroupId, sizeof(tableGroupId), true, uid, pTaskInfo, false, &pAggInfo->aggSup);
  assert (pResultRow != NULL);

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

  setResultRowOutputBufInitCtx_rv(pAggInfo->pResultBuf, pResultRow, pCtx, numOfOutput, rowCellInfoOffset);
}

void setExecutionContext(int32_t numOfOutput, int32_t tableGroupId, TSKEY nextKey, SExecTaskInfo* pTaskInfo, STableQueryInfo *pTableQueryInfo, SAggOperatorInfo* pAggInfo) {
  // lastKey needs to be updated
  pTableQueryInfo->lastKey = nextKey;
  if (pAggInfo->groupId != INT32_MIN && pAggInfo->groupId == tableGroupId) {
    return;
  }

  doSetTableGroupOutputBuf(pAggInfo, numOfOutput, tableGroupId, pTaskInfo);

  // record the current active group id
  pAggInfo->groupId = tableGroupId;
}

void setResultOutputBuf(STaskRuntimeEnv *pRuntimeEnv, SResultRow *pResult, SqlFunctionCtx* pCtx,
    int32_t numOfCols, int32_t* rowCellInfoOffset) {
  // Note: pResult->pos[i]->num == 0, there is only fixed number of results for each group
  SFilePage *page = getBufPage(pRuntimeEnv->pResultBuf, pResult->pageId);

  int16_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pCtx[i].pOutput = getPosInResultPage(pRuntimeEnv->pQueryAttr, page, pResult->offset, offset);
    offset += pCtx[i].resDataInfo.bytes;

    int32_t functionId = pCtx[i].functionId;
    if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF || functionId == FUNCTION_DERIVATIVE) {
      if(i > 0) pCtx[i].ptsOutputBuf = pCtx[i-1].pOutput;
    }

    /*
     * set the output buffer information and intermediate buffer,
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);
  }
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
//      //qDebug("QInfo:0x%"PRIx64" set tag value for join comparison, colId:%" PRId64 ", val:%s", GET_TASKID(pRuntimeEnv),
////             pExprInfo->base.param[0].i, pCtx[0].tag.pz);
//    } else {
//      //qDebug("QInfo:0x%"PRIx64" set tag value for join comparison, colId:%" PRId64 ", val:%" PRId64, GET_TASKID(pRuntimeEnv),
////             pExprInfo->base.param[0].i, pCtx[0].tag.i);
//    }
//  }
}

int32_t setTimestampListJoinInfo(STaskRuntimeEnv* pRuntimeEnv, SVariant* pTag, STableQueryInfo *pTableQueryInfo) {
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

// TODO refactor: this funciton should be merged with setparamForStableStddevColumnData function.
void setParamForStableStddev(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, int32_t numOfOutput, SExprInfo* pExprInfo) {
#if 0
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

  int32_t numOfExprs = pQueryAttr->numOfOutput;
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExprInfo1 = &(pExprInfo[i]);
    if (pExprInfo1->base.functionId != FUNCTION_STDDEV_DST) {
      continue;
    }

    SExprBasicInfo* pExpr = &pExprInfo1->base;

    pCtx[i].param[0].arr = NULL;
    pCtx[i].param[0].nType = TSDB_DATA_TYPE_INT;  // avoid freeing the memory by setting the type to be int

    // TODO use hash to speedup this loop
    int32_t numOfGroup = (int32_t)taosArrayGetSize(pRuntimeEnv->prevResult);
    for (int32_t j = 0; j < numOfGroup; ++j) {
      SInterResult* p = taosArrayGet(pRuntimeEnv->prevResult, j);
      if (pQueryAttr->tagLen == 0 || memcmp(p->tags, pRuntimeEnv->tagVal, pQueryAttr->tagLen) == 0) {
        int32_t numOfCols = (int32_t)taosArrayGetSize(p->pResult);
        for (int32_t k = 0; k < numOfCols; ++k) {
          SStddevInterResult* pres = taosArrayGet(p->pResult, k);
          if (pres->info.colId == pExpr->colInfo.colId) {
            pCtx[i].param[0].arr = pres->pResult;
            break;
          }
        }
      }
    }
  }
#endif
}

void setParamForStableStddevByColData(STaskRuntimeEnv* pRuntimeEnv, SqlFunctionCtx* pCtx, int32_t numOfOutput, SExprInfo* pExpr, char* val, int16_t bytes) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
#if 0
  int32_t numOfExprs = pQueryAttr->numOfOutput;
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SExprBasicInfo* pExpr1 = &pExpr[i].base;
    if (pExpr1->functionId != FUNCTION_STDDEV_DST) {
      continue;
    }

    pCtx[i].param[0].arr = NULL;
    pCtx[i].param[0].nType = TSDB_DATA_TYPE_INT;  // avoid freeing the memory by setting the type to be int

    // TODO use hash to speedup this loop
    int32_t numOfGroup = (int32_t)taosArrayGetSize(pRuntimeEnv->prevResult);
    for (int32_t j = 0; j < numOfGroup; ++j) {
      SInterResult* p = taosArrayGet(pRuntimeEnv->prevResult, j);
      if (bytes == 0 || memcmp(p->tags, val, bytes) == 0) {
        int32_t numOfCols = (int32_t)taosArrayGetSize(p->pResult);
        for (int32_t k = 0; k < numOfCols; ++k) {
          SStddevInterResult* pres = taosArrayGet(p->pResult, k);
          if (pres->info.colId == pExpr1->colInfo.colId) {
            pCtx[i].param[0].arr = pres->pResult;
            break;
          }
        }
      }
    }
  }
#endif
}

/*
 * There are two cases to handle:
 *
 * 1. Query range is not set yet (queryRangeSet = 0). we need to set the query range info, including pQueryAttr->lastKey,
 *    pQueryAttr->window.skey, and pQueryAttr->eKey.
 * 2. Query range is set and query is in progress. There may be another result with the same query ranges to be
 *    merged during merge stage. In this case, we need the pTableQueryInfo->lastResRows to decide if there
 *    is a previous result generated or not.
 */
void setIntervalQueryRange(STaskRuntimeEnv *pRuntimeEnv, TSKEY key) {
  STaskAttr           *pQueryAttr = pRuntimeEnv->pQueryAttr;
  STableQueryInfo  *pTableQueryInfo = pRuntimeEnv->current;
  SResultRowInfo   *pResultRowInfo = &pTableQueryInfo->resInfo;

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
static int32_t doCopyToSDataBlock(SDiskbasedBuf *pBuf, SGroupResInfo* pGroupResInfo, int32_t orderType, SSDataBlock* pBlock, int32_t rowCapacity, int32_t* rowCellOffset) {
  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
  int32_t numOfResult = pBlock->info.rows; // there are already exists result rows

  int32_t start = 0;
  int32_t step = -1;

  //qDebug("QInfo:0x%"PRIx64" start to copy data from windowResInfo to output buf", GET_TASKID(pRuntimeEnv));
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
    SFilePage *page = getBufPage(pBuf, pPos->pageId);

    SResultRow* pRow = (SResultRow*)((char*)page + pPos->offset);
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      continue;
    }

    // TODO copy multiple rows?
    int32_t numOfRowsToCopy = pRow->numOfRows;
    if (numOfResult + numOfRowsToCopy  >= rowCapacity) {
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < pBlock->info.numOfCols; ++j) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, j);
      SResultRowEntryInfo* pEntryInfo = getResultCell(pRow, j, rowCellOffset);

      char* in = GET_ROWCELL_INTERBUF(pEntryInfo);
      colDataAppend(pColInfoData, nrows, in, pEntryInfo->numOfRes == 0);
    }

    releaseBufPage(pBuf, page);
    nrows += 1;

    numOfResult += numOfRowsToCopy;
    if (numOfResult == rowCapacity) {  // output buffer is full
      break;
    }
  }

  //qDebug("QInfo:0x%"PRIx64" copy data to query buf completed", GET_TASKID(pRuntimeEnv));
  pBlock->info.rows = numOfResult;
  return 0;
}

static void toSDatablock(SGroupResInfo *pGroupResInfo, SDiskbasedBuf* pBuf, SSDataBlock* pBlock, int32_t rowCapacity, int32_t* rowCellOffset) {
  assert(pGroupResInfo->currentGroup <= pGroupResInfo->totalGroup);

  blockDataClearup(pBlock);
  if (!hasRemainDataInCurrentGroup(pGroupResInfo)) {
    return;
  }

  int32_t orderType = TSDB_ORDER_ASC;//(pQueryAttr->pGroupbyExpr != NULL) ? pQueryAttr->pGroupbyExpr->orderType : TSDB_ORDER_ASC;
  doCopyToSDataBlock(pBuf, pGroupResInfo, orderType, pBlock, rowCapacity, rowCellOffset);

  // add condition (pBlock->info.rows >= 1) just to runtime happy
  blockDataUpdateTsWindow(pBlock);
}

static void updateNumOfRowsInResultRows(SqlFunctionCtx* pCtx, int32_t numOfOutput,
                                        SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset) {
  // update the number of result for each, only update the number of rows for the corresponding window result.
//  if (QUERY_IS_INTERVAL_QUERY(pQueryAttr)) {
//    return;
//  }

  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRow *pResult = pResultRowInfo->pResult[i];

    for (int32_t j = 0; j < numOfOutput; ++j) {
      int32_t functionId = pCtx[j].functionId;
      if (functionId == FUNCTION_TS || functionId == FUNCTION_TAG || functionId == FUNCTION_TAGPRJ) {
        continue;
      }

      SResultRowEntryInfo* pCell = getResultCell(pResult, j, rowCellInfoOffset);
      pResult->numOfRows = (uint16_t)(TMAX(pResult->numOfRows, pCell->numOfRes));
    }
  }
}

static int32_t compressQueryColData(SColumnInfoData *pColRes, int32_t numOfRows, char *data, int8_t compressed) {
  int32_t colSize = pColRes->info.bytes * numOfRows;
  return (*(tDataTypes[pColRes->info.type].compFunc))(pColRes->pData, colSize, numOfRows, data,
                                                                 colSize + COMP_OVERFLOW_BYTES, compressed, NULL, 0);
}

static void doCopyQueryResultToMsg(SQInfo *pQInfo, int32_t numOfRows, char *data, int8_t compressed, int32_t *compLen) {
  STaskRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;

  SSDataBlock* pRes = pRuntimeEnv->outputBuf;

  int32_t *compSizes = NULL;
  int32_t numOfCols = pQueryAttr->pExpr2 ? pQueryAttr->numOfExpr2 : pQueryAttr->numOfOutput;

  if (compressed) {
    compSizes = calloc(numOfCols, sizeof(int32_t));
  }

  if (pQueryAttr->pExpr2 == NULL) {
    for (int32_t col = 0; col < numOfCols; ++col) {
      SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, col);
      if (compressed) {
        compSizes[col] = compressQueryColData(pColRes, pRes->info.rows, data, compressed);
        data += compSizes[col];
        *compLen += compSizes[col];
        compSizes[col] = htonl(compSizes[col]);
      } else {
        memmove(data, pColRes->pData, pColRes->info.bytes * pRes->info.rows);
        data += pColRes->info.bytes * pRes->info.rows;
      }
    }
  } else {
    for (int32_t col = 0; col < numOfCols; ++col) {
      SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, col);
      if (compressed) {
        compSizes[col] = htonl(compressQueryColData(pColRes, numOfRows, data, compressed));
        data += compSizes[col];
        *compLen += compSizes[col];
        compSizes[col] = htonl(compSizes[col]);
      } else {
        memmove(data, pColRes->pData, pColRes->info.bytes * numOfRows);
        data += pColRes->info.bytes * numOfRows;
      }
    }
  }

  if (compressed) {
    memmove(data, (char *)compSizes, numOfCols * sizeof(int32_t));
    data += numOfCols * sizeof(int32_t);

    tfree(compSizes);
  }

  int32_t numOfTables = (int32_t) taosHashGetSize(pRuntimeEnv->pTableRetrieveTsMap);
  *(int32_t*)data = htonl(numOfTables);
  data += sizeof(int32_t);

  int32_t total = 0;
  STableIdInfo* item = taosHashIterate(pRuntimeEnv->pTableRetrieveTsMap, NULL);

  while(item) {
    STableIdInfo* pDst = (STableIdInfo*)data;
    pDst->uid = htobe64(item->uid);
    pDst->key = htobe64(item->key);

    data += sizeof(STableIdInfo);
    total++;

    //qDebug("QInfo:0x%"PRIx64" set subscribe info, tid:%d, uid:%"PRIu64", skey:%"PRId64, pQInfo->qId, item->tid, item->uid, item->key);
    item = taosHashIterate(pRuntimeEnv->pTableRetrieveTsMap, item);
  }

  //qDebug("QInfo:0x%"PRIx64" set %d subscribe info", pQInfo->qId, total);

  // Check if query is completed or not for stable query or normal table query respectively.
  if (Q_STATUS_EQUAL(pRuntimeEnv->status, TASK_COMPLETED) && pRuntimeEnv->proot->status == OP_EXEC_DONE) {
//    setTaskStatus(pOperator->pTaskInfo, QUERY_OVER);
  }
}

int32_t doFillTimeIntervalGapsInResults(struct SFillInfo* pFillInfo, SSDataBlock *pOutput, int32_t capacity, void** p) {
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

  event.eventType    = eventType;
  event.eventTime    = taosGetTimestampUs();
  event.operatorType = operatorInfo->operatorType;

  if (operatorInfo->pRuntimeEnv) {
    SQInfo* pQInfo = operatorInfo->pRuntimeEnv->qinfo;
    if (pQInfo->summary.queryProfEvents) {
      taosArrayPush(pQInfo->summary.queryProfEvents, &event);
    }
  }
}

void publishQueryAbortEvent(SExecTaskInfo * pTaskInfo, int32_t code) {
  SQueryProfEvent event;
  event.eventType = QUERY_PROF_QUERY_ABORT;
  event.eventTime = taosGetTimestampUs();
  event.abortCode = code;

  if (pTaskInfo->cost.queryProfEvents) {
    taosArrayPush(pTaskInfo->cost.queryProfEvents, &event);
  }
}

typedef struct  {
  uint8_t operatorType;
  int64_t beginTime;
  int64_t endTime;
  int64_t selfTime;
  int64_t descendantsTime;
} SOperatorStackItem;

static void doOperatorExecProfOnce(SOperatorStackItem* item, SQueryProfEvent* event, SArray* opStack, SHashObj* profResults) {
  item->endTime = event->eventTime;
  item->selfTime = (item->endTime - item->beginTime) - (item->descendantsTime);

  for (int32_t j = 0; j < taosArrayGetSize(opStack); ++j) {
    SOperatorStackItem* ancestor = taosArrayGet(opStack, j);
    ancestor->descendantsTime += item->selfTime;
  }

  uint8_t operatorType = item->operatorType;
  SOperatorProfResult* result = taosHashGet(profResults, &operatorType, sizeof(operatorType));
  if (result != NULL) {
    result->sumRunTimes++;
    result->sumSelfTime += item->selfTime;
  } else {
    SOperatorProfResult opResult;
    opResult.operatorType = operatorType;
    opResult.sumSelfTime = item->selfTime;
    opResult.sumRunTimes = 1;
    taosHashPut(profResults, &(operatorType), sizeof(operatorType),
                &opResult, sizeof(opResult));
  }
}

void calculateOperatorProfResults(SQInfo* pQInfo) {
  if (pQInfo->summary.queryProfEvents == NULL) {
    //qDebug("QInfo:0x%"PRIx64" query prof events array is null", pQInfo->qId);
    return;
  }

  if (pQInfo->summary.operatorProfResults == NULL) {
    //qDebug("QInfo:0x%"PRIx64" operator prof results hash is null", pQInfo->qId);
    return;
  }

  SArray* opStack = taosArrayInit(32, sizeof(SOperatorStackItem));
  if (opStack == NULL) {
    return;
  }

  size_t size = taosArrayGetSize(pQInfo->summary.queryProfEvents);
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

void queryCostStatis(SExecTaskInfo *pTaskInfo) {
  STaskCostInfo *pSummary = &pTaskInfo->cost;

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

  qDebug("%s :cost summary: elapsed time:%"PRId64" us, first merge:%"PRId64" us, total blocks:%d, "
         "load block statis:%d, load data block:%d, total rows:%"PRId64 ", check rows:%"PRId64,
         GET_TASKID(pTaskInfo), pSummary->elapsedTime, pSummary->firstStageMergeTime, pSummary->totalBlocks, pSummary->loadBlockStatis,
         pSummary->loadBlocks, pSummary->totalRows, pSummary->totalCheckedRows);
//
  //qDebug("QInfo:0x%"PRIx64" :cost summary: winResPool size:%.2f Kb, numOfWin:%"PRId64", tableInfoSize:%.2f Kb, hashTable:%.2f Kb", pQInfo->qId, pSummary->winInfoSize/1024.0,
//      pSummary->numOfTimeWindows, pSummary->tableInfoSize/1024.0, pSummary->hashSize/1024.0);

  if (pSummary->operatorProfResults) {
    SOperatorProfResult* opRes = taosHashIterate(pSummary->operatorProfResults, NULL);
    while (opRes != NULL) {
      //qDebug("QInfo:0x%" PRIx64 " :cost summary: operator : %d, exec times: %" PRId64 ", self time: %" PRId64,
//             pQInfo->qId, opRes->operatorType, opRes->sumRunTimes, opRes->sumSelfTime);
      opRes = taosHashIterate(pSummary->operatorProfResults, opRes);
    }
  }
}

//static void updateOffsetVal(STaskRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pBlockInfo) {
//  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//  STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
//
//  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
//
//  if (pQueryAttr->limit.offset == pBlockInfo->rows) {  // current block will ignore completed
//    pTableQueryInfo->lastKey = QUERY_IS_ASC_QUERY(pQueryAttr) ? pBlockInfo->window.ekey + step : pBlockInfo->window.skey + step;
//    pQueryAttr->limit.offset = 0;
//    return;
//  }
//
//  if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//    pQueryAttr->pos = (int32_t)pQueryAttr->limit.offset;
//  } else {
//    pQueryAttr->pos = pBlockInfo->rows - (int32_t)pQueryAttr->limit.offset - 1;
//  }
//
//  assert(pQueryAttr->pos >= 0 && pQueryAttr->pos <= pBlockInfo->rows - 1);
//
//  SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pTsdbReadHandle, NULL);
//  SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//  // update the pQueryAttr->limit.offset value, and pQueryAttr->pos value
//  TSKEY *keys = (TSKEY *) pColInfoData->pData;
//
//  // update the offset value
//  pTableQueryInfo->lastKey = keys[pQueryAttr->pos];
//  pQueryAttr->limit.offset = 0;
//
//  int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
//
//  //qDebug("QInfo:0x%"PRIx64" check data block, brange:%" PRId64 "-%" PRId64 ", numBlocksOfStep:%d, numOfRes:%d, lastKey:%"PRId64, GET_TASKID(pRuntimeEnv),
//         pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes, pQuery->current->lastKey);
//}

//void skipBlocks(STaskRuntimeEnv *pRuntimeEnv) {
//  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//
//  if (pQueryAttr->limit.offset <= 0 || pQueryAttr->numOfFilterCols > 0) {
//    return;
//  }
//
//  pQueryAttr->pos = 0;
//  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQueryAttr->order.order);
//
//  STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
//  TsdbQueryHandleT pTsdbReadHandle = pRuntimeEnv->pTsdbReadHandle;
//
//  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
//  while (tsdbNextDataBlock(pTsdbReadHandle)) {
//    if (isTaskKilled(pRuntimeEnv->qinfo)) {
//      longjmp(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
//    }
//
//    tsdbRetrieveDataBlockInfo(pTsdbReadHandle, &blockInfo);
//
//    if (pQueryAttr->limit.offset > blockInfo.rows) {
//      pQueryAttr->limit.offset -= blockInfo.rows;
//      pTableQueryInfo->lastKey = (QUERY_IS_ASC_QUERY(pQueryAttr)) ? blockInfo.window.ekey : blockInfo.window.skey;
//      pTableQueryInfo->lastKey += step;
//
//      //qDebug("QInfo:0x%"PRIx64" skip rows:%d, offset:%" PRId64, GET_TASKID(pRuntimeEnv), blockInfo.rows,
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

//static TSKEY doSkipIntervalProcess(STaskRuntimeEnv* pRuntimeEnv, STimeWindow* win, SDataBlockInfo* pBlockInfo, STableQueryInfo* pTableQueryInfo) {
//  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
//
//  assert(pQueryAttr->limit.offset == 0);
//  STimeWindow tw = *win;
//  getNextTimeWindow(pQueryAttr, &tw);
//
//  if ((tw.skey <= pBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQueryAttr)) ||
//      (tw.ekey >= pBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQueryAttr))) {
//
//    // load the data block and check data remaining in current data block
//    // TODO optimize performance
//    SArray *         pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pTsdbReadHandle, NULL);
//    SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//    tw = *win;
//    int32_t startPos =
//        getNextQualifiedWindow(pQueryAttr, &tw, pBlockInfo, pColInfoData->pData, binarySearchForKey, -1);
//    assert(startPos >= 0);
//
//    // set the abort info
//    pQueryAttr->pos = startPos;
//
//    // reset the query start timestamp
//    pTableQueryInfo->win.skey = ((TSKEY *)pColInfoData->pData)[startPos];
//    pQueryAttr->window.skey = pTableQueryInfo->win.skey;
//    TSKEY key = pTableQueryInfo->win.skey;
//
//    pWindowResInfo->prevSKey = tw.skey;
//    int32_t index = pRuntimeEnv->resultRowInfo.curIndex;
//
//    int32_t numOfRes = tableApplyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, NULL, binarySearchForKey, pDataBlock);
//    pRuntimeEnv->resultRowInfo.curIndex = index;  // restore the window index
//
//    //qDebug("QInfo:0x%"PRIx64" check data block, brange:%" PRId64 "-%" PRId64 ", numOfRows:%d, numOfRes:%d, lastKey:%" PRId64,
//           GET_TASKID(pRuntimeEnv), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, numOfRes,
//           pQueryAttr->current->lastKey);
//
//    return key;
//  } else {  // do nothing
//    pQueryAttr->window.skey      = tw.skey;
//    pWindowResInfo->prevSKey = tw.skey;
//    pTableQueryInfo->lastKey = tw.skey;
//
//    return tw.skey;
//  }
//
//  return true;
//}

//static bool skipTimeInterval(STaskRuntimeEnv *pRuntimeEnv, TSKEY* start) {
//  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
//  if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//    assert(*start <= pRuntimeEnv->current->lastKey);
//  } else {
//    assert(*start >= pRuntimeEnv->current->lastKey);
//  }
//
//  // if queried with value filter, do NOT forward query start position
//  if (pQueryAttr->limit.offset <= 0 || pQueryAttr->numOfFilterCols > 0 || pRuntimeEnv->pTsBuf != NULL || pRuntimeEnv->pFillInfo != NULL) {
//    return true;
//  }
//
//  /*
//   * 1. for interval without interpolation query we forward pQueryAttr->interval.interval at a time for
//   *    pQueryAttr->limit.offset times. Since hole exists, pQueryAttr->interval.interval*pQueryAttr->limit.offset value is
//   *    not valid. otherwise, we only forward pQueryAttr->limit.offset number of points
//   */
//  assert(pRuntimeEnv->resultRowInfo.prevSKey == TSKEY_INITIAL_VAL);
//
//  STimeWindow w = TSWINDOW_INITIALIZER;
//  bool ascQuery = QUERY_IS_ASC_QUERY(pQueryAttr);
//
//  SResultRowInfo *pWindowResInfo = &pRuntimeEnv->resultRowInfo;
//  STableQueryInfo *pTableQueryInfo = pRuntimeEnv->current;
//
//  SDataBlockInfo blockInfo = SDATA_BLOCK_INITIALIZER;
//  while (tsdbNextDataBlock(pRuntimeEnv->pTsdbReadHandle)) {
//    tsdbRetrieveDataBlockInfo(pRuntimeEnv->pTsdbReadHandle, &blockInfo);
//
//    if (QUERY_IS_ASC_QUERY(pQueryAttr)) {
//      if (pWindowResInfo->prevSKey == TSKEY_INITIAL_VAL) {
//        getAlignQueryTimeWindow(pQueryAttr, blockInfo.window.skey, blockInfo.window.skey, pQueryAttr->window.ekey, &w);
//        pWindowResInfo->prevSKey = w.skey;
//      }
//    } else {
//      getAlignQueryTimeWindow(pQueryAttr, blockInfo.window.ekey, pQueryAttr->window.ekey, blockInfo.window.ekey, &w);
//      pWindowResInfo->prevSKey = w.skey;
//    }
//
//    // the first time window
//    STimeWindow win = getActiveTimeWindow(pWindowResInfo, pWindowResInfo->prevSKey, pQueryAttr);
//
//    while (pQueryAttr->limit.offset > 0) {
//      STimeWindow tw = win;
//
//      if ((win.ekey <= blockInfo.window.ekey && ascQuery) || (win.ekey >= blockInfo.window.skey && !ascQuery)) {
//        pQueryAttr->limit.offset -= 1;
//        pWindowResInfo->prevSKey = win.skey;
//
//        // current time window is aligned with blockInfo.window.ekey
//        // restart it from next data block by set prevSKey to be TSKEY_INITIAL_VAL;
//        if ((win.ekey == blockInfo.window.ekey && ascQuery) || (win.ekey == blockInfo.window.skey && !ascQuery)) {
//          pWindowResInfo->prevSKey = TSKEY_INITIAL_VAL;
//        }
//      }
//
//      if (pQueryAttr->limit.offset == 0) {
//        *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
//        return true;
//      }
//
//      // current window does not ended in current data block, try next data block
//      getNextTimeWindow(pQueryAttr, &tw);
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
//        SArray *pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pTsdbReadHandle, NULL);
//        SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock, 0);
//
//        if ((win.ekey > blockInfo.window.ekey && ascQuery) || (win.ekey < blockInfo.window.skey && !ascQuery)) {
//          pQueryAttr->limit.offset -= 1;
//        }
//
//        if (pQueryAttr->limit.offset == 0) {
//          *start = doSkipIntervalProcess(pRuntimeEnv, &win, &blockInfo, pTableQueryInfo);
//          return true;
//        } else {
//          tw = win;
//          int32_t startPos =
//              getNextQualifiedWindow(pQueryAttr, &tw, &blockInfo, pColInfoData->pData, binarySearchForKey, -1);
//          assert(startPos >= 0);
//
//          // set the abort info
//          pQueryAttr->pos = startPos;
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

int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num) {
  if (p->pDownstream == NULL) {
    assert(p->numOfDownstream == 0);
  }

  p->pDownstream = calloc(1, num * POINTER_BYTES);
  if (p->pDownstream == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(p->pDownstream, pDownstream, num * POINTER_BYTES);
  p->numOfDownstream = num;
  return TSDB_CODE_SUCCESS;
}

static void doDestroyTableQueryInfo(STableGroupInfo* pTableqinfoGroupInfo);

static int32_t setupQueryHandle(void* tsdb, STaskRuntimeEnv* pRuntimeEnv, int64_t qId, bool isSTableQuery) {
  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;
#if 0
  // TODO set the tags scan handle
  if (onlyQueryTags(pQueryAttr)) {
    return TSDB_CODE_SUCCESS;
  }

  STsdbQueryCond cond = createTsdbQueryCond(pQueryAttr, &pQueryAttr->window);
  if (pQueryAttr->tsCompQuery || pQueryAttr->pointInterpQuery) {
    cond.type = BLOCK_LOAD_TABLE_SEQ_ORDER;
  }

  if (!isSTableQuery
    && (pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 1)
    && (cond.order == TSDB_ORDER_ASC)
    && (!QUERY_IS_INTERVAL_QUERY(pQueryAttr))
    && (!pQueryAttr->groupbyColumn)
    && (!pQueryAttr->simpleAgg)
  ) {
    SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);
    STableQueryInfo* pCheckInfo = taosArrayGetP(pa, 0);
    cond.twindow = pCheckInfo->win;
  }

  terrno = TSDB_CODE_SUCCESS;
  if (isFirstLastRowQuery(pQueryAttr)) {
    pRuntimeEnv->pTsdbReadHandle = tsdbQueryLastRow(tsdb, &cond, &pQueryAttr->tableGroupInfo, qId, &pQueryAttr->memRef);

    // update the query time window
    pQueryAttr->window = cond.twindow;
    if (pQueryAttr->tableGroupInfo.numOfTables == 0) {
      pRuntimeEnv->tableqinfoGroupInfo.numOfTables = 0;
    } else {
      size_t numOfGroups = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
      for(int32_t i = 0; i < numOfGroups; ++i) {
        SArray *group = GET_TABLEGROUP(pRuntimeEnv, i);

        size_t t = taosArrayGetSize(group);
        for (int32_t j = 0; j < t; ++j) {
          STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);

          pCheckInfo->win = pQueryAttr->window;
          pCheckInfo->lastKey = pCheckInfo->win.skey;
        }
      }
    }
  } else if (isCachedLastQuery(pQueryAttr)) {
    pRuntimeEnv->pTsdbReadHandle = tsdbQueryCacheLast(tsdb, &cond, &pQueryAttr->tableGroupInfo, qId, &pQueryAttr->memRef);
  } else if (pQueryAttr->pointInterpQuery) {
    pRuntimeEnv->pTsdbReadHandle = tsdbQueryRowsInExternalWindow(tsdb, &cond, &pQueryAttr->tableGroupInfo, qId, &pQueryAttr->memRef);
  } else {
    pRuntimeEnv->pTsdbReadHandle = tsdbQueryTables(tsdb, &cond, &pQueryAttr->tableGroupInfo, qId, &pQueryAttr->memRef);
  }
#endif
  return terrno;
}

int32_t doInitQInfo(SQInfo* pQInfo, STSBuf* pTsBuf, void* tsdb, void* sourceOptr, int32_t tbScanner, SArray* pOperator,
    void* param) {
  STaskRuntimeEnv *pRuntimeEnv = &pQInfo->runtimeEnv;

  STaskAttr *pQueryAttr = pQInfo->runtimeEnv.pQueryAttr;
  pQueryAttr->tsdb = tsdb;

  if (tsdb != NULL) {
    int32_t code = setupQueryHandle(tsdb, pRuntimeEnv, pQInfo->qId, pQueryAttr->stableQuery);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  pQueryAttr->interBufSize = getOutputInterResultBufSize(pQueryAttr);

  pRuntimeEnv->groupResInfo.totalGroup = (int32_t) (pQueryAttr->stableQuery? GET_NUM_OF_TABLEGROUP(pRuntimeEnv):0);
  pRuntimeEnv->enableGroupData = false;

  pRuntimeEnv->pQueryAttr = pQueryAttr;
  pRuntimeEnv->pTsBuf = pTsBuf;
  pRuntimeEnv->cur.vgroupIndex = -1;
  setResultBufSize(pQueryAttr, &pRuntimeEnv->resultInfo);

  if (sourceOptr != NULL) {
    assert(pRuntimeEnv->proot == NULL);
    pRuntimeEnv->proot = sourceOptr;
  }

  if (pTsBuf != NULL) {
    int16_t order = (pQueryAttr->order.order == pRuntimeEnv->pTsBuf->tsOrder) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTsBuf, order);
  }

  int32_t ps = 4096;
  getIntermediateBufInfo(pRuntimeEnv, &ps, &pQueryAttr->intermediateResultRowSize);

  int32_t TENMB = 1024*1024*10;
  int32_t code = createDiskbasedBuf(&pRuntimeEnv->pResultBuf, ps, TENMB, "", "/tmp");
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // create runtime environment
  int32_t numOfTables = (int32_t)pQueryAttr->tableGroupInfo.numOfTables;
  pQInfo->summary.tableInfoSize += (numOfTables * sizeof(STableQueryInfo));
  pQInfo->summary.queryProfEvents = taosArrayInit(512, sizeof(SQueryProfEvent));
  if (pQInfo->summary.queryProfEvents == NULL) {
    //qDebug("QInfo:0x%"PRIx64" failed to allocate query prof events array", pQInfo->qId);
  }

  pQInfo->summary.operatorProfResults =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_TINYINT), true, HASH_NO_LOCK);

  if (pQInfo->summary.operatorProfResults == NULL) {
    //qDebug("QInfo:0x%"PRIx64" failed to allocate operator prof results hash", pQInfo->qId);
  }

  code = setupQueryRuntimeEnv(pRuntimeEnv, (int32_t) pQueryAttr->tableGroupInfo.numOfTables, pOperator, param);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

//  setTaskStatus(pOperator->pTaskInfo, QUERY_NOT_COMPLETED);
  return TSDB_CODE_SUCCESS;
}

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

//STsdbQueryCond createTsdbQueryCond(STaskAttr* pQueryAttr, STimeWindow* win) {
//  STsdbQueryCond cond = {
//      .colList   = pQueryAttr->tableCols,
//      .order     = pQueryAttr->order.order,
//      .numOfCols = pQueryAttr->numOfCols,
//      .type      = BLOCK_LOAD_OFFSET_SEQ_ORDER,
//      .loadExternalRows = false,
//  };
//
//  TIME_WINDOW_COPY(cond.twindow, *win);
//  return cond;
//}

static STableIdInfo createTableIdInfo(STableQueryInfo* pTableQueryInfo) {
  STableIdInfo tidInfo;
//  STableId* id = TSDB_TABLEID(pTableQueryInfo->pTable);
//
//  tidInfo.uid = id->uid;
//  tidInfo.tid = id->tid;
//  tidInfo.key = pTableQueryInfo->lastKey;

  return tidInfo;
}

//static void updateTableIdInfo(STableQueryInfo* pTableQueryInfo, SSDataBlock* pBlock, SHashObj* pTableIdInfo, int32_t order) {
//  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);
//  pTableQueryInfo->lastKey = ((order == TSDB_ORDER_ASC)? pBlock->info.window.ekey:pBlock->info.window.skey) + step;
//
//  if (pTableQueryInfo->pTable == NULL) {
//    return;
//  }
//
//  STableIdInfo tidInfo = createTableIdInfo(pTableQueryInfo);
//  STableIdInfo *idinfo = taosHashGet(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid));
//  if (idinfo != NULL) {
//    assert(idinfo->tid == tidInfo.tid && idinfo->uid == tidInfo.uid);
//    idinfo->key = tidInfo.key;
//  } else {
//    taosHashPut(pTableIdInfo, &tidInfo.tid, sizeof(tidInfo.tid), &tidInfo, sizeof(STableIdInfo));
//  }
//}

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

static SSDataBlock* doTableScanImpl(SOperatorInfo *pOperator, bool* newgroup) {
  STableScanInfo   *pTableScanInfo = pOperator->info;
  SExecTaskInfo    *pTaskInfo = pOperator->pTaskInfo;

  SSDataBlock      *pBlock = &pTableScanInfo->block;
  STableGroupInfo  *pTableGroupInfo = &pOperator->pTaskInfo->tableqinfoGroupInfo;

  *newgroup = false;

  while (tsdbNextDataBlock(pTableScanInfo->pTsdbReadHandle)) {
    if (isTaskKilled(pOperator->pTaskInfo)) {
      longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    pTableScanInfo->numOfBlocks += 1;
    tsdbRetrieveDataBlockInfo(pTableScanInfo->pTsdbReadHandle, &pBlock->info);

    // todo opt
//    if (pTableGroupInfo->numOfTables > 1 || (pRuntimeEnv->current == NULL && pTableGroupInfo->numOfTables == 1)) {
//      STableQueryInfo** pTableQueryInfo =
//          (STableQueryInfo**)taosHashGet(pTableGroupInfo->map, &pBlock->info.uid, sizeof(pBlock->info.uid));
//      if (pTableQueryInfo == NULL) {
//        break;
//      }
//
//      pRuntimeEnv->current = *pTableQueryInfo;
//      doTableQueryInfoTimeWindowCheck(pTaskInfo, *pTableQueryInfo, pTableScanInfo->order);
//    }

    // this function never returns error?
    uint32_t status = BLK_DATA_ALL_NEEDED;
    int32_t code = loadDataBlock(pTaskInfo, pTableScanInfo, pBlock, &status);
//    int32_t  code = loadDataBlockOnDemand(pOperator->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pOperator->pTaskInfo->env, code);
    }

    // current block is ignored according to filter result by block statistics data, continue load the next block
    if (status == BLK_DATA_DISCARD || pBlock->info.rows == 0) {
      continue;
    }

    return pBlock;
  }

  return NULL;
}

static SSDataBlock* doTableScan(SOperatorInfo *pOperator, bool *newgroup) {
  STableScanInfo *pTableScanInfo = pOperator->info;
  SExecTaskInfo  *pTaskInfo = pOperator->pTaskInfo;

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  // The read handle is not initialized yet, since no qualified tables exists
  if (pTableScanInfo->pTsdbReadHandle == NULL) {
    return NULL;
  }

  SResultRowInfo* pResultRowInfo = pTableScanInfo->pResultRowInfo;
  *newgroup = false;

  while (pTableScanInfo->current < pTableScanInfo->times) {
    SSDataBlock* p = doTableScanImpl(pOperator, newgroup);
    if (p != NULL) {
      return p;
    }

    if (++pTableScanInfo->current >= pTableScanInfo->times) {
      if (pTableScanInfo->reverseTimes <= 0/* || isTsdbCacheLastRow(pTableScanInfo->pTsdbReadHandle)*/) {
        return NULL;
      } else {
        break;
      }
    }

    // do prepare for the next round table scan operation
//    STsdbQueryCond cond = createTsdbQueryCond(pQueryAttr, &pQueryAttr->window);
//    tsdbResetQueryHandle(pTableScanInfo->pTsdbReadHandle, &cond);

    setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
    pTableScanInfo->scanFlag = REPEAT_SCAN;

//    if (pTaskInfo->pTsBuf) {
//      bool ret = tsBufNextPos(pRuntimeEnv->pTsBuf);
//      assert(ret);
//    }
//
    if (pResultRowInfo->size > 0) {
      pResultRowInfo->curPos = 0;
    }

    qDebug("%s start to repeat scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           GET_TASKID(pTaskInfo), pTaskInfo->window.skey, pTaskInfo->window.ekey);
  }

  SSDataBlock *p = NULL;
  // todo refactor
  if (pTableScanInfo->reverseTimes > 0) {
    setupEnvForReverseScan(pTableScanInfo, pTableScanInfo->pCtx, pTableScanInfo->numOfOutput);
//    STsdbQueryCond cond = createTsdbQueryCond(pQueryAttr, &pQueryAttr->window);
//    tsdbResetQueryHandle(pTableScanInfo->pTsdbReadHandle, &cond);

    qDebug("%s start to reverse scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           GET_TASKID(pTaskInfo), pTaskInfo->window.skey, pTaskInfo->window.ekey);

    if (pResultRowInfo->size > 0) {
      pResultRowInfo->curPos = pResultRowInfo->size - 1;
    }

    p = doTableScanImpl(pOperator, newgroup);
  }

  return p;
}

static SSDataBlock* doBlockInfoScan(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableScanInfo *pTableScanInfo = pOperator->info;
  *newgroup = false;
#if 0
  STableBlockDist tableBlockDist = {0};
  tableBlockDist.numOfTables     = (int32_t)pOperator->pRuntimeEnv->tableqinfoGroupInfo.numOfTables;

  int32_t numRowSteps = TSDB_DEFAULT_MAX_ROW_FBLOCK / TSDB_BLOCK_DIST_STEP_ROWS;
  if (TSDB_DEFAULT_MAX_ROW_FBLOCK % TSDB_BLOCK_DIST_STEP_ROWS != 0) {
    ++numRowSteps;
  }
  tableBlockDist.dataBlockInfos  = taosArrayInit(numRowSteps, sizeof(SFileBlockInfo));
  taosArraySetSize(tableBlockDist.dataBlockInfos, numRowSteps);
  tableBlockDist.maxRows = INT_MIN;
  tableBlockDist.minRows = INT_MAX;

  tsdbGetFileBlocksDistInfo(pTableScanInfo->pTsdbReadHandle, &tableBlockDist);
  tableBlockDist.numOfRowsInMemTable = (int32_t) tsdbGetNumOfRowsInMemTable(pTableScanInfo->pTsdbReadHandle);

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
  pOperator->pRuntimeEnv->current = taosArrayGetP(g, 0);

  pOperator->status = OP_EXEC_DONE;
  return pBlock;
#endif
}

static SSDataBlock* doStreamBlockScan(SOperatorInfo *pOperator, bool* newgroup) {
  // NOTE: this operator does never check if current status is done or not
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStreamBlockScanInfo* pInfo = pOperator->info;

  if (pInfo->blockType == STREAM_DATA_TYPE_SSDAT_BLOCK) {
    if (pInfo->blockValid) {
      pInfo->blockValid = false;  // this block can only be used once.
      return pInfo->pRes;
    } else {
      return NULL;
    }
  }

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
  blockDataClearup(pInfo->pRes);

  while (tqNextDataBlock(pInfo->readerHandle)) {
    pTaskInfo->code = tqRetrieveDataBlockInfo(pInfo->readerHandle, pBlockInfo);
    if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
      terrno = pTaskInfo->code;
      return NULL;
    }

    if (pBlockInfo->rows == 0) {
      return NULL;
    }

    pInfo->pRes->pDataBlock = tqRetrieveDataBlock(pInfo->readerHandle);
    if (pInfo->pRes->pDataBlock == NULL) {
      // TODO add log
      pTaskInfo->code = terrno;
      return NULL;
    }

    break;
  }

  // record the scan action.
  pInfo->numOfExec++;
  pInfo->numOfRows += pBlockInfo->rows;

  return (pBlockInfo->rows == 0)? NULL:pInfo->pRes;
}

int32_t loadRemoteDataCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  SSourceDataInfo* pSourceDataInfo = (SSourceDataInfo*) param;
  pSourceDataInfo->pRsp = pMsg->pData;

  SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
  pRsp->numOfRows = htonl(pRsp->numOfRows);
  pRsp->useconds  = htobe64(pRsp->useconds);
  pRsp->compLen   = htonl(pRsp->compLen);

  pSourceDataInfo->status = EX_SOURCE_DATA_READY;
  tsem_post(&pSourceDataInfo->pEx->ready);
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->msgInfo.pData);
  tfree(pMsgBody);
}

void qProcessFetchRsp(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo *pSendInfo = (SMsgSendInfo *) pMsg->ahandle;
  assert(pMsg->ahandle != NULL);

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL};

  if (pMsg->contLen > 0) {
    buf.pData = calloc(1, pMsg->contLen);
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

static int32_t doSendFetchDataRequest(SExchangeInfo *pExchangeInfo, SExecTaskInfo *pTaskInfo, int32_t sourceIndex) {
  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SResFetchReq* pMsg = calloc(1, sizeof(SResFetchReq));
  if (NULL == pMsg) {
    pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return pTaskInfo->code;
  }

  SDownstreamSourceNode *pSource = taosArrayGet(pExchangeInfo->pSources, sourceIndex);
  SSourceDataInfo   *pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);

  qDebug("%s build fetch msg and send to vgId:%d, ep:%s, taskId:0x%" PRIx64 ", %d/%" PRIzu,
         GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->addr.epSet.eps[0].fqdn, pSource->taskId, sourceIndex, totalSources);

  pMsg->header.vgId = htonl(pSource->addr.nodeId);
  pMsg->sId = htobe64(pSource->schedId);
  pMsg->taskId = htobe64(pSource->taskId);
  pMsg->queryId = htobe64(pTaskInfo->id.queryId);

  // send the fetch remote task result reques
  SMsgSendInfo* pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    tfree(pMsg);
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

static int32_t setSDataBlockFromFetchRsp(SSDataBlock* pRes, SExchangeInfo *pExchangeInfo, SSourceDataInfo* pDataInfo, int32_t numOfOutput, int64_t startTs) {
  char* pData = pDataInfo->pRsp->data;
  SRetrieveTableRsp* pRsp = pDataInfo->pRsp;

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, i);

    char* tmp = realloc(pColInfoData->pData, pColInfoData->info.bytes * pRsp->numOfRows);
    if (tmp == NULL) {
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }

    size_t len = pRsp->numOfRows * pColInfoData->info.bytes;
    memcpy(tmp, pData, len);

    pColInfoData->pData = tmp;
    pData += len;
  }

  pRes->info.rows = pRsp->numOfRows;

  int64_t el = taosGetTimestampUs() - startTs;

  pExchangeInfo->totalRows += pRsp->numOfRows;
  pExchangeInfo->totalSize += pRsp->compLen;
  pDataInfo->totalRows += pRsp->numOfRows;

  pExchangeInfo->totalElapsed += el;

  return TSDB_CODE_SUCCESS;
}

static void* setAllSourcesCompleted(SOperatorInfo *pOperator, int64_t startTs) {
  SExchangeInfo *pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int64_t el = taosGetTimestampUs() - startTs;
  pExchangeInfo->totalElapsed += el;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  qDebug("%s all %"PRIzu" sources are exhausted, total rows: %"PRIu64" bytes:%"PRIu64", elapsed:%.2f ms", GET_TASKID(pTaskInfo), totalSources,
         pExchangeInfo->totalRows, pExchangeInfo->totalSize, pExchangeInfo->totalElapsed/1000.0);

  doSetOperatorCompleted(pOperator);
  return NULL;
}

static SSDataBlock* concurrentlyLoadRemoteDataImpl(SOperatorInfo *pOperator, SExchangeInfo *pExchangeInfo, SExecTaskInfo *pTaskInfo) {
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

      SRetrieveTableRsp* pRsp = pDataInfo->pRsp;
      SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, i);

      SSDataBlock* pRes = pExchangeInfo->pResult;

      if (pRsp->numOfRows == 0) {
        qDebug("%s vgId:%d, taskID:0x%" PRIx64 " index:%d completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 " try next",
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, i + 1, pDataInfo->totalRows,
               pExchangeInfo->totalRows);
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
        completed += 1;
        continue;
      }

      code = setSDataBlockFromFetchRsp(pExchangeInfo->pResult, pExchangeInfo, pDataInfo, pOperator->numOfOutput, startTs);
      if (code != 0) {
        goto _error;
      }

      if (pRsp->completed == 1) {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, rowsOfSource:%" PRIu64
               ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows,
               pDataInfo->totalRows, pExchangeInfo->totalRows, pExchangeInfo->totalSize, i + 1,
               totalSources);
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      } else {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows, pExchangeInfo->totalRows,
               pExchangeInfo->totalSize);
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

static int32_t prepareConcurrentlyLoad(SOperatorInfo *pOperator) {
  SExchangeInfo *pExchangeInfo = pOperator->info;
  SExecTaskInfo *pTaskInfo = pOperator->pTaskInfo;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  // Asynchronously send all fetch requests to all sources.
  for(int32_t i = 0; i < totalSources; ++i) {
    int32_t code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      return code;
    }
  }

  int64_t endTs = taosGetTimestampUs();
  qDebug("%s send all fetch request to %"PRIzu" sources completed, elapsed:%"PRId64, GET_TASKID(pTaskInfo), totalSources, endTs - startTs);

  tsem_wait(&pExchangeInfo->ready);
  pOperator->cost.openCost = taosGetTimestampUs() - startTs;

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* seqLoadRemoteData(SOperatorInfo *pOperator) {
  SExchangeInfo *pExchangeInfo = pOperator->info;
  SExecTaskInfo *pTaskInfo = pOperator->pTaskInfo;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  while(1) {
    if (pExchangeInfo->current >= totalSources) {
      return setAllSourcesCompleted(pOperator, startTs);
    }

    doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);

    tsem_wait(&pExchangeInfo->ready);

    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);

    SRetrieveTableRsp* pRsp = pDataInfo->pRsp;
    if (pRsp->numOfRows == 0) {
      qDebug("%s vgId:%d, taskID:0x%"PRIx64" %d of total completed, rowsOfSource:%"PRIu64", totalRows:%"PRIu64" try next",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pExchangeInfo->current + 1,
             pDataInfo->totalRows, pExchangeInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
      continue;
    }

    SSDataBlock* pRes = pExchangeInfo->pResult;
    setSDataBlockFromFetchRsp(pExchangeInfo->pResult, pExchangeInfo, pDataInfo, pOperator->numOfOutput, startTs);

    if (pRsp->completed == 1) {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, rowsOfSource:%" PRIu64
                 ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows,
             pDataInfo->totalRows, pExchangeInfo->totalRows, pExchangeInfo->totalSize, pExchangeInfo->current + 1,
             totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pRes->info.rows, pExchangeInfo->totalRows, pExchangeInfo->totalSize);
    }

    return pExchangeInfo->pResult;
  }
}

static int32_t prepareLoadRemoteData(SOperatorInfo *pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SExchangeInfo *pExchangeInfo = pOperator->info;
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

static SSDataBlock* doLoadRemoteData(SOperatorInfo *pOperator, bool* newgroup) {
  SExchangeInfo *pExchangeInfo = pOperator->info;
  SExecTaskInfo *pTaskInfo = pOperator->pTaskInfo;

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  if (pOperator->status == OP_EXEC_DONE) {
    qDebug("%s all %"PRIzu" source(s) are exhausted, total rows:%"PRIu64" bytes:%"PRIu64", elapsed:%.2f ms", GET_TASKID(pTaskInfo), totalSources,
           pExchangeInfo->totalRows, pExchangeInfo->totalSize, pExchangeInfo->totalElapsed/1000.0);
    return NULL;
  }

  *newgroup = false;
  if (pExchangeInfo->seqLoadData) {
    return seqLoadRemoteData(pOperator);
  } else {
    return concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
  }

#if 0
  _error:
  tfree(pMsg);
  tfree(pMsgSendInfo);

  terrno = pTaskInfo->code;
  return NULL;
#endif
}

static int32_t initDataSource(int32_t numOfSources, SExchangeInfo* pInfo) {
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for(int32_t i = 0; i < numOfSources; ++i) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.pEx    = pInfo;
    dataInfo.index  = i;

    void* ret = taosArrayPush(pInfo->pSourceDataInfo, &dataInfo);
    if (ret == NULL) {
      taosArrayDestroy(pInfo->pSourceDataInfo);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createExchangeOperatorInfo(const SNodeList* pSources, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  SExchangeInfo* pInfo     = calloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  size_t numOfSources = LIST_LENGTH(pSources);
  pInfo->pSources = taosArrayInit(numOfSources, sizeof(SDownstreamSourceNode));
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL || pInfo->pSources == NULL) {
    goto _error;
  }

  for(int32_t i = 0; i < numOfSources; ++i) {
    SNodeListNode* pNode = nodesListGetNode((SNodeList*) pSources, i);
    taosArrayPush(pInfo->pSources, pNode);
  }

  int32_t code = initDataSource(numOfSources, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  size_t size        = pBlock->info.numOfCols;
  pInfo->pResult     = pBlock;
  pInfo->seqLoadData = true;

  pInfo->seqLoadData = true; // sequentially load data from the source node
  tsem_init(&pInfo->ready, 0, 0);

  pOperator->name         = "ExchangeOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = size;
  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->_openFn      = prepareLoadRemoteData;  // assign a dummy function.
  pOperator->getNextFn    = doLoadRemoteData;
  pOperator->closeFn      = destroyExchangeOperatorInfo;

#if 1
  { // todo refactor
    SRpcInit rpcInit;
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = "EX";
    rpcInit.numOfThreads = 1;
    rpcInit.cfp = qProcessFetchRsp;
    rpcInit.sessions = tsMaxConnections;
    rpcInit.connType = TAOS_CONN_CLIENT;
    rpcInit.user = (char *)"root";
    rpcInit.idleTime = tsShellActivityTimer * 1000;
    rpcInit.ckey = "key";
    rpcInit.spi = 1;
    rpcInit.secret = (char *)"dcc5bed04851fec854c035b2e40263b6";

    pInfo->pTransporter = rpcOpen(&rpcInit);
    if (pInfo->pTransporter == NULL) {
      return NULL; // todo
    }
  }
#endif

  return pOperator;

  _error:
  if (pInfo != NULL) {
    destroyExchangeOperatorInfo(pInfo, numOfSources);
  }

  tfree(pInfo);
  tfree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SSDataBlock* createResultDataBlock(const SArray* pExprInfo) {
  SSDataBlock* pResBlock = calloc(1, sizeof(SSDataBlock));
  if (pResBlock == NULL) {
    return NULL;
  }

  size_t numOfCols = taosArrayGetSize(pExprInfo);
  pResBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  SArray* pResult = pResBlock->pDataBlock;
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData colInfoData = {0};
    SExprInfo* p = taosArrayGetP(pExprInfo, i);

    SResSchema* pSchema = &p->base.resSchema;
    colInfoData.info.type  = pSchema->type;
    colInfoData.info.colId = pSchema->colId;
    colInfoData.info.bytes = pSchema->bytes;
    colInfoData.info.scale = pSchema->scale;
    colInfoData.info.precision = pSchema->precision;
    taosArrayPush(pResult, &colInfoData);
  }

  return pResBlock;
}

SOperatorInfo* createTableScanOperatorInfo(void* pTsdbReadHandle, int32_t order, int32_t numOfOutput, int32_t repeatTime, int32_t reverseTime, SExecTaskInfo* pTaskInfo) {
  assert(repeatTime > 0);

  STableScanInfo* pInfo    = calloc(1, sizeof(STableScanInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    tfree(pInfo);
    tfree(pOperator);

    pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->pTsdbReadHandle = pTsdbReadHandle;
  pInfo->times             = repeatTime;
  pInfo->reverseTimes      = reverseTime;
  pInfo->order             = order;
  pInfo->current           = 0;
  pInfo->scanFlag          = MAIN_SCAN;
  pOperator->name          = "TableScanOperator";
  pOperator->operatorType  = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pOperator->blockingOptr  = false;
  pOperator->status        = OP_NOT_OPENED;
  pOperator->info          = pInfo;
  pOperator->numOfOutput   = numOfOutput;
  pOperator->_openFn       = operatorDummyOpenFn;
  pOperator->getNextFn     = doTableScan;
  pOperator->closeFn       = operatorDummyCloseFn;
  pOperator->pTaskInfo     = pTaskInfo;

  return pOperator;
}

SOperatorInfo* createTableSeqScanOperatorInfo(void* pTsdbReadHandle, STaskRuntimeEnv* pRuntimeEnv) {
  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));

  pInfo->pTsdbReadHandle     = pTsdbReadHandle;
  pInfo->times            = 1;
  pInfo->reverseTimes     = 0;
  pInfo->order            = pRuntimeEnv->pQueryAttr->order.order;
  pInfo->current          = 0;
  pInfo->prevGroupId      = -1;
  pRuntimeEnv->enableGroupData = true;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableSeqScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = pRuntimeEnv->pQueryAttr->numOfCols;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->getNextFn    = doTableScanImpl;

  return pOperator;
}

SOperatorInfo* createTableBlockInfoScanOperator(void* pTsdbReadHandle, STaskRuntimeEnv* pRuntimeEnv) {
  STableScanInfo* pInfo = calloc(1, sizeof(STableScanInfo));

  pInfo->pTsdbReadHandle     = pTsdbReadHandle;
  pInfo->block.pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {{0}};
  infoData.info.type = TSDB_DATA_TYPE_BINARY;
  infoData.info.bytes = 1024;
  infoData.info.colId = 0;
  taosArrayPush(pInfo->block.pDataBlock, &infoData);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "TableBlockInfoScanOperator";
//  pOperator->operatorType = OP_TableBlockInfoScan;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
//  pOperator->numOfOutput  = pRuntimeEnv->pQueryAttr->numOfCols;
  pOperator->getNextFn = doBlockInfoScan;

  return pOperator;
}

SOperatorInfo* createStreamScanOperatorInfo(void *streamReadHandle, SSDataBlock* pResBlock, SArray* pColList, SArray* pTableIdList, SExecTaskInfo* pTaskInfo) {
  SStreamBlockScanInfo* pInfo = calloc(1, sizeof(SStreamBlockScanInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    tfree(pInfo);
    tfree(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  // set the extract column id to streamHandle
  tqReadHandleSetColIdList((STqReadHandle* )streamReadHandle, pColList);
  int32_t code = tqReadHandleSetTbUidList(streamReadHandle, pTableIdList);
  if (code != 0) {
    tfree(pInfo);
    tfree(pOperator);
    return NULL;
  }

  pInfo->readerHandle      = streamReadHandle;
  pInfo->pRes              = pResBlock;

  pOperator->name          = "StreamBlockScanOperator";
  pOperator->operatorType  = QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
  pOperator->blockingOptr  = false;
  pOperator->status        = OP_NOT_OPENED;
  pOperator->info          = pInfo;
  pOperator->numOfOutput   = pResBlock->info.numOfCols;
  pOperator->_openFn       = operatorDummyOpenFn;
  pOperator->getNextFn     = doStreamBlockScan;
  pOperator->closeFn       = operatorDummyCloseFn;

  pOperator->pTaskInfo     = pTaskInfo;
  return pOperator;
}


static int32_t loadSysTableContentCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SSourceDataInfo* pSourceDataInfo = (SSourceDataInfo*) param;
  pSourceDataInfo->pRsp = pMsg->pData;

  SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
  pRsp->numOfRows = htonl(pRsp->numOfRows);
  pRsp->useconds  = htobe64(pRsp->useconds);
  pRsp->compLen   = htonl(pRsp->compLen);

  pSourceDataInfo->status = EX_SOURCE_DATA_READY;
  tsem_post(&pSourceDataInfo->pEx->ready);
}

static SSDataBlock* doSysTableScan(SOperatorInfo *pOperator, bool* newgroup) {
// build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  // retrieve local table list info from vnode
  if (pInfo->type == TSDB_MGMT_TABLE_TABLE) {
    if (pInfo->pCur == NULL) {
      pInfo->pCur = metaOpenTbCursor(pInfo->readHandle);
    }

    SColumnInfoData* pTableNameCol = taosArrayGet(pInfo->pRes->pDataBlock, 0);

    char *  name = NULL;
    int32_t numOfRows = 0;
    while ((name = metaTbCursorNext(pInfo->pCur)) != NULL) {
      colDataAppend(pTableNameCol, numOfRows, name, false);
      numOfRows += 1;
      if (numOfRows >= pInfo->capacity) {
        break;
      }
    }

    pInfo->totalRows += numOfRows;
    pInfo->pRes->info.rows = numOfRows;

//    pInfo->elapsedTime;
//    pInfo->totalBytes;
    return (pInfo->pRes->info.rows == 0)? NULL:pInfo->pRes;
  } else {  // load the meta from mnode of the given epset
    if (pInfo->pReq == NULL) {
      pInfo->pReq = calloc(1, sizeof(SRetrieveTableReq));
      if (pInfo->pReq == NULL) {
        pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
        return NULL;
      }

      pInfo->pReq->type = pInfo->type;
    }

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      return NULL;
    }

    pMsgSendInfo->param = NULL;
    pMsgSendInfo->msgInfo.pData = pInfo->pReq;
    pMsgSendInfo->msgInfo.len = sizeof(SRetrieveTableReq);
    pMsgSendInfo->msgType = TDMT_MND_SYSTABLE_RETRIEVE;
    pMsgSendInfo->fp = loadRemoteDataCallback;

    int64_t transporterId = 0;
    int32_t code = asyncSendMsgToServer(pInfo->pTransporter, &pInfo->epSet, &transporterId, pMsgSendInfo);

    tsem_wait(&pInfo->ready);
    // handle the response and return to the caller
  }

  return NULL;
}

SOperatorInfo* createSysTableScanOperatorInfo(void* pSysTableReadHandle, const SArray* pExprInfo, const SSchema* pSchema,
    int32_t tableType, SEpSet epset, SExecTaskInfo* pTaskInfo) {
  SSysTableScanInfo* pInfo = calloc(1, sizeof(SSysTableScanInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    tfree(pInfo);
    tfree(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  // todo: create the schema of result data block
  pInfo->capacity = 4096;
  pInfo->type     = tableType;
  if (pInfo->type == TSDB_MGMT_TABLE_TABLE) {
    pInfo->readHandle = pSysTableReadHandle;
    blockDataEnsureCapacity(pInfo->pRes, pInfo->capacity);
  } else {
    tsem_init(&pInfo->ready, 0, 0);
    pInfo->epSet = epset;
  }

  pInfo->readHandle        = pSysTableReadHandle;
  pOperator->name          = "SysTableScanOperator";
  pOperator->operatorType  = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;
  pOperator->blockingOptr  = false;
  pOperator->status        = OP_IN_EXECUTING;
  pOperator->info          = pInfo;
  pOperator->numOfOutput   = taosArrayGetSize(pExprInfo);
  pOperator->getNextFn     = doSysTableScan;
  pOperator->closeFn       = destroySysTableScannerOperatorInfo;
  pOperator->pTaskInfo     = pTaskInfo;

  return pOperator;
}

void setTableScanFilterOperatorInfo(STableScanInfo* pTableScanInfo, SOperatorInfo* pDownstream) {
  assert(pTableScanInfo != NULL && pDownstream != NULL);

  pTableScanInfo->pExpr = pDownstream->pExpr;   // TODO refactor to use colId instead of pExpr
  pTableScanInfo->numOfOutput = pDownstream->numOfOutput;
#if 0
  if (pDownstream->operatorType == OP_Aggregate || pDownstream->operatorType == OP_MultiTableAggregate) {
    SAggOperatorInfo* pAggInfo = pDownstream->info;

    pTableScanInfo->pCtx = pAggInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pAggInfo->binfo.rowCellInfoOffset;
  } else if (pDownstream->operatorType == OP_TimeWindow || pDownstream->operatorType == OP_AllTimeWindow) {
    STableIntervalOperatorInfo *pIntervalInfo = pDownstream->info;

    pTableScanInfo->pCtx = pIntervalInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pIntervalInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pIntervalInfo->rowCellInfoOffset;

  } else if (pDownstream->operatorType == OP_Groupby) {
    SGroupbyOperatorInfo *pGroupbyInfo = pDownstream->info;

    pTableScanInfo->pCtx = pGroupbyInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pGroupbyInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pGroupbyInfo->binfo.rowCellInfoOffset;

  } else if (pDownstream->operatorType == OP_MultiTableTimeInterval || pDownstream->operatorType == OP_AllMultiTableTimeInterval) {
    STableIntervalOperatorInfo *pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->rowCellInfoOffset;

  } else if (pDownstream->operatorType == OP_Project) {
    SProjectOperatorInfo *pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->binfo.rowCellInfoOffset;
  } else if (pDownstream->operatorType == OP_SessionWindow) {
    SSessionAggOperatorInfo* pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->binfo.rowCellInfoOffset;
  } else if (pDownstream->operatorType == OP_StateWindow) {
    SStateWindowOperatorInfo* pInfo = pDownstream->info;

    pTableScanInfo->pCtx = pInfo->binfo.pCtx;
    pTableScanInfo->pResultRowInfo = &pInfo->binfo.resultRowInfo;
    pTableScanInfo->rowCellInfoOffset = pInfo->binfo.rowCellInfoOffset;
  } else {
    assert(0);
  }
#endif

}

SArray* getOrderCheckColumns(STaskAttr* pQuery) {
  int32_t numOfCols = (pQuery->pGroupbyExpr == NULL)? 0: taosArrayGetSize(pQuery->pGroupbyExpr->columnInfo);

  SArray* pOrderColumns = NULL;
  if (numOfCols > 0) {
    pOrderColumns = taosArrayDup(pQuery->pGroupbyExpr->columnInfo);
  } else {
    pOrderColumns = taosArrayInit(4, sizeof(SColIndex));
  }

  if (pQuery->interval.interval > 0) {
    if (pOrderColumns == NULL) {
      pOrderColumns = taosArrayInit(1, sizeof(SColIndex));
    }

    SColIndex colIndex = {.colIndex = 0, .colId = 0, .flag = TSDB_COL_NORMAL};
    taosArrayPush(pOrderColumns, &colIndex);
  }

  {
    numOfCols = (int32_t) taosArrayGetSize(pOrderColumns);
    for(int32_t i = 0; i < numOfCols; ++i) {
      SColIndex* index = taosArrayGet(pOrderColumns, i);
      for(int32_t j = 0; j < pQuery->numOfOutput; ++j) {
        SExprBasicInfo* pExpr = &pQuery->pExpr1[j].base;
        int32_t functionId = getExprFunctionId(&pQuery->pExpr1[j]);

        if (index->colId == pExpr->pParam[0].pCol->colId &&
            (functionId == FUNCTION_PRJ || functionId == FUNCTION_TAG || functionId == FUNCTION_TS)) {
          index->colIndex = j;
          index->colId = pExpr->resSchema.colId;
        }
      }
    }
  }

  return pOrderColumns;
}

SArray* getResultGroupCheckColumns(STaskAttr* pQuery) {
  int32_t numOfCols = (pQuery->pGroupbyExpr == NULL)? 0 : taosArrayGetSize(pQuery->pGroupbyExpr->columnInfo);

  SArray* pOrderColumns = NULL;
  if (numOfCols > 0) {
    pOrderColumns = taosArrayDup(pQuery->pGroupbyExpr->columnInfo);
  } else {
    pOrderColumns = taosArrayInit(4, sizeof(SColIndex));
  }

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColIndex* index = taosArrayGet(pOrderColumns, i);

    bool found = false;
    for(int32_t j = 0; j < pQuery->numOfOutput; ++j) {
      SExprBasicInfo* pExpr = &pQuery->pExpr1[j].base;
      int32_t functionId = getExprFunctionId(&pQuery->pExpr1[j]);

      // FUNCTION_TAG_DUMMY function needs to be ignored
//      if (index->colId == pExpr->pColumns->info.colId &&
//          ((TSDB_COL_IS_TAG(pExpr->pColumns->flag) && functionId == FUNCTION_TAG) ||
//           (TSDB_COL_IS_NORMAL_COL(pExpr->pColumns->flag) && functionId == FUNCTION_PRJ))) {
//        index->colIndex = j;
//        index->colId = pExpr->resSchema.colId;
//        found = true;
//        break;
//      }
    }

    assert(found && index->colIndex >= 0 && index->colIndex < pQuery->numOfOutput);
  }

  return pOrderColumns;
}

static int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx *pCtx, int32_t numOfOutput, const char* pKey);
static void cleanupAggSup(SAggSupporter* pAggSup);

static void destroySortedMergeOperatorInfo(void* param, int32_t numOfOutput) {
  SSortedMergeOperatorInfo* pInfo = (SSortedMergeOperatorInfo*) param;
  taosArrayDestroy(pInfo->orderInfo);
  taosArrayDestroy(pInfo->groupInfo);

  if (pInfo->pSortHandle != NULL) {
    tsortDestroySortHandle(pInfo->pSortHandle);
  }
  blockDataDestroy(pInfo->binfo.pRes);

  cleanupAggSup(&pInfo->aggSup);
}

static void destroySlimitOperatorInfo(void* param, int32_t numOfOutput) {
  SSLimitOperatorInfo *pInfo = (SSLimitOperatorInfo*) param;
  taosArrayDestroy(pInfo->orderColumnList);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  tfree(pInfo->prevRow);
}

static void assignExprInfo(SExprInfo* dst, const SExprInfo* src) {
  assert(dst != NULL && src != NULL);

  *dst = *src;

  dst->pExpr = exprdup(src->pExpr);
  dst->base.pParam = calloc(src->base.numOfParams, sizeof(SColumn));
  memcpy(dst->base.pParam, src->base.pParam, sizeof(SColumn) * src->base.numOfParams);

//  memset(dst->base.param, 0, sizeof(SVariant) * tListLen(dst->base.param));
//  for (int32_t j = 0; j < src->base.numOfParams; ++j) {
//    taosVariantAssign(&dst->base.param[j], &src->base.param[j]);
//  }
}

static SExprInfo* exprArrayDup(SArray* pExprList) {
  size_t numOfOutput = taosArrayGetSize(pExprList);

  SExprInfo* p = calloc(numOfOutput, sizeof(SExprInfo));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprList, i);
    assignExprInfo(&p[i], pExpr);
  }

  return p;
}

// TODO merge aggregate super table
static void appendOneRowToDataBlock(SSDataBlock *pBlock, STupleHandle* pTupleHandle) {
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

static SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, bool hasVarCol, int32_t capacity) {
  blockDataClearup(pDataBlock);

  while(1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(pDataBlock, pTupleHandle);
    if (pDataBlock->info.rows >= capacity) {
      return pDataBlock;
    }
  }

  return (pDataBlock->info.rows > 0)? pDataBlock:NULL;
}

SSDataBlock* loadNextDataBlock(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  bool newgroup = false;

  return pOperator->getNextFn(pOperator, &newgroup);
}

static bool needToMerge(SSDataBlock* pBlock, SArray* groupInfo, char **buf, int32_t rowIndex) {
  size_t size = taosArrayGetSize(groupInfo);
  if (size == 0) {
    return true;
  }

  for (int32_t i = 0; i < size; ++i) {
    int32_t* index = taosArrayGet(groupInfo, i);

    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, *index);
    bool isNull = colDataIsNull(pColInfo, rowIndex, pBlock->info.rows, NULL);

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

static void doMergeResultImpl(SSortedMergeOperatorInfo* pInfo, SqlFunctionCtx *pCtx, int32_t numOfExpr, int32_t rowIndex) {
  for (int32_t j = 0; j < numOfExpr; ++j) { // TODO set row index
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

static void doFinalizeResultImpl(SqlFunctionCtx *pCtx, int32_t numOfExpr) {
  for(int32_t j = 0; j < numOfExpr; ++j) {
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
  int32_t size = (int32_t) taosArrayGetSize(pColumnList);

  for(int32_t i = 0; i < size; ++i) {
    int32_t* index = taosArrayGet(pColumnList, i);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, *index);

    char* data = colDataGetData(pColInfo, rowIndex);
    memcpy(rowColData[i], data, colDataGetLength(pColInfo, rowIndex));
  }

  return true;
}

static void doMergeImpl(SOperatorInfo* pOperator, int32_t numOfExpr, SSDataBlock* pBlock) {
  SSortedMergeOperatorInfo* pInfo = pOperator->info;

  SqlFunctionCtx* pCtx = pInfo->binfo.pCtx;
  for(int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    pCtx[i].size = 1;
  }

  for(int32_t i = 0; i < pBlock->info.rows; ++i) {
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
  SSortHandle* pHandle = pInfo->pSortHandle;

  SSDataBlock* pDataBlock = createOneDataBlock(pInfo->binfo.pRes);
  blockDataEnsureCapacity(pDataBlock, pInfo->binfo.capacity);

  while(1) {

    blockDataClearup(pDataBlock);
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
    //  updateOutputBuf(&pInfo->binfo, &pAggInfo->bufCapacity, pBlock->info.rows * pAggInfo->resultRowFactor, pOperator->pRuntimeEnv, true);
    doMergeImpl(pOperator, pOperator->numOfOutput, pDataBlock);
    // flush to tuple store, and after all data have been handled, return to upstream node or sink node
  }

  doFinalizeResultImpl(pInfo->binfo.pCtx, pOperator->numOfOutput);
  int32_t numOfRows = getNumOfResult(pInfo->binfo.pCtx, pOperator->numOfOutput, NULL);
  //        setTagValueForMultipleRows(pCtx, pOperator->numOfOutput, numOfRows);

  // TODO check for available buffer;

  // next group info data
  pInfo->binfo.pRes->info.rows += numOfRows;
  return (pInfo->binfo.pRes->info.rows > 0)? pInfo->binfo.pRes:NULL;
}

static SSDataBlock* doSortedMerge(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSortedMergeOperatorInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    return getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pInfo->hasVarCol, pInfo->binfo.capacity);
  }

  SSchema* p = blockDataExtractSchema(pInfo->binfo.pRes, NULL);
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->orderInfo, pInfo->nullFirst, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize,
                                        numOfBufPage, p, pInfo->binfo.pRes->info.numOfCols, "GET_TASKID(pTaskInfo)");

  tfree(p);
  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock);

  for(int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SGenericSource* ps = calloc(1, sizeof(SGenericSource));
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

static SArray* createBlockOrder(SExprInfo* pExprInfo, int32_t numOfCols, SArray* pOrderVal) {
  SArray* pOrderInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));

  size_t numOfOrder = taosArrayGetSize(pOrderVal);
  for (int32_t j = 0; j < numOfOrder; ++j) {
    SBlockOrderInfo orderInfo = {0};
    SOrder*         pOrder = taosArrayGet(pOrderVal, j);
    orderInfo.order = pOrder->order;

    for (int32_t i = 0; i < numOfCols; ++i) {
      SExprInfo* pExpr = &pExprInfo[i];
      if (pExpr->base.resSchema.colId == pOrder->col.colId) {
        orderInfo.colIndex = i;
        break;
      }
    }

    taosArrayPush(pOrderInfo, &orderInfo);
  }

  return pOrderInfo;
}

static int32_t initGroupCol(SExprInfo* pExprInfo, int32_t numOfCols, SArray* pGroupInfo, SSortedMergeOperatorInfo* pInfo) {
  if (pGroupInfo == NULL || taosArrayGetSize(pGroupInfo) == 0) {
    return 0;
  }

  int32_t len = 0;
  SArray* plist = taosArrayInit(3, sizeof(SColumn));
  pInfo->groupInfo = taosArrayInit(3, sizeof(int32_t));

  if (plist == NULL || pInfo->groupInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  size_t  numOfGroupCol = taosArrayGetSize(pInfo->groupInfo);
  for(int32_t i = 0; i < numOfGroupCol; ++i) {
    SColumn* pCol = taosArrayGet(pGroupInfo, i);
    for(int32_t j = 0; j < numOfCols; ++j) {
      SExprInfo* pe = &pExprInfo[j];
      if (pe->base.resSchema.colId == pCol->colId) {
        taosArrayPush(plist, pCol);
        taosArrayPush(pInfo->groupInfo, &j);
        len += pCol->bytes;
        break;
      }
    }
  }

  ASSERT(taosArrayGetSize(pGroupInfo) == taosArrayGetSize(plist));

  pInfo->groupVal = calloc(1, (POINTER_BYTES * numOfGroupCol + len));
  if (pInfo->groupVal == NULL) {
    taosArrayDestroy(plist);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t offset = 0;
  char   *start = (char*)(pInfo->groupVal + (POINTER_BYTES * numOfGroupCol));
  for(int32_t i = 0; i < numOfGroupCol; ++i) {
    pInfo->groupVal[i] = start + offset;
    SColumn* pCol = taosArrayGet(plist, i);
    offset += pCol->bytes;
  }

  taosArrayDestroy(plist);

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createSortedMergeOperatorInfo(SOperatorInfo** downstream, int32_t numOfDownstream, SExprInfo* pExprInfo, int32_t num, SArray* pOrderVal, SArray* pGroupInfo, SExecTaskInfo* pTaskInfo) {
  SSortedMergeOperatorInfo* pInfo = calloc(1, sizeof(SSortedMergeOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->binfo.pCtx   = createSqlFunctionCtx_rv(pExprInfo, num, &pInfo->binfo.rowCellInfoOffset);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)1);

  if (pInfo->binfo.pCtx == NULL || pInfo->binfo.pRes == NULL) {
    goto _error;
  }

  int32_t code = doInitAggInfoSup(&pInfo->aggSup, pInfo->binfo.pCtx, num, pTaskInfo->id.str);
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
  pInfo->sortBufSize    = 1024 * 16; // 1MB
  pInfo->bufPageSize    = 1024;
  pInfo->orderInfo      = createBlockOrder(pExprInfo, num, pOrderVal);

  pInfo->binfo.capacity = blockDataGetCapacityInRow(pInfo->binfo.pRes, pInfo->bufPageSize);

  pOperator->name         = "SortedMerge";
  // pOperator->operatorType = OP_SortedMerge;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = num;
  pOperator->pExpr        = pExprInfo;

  pOperator->pTaskInfo    = pTaskInfo;
  pOperator->getNextFn    = doSortedMerge;
  pOperator->closeFn      = destroySortedMergeOperatorInfo;

  code = appendDownstream(pOperator, downstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

  _error:
  if (pInfo != NULL) {
    destroySortedMergeOperatorInfo(pInfo, num);
  }

  tfree(pInfo);
  tfree(pOperator);
  terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
  return NULL;
}

static SSDataBlock* doSort(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SOrderOperatorInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    return getSortedBlockData(pInfo->pSortHandle, pInfo->pDataBlock, pInfo->hasVarCol, pInfo->numOfRowsInRes);
  }

  SSchema* p = blockDataExtractSchema(pInfo->pDataBlock, NULL);
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->orderInfo, pInfo->nullFirst, SORT_SINGLESOURCE_SORT, pInfo->bufPageSize,
                                        numOfBufPage, p, pInfo->pDataBlock->info.numOfCols, "GET_TASKID(pTaskInfo)");

  tfree(p);
  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock);

  SGenericSource* ps = calloc(1, sizeof(SGenericSource));
  ps->param = pOperator;
  tsortAddSource(pInfo->pSortHandle, ps);

  // TODO set error code;
  int32_t code = tsortOpen(pInfo->pSortHandle);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
  }

  pOperator->status = OP_RES_TO_RETURN;
  return getSortedBlockData(pInfo->pSortHandle, pInfo->pDataBlock, pInfo->hasVarCol, pInfo->numOfRowsInRes);
}

SOperatorInfo *createOrderOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SArray* pOrderVal, SExecTaskInfo* pTaskInfo) {
  SOrderOperatorInfo* pInfo = calloc(1, sizeof(SOrderOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    tfree(pInfo);
    tfree(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->sortBufSize    = 1024 * 16; // 1MB
  pInfo->bufPageSize    = 1024;
  pInfo->numOfRowsInRes = 1024;

  pInfo->orderInfo  = createBlockOrder(pExprInfo, numOfCols, pOrderVal);

  for(int32_t i = 0; i < numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE(pExprInfo[i].base.resSchema.type)) {
      pInfo->hasVarCol = true;
      break;
    }
  }

  if (pInfo->orderInfo == NULL || pInfo->pDataBlock == NULL) {
    tfree(pOperator);
    destroyOrderOperatorInfo(pInfo, numOfCols);
    tfree(pInfo);

    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pOperator->name          = "Order";
  pOperator->operatorType  = QUERY_NODE_PHYSICAL_PLAN_SORT;
  pOperator->blockingOptr  = true;
  pOperator->status        = OP_NOT_OPENED;
  pOperator->info          = pInfo;

  pOperator->pTaskInfo     = pTaskInfo;
  pOperator->getNextFn = doSort;
  pOperator->closeFn = destroyOrderOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

static int32_t getTableScanOrder(STableScanInfo* pTableScanInfo) {
  return pTableScanInfo->order;
}

// this is a blocking operator
static int32_t doOpenAggregateOptr(SOperatorInfo *pOperator) {
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
  }

  finalizeQueryResult(pInfo->pCtx, pOperator->numOfOutput);

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* getAggregateResult(SOperatorInfo *pOperator, bool* newgroup) {
  SAggOperatorInfo *pAggInfo = pOperator->info;
  SOptrBasicInfo*   pInfo = &pAggInfo->binfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo  *pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  getNumOfResult(pInfo->pCtx, pOperator->numOfOutput, pInfo->pRes);
  doSetOperatorCompleted(pOperator);

  return (blockDataGetNumOfRows(pInfo->pRes) != 0)? pInfo->pRes:NULL;
}

static SSDataBlock* doMultiTableAggregate(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo* pInfo = &pAggInfo->binfo;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSDatablock(&pAggInfo->groupResInfo, pAggInfo->pResultBuf, pInfo->pRes, pAggInfo->binfo.capacity, pAggInfo->binfo.rowCellInfoOffset);

    if (pInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pAggInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pInfo->pRes;
  }

  // table scan order
  int32_t order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
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
    
    setExecutionContext(pOperator->numOfOutput, pAggInfo->current->groupIndex, key, pTaskInfo, pAggInfo->current, pAggInfo);
    doAggregateImpl(pOperator, 0, pInfo->pCtx);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->resultRowInfo);
  updateNumOfRowsInResultRows(pInfo->pCtx, pOperator->numOfOutput, &pInfo->resultRowInfo, pInfo->rowCellInfoOffset);

  initGroupResInfo(&pAggInfo->groupResInfo, &pInfo->resultRowInfo);
  toSDatablock(&pAggInfo->groupResInfo, pAggInfo->pResultBuf, pInfo->pRes, pAggInfo->binfo.capacity, pAggInfo->binfo.rowCellInfoOffset);

  if (pInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pAggInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  return pInfo->pRes;
}

static SSDataBlock* doProjectOperation(SOperatorInfo *pOperator, bool* newgroup) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo *pInfo = &pProjectInfo->binfo;

  SSDataBlock* pRes = pInfo->pRes;
  blockDataClearup(pRes);

  if (pProjectInfo->existDataBlock) {  // TODO refactor
//    STableQueryInfo* pTableQueryInfo = pRuntimeEnv->current;
    SSDataBlock* pBlock = pProjectInfo->existDataBlock;
    pProjectInfo->existDataBlock = NULL;
    *newgroup = true;

    // todo dynamic set tags
//    if (pTableQueryInfo != NULL) {
//      setTagValue(pOperator, pTableQueryInfo->pTable, pInfo->pCtx, pOperator->numOfOutput);
//    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, TSDB_ORDER_ASC);
    updateOutputBuf(pInfo, &pInfo->capacity, pBlock->info.rows);

    projectApplyFunctions(pInfo->pCtx, pOperator->numOfOutput);

    pRes->info.rows = getNumOfResult(pInfo->pCtx, pOperator->numOfOutput, NULL);
    if (pRes->info.rows >= pProjectInfo->threshold) {
      copyTsColoum(pRes, pInfo->pCtx, pOperator->numOfOutput);
      resetResultRowEntryResult(pInfo->pCtx, pOperator->numOfOutput);
      return pRes;
    }
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
    bool prevVal = *newgroup;

    // The downstream exec may change the value of the newgroup, so use a local variable instead.
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      assert(*newgroup == false);
      *newgroup = prevVal;
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      break;
    }

    // Return result of the previous group in the firstly.
    if (*newgroup) {
      if (pRes->info.rows > 0) {
        pProjectInfo->existDataBlock = pBlock;
        break;
      } else { // init output buffer for a new group data
//        for (int32_t j = 0; j < pOperator->numOfOutput; ++j) {
//          aAggs[pInfo->pCtx[j].functionId].xFinalize(&pInfo->pCtx[j]);
//        }
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
    updateOutputBuf(pInfo, &pInfo->capacity, pBlock->info.rows);

    projectApplyFunctions(pInfo->pCtx, pOperator->numOfOutput);
//    pRes->info.rows = getNumOfResult(pInfo->pCtx, pOperator->numOfOutput, pRes);
    if (pRes->info.rows >= pProjectInfo->threshold) {
      break;
    }
  }

  copyTsColoum(pRes, pInfo->pCtx, pOperator->numOfOutput);
//  resetResultRowEntryResult(pInfo->pCtx, pOperator->numOfOutput);
  return (pInfo->pRes->info.rows > 0)? pInfo->pRes:NULL;
}

static SSDataBlock* doLimit(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SLimitOperatorInfo* pInfo = pOperator->info;

  SSDataBlock* pBlock = NULL;
  SOperatorInfo* pDownstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(pDownstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    pBlock = pDownstream->getNextFn(pDownstream, newgroup);
    publishOperatorProfEvent(pDownstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    if (pInfo->currentOffset == 0) {
      break;
    } else if (pInfo->currentOffset >= pBlock->info.rows) {
      pInfo->currentOffset -= pBlock->info.rows;
    } else { // TODO handle the data movement
      int32_t remain = (int32_t)(pBlock->info.rows - pInfo->currentOffset);
      pBlock->info.rows = remain;

      for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

        int16_t bytes = pColInfoData->info.bytes;
        memmove(pColInfoData->pData, pColInfoData->pData + bytes * pInfo->currentOffset, remain * bytes);
      }

      pInfo->currentOffset = 0;
      break;
    }
  }

  if (pInfo->currentRows + pBlock->info.rows >= pInfo->limit.limit) {
    pBlock->info.rows = (int32_t)(pInfo->limit.limit - pInfo->currentRows);
    pInfo->currentRows = pInfo->limit.limit;

    doSetOperatorCompleted(pOperator);
  } else {
    pInfo->currentRows += pBlock->info.rows;
  }

  return pBlock;
}

static SSDataBlock* doFilter(void* param, bool* newgroup) {
  SOperatorInfo *pOperator = (SOperatorInfo *)param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SFilterOperatorInfo* pCondInfo = pOperator->info;
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  while (1) {
    publishOperatorProfEvent(pOperator->pDownstream[0], QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock *pBlock = pOperator->pDownstream[0]->getNextFn(pOperator->pDownstream[0], newgroup);
    publishOperatorProfEvent(pOperator->pDownstream[0], QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

    doSetFilterColumnInfo(pCondInfo->pFilterInfo, pCondInfo->numOfFilterCols, pBlock);
    assert(pRuntimeEnv->pTsBuf == NULL);
    filterRowsInDataBlock(pRuntimeEnv, pCondInfo->pFilterInfo, pCondInfo->numOfFilterCols, pBlock, true);

    if (pBlock->info.rows > 0) {
      return pBlock;
    }
  }

  doSetOperatorCompleted(pOperator);
  return NULL;
}

static int32_t doOpenIntervalAgg(SOperatorInfo *pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  STableIntervalOperatorInfo* pInfo = pOperator->info;

  //  int32_t order = pQueryAttr->order.order;
  //  STimeWindow win = pQueryAttr->window;
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
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, TSDB_ORDER_ASC);
    hashIntervalAgg(pOperator, &pInfo->binfo.resultRowInfo, pBlock, 0);
  }

  closeAllResultRows(&pInfo->binfo.resultRowInfo);
  finalizeMultiTupleQueryResult(pInfo->binfo.pCtx, pOperator->numOfOutput, pInfo->aggSup.pResultBuf, &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);

  initGroupResInfo(&pInfo->groupResInfo, &pInfo->binfo.resultRowInfo);
  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doIntervalAgg(SOperatorInfo *pOperator, bool* newgroup) {
  STableIntervalOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  blockDataEnsureCapacity(pInfo->binfo.pRes, pInfo->binfo.capacity);
  toSDatablock(&pInfo->groupResInfo, pInfo->aggSup.pResultBuf, pInfo->binfo.pRes, pInfo->binfo.capacity, pInfo->binfo.rowCellInfoOffset);

  if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  return pInfo->binfo.pRes->info.rows == 0? NULL:pInfo->binfo.pRes;
}

static SSDataBlock* doAllIntervalAgg(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;

  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (pOperator->status == OP_RES_TO_RETURN) {
//    toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

    if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }

    return pIntervalInfo->binfo.pRes;
  }

  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t order = pQueryAttr->order.order;
  STimeWindow win = pQueryAttr->window;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }

//    setTagValue(pOperator, pRuntimeEnv->current->pTable, pIntervalInfo->pCtx, pOperator->numOfOutput);

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pIntervalInfo->binfo.pCtx, pBlock, pQueryAttr->order.order);
    hashAllIntervalAgg(pOperator, &pIntervalInfo->binfo.resultRowInfo, pBlock, 0);
  }

  // restore the value
  pQueryAttr->order.order = order;
  pQueryAttr->window = win;

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pIntervalInfo->binfo.resultRowInfo);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  finalizeQueryResult(pIntervalInfo->binfo.pCtx, pOperator->numOfOutput);

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pIntervalInfo->binfo.resultRowInfo);
//  toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pIntervalInfo->pRes);

  if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pIntervalInfo->binfo.pRes->info.rows == 0? NULL:pIntervalInfo->binfo.pRes;
}

static SSDataBlock* doSTableIntervalAgg(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (pOperator->status == OP_RES_TO_RETURN) {
    int64_t st = taosGetTimestampUs();

//    copyToSDataBlock(NULL, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
    if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }

    SQInfo* pQInfo = pRuntimeEnv->qinfo;
    pQInfo->summary.firstStageMergeTime += (taosGetTimestampUs() - st);

    return pIntervalInfo->binfo.pRes;
  }

  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t order = pQueryAttr->order.order;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
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
  pQueryAttr->order.order = order;   // TODO : restore the order
  doCloseAllTimeWindow(pRuntimeEnv);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);

//  copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
  if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pIntervalInfo->binfo.pRes;
}

static SSDataBlock* doAllSTableIntervalAgg(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableIntervalOperatorInfo* pIntervalInfo = pOperator->info;
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;

  if (pOperator->status == OP_RES_TO_RETURN) {
//    copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
    if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pIntervalInfo->binfo.pRes;
  }

  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t order = pQueryAttr->order.order;

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
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

    hashAllIntervalAgg(pOperator, &pTableQueryInfo->resInfo, pBlock, pTableQueryInfo->groupIndex);
  }

  pOperator->status = OP_RES_TO_RETURN;
  pQueryAttr->order.order = order;   // TODO : restore the order
  doCloseAllTimeWindow(pRuntimeEnv);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);

  int64_t st = taosGetTimestampUs();
//  copyToSDataBlock(pRuntimeEnv, 3000, pIntervalInfo->pRes, pIntervalInfo->rowCellInfoOffset);
  if (pIntervalInfo->binfo.pRes->info.rows == 0 || !hasRemainData(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  SQInfo* pQInfo = pRuntimeEnv->qinfo;
  pQInfo->summary.firstStageMergeTime += (taosGetTimestampUs() - st);

  return pIntervalInfo->binfo.pRes;
}

static void doStateWindowAggImpl(SOperatorInfo* pOperator, SStateWindowOperatorInfo *pInfo, SSDataBlock *pSDataBlock) {
  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  STableQueryInfo*  item = pRuntimeEnv->current;
  SColumnInfoData* pColInfoData = taosArrayGet(pSDataBlock->pDataBlock, pInfo->colIndex);

  SOptrBasicInfo* pBInfo = &pInfo->binfo;

  bool    masterScan = IS_MAIN_SCAN(pRuntimeEnv);
  int16_t     bytes = pColInfoData->info.bytes;
  int16_t     type = pColInfoData->info.type;

  SColumnInfoData* pTsColInfoData = taosArrayGet(pSDataBlock->pDataBlock, 0);
  TSKEY* tsList = (TSKEY*)pTsColInfoData->pData;
  if (IS_REPEAT_SCAN(pRuntimeEnv) && !pInfo->reptScan) {
    pInfo->reptScan = true;
    tfree(pInfo->prevData);
  }

  pInfo->numOfRows = 0;
  for (int32_t j = 0; j < pSDataBlock->info.rows; ++j) {
    char* val = ((char*)pColInfoData->pData) + bytes * j;
    if (isNull(val, type)) {
      continue;
    }
    if (pInfo->prevData == NULL) {
      pInfo->prevData = malloc(bytes);
      memcpy(pInfo->prevData, val, bytes);
      pInfo->numOfRows = 1;
      pInfo->curWindow.skey = tsList[j];
      pInfo->curWindow.ekey = tsList[j];
      pInfo->start = j;

    } else if (memcmp(pInfo->prevData, val, bytes) == 0) {
      pInfo->curWindow.ekey = tsList[j];
      pInfo->numOfRows += 1;
      //pInfo->start = j;
      if (j == 0 && pInfo->start != 0) {
        pInfo->numOfRows = 1;
        pInfo->start = 0;
      }
    } else {
      SResultRow* pResult = NULL;
      pInfo->curWindow.ekey = pInfo->curWindow.skey;
      int32_t ret = setResultOutputBufByKey(pRuntimeEnv, &pBInfo->resultRowInfo, pSDataBlock->info.uid, &pInfo->curWindow, masterScan,
                                            &pResult, item->groupIndex, pBInfo->pCtx, pOperator->numOfOutput,
                                            pBInfo->rowCellInfoOffset);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
      }
//      doApplyFunctions(pRuntimeEnv, pBInfo->pCtx, &pInfo->curWindow, pInfo->start, pInfo->numOfRows, tsList,
//                       pSDataBlock->info.rows, pOperator->numOfOutput);

      pInfo->curWindow.skey = tsList[j];
      pInfo->curWindow.ekey = tsList[j];
      memcpy(pInfo->prevData, val, bytes);
      pInfo->numOfRows = 1;
      pInfo->start = j;

    }
  }

  SResultRow* pResult = NULL;

  pInfo->curWindow.ekey = pInfo->curWindow.skey;
  int32_t ret = setResultOutputBufByKey(pRuntimeEnv, &pBInfo->resultRowInfo, pSDataBlock->info.uid, &pInfo->curWindow, masterScan,
                                        &pResult, item->groupIndex, pBInfo->pCtx, pOperator->numOfOutput,
                                        pBInfo->rowCellInfoOffset);
  if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
    longjmp(pRuntimeEnv->env, TSDB_CODE_QRY_APP_ERROR);
  }

//  doApplyFunctions(pRuntimeEnv, pBInfo->pCtx, &pInfo->curWindow, pInfo->start, pInfo->numOfRows, tsList,
//                   pSDataBlock->info.rows, pOperator->numOfOutput);
}

static SSDataBlock* doStateWindowAgg(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SStateWindowOperatorInfo* pWindowInfo = pOperator->info;
  SOptrBasicInfo* pBInfo = &pWindowInfo->binfo;

  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (pOperator->status == OP_RES_TO_RETURN) {
//    toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pBInfo->pRes);

    if (pBInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pBInfo->pRes;
  }

  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  int32_t order = pQueryAttr->order.order;
  STimeWindow win = pQueryAttr->window;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      break;
    }
    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, pQueryAttr->order.order);
    if (pWindowInfo->colIndex == -1) {
      pWindowInfo->colIndex = getGroupbyColumnIndex(pRuntimeEnv->pQueryAttr->pGroupbyExpr, pBlock);
    }
    doStateWindowAggImpl(pOperator,  pWindowInfo, pBlock);
  }

  // restore the value
  pQueryAttr->order.order = order;
  pQueryAttr->window = win;

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pBInfo->resultRowInfo);
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  finalizeQueryResult(pBInfo->pCtx, pOperator->numOfOutput);

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pBInfo->resultRowInfo);
//  toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pBInfo->pRes);

  if (pBInfo->pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pBInfo->pRes->info.rows == 0? NULL:pBInfo->pRes;
}

static SSDataBlock* doSessionWindowAgg(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSessionAggOperatorInfo* pWindowInfo = pOperator->info;
  SOptrBasicInfo* pBInfo = &pWindowInfo->binfo;

  if (pOperator->status == OP_RES_TO_RETURN) {
//    toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pBInfo->pRes);
    if (pBInfo->pRes->info.rows == 0/* || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)*/) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pBInfo->pRes;
  }

  int32_t order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pBInfo->pCtx, pBlock, order);
    doSessionWindowAggImpl(pOperator, pWindowInfo, pBlock);
  }

  // restore the value
  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pBInfo->resultRowInfo);
//  setTaskStatus(pOperator->pTaskInfo, QUERY_COMPLETED);
  finalizeQueryResult(pBInfo->pCtx, pOperator->numOfOutput);

//  initGroupResInfo(&pBInfo->groupResInfo, &pBInfo->resultRowInfo);
//  toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pBInfo->pRes);
  if (pBInfo->pRes->info.rows == 0/* || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)*/) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pBInfo->pRes->info.rows == 0? NULL:pBInfo->pRes;
}

static SSDataBlock* hashGroupbyAggregate(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SGroupbyOperatorInfo *pInfo = pOperator->info;

  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  if (pOperator->status == OP_RES_TO_RETURN) {
//    toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);

    if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }

    return pInfo->binfo.pRes;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while(1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, pRuntimeEnv->pQueryAttr->order.order);
//    setTagValue(pOperator, pRuntimeEnv->current->pTable, pInfo->binfo.pCtx, pOperator->numOfOutput);
    if (pInfo->colIndex == -1) {
      pInfo->colIndex = getGroupbyColumnIndex(pRuntimeEnv->pQueryAttr->pGroupbyExpr, pBlock);
    }

    doHashGroupbyAgg(pOperator, pInfo, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->binfo.resultRowInfo);
//  setTaskStatus(pOperator->pTaskInfo, QUERY_COMPLETED);

  if (!pRuntimeEnv->pQueryAttr->stableQuery) { // finalize include the update of result rows
    finalizeQueryResult(pInfo->binfo.pCtx, pOperator->numOfOutput);
  } else {
    updateNumOfRowsInResultRows(pInfo->binfo.pCtx, pOperator->numOfOutput, &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);
  }

  initGroupResInfo(&pRuntimeEnv->groupResInfo, &pInfo->binfo.resultRowInfo);
  if (!pRuntimeEnv->pQueryAttr->stableQuery) {
    sortGroupResByOrderList(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);
  }

//  toSDatablock(&pRuntimeEnv->groupResInfo, pRuntimeEnv, pInfo->binfo.pRes);
  if (pInfo->binfo.pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pRuntimeEnv->groupResInfo)) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pInfo->binfo.pRes;
}

static void doHandleRemainBlockForNewGroupImpl(SFillOperatorInfo *pInfo, STaskRuntimeEnv* pRuntimeEnv, bool* newgroup) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;
  int64_t ekey = Q_STATUS_EQUAL(pRuntimeEnv->status, TASK_COMPLETED)?pRuntimeEnv->pQueryAttr->window.ekey:pInfo->existNewGroupBlock->info.window.ekey;
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->existNewGroupBlock->info.rows, ekey);
  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->existNewGroupBlock);

  doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pRuntimeEnv->resultInfo.capacity, pInfo->p);
  pInfo->existNewGroupBlock = NULL;
  *newgroup = true;
}

static void doHandleRemainBlockFromNewGroup(SFillOperatorInfo *pInfo, STaskRuntimeEnv  *pRuntimeEnv, bool *newgroup) {
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    *newgroup = false;
    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, (int32_t)pRuntimeEnv->resultInfo.capacity, pInfo->p);
    if (pInfo->pRes->info.rows > pRuntimeEnv->resultInfo.threshold || (!pInfo->multigroupResult)) {
      return;
    }
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pInfo, pRuntimeEnv, newgroup);
  }
}

static SSDataBlock* doFill(SOperatorInfo *pOperator, bool* newgroup) {
  SFillOperatorInfo *pInfo = pOperator->info;
  pInfo->pRes->info.rows = 0;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STaskRuntimeEnv  *pRuntimeEnv = pOperator->pRuntimeEnv;
  doHandleRemainBlockFromNewGroup(pInfo, pRuntimeEnv, newgroup);
  if (pInfo->pRes->info.rows > pRuntimeEnv->resultInfo.threshold || (!pInfo->multigroupResult && pInfo->pRes->info.rows > 0)) {
    return pInfo->pRes;
  }

  while(1) {
    publishOperatorProfEvent(pOperator->pDownstream[0], QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = pOperator->pDownstream[0]->getNextFn(pOperator->pDownstream[0], newgroup);
    publishOperatorProfEvent(pOperator->pDownstream[0], QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (*newgroup) {
      assert(pBlock != NULL);
    }

    if (*newgroup && pInfo->totalInputRows > 0) {  // there are already processed current group data block
      pInfo->existNewGroupBlock = pBlock;
      *newgroup = false;

      // Fill the previous group data block, before handle the data block of new group.
      // Close the fill operation for previous group data block
      taosFillSetStartInfo(pInfo->pFillInfo, 0, pRuntimeEnv->pQueryAttr->window.ekey);
    } else {
      if (pBlock == NULL) {
        if (pInfo->totalInputRows == 0) {
          pOperator->status = OP_EXEC_DONE;
          return NULL;
        }

        taosFillSetStartInfo(pInfo->pFillInfo, 0, pRuntimeEnv->pQueryAttr->window.ekey);
      } else {
        pInfo->totalInputRows += pBlock->info.rows;
        taosFillSetStartInfo(pInfo->pFillInfo, pBlock->info.rows, pBlock->info.window.ekey);
        taosFillSetInputDataBlock(pInfo->pFillInfo, pBlock);
      }
    }

    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pRuntimeEnv->resultInfo.capacity, pInfo->p);

    // current group has no more result to return
    if (pInfo->pRes->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pInfo->pRes->info.rows > pRuntimeEnv->resultInfo.threshold || pBlock == NULL || (!pInfo->multigroupResult)) {
        return pInfo->pRes;
      }

      doHandleRemainBlockFromNewGroup(pInfo, pRuntimeEnv, newgroup);
      if (pInfo->pRes->info.rows > pRuntimeEnv->resultInfo.threshold || pBlock == NULL) {
        return pInfo->pRes;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      assert(pBlock != NULL);
      doHandleRemainBlockForNewGroupImpl(pInfo, pRuntimeEnv, newgroup);

      if (pInfo->pRes->info.rows > pRuntimeEnv->resultInfo.threshold) {
        return pInfo->pRes;
      }
    } else {
      return NULL;
    }
  }
}

// todo set the attribute of query scan count
static int32_t getNumOfScanTimes(STaskAttr* pQueryAttr) {
  for(int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
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
    for(int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      destroyOperatorInfo(pOperator->pDownstream[i]);
    }

    tfree(pOperator->pDownstream);
    pOperator->numOfDownstream = 0;
  }

  tfree(pOperator->info);
  tfree(pOperator);
}

int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx *pCtx, int32_t numOfOutput, const char* pKey) {
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  pAggSup->resultRowSize       = getResultRowSize(pCtx, numOfOutput);
  pAggSup->keyBuf              = calloc(1, sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES);
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
  tfree(pAggSup->keyBuf);
  taosHashCleanup(pAggSup->pResultRowHashTable);
  taosHashCleanup(pAggSup->pResultRowListSet);
  taosArrayDestroy(pAggSup->pResultRowArrayList);
  destroyDiskbasedBuf(pAggSup->pResultBuf);
}

static int32_t initAggInfo(SOptrBasicInfo* pBasicInfo, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols,
                           int32_t numOfRows, SSDataBlock* pResultBlock, const char* pkey) {
  pBasicInfo->pCtx = createSqlFunctionCtx_rv(pExprInfo, numOfCols, &pBasicInfo->rowCellInfoOffset);
  pBasicInfo->pRes = pResultBlock;
  pBasicInfo->capacity = numOfRows;

  doInitAggInfoSup(pAggSup, pBasicInfo->pCtx, numOfCols, pkey);
}

static STableQueryInfo* initTableQueryInfo(const STableGroupInfo* pTableGroupInfo) {
  STableQueryInfo* pTableQueryInfo = calloc(pTableGroupInfo->numOfTables, sizeof(STableQueryInfo));
  if (pTableQueryInfo == NULL) {
    return NULL;
  }

  int32_t index = 0;
  for(int32_t i = 0; i < taosArrayGetSize(pTableGroupInfo->pGroupList); ++i) {
    SArray* pa = taosArrayGetP(pTableGroupInfo->pGroupList, i);
    for(int32_t j = 0; j < taosArrayGetSize(pa); ++j) {
      STableKeyInfo* pk = taosArrayGet(pa, j);

      STableQueryInfo* pTQueryInfo = &pTableQueryInfo[index++];
      pTQueryInfo->uid        = pk->uid;
      pTQueryInfo->lastKey    = pk->lastKey;
      pTQueryInfo->groupIndex = i;
    }
  }

  STimeWindow win = {0, INT64_MAX};
  createTableQueryInfo(pTableQueryInfo, false, win);
  return pTableQueryInfo;
}

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock,
                                           SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo) {
  SAggOperatorInfo* pInfo = calloc(1, sizeof(SAggOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  //(int32_t)(getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery));
  int32_t numOfRows = 1;
  int32_t code = initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResultBlock, pTaskInfo->id.str);
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

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;
  _error:
  destroyAggOperatorInfo(pInfo, numOfCols);
  tfree(pInfo);
  tfree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

static void doDestroyBasicInfo(SOptrBasicInfo* pInfo, int32_t numOfOutput) {
  assert(pInfo != NULL);

  destroySqlFunctionCtx(pInfo->pCtx, numOfOutput);
  tfree(pInfo->rowCellInfoOffset);

  cleanupResultRowInfo(&pInfo->resultRowInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

void destroyBasicOperatorInfo(void* param, int32_t numOfOutput) {
  SOptrBasicInfo* pInfo = (SOptrBasicInfo*) param;
  doDestroyBasicInfo(pInfo, numOfOutput);
}

void destroyStateWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SStateWindowOperatorInfo* pInfo = (SStateWindowOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  tfree(pInfo->prevData);
}

void destroyAggOperatorInfo(void* param, int32_t numOfOutput) {
  SAggOperatorInfo* pInfo = (SAggOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

void destroyIntervalOperatorInfo(void* param, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = (STableIntervalOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  cleanupAggSup(&pInfo->aggSup);
}

void destroySWindowOperatorInfo(void* param, int32_t numOfOutput) {
  SSessionAggOperatorInfo* pInfo = (SSessionAggOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

void destroySFillOperatorInfo(void* param, int32_t numOfOutput) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*) param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  tfree(pInfo->p);
}

void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  tfree(pInfo->prevData);
}

void destroyProjectOperatorInfo(void* param, int32_t numOfOutput) {
  SProjectOperatorInfo* pInfo = (SProjectOperatorInfo*) param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

void destroyTagScanOperatorInfo(void* param, int32_t numOfOutput) {
  STagScanInfo* pInfo = (STagScanInfo*) param;
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

void destroyOrderOperatorInfo(void* param, int32_t numOfOutput) {
  SOrderOperatorInfo* pInfo = (SOrderOperatorInfo*) param;
  pInfo->pDataBlock = blockDataDestroy(pInfo->pDataBlock);

  taosArrayDestroy(pInfo->orderInfo);
}

static void destroyConditionOperatorInfo(void* param, int32_t numOfOutput) {
  SFilterOperatorInfo* pInfo = (SFilterOperatorInfo*) param;
  doDestroyFilterInfo(pInfo->pFilterInfo, pInfo->numOfFilterCols);
}

static void destroyDistinctOperatorInfo(void* param, int32_t numOfOutput) {
  SDistinctOperatorInfo* pInfo = (SDistinctOperatorInfo*) param;
  taosHashCleanup(pInfo->pSet);
  tfree(pInfo->buf);
  taosArrayDestroy(pInfo->pDistinctDataInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

static void destroySysTableScannerOperatorInfo(void* param, int32_t numOfOutput) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*) param;
  tsem_destroy(&pInfo->ready);
  blockDataDestroy(pInfo->pRes);

  if (pInfo->type == TSDB_MGMT_TABLE_TABLE) {
    metaCloseTbCursor(pInfo->pCur);
  }
}

void destroyExchangeOperatorInfo(void* param, int32_t numOfOutput) {
  SExchangeInfo* pExInfo = (SExchangeInfo*) param;
  taosArrayDestroy(pExInfo->pSources);
  taosArrayDestroy(pExInfo->pSourceDataInfo);
  if (pExInfo->pResult != NULL) {
    blockDataDestroy(pExInfo->pResult);
  }

  tsem_destroy(&pExInfo->ready);
}

SOperatorInfo* createMultiTableAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo) {
  SAggOperatorInfo* pInfo = calloc(1, sizeof(SAggOperatorInfo));

  int32_t numOfRows = 1;
  int32_t code = initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResBlock, pTaskInfo->id.str);
  pInfo->pTableQueryInfo = initTableQueryInfo(pTableGroupInfo);
  if (code != TSDB_CODE_SUCCESS || pInfo->pTableQueryInfo == NULL) {
    goto _error;
  }

  size_t  tableGroup = taosArrayGetSize(pTableGroupInfo->pGroupList);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, (int32_t)tableGroup);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "MultiTableAggregate";
  // pOperator->operatorType = OP_MultiTableAggregate;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;
  pOperator->pTaskInfo    = pTaskInfo;

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

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t num, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo) {
  SProjectOperatorInfo* pInfo = calloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->binfo.pRes = pResBlock;
  pInfo->binfo.pCtx = createSqlFunctionCtx_rv(pExprInfo, num, &pInfo->binfo.rowCellInfoOffset);
  if (pInfo->binfo.pCtx == NULL) {
    goto _error;
  }

//  initResultRowInfo(&pBInfo->resultRowInfo, 8);
//  setFunctionResultOutput(pBInfo, MAIN_SCAN);

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

  pOperator->pTaskInfo    = pTaskInfo;
  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_OUT_OF_MEMORY) {
    goto _error;
  }

  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SColumnInfo* extractColumnFilterInfo(SExprInfo* pExpr, int32_t numOfOutput, int32_t* numOfFilterCols) {
#if 0
  SColumnInfo* pCols = calloc(numOfOutput, sizeof(SColumnInfo));

  int32_t numOfFilter = 0;
  for(int32_t i = 0; i < numOfOutput; ++i) {
    if (pExpr[i].base.flist.numOfFilters > 0) {
      numOfFilter += 1;
    }

    pCols[i].type  = pExpr[i].base.resSchema.type;
    pCols[i].bytes = pExpr[i].base.resSchema.bytes;
    pCols[i].colId = pExpr[i].base.resSchema.colId;

    pCols[i].flist.numOfFilters = pExpr[i].base.flist.numOfFilters;
    if (pCols[i].flist.numOfFilters != 0) { 
      pCols[i].flist.filterInfo   = calloc(pCols[i].flist.numOfFilters, sizeof(SColumnFilterInfo));
      memcpy(pCols[i].flist.filterInfo, pExpr[i].base.flist.filterInfo, pCols[i].flist.numOfFilters * sizeof(SColumnFilterInfo));
    } else {
      // avoid runtime error
      pCols[i].flist.filterInfo   = NULL; 
    }
  }

  assert(numOfFilter > 0);

  *numOfFilterCols = numOfFilter;
  return pCols;
#endif

  return 0;
}

SOperatorInfo* createLimitOperatorInfo(SOperatorInfo* downstream, int32_t numOfDownstream, SLimit* pLimit, SExecTaskInfo* pTaskInfo) {
  ASSERT(numOfDownstream == 1);
  SLimitOperatorInfo* pInfo = calloc(1, sizeof(SLimitOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->limit            = *pLimit;
  pInfo->currentOffset    = pLimit->offset;

  pOperator->name         = "LimitOperator";
//  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_LIMIT;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->_openFn      = operatorDummyOpenFn;
  pOperator->getNextFn    = doLimit;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;
  int32_t code = appendDownstream(pOperator, &downstream, 1);

  return pOperator;
  _error:
  tfree(pInfo);
  tfree(pOperator);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, SInterval* pInterval,
                                          const STableGroupInfo* pTableGroupInfo, SExecTaskInfo* pTaskInfo) {
  STableIntervalOperatorInfo* pInfo = calloc(1, sizeof(STableIntervalOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->order     = TSDB_ORDER_ASC;
  pInfo->precision = TSDB_TIME_PRECISION_MICRO;
  pInfo->win       = pTaskInfo->window;
  pInfo->interval  = *pInterval;

  pInfo->win.skey = INT64_MIN;
  pInfo->win.ekey = INT64_MAX;

  int32_t numOfRows = 4096;
  int32_t code = initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, numOfRows, pResBlock, pTaskInfo->id.str);
//  pInfo->pTableQueryInfo = initTableQueryInfo(pTableGroupInfo);
  if (code != TSDB_CODE_SUCCESS/* || pInfo->pTableQueryInfo == NULL*/) {
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
  pOperator->getNextFn    = doIntervalAgg;
  pOperator->closeFn      = destroyIntervalOperatorInfo;

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

  _error:
  destroyIntervalOperatorInfo(pInfo, numOfCols);
  tfree(pInfo);
  tfree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

SOperatorInfo* createAllTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = calloc(1, sizeof(STableIntervalOperatorInfo));

  pInfo->binfo.pCtx = createSqlFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "AllTimeIntervalAggOperator";
//  pOperator->operatorType = OP_AllTimeWindow;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->getNextFn = doAllIntervalAgg;
  pOperator->closeFn = destroyBasicOperatorInfo;

    int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

SOperatorInfo* createStatewindowOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SStateWindowOperatorInfo* pInfo = calloc(1, sizeof(SStateWindowOperatorInfo));
  pInfo->colIndex   = -1;
  pInfo->reptScan   = false;
  pInfo->binfo.pCtx = createSqlFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "StateWindowOperator";
//  pOperator->operatorType = OP_StateWindow;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->getNextFn = doStateWindowAgg;
  pOperator->closeFn = destroyStateWindowOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo) {
  SSessionAggOperatorInfo* pInfo = calloc(1, sizeof(SSessionAggOperatorInfo));
  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t code = doInitAggInfoSup(&pInfo->aggSup, pInfo->binfo.pCtx, numOfCols, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  pInfo->binfo.pRes       = pResBlock;
  pInfo->prevTs           = INT64_MIN;
  pInfo->reptScan         = false;
  pOperator->name         = "SessionWindowAggOperator";
//  pOperator->operatorType = OP_SessionWindow;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;

  pOperator->info         = pInfo;
  pOperator->getNextFn    = doSessionWindowAgg;
  pOperator->closeFn      = destroySWindowOperatorInfo;
  pOperator->pTaskInfo    = pTaskInfo;

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  if (pInfo != NULL) {
    destroySWindowOperatorInfo(pInfo, numOfCols);
  }

  tfree(pInfo);
  tfree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

SOperatorInfo* createMultiTableTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = calloc(1, sizeof(STableIntervalOperatorInfo));

  pInfo->binfo.pCtx = createSqlFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "MultiTableTimeIntervalOperator";
//  pOperator->operatorType = OP_MultiTableTimeInterval;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->getNextFn = doSTableIntervalAgg;
  pOperator->closeFn = destroyBasicOperatorInfo;

    int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

SOperatorInfo* createAllMultiTableTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput) {
  STableIntervalOperatorInfo* pInfo = calloc(1, sizeof(STableIntervalOperatorInfo));

  pInfo->binfo.pCtx = createSqlFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "AllMultiTableTimeIntervalOperator";
//  pOperator->operatorType = OP_AllMultiTableTimeInterval;
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;

  pOperator->getNextFn = doAllSTableIntervalAgg;
  pOperator->closeFn = destroyBasicOperatorInfo;

    int32_t code = appendDownstream(pOperator, &downstream, 1);

  return pOperator;
}

SOperatorInfo* createGroupbyOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = calloc(1, sizeof(SGroupbyOperatorInfo));
  pInfo->colIndex = -1;  // group by column index


  pInfo->binfo.pCtx = createSqlFunctionCtx(pRuntimeEnv, pExpr, numOfOutput, &pInfo->binfo.rowCellInfoOffset);

  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;

  pQueryAttr->resultRowSize = (pQueryAttr->resultRowSize *
      (int32_t)(getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery)));

  pInfo->binfo.pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "GroupbyAggOperator";
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
//  pOperator->operatorType = OP_Groupby;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->getNextFn = hashGroupbyAggregate;
  pOperator->closeFn = destroyGroupbyOperatorInfo;

    int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

SOperatorInfo* createFillOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput, bool multigroupResult) {
  SFillOperatorInfo* pInfo = calloc(1, sizeof(SFillOperatorInfo));
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);
  pInfo->multigroupResult = multigroupResult;

  {
    STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
    struct SFillColInfo* pColInfo = createFillColInfo(pExpr, numOfOutput, pQueryAttr->fillVal);
    STimeWindow w = TSWINDOW_INITIALIZER;

    TSKEY sk = TMIN(pQueryAttr->window.skey, pQueryAttr->window.ekey);
    TSKEY ek = TMAX(pQueryAttr->window.skey, pQueryAttr->window.ekey);
//    getAlignQueryTimeWindow(pQueryAttr, pQueryAttr->window.skey, sk, ek, &w);

    pInfo->pFillInfo =
        taosCreateFillInfo(pQueryAttr->order.order, w.skey, 0, (int32_t)pRuntimeEnv->resultInfo.capacity, numOfOutput,
                           pQueryAttr->interval.sliding, pQueryAttr->interval.slidingUnit,
                           (int8_t)pQueryAttr->precision, pQueryAttr->fillType, pColInfo, pRuntimeEnv->qinfo);

    pInfo->p = calloc(numOfOutput, POINTER_BYTES);
  }

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "FillOperator";
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
//  pOperator->operatorType = OP_Fill;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->getNextFn = doFill;
  pOperator->closeFn = destroySFillOperatorInfo;

    int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

SOperatorInfo* createSLimitOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput, void* pMerger, bool multigroupResult) {
  SSLimitOperatorInfo* pInfo = calloc(1, sizeof(SSLimitOperatorInfo));

//  pInfo->orderColumnList = getResultGroupCheckColumns(pQueryAttr);
//  pInfo->slimit          = pQueryAttr->slimit;
//  pInfo->limit           = pQueryAttr->limit;
//  pInfo->capacity        = pRuntimeEnv->resultInfo.capacity;
//  pInfo->threshold       = (int64_t)(pInfo->capacity * 0.8);
//  pInfo->currentOffset   = pQueryAttr->limit.offset;
//  pInfo->currentGroupOffset = pQueryAttr->slimit.offset;
  pInfo->multigroupResult= multigroupResult;

  // TODO refactor
  int32_t len = 0;
  for(int32_t i = 0; i < numOfOutput; ++i) {
    len += pExpr[i].base.resSchema.bytes;
  }

  int32_t numOfCols = (pInfo->orderColumnList != NULL)? (int32_t) taosArrayGetSize(pInfo->orderColumnList):0;
  pInfo->prevRow = calloc(1, (POINTER_BYTES * numOfCols + len));

  int32_t offset = POINTER_BYTES * numOfCols;
  for(int32_t i = 0; i < numOfCols; ++i) {
    pInfo->prevRow[i] = (char*)pInfo->prevRow + offset;

    SColIndex* index = taosArrayGet(pInfo->orderColumnList, i);
    offset += pExpr[index->colIndex].base.resSchema.bytes;
  }

  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));

  pOperator->name         = "SLimitOperator";
  // pOperator->operatorType = OP_SLimit;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
//  pOperator->exec         = doSLimit;
  pOperator->info         = pInfo;
  pOperator->closeFn = destroySlimitOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

static SSDataBlock* doTagScan(SOperatorInfo *pOperator, bool* newgroup) {
#if 0
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STaskRuntimeEnv* pRuntimeEnv = pOperator->pRuntimeEnv;
  int32_t maxNumOfTables = (int32_t)pRuntimeEnv->resultInfo.capacity;

  STagScanInfo *pInfo = pOperator->info;
  SSDataBlock  *pRes = pInfo->pRes;
  *newgroup = false;

  int32_t count = 0;
  SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);

  int32_t functionId = getExprFunctionId(&pOperator->pExpr[0]);
  if (functionId == FUNCTION_TID_TAG) { // return the tags & table Id
    STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
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
  return 0;
}

SOperatorInfo* createTagScanOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput) {
  STagScanInfo* pInfo = calloc(1, sizeof(STagScanInfo));
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, pRuntimeEnv->resultInfo.capacity);

  size_t numOfGroup = GET_NUM_OF_TABLEGROUP(pRuntimeEnv);
  assert(numOfGroup == 0 || numOfGroup == 1);

  pInfo->totalTables = pRuntimeEnv->tableqinfoGroupInfo.numOfTables;
  pInfo->curPos = 0;

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "SeqTableTagScan";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->getNextFn = doTagScan;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->closeFn = destroyTagScanOperatorInfo;

  return pOperator;
}
static bool initMultiDistinctInfo(SDistinctOperatorInfo *pInfo, SOperatorInfo* pOperator, SSDataBlock *pBlock) {
  if (taosArrayGetSize(pInfo->pDistinctDataInfo) == pOperator->numOfOutput) {
     // distinct info already inited  
    return true;
  }
  for (int i = 0; i < pOperator->numOfOutput; i++) {
//    pInfo->totalBytes += pOperator->pExpr[i].base.colBytes;
  }
  for (int i = 0; i < pOperator->numOfOutput; i++) {
    int numOfBlock = (int)(taosArrayGetSize(pBlock->pDataBlock));
    assert(i < numOfBlock);
    for (int j = 0; j < numOfBlock; j++) {
      SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, j);
      if (pColDataInfo->info.colId == pOperator->pExpr[i].base.resSchema.colId) {
        SDistinctDataInfo item = {.index = j, .type = pColDataInfo->info.type, .bytes = pColDataInfo->info.bytes};
        taosArrayInsert(pInfo->pDistinctDataInfo, i, &item);
      }
    }
  }
  pInfo->totalBytes += (int32_t)strlen(MULTI_KEY_DELIM) * (pOperator->numOfOutput);
  pInfo->buf        =  calloc(1, pInfo->totalBytes);
  return  taosArrayGetSize(pInfo->pDistinctDataInfo) == pOperator->numOfOutput ? true : false;
}

static void buildMultiDistinctKey(SDistinctOperatorInfo *pInfo, SSDataBlock *pBlock, int32_t rowId) {
  char *p = pInfo->buf;
  memset(p, 0, pInfo->totalBytes); 

  for (int i = 0; i < taosArrayGetSize(pInfo->pDistinctDataInfo); i++) {
    SDistinctDataInfo* pDistDataInfo = (SDistinctDataInfo *)taosArrayGet(pInfo->pDistinctDataInfo, i); 
    SColumnInfoData*   pColDataInfo = taosArrayGet(pBlock->pDataBlock, pDistDataInfo->index);
    char *val = ((char *)pColDataInfo->pData) + pColDataInfo->info.bytes * rowId;
    if (isNull(val, pDistDataInfo->type)) { 
      p += pDistDataInfo->bytes; 
      continue;
    }
    if (IS_VAR_DATA_TYPE(pDistDataInfo->type)) {
      memcpy(p, varDataVal(val), varDataLen(val));
      p += varDataLen(val);
    } else {
      memcpy(p, val, pDistDataInfo->bytes);
      p += pDistDataInfo->bytes;
    }
    memcpy(p, MULTI_KEY_DELIM, strlen(MULTI_KEY_DELIM));
    p += strlen(MULTI_KEY_DELIM);
  }
}

static SSDataBlock* hashDistinct(SOperatorInfo *pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SDistinctOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pRes = pInfo->pRes;

  pRes->info.rows = 0;
  SSDataBlock* pBlock = NULL;
   
  while(1) {
    publishOperatorProfEvent(pOperator->pDownstream[0], QUERY_PROF_BEFORE_OPERATOR_EXEC);
    pBlock = pOperator->pDownstream[0]->getNextFn(pOperator->pDownstream[0], newgroup);
    publishOperatorProfEvent(pOperator->pDownstream[0], QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      doSetOperatorCompleted(pOperator);
      break;
    }
    if (!initMultiDistinctInfo(pInfo, pOperator, pBlock)) {
      doSetOperatorCompleted(pOperator);
      break;
    }
    // ensure result output buf 
    if (pRes->info.rows + pBlock->info.rows > pInfo->outputCapacity) {
      int32_t newSize = pRes->info.rows + pBlock->info.rows;
      for (int i = 0; i < taosArrayGetSize(pRes->pDataBlock); i++) {
        SColumnInfoData*   pResultColInfoData = taosArrayGet(pRes->pDataBlock, i);
        SDistinctDataInfo* pDistDataInfo = taosArrayGet(pInfo->pDistinctDataInfo,  i);
        char* tmp = realloc(pResultColInfoData->pData, newSize * pDistDataInfo->bytes);
        if (tmp == NULL) {
          return NULL;
        } else {
          pResultColInfoData->pData = tmp;
        }
      }
      pInfo->outputCapacity = newSize;
    }

    for (int32_t i = 0; i < pBlock->info.rows; i++) {
      buildMultiDistinctKey(pInfo, pBlock, i);
      if (taosHashGet(pInfo->pSet, pInfo->buf, pInfo->totalBytes) == NULL) {
        int32_t dummy;
        taosHashPut(pInfo->pSet, pInfo->buf, pInfo->totalBytes, &dummy, sizeof(dummy));
        for (int j = 0; j < taosArrayGetSize(pRes->pDataBlock); j++) {
          SDistinctDataInfo* pDistDataInfo = taosArrayGet(pInfo->pDistinctDataInfo, j);  // distinct meta info
          SColumnInfoData*   pColInfoData = taosArrayGet(pBlock->pDataBlock, pDistDataInfo->index); //src
          SColumnInfoData*   pResultColInfoData = taosArrayGet(pRes->pDataBlock, j);  // dist 

          char* val = ((char*)pColInfoData->pData) + pDistDataInfo->bytes * i;
          char *start = pResultColInfoData->pData +  pDistDataInfo->bytes * pInfo->pRes->info.rows; 
          memcpy(start, val, pDistDataInfo->bytes);
        }
        pRes->info.rows += 1;
      } 
    }

    if (pRes->info.rows >= pInfo->threshold) {
      break;
    }
  }
  return (pInfo->pRes->info.rows > 0)? pInfo->pRes:NULL;
}

SOperatorInfo* createDistinctOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfOutput) {
  SDistinctOperatorInfo* pInfo = calloc(1, sizeof(SDistinctOperatorInfo));
  pInfo->totalBytes      = 0;
  pInfo->buf             = NULL;
  pInfo->threshold       = tsMaxNumOfDistinctResults; // distinct result threshold
  pInfo->outputCapacity  = 4096;
  pInfo->pDistinctDataInfo = taosArrayInit(numOfOutput, sizeof(SDistinctDataInfo)); 
  pInfo->pSet = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  pInfo->pRes = createOutputBuf(pExpr, numOfOutput, (int32_t) pInfo->outputCapacity);
  

  SOperatorInfo* pOperator = calloc(1, sizeof(SOperatorInfo));
  pOperator->name         = "DistinctOperator";
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
//  pOperator->operatorType = OP_Distinct;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->info         = pInfo;
  pOperator->pRuntimeEnv  = pRuntimeEnv;
  pOperator->getNextFn = hashDistinct;
  pOperator->pExpr        = pExpr; 
  pOperator->closeFn = destroyDistinctOperatorInfo;

    int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;
}

static int32_t getColumnIndexInSource(SQueriedTableInfo *pTableInfo, SExprBasicInfo *pExpr, SColumnInfo* pTagCols) {
  int32_t j = 0;

  if (TSDB_COL_IS_TAG(pExpr->pParam[0].pCol->type)) {
    if (pExpr->pParam[0].pCol->colId == TSDB_TBNAME_COLUMN_INDEX) {
      return TSDB_TBNAME_COLUMN_INDEX;
    }

    while(j < pTableInfo->numOfTags) {
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

bool validateExprColumnInfo(SQueriedTableInfo *pTableInfo, SExprBasicInfo *pExpr, SColumnInfo* pTagCols) {
  int32_t j = getColumnIndexInSource(pTableInfo, pExpr, pTagCols);
  return j != INT32_MIN;
}

static bool validateQueryTableCols(SQueriedTableInfo* pTableInfo, SExprBasicInfo** pExpr, int32_t numOfOutput,
                                   SColumnInfo* pTagCols, void* pMsg) {
  int32_t numOfTotal = pTableInfo->numOfCols + pTableInfo->numOfTags;
  if (pTableInfo->numOfCols < 0 || pTableInfo->numOfTags < 0 || numOfTotal > TSDB_MAX_COLUMNS) {
    //qError("qmsg:%p illegal value of numOfCols %d numOfTags:%d", pMsg, pTableInfo->numOfCols, pTableInfo->numOfTags);
    return false;
  }

  if (numOfTotal == 0) {  // table total columns are not required.
//    for(int32_t i = 0; i < numOfOutput; ++i) {
//      SExprBasicInfo* p = pExpr[i];
//      if ((p->functionId == FUNCTION_TAGPRJ) ||
//          (p->functionId == FUNCTION_TID_TAG && p->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) ||
//          (p->functionId == FUNCTION_COUNT && p->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) ||
//          (p->functionId == FUNCTION_BLKINFO)) {
//        continue;
//      }
//
//      return false;
//    }
  }

  for(int32_t i = 0; i < numOfOutput; ++i) {
    if (!validateExprColumnInfo(pTableInfo, pExpr[i], pTagCols)) {
      return TSDB_CODE_QRY_INVALID_MSG;
    }
  }

  return true;
}

static int32_t deserializeColFilterInfo(SColumnFilterInfo* pColFilters, int16_t numOfFilters, char** pMsg) {
  for (int32_t f = 0; f < numOfFilters; ++f) {
    SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)(*pMsg);

    SColumnFilterInfo *pColFilter = &pColFilters[f];
    pColFilter->filterstr = htons(pFilterMsg->filterstr);

    (*pMsg) += sizeof(SColumnFilterInfo);

    if (pColFilter->filterstr) {
      pColFilter->len = htobe64(pFilterMsg->len);

      pColFilter->pz = (int64_t)calloc(1, (size_t)(pColFilter->len + 1 * TSDB_NCHAR_SIZE)); // note: null-terminator
      if (pColFilter->pz == 0) {
        return TSDB_CODE_QRY_OUT_OF_MEMORY;
      }

      memcpy((void *)pColFilter->pz, (*pMsg), (size_t)pColFilter->len);
      (*pMsg) += (pColFilter->len + 1);
    } else {
      pColFilter->lowerBndi = htobe64(pFilterMsg->lowerBndi);
      pColFilter->upperBndi = htobe64(pFilterMsg->upperBndi);
    }

    pColFilter->lowerRelOptr = htons(pFilterMsg->lowerRelOptr);
    pColFilter->upperRelOptr = htons(pFilterMsg->upperRelOptr);
  }

  return TSDB_CODE_SUCCESS;
}

static SResSchema createResSchema(int32_t type, int32_t bytes, int32_t slotId, int32_t scale, int32_t precision, const char* name) {
  SResSchema s = {0};
  s.scale = scale;
  s.precision = precision;
  s.type  = type;
  s.bytes = bytes;
  s.colId = slotId;
  strncpy(s.name, name, tListLen(s.name));

  return s;
}

SExprInfo* createExprInfo(SNodeList* pNodeList, int32_t* numOfExprs) {
  int32_t numOfFuncs = LIST_LENGTH(pNodeList);

  *numOfExprs = numOfFuncs;
  SExprInfo* pExprs = calloc(numOfFuncs, sizeof(SExprInfo));

  for(int32_t i = 0; i < numOfFuncs; ++i) {
    SExprInfo* pExp = &pExprs[i];

    pExp->pExpr = calloc(1, sizeof(tExprNode));
    pExp->pExpr->_function.num = 1;

    pExp->base.pParam = calloc(1, sizeof(SFunctParam));
    pExp->base.numOfParams = 1;

    pExp->base.pParam[0].pCol = calloc(1, sizeof(SColumn));
    SColumn* pCol = pExp->base.pParam[0].pCol;

    STargetNode* pTargetNode = (STargetNode*) nodesListGetNode(pNodeList, i);
    ASSERT(pTargetNode->slotId == i);

    SFunctionNode* pFuncNode = (SFunctionNode*)pTargetNode->pExpr;

    SDataType *pType = &pFuncNode->node.resType;
    pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId,
                                           pType->scale, pType->precision, pFuncNode->node.aliasName);

    pExp->pExpr->_function.pFunctNode = pFuncNode;
    strncpy(pExp->pExpr->_function.functionName, pFuncNode->functionName, tListLen(pExp->pExpr->_function.functionName));

    // TODO: value parameter needs to be handled
    int32_t numOfParam = LIST_LENGTH(pFuncNode->pParameterList);
    for(int32_t j = 0; j < numOfParam; ++j) {
      SNode* p1 = nodesListGetNode(pFuncNode->pParameterList, j);
      SColumnNode* pcn  = (SColumnNode*)p1;

      pCol->slotId      = pcn->slotId;
      pCol->bytes       = pcn->node.resType.bytes;
      pCol->type        = pcn->node.resType.type;
      pCol->scale       = pcn->node.resType.scale;
      pCol->precision   = pcn->node.resType.precision;
      pCol->dataBlockId = pcn->dataBlockId;
    }
  }

  return pExprs;
}

static SExecTaskInfo* createExecTaskInfo(uint64_t queryId, uint64_t taskId) {
  SExecTaskInfo* pTaskInfo = calloc(1, sizeof(SExecTaskInfo));
  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);

  pTaskInfo->cost.created = taosGetTimestampMs();
  pTaskInfo->id.queryId = queryId;

  char* p = calloc(1, 128);
  snprintf(p, 128, "TID:0x%"PRIx64" QID:0x%"PRIx64, taskId, queryId);
  pTaskInfo->id.str = strdup(p);

  return pTaskInfo;
}

static tsdbReaderT doCreateDataReader(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle, uint64_t queryId, uint64_t taskId);

static int32_t doCreateTableGroup(void* metaHandle, int32_t tableType, uint64_t tableUid, STableGroupInfo* pGroupInfo, uint64_t queryId, uint64_t taskId);
static SArray* extractTableIdList(const STableGroupInfo* pTableGroupInfo);
static SArray* extractScanColumnId(SNodeList* pNodeList);

SOperatorInfo* doCreateOperatorTreeNode(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, uint64_t queryId, uint64_t taskId, STableGroupInfo* pTableGroupInfo) {
  if (nodeType(pPhyNode) == QUERY_NODE_PHYSICAL_PLAN_PROJECT) { // ignore the project node
    pPhyNode = nodesListGetNode(pPhyNode->pChildren, 0);
  }

  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(pPhyNode)) {
      SScanPhysiNode* pScanPhyNode = (SScanPhysiNode*)pPhyNode;

      size_t      numOfCols = LIST_LENGTH(pScanPhyNode->pScanCols);
      tsdbReaderT pDataReader = doCreateDataReader((STableScanPhysiNode*)pPhyNode, pHandle, (uint64_t)queryId, taskId);

      int32_t code = doCreateTableGroup(pHandle->meta, pScanPhyNode->tableType, pScanPhyNode->uid, pTableGroupInfo, queryId, taskId);
      return createTableScanOperatorInfo(pDataReader, pScanPhyNode->order, numOfCols, pScanPhyNode->count,
                                         pScanPhyNode->reverse, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == nodeType(pPhyNode)) {
      SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pPhyNode;
      SSDataBlock* pResBlock = createOutputBuf_rv1(pExchange->node.pOutputDataBlockDesc);
      return createExchangeOperatorInfo(pExchange->pSrcEndPoints, pResBlock, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == nodeType(pPhyNode)) {
      SScanPhysiNode* pScanPhyNode = (SScanPhysiNode*)pPhyNode;  // simple child table.

      STableGroupInfo groupInfo = {0};

      int32_t code = doCreateTableGroup(pHandle->meta, pScanPhyNode->tableType, pScanPhyNode->uid, &groupInfo, queryId, taskId);
      SArray* tableIdList = extractTableIdList(&groupInfo);

      SSDataBlock* pResBlock = createOutputBuf_rv1(pScanPhyNode->node.pOutputDataBlockDesc);
      SArray* colList = extractScanColumnId(pScanPhyNode->pScanCols);

      SOperatorInfo* pOperator = createStreamScanOperatorInfo(pHandle->reader, pResBlock, colList, tableIdList, pTaskInfo);

      taosArrayDestroy(tableIdList);
      return pOperator;
    }
  }

  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == nodeType(pPhyNode)) {
    size_t size = LIST_LENGTH(pPhyNode->pChildren);
    assert(size == 1);

    for (int32_t i = 0; i < size; ++i) {
      SPhysiNode*    pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
      SOperatorInfo* op = doCreateOperatorTreeNode(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableGroupInfo);

      int32_t    num = 0;
      SExprInfo* pExprInfo = createExprInfo(((SProjectPhysiNode*)pPhyNode)->pProjections, &num);
      SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);
      return createProjectOperatorInfo(op, pExprInfo, num, pResBlock, pTaskInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_AGG == nodeType(pPhyNode)) {
    size_t size = LIST_LENGTH(pPhyNode->pChildren);
    assert(size == 1);

    for (int32_t i = 0; i < size; ++i) {
      SPhysiNode*    pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
      SOperatorInfo* op = doCreateOperatorTreeNode(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableGroupInfo);

      int32_t num = 0;
      SExprInfo* pExprInfo = createExprInfo(((SAggPhysiNode*)pPhyNode)->pAggFuncs, &num);
      SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);
      return createAggregateOperatorInfo(op, pExprInfo, num, pResBlock, pTaskInfo, pTableGroupInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_INTERVAL == nodeType(pPhyNode)) {
    size_t size = LIST_LENGTH(pPhyNode->pChildren);
    assert(size == 1);

    for (int32_t i = 0; i < size; ++i) {
      SPhysiNode*    pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
      SOperatorInfo* op = doCreateOperatorTreeNode(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableGroupInfo);

      SIntervalPhysiNode* pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;

      int32_t num = 0;
      SExprInfo* pExprInfo = createExprInfo(pIntervalPhyNode->pFuncs, &num);
      SSDataBlock* pResBlock = createOutputBuf_rv1(pPhyNode->pOutputDataBlockDesc);

      SInterval interval = {.interval = pIntervalPhyNode->interval, .sliding = pIntervalPhyNode->sliding, .intervalUnit = 'a', .slidingUnit = 'a'};
      return createIntervalOperatorInfo(op, pExprInfo, num, pResBlock, &interval, pTableGroupInfo, pTaskInfo);
    }
  } /*else if (pPhyNode->info.type == OP_MultiTableAggregate) {
    size_t size = taosArrayGetSize(pPhyNode->pChildren);
    assert(size == 1);

    for (int32_t i = 0; i < size; ++i) {
      SPhysiNode*      pChildNode = taosArrayGetP(pPhyNode->pChildren, i);
      SOperatorInfo* op = doCreateOperatorTreeNode(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableGroupInfo);
      return createMultiTableAggOperatorInfo(op, pPhyNode->pTargets, pTaskInfo, pTableGroupInfo);
    }
  }*/
}

static tsdbReaderT createDataReaderImpl(STableScanPhysiNode* pTableScanNode, STableGroupInfo* pGroupInfo, void* readHandle, uint64_t queryId, uint64_t taskId) {
  STsdbQueryCond cond = {.loadExternalRows = false};

  cond.order     = pTableScanNode->scan.order;
  cond.numOfCols = LIST_LENGTH(pTableScanNode->scan.pScanCols);
  cond.colList   = calloc(cond.numOfCols, sizeof(SColumnInfo));
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
  size_t numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(int16_t));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for(int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*) nodesListGetNode(pNodeList, i);
    SColumnNode* pColNode = (SColumnNode*) pNode->pExpr;
    taosArrayPush(pList, &pColNode->colId);
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

tsdbReaderT doCreateDataReader(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle, uint64_t queryId, uint64_t taskId) {
  STableGroupInfo groupInfo = {0};

  uint64_t uid = pTableScanNode->scan.uid;
  int32_t code = doCreateTableGroup(pHandle->meta, pTableScanNode->scan.tableType, uid, &groupInfo, queryId, taskId);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (groupInfo.numOfTables == 0) {
    code = 0;
    qDebug("no table qualified for query, TID:0x%"PRIx64", QID:0x%"PRIx64, taskId, queryId);
    goto _error;
  }

  return createDataReaderImpl(pTableScanNode, &groupInfo, pHandle->reader, queryId, taskId);

  _error:
  terrno = code;
  return NULL;
}

int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId) {
  uint64_t queryId = pPlan->id.queryId;

  int32_t code = TSDB_CODE_SUCCESS;
  *pTaskInfo = createExecTaskInfo(queryId, taskId);
  if (*pTaskInfo == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _complete;
  }

  STableGroupInfo group = {0};
  (*pTaskInfo)->pRoot = doCreateOperatorTreeNode(pPlan->pNode, *pTaskInfo, pHandle, queryId, taskId, &group);
  if ((*pTaskInfo)->pRoot == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _complete;
  }

  return code;

_complete:
  tfree(*pTaskInfo);

  terrno = code;
  return code;
}

int32_t cloneExprFilterInfo(SColumnFilterInfo **dst, SColumnFilterInfo* src, int32_t filterNum) {
  if (filterNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  *dst = calloc(filterNum, sizeof(*src));
  if (*dst == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  memcpy(*dst, src, sizeof(*src) * filterNum);

  for (int32_t i = 0; i < filterNum; i++) {
    if ((*dst)[i].filterstr && dst[i]->len > 0) {
      void *pz = calloc(1, (size_t)(*dst)[i].len + 1);

      if (pz == NULL) {
        if (i == 0) {
          free(*dst);
        } else {
          freeColumnFilterInfo(*dst, i);
        }

        return TSDB_CODE_QRY_OUT_OF_MEMORY;
      }

      memcpy(pz, (void *)src->pz, (size_t)src->len + 1);

      (*dst)[i].pz = (int64_t)pz;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t updateOutputBufForTopBotQuery(SQueriedTableInfo* pTableInfo, SColumnInfo* pTagCols, SExprInfo* pExprs, int32_t numOfOutput, int32_t tagLen, bool superTable) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    int16_t functId = getExprFunctionId(&pExprs[i]);

    if (functId == FUNCTION_TOP || functId == FUNCTION_BOTTOM) {
      int32_t j = getColumnIndexInSource(pTableInfo, &pExprs[i].base, pTagCols);
      if (j < 0 || j >= pTableInfo->numOfCols) {
        return TSDB_CODE_QRY_INVALID_MSG;
      } else {
        SColumnInfo* pCol = &pTableInfo->colList[j];
//        int32_t ret = getResultDataInfo(pCol->type, pCol->bytes, functId, (int32_t)pExprs[i].base.param[0].i,
//                                        &pExprs[i].base.resSchema.type, &pExprs[i].base.resSchema.bytes, &pExprs[i].base.interBytes, tagLen, superTable, NULL);
//        assert(ret == TSDB_CODE_SUCCESS);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

// TODO tag length should be passed from client, refactor
int32_t createQueryFilter(char *data, uint16_t len, SFilterInfo** pFilters) {
  tExprNode* expr = NULL;
  
  TRY(TSDB_MAX_TAG_CONDITIONS) {
    expr = exprTreeFromBinary(data, len);
  } CATCH( code ) {
    CLEANUP_EXECUTE();
    return code;
  } END_TRY

  if (expr == NULL) {
    //qError("failed to create expr tree");
    return TSDB_CODE_QRY_APP_ERROR;
  }

//  int32_t ret = filterInitFromTree(expr, pFilters, 0);
//  tExprTreeDestroy(expr, NULL);

//  return ret;
}

//int32_t doCreateFilterInfo(SColumnInfo* pCols, int32_t numOfCols, int32_t numOfFilterCols, SSingleColumnFilterInfo** pFilterInfo, uint64_t qId) {
//  *pFilterInfo = calloc(1, sizeof(SSingleColumnFilterInfo) * numOfFilterCols);
//  if (*pFilterInfo == NULL) {
//    return TSDB_CODE_QRY_OUT_OF_MEMORY;
//  }
//
//  for (int32_t i = 0, j = 0; i < numOfCols; ++i) {
//    if (pCols[i].flist.numOfFilters > 0) {
//      SSingleColumnFilterInfo* pFilter = &((*pFilterInfo)[j]);
//
//      memcpy(&pFilter->info, &pCols[i], sizeof(SColumnInfo));
//      pFilter->info = pCols[i];
//
//      pFilter->numOfFilters = pCols[i].flist.numOfFilters;
//      pFilter->pFilters = calloc(pFilter->numOfFilters, sizeof(SColumnFilterElem));
//      if (pFilter->pFilters == NULL) {
//        return TSDB_CODE_QRY_OUT_OF_MEMORY;
//      }
//
//      for (int32_t f = 0; f < pFilter->numOfFilters; ++f) {
//        SColumnFilterElem* pSingleColFilter = &pFilter->pFilters[f];
//        pSingleColFilter->filterInfo = pCols[i].flist.filterInfo[f];
//
//        int32_t lower = pSingleColFilter->filterInfo.lowerRelOptr;
//        int32_t upper = pSingleColFilter->filterInfo.upperRelOptr;
//        if (lower == TSDB_RELATION_INVALID && upper == TSDB_RELATION_INVALID) {
//          //qError("QInfo:0x%"PRIx64" invalid filter info", qId);
//          return TSDB_CODE_QRY_INVALID_MSG;
//        }
//
//        pSingleColFilter->fp = getFilterOperator(lower, upper);
//        if (pSingleColFilter->fp == NULL) {
//          //qError("QInfo:0x%"PRIx64" invalid filter info", qId);
//          return TSDB_CODE_QRY_INVALID_MSG;
//        }
//
//        pSingleColFilter->bytes = pCols[i].bytes;
//
//        if (lower == TSDB_RELATION_IN) {
////          buildFilterSetFromBinary(&pSingleColFilter->q, (char *)(pSingleColFilter->filterInfo.pz), (int32_t)(pSingleColFilter->filterInfo.len));
//        }
//      }
//
//      j++;
//    }
//  }
//
//  return TSDB_CODE_SUCCESS;
//}

void* doDestroyFilterInfo(SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols) {
//  for (int32_t i = 0; i < numOfFilterCols; ++i) {
//    if (pFilterInfo[i].numOfFilters > 0) {
//      if (pFilterInfo[i].pFilters->filterInfo.lowerRelOptr == TSDB_RELATION_IN) {
//        taosHashCleanup((SHashObj *)(pFilterInfo[i].pFilters->q));
//      }
//      tfree(pFilterInfo[i].pFilters);
//    }
//  }
//
//  tfree(pFilterInfo);
  return NULL;
}

int32_t createFilterInfo(STaskAttr* pQueryAttr, uint64_t qId) {
  for (int32_t i = 0; i < pQueryAttr->numOfCols; ++i) {
//    if (pQueryAttr->tableCols[i].flist.numOfFilters > 0 && pQueryAttr->tableCols[i].flist.filterInfo != NULL) {
//      pQueryAttr->numOfFilterCols++;
//    }
  }

  if (pQueryAttr->numOfFilterCols == 0) {
    return TSDB_CODE_SUCCESS;
  }

//  doCreateFilterInfo(pQueryAttr->tableCols, pQueryAttr->numOfCols, pQueryAttr->numOfFilterCols,
//                     &pQueryAttr->pFilterInfo, qId);

  pQueryAttr->createFilterOperator = true;

  return TSDB_CODE_SUCCESS;
}

static void doUpdateExprColumnIndex(STaskAttr *pQueryAttr) {
  assert(pQueryAttr->pExpr1 != NULL && pQueryAttr != NULL);

  for (int32_t k = 0; k < pQueryAttr->numOfOutput; ++k) {
    SExprBasicInfo *pSqlExprMsg = &pQueryAttr->pExpr1[k].base;
//    if (pSqlExprMsg->functionId == FUNCTION_ARITHM) {
//      continue;
//    }

    // todo opt performance
    SColIndex *pColIndex = NULL;/*&pSqlExprMsg->colInfo;*/
    if (TSDB_COL_IS_NORMAL_COL(pColIndex->flag)) {
      int32_t f = 0;
      for (f = 0; f < pQueryAttr->numOfCols; ++f) {
        if (pColIndex->colId == pQueryAttr->tableCols[f].colId) {
          pColIndex->colIndex = f;
          break;
        }
      }

      assert(f < pQueryAttr->numOfCols);
    } else if (pColIndex->colId <= TSDB_UD_COLUMN_INDEX) {
      // do nothing for user-defined constant value result columns
    } else {
      int32_t f = 0;
      for (f = 0; f < pQueryAttr->numOfTags; ++f) {
        if (pColIndex->colId == pQueryAttr->tagColList[f].colId) {
          pColIndex->colIndex = f;
          break;
        }
      }

      assert(f < pQueryAttr->numOfTags || pColIndex->colId == TSDB_TBNAME_COLUMN_INDEX);
    }
  }
}

void setResultBufSize(STaskAttr* pQueryAttr, SRspResultInfo* pResultInfo) {
  const int32_t DEFAULT_RESULT_MSG_SIZE = 1024 * (1024 + 512);

  // the minimum number of rows for projection query
  const int32_t MIN_ROWS_FOR_PRJ_QUERY = 8192;
  const int32_t DEFAULT_MIN_ROWS = 4096;

  const float THRESHOLD_RATIO = 0.85f;

  if (isProjQuery(pQueryAttr)) {
    int32_t numOfRes = DEFAULT_RESULT_MSG_SIZE / pQueryAttr->resultRowSize;
    if (numOfRes < MIN_ROWS_FOR_PRJ_QUERY) {
      numOfRes = MIN_ROWS_FOR_PRJ_QUERY;
    }

    pResultInfo->capacity  = numOfRes;
  } else {  // in case of non-prj query, a smaller output buffer will be used.
    pResultInfo->capacity = DEFAULT_MIN_ROWS;
  }

  pResultInfo->threshold = (int32_t)(pResultInfo->capacity * THRESHOLD_RATIO);
  pResultInfo->total = 0;
}

//TODO refactor
void freeColumnFilterInfo(SColumnFilterInfo* pFilter, int32_t numOfFilters) {
    if (pFilter == NULL || numOfFilters == 0) {
      return;
    }

    for (int32_t i = 0; i < numOfFilters; i++) {
      if (pFilter[i].filterstr && pFilter[i].pz) {
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

void doDestroyTask(SExecTaskInfo *pTaskInfo) {
  qDebug("%s execTask is freed", GET_TASKID(pTaskInfo));

  doDestroyTableQueryInfo(&pTaskInfo->tableqinfoGroupInfo);
//  taosArrayDestroy(pTaskInfo->summary.queryProfEvents);
//  taosHashCleanup(pTaskInfo->summary.operatorProfResults);

  tfree(pTaskInfo->sql);
  tfree(pTaskInfo->id.str);
  tfree(pTaskInfo);
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
      int32_t len = (varDataLen(val) > maxLen)? maxLen:varDataLen(val);
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
  return (int64_t)(s1* 1.5 * numOfTables);
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

bool checkNeedToCompressQueryCol(SQInfo *pQInfo) {
  STaskRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  STaskAttr *pQueryAttr = pRuntimeEnv->pQueryAttr;

  SSDataBlock* pRes = pRuntimeEnv->outputBuf;

  if (GET_NUM_OF_RESULTS(&(pQInfo->runtimeEnv)) <= 0) {
    return false;
  }

  int32_t numOfRows = pQueryAttr->pExpr2 ? GET_NUM_OF_RESULTS(pRuntimeEnv) : pRes->info.rows;
  int32_t numOfCols = pQueryAttr->pExpr2 ? pQueryAttr->numOfExpr2 : pQueryAttr->numOfOutput;

  for (int32_t col = 0; col < numOfCols; ++col) {
    SColumnInfoData* pColRes = taosArrayGet(pRes->pDataBlock, col);
    int32_t colSize = pColRes->info.bytes * numOfRows;
    if (NEEDTO_COMPRESS_QUERY(colSize)) {
      return true;
    }
  }

  return false;
}

void releaseQueryBuf(size_t numOfTables) {
  if (tsQueryBufferSizeBytes < 0) {
    return;
  }

  int64_t t = getQuerySupportBufSize(numOfTables);

  // restore value is not enough buffer available
  atomic_add_fetch_64(&tsQueryBufferSizeBytes, t);
}
