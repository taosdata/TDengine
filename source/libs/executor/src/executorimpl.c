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
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "tfill.h"
#include "tname.h"

#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tsort.h"
#include "ttime.h"

#include "executorimpl.h"
#include "query.h"
#include "tcompare.h"
#include "tcompression.h"
#include "thash.h"
#include "ttypes.h"
#include "vnode.h"
#include "index.h"

#define IS_MAIN_SCAN(runtime)          ((runtime)->scanFlag == MAIN_SCAN)
#define IS_REVERSE_SCAN(runtime)       ((runtime)->scanFlag == REVERSE_SCAN)
#define IS_REPEAT_SCAN(runtime)        ((runtime)->scanFlag == REPEAT_SCAN)
#define SET_MAIN_SCAN_FLAG(runtime)    ((runtime)->scanFlag = MAIN_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define SDATA_BLOCK_INITIALIZER \
  (SDataBlockInfo) { {0}, 0 }

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

enum {
  TS_JOIN_TS_EQUAL = 0,
  TS_JOIN_TS_NOT_EQUALS = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

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
//#define GET_NUM_OF_TABLEGROUP(q)    taosArrayGetSize((q)->tableqinfoGroupInfo.pGroupList)
#define QUERY_IS_INTERVAL_QUERY(_q) ((_q)->interval.interval > 0)

int32_t getMaximumIdleDurationSec() { return tsShellActivityTimer * 2; }

static int32_t getExprFunctionId(SExprInfo* pExprInfo) {
  assert(pExprInfo != NULL && pExprInfo->pExpr != NULL && pExprInfo->pExpr->nodeType == TEXPR_UNARYEXPR_NODE);
  return 0;
}

static void doSetTagValueToResultBuf(char* output, const char* val, int16_t type, int16_t bytes);

static void setBlockStatisInfo(SqlFunctionCtx* pCtx, SExprInfo* pExpr, SSDataBlock* pSDataBlock);

static void destroyTableQueryInfoImpl(STableQueryInfo* pTableQueryInfo);

static SColumnInfo* extractColumnFilterInfo(SExprInfo* pExpr, int32_t numOfOutput, int32_t* numOfFilterCols);

static void releaseQueryBuf(size_t numOfTables);

static int32_t getNumOfScanTimes(STaskAttr* pQueryAttr);

static void destroySFillOperatorInfo(void* param, int32_t numOfOutput);
static void destroyProjectOperatorInfo(void* param, int32_t numOfOutput);
static void destroyTagScanOperatorInfo(void* param, int32_t numOfOutput);
static void destroyOrderOperatorInfo(void* param, int32_t numOfOutput);
static void destroyAggOperatorInfo(void* param, int32_t numOfOutput);

static void destroyIntervalOperatorInfo(void* param, int32_t numOfOutput);
static void destroyExchangeOperatorInfo(void* param, int32_t numOfOutput);

static void destroyOperatorInfo(SOperatorInfo* pOperator);
static void destroySysTableScannerOperatorInfo(void* param, int32_t numOfOutput);

void doSetOperatorCompleted(SOperatorInfo* pOperator) {
  pOperator->status = OP_EXEC_DONE;

  pOperator->cost.totalCost = (taosGetTimestampUs() - pOperator->pTaskInfo->cost.start * 1000) / 1000.0;
  if (pOperator->pTaskInfo != NULL) {
    setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  }
}

int32_t operatorDummyOpenFn(SOperatorInfo* pOperator) {
  OPTR_SET_OPENED(pOperator);
  pOperator->cost.openCost = 0;
  return TSDB_CODE_SUCCESS;
}

SOperatorFpSet createOperatorFpSet(__optr_open_fn_t openFn, __optr_fn_t nextFn, __optr_fn_t streamFn,
                                   __optr_fn_t cleanup, __optr_close_fn_t closeFn, __optr_encode_fn_t encode,
                                   __optr_decode_fn_t decode, __optr_explain_fn_t explain) {
  SOperatorFpSet fpSet = {
      ._openFn = openFn,
      .getNextFn = nextFn,
      .getStreamResFn = streamFn,
      .cleanupFn = cleanup,
      .closeFn = closeFn,
      .encodeResultRow = encode,
      .decodeResultRow = decode,
      .getExplainFn = explain,
  };

  return fpSet;
}

void operatorDummyCloseFn(void* param, int32_t numOfCols) {}

static int32_t doCopyToSDataBlock(SExecTaskInfo* taskInfo, SSDataBlock* pBlock, SExprInfo* pExprInfo,
                                  SDiskbasedBuf* pBuf, SGroupResInfo* pGroupResInfo, const int32_t* rowCellOffset,
                                  SqlFunctionCtx* pCtx, int32_t numOfExprs);

static void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void setResultBufSize(STaskAttr* pQueryAttr, SResultInfo* pResultInfo);
static void doSetTableGroupOutputBuf(SAggOperatorInfo* pAggInfo, int32_t numOfOutput, uint64_t groupId,
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
  return 0;
  //  char*   in1 = getPosInResultPage(pRuntimeEnv->pQueryAttr, page1, pRow1->offset, offset);
  //  char*   in2 = getPosInResultPage(pRuntimeEnv->pQueryAttr, page2, pRow2->offset, offset);

  //  return (in1 != NULL && in2 != NULL) ? supporter->comFunc(in1, in2) : 0;
}

// setup the output buffer for each operator
SSDataBlock* createResDataBlock(SDataBlockDescNode* pNode) {
  int32_t numOfCols = LIST_LENGTH(pNode->pSlots);

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  pBlock->info.blockId = pNode->dataBlockId;
  pBlock->info.rowSize = pNode->totalRowSize;  // todo ??
  pBlock->info.type = STREAM_INVALID;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData idata = {{0}};
    SSlotDescNode*  pDescNode = nodesListGetNode(pNode->pSlots, i);
    //    if (!pDescNode->output) {  // todo disable it temporarily
    //      continue;
    //    }

    idata.info.type = pDescNode->dataType.type;
    idata.info.bytes = pDescNode->dataType.bytes;
    idata.info.scale = pDescNode->dataType.scale;
    idata.info.slotId = pDescNode->slotId;
    idata.info.precision = pDescNode->dataType.precision;

    if (IS_VAR_DATA_TYPE(idata.info.type)) {
      pBlock->info.hasVarCol = true;
    }

    taosArrayPush(pBlock->pDataBlock, &idata);
  }

  pBlock->info.numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  return pBlock;
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

  if (newCapacity <= pResultRowInfo->capacity) {
    newCapacity += 4;
  }

  char* p = taosMemoryRealloc(pResultRowInfo->pPosition, newCapacity * sizeof(SResultRowPosition));
  if (p == NULL) {
    longjmp(env, TSDB_CODE_OUT_OF_MEMORY);
  }

  pResultRowInfo->pPosition = (SResultRowPosition*)p;

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

  setBufPageDirty(pData, true);

  // set the number of rows in current disk page
  SResultRow* pResultRow = (SResultRow*)((char*)pData + pData->num);
  pResultRow->pageId = pageId;
  pResultRow->offset = (int32_t)pData->num;

  pData->num += interBufSize;

  return pResultRow;
}

/**
 * the struct of key in hash table
 * +----------+---------------+
 * | group id |   key data    |
 * | 8 bytes  | actual length |
 * +----------+---------------+
 */
SResultRow* doSetResultOutBufByKey(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo, char* pData,
                                   int16_t bytes, bool masterscan, uint64_t groupId, SExecTaskInfo* pTaskInfo,
                                   bool isIntervalQuery, SAggSupporter* pSup) {
  SET_RES_WINDOW_KEY(pSup->keyBuf, pData, bytes, groupId);

  SResultRowPosition* p1 =
      (SResultRowPosition*)taosHashGet(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  SResultRow* pResult = NULL;

  // in case of repeat scan/reverse scan, no new time window added.
  if (isIntervalQuery) {
    if (masterscan && p1 != NULL) {  // the *p1 may be NULL in case of sliding+offset exists.
      pResult = getResultRowByPos(pResultBuf, p1);
      ASSERT(pResult->pageId == p1->pageId && pResult->offset == p1->offset);
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existInCurrentResusltRowInfo in the
    // pResultRowInfo object.
    if (p1 != NULL) {
      pResult = getResultRowByPos(pResultBuf, p1);
      ASSERT(pResult->pageId == p1->pageId && pResult->offset == p1->offset);
    }
  }

  // 1. close current opened time window
  if (pResultRowInfo->cur.pageId != -1 && ((pResult == NULL) || (pResult->pageId != pResultRowInfo->cur.pageId &&
                                                                 pResult->offset != pResultRowInfo->cur.offset))) {
    // todo extract function
    SResultRowPosition pos = pResultRowInfo->cur;
    SFilePage*         pPage = getBufPage(pResultBuf, pos.pageId);
    SResultRow*        pRow = (SResultRow*)((char*)pPage + pos.offset);
    closeResultRow(pRow);
    releaseBufPage(pResultBuf, pPage);
  }

  // allocate a new buffer page
  prepareResultListBuffer(pResultRowInfo, pTaskInfo->env);
  if (pResult == NULL) {
    ASSERT(pSup->resultRowSize > 0);
    pResult = getNewResultRow_rv(pResultBuf, groupId, pSup->resultRowSize);
    initResultRow(pResult);

    // add a new result set for a new group
    SResultRowPosition pos = {.pageId = pResult->pageId, .offset = pResult->offset};
    taosHashPut(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pos,
                sizeof(SResultRowPosition));
  }

  // 2. set the new time window to be the new active time window
  pResultRowInfo->pPosition[pResultRowInfo->size++] =
      (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};
  pResultRowInfo->cur = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};

  // too many time window in query
  if (pResultRowInfo->size > MAX_INTERVAL_TIME_WINDOW) {
    longjmp(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  return pResult;
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

static void doUpdateResultRowIndex(SResultRowInfo* pResultRowInfo, TSKEY lastKey, bool ascQuery,
                                   bool timeWindowInterpo) {
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
//
// static void updateResultRowInfoActiveIndex(SResultRowInfo* pResultRowInfo, const STimeWindow* pWin, TSKEY lastKey,
//                                           bool ascQuery, bool interp) {
//  if ((lastKey > pWin->ekey && ascQuery) || (lastKey < pWin->ekey && (!ascQuery))) {
//    closeAllResultRows(pResultRowInfo);
//    pResultRowInfo->curPos = pResultRowInfo->size - 1;
//  } else {
//    int32_t step = ascQuery ? 1 : -1;
//    doUpdateResultRowIndex(pResultRowInfo, lastKey - step, ascQuery, interp);
//  }
//}

//  query_range_start, query_range_end, window_duration, window_start, window_end
void initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow) {
  pColData->info.type = TSDB_DATA_TYPE_TIMESTAMP;
  pColData->info.bytes = sizeof(int64_t);

  colInfoDataEnsureCapacity(pColData, 0, 5);
  colDataAppendInt64(pColData, 0, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 1, &pQueryWindow->ekey);

  int64_t interval = 0;
  colDataAppendInt64(pColData, 2, &interval);  // this value may be variable in case of 'n' and 'y'.
  colDataAppendInt64(pColData, 3, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 4, &pQueryWindow->ekey);
}

void doApplyFunctions(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, STimeWindow* pWin,
                      SColumnInfoData* pTimeWindowData, int32_t offset, int32_t forwardStep, TSKEY* tsCol,
                      int32_t numOfTotal, int32_t numOfOutput, int32_t order) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    // keep it temporarily
    bool    hasAgg = pCtx[k].input.colDataAggIsSet;
    int32_t numOfRows = pCtx[k].input.numOfRows;
    int32_t startOffset = pCtx[k].input.startRowIndex;

    pCtx[k].input.startRowIndex = offset;
    pCtx[k].input.numOfRows = forwardStep;

    if (tsCol != NULL) {
      pCtx[k].ptsList = tsCol;
    }

    // not a whole block involved in query processing, statistics data can not be used
    // NOTE: the original value of isSet have been changed here
    if (pCtx[k].input.colDataAggIsSet && forwardStep < numOfTotal) {
      pCtx[k].input.colDataAggIsSet = false;
    }

    if (fmIsWindowPseudoColumnFunc(pCtx[k].functionId)) {
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[k]);
      char*                p = GET_ROWCELL_INTERBUF(pEntryInfo);

      SColumnInfoData idata = {0};
      idata.info.type = TSDB_DATA_TYPE_BIGINT;
      idata.info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      idata.pData = p;

      SScalarParam out = {.columnData = &idata};
      SScalarParam tw = {.numOfRows = 5, .columnData = pTimeWindowData};
      pCtx[k].sfp.process(&tw, 1, &out);
      pEntryInfo->numOfRes = 1;
      continue;
    }
    int32_t code = TSDB_CODE_SUCCESS;
    if (functionNeedToExecute(&pCtx[k]) && pCtx[k].fpSet.process != NULL) {
      code = pCtx[k].fpSet.process(&pCtx[k]);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s apply functions error, code: %s", GET_TASKID(taskInfo), tstrerror(code));
        taskInfo->code = code;
        longjmp(taskInfo->env, code);
      }
    }

    // restore it
    pCtx[k].input.colDataAggIsSet = hasAgg;
    pCtx[k].input.startRowIndex = startOffset;
    pCtx[k].input.numOfRows = numOfRows;
  }
}

static FORCE_INLINE TSKEY reviseWindowEkey(STaskAttr* pQueryAttr, STimeWindow* pWindow) {
  TSKEY   ekey = -1;
  int32_t order = TSDB_ORDER_ASC;
  if (order == TSDB_ORDER_ASC) {
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

static int32_t doSetInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order,
                                   int32_t scanFlag, bool createDummyCol);

static void doSetInputDataBlockInfo(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock,
                                    int32_t order) {
  for (int32_t i = 0; i < pOperator->numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;
    setBlockStatisInfo(&pCtx[i], &pOperator->pExpr[i], pBlock);
  }
}

void setInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order,
                       int32_t scanFlag, bool createDummyCol) {
  if (pBlock->pBlockAgg != NULL) {
    doSetInputDataBlockInfo(pOperator, pCtx, pBlock, order);
  } else {
    doSetInputDataBlock(pOperator, pCtx, pBlock, order, scanFlag, createDummyCol);
  }
}

static int32_t doCreateConstantValColumnInfo(SInputColumnInfoData* pInput, SFunctParam* pFuncParam, int32_t paramIndex,
                                             int32_t numOfRows) {
  SColumnInfoData* pColInfo = NULL;
  if (pInput->pData[paramIndex] == NULL) {
    pColInfo = taosMemoryCalloc(1, sizeof(SColumnInfoData));
    if (pColInfo == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    // Set the correct column info (data type and bytes)
    pColInfo->info.type = pFuncParam->param.nType;
    pColInfo->info.bytes = pFuncParam->param.nLen;

    pInput->pData[paramIndex] = pColInfo;
  } else {
    pColInfo = pInput->pData[paramIndex];
  }

  colInfoDataEnsureCapacity(pColInfo, 0, numOfRows);

  int8_t type = pFuncParam->param.nType;
  if (type == TSDB_DATA_TYPE_BIGINT || type == TSDB_DATA_TYPE_UBIGINT) {
    int64_t v = pFuncParam->param.i;
    for (int32_t i = 0; i < numOfRows; ++i) {
      colDataAppendInt64(pColInfo, i, &v);
    }
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    double v = pFuncParam->param.d;
    for (int32_t i = 0; i < numOfRows; ++i) {
      colDataAppendDouble(pColInfo, i, &v);
    }
  } else if (type == TSDB_DATA_TYPE_VARCHAR) {
    char* tmp = taosMemoryMalloc(pFuncParam->param.nLen + VARSTR_HEADER_SIZE);
    STR_WITH_SIZE_TO_VARSTR(tmp, pFuncParam->param.pz, pFuncParam->param.nLen);
    for (int32_t i = 0; i < numOfRows; ++i) {
      colDataAppend(pColInfo, i, tmp, false);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doSetInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order,
                                   int32_t scanFlag, bool createDummyCol) {
  int32_t code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < pOperator->numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;

    pCtx[i].pSrcBlock = pBlock;
    pCtx[i].scanFlag = scanFlag;

    SInputColumnInfoData* pInput = &pCtx[i].input;
    pInput->uid = pBlock->info.uid;
    pInput->colDataAggIsSet = false;

    SExprInfo* pOneExpr = &pOperator->pExpr[i];
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        pInput->pData[j] = taosArrayGet(pBlock->pDataBlock, slotId);
        pInput->totalRows = pBlock->info.rows;
        pInput->numOfRows = pBlock->info.rows;
        pInput->startRowIndex = 0;

        // NOTE: the last parameter is the primary timestamp column
        if (fmIsTimelineFunc(pCtx[i].functionId) && (j == pOneExpr->base.numOfParams - 1)) {
          pInput->pPTS = pInput->pData[j];
        }
        ASSERT(pInput->pData[j] != NULL);
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        // todo avoid case: top(k, 12), 12 is the value parameter.
        // sum(11), 11 is also the value parameter.
        if (createDummyCol && pOneExpr->base.numOfParams == 1) {
          pInput->totalRows = pBlock->info.rows;
          pInput->numOfRows = pBlock->info.rows;
          pInput->startRowIndex = 0;

          code = doCreateConstantValColumnInfo(pInput, pFuncParam, j, pBlock->info.rows);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
        }
      }
    }
  }

  return code;
}

static int32_t doAggregateImpl(SOperatorInfo* pOperator, TSKEY startTs, SqlFunctionCtx* pCtx) {
  for (int32_t k = 0; k < pOperator->numOfExprs; ++k) {
    if (functionNeedToExecute(&pCtx[k])) {
      // todo add a dummy funtion to avoid process check
      if (pCtx[k].fpSet.process != NULL) {
        int32_t code = pCtx[k].fpSet.process(&pCtx[k]);
        if (code != TSDB_CODE_SUCCESS) {
          qError("%s aggregate function error happens, code: %s", GET_TASKID(pOperator->pTaskInfo), tstrerror(code));
          return code;
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void setPseudoOutputColInfo(SSDataBlock* pResult, SqlFunctionCtx* pCtx, SArray* pPseudoList) {
  size_t num = (pPseudoList != NULL) ? taosArrayGetSize(pPseudoList) : 0;
  for (int32_t i = 0; i < num; ++i) {
    pCtx[i].pOutput = taosArrayGet(pResult->pDataBlock, i);
  }
}

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList) {
  setPseudoOutputColInfo(pResult, pCtx, pPseudoList);
  pResult->info.groupId = pSrcBlock->info.groupId;

  // if the source equals to the destination, it is to create a new column as the result of scalar function or some
  // operators.
  bool createNewColModel = (pResult == pSrcBlock);

  int32_t numOfRows = 0;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t         outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx* pfCtx = &pCtx[k];

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_COLUMN) {  // it is a project query
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pResult->info.rows > 0 && !createNewColModel) {
        colDataMergeCol(pColInfoData, pResult->info.rows, &pResult->info.capacity, pfCtx->input.pData[0],
                        pfCtx->input.numOfRows);
      } else {
        colDataAssign(pColInfoData, pfCtx->input.pData[0], pfCtx->input.numOfRows);
      }

      numOfRows = pfCtx->input.numOfRows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);

      int32_t offset = createNewColModel ? 0 : pResult->info.rows;
      for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
        colDataAppend(pColInfoData, i + offset,
                      taosVariantGet(&pExpr[k].base.pParam[0].param, pExpr[k].base.pParam[0].param.nType),
                      TSDB_DATA_TYPE_NULL == pExpr[k].base.pParam[0].param.nType);
      }

      numOfRows = pSrcBlock->info.rows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_OPERATOR) {
      SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
      taosArrayPush(pBlockList, &pSrcBlock);

      SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

      SScalarParam dest = {.columnData = &idata};
      int32_t      code = scalarCalculate(pExpr[k].pExpr->_optrRoot.pRootNode, pBlockList, &dest);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pBlockList);
        return code;
      }

      int32_t startOffset = createNewColModel ? 0 : pResult->info.rows;
      colInfoDataEnsureCapacity(pResColData, startOffset, pResult->info.capacity);
      colDataMergeCol(pResColData, startOffset, &pResult->info.capacity, &idata, dest.numOfRows);

      numOfRows = dest.numOfRows;
      taosArrayDestroy(pBlockList);
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      ASSERT(!fmIsAggFunc(pfCtx->functionId));

      // _rowts/_c0, not tbname column
      if (fmIsPseudoColumnFunc(pfCtx->functionId) && (!fmIsScanPseudoColumnFunc(pfCtx->functionId))) {
        // do nothing
      } else if (fmIsIndefiniteRowsFunc(pfCtx->functionId)) {
        SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[k]);
        pfCtx->fpSet.init(&pCtx[k], pResInfo);

        pfCtx->pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        pfCtx->offset = createNewColModel ? 0 : pResult->info.rows;  // set the start offset

        // set the timestamp(_rowts) output buffer
        if (taosArrayGetSize(pPseudoList) > 0) {
          int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
          pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
        }

        numOfRows = pfCtx->fpSet.process(pfCtx);
      } else {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        taosArrayPush(pBlockList, &pSrcBlock);

        SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
        SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

        SScalarParam dest = {.columnData = &idata};
        int32_t      code = scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pBlockList);
          return code;
        }

        int32_t startOffset = createNewColModel ? 0 : pResult->info.rows;
        colInfoDataEnsureCapacity(pResColData, startOffset, pResult->info.capacity);
        colDataMergeCol(pResColData, startOffset, &pResult->info.capacity, &idata, dest.numOfRows);

        numOfRows = dest.numOfRows;
        taosArrayDestroy(pBlockList);
      }
    } else {
      ASSERT(0);
    }
  }

  if (!createNewColModel) {
    pResult->info.rows += numOfRows;
  }

  return TSDB_CODE_SUCCESS;
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

int32_t setGroupResultOutputBuf(SOptrBasicInfo* binfo, int32_t numOfCols, char* pData, int16_t type, int16_t bytes,
                                int32_t groupId, SDiskbasedBuf* pBuf, SExecTaskInfo* pTaskInfo,
                                SAggSupporter* pAggSup) {
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = binfo->pCtx;

  SResultRow* pResultRow =
      doSetResultOutBufByKey(pBuf, pResultRowInfo, (char*)pData, bytes, true, groupId, pTaskInfo, false, pAggSup);
  assert(pResultRow != NULL);

  setResultRowKey(pResultRow, pData, type);
  setResultRowInitCtx(pResultRow, pCtx, numOfCols, binfo->rowCellInfoOffset);
  return TSDB_CODE_SUCCESS;
}

bool functionNeedToExecute(SqlFunctionCtx* pCtx) {
  struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  // in case of timestamp column, always generated results.
  int32_t functionId = pCtx->functionId;
  if (functionId == -1) {
    return false;
  }

  if (pCtx->scanFlag == REPEAT_SCAN) {
    return fmIsRepeatScanFunc(pCtx->functionId);
  }

  if (isRowEntryCompleted(pResInfo)) {
    return false;
  }

  //  if (functionId == FUNCTION_FIRST_DST || functionId == FUNCTION_FIRST) {
  //    //    return QUERY_IS_ASC_QUERY(pQueryAttr);
  //  }
  //
  //  // denote the order type
  //  if ((functionId == FUNCTION_LAST_DST || functionId == FUNCTION_LAST)) {
  //    //    return pCtx->param[0].i == pQueryAttr->order.order;
  //  }

  // in the reverse table scan, only the following functions need to be executed
  //  if (IS_REVERSE_SCAN(pRuntimeEnv) ||
  //      (pRuntimeEnv->scanFlag == REPEAT_SCAN && functionId != FUNCTION_STDDEV && functionId != FUNCTION_PERCT)) {
  //    return false;
  //  }

  return true;
}

static int32_t doCreateConstantValColumnAggInfo(SInputColumnInfoData* pInput, SFunctParam* pFuncParam, int32_t type,
                                                int32_t paramIndex, int32_t numOfRows) {
  if (pInput->pData[paramIndex] == NULL) {
    pInput->pData[paramIndex] = taosMemoryCalloc(1, sizeof(SColumnInfoData));
    if (pInput->pData[paramIndex] == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    // Set the correct column info (data type and bytes)
    pInput->pData[paramIndex]->info.type = type;
    pInput->pData[paramIndex]->info.bytes = tDataTypes[type].bytes;
  }

  SColumnDataAgg* da = NULL;
  if (pInput->pColumnDataAgg[paramIndex] == NULL) {
    da = taosMemoryCalloc(1, sizeof(SColumnDataAgg));
    pInput->pColumnDataAgg[paramIndex] = da;
    if (da == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    da = pInput->pColumnDataAgg[paramIndex];
  }

  ASSERT(!IS_VAR_DATA_TYPE(type));

  if (type == TSDB_DATA_TYPE_BIGINT) {
    int64_t v = pFuncParam->param.i;
    *da = (SColumnDataAgg){.numOfNull = 0, .min = v, .max = v, .maxIndex = 0, .minIndex = 0, .sum = v * numOfRows};
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    double v = pFuncParam->param.d;
    *da = (SColumnDataAgg){.numOfNull = 0, .maxIndex = 0, .minIndex = 0};

    *(double*)&da->min = v;
    *(double*)&da->max = v;
    *(double*)&da->sum = v * numOfRows;
  } else if (type == TSDB_DATA_TYPE_BOOL) {  // todo validate this data type
    bool v = pFuncParam->param.i;

    *da = (SColumnDataAgg){.numOfNull = 0, .maxIndex = 0, .minIndex = 0};
    *(bool*)&da->min = 0;
    *(bool*)&da->max = v;
    *(bool*)&da->sum = v * numOfRows;
  } else if (type == TSDB_DATA_TYPE_TIMESTAMP) {
    // do nothing
  } else {
    ASSERT(0);
  }

  return TSDB_CODE_SUCCESS;
}

void setBlockStatisInfo(SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, SSDataBlock* pBlock) {
  int32_t numOfRows = pBlock->info.rows;

  SInputColumnInfoData* pInput = &pCtx->input;
  pInput->numOfRows = numOfRows;
  pInput->totalRows = numOfRows;

  if (pBlock->pBlockAgg != NULL) {
    pInput->colDataAggIsSet = true;

    for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pExprInfo->base.pParam[j];

      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        pInput->pColumnDataAgg[j] = pBlock->pBlockAgg[slotId];
        if (pInput->pColumnDataAgg[j] == NULL) {
          pInput->colDataAggIsSet = false;
        }

        // Here we set the column info data since the data type for each column data is required, but
        // the data in the corresponding SColumnInfoData will not be used.
        pInput->pData[j] = taosArrayGet(pBlock->pDataBlock, slotId);
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        doCreateConstantValColumnAggInfo(pInput, pFuncParam, pFuncParam->param.nType, j, pBlock->info.rows);
      }
    }
  } else {
    pInput->colDataAggIsSet = false;
  }

  // set the statistics data for primary time stamp column
  //  if (pCtx->functionId == FUNCTION_SPREAD && pColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
  //    pCtx->isAggSet = true;
  //    pCtx->agg.min = pBlock->info.window.skey;
  //    pCtx->agg.max = pBlock->info.window.ekey;
  //  }
}

// set the output buffer for the selectivity + tag query
static int32_t setSelectValueColumnInfo(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t num = 0;

  SqlFunctionCtx*  p = NULL;
  SqlFunctionCtx** pValCtx = taosMemoryCalloc(numOfOutput, POINTER_BYTES);
  if (pValCtx == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (strcmp(pCtx[i].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
      pValCtx[num++] = &pCtx[i];
    } else if (fmIsSelectFunc(pCtx[i].functionId)) {
      p = &pCtx[i];
    }
    //    if (functionId == FUNCTION_TAG_DUMMY || functionId == FUNCTION_TS_DUMMY) {
    //      tagLen += pCtx[i].resDataInfo.bytes;
    //      pTagCtx[num++] = &pCtx[i];
    //    } else if (functionId == FUNCTION_TS || functionId == FUNCTION_TAG) {
    //      // tag function may be the group by tag column
    //      // ts may be the required primary timestamp column
    //      continue;
    //    } else {
    //      // the column may be the normal column, group by normal_column, the functionId is FUNCTION_PRJ
    //    }
  }

  if (p != NULL) {
    p->subsidiaries.pCtx = pValCtx;
    p->subsidiaries.num = num;
  } else {
    taosMemoryFreeClear(pValCtx);
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
    pCtx->curBufPage = -1;
    pCtx->pExpr = pExpr;

    if (pExpr->pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SFuncExecEnv env = {0};
      pCtx->functionId = pExpr->pExpr->_function.pFunctNode->funcId;

      if (fmIsAggFunc(pCtx->functionId) || fmIsIndefiniteRowsFunc(pCtx->functionId)) {
        bool isUdaf = fmIsUserDefinedFunc(pCtx->functionId);
        if (!isUdaf) {
          fmGetFuncExecFuncs(pCtx->functionId, &pCtx->fpSet);
        } else {
          char* udfName = pExpr->pExpr->_function.pFunctNode->functionName;
          strncpy(pCtx->udfName, udfName, strlen(udfName));
          fmGetUdafExecFuncs(pCtx->functionId, &pCtx->fpSet);
        }
        pCtx->fpSet.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
      } else {
        fmGetScalarFuncExecFuncs(pCtx->functionId, &pCtx->sfp);
        if (pCtx->sfp.getEnv != NULL) {
          pCtx->sfp.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
        }
      }
      pCtx->resDataInfo.interBufSize = env.calcMemSize;
    } else if (pExpr->pExpr->nodeType == QUERY_NODE_COLUMN || pExpr->pExpr->nodeType == QUERY_NODE_OPERATOR ||
               pExpr->pExpr->nodeType == QUERY_NODE_VALUE) {
      // for simple column, the result buffer needs to hold at least one element.
      pCtx->resDataInfo.interBufSize = pFunct->resSchema.bytes;
    }

    pCtx->input.numOfInputCols = pFunct->numOfParams;
    pCtx->input.pData = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);
    pCtx->input.pColumnDataAgg = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);

    pCtx->pTsOutput = NULL;
    pCtx->resDataInfo.bytes = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type = pFunct->resSchema.type;
    pCtx->order = TSDB_ORDER_ASC;
    pCtx->start.key = INT64_MIN;
    pCtx->end.key = INT64_MIN;
    pCtx->numOfParams = pExpr->base.numOfParams;

    pCtx->param = pFunct->pParam;
    //    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
    //      // set the order information for top/bottom query
    //      int32_t functionId = pCtx->functionId;
    //      if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM || functionId == FUNCTION_DIFF) {
    //        int32_t f = getExprFunctionId(&pExpr[0]);
    //        assert(f == FUNCTION_TS || f == FUNCTION_TS_DUMMY);
    //
    //        //      pCtx->param[2].i = pQueryAttr->order.order;
    //        //      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
    //        //      pCtx->param[3].i = functionId;
    //        //      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;
    //
    //        //      pCtx->param[1].i = pQueryAttr->order.col.info.colId;
    //      } else if (functionId == FUNCTION_INTERP) {
    //        //      pCtx->param[2].i = (int8_t)pQueryAttr->fillType;
    //        //      if (pQueryAttr->fillVal != NULL) {
    //        //        if (isNull((const char *)&pQueryAttr->fillVal[i], pCtx->inputType)) {
    //        //          pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
    //        //        } else {  // todo refactor, taosVariantCreateFromBinary should handle the NULL value
    //        //          if (pCtx->inputType != TSDB_DATA_TYPE_BINARY && pCtx->inputType != TSDB_DATA_TYPE_NCHAR) {
    //        //            taosVariantCreateFromBinary(&pCtx->param[1], (char *)&pQueryAttr->fillVal[i],
    //        pCtx->inputBytes, pCtx->inputType);
    //        //          }
    //        //        }
    //        //      }
    //      } else if (functionId == FUNCTION_TWA) {
    //        //      pCtx->param[1].i = pQueryAttr->window.skey;
    //        //      pCtx->param[1].nType = TSDB_DATA_TYPE_BIGINT;
    //        //      pCtx->param[2].i = pQueryAttr->window.ekey;
    //        //      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
    //      } else if (functionId == FUNCTION_ARITHM) {
    //        //      pCtx->param[1].pz = (char*) getScalarFuncSupport(pRuntimeEnv->scalarSup, i);
    //      }
    //    }
  }

  for (int32_t i = 1; i < numOfOutput; ++i) {
    (*rowCellInfoOffset)[i] =
        (int32_t)((*rowCellInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) + pFuncCtx[i - 1].resDataInfo.interBufSize);
  }

  setSelectValueColumnInfo(pFuncCtx, numOfOutput);
  return pFuncCtx;
}

static void* destroySqlFunctionCtx(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  if (pCtx == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    for (int32_t j = 0; j < pCtx[i].numOfParams; ++j) {
      taosVariantDestroy(&pCtx[i].param[j].param);
    }

    taosVariantDestroy(&pCtx[i].tag);
    taosMemoryFreeClear(pCtx[i].subsidiaries.pCtx);
    taosMemoryFree(pCtx[i].input.pData);
    taosMemoryFree(pCtx[i].input.pColumnDataAgg);
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

  int32_t order = TSDB_ORDER_ASC;
  if (order != TSDB_ORDER_DESC || !TSWINDOW_IS_EQUAL(pQueryAttr->window, TSWINDOW_DESC_INITIALIZER)) {
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
void getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, STimeWindow* win) {
  win->skey = taosTimeTruncate(key, pInterval, precision);

  /*
   * if the realSkey > INT64_MAX - pInterval->interval, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  win->ekey = taosTimeAdd(win->skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
  if (win->ekey < win->skey) {
    win->ekey = INT64_MAX;
  }
}

static int32_t updateBlockLoadStatus(STaskAttr* pQuery, int32_t status) {
  bool hasFirstLastFunc = false;
  bool hasOtherFunc = false;

  if (status == BLK_DATA_DATA_LOAD || status == BLK_DATA_FILTEROUT) {
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

  if (hasFirstLastFunc && status == BLK_DATA_NOT_LOAD) {
    if (!hasOtherFunc) {
      return BLK_DATA_FILTEROUT;
    } else {
      return BLK_DATA_DATA_LOAD;
    }
  }

  return status;
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
//       TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey);
//     }
//
//     pQueryAttr->needReverseScan = false;
//     return;
//   }
//
//   if (pQueryAttr->groupbyColumn && pQueryAttr->order.order == TSDB_ORDER_DESC) {
//     pQueryAttr->order.order = TSDB_ORDER_ASC;
//     if (pQueryAttr->window.skey > pQueryAttr->window.ekey) {
//       TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey);
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
//        TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey);
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
//        TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey);
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
//          TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey);
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
//          TSWAP(pQueryAttr->window.skey, pQueryAttr->window.ekey);
//          doUpdateLastKey(pQueryAttr);
//        }
//
//        pQueryAttr->order.order = TSDB_ORDER_DESC;
//        pQueryAttr->needReverseScan = false;
//      }
//    }
//  }
//}

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

  if (true) {
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

static uint32_t doFilterByBlockTimeWindow(STableScanInfo* pTableScanInfo, SSDataBlock* pBlock) {
  SqlFunctionCtx* pCtx = pTableScanInfo->pCtx;
  uint32_t        status = BLK_DATA_NOT_LOAD;

  int32_t numOfOutput = pTableScanInfo->numOfOutput;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    int32_t functionId = pCtx[i].functionId;
    int32_t colId = pTableScanInfo->pExpr[i].base.pParam[0].pCol->colId;

    // group by + first/last should not apply the first/last block filter
    if (functionId < 0) {
      status |= BLK_DATA_DATA_LOAD;
      return status;
    } else {
      //      status |= aAggs[functionId].dataReqFunc(&pTableScanInfo->pCtx[i], &pBlock->info.window, colId);
      //      if ((status & BLK_DATA_DATA_LOAD) == BLK_DATA_DATA_LOAD) {
      //        return status;
      //      }
    }
  }

  return status;
}

int32_t loadDataBlockOnDemand(SExecTaskInfo* pTaskInfo, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock,
                              uint32_t* status) {
  *status = BLK_DATA_NOT_LOAD;

  pBlock->pDataBlock = NULL;
  pBlock->pBlockAgg = NULL;

  //  int64_t groupId = pRuntimeEnv->current->groupIndex;
  //  bool    ascQuery = QUERY_IS_ASC_QUERY(pQueryAttr);

  STaskCostInfo* pCost = &pTaskInfo->cost;

//  pCost->totalBlocks += 1;
//  pCost->totalRows += pBlock->info.rows;
#if 0
  // Calculate all time windows that are overlapping or contain current data block.
  // If current data block is contained by all possible time window, do not load current data block.
  if (/*pQueryAttr->pFilters || */pQueryAttr->groupbyColumn || pQueryAttr->sw.gap > 0 ||
      (QUERY_IS_INTERVAL_QUERY(pQueryAttr) && overlapWithTimeWindow(pTaskInfo, &pBlock->info))) {
    (*status) = BLK_DATA_DATA_LOAD;
  }

  // check if this data block is required to load
  if ((*status) != BLK_DATA_DATA_LOAD) {
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
      (*status) = BLK_DATA_DATA_LOAD;
    }
  }

  SDataBlockInfo* pBlockInfo = &pBlock->info;
//  *status = updateBlockLoadStatus(pRuntimeEnv->pQueryAttr, *status);

  if ((*status) == BLK_DATA_NOT_LOAD || (*status) == BLK_DATA_FILTEROUT) {
    //qDebug("QInfo:0x%"PRIx64" data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo->qId, pBlockInfo->window.skey,
//           pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->skipBlocks += 1;
  } else if ((*status) == BLK_DATA_SMA_LOAD) {
    // this function never returns error?
    pCost->loadBlockStatis += 1;
//    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->pTsdbReadHandle, &pBlock->pBlockAgg);

    if (pBlock->pBlockAgg == NULL) {  // data block statistics does not exist, load data block
//      pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pTsdbReadHandle, NULL);
      pCost->totalCheckedRows += pBlock->info.rows;
    }
  } else {
    assert((*status) == BLK_DATA_DATA_LOAD);

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
            pCost->skipBlocks += 1;
            //qDebug("QInfo:0x%"PRIx64" data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo->qId,
//                   pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
            (*status) = BLK_DATA_FILTEROUT;
            return TSDB_CODE_SUCCESS;
          }
        }
      }
    }

    // current block has been discard due to filter applied
//    if (!doFilterByBlockStatistics(pRuntimeEnv, pBlock->pBlockAgg, pTableScanInfo->pCtx, pBlockInfo->rows)) {
//      pCost->skipBlocks += 1;
//      qDebug("QInfo:0x%"PRIx64" data block discard, brange:%" PRId64 "-%" PRId64 ", rows:%d", pQInfo->qId, pBlockInfo->window.skey,
//             pBlockInfo->window.ekey, pBlockInfo->rows);
//      (*status) = BLK_DATA_FILTEROUT;
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

static void updateTableQueryInfoForReverseScan(STableQueryInfo* pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  //  TSWAP(pTableQueryInfo->win.skey, pTableQueryInfo->win.ekey);
  //  pTableQueryInfo->lastKey = pTableQueryInfo->win.skey;

  //  SWITCH_ORDER(pTableQueryInfo->cur.order);
  //  pTableQueryInfo->cur.vgroupIndex = -1;

  // set the index to be the end slot of result rows array
  //  SResultRowInfo* pResultRowInfo = &pTableQueryInfo->resInfo;
  //  if (pResultRowInfo->size > 0) {
  //    pResultRowInfo->curPos = pResultRowInfo->size - 1;
  //  } else {
  //    pResultRowInfo->curPos = -1;
  //  }
}

void initResultRow(SResultRow* pResultRow) {
  //  pResultRow->pEntryInfo = (struct SResultRowEntryInfo*)((char*)pResultRow + sizeof(SResultRow));
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
void setFunctionResultOutput(SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage, int32_t numOfExprs,
                             SExecTaskInfo* pTaskInfo) {
  SqlFunctionCtx* pCtx = pInfo->pCtx;
  SSDataBlock*    pDataBlock = pInfo->pRes;
  int32_t*        rowCellInfoOffset = pInfo->rowCellInfoOffset;

  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;
  initResultRowInfo(pResultRowInfo, 16);

  int64_t     tid = 0;
  int64_t     groupId = 0;
  SResultRow* pRow = doSetResultOutBufByKey(pSup->pResultBuf, pResultRowInfo, (char*)&tid, sizeof(tid), true, groupId,
                                            pTaskInfo, false, pSup);

  for (int32_t i = 0; i < numOfExprs; ++i) {
    struct SResultRowEntryInfo* pEntry = getResultCell(pRow, i, rowCellInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo = pEntry;
    pCtx[i].scanFlag = stage;
  }

  initCtxOutputBuffer(pCtx, numOfExprs);
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
    if (isRowEntryInitialized(pResInfo) || fmIsPseudoColumnFunc(pCtx[j].functionId) || pCtx[j].functionId == -1 ||
        fmIsScalarFunc(pCtx[j].functionId)) {
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

void destroyTableQueryInfoImpl(STableQueryInfo* pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
  }

  //  taosVariantDestroy(&pTableQueryInfo->tag);
  //  cleanupResultRowInfo(&pTableQueryInfo->resInfo);
}

void setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultCell(pResult, i, rowCellInfoOffset);

    struct SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (isRowEntryCompleted(pResInfo) && isRowEntryInitialized(pResInfo)) {
      continue;
    }

    if (fmIsWindowPseudoColumnFunc(pCtx[i].functionId)) {
      continue;
    }

    if (!pResInfo->initialized) {
      if (pCtx[i].functionId != -1) {
        pCtx[i].fpSet.init(&pCtx[i], pResInfo);
      } else {
        pResInfo->initialized = true;
      }
    }
  }
}

static void extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const int8_t* rowRes, bool keep);
void        doFilter(const SNode* pFilterNode, SSDataBlock* pBlock, SArray* pColMatchInfo) {
  if (pFilterNode == NULL) {
    return;
  }

  SFilterInfo* filter = NULL;

  // todo move to the initialization function
  int32_t code = filterInitFromNode((SNode*)pFilterNode, &filter, 0);

  SFilterColumnParam param1 = {.numOfCols = pBlock->info.numOfCols, .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param1);

  int8_t* rowRes = NULL;

  // todo the keep seems never to be True??
  bool keep = filterExecute(filter, pBlock, &rowRes, NULL, param1.numOfCols);
  filterFreeInfo(filter);

  extractQualifiedTupleByFilterResult(pBlock, rowRes, keep);
  blockDataUpdateTsWindow(pBlock, 0);
}

void extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const int8_t* rowRes, bool keep) {
  if (keep) {
    return;
  }

  if (rowRes != NULL) {
    SSDataBlock* px = createOneDataBlock(pBlock, false);
    blockDataEnsureCapacity(px, pBlock->info.rows);

    int32_t totalRows = pBlock->info.rows;

    for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
      SColumnInfoData* pDst = taosArrayGet(px->pDataBlock, i);
      SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, i);

      // it is a reserved column for scalar function, and no data in this column yet.
      if (pSrc->pData == NULL) {
        continue;
      }

      int32_t numOfRows = 0;
      for (int32_t j = 0; j < totalRows; ++j) {
        if (rowRes[j] == 0) {
          continue;
        }

        if (colDataIsNull_s(pSrc, j)) {
          colDataAppendNULL(pDst, numOfRows);
        } else {
          colDataAppend(pDst, numOfRows, colDataGetData(pSrc, j), false);
        }
        numOfRows += 1;
      }

      if (pBlock->info.rows == totalRows) {
        pBlock->info.rows = numOfRows;
      } else {
        ASSERT(pBlock->info.rows == numOfRows);
      }

      *pSrc = *pDst;
    }
  } else {
    // do nothing
    pBlock->info.rows = 0;
  }
}

void doSetTableGroupOutputBuf(SAggOperatorInfo* pAggInfo, int32_t numOfOutput, uint64_t groupId,
                              SExecTaskInfo* pTaskInfo) {
  // for simple group by query without interval, all the tables belong to one group result.
  int64_t uid = 0;

  SResultRowInfo* pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
  SqlFunctionCtx* pCtx = pAggInfo->binfo.pCtx;
  int32_t*        rowCellInfoOffset = pAggInfo->binfo.rowCellInfoOffset;

  SResultRow* pResultRow = doSetResultOutBufByKey(pAggInfo->aggSup.pResultBuf, pResultRowInfo, (char*)&groupId,
                                                  sizeof(groupId), true, groupId, pTaskInfo, false, &pAggInfo->aggSup);
  assert(pResultRow != NULL);

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pResultRow->pageId == -1) {
    int32_t ret =
        addNewWindowResultBuf(pResultRow, pAggInfo->aggSup.pResultBuf, groupId, pAggInfo->binfo.pRes->info.rowSize);
    if (ret != TSDB_CODE_SUCCESS) {
      return;
    }
  }

  setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowCellInfoOffset);
}

void setExecutionContext(int32_t numOfOutput, uint64_t groupId, SExecTaskInfo* pTaskInfo, SAggOperatorInfo* pAggInfo) {
  if (pAggInfo->groupId != INT32_MIN && pAggInfo->groupId == groupId) {
    return;
  }

  doSetTableGroupOutputBuf(pAggInfo, numOfOutput, groupId, pTaskInfo);

  // record the current active group id
  pAggInfo->groupId = groupId;
}

static void doUpdateNumOfRows(SResultRow* pRow, int32_t numOfExprs, const int32_t* rowCellOffset) {
  for (int32_t j = 0; j < numOfExprs; ++j) {
    struct SResultRowEntryInfo* pResInfo = getResultCell(pRow, j, rowCellOffset);
    if (!isRowEntryInitialized(pResInfo)) {
      continue;
    }

    if (pRow->numOfRows < pResInfo->numOfRes) {
      pRow->numOfRows = pResInfo->numOfRes;
    }
  }
}

int32_t doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprInfo* pExprInfo, SDiskbasedBuf* pBuf,
                           SGroupResInfo* pGroupResInfo, const int32_t* rowCellOffset, SqlFunctionCtx* pCtx,
                           int32_t numOfExprs) {
  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
  int32_t start = pGroupResInfo->index;

  for (int32_t i = start; i < numOfRows; i += 1) {
    SResKeyPos* pPos = taosArrayGetP(pGroupResInfo->pRows, i);
    SFilePage*  page = getBufPage(pBuf, pPos->pos.pageId);

    SResultRow* pRow = (SResultRow*)((char*)page + pPos->pos.offset);

    doUpdateNumOfRows(pRow, numOfExprs, rowCellOffset);
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      releaseBufPage(pBuf, page);
      continue;
    }

    if (pBlock->info.groupId == 0) {
      pBlock->info.groupId = pPos->groupId;
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.groupId != pPos->groupId) {
        releaseBufPage(pBuf, page);
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      releaseBufPage(pBuf, page);
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultCell(pRow, j, rowCellOffset);
      if (pCtx[j].fpSet.finalize) {
        int32_t code = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(code)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
          longjmp(pTaskInfo->env, code);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstartts, top(k, 20)
        // the _wstartts needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          colDataAppend(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    releaseBufPage(pBuf, page);
    pBlock->info.rows += pRow->numOfRows;
    if (pBlock->info.rows >= pBlock->info.capacity) {  // output buffer is full
      break;
    }
  }

  qDebug("%s result generated, rows:%d, groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.groupId);
  blockDataUpdateTsWindow(pBlock, 0);
  return 0;
}

void doBuildResultDatablock(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                            SDiskbasedBuf* pBuf) {
  SExprInfo*     pExprInfo = pOperator->pExpr;
  int32_t        numOfExprs = pOperator->numOfExprs;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int32_t*        rowCellOffset = pbInfo->rowCellInfoOffset;
  SSDataBlock*    pBlock = pbInfo->pRes;
  SqlFunctionCtx* pCtx = pbInfo->pCtx;

  blockDataCleanup(pBlock);
  if (!hashRemainDataInGroupInfo(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.groupId = 0;
  doCopyToSDataBlock(pTaskInfo, pBlock, pExprInfo, pBuf, pGroupResInfo, rowCellOffset, pCtx, numOfExprs);
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

int32_t doFillTimeIntervalGapsInResults(struct SFillInfo* pFillInfo, SSDataBlock* pBlock, int32_t capacity) {
  //  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
  //    SColumnInfoData* pColInfoData = taosArrayGet(pOutput->pDataBlock, i);
  //    p[i] = pColInfoData->pData + (pColInfoData->info.bytes * pOutput->info.rows);
  //  }

  int32_t numOfRows = (int32_t)taosFillResultDataBlock(pFillInfo, pBlock, capacity - pBlock->info.rows);
  pBlock->info.rows += numOfRows;

  return pBlock->info.rows;
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

  SFileBlockLoadRecorder* pRecorder = pSummary->pRecoder;
  if (pSummary->pRecoder != NULL) {
    qDebug("%s :cost summary: elapsed time:%" PRId64 " us, first merge:%" PRId64
           " us, total blocks:%d, "
           "load block statis:%d, load data block:%d, total rows:%" PRId64 ", check rows:%" PRId64,
           GET_TASKID(pTaskInfo), pSummary->elapsedTime, pSummary->firstStageMergeTime, pRecorder->totalBlocks,
           pRecorder->loadBlockStatis, pRecorder->loadBlocks, pRecorder->totalRows, pRecorder->totalCheckedRows);
  }
  // qDebug("QInfo:0x%"PRIx64" :cost summary: winResPool size:%.2f Kb, numOfWin:%"PRId64", tableInfoSize:%.2f Kb,
  // hashTable:%.2f Kb", pQInfo->qId, pSummary->winInfoSize/1024.0,
  //      pSummary->numOfTimeWindows, pSummary->tableInfoSize/1024.0, pSummary->hashSize/1024.0);
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

static void doDestroyTableList(STableListInfo* pTableqinfoList);

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

int32_t loadRemoteDataCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  SSourceDataInfo* pSourceDataInfo = (SSourceDataInfo*)param;
  if (code == TSDB_CODE_SUCCESS) {
    pSourceDataInfo->pRsp = pMsg->pData;

    SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->compLen = htonl(pRsp->compLen);
    pRsp->numOfCols = htonl(pRsp->numOfCols);
    pRsp->useconds = htobe64(pRsp->useconds);
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
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->info.ahandle;
  assert(pMsg->info.ahandle != NULL);

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

// NOTE: sources columns are more than the destination SSDatablock columns.
void relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols) {
  size_t numOfSrcCols = taosArrayGetSize(pCols);

  int32_t i = 0, j = 0;
  while (i < numOfSrcCols && j < taosArrayGetSize(pColMatchInfo)) {
    SColumnInfoData* p = taosArrayGet(pCols, i);
    SColMatchInfo*   pmInfo = taosArrayGet(pColMatchInfo, j);
    if (!pmInfo->output) {
      j++;
      continue;
    }

    if (p->info.colId == pmInfo->colId) {
      taosArraySet(pBlock->pDataBlock, pmInfo->targetSlotId, p);
      i++;
      j++;
    } else if (p->info.colId < pmInfo->colId) {
      i++;
    } else {
      ASSERT(0);
    }
  }
}

int32_t setSDataBlockFromFetchRsp(SSDataBlock* pRes, SLoadRemoteDataInfo* pLoadInfo, int32_t numOfRows, char* pData,
                                  int32_t compLen, int32_t numOfOutput, int64_t startTs, uint64_t* total,
                                  SArray* pColList) {
  if (pColList == NULL) {  // data from other sources
    blockDataEnsureCapacity(pRes, numOfRows);

    int32_t dataLen = *(int32_t*)pData;
    pData += sizeof(int32_t);

    pRes->info.groupId = *(uint64_t*)pData;
    pData += sizeof(uint64_t);

    int32_t* colLen = (int32_t*)pData;

    char* pStart = pData + sizeof(int32_t) * numOfOutput;
    for (int32_t i = 0; i < numOfOutput; ++i) {
      colLen[i] = htonl(colLen[i]);
      ASSERT(colLen[i] >= 0);

      SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, i);
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        pColInfoData->varmeta.length = colLen[i];
        pColInfoData->varmeta.allocLen = colLen[i];

        memcpy(pColInfoData->varmeta.offset, pStart, sizeof(int32_t) * numOfRows);
        pStart += sizeof(int32_t) * numOfRows;

        if (colLen[i] > 0) {
          pColInfoData->pData = taosMemoryMalloc(colLen[i]);
        }
      } else {
        memcpy(pColInfoData->nullbitmap, pStart, BitmapLen(numOfRows));
        pStart += BitmapLen(numOfRows);
      }

      if (colLen[i] > 0) {
        memcpy(pColInfoData->pData, pStart, colLen[i]);
      }

      // TODO setting this flag to true temporarily so aggregate function on stable will
      // examine NULL value for non-primary key column
      pColInfoData->hasNull = true;
      pStart += colLen[i];
    }
  } else {  // extract data according to pColList
    ASSERT(numOfOutput == taosArrayGetSize(pColList));
    char* pStart = pData;

    int32_t numOfCols = htonl(*(int32_t*)pStart);
    pStart += sizeof(int32_t);

    // todo refactor:extract method
    SSysTableSchema* pSchema = (SSysTableSchema*)pStart;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SSysTableSchema* p = (SSysTableSchema*)pStart;

      p->colId = htons(p->colId);
      p->bytes = htonl(p->bytes);
      pStart += sizeof(SSysTableSchema);
    }

    SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
    pBlock->info.numOfCols = numOfCols;

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData idata = {0};

      idata.info.type = pSchema[i].type;
      idata.info.bytes = pSchema[i].bytes;
      idata.info.colId = pSchema[i].colId;
      idata.hasNull = true;

      taosArrayPush(pBlock->pDataBlock, &idata);
      if (IS_VAR_DATA_TYPE(idata.info.type)) {
        pBlock->info.hasVarCol = true;
      }
    }

    blockDataEnsureCapacity(pBlock, numOfRows);

    int32_t  dataLen = *(int32_t*)pStart;
    uint64_t groupId = *(uint64_t*)(pStart + sizeof(int32_t));
    pStart += sizeof(int32_t) + sizeof(uint64_t);

    int32_t* colLen = (int32_t*)(pStart);
    pStart += sizeof(int32_t) * numOfCols;

    for (int32_t i = 0; i < numOfCols; ++i) {
      colLen[i] = htonl(colLen[i]);
      ASSERT(colLen[i] >= 0);

      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
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

    // data from mnode
    relocateColumnData(pRes, pColList, pBlock->pDataBlock);
    taosArrayDestroy(pBlock->pDataBlock);
    taosMemoryFree(pBlock);
    //    blockDataDestroy(pBlock);
  }

  pRes->info.rows = numOfRows;

  // todo move this to time window aggregator, since the primary timestamp may not be known by exchange operator.
  blockDataUpdateTsWindow(pRes, 0);

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
      code = setSDataBlockFromFetchRsp(pExchangeInfo->pResult, pLoadInfo, pTableRsp->numOfRows, pTableRsp->data,
                                       pTableRsp->compLen, pTableRsp->numOfCols, startTs, &pDataInfo->totalRows, NULL);
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
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " numOfRows:%d, totalRows:%" PRIu64
               ", totalBytes:%" PRIu64,
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

    SSourceDataInfo*       pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, taskID:0x%" PRIx64 " error happens, code:%s", GET_TASKID(pTaskInfo), pSource->addr.nodeId,
             pSource->taskId, tstrerror(pDataInfo->code));
      pOperator->pTaskInfo->code = pDataInfo->code;
      return NULL;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
    if (pRsp->numOfRows == 0) {
      qDebug("%s vgId:%d, taskID:0x%" PRIx64 " %d of total completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64
             " try next",
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
                                  pTableRsp->compLen, pTableRsp->numOfCols, startTs, &pDataInfo->totalRows, NULL);

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

    pOperator->resultInfo.totalRows += pRes->info.rows;
    return pExchangeInfo->pResult;
  }
}

static int32_t prepareLoadRemoteData(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t st = taosGetTimestampUs();

  SExchangeInfo* pExchangeInfo = pOperator->info;
  if (!pExchangeInfo->seqLoadData) {
    int32_t code = prepareConcurrentlyLoad(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  OPTR_SET_OPENED(pOperator);
  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
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

  if (pExchangeInfo->seqLoadData) {
    return seqLoadRemoteData(pOperator);
  } else {
    return concurrentlyLoadRemoteData(pOperator);
  }
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

SOperatorInfo* createExchangeOperatorInfo(void* pTransporter, const SNodeList* pSources, SSDataBlock* pBlock,
                                          SExecTaskInfo* pTaskInfo) {
  SExchangeInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
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

  pInfo->pResult = pBlock;
  pInfo->seqLoadData = true;

  tsem_init(&pInfo->ready, 0, 0);

  pOperator->name = "ExchangeOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfExprs = pBlock->info.numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(prepareLoadRemoteData, doLoadRemoteData, NULL, NULL,
                                         destroyExchangeOperatorInfo, NULL, NULL, NULL);
  pInfo->pTransporter = pTransporter;
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

static int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                                const char* pKey);

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
                                             //    pCtx[j].startRow = rowIndex;
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
    //    pCtx[j].fpSet.finalize(&pCtx[j]);
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
    //    pCtx[i].size = 1;
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
        int32_t numOfRows = getNumOfResult(pInfo->binfo.pCtx, pOperator->numOfExprs, NULL);
        //        setTagValueForMultipleRows(pCtx, pOperator->numOfExprs, numOfRows);

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

  SSDataBlock* pDataBlock = createOneDataBlock(pInfo->binfo.pRes, false);
  blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);

  while (1) {
    blockDataCleanup(pDataBlock);
    while (1) {
      STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
      if (pTupleHandle == NULL) {
        break;
      }

      // build datablock for merge for one group
      appendOneRowToDataBlock(pDataBlock, pTupleHandle);
      if (pDataBlock->info.rows >= pOperator->resultInfo.capacity) {
        break;
      }
    }

    if (pDataBlock->info.rows == 0) {
      break;
    }

    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pDataBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    //  updateOutputBuf(&pInfo->binfo, &pAggInfo->bufCapacity, pBlock->info.rows * pAggInfo->resultRowFactor,
    //  pOperator->pRuntimeEnv, true);
    doMergeImpl(pOperator, pOperator->numOfExprs, pDataBlock);
    // flush to tuple store, and after all data have been handled, return to upstream node or sink node
  }

  doFinalizeResultImpl(pInfo->binfo.pCtx, pOperator->numOfExprs);
  int32_t numOfRows = getNumOfResult(pInfo->binfo.pCtx, pOperator->numOfExprs, NULL);
  //        setTagValueForMultipleRows(pCtx, pOperator->numOfExprs, numOfRows);

  // TODO check for available buffer;

  // next group info data
  pInfo->binfo.pRes->info.rows += numOfRows;
  return (pInfo->binfo.pRes->info.rows > 0) ? pInfo->binfo.pRes : NULL;
}

static SSDataBlock* doSortedMerge(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SSortedMergeOperatorInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    return getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity, NULL);
  }

  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, NULL, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize,
                                             numOfBufPage, pInfo->binfo.pRes, "GET_TASKID(pTaskInfo)");

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, NULL, NULL);

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

  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  int32_t code = doInitAggInfoSup(&pInfo->aggSup, pInfo->binfo.pCtx, num, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setFunctionResultOutput(&pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, num, pTaskInfo);
  code = initGroupCol(pExprInfo, num, pGroupInfo, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  //  pInfo->resultRowFactor = (int32_t)(getRowNumForMultioutput(pRuntimeEnv->pQueryAttr,
  //      pRuntimeEnv->pQueryAttr->topBotQuery, false));
  pInfo->sortBufSize = 1024 * 16;  // 1MB
  pInfo->bufPageSize = 1024;
  pInfo->pSortInfo = pSortInfo;

  pOperator->resultInfo.capacity = blockDataGetCapacityInRow(pInfo->binfo.pRes, pInfo->bufPageSize);

  pOperator->name = "SortedMerge";
  // pOperator->operatorType = OP_SortedMerge;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfExprs = num;
  pOperator->pExpr = pExprInfo;

  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doSortedMerge, NULL, NULL, destroySortedMergeOperatorInfo,
                                         NULL, NULL, NULL);
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

int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag) {
  // todo add more information about exchange operation
  int32_t type = pOperator->operatorType;
  if (type == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE || type == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    *order = TSDB_ORDER_ASC;
    *scanFlag = MAIN_SCAN;
    return TSDB_CODE_SUCCESS;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = pOperator->info;
    *order = pTableScanInfo->cond.order;
    *scanFlag = pTableScanInfo->scanFlag;
    return TSDB_CODE_SUCCESS;
  } else {
    if (pOperator->pDownstream == NULL || pOperator->pDownstream[0] == NULL) {
      return TSDB_CODE_INVALID_PARA;
    } else {
      return getTableScanInfo(pOperator->pDownstream[0], order, scanFlag);
    }
  }
}

// this is a blocking operator
static int32_t doOpenAggregateOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  SAggOperatorInfo* pAggInfo = pOperator->info;

  SOptrBasicInfo* pInfo = &pAggInfo->binfo;
  SOperatorInfo*  downstream = pOperator->pDownstream[0];

  int64_t st = taosGetTimestampUs();

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    int32_t code = getTableScanInfo(pOperator, &order, &scanFlag);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, code);
    }

    // there is an scalar expression that needs to be calculated before apply the group aggregation.
    if (pAggInfo->pScalarExprInfo != NULL) {
      code = projectApplyFunctions(pAggInfo->pScalarExprInfo, pBlock, pBlock, pAggInfo->pScalarCtx,
                                   pAggInfo->numOfScalarExpr, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, code);
      }
    }

    // the pDataBlock are always the same one, no need to call this again
    setExecutionContext(pOperator->numOfExprs, pBlock->info.groupId, pTaskInfo, pAggInfo);
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, order, scanFlag, true);
    code = doAggregateImpl(pOperator, 0, pInfo->pCtx);
    if (code != 0) {
      longjmp(pTaskInfo->env, code);
    }

#if 0  // test for encode/decode result info
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

  closeAllResultRows(&pAggInfo->binfo.resultRowInfo);
  initGroupedResultInfo(&pAggInfo->groupResInfo, pAggInfo->aggSup.pResultRowHashTable, 0);
  OPTR_SET_OPENED(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* getAggregateResult(SOperatorInfo* pOperator) {
  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo*   pInfo = &pAggInfo->binfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    doSetOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  doBuildResultDatablock(pOperator, pInfo, &pAggInfo->groupResInfo, pAggInfo->aggSup.pResultBuf);
  if (pInfo->pRes->info.rows == 0 || !hashRemainDataInGroupInfo(&pAggInfo->groupResInfo)) {
    doSetOperatorCompleted(pOperator);
  }

  size_t rows = blockDataGetNumOfRows(pInfo->pRes);  // pInfo->pRes : NULL;
  pOperator->resultInfo.totalRows += rows;

  return (rows == 0) ? NULL : pInfo->pRes;
}

void aggEncodeResultRow(SOperatorInfo* pOperator, SAggSupporter* pSup, SOptrBasicInfo* pInfo, char** result,
                        int32_t* length) {
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
  SResultRowPosition* pos = &pInfo->resultRowInfo.cur;
  void*               pPage = getBufPage(pSup->pResultBuf, pos->pageId);
  SResultRow*         pRow = (SResultRow*)((char*)pPage + pos->offset);
  setBufPageDirty(pPage, true);
  releaseBufPage(pSup->pResultBuf, pPage);

  void* pIter = taosHashIterate(pSup->pResultRowHashTable, NULL);
  while (pIter) {
    void*               key = taosHashGetKey(pIter, &keyLen);
    SResultRowPosition* p1 = (SResultRowPosition*)pIter;

    pPage = (SFilePage*)getBufPage(pSup->pResultBuf, p1->pageId);
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

bool aggDecodeResultRow(SOperatorInfo* pOperator, SAggSupporter* pSup, SOptrBasicInfo* pInfo, char* result,
                        int32_t length) {
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
    //    pInfo->resultRowInfo.cur = pInfo->resultRowInfo.size;
    //    pInfo->resultRowInfo.pPosition[pInfo->resultRowInfo.size++] =
    //        (SResultRowPosition){.pageId = resultRow->pageId, .offset = resultRow->offset};
    pInfo->resultRowInfo.cur = (SResultRowPosition){.pageId = resultRow->pageId, .offset = resultRow->offset};
  }

  if (offset != length) {
    longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_INVALID_INPUT);
  }
  return true;
}

enum {
  PROJECT_RETRIEVE_CONTINUE = 0x1,
  PROJECT_RETRIEVE_DONE = 0x2,
};

static int32_t handleLimitOffset(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;
  SSDataBlock*          pRes = pInfo->pRes;

  if (pProjectInfo->curSOffset > 0) {
    if (pProjectInfo->groupId == 0) {  // it is the first group
      pProjectInfo->groupId = pBlock->info.groupId;
      blockDataCleanup(pInfo->pRes);
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pProjectInfo->groupId != pBlock->info.groupId) {
      pProjectInfo->curSOffset -= 1;

      // ignore data block in current group
      if (pProjectInfo->curSOffset > 0) {
        blockDataCleanup(pInfo->pRes);
        return PROJECT_RETRIEVE_CONTINUE;
      }
    }

    // set current group id of the project operator
    pProjectInfo->groupId = pBlock->info.groupId;
  }

  if (pProjectInfo->groupId != 0 && pProjectInfo->groupId != pBlock->info.groupId) {
    pProjectInfo->curGroupOutput += 1;
    if ((pProjectInfo->slimit.limit > 0) && (pProjectInfo->slimit.limit <= pProjectInfo->curGroupOutput)) {
      pOperator->status = OP_EXEC_DONE;
      blockDataCleanup(pRes);

      return PROJECT_RETRIEVE_DONE;
    }

    // reset the value for a new group data
    pProjectInfo->curOffset = 0;
    pProjectInfo->curOutput = 0;
  }

  // here we reach the start position, according to the limit/offset requirements.

  // set current group id
  pProjectInfo->groupId = pBlock->info.groupId;

  if (pProjectInfo->curOffset >= pRes->info.rows) {
    pProjectInfo->curOffset -= pRes->info.rows;
    blockDataCleanup(pRes);
    return PROJECT_RETRIEVE_CONTINUE;
  } else if (pProjectInfo->curOffset < pRes->info.rows && pProjectInfo->curOffset > 0) {
    blockDataTrimFirstNRows(pRes, pProjectInfo->curOffset);
    pProjectInfo->curOffset = 0;
  }

  // check for the limitation in each group
  if (pProjectInfo->limit.limit > 0 && pProjectInfo->curOutput + pRes->info.rows >= pProjectInfo->limit.limit) {
    pRes->info.rows = (int32_t)(pProjectInfo->limit.limit - pProjectInfo->curOutput);

    if (pProjectInfo->slimit.limit == -1 || pProjectInfo->slimit.limit <= pProjectInfo->curGroupOutput) {
      pOperator->status = OP_EXEC_DONE;
    }

    return PROJECT_RETRIEVE_DONE;
  }

  // todo optimize performance
  // If there are slimit/soffset value exists, multi-round result can not be packed into one group, since the
  // they may not belong to the same group the limit/offset value is not valid in this case.
  if (pRes->info.rows >= pOperator->resultInfo.threshold || pProjectInfo->slimit.offset != -1 ||
      pProjectInfo->slimit.limit != -1) {
    return PROJECT_RETRIEVE_DONE;
  } else {  // not full enough, continue to accumulate the output data in the buffer.
    return PROJECT_RETRIEVE_CONTINUE;
  }
}

static SSDataBlock* doProjectOperation(SOperatorInfo* pOperator) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;

  SSDataBlock* pRes = pInfo->pRes;
  blockDataCleanup(pRes);

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

#if 0
  if (pProjectInfo->existDataBlock) {  // TODO refactor
    SSDataBlock* pBlock = pProjectInfo->existDataBlock;
    pProjectInfo->existDataBlock = NULL;

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, TSDB_ORDER_ASC);

    blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
    projectApplyFunctions(pOperator->pExpr, pInfo->pRes, pBlock, pInfo->pCtx, pOperator->numOfExprs);
    if (pRes->info.rows >= pProjectInfo->binfo.capacity * 0.8) {
      copyTsColoum(pRes, pInfo->pCtx, pOperator->numOfExprs);
      resetResultRowEntryResult(pInfo->pCtx, pOperator->numOfExprs);
      return pRes;
    }
  }
#endif

  int64_t st = 0;
  int32_t order = 0;
  int32_t scanFlag = 0;

  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    // The downstream exec may change the value of the newgroup, so use a local variable instead.
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      doSetOperatorCompleted(pOperator);
      break;
    }

#if 0
    // Return result of the previous group in the firstly.
    if (false) {
      if (pRes->info.rows > 0) {
        pProjectInfo->existDataBlock = pBlock;
        break;
      } else {  // init output buffer for a new group data
        initCtxOutputBuffer(pInfo->pCtx, pOperator->numOfExprs);
      }
    }
#endif

    // the pDataBlock are always the same one, no need to call this again
    int32_t code = getTableScanInfo(pOperator->pDownstream[0], &order, &scanFlag);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, code);
    }

    setInputDataBlock(pOperator, pInfo->pCtx, pBlock, order, scanFlag, false);
    blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);

    code = projectApplyFunctions(pOperator->pExpr, pInfo->pRes, pBlock, pInfo->pCtx, pOperator->numOfExprs,
                                 pProjectInfo->pPseudoColInfo);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, code);
    }

    int32_t status = handleLimitOffset(pOperator, pBlock);
    if (status == PROJECT_RETRIEVE_CONTINUE) {
      continue;
    } else if (status == PROJECT_RETRIEVE_DONE) {
      break;
    }
  }

  pProjectInfo->curOutput += pInfo->pRes->info.rows;

  size_t rows = pInfo->pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  return (rows > 0) ? pInfo->pRes : NULL;
}

static void doHandleRemainBlockForNewGroupImpl(SFillOperatorInfo* pInfo, SResultInfo* pResultInfo, bool* newgroup,
                                               SExecTaskInfo* pTaskInfo) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;

  int64_t ekey = Q_STATUS_EQUAL(pTaskInfo->status, TASK_COMPLETED) ? pTaskInfo->window.ekey
                                                                   : pInfo->existNewGroupBlock->info.window.ekey;
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->existNewGroupBlock->info.rows, ekey);
  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->existNewGroupBlock);

  doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, pResultInfo->capacity);
  pInfo->existNewGroupBlock = NULL;
  *newgroup = true;
}

static void doHandleRemainBlockFromNewGroup(SFillOperatorInfo* pInfo, SResultInfo* pResultInfo, bool* newgroup,
                                            SExecTaskInfo* pTaskInfo) {
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    *newgroup = false;
    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pInfo->pRes, (int32_t)pResultInfo->capacity);
    if (pInfo->pRes->info.rows > pResultInfo->threshold || (!pInfo->multigroupResult)) {
      return;
    }
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pInfo, pResultInfo, newgroup, pTaskInfo);
  }
}

static SSDataBlock* doFill(SOperatorInfo* pOperator) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  SSDataBlock* pResBlock = pInfo->pRes;

  blockDataCleanup(pResBlock);
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // todo handle different group data interpolation
  bool  n = false;
  bool* newgroup = &n;
  doHandleRemainBlockFromNewGroup(pInfo, pResultInfo, newgroup, pTaskInfo);
  if (pResBlock->info.rows > pResultInfo->threshold || (!pInfo->multigroupResult && pResBlock->info.rows > 0)) {
    return pResBlock;
  }

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = pDownstream->fpSet.getNextFn(pDownstream);
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

    blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);
    doFillTimeIntervalGapsInResults(pInfo->pFillInfo, pResBlock, pOperator->resultInfo.capacity);

    // current group has no more result to return
    if (pResBlock->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pResBlock->info.rows > pResultInfo->threshold || pBlock == NULL || (!pInfo->multigroupResult)) {
        return pResBlock;
      }

      doHandleRemainBlockFromNewGroup(pInfo, pResultInfo, newgroup, pTaskInfo);
      if (pResBlock->info.rows > pOperator->resultInfo.threshold || pBlock == NULL) {
        return pResBlock;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      assert(pBlock != NULL);
      doHandleRemainBlockForNewGroupImpl(pInfo, pResultInfo, newgroup, pTaskInfo);
      if (pResBlock->info.rows > pResultInfo->threshold) {
        return pResBlock;
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

  if (pOperator->fpSet.closeFn != NULL) {
    pOperator->fpSet.closeFn(pOperator->info, pOperator->numOfExprs);
  }

  if (pOperator->pDownstream != NULL) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      destroyOperatorInfo(pOperator->pDownstream[i]);
    }

    taosMemoryFreeClear(pOperator->pDownstream);
    pOperator->numOfDownstream = 0;
  }

  if (pOperator->pExpr != NULL) {
    for (int32_t i = 0; i < pOperator->numOfExprs; ++i) {
      SExprInfo* pExprInfo = &pOperator->pExpr[i];
      if (pExprInfo->pExpr->nodeType == QUERY_NODE_COLUMN) {
        taosMemoryFree(pExprInfo->base.pParam[0].pCol);
      }
      taosMemoryFree(pExprInfo->base.pParam);
      taosMemoryFree(pExprInfo->pExpr);
    }
  }

  taosMemoryFreeClear(pOperator->pExpr);
  taosMemoryFreeClear(pOperator->info);
  taosMemoryFreeClear(pOperator);
}

int32_t getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, uint32_t* defaultBufsz) {
  *defaultPgsz = 4096;
  while (*defaultPgsz < rowSize * 4) {
    *defaultPgsz <<= 1u;
  }

  // at least four pages need to be in buffer
  *defaultBufsz = 4096 * 256;
  if ((*defaultBufsz) <= (*defaultPgsz)) {
    (*defaultBufsz) = (*defaultPgsz) * 4;
  }

  return 0;
}

int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                         const char* pKey) {
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  pAggSup->resultRowSize = getResultRowSize(pCtx, numOfOutput);
  pAggSup->keyBuf = taosMemoryCalloc(1, keyBufSize + POINTER_BYTES + sizeof(int64_t));
  pAggSup->pResultRowHashTable = taosHashInit(10, hashFn, true, HASH_NO_LOCK);

  if (pAggSup->keyBuf == NULL || pAggSup->pResultRowHashTable == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  uint32_t defaultPgsz  = 0;
  uint32_t defaultBufsz = 0;
  getBufferPgSize(pAggSup->resultRowSize, &defaultPgsz, &defaultBufsz);

  int32_t  code = createDiskbasedBuf(&pAggSup->pResultBuf, defaultPgsz, defaultBufsz, pKey, TD_TMP_DIR_PATH);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

void cleanupAggSup(SAggSupporter* pAggSup) {
  taosMemoryFreeClear(pAggSup->keyBuf);
  taosHashCleanup(pAggSup->pResultRowHashTable);
  destroyDiskbasedBuf(pAggSup->pResultBuf);
}

int32_t initAggInfo(SOptrBasicInfo* pBasicInfo, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols,
                    SSDataBlock* pResultBlock, size_t keyBufSize, const char* pkey) {
  pBasicInfo->pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pBasicInfo->rowCellInfoOffset);
  pBasicInfo->pRes = pResultBlock;

  doInitAggInfoSup(pAggSup, pBasicInfo->pCtx, numOfCols, keyBufSize, pkey);

  for (int32_t i = 0; i < numOfCols; ++i) {
    pBasicInfo->pCtx[i].pBuf = pAggSup->pResultBuf;
  }

  return TSDB_CODE_SUCCESS;
}

void initResultSizeInfo(SOperatorInfo* pOperator, int32_t numOfRows) {
  pOperator->resultInfo.capacity = numOfRows;
  pOperator->resultInfo.threshold = numOfRows * 0.75;

  if (pOperator->resultInfo.threshold == 0) {
    pOperator->resultInfo.capacity = numOfRows;
  }
}

//static STableQueryInfo* initTableQueryInfo(const STableListInfo* pTableListInfo) {
//  int32_t size = taosArrayGetSize(pTableListInfo->pTableList);
//  if (size == 0) {
//    return NULL;
//  }
//
//  STableQueryInfo* pTableQueryInfo = taosMemoryCalloc(size, sizeof(STableQueryInfo));
//  if (pTableQueryInfo == NULL) {
//    return NULL;
//  }
//
//  for (int32_t j = 0; j < size; ++j) {
//    STableKeyInfo*   pk = taosArrayGet(pTableListInfo->pTableList, j);
//    STableQueryInfo* pTQueryInfo = &pTableQueryInfo[j];
//    pTQueryInfo->lastKey = pk->lastKey;
//  }
//
//  pTableQueryInfo->lastKey = 0;
//  return pTableQueryInfo;
//}

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, SExprInfo* pScalarExprInfo,
                                           int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo) {
  SAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAggOperatorInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t numOfRows = 1024;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(pOperator, numOfRows);
  int32_t code =
      initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResultBlock, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t numOfGroup = 10;  // todo replaced with true value
  pInfo->groupId = INT32_MIN;
  initResultRowInfo(&pInfo->binfo.resultRowInfo, numOfGroup);

  pInfo->pScalarExprInfo = pScalarExprInfo;
  pInfo->numOfScalarExpr = numOfScalarExpr;
  if (pInfo->pScalarExprInfo != NULL) {
    pInfo->pScalarCtx = createSqlFunctionCtx(pScalarExprInfo, numOfScalarExpr, &pInfo->rowCellInfoOffset);
  }

  pOperator->name = "TableAggregate";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_AGG;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pExpr = pExprInfo;
  pOperator->numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(doOpenAggregateOptr, getAggregateResult, NULL, NULL, destroyAggOperatorInfo,
                                         aggEncodeResultRow, aggDecodeResultRow, NULL);

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

void destroyAggOperatorInfo(void* param, int32_t numOfOutput) {
  SAggOperatorInfo* pInfo = (SAggOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
}

void destroySFillOperatorInfo(void* param, int32_t numOfOutput) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*)param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  taosMemoryFreeClear(pInfo->p);
}

static void destroyProjectOperatorInfo(void* param, int32_t numOfOutput) {
  if (NULL == param) {
    return;
  }
  SProjectOperatorInfo* pInfo = (SProjectOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  cleanupAggSup(&pInfo->aggSup);
  taosArrayDestroy(pInfo->pPseudoColInfo);
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

static SArray* setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols) {
  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  for (int32_t i = 0; i < numOfCols; ++i) {
    if (fmIsPseudoColumnFunc(pCtx[i].functionId)) {
      taosArrayPush(pList, &i);
    }
  }

  return pList;
}

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t num,
                                         SSDataBlock* pResBlock, SLimit* pLimit, SLimit* pSlimit,
                                         SExecTaskInfo* pTaskInfo) {
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->limit = *pLimit;
  pInfo->slimit = *pSlimit;
  pInfo->curOffset = pLimit->offset;
  pInfo->curSOffset = pSlimit->offset;

  pInfo->binfo.pRes = pResBlock;

  int32_t numOfCols = num;
  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }
  initResultSizeInfo(pOperator, numOfRows);

  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResBlock, keyBufSize, pTaskInfo->id.str);
  setFunctionResultOutput(&pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfCols, pTaskInfo);

  pInfo->pPseudoColInfo = setRowTsColumnOutputInfo(pInfo->binfo.pCtx, numOfCols);
  pOperator->name = "ProjectOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_PROJECT;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pExpr = pExprInfo;
  pOperator->numOfExprs = num;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doProjectOperation, NULL, NULL,
                                         destroyProjectOperatorInfo, NULL, NULL, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

static int32_t initFillInfo(SFillOperatorInfo* pInfo, SExprInfo* pExpr, int32_t numOfCols, SNodeListNode* pValNode,
                            STimeWindow win, int32_t capacity, const char* id, SInterval* pInterval, int32_t fillType) {
  SFillColInfo* pColInfo = createFillColInfo(pExpr, numOfCols, pValNode);

  STimeWindow w = TSWINDOW_INITIALIZER;
  getAlignQueryTimeWindow(pInterval, pInterval->precision, win.skey, &w);

  int32_t order = TSDB_ORDER_ASC;
  pInfo->pFillInfo = taosCreateFillInfo(order, w.skey, 0, capacity, numOfCols, pInterval, fillType, pColInfo, id);

  pInfo->p = taosMemoryCalloc(numOfCols, POINTER_BYTES);
  if (pInfo->pFillInfo == NULL || pInfo->p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                      SInterval* pInterval, STimeWindow* pWindow, SSDataBlock* pResBlock,
                                      int32_t fillType, SNodeListNode* pValueNode, bool multigroupResult,
                                      SExecTaskInfo* pTaskInfo) {
  SFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SFillOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  pInfo->pRes = pResBlock;
  pInfo->multigroupResult = multigroupResult;

  int32_t type = TSDB_FILL_NONE;
  switch (fillType) {
    case FILL_MODE_PREV:
      type = TSDB_FILL_PREV;
      break;
    case FILL_MODE_NONE:
      type = TSDB_FILL_NONE;
      break;
    case FILL_MODE_NULL:
      type = TSDB_FILL_NULL;
      break;
    case FILL_MODE_NEXT:
      type = TSDB_FILL_NEXT;
      break;
    case FILL_MODE_VALUE:
      type = TSDB_FILL_SET_VALUE;
      break;
    case FILL_MODE_LINEAR:
      type = TSDB_FILL_LINEAR;
      break;
    default:
      type = TSDB_FILL_NONE;
  }

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  initResultSizeInfo(pOperator, 4096);

  int32_t code = initFillInfo(pInfo, pExpr, numOfCols, pValueNode, *pWindow, pResultInfo->capacity, pTaskInfo->id.str,
                              pInterval, type);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name = "FillOperator";
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_FILL;
  pOperator->pExpr = pExpr;
  pOperator->numOfExprs = numOfCols;
  pOperator->info = pInfo;

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doFill, NULL, NULL, destroySFillOperatorInfo, NULL, NULL, NULL);
  pOperator->pTaskInfo = pTaskInfo;
  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  taosMemoryFreeClear(pOperator);
  taosMemoryFreeClear(pInfo);
  return NULL;
}

static SResSchema createResSchema(int32_t type, int32_t bytes, int32_t slotId, int32_t scale, int32_t precision,
                                  const char* name) {
  SResSchema s = {0};
  s.scale = scale;
  s.type = type;
  s.bytes = bytes;
  s.slotId = slotId;
  s.precision = precision;
  strncpy(s.name, name, tListLen(s.name));

  return s;
}

static SColumn* createColumn(int32_t blockId, int32_t slotId, int32_t colId, SDataType* pType) {
  SColumn* pCol = taosMemoryCalloc(1, sizeof(SColumn));
  if (pCol == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCol->slotId = slotId;
  pCol->colId = colId;
  pCol->bytes = pType->bytes;
  pCol->type = pType->type;
  pCol->scale = pType->scale;
  pCol->precision = pType->precision;
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

    int32_t type = nodeType(pTargetNode->pExpr);
    // it is a project query, or group by column
    if (type == QUERY_NODE_COLUMN) {
      pExp->pExpr->nodeType = QUERY_NODE_COLUMN;
      SColumnNode* pColNode = (SColumnNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pColNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pColNode->colName);
      pExp->base.pParam[0].pCol = createColumn(pColNode->dataBlockId, pColNode->slotId, pColNode->colId, pType);
      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_COLUMN;
    } else if (type == QUERY_NODE_VALUE) {
      pExp->pExpr->nodeType = QUERY_NODE_VALUE;
      SValueNode* pValNode = (SValueNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pValNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pValNode->node.aliasName);
      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_VALUE;
      valueNodeToVariant(pValNode, &pExp->base.pParam[0].param);
    } else if (type == QUERY_NODE_FUNCTION) {
      pExp->pExpr->nodeType = QUERY_NODE_FUNCTION;
      SFunctionNode* pFuncNode = (SFunctionNode*)pTargetNode->pExpr;

      SDataType* pType = &pFuncNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pFuncNode->node.aliasName);

      pExp->pExpr->_function.functionId = pFuncNode->funcId;
      pExp->pExpr->_function.pFunctNode = pFuncNode;

      strncpy(pExp->pExpr->_function.functionName, pFuncNode->functionName,
              tListLen(pExp->pExpr->_function.functionName));
#if 1
      // todo refactor: add the parameter for tbname function
      if (strcmp(pExp->pExpr->_function.functionName, "tbname") == 0) {
        pFuncNode->pParameterList = nodesMakeList();
        ASSERT(LIST_LENGTH(pFuncNode->pParameterList) == 0);
        SValueNode* res = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
        if (NULL == res) {  // todo handle error
        } else {
          res->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_BIGINT};
          nodesListAppend(pFuncNode->pParameterList, res);
        }
      }
#endif

      int32_t numOfParam = LIST_LENGTH(pFuncNode->pParameterList);

      pExp->base.pParam = taosMemoryCalloc(numOfParam, sizeof(SFunctParam));
      pExp->base.numOfParams = numOfParam;

      for (int32_t j = 0; j < numOfParam; ++j) {
        SNode* p1 = nodesListGetNode(pFuncNode->pParameterList, j);
        if (p1->type == QUERY_NODE_COLUMN) {
          SColumnNode* pcn = (SColumnNode*)p1;

          pExp->base.pParam[j].type = FUNC_PARAM_TYPE_COLUMN;
          pExp->base.pParam[j].pCol = createColumn(pcn->dataBlockId, pcn->slotId, pcn->colId, &pcn->node.resType);
        } else if (p1->type == QUERY_NODE_VALUE) {
          SValueNode* pvn = (SValueNode*)p1;
          pExp->base.pParam[j].type = FUNC_PARAM_TYPE_VALUE;
          valueNodeToVariant(pvn, &pExp->base.pParam[j].param);
        }
      }
    } else if (type == QUERY_NODE_OPERATOR) {
      pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
      SOperatorNode* pNode = (SOperatorNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pNode->node.aliasName);
      pExp->pExpr->_optrRoot.pRootNode = pTargetNode->pExpr;
    } else {
      ASSERT(0);
    }
  }

  return pExprs;
}

static SExecTaskInfo* createExecTaskInfo(uint64_t queryId, uint64_t taskId, EOPTR_EXEC_MODEL model, char* dbFName) {
  SExecTaskInfo* pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);

  pTaskInfo->schemaVer.dbname = strdup(dbFName);
  pTaskInfo->cost.created = taosGetTimestampMs();
  pTaskInfo->id.queryId = queryId;
  pTaskInfo->execModel = model;

  char* p = taosMemoryCalloc(1, 128);
  snprintf(p, 128, "TID:0x%" PRIx64 " QID:0x%" PRIx64, taskId, queryId);
  pTaskInfo->id.str = p;

  return pTaskInfo;
}

static tsdbReaderT doCreateDataReader(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle,
                                      STableListInfo* pTableGroupInfo, uint64_t queryId, uint64_t taskId, SNode* pTagCond);

static int32_t getTableList(void* metaHandle, int32_t tableType, uint64_t tableUid, STableListInfo* pListInfo, SNode* pTagCond);
static SArray* extractTableIdList(const STableListInfo* pTableGroupInfo);
static SArray* extractColumnInfo(SNodeList* pNodeList);

static SArray* createSortInfo(SNodeList* pNodeList);
static SArray* extractPartitionColInfo(SNodeList* pNodeList);

void extractTableSchemaVersion(SReadHandle* pHandle, uint64_t uid, SExecTaskInfo* pTaskInfo) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, pHandle->meta, 0);
  metaGetTableEntryByUid(&mr, uid);

  pTaskInfo->schemaVer.tablename = strdup(mr.me.name);

  if (mr.me.type == TSDB_SUPER_TABLE) {
    pTaskInfo->schemaVer.sversion = mr.me.stbEntry.schemaRow.version;
    pTaskInfo->schemaVer.tversion = mr.me.stbEntry.schemaTag.version;
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    tb_uid_t suid = mr.me.ctbEntry.suid;
    metaGetTableEntryByUid(&mr, suid);
    pTaskInfo->schemaVer.sversion = mr.me.stbEntry.schemaRow.version;
    pTaskInfo->schemaVer.tversion = mr.me.stbEntry.schemaTag.version;
  } else {
    pTaskInfo->schemaVer.sversion = mr.me.ntbEntry.schemaRow.version;
  }

  metaReaderClear(&mr);
}

SOperatorInfo* createOperatorTree(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle,
                                  uint64_t queryId, uint64_t taskId, STableListInfo* pTableListInfo, SNode* pTagCond) {
  int32_t type = nodeType(pPhyNode);

  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;

      tsdbReaderT pDataReader = doCreateDataReader(pTableScanNode, pHandle, pTableListInfo, (uint64_t)queryId, taskId, pTagCond);
      if (pDataReader == NULL && terrno != 0) {
        return NULL;
      }

      extractTableSchemaVersion(pHandle, pTableScanNode->scan.uid, pTaskInfo);
      SOperatorInfo* pOperator = createTableScanOperatorInfo(pTableScanNode, pDataReader, pHandle, pTaskInfo);

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->readRecorder;

      return pOperator;
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == type) {
      SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pPhyNode;
      SSDataBlock*        pResBlock = createResDataBlock(pExchange->node.pOutputDataBlockDesc);
      return createExchangeOperatorInfo(pHandle->pMsgCb->clientRpc, pExchange->pSrcEndPoints, pResBlock, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
      SScanPhysiNode*      pScanPhyNode = (SScanPhysiNode*)pPhyNode;  // simple child table.
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;

      int32_t numOfCols = 0;

      tsdbReaderT pDataReader = NULL;
      if (pHandle->vnode) {
        pDataReader = doCreateDataReader(pTableScanNode, pHandle, pTableListInfo, (uint64_t)queryId, taskId, pTagCond);
      } else {
        getTableList(pHandle->meta, pScanPhyNode->tableType, pScanPhyNode->uid, pTableListInfo, pTagCond);
      }

      if (pDataReader == NULL && terrno != 0) {
        qDebug("pDataReader is     NULL");
        // return NULL;
      } else {
        qDebug("pDataReader is not NULL");
      }

      SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;
      SOperatorInfo*      pOperatorDumy = createTableScanOperatorInfo(pTableScanNode, pDataReader, pHandle, pTaskInfo);

      SArray* tableIdList = extractTableIdList(pTableListInfo);

      SSDataBlock* pResBlock = createResDataBlock(pDescNode);
      SArray*      pCols =
          extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, pTaskInfo, COL_MATCH_FROM_COL_ID);

      SOperatorInfo* pOperator =
          createStreamScanOperatorInfo(pHandle->reader, pDataReader, pHandle, pScanPhyNode->uid, pResBlock, pCols,
                                       tableIdList, pTaskInfo, pScanPhyNode->node.pConditions, pOperatorDumy);
      taosArrayDestroy(tableIdList);
      return pOperator;
    } else if (QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == type) {
      SSystemTableScanPhysiNode* pSysScanPhyNode = (SSystemTableScanPhysiNode*)pPhyNode;
      SScanPhysiNode*            pScanNode = &pSysScanPhyNode->scan;

      SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

      SSDataBlock* pResBlock = createResDataBlock(pDescNode);

      int32_t numOfOutputCols = 0;
      SArray* colList =
          extractColMatchInfo(pScanNode->pScanCols, pDescNode, &numOfOutputCols, pTaskInfo, COL_MATCH_FROM_COL_ID);
      SOperatorInfo* pOperator = createSysTableScanOperatorInfo(
          pHandle, pResBlock, &pScanNode->tableName, pScanNode->node.pConditions, pSysScanPhyNode->mgmtEpSet, colList,
          pTaskInfo, pSysScanPhyNode->showRewrite, pSysScanPhyNode->accountId);
      return pOperator;
    } else if (QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN == type) {
      STagScanPhysiNode* pScanPhyNode = (STagScanPhysiNode*)pPhyNode;

      SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;

      SSDataBlock* pResBlock = createResDataBlock(pDescNode);

      int32_t code = getTableList(pHandle->meta, pScanPhyNode->tableType, pScanPhyNode->uid, pTableListInfo, pTagCond);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }

      int32_t    num = 0;
      SExprInfo* pExprInfo = createExprInfo(pScanPhyNode->pScanPseudoCols, NULL, &num);

      int32_t numOfOutputCols = 0;
      SArray* colList = extractColMatchInfo(pScanPhyNode->pScanPseudoCols, pDescNode, &numOfOutputCols, pTaskInfo,
                                            COL_MATCH_FROM_COL_ID);

      SOperatorInfo* pOperator =
          createTagScanOperatorInfo(pHandle, pExprInfo, num, pResBlock, colList, pTableListInfo, pTaskInfo);
      return pOperator;
    } else {
      ASSERT(0);
    }
  }

  int32_t num = 0;
  size_t  size = LIST_LENGTH(pPhyNode->pChildren);

  SOperatorInfo** ops = taosMemoryCalloc(size, POINTER_BYTES);
  for (int32_t i = 0; i < size; ++i) {
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
    ops[i] = createOperatorTree(pChildNode, pTaskInfo, pHandle, queryId, taskId, pTableListInfo, pTagCond);
    if (ops[i] == NULL) {
      return NULL;
    }
  }

  SOperatorInfo* pOptr = NULL;
  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
    SProjectPhysiNode* pProjPhyNode = (SProjectPhysiNode*)pPhyNode;
    SExprInfo*         pExprInfo = createExprInfo(pProjPhyNode->pProjections, NULL, &num);

    SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
    SLimit       limit = {.limit = pProjPhyNode->limit, .offset = pProjPhyNode->offset};
    SLimit       slimit = {.limit = pProjPhyNode->slimit, .offset = pProjPhyNode->soffset};
    pOptr = createProjectOperatorInfo(ops[0], pExprInfo, num, pResBlock, &limit, &slimit, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_AGG == type) {
    SAggPhysiNode* pAggNode = (SAggPhysiNode*)pPhyNode;
    SExprInfo*     pExprInfo = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &num);
    SSDataBlock*   pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);

    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    if (pAggNode->pExprs != NULL) {
      pScalarExprInfo = createExprInfo(pAggNode->pExprs, NULL, &numOfScalarExpr);
    }

    if (pAggNode->pGroupKeys != NULL) {
      SArray* pColList = extractColumnInfo(pAggNode->pGroupKeys);
      pOptr = createGroupOperatorInfo(ops[0], pExprInfo, num, pResBlock, pColList, pAggNode->node.pConditions,
                                      pScalarExprInfo, numOfScalarExpr, pTaskInfo);
    } else {
      pOptr = createAggregateOperatorInfo(ops[0], pExprInfo, num, pResBlock, pScalarExprInfo, numOfScalarExpr,
                                          pTaskInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_INTERVAL == type || QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL == type) {
    SIntervalPhysiNode* pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;

    SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);

    SInterval interval = {.interval = pIntervalPhyNode->interval,
                          .sliding = pIntervalPhyNode->sliding,
                          .intervalUnit = pIntervalPhyNode->intervalUnit,
                          .slidingUnit = pIntervalPhyNode->slidingUnit,
                          .offset = pIntervalPhyNode->offset,
                          .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};

    STimeWindowAggSupp as = {.waterMark = pIntervalPhyNode->window.watermark,
                             .calTrigger = pIntervalPhyNode->window.triggerType};

    int32_t tsSlotId = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
    pOptr = createIntervalOperatorInfo(ops[0], pExprInfo, num, pResBlock, &interval, tsSlotId, &as, pTaskInfo);

  } else if (QUERY_NODE_PHYSICAL_PLAN_SORT == type) {
    SSortPhysiNode* pSortPhyNode = (SSortPhysiNode*)pPhyNode;

    SDataBlockDescNode* pDescNode = pPhyNode->pOutputDataBlockDesc;

    SSDataBlock* pResBlock = createResDataBlock(pDescNode);
    SArray*      info = createSortInfo(pSortPhyNode->pSortKeys);

    int32_t    numOfCols = 0;
    SExprInfo* pExprInfo = createExprInfo(pSortPhyNode->pExprs, NULL, &numOfCols);

    int32_t numOfOutputCols = 0;
    SArray* pColList =
        extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, pTaskInfo, COL_MATCH_FROM_SLOT_ID);

    pOptr = createSortOperatorInfo(ops[0], pResBlock, info, pExprInfo, numOfCols, pColList, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW == type) {
    SSessionWinodwPhysiNode* pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;

    STimeWindowAggSupp as = {.waterMark = pSessionNode->window.watermark,
                             .calTrigger = pSessionNode->window.triggerType};

    SExprInfo*   pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
    int32_t      tsSlotId = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;

    pOptr =
        createSessionAggOperatorInfo(ops[0], pExprInfo, num, pResBlock, pSessionNode->gap, tsSlotId, &as, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION_WINDOW == type) {
    SSessionWinodwPhysiNode* pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;

    STimeWindowAggSupp as = {.waterMark = pSessionNode->window.watermark,
                             .calTrigger = pSessionNode->window.triggerType};

    SExprInfo*   pExprInfo = createExprInfo(pSessionNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
    int32_t      tsSlotId = ((SColumnNode*)pSessionNode->window.pTspk)->slotId;

    pOptr = createStreamSessionAggOperatorInfo(ops[0], pExprInfo, num, pResBlock, pSessionNode->gap, tsSlotId, &as,
                                               pTaskInfo);

  } else if (QUERY_NODE_PHYSICAL_PLAN_PARTITION == type) {
    SPartitionPhysiNode* pPartNode = (SPartitionPhysiNode*)pPhyNode;
    SArray*              pColList = extractPartitionColInfo(pPartNode->pPartitionKeys);
    SSDataBlock*         pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);

    SExprInfo* pExprInfo = createExprInfo(pPartNode->pTargets, NULL, &num);
    pOptr = createPartitionOperatorInfo(ops[0], pExprInfo, num, pResBlock, pColList, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STATE_WINDOW == type) {
    SStateWinodwPhysiNode* pStateNode = (SStateWinodwPhysiNode*)pPhyNode;

    STimeWindowAggSupp as = {.waterMark = pStateNode->window.watermark, .calTrigger = pStateNode->window.triggerType};

    SExprInfo*   pExprInfo = createExprInfo(pStateNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
    int32_t      tsSlotId = ((SColumnNode*)pStateNode->window.pTspk)->slotId;

    SColumnNode* pColNode = (SColumnNode*)((STargetNode*)pStateNode->pStateKey)->pExpr;
    SColumn      col = extractColumnFromColumnNode(pColNode);
    pOptr = createStatewindowOperatorInfo(ops[0], pExprInfo, num, pResBlock, &as, tsSlotId, &col, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_JOIN == type) {
    SJoinPhysiNode* pJoinNode = (SJoinPhysiNode*)pPhyNode;
    SSDataBlock*    pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);

    SExprInfo* pExprInfo = createExprInfo(pJoinNode->pTargets, NULL, &num);
    pOptr = createMergeJoinOperatorInfo(ops, size, pExprInfo, num, pResBlock, pJoinNode->pOnConditions, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_FILL == type) {
    SFillPhysiNode* pFillNode = (SFillPhysiNode*)pPhyNode;
    SSDataBlock*    pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);
    SExprInfo*      pExprInfo = createExprInfo(pFillNode->pTargets, NULL, &num);

    SInterval* pInterval = &((SIntervalAggOperatorInfo*)ops[0]->info)->interval;
    pOptr = createFillOperatorInfo(ops[0], pExprInfo, num, pInterval, &pFillNode->timeRange, pResBlock, pFillNode->mode,
                                   (SNodeListNode*)pFillNode->pValues, false, pTaskInfo);
  } else {
    ASSERT(0);
  }

  taosMemoryFree(ops);
  return pOptr;
}

int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode) {
  pCond->loadExternalRows = false;

  pCond->order = pTableScanNode->scanSeq[0] > 0 ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  pCond->numOfCols = LIST_LENGTH(pTableScanNode->scan.pScanCols);
  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  if (pCond->colList == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return terrno;
  }

  pCond->twindow = pTableScanNode->scanRange;

#if 1
  // todo work around a problem, remove it later
  if ((pCond->order == TSDB_ORDER_ASC && pCond->twindow.skey > pCond->twindow.ekey) ||
      (pCond->order == TSDB_ORDER_DESC && pCond->twindow.skey < pCond->twindow.ekey)) {
    TSWAP(pCond->twindow.skey, pCond->twindow.ekey);
  }
#endif

  pCond->type = BLOCK_LOAD_OFFSET_SEQ_ORDER;
  //  pCond->type = pTableScanNode->scanFlag;

  int32_t j = 0;
  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pTableScanNode->scan.pScanCols, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
    if (pColNode->colType == COLUMN_TYPE_TAG) {
      continue;
    }

    pCond->colList[j].type = pColNode->node.resType.type;
    pCond->colList[j].bytes = pColNode->node.resType.bytes;
    pCond->colList[j].colId = pColNode->colId;
    j += 1;
  }

  pCond->numOfCols = j;
  return TSDB_CODE_SUCCESS;
}

SColumn extractColumnFromColumnNode(SColumnNode* pColNode) {
  SColumn c = {0};
  c.slotId = pColNode->slotId;
  c.colId = pColNode->colId;
  c.type = pColNode->node.resType.type;
  c.bytes = pColNode->node.resType.bytes;
  c.scale = pColNode->node.resType.scale;
  c.precision = pColNode->node.resType.precision;
  return c;
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

    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SColumn c = extractColumnFromColumnNode(pColNode);
      taosArrayPush(pList, &c);
    } else if (nodeType(pNode->pExpr) == QUERY_NODE_VALUE) {
      SValueNode* pValNode = (SValueNode*)pNode->pExpr;
      SColumn     c = {0};
      c.slotId = pNode->slotId;
      c.colId = pNode->slotId;
      c.type = pValNode->node.type;
      c.bytes = pValNode->node.resType.bytes;
      c.scale = pValNode->node.resType.scale;
      c.precision = pValNode->node.resType.precision;

      taosArrayPush(pList, &c);
    }
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
    c.colId = pColNode->colId;
    c.type = pColNode->node.resType.type;
    c.bytes = pColNode->node.resType.bytes;
    c.precision = pColNode->node.resType.precision;
    c.scale = pColNode->node.resType.scale;

    taosArrayPush(pList, &c);
  }

  return pList;
}

SArray* createSortInfo(SNodeList* pNodeList) {
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
    bi.slotId = pColNode->slotId;
    taosArrayPush(pList, &bi);
  }

  return pList;
}

SArray* extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            SExecTaskInfo* pTaskInfo, int32_t type) {
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
    c.output = true;
    c.colId = pColNode->colId;
    c.srcSlotId = pColNode->slotId;
    c.matchType = type;
    c.targetSlotId = pNode->slotId;
    taosArrayPush(pList, &c);
  }

  *numOfOutputCols = 0;
  int32_t num = LIST_LENGTH(pOutputNodeList->pSlots);
  for (int32_t i = 0; i < num; ++i) {
    SSlotDescNode* pNode = (SSlotDescNode*)nodesListGetNode(pOutputNodeList->pSlots, i);

    // todo: add reserve flag check
    // it is a column reserved for the arithmetic expression calculation
    if (pNode->slotId >= numOfCols) {
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

int32_t getTableList(void* metaHandle, int32_t tableType, uint64_t tableUid,
                           STableListInfo* pListInfo, SNode* pTagCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  pListInfo->pTableList = taosArrayInit(8, sizeof(STableKeyInfo));

  if (tableType == TSDB_SUPER_TABLE) {
    if(pTagCond){
      SArray* res = taosArrayInit(8, sizeof(uint64_t));
      code = doFilterTag(pTagCond, res);
      if (code != TSDB_CODE_SUCCESS) {
        qError("doFilterTag error:%d", code);
        taosArrayDestroy(res);
        terrno = code;
        return code;
      }
      for(int i = 0; i < taosArrayGetSize(res); i++){
        STableKeyInfo info = {.lastKey = TSKEY_INITIAL_VAL, .uid = *(uint64_t*)taosArrayGet(res, i)};
        taosArrayPush(pListInfo->pTableList, &info);
      }
      taosArrayDestroy(res);
    }else{
      code = tsdbGetAllTableList(metaHandle, tableUid, pListInfo->pTableList);
    }
  } else {  // Create one table group.
    STableKeyInfo info = {.lastKey = 0, .uid = tableUid};
    taosArrayPush(pListInfo->pTableList, &info);
  }

  return code;
}

SArray* extractTableIdList(const STableListInfo* pTableGroupInfo) {
  SArray* tableIdList = taosArrayInit(4, sizeof(uint64_t));

  // Transfer the Array of STableKeyInfo into uid list.
  for (int32_t i = 0; i < taosArrayGetSize(pTableGroupInfo->pTableList); ++i) {
    STableKeyInfo* pkeyInfo = taosArrayGet(pTableGroupInfo->pTableList, i);
    taosArrayPush(tableIdList, &pkeyInfo->uid);
  }

  return tableIdList;
}

tsdbReaderT doCreateDataReader(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle,
                               STableListInfo* pTableListInfo, uint64_t queryId, uint64_t taskId, SNode* pTagCond) {
  int32_t  code = getTableList(pHandle->meta, pTableScanNode->scan.tableType, pTableScanNode->scan.uid, pTableListInfo, pTagCond);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (taosArrayGetSize(pTableListInfo->pTableList) == 0) {
    code = 0;
    qDebug("no table qualified for query, TID:0x%" PRIx64 ", QID:0x%" PRIx64, taskId, queryId);
    goto _error;
  }

  SQueryTableDataCond cond = {0};
  code = initQueryTableDataCond(&cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return tsdbQueryTables(pHandle->vnode, &cond, pTableListInfo, queryId, taskId);

_error:
  terrno = code;
  return NULL;
}

int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                               EOPTR_EXEC_MODEL model) {
  uint64_t queryId = pPlan->id.queryId;

  int32_t code = TSDB_CODE_SUCCESS;
  *pTaskInfo = createExecTaskInfo(queryId, taskId, model, pPlan->dbFName);
  if (*pTaskInfo == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _complete;
  }

  (*pTaskInfo)->pRoot =
      createOperatorTree(pPlan->pNode, *pTaskInfo, pHandle, queryId, taskId, &(*pTaskInfo)->tableqinfoList, pPlan->pTagCond);
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

static void doDestroyTableList(STableListInfo* pTableqinfoList) {
  taosArrayDestroy(pTableqinfoList->pTableList);
  taosHashCleanup(pTableqinfoList->map);

  pTableqinfoList->pTableList = NULL;
  pTableqinfoList->map = NULL;
}

void doDestroyTask(SExecTaskInfo* pTaskInfo) {
  qDebug("%s execTask is freed", GET_TASKID(pTaskInfo));

  doDestroyTableList(&pTaskInfo->tableqinfoList);
  destroyOperatorInfo(pTaskInfo->pRoot);
  //  taosArrayDestroy(pTaskInfo->summary.queryProfEvents);
  //  taosHashCleanup(pTaskInfo->summary.operatorProfResults);

  taosMemoryFree(pTaskInfo->schemaVer.dbname);
  taosMemoryFree(pTaskInfo->schemaVer.tablename);
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

int32_t getOperatorExplainExecInfo(SOperatorInfo* operatorInfo, SExplainExecInfo** pRes, int32_t* capacity,
                                   int32_t* resNum) {
  if (*resNum >= *capacity) {
    *capacity += 10;

    *pRes = taosMemoryRealloc(*pRes, (*capacity) * sizeof(SExplainExecInfo));
    if (NULL == *pRes) {
      qError("malloc %d failed", (*capacity) * (int32_t)sizeof(SExplainExecInfo));
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
  }

  SExplainExecInfo* pInfo = &(*pRes)[*resNum];

  pInfo->numOfRows = operatorInfo->resultInfo.totalRows;
  pInfo->startupCost = operatorInfo->cost.openCost;
  pInfo->totalCost = operatorInfo->cost.totalCost;

  if (operatorInfo->fpSet.getExplainFn) {
    int32_t code = operatorInfo->fpSet.getExplainFn(operatorInfo, &pInfo->verboseInfo, &pInfo->verboseLen);
    if (code) {
      qError("%s operator getExplainFn failed, code:%s", GET_TASKID(operatorInfo->pTaskInfo), tstrerror(code));
      return code;
    }
  } else {
    pInfo->verboseLen = 0;
    pInfo->verboseInfo = NULL;
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

int32_t initCacheSupporter(SCatchSupporter* pCatchSup, size_t rowSize, const char* pKey, const char* pDir) {
  pCatchSup->keySize = sizeof(int64_t) + sizeof(int64_t) + sizeof(TSKEY);
  pCatchSup->pKeyBuf = taosMemoryCalloc(1, pCatchSup->keySize);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pCatchSup->pWindowHashTable = taosHashInit(10000, hashFn, true, HASH_NO_LOCK);
  if (pCatchSup->pKeyBuf == NULL || pCatchSup->pWindowHashTable == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t pageSize = rowSize * 32;
  int32_t bufSize = pageSize * 4096;
  return createDiskbasedBuf(&pCatchSup->pDataBuf, pageSize, bufSize, pKey, pDir);
}

int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, const char* pKey) {
  pSup->keySize = sizeof(int64_t) + sizeof(TSKEY);
  pSup->pKeyBuf = taosMemoryCalloc(1, pSup->keySize);
  pSup->pResultRows = taosArrayInit(1024, sizeof(SResultWindowInfo));
  if (pSup->pKeyBuf == NULL || pSup->pResultRows == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t pageSize = 4096;
  while (pageSize < pSup->resultRowSize * 4) {
    pageSize <<= 1u;
  }
  // at least four pages need to be in buffer
  int32_t bufSize = 4096 * 256;
  if (bufSize <= pageSize) {
    bufSize = pageSize * 4;
  }
  return createDiskbasedBuf(&pSup->pResultBuf, pageSize, bufSize, pKey, "/tmp/");
}
