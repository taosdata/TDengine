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
#include "ttime.h"

#include "executorimpl.h"
#include "index.h"
#include "query.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"
#include "vnode.h"

#define IS_MAIN_SCAN(runtime)          ((runtime)->scanFlag == MAIN_SCAN)
#define SET_REVERSE_SCAN_FLAG(runtime) ((runtime)->scanFlag = REVERSE_SCAN)

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

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
#define QUERY_IS_INTERVAL_QUERY(_q) ((_q)->interval.interval > 0)

typedef struct SAggOperatorInfo {
  SOptrBasicInfo   binfo;
  SAggSupporter    aggSup;
  STableQueryInfo* current;
  uint64_t         groupId;
  SGroupResInfo    groupResInfo;
  SExprSupp        scalarExprSup;
  bool             groupKeyOptimized;
} SAggOperatorInfo;

static void setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExpr, SSDataBlock* pBlock);

static void releaseQueryBuf(size_t numOfTables);

static void    destroyAggOperatorInfo(void* param);
static void    initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void    doSetTableGroupOutputBuf(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId);
static void    doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);
static int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                                const char* pKey);
static void    extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, bool keep,
                                                   int32_t status);
static int32_t doSetInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                                   bool createDummyCol);
static int32_t doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                  SGroupResInfo* pGroupResInfo);

void setOperatorCompleted(SOperatorInfo* pOperator) {
  pOperator->status = OP_EXEC_DONE;
  pOperator->cost.totalCost = (taosGetTimestampUs() - pOperator->pTaskInfo->cost.start) / 1000.0;
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
}

void setOperatorInfo(SOperatorInfo* pOperator, const char* name, int32_t type, bool blocking, int32_t status,
                     void* pInfo, SExecTaskInfo* pTaskInfo) {
  pOperator->name = (char*)name;
  pOperator->operatorType = type;
  pOperator->blocking = blocking;
  pOperator->status = status;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;
}

int32_t optrDummyOpenFn(SOperatorInfo* pOperator) {
  OPTR_SET_OPENED(pOperator);
  pOperator->cost.openCost = 0;
  return TSDB_CODE_SUCCESS;
}

SOperatorFpSet createOperatorFpSet(__optr_open_fn_t openFn, __optr_fn_t nextFn, __optr_fn_t cleanup,
                                   __optr_close_fn_t closeFn, __optr_reqBuf_fn_t reqBufFn,
                                   __optr_explain_fn_t explain) {
  SOperatorFpSet fpSet = {
      ._openFn = openFn,
      .getNextFn = nextFn,
      .cleanupFn = cleanup,
      .closeFn = closeFn,
      .reqBufFn = reqBufFn,
      .getExplainFn = explain,
  };

  return fpSet;
}

SResultRow* getNewResultRow(SDiskbasedBuf* pResultBuf, int32_t* currentPageId, int32_t interBufSize) {
  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  if (*currentPageId == -1) {
    pData = getNewBufPage(pResultBuf, &pageId);
    pData->num = sizeof(SFilePage);
  } else {
    pData = getBufPage(pResultBuf, *currentPageId);
    if (pData == NULL) {
      qError("failed to get buffer, code:%s", tstrerror(terrno));
      return NULL;
    }

    pageId = *currentPageId;

    if (pData->num + interBufSize > getBufPageSize(pResultBuf)) {
      // release current page first, and prepare the next one
      releaseBufPage(pResultBuf, pData);

      pData = getNewBufPage(pResultBuf, &pageId);
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
  *currentPageId = pageId;

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
      (SResultRowPosition*)tSimpleHashGet(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));

  SResultRow* pResult = NULL;

  // in case of repeat scan/reverse scan, no new time window added.
  if (isIntervalQuery) {
    if (p1 != NULL) {  // the *p1 may be NULL in case of sliding+offset exists.
      pResult = getResultRowByPos(pResultBuf, p1, true);
      if (NULL == pResult) {
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      ASSERT(pResult->pageId == p1->pageId && pResult->offset == p1->offset);
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existInCurrentResusltRowInfo in the
    // pResultRowInfo object.
    if (p1 != NULL) {
      // todo
      pResult = getResultRowByPos(pResultBuf, p1, true);
      if (NULL == pResult) {
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      ASSERT(pResult->pageId == p1->pageId && pResult->offset == p1->offset);
    }
  }

  // 1. close current opened time window
  if (pResultRowInfo->cur.pageId != -1 && ((pResult == NULL) || (pResult->pageId != pResultRowInfo->cur.pageId))) {
    SResultRowPosition pos = pResultRowInfo->cur;
    SFilePage*         pPage = getBufPage(pResultBuf, pos.pageId);
    if (pPage == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
    releaseBufPage(pResultBuf, pPage);
  }

  // allocate a new buffer page
  if (pResult == NULL) {
    ASSERT(pSup->resultRowSize > 0);
    pResult = getNewResultRow(pResultBuf, &pSup->currentPageId, pSup->resultRowSize);
    if (pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    // add a new result set for a new group
    SResultRowPosition pos = {.pageId = pResult->pageId, .offset = pResult->offset};
    tSimpleHashPut(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pos,
                   sizeof(SResultRowPosition));
  }

  // 2. set the new time window to be the new active time window
  pResultRowInfo->cur = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};

  // too many time window in query
  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_BATCH &&
      tSimpleHashGetSize(pSup->pResultRowHashTable) > MAX_INTERVAL_TIME_WINDOW) {
    T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW);
  }

  return pResult;
}

// a new buffer page for each table. Needs to opt this design
static int32_t addNewWindowResultBuf(SResultRow* pWindowRes, SDiskbasedBuf* pResultBuf, uint32_t size) {
  if (pWindowRes->pageId != -1) {
    return 0;
  }

  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SArray* list = getDataBufPagesIdList(pResultBuf);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewBufPage(pResultBuf, &pageId);
    pData->num = sizeof(SFilePage);
  } else {
    SPageInfo* pi = getLastPageInfo(list);
    pData = getBufPage(pResultBuf, getPageId(pi));
    if (pData == NULL) {
      qError("failed to get buffer, code:%s", tstrerror(terrno));
      return terrno;
    }

    pageId = getPageId(pi);

    if (pData->num + size > getBufPageSize(pResultBuf)) {
      // release current page first, and prepare the next one
      releaseBufPageInfo(pResultBuf, pi);

      pData = getNewBufPage(pResultBuf, &pageId);
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

//  query_range_start, query_range_end, window_duration, window_start, window_end
void initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow) {
  pColData->info.type = TSDB_DATA_TYPE_TIMESTAMP;
  pColData->info.bytes = sizeof(int64_t);

  colInfoDataEnsureCapacity(pColData, 5, false);
  colDataAppendInt64(pColData, 0, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 1, &pQueryWindow->ekey);

  int64_t interval = 0;
  colDataAppendInt64(pColData, 2, &interval);  // this value may be variable in case of 'n' and 'y'.
  colDataAppendInt64(pColData, 3, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 4, &pQueryWindow->ekey);
}

typedef struct {
  bool    hasAgg;
  int32_t numOfRows;
  int32_t startOffset;
} SFunctionCtxStatus;

static void functionCtxSave(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus) {
  pStatus->hasAgg = pCtx->input.colDataSMAIsSet;
  pStatus->numOfRows = pCtx->input.numOfRows;
  pStatus->startOffset = pCtx->input.startRowIndex;
}

static void functionCtxRestore(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus) {
  pCtx->input.colDataSMAIsSet = pStatus->hasAgg;
  pCtx->input.numOfRows = pStatus->numOfRows;
  pCtx->input.startRowIndex = pStatus->startOffset;
}

void applyAggFunctionOnPartialTuples(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, SColumnInfoData* pTimeWindowData,
                                     int32_t offset, int32_t forwardStep, int32_t numOfTotal, int32_t numOfOutput) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    // keep it temporarily
    SFunctionCtxStatus status = {0};
    functionCtxSave(&pCtx[k], &status);

    pCtx[k].input.startRowIndex = offset;
    pCtx[k].input.numOfRows = forwardStep;

    // not a whole block involved in query processing, statistics data can not be used
    // NOTE: the original value of isSet have been changed here
    if (pCtx[k].input.colDataSMAIsSet && forwardStep < numOfTotal) {
      pCtx[k].input.colDataSMAIsSet = false;
    }

    if (fmIsWindowPseudoColumnFunc(pCtx[k].functionId)) {
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[k]);

      char* p = GET_ROWCELL_INTERBUF(pEntryInfo);

      SColumnInfoData idata = {0};
      idata.info.type = TSDB_DATA_TYPE_BIGINT;
      idata.info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      idata.pData = p;

      SScalarParam out = {.columnData = &idata};
      SScalarParam tw = {.numOfRows = 5, .columnData = pTimeWindowData};
      pCtx[k].sfp.process(&tw, 1, &out);
      pEntryInfo->numOfRes = 1;
    } else {
      int32_t code = TSDB_CODE_SUCCESS;
      if (functionNeedToExecute(&pCtx[k]) && pCtx[k].fpSet.process != NULL) {
        code = pCtx[k].fpSet.process(&pCtx[k]);

        if (code != TSDB_CODE_SUCCESS) {
          qError("%s apply functions error, code: %s", GET_TASKID(taskInfo), tstrerror(code));
          taskInfo->code = code;
          T_LONG_JMP(taskInfo->env, code);
        }
      }

      // restore it
      functionCtxRestore(&pCtx[k], &status);
    }
  }
}

static void doSetInputDataBlockInfo(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order) {
  SqlFunctionCtx* pCtx = pExprSup->pCtx;
  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;
    setBlockSMAInfo(&pCtx[i], &pExprSup->pExprInfo[i], pBlock);
    pCtx[i].pSrcBlock = pBlock;
  }
}

void setInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag, bool createDummyCol) {
  if (pBlock->pBlockAgg != NULL) {
    doSetInputDataBlockInfo(pExprSup, pBlock, order);
  } else {
    doSetInputDataBlock(pExprSup, pBlock, order, scanFlag, createDummyCol);
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

  colInfoDataEnsureCapacity(pColInfo, numOfRows, false);

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
    taosMemoryFree(tmp);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doSetInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                                   bool createDummyCol) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SqlFunctionCtx* pCtx = pExprSup->pCtx;

  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;

    pCtx[i].pSrcBlock = pBlock;
    pCtx[i].scanFlag = scanFlag;

    SInputColumnInfoData* pInput = &pCtx[i].input;
    pInput->uid = pBlock->info.id.uid;
    pInput->colDataSMAIsSet = false;

    SExprInfo* pOneExpr = &pExprSup->pExprInfo[i];
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        pInput->pData[j] = taosArrayGet(pBlock->pDataBlock, slotId);
        pInput->totalRows = pBlock->info.rows;
        pInput->numOfRows = pBlock->info.rows;
        pInput->startRowIndex = 0;

        // NOTE: the last parameter is the primary timestamp column
        // todo: refactor this
        if (fmIsImplicitTsFunc(pCtx[i].functionId) && (j == pOneExpr->base.numOfParams - 1)) {
          pInput->pPTS = pInput->pData[j];  // in case of merge function, this is not always the ts column data.
          //          ASSERT(pInput->pPTS->info.type == TSDB_DATA_TYPE_TIMESTAMP);
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

static int32_t doAggregateImpl(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx) {
  for (int32_t k = 0; k < pOperator->exprSupp.numOfExprs; ++k) {
    if (functionNeedToExecute(&pCtx[k])) {
      // todo add a dummy funtion to avoid process check
      if (pCtx[k].fpSet.process == NULL) {
        continue;
      }

      int32_t code = pCtx[k].fpSet.process(&pCtx[k]);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s aggregate function error happens, code: %s", GET_TASKID(pOperator->pTaskInfo), tstrerror(code));
        return code;
      }
    }
  }

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

  return true;
}

static int32_t doCreateConstantValColumnSMAInfo(SInputColumnInfoData* pInput, SFunctParam* pFuncParam, int32_t type,
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

  if (type == TSDB_DATA_TYPE_BIGINT) {
    int64_t v = pFuncParam->param.i;
    *da = (SColumnDataAgg){.numOfNull = 0, .min = v, .max = v, .sum = v * numOfRows};
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    double v = pFuncParam->param.d;
    *da = (SColumnDataAgg){.numOfNull = 0};

    *(double*)&da->min = v;
    *(double*)&da->max = v;
    *(double*)&da->sum = v * numOfRows;
  } else if (type == TSDB_DATA_TYPE_BOOL) {  // todo validate this data type
    bool v = pFuncParam->param.i;

    *da = (SColumnDataAgg){.numOfNull = 0};
    *(bool*)&da->min = 0;
    *(bool*)&da->max = v;
    *(bool*)&da->sum = v * numOfRows;
  } else if (type == TSDB_DATA_TYPE_TIMESTAMP) {
    // do nothing
  } else {
    qError("invalid constant type for sma info");
  }

  return TSDB_CODE_SUCCESS;
}

void setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, SSDataBlock* pBlock) {
  int32_t numOfRows = pBlock->info.rows;

  SInputColumnInfoData* pInput = &pCtx->input;
  pInput->numOfRows = numOfRows;
  pInput->totalRows = numOfRows;

  if (pBlock->pBlockAgg != NULL) {
    pInput->colDataSMAIsSet = true;

    for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pExprInfo->base.pParam[j];

      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        pInput->pColumnDataAgg[j] = pBlock->pBlockAgg[slotId];
        if (pInput->pColumnDataAgg[j] == NULL) {
          pInput->colDataSMAIsSet = false;
        }

        // Here we set the column info data since the data type for each column data is required, but
        // the data in the corresponding SColumnInfoData will not be used.
        pInput->pData[j] = taosArrayGet(pBlock->pDataBlock, slotId);
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        doCreateConstantValColumnSMAInfo(pInput, pFuncParam, pFuncParam->param.nType, j, pBlock->info.rows);
      }
    }
  } else {
    pInput->colDataSMAIsSet = false;
  }
}

bool isTaskKilled(SExecTaskInfo* pTaskInfo) { return (0 != pTaskInfo->code) ? true : false; }

void setTaskKilled(SExecTaskInfo* pTaskInfo, int32_t rspCode) { pTaskInfo->code = rspCode; }

/////////////////////////////////////////////////////////////////////////////////////////////
STimeWindow getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key) {
  STimeWindow win = {0};
  win.skey = taosTimeTruncate(key, pInterval, precision);

  /*
   * if the realSkey > INT64_MAX - pInterval->interval, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  win.ekey = taosTimeAdd(win.skey, pInterval->interval, pInterval->intervalUnit, precision) - 1;
  if (win.ekey < win.skey) {
    win.ekey = INT64_MAX;
  }

  return win;
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
                                    pTableScanInfo->rowEntryInfoOffset);
      } else {
        if (setResultOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pBlock->info.id.uid, &win, masterScan, &pResult, groupId,
                                    pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                    pTableScanInfo->rowEntryInfoOffset) != TSDB_CODE_SUCCESS) {
          T_LONG_JMP(pRuntimeEnv->env, TSDB_CODE_OUT_OF_MEMORY);
        }
      }
    } else if (pQueryAttr->stableQuery && (!pQueryAttr->tsCompQuery) && (!pQueryAttr->diffQuery)) { // stable aggregate, not interval aggregate or normal column aggregate
      doSetTableGroupOutputBuf(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pTableScanInfo->pCtx,
                               pTableScanInfo->rowEntryInfoOffset, pTableScanInfo->numOfOutput,
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
//    tsdbRetrieveDatablockSMA(pTableScanInfo->pTsdbReadHandle, &pBlock->pBlockAgg);

    if (pBlock->pBlockAgg == NULL) {  // data block statistics does not exist, load data block
//      pBlock->pDataBlock = tsdbRetrieveDataBlock(pTableScanInfo->pTsdbReadHandle, NULL);
      pCost->totalCheckedRows += pBlock->info.rows;
    }
  } else {
    assert((*status) == BLK_DATA_DATA_LOAD);

    // load the data block statistics to perform further filter
    pCost->loadBlockStatis += 1;
//    tsdbRetrieveDatablockSMA(pTableScanInfo->pTsdbReadHandle, &pBlock->pBlockAgg);

    if (pQueryAttr->topBotQuery && pBlock->pBlockAgg != NULL) {
      { // set previous window
        if (QUERY_IS_INTERVAL_QUERY(pQueryAttr)) {
          SResultRow* pResult = NULL;

          bool  masterScan = IS_MAIN_SCAN(pRuntimeEnv);
          TSKEY k = ascQuery? pBlock->info.window.skey : pBlock->info.window.ekey;

          STimeWindow win = getActiveTimeWindow(pTableScanInfo->pResultRowInfo, k, pQueryAttr);
          if (setResultOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pBlock->info.id.uid, &win, masterScan, &pResult, groupId,
                                      pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                      pTableScanInfo->rowEntryInfoOffset) != TSDB_CODE_SUCCESS) {
            T_LONG_JMP(pRuntimeEnv->env, TSDB_CODE_OUT_OF_MEMORY);
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
//    if (!doFilterByBlockSMA(pRuntimeEnv, pBlock->pBlockAgg, pTableScanInfo->pCtx, pBlockInfo->rows)) {
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
//      filterSetColFieldData(pQueryAttr->pFilters, taosArrayGetSize(pBlock->pDataBlock), pBlock->pDataBlock);
//    }

//    if (pQueryAttr->pFilters != NULL || pRuntimeEnv->pTsBuf != NULL) {
//      filterColRowsInDataBlock(pRuntimeEnv, pBlock, ascQuery);
//    }
  }
#endif
  return TSDB_CODE_SUCCESS;
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

void setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset) {
  bool init = false;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].resultInfo = getResultEntryInfo(pResult, i, rowEntryInfoOffset);
    if (init) {
      continue;
    }

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
    } else {
      init = true;
    }
  }
}

void doFilter(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SColMatchInfo* pColMatchInfo) {
  if (pFilterInfo == NULL || pBlock->info.rows == 0) {
    return;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  int32_t            code = filterSetDataFromSlotId(pFilterInfo, &param1);

  SColumnInfoData* p = NULL;
  int32_t          status = 0;

  // todo the keep seems never to be True??
  bool keep = filterExecute(pFilterInfo, pBlock, &p, NULL, param1.numOfCols, &status);
  extractQualifiedTupleByFilterResult(pBlock, p, keep, status);

  if (pColMatchInfo != NULL) {
    size_t size = taosArrayGetSize(pColMatchInfo->pList);
    for (int32_t i = 0; i < size; ++i) {
      SColMatchItem* pInfo = taosArrayGet(pColMatchInfo->pList, i);
      if (pInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pInfo->dstSlotId);
        if (pColData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
          blockDataUpdateTsWindow(pBlock, pInfo->dstSlotId);
          break;
        }
      }
    }
  }

  colDataDestroy(p);
  taosMemoryFree(p);
}

void extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, bool keep, int32_t status) {
  if (keep) {
    return;
  }

  int8_t* pIndicator = (int8_t*)p->pData;
  int32_t totalRows = pBlock->info.rows;

  if (status == FILTER_RESULT_ALL_QUALIFIED) {
    // here nothing needs to be done
  } else if (status == FILTER_RESULT_NONE_QUALIFIED) {
    pBlock->info.rows = 0;
  } else {
    int32_t bmLen = BitmapLen(totalRows);
    char*   pBitmap = NULL;
    int32_t maxRows = 0;

    size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      // it is a reserved column for scalar function, and no data in this column yet.
      if (pDst->pData == NULL) {
        continue;
      }

      int32_t numOfRows = 0;

      if (IS_VAR_DATA_TYPE(pDst->info.type)) {
        int32_t j = 0;
        pDst->varmeta.length = 0;

        while(j < totalRows) {
          if (pIndicator[j] == 0) {
            j += 1;
            continue;
          }

          if (colDataIsNull_var(pDst, j)) {
            colDataSetNull_var(pDst, numOfRows);
          } else {
            char* p1 = colDataGetVarData(pDst, j);
            colDataAppend(pDst, numOfRows, p1, false);
          }
          numOfRows += 1;
          j += 1;
        }

        if (maxRows < numOfRows) {
          maxRows = numOfRows;
        }
      } else {
        if (pBitmap == NULL) {
          pBitmap = taosMemoryCalloc(1, bmLen);
        }

        memcpy(pBitmap, pDst->nullbitmap, bmLen);
        memset(pDst->nullbitmap, 0, bmLen);

        int32_t j = 0;

        switch (pDst->info.type) {
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_UBIGINT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
            while (j < totalRows) {
              if (pIndicator[j] == 0) {
                j += 1;
                continue;
              }

              if (colDataIsNull_f(pBitmap, j)) {
                colDataSetNull_f(pDst->nullbitmap, numOfRows);
              } else {
                ((int64_t*)pDst->pData)[numOfRows] = ((int64_t*)pDst->pData)[j];
              }
              numOfRows += 1;
              j += 1;
            }
            break;
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_UINT:
            while (j < totalRows) {
              if (pIndicator[j] == 0) {
                j += 1;
                continue;
              }
              if (colDataIsNull_f(pBitmap, j)) {
                colDataSetNull_f(pDst->nullbitmap, numOfRows);
              } else {
                ((int32_t*)pDst->pData)[numOfRows] = ((int32_t*)pDst->pData)[j];
              }
              numOfRows += 1;
              j += 1;
            }
            break;
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_USMALLINT:
            while (j < totalRows) {
              if (pIndicator[j] == 0) {
                j += 1;
                continue;
              }
              if (colDataIsNull_f(pBitmap, j)) {
                colDataSetNull_f(pDst->nullbitmap, numOfRows);
              } else {
                ((int16_t*)pDst->pData)[numOfRows] = ((int16_t*)pDst->pData)[j];
              }
              numOfRows += 1;
              j += 1;
            }
            break;
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_UTINYINT:
            while (j < totalRows) {
              if (pIndicator[j] == 0) {
                j += 1;
                continue;
              }
              if (colDataIsNull_f(pBitmap, j)) {
                colDataSetNull_f(pDst->nullbitmap, numOfRows);
              } else {
                ((int8_t*)pDst->pData)[numOfRows] = ((int8_t*)pDst->pData)[j];
              }
              numOfRows += 1;
              j += 1;
            }
            break;
        }
      }

      if (maxRows < numOfRows) {
        maxRows = numOfRows;
      }
    }

    pBlock->info.rows = maxRows;
    if (pBitmap != NULL) {
      taosMemoryFree(pBitmap);
    }
  }
}

void doSetTableGroupOutputBuf(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId) {
  // for simple group by query without interval, all the tables belong to one group result.
  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  SAggOperatorInfo* pAggInfo = pOperator->info;

  SResultRowInfo* pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t*        rowEntryInfoOffset = pOperator->exprSupp.rowEntryInfoOffset;

  SResultRow* pResultRow = doSetResultOutBufByKey(pAggInfo->aggSup.pResultBuf, pResultRowInfo, (char*)&groupId,
                                                  sizeof(groupId), true, groupId, pTaskInfo, false, &pAggInfo->aggSup);
  assert(pResultRow != NULL);

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pResultRow->pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pResultRow, pAggInfo->aggSup.pResultBuf, pAggInfo->binfo.pRes->info.rowSize);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
  }

  setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);
}

static void setExecutionContext(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId) {
  SAggOperatorInfo* pAggInfo = pOperator->info;
  if (pAggInfo->groupId != UINT64_MAX && pAggInfo->groupId == groupId) {
    return;
  }

  doSetTableGroupOutputBuf(pOperator, numOfOutput, groupId);

  // record the current active group id
  pAggInfo->groupId = groupId;
}

static void doUpdateNumOfRows(SqlFunctionCtx* pCtx, SResultRow* pRow, int32_t numOfExprs,
                              const int32_t* rowEntryOffset) {
  bool returnNotNull = false;
  for (int32_t j = 0; j < numOfExprs; ++j) {
    SResultRowEntryInfo* pResInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
    if (!isRowEntryInitialized(pResInfo)) {
      continue;
    }

    if (pRow->numOfRows < pResInfo->numOfRes) {
      pRow->numOfRows = pResInfo->numOfRes;
    }

    if (fmIsNotNullOutputFunc(pCtx[j].functionId)) {
      returnNotNull = true;
    }
  }
  // if all expr skips all blocks, e.g. all null inputs for max function, output one row in final result.
  //  except for first/last, which require not null output, output no rows
  if (pRow->numOfRows == 0 && !returnNotNull) {
    pRow->numOfRows = 1;
  }
}

static void doCopyResultToDataBlock(SExprInfo* pExprInfo, int32_t numOfExprs, SResultRow* pRow, SqlFunctionCtx* pCtx,
                                    SSDataBlock* pBlock, const int32_t* rowEntryOffset, SExecTaskInfo* pTaskInfo) {
  for (int32_t j = 0; j < numOfExprs; ++j) {
    int32_t slotId = pExprInfo[j].base.resSchema.slotId;

    pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
    if (pCtx[j].fpSet.finalize) {
      if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_group_key") == 0) {
        // for groupkey along with functions that output multiple lines(e.g. Histogram)
        // need to match groupkey result for each output row of that function.
        if (pCtx[j].resultInfo->numOfRes != 0) {
          pCtx[j].resultInfo->numOfRes = pRow->numOfRows;
        }
      }

      int32_t code = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
      if (TAOS_FAILED(code)) {
        qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
        T_LONG_JMP(pTaskInfo->env, code);
      }
    } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
      // do nothing
    } else {
      // expand the result into multiple rows. E.g., _wstart, top(k, 20)
      // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
      char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
      for (int32_t k = 0; k < pRow->numOfRows; ++k) {
        colDataAppend(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
      }
    }
  }
}

// todo refactor. SResultRow has direct pointer in miainfo
int32_t finalizeResultRows(SDiskbasedBuf* pBuf, SResultRowPosition* resultRowPosition, SExprSupp* pSup,
                           SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  SFilePage*  page = getBufPage(pBuf, resultRowPosition->pageId);
  if (page == NULL) {
    qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  SResultRow* pRow = (SResultRow*)((char*)page + resultRowPosition->offset);

  SqlFunctionCtx* pCtx = pSup->pCtx;
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  const int32_t*  rowEntryOffset = pSup->rowEntryInfoOffset;

  doUpdateNumOfRows(pCtx, pRow, pSup->numOfExprs, rowEntryOffset);
  if (pRow->numOfRows == 0) {
    releaseBufPage(pBuf, page);
    return 0;
  }

  int32_t size = pBlock->info.capacity;
  while (pBlock->info.rows + pRow->numOfRows > size) {
    size = size * 1.25;
  }

  int32_t code = blockDataEnsureCapacity(pBlock, size);
  if (TAOS_FAILED(code)) {
    releaseBufPage(pBuf, page);
    qError("%s ensure result data capacity failed, code %s", GET_TASKID(pTaskInfo), tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  doCopyResultToDataBlock(pExprInfo, pSup->numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo);

  releaseBufPage(pBuf, page);
  pBlock->info.rows += pRow->numOfRows;
  return 0;
}

int32_t doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                           SGroupResInfo* pGroupResInfo) {
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SResKeyPos* pPos = taosArrayGetP(pGroupResInfo->pRows, i);
    SFilePage*  page = getBufPage(pBuf, pPos->pos.pageId);
    if (page == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    SResultRow* pRow = (SResultRow*)((char*)page + pPos->pos.offset);

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);

    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      releaseBufPage(pBuf, page);
      continue;
    }

    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = pPos->groupId;
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.id.groupId != pPos->groupId) {
        releaseBufPage(pBuf, page);
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      releaseBufPage(pBuf, page);
      break;
    }

    pGroupResInfo->index += 1;
    doCopyResultToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo);

    releaseBufPage(pBuf, page);
    pBlock->info.rows += pRow->numOfRows;
  }

  qDebug("%s result generated, rows:%d, groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);
  pBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pBlock, 0);
  return 0;
}

void doBuildStreamResBlock(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                           SDiskbasedBuf* pBuf) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*   pBlock = pbInfo->pRes;

  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  ASSERT(!pbInfo->mergeResultBlock);
  doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo);

  void* tbname = NULL;
  if (streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
    pBlock->info.parTbName[0] = 0;
  } else {
    memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
  }
  tdbFree(tbname);
}

void doBuildResultDatablock(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                            SDiskbasedBuf* pBuf) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*   pBlock = pbInfo->pRes;

  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  // clear the existed group id
  pBlock->info.id.groupId = 0;
  if (!pbInfo->mergeResultBlock) {
    doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo);
  } else {
    while (hasRemainResults(pGroupResInfo)) {
      doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo);
      if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }

      // clearing group id to continue to merge data that belong to different groups
      pBlock->info.id.groupId = 0;
    }

    // clear the group id info in SSDataBlock, since the client does not need it
    pBlock->info.id.groupId = 0;
  }
}

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
//     T_LONG_JMP(pRuntimeEnv->env, terrno);
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

int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag) {
  // todo add more information about exchange operation
  int32_t type = pOperator->operatorType;
  if (type == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE || type == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN) {
    *order = TSDB_ORDER_ASC;
    *scanFlag = MAIN_SCAN;
    return TSDB_CODE_SUCCESS;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = pOperator->info;
    *order = pTableScanInfo->base.cond.order;
    *scanFlag = pTableScanInfo->base.scanFlag;
    return TSDB_CODE_SUCCESS;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    STableMergeScanInfo* pTableScanInfo = pOperator->info;
    *order = pTableScanInfo->base.cond.order;
    *scanFlag = pTableScanInfo->base.scanFlag;
    return TSDB_CODE_SUCCESS;
  } else {
    if (pOperator->pDownstream == NULL || pOperator->pDownstream[0] == NULL) {
      return TSDB_CODE_INVALID_PARA;
    } else {
      return getTableScanInfo(pOperator->pDownstream[0], order, scanFlag);
    }
  }
}

static int32_t createDataBlockForEmptyInput(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  if (!tsCountAlwaysReturnValue) {
    return TSDB_CODE_SUCCESS;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  if (pAggInfo->groupKeyOptimized) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_PARTITION ||
      (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN &&
       ((STableScanInfo*)downstream->info)->hasGroupByTag == true)) {
    return TSDB_CODE_SUCCESS;
  }

  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  bool            hasCountFunc = false;

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    const char* pName = pCtx[i].pExpr->pExpr->_function.functionName;
    if ((strcmp(pName, "count") == 0) || (strcmp(pName, "hyperloglog") == 0) ||
        (strcmp(pName, "_hyperloglog_partial") == 0) || (strcmp(pName, "_hyperloglog_merge") == 0)) {
      hasCountFunc = true;
      break;
    }
  }

  if (!hasCountFunc) {
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pBlock = createDataBlock();
  pBlock->info.rows = 1;
  pBlock->info.capacity = 0;

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.hasNull = true;
    colInfo.info.type = TSDB_DATA_TYPE_NULL;
    colInfo.info.bytes = 1;

    SExprInfo* pOneExpr = &pOperator->exprSupp.pExprInfo[i];
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
        if (slotId >= numOfCols) {
          taosArrayEnsureCap(pBlock->pDataBlock, slotId + 1);
          for (int32_t k = numOfCols; k < slotId + 1; ++k) {
            taosArrayPush(pBlock->pDataBlock, &colInfo);
          }
        }
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        // do nothing
      }
    }
  }

  blockDataEnsureCapacity(pBlock, pBlock->info.rows);
  *ppBlock = pBlock;

  return TSDB_CODE_SUCCESS;
}

static void destroyDataBlockForEmptyInput(bool blockAllocated, SSDataBlock** ppBlock) {
  if (!blockAllocated) {
    return;
  }

  blockDataDestroy(*ppBlock);
  *ppBlock = NULL;
}

// this is a blocking operator
static int32_t doOpenAggregateOptr(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  SAggOperatorInfo* pAggInfo = pOperator->info;

  SExprSupp*     pSup = &pOperator->exprSupp;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  int64_t st = taosGetTimestampUs();

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;

  bool hasValidBlock = false;
  bool blockAllocated = false;

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      if (!hasValidBlock) {
        createDataBlockForEmptyInput(pOperator, &pBlock);
        if (pBlock == NULL) {
          break;
        }
        blockAllocated = true;
      } else {
        break;
      }
    }
    hasValidBlock = true;

    int32_t code = getTableScanInfo(pOperator, &order, &scanFlag);
    if (code != TSDB_CODE_SUCCESS) {
      destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // there is an scalar expression that needs to be calculated before apply the group aggregation.
    if (pAggInfo->scalarExprSup.pExprInfo != NULL && !blockAllocated) {
      SExprSupp* pSup1 = &pAggInfo->scalarExprSup;
      code = projectApplyFunctions(pSup1->pExprInfo, pBlock, pBlock, pSup1->pCtx, pSup1->numOfExprs, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    // the pDataBlock are always the same one, no need to call this again
    setExecutionContext(pOperator, pOperator->exprSupp.numOfExprs, pBlock->info.id.groupId);
    setInputDataBlock(pSup, pBlock, order, scanFlag, true);
    code = doAggregateImpl(pOperator, pSup->pCtx);
    if (code != 0) {
      destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }

    destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
  }

  // the downstream operator may return with error code, so let's check the code before generating results.
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }

  initGroupedResultInfo(&pAggInfo->groupResInfo, pAggInfo->aggSup.pResultRowHashTable, 0);
  OPTR_SET_OPENED(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return pTaskInfo->code;
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
    setOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  while (1) {
    doBuildResultDatablock(pOperator, pInfo, &pAggInfo->groupResInfo, pAggInfo->aggSup.pResultBuf);
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

    if (!hasRemainResults(&pAggInfo->groupResInfo)) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pInfo->pRes->info.rows > 0) {
      break;
    }
  }

  size_t rows = blockDataGetNumOfRows(pInfo->pRes);
  pOperator->resultInfo.totalRows += rows;

  return (rows == 0) ? NULL : pInfo->pRes;
}

void destroyExprInfo(SExprInfo* pExpr, int32_t numOfExprs) {
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExpr[i];
    for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
      if (pExprInfo->base.pParam[j].type == FUNC_PARAM_TYPE_COLUMN) {
        taosMemoryFreeClear(pExprInfo->base.pParam[j].pCol);
      } else if (pExprInfo->base.pParam[j].type == FUNC_PARAM_TYPE_VALUE) {
        taosVariantDestroy(&pExprInfo->base.pParam[j].param);
      }
    }

    taosMemoryFree(pExprInfo->base.pParam);
    taosMemoryFree(pExprInfo->pExpr);
  }
}

void destroyOperatorInfo(SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return;
  }

  if (pOperator->fpSet.closeFn != NULL) {
    pOperator->fpSet.closeFn(pOperator->info);
  }

  if (pOperator->pDownstream != NULL) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      destroyOperatorInfo(pOperator->pDownstream[i]);
    }

    taosMemoryFreeClear(pOperator->pDownstream);
    pOperator->numOfDownstream = 0;
  }

  cleanupExprSupp(&pOperator->exprSupp);
  taosMemoryFreeClear(pOperator);
}

// each operator should be set their own function to return total cost buffer
int32_t optrDefaultBufFn(SOperatorInfo* pOperator) {
  if (pOperator->blocking) {
    return -1;
  } else {
    return 0;
  }
}

int32_t getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, uint32_t* defaultBufsz) {
  *defaultPgsz = 4096;
  while (*defaultPgsz < rowSize * 4) {
    *defaultPgsz <<= 1u;
  }

  // The default buffer for each operator in query is 10MB.
  // at least four pages need to be in buffer
  // TODO: make this variable to be configurable.
  *defaultBufsz = 4096 * 2560;
  if ((*defaultBufsz) <= (*defaultPgsz)) {
    (*defaultBufsz) = (*defaultPgsz) * 4;
  }

  return 0;
}

int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                         const char* pKey) {
  int32_t    code = 0;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  pAggSup->currentPageId = -1;
  pAggSup->resultRowSize = getResultRowSize(pCtx, numOfOutput);
  pAggSup->keyBuf = taosMemoryCalloc(1, keyBufSize + POINTER_BYTES + sizeof(int64_t));
  pAggSup->pResultRowHashTable = tSimpleHashInit(100, hashFn);

  if (pAggSup->keyBuf == NULL || pAggSup->pResultRowHashTable == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  uint32_t defaultPgsz = 0;
  uint32_t defaultBufsz = 0;
  getBufferPgSize(pAggSup->resultRowSize, &defaultPgsz, &defaultBufsz);

  if (!osTempSpaceAvailable()) {
    code = TSDB_CODE_NO_AVAIL_DISK;
    qError("Init stream agg supporter failed since %s, %s", terrstr(code), pKey);
    return code;
  }

  code = createDiskbasedBuf(&pAggSup->pResultBuf, defaultPgsz, defaultBufsz, pKey, tsTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    qError("Create agg result buf failed since %s, %s", tstrerror(code), pKey);
    return code;
  }

  return code;
}

void cleanupAggSup(SAggSupporter* pAggSup) {
  taosMemoryFreeClear(pAggSup->keyBuf);
  tSimpleHashCleanup(pAggSup->pResultRowHashTable);
  destroyDiskbasedBuf(pAggSup->pResultBuf);
}

int32_t initAggSup(SExprSupp* pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols, size_t keyBufSize,
                   const char* pkey, void* pState) {
  int32_t code = initExprSupp(pSup, pExprInfo, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = doInitAggInfoSup(pAggSup, pSup->pCtx, numOfCols, keyBufSize, pkey);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (pState) {
      pSup->pCtx[i].saveHandle.pBuf = NULL;
      pSup->pCtx[i].saveHandle.pState = pState;
      pSup->pCtx[i].exprIdx = i;
    } else {
      pSup->pCtx[i].saveHandle.pBuf = pAggSup->pResultBuf;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void initResultSizeInfo(SResultInfo* pResultInfo, int32_t numOfRows) {
  ASSERT(numOfRows != 0);
  pResultInfo->capacity = numOfRows;
  pResultInfo->threshold = numOfRows * 0.75;

  if (pResultInfo->threshold == 0) {
    pResultInfo->threshold = numOfRows;
  }
}

void initBasicInfo(SOptrBasicInfo* pInfo, SSDataBlock* pBlock) {
  pInfo->pRes = pBlock;
  initResultRowInfo(&pInfo->resultRowInfo);
}

void* destroySqlFunctionCtx(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  if (pCtx == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    for (int32_t j = 0; j < pCtx[i].numOfParams; ++j) {
      taosVariantDestroy(&pCtx[i].param[j].param);
    }

    taosMemoryFreeClear(pCtx[i].subsidiaries.pCtx);
    taosMemoryFreeClear(pCtx[i].subsidiaries.buf);
    taosMemoryFree(pCtx[i].input.pData);
    taosMemoryFree(pCtx[i].input.pColumnDataAgg);
  }

  taosMemoryFreeClear(pCtx);
  return NULL;
}

int32_t initExprSupp(SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfExpr) {
  pSup->pExprInfo = pExprInfo;
  pSup->numOfExprs = numOfExpr;
  if (pSup->pExprInfo != NULL) {
    pSup->pCtx = createSqlFunctionCtx(pExprInfo, numOfExpr, &pSup->rowEntryInfoOffset);
    if (pSup->pCtx == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void cleanupExprSupp(SExprSupp* pSupp) {
  destroySqlFunctionCtx(pSupp->pCtx, pSupp->numOfExprs);
  if (pSupp->pExprInfo != NULL) {
    destroyExprInfo(pSupp->pExprInfo, pSupp->numOfExprs);
    taosMemoryFreeClear(pSupp->pExprInfo);
  }

  if (pSupp->pFilterInfo != NULL) {
    filterFreeInfo(pSupp->pFilterInfo);
    pSupp->pFilterInfo = NULL;
  }

  taosMemoryFree(pSupp->rowEntryInfoOffset);
}

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode,
                                           SExecTaskInfo* pTaskInfo) {
  SAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAggOperatorInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAggNode->node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &num);
  int32_t    code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                               pTaskInfo->streamInfo.pState);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t    numOfScalarExpr = 0;
  SExprInfo* pScalarExprInfo = NULL;
  if (pAggNode->pExprs != NULL) {
    pScalarExprInfo = createExprInfo(pAggNode->pExprs, NULL, &numOfScalarExpr);
  }

  code = initExprSupp(&pInfo->scalarExprSup, pScalarExprInfo, numOfScalarExpr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = filterInitFromNode((SNode*)pAggNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.mergeResultBlock = pAggNode->mergeDataBlock;
  pInfo->groupKeyOptimized = pAggNode->groupKeyOptimized;
  pInfo->groupId = UINT64_MAX;

  setOperatorInfo(pOperator, "TableAggregate", QUERY_NODE_PHYSICAL_PLAN_HASH_AGG, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(doOpenAggregateOptr, getAggregateResult, NULL, destroyAggOperatorInfo,
                                         optrDefaultBufFn, NULL);

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = downstream->info;
    pTableScanInfo->base.pdInfo.pExprSup = &pOperator->exprSupp;
    pTableScanInfo->base.pdInfo.pAggSup = &pInfo->aggSup;
  }

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyAggOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    cleanupExprSupp(&pOperator->exprSupp);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void cleanupBasicInfo(SOptrBasicInfo* pInfo) {
  assert(pInfo != NULL);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

static void freeItem(void* pItem) {
  void** p = pItem;
  if (*p != NULL) {
    taosMemoryFreeClear(*p);
  }
}

void destroyAggOperatorInfo(void* param) {
  SAggOperatorInfo* pInfo = (SAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarExprSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  taosMemoryFreeClear(param);
}

static SExecTaskInfo* createExecTaskInfo(uint64_t queryId, uint64_t taskId, EOPTR_EXEC_MODEL model, char* dbFName) {
  SExecTaskInfo* pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  if (pTaskInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);

  pTaskInfo->schemaInfo.dbname = strdup(dbFName);
  pTaskInfo->id.queryId = queryId;
  pTaskInfo->execModel = model;
  pTaskInfo->pTableInfoList = tableListCreate();
  pTaskInfo->stopInfo.pStopInfo = taosArrayInit(4, sizeof(SExchangeOpStopInfo));
  pTaskInfo->pResultBlockList = taosArrayInit(128, POINTER_BYTES);

  char* p = taosMemoryCalloc(1, 128);
  snprintf(p, 128, "TID:0x%" PRIx64 " QID:0x%" PRIx64, taskId, queryId);
  pTaskInfo->id.str = p;

  return pTaskInfo;
}

SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode);

int32_t extractTableSchemaInfo(SReadHandle* pHandle, SScanPhysiNode* pScanNode, SExecTaskInfo* pTaskInfo) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, pHandle->meta, 0);
  int32_t code = metaGetTableEntryByUidCache(&mr, pScanNode->uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get the table meta, uid:0x%" PRIx64 ", suid:0x%" PRIx64 ", %s", pScanNode->uid, pScanNode->suid,
           GET_TASKID(pTaskInfo));

    metaReaderClear(&mr);
    return terrno;
  }

  SSchemaInfo* pSchemaInfo = &pTaskInfo->schemaInfo;
  pSchemaInfo->tablename = strdup(mr.me.name);

  if (mr.me.type == TSDB_SUPER_TABLE) {
    pSchemaInfo->sw = tCloneSSchemaWrapper(&mr.me.stbEntry.schemaRow);
    pSchemaInfo->tversion = mr.me.stbEntry.schemaTag.version;
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);

    tb_uid_t suid = mr.me.ctbEntry.suid;
    metaGetTableEntryByUidCache(&mr, suid);
    pSchemaInfo->sw = tCloneSSchemaWrapper(&mr.me.stbEntry.schemaRow);
    pSchemaInfo->tversion = mr.me.stbEntry.schemaTag.version;
  } else {
    pSchemaInfo->sw = tCloneSSchemaWrapper(&mr.me.ntbEntry.schemaRow);
  }

  metaReaderClear(&mr);

  pSchemaInfo->qsw = extractQueriedColumnSchema(pScanNode);
  return TSDB_CODE_SUCCESS;
}

SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode) {
  int32_t numOfCols = LIST_LENGTH(pScanNode->pScanCols);
  int32_t numOfTags = LIST_LENGTH(pScanNode->pScanPseudoCols);

  SSchemaWrapper* pqSw = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
  pqSw->pSchema = taosMemoryCalloc(numOfCols + numOfTags, sizeof(SSchema));

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pScanNode->pScanCols, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

    SSchema* pSchema = &pqSw->pSchema[pqSw->nCols++];
    pSchema->colId = pColNode->colId;
    pSchema->type = pColNode->node.resType.type;
    pSchema->bytes = pColNode->node.resType.bytes;
    tstrncpy(pSchema->name, pColNode->colName, tListLen(pSchema->name));
  }

  // this the tags and pseudo function columns, we only keep the tag columns
  for (int32_t i = 0; i < numOfTags; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pScanNode->pScanPseudoCols, i);

    int32_t type = nodeType(pNode->pExpr);
    if (type == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SSchema* pSchema = &pqSw->pSchema[pqSw->nCols++];
      pSchema->colId = pColNode->colId;
      pSchema->type = pColNode->node.resType.type;
      pSchema->bytes = pColNode->node.resType.bytes;
      tstrncpy(pSchema->name, pColNode->colName, tListLen(pSchema->name));
    }
  }

  return pqSw;
}

static void cleanupTableSchemaInfo(SSchemaInfo* pSchemaInfo) {
  taosMemoryFreeClear(pSchemaInfo->dbname);
  taosMemoryFreeClear(pSchemaInfo->tablename);
  tDeleteSSchemaWrapper(pSchemaInfo->sw);
  tDeleteSSchemaWrapper(pSchemaInfo->qsw);
}

static void cleanupStreamInfo(SStreamTaskInfo* pStreamInfo) { tDeleteSSchemaWrapper(pStreamInfo->schema); }

bool groupbyTbname(SNodeList* pGroupList) {
  bool bytbname = false;
  if (LIST_LENGTH(pGroupList) == 1) {
    SNode* p = nodesListGetNode(pGroupList, 0);
    if (p->type == QUERY_NODE_FUNCTION) {
      // partition by tbname/group by tbname
      bytbname = (strcmp(((struct SFunctionNode*)p)->functionName, "tbname") == 0);
    }
  }

  return bytbname;
}

SOperatorInfo* createOperatorTree(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SNode* pTagCond,
                                  SNode* pTagIndexCond, const char* pUser) {
  int32_t         type = nodeType(pPhyNode);
  STableListInfo* pTableListInfo = pTaskInfo->pTableInfoList;
  const char*     idstr = GET_TASKID(pTaskInfo);

  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    SOperatorInfo* pOperator = NULL;
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;

      // NOTE: this is an patch to fix the physical plan
      // TODO remove it later
      if (pTableScanNode->scan.node.pLimit != NULL) {
        pTableScanNode->groupSort = true;
      }

      int32_t code =
          createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort, pHandle,
                                  pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        qError("failed to createScanTableListInfo, code:%s, %s", tstrerror(code), idstr);
        return NULL;
      }

      code = extractTableSchemaInfo(pHandle, &pTableScanNode->scan, pTaskInfo);
      if (code) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      pOperator = createTableScanOperatorInfo(pTableScanNode, pHandle, pTaskInfo);
      if (NULL == pOperator) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->base.readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == type) {
      STableMergeScanPhysiNode* pTableScanNode = (STableMergeScanPhysiNode*)pPhyNode;

      int32_t code = createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, true, pHandle,
                                             pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
        return NULL;
      }

      code = extractTableSchemaInfo(pHandle, &pTableScanNode->scan, pTaskInfo);
      if (code) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      pOperator = createTableMergeScanOperatorInfo(pTableScanNode, pHandle, pTaskInfo);
      if (NULL == pOperator) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->base.readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == type) {
      pOperator = createExchangeOperatorInfo(pHandle ? pHandle->pMsgCb->clientRpc : NULL, (SExchangePhysiNode*)pPhyNode,
                                             pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;
      if (pHandle->vnode) {
        int32_t code =
            createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort,
                                    pHandle, pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
        if (code) {
          pTaskInfo->code = code;
          qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
          return NULL;
        }

#ifndef NDEBUG
        int32_t sz = tableListGetSize(pTableListInfo);
        qDebug("create stream task, total:%d", sz);

        for (int32_t i = 0; i < sz; i++) {
          STableKeyInfo* pKeyInfo = tableListGetInfo(pTableListInfo, i);
          qDebug("add table uid:%" PRIu64 ", gid:%" PRIu64, pKeyInfo->uid, pKeyInfo->groupId);
        }
#endif
      }

      pTaskInfo->schemaInfo.qsw = extractQueriedColumnSchema(&pTableScanNode->scan);
      pOperator = createStreamScanOperatorInfo(pHandle, pTableScanNode, pTagCond, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == type) {
      SSystemTableScanPhysiNode* pSysScanPhyNode = (SSystemTableScanPhysiNode*)pPhyNode;
      pOperator = createSysTableScanOperatorInfo(pHandle, pSysScanPhyNode, pUser, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN == type) {
      STableCountScanPhysiNode* pTblCountScanNode = (STableCountScanPhysiNode*)pPhyNode;
      pOperator = createTableCountScanOperatorInfo(pHandle, pTblCountScanNode, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN == type) {
      STagScanPhysiNode* pScanPhyNode = (STagScanPhysiNode*)pPhyNode;

      int32_t code = createScanTableListInfo(pScanPhyNode, NULL, false, pHandle, pTableListInfo, pTagCond,
                                             pTagIndexCond, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        qError("failed to getTableList, code: %s", tstrerror(code));
        return NULL;
      }

      pOperator = createTagScanOperatorInfo(pHandle, pScanPhyNode, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN == type) {
      SBlockDistScanPhysiNode* pBlockNode = (SBlockDistScanPhysiNode*)pPhyNode;

      if (pBlockNode->tableType == TSDB_SUPER_TABLE) {
        SArray* pList = taosArrayInit(4, sizeof(STableKeyInfo));
        int32_t code = vnodeGetAllTableList(pHandle->vnode, pBlockNode->uid, pList);
        if (code != TSDB_CODE_SUCCESS) {
          pTaskInfo->code = terrno;
          return NULL;
        }

        size_t num = taosArrayGetSize(pList);
        for (int32_t i = 0; i < num; ++i) {
          STableKeyInfo* p = taosArrayGet(pList, i);
          tableListAddTableInfo(pTableListInfo, p->uid, 0);
        }

        taosArrayDestroy(pList);
      } else {  // Create group with only one table
        tableListAddTableInfo(pTableListInfo, pBlockNode->uid, 0);
      }

      pOperator = createDataBlockInfoScanOperator(pHandle, pBlockNode, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN == type) {
      SLastRowScanPhysiNode* pScanNode = (SLastRowScanPhysiNode*)pPhyNode;

      int32_t code = createScanTableListInfo(&pScanNode->scan, pScanNode->pGroupTags, true, pHandle, pTableListInfo,
                                             pTagCond, pTagIndexCond, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return NULL;
      }

      code = extractTableSchemaInfo(pHandle, &pScanNode->scan, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return NULL;
      }

      pOperator = createCacherowsScanOperator(pScanNode, pHandle, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
      pOperator = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pPhyNode, pTaskInfo);
    } else {
      ASSERT(0);
    }

    if (pOperator != NULL) {
      pOperator->resultDataBlockId = pPhyNode->pOutputDataBlockDesc->dataBlockId;
    }

    return pOperator;
  }

  size_t          size = LIST_LENGTH(pPhyNode->pChildren);
  SOperatorInfo** ops = taosMemoryCalloc(size, POINTER_BYTES);
  if (ops == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < size; ++i) {
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
    ops[i] = createOperatorTree(pChildNode, pTaskInfo, pHandle, pTagCond, pTagIndexCond, pUser);
    if (ops[i] == NULL) {
      taosMemoryFree(ops);
      return NULL;
    }
  }

  SOperatorInfo* pOptr = NULL;
  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
    pOptr = createProjectOperatorInfo(ops[0], (SProjectPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_AGG == type) {
    SAggPhysiNode* pAggNode = (SAggPhysiNode*)pPhyNode;
    if (pAggNode->pGroupKeys != NULL) {
      pOptr = createGroupOperatorInfo(ops[0], pAggNode, pTaskInfo);
    } else {
      pOptr = createAggregateOperatorInfo(ops[0], pAggNode, pTaskInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL == type) {
    SIntervalPhysiNode* pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;

    bool isStream = (QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL == type);
    pOptr = createIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo, isStream);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL == type) {
    pOptr = createStreamIntervalOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL == type) {
    SMergeAlignedIntervalPhysiNode* pIntervalPhyNode = (SMergeAlignedIntervalPhysiNode*)pPhyNode;
    pOptr = createMergeAlignedIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL == type) {
    SMergeIntervalPhysiNode* pIntervalPhyNode = (SMergeIntervalPhysiNode*)pPhyNode;
    pOptr = createMergeIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL == type) {
    int32_t children = 0;
    pOptr = createStreamFinalIntervalOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL == type) {
    int32_t children = pHandle->numOfVgroups;
    pOptr = createStreamFinalIntervalOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_SORT == type) {
    pOptr = createSortOperatorInfo(ops[0], (SSortPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT == type) {
    pOptr = createGroupSortOperatorInfo(ops[0], (SGroupSortPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE == type) {
    SMergePhysiNode* pMergePhyNode = (SMergePhysiNode*)pPhyNode;
    pOptr = createMultiwayMergeOperatorInfo(ops, size, pMergePhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION == type) {
    SSessionWinodwPhysiNode* pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;
    pOptr = createSessionAggOperatorInfo(ops[0], pSessionNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION == type) {
    pOptr = createStreamSessionAggOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION == type) {
    int32_t children = 0;
    pOptr = createStreamFinalSessionAggOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION == type) {
    int32_t children = pHandle->numOfVgroups;
    pOptr = createStreamFinalSessionAggOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_PARTITION == type) {
    pOptr = createPartitionOperatorInfo(ops[0], (SPartitionPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION == type) {
    pOptr = createStreamPartitionOperatorInfo(ops[0], (SStreamPartitionPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE == type) {
    SStateWinodwPhysiNode* pStateNode = (SStateWinodwPhysiNode*)pPhyNode;
    pOptr = createStatewindowOperatorInfo(ops[0], pStateNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE == type) {
    pOptr = createStreamStateAggOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == type) {
    pOptr = createMergeJoinOperatorInfo(ops, size, (SSortMergeJoinPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_FILL == type) {
    pOptr = createFillOperatorInfo(ops[0], (SFillPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL == type) {
    pOptr = createStreamFillOperatorInfo(ops[0], (SStreamFillPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC == type) {
    pOptr = createIndefinitOutputOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC == type) {
    pOptr = createTimeSliceOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else {
    ASSERT(0);
  }

  taosMemoryFree(ops);
  if (pOptr) {
    pOptr->resultDataBlockId = pPhyNode->pOutputDataBlockDesc->dataBlockId;
  }

  return pOptr;
}

static int32_t extractTbscanInStreamOpTree(SOperatorInfo* pOperator, STableScanInfo** ppInfo) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    if (pOperator->numOfDownstream == 0) {
      qError("failed to find stream scan operator");
      return TSDB_CODE_APP_ERROR;
    }

    if (pOperator->numOfDownstream > 1) {
      qError("join not supported for stream block scan");
      return TSDB_CODE_APP_ERROR;
    }
    return extractTbscanInStreamOpTree(pOperator->pDownstream[0], ppInfo);
  } else {
    SStreamScanInfo* pInfo = pOperator->info;
    *ppInfo = pInfo->pTableScanOp->info;
    return 0;
  }
}

int32_t extractTableScanNode(SPhysiNode* pNode, STableScanPhysiNode** ppNode) {
  if (pNode->pChildren == NULL || LIST_LENGTH(pNode->pChildren) == 0) {
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == pNode->type) {
      *ppNode = (STableScanPhysiNode*)pNode;
      return 0;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      return -1;
    }
  } else {
    if (LIST_LENGTH(pNode->pChildren) != 1) {
      terrno = TSDB_CODE_APP_ERROR;
      return -1;
    }
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pNode->pChildren, 0);
    return extractTableScanNode(pChildNode, ppNode);
  }
  return -1;
}

int32_t createDataSinkParam(SDataSinkNode* pNode, void** pParam, qTaskInfo_t* pTaskInfo, SReadHandle* readHandle) {
  SExecTaskInfo* pTask = *(SExecTaskInfo**)pTaskInfo;

  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT: {
      SInserterParam* pInserterParam = taosMemoryCalloc(1, sizeof(SInserterParam));
      if (NULL == pInserterParam) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pInserterParam->readHandle = readHandle;

      *pParam = pInserterParam;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DELETE: {
      SDeleterParam* pDeleterParam = taosMemoryCalloc(1, sizeof(SDeleterParam));
      if (NULL == pDeleterParam) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      int32_t tbNum = tableListGetSize(pTask->pTableInfoList);
      pDeleterParam->suid = tableListGetSuid(pTask->pTableInfoList);

      // TODO extract uid list
      pDeleterParam->pUidList = taosArrayInit(tbNum, sizeof(uint64_t));
      if (NULL == pDeleterParam->pUidList) {
        taosMemoryFree(pDeleterParam);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      for (int32_t i = 0; i < tbNum; ++i) {
        STableKeyInfo* pTable = tableListGetInfo(pTask->pTableInfoList, i);
        taosArrayPush(pDeleterParam->pUidList, &pTable->uid);
      }

      *pParam = pDeleterParam;
      break;
    }
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                               char* sql, EOPTR_EXEC_MODEL model) {
  uint64_t queryId = pPlan->id.queryId;

  *pTaskInfo = createExecTaskInfo(queryId, taskId, model, pPlan->dbFName);
  if (*pTaskInfo == NULL) {
    goto _complete;
  }

  if (pHandle) {
    /*(*pTaskInfo)->streamInfo.fillHistoryVer1 = pHandle->fillHistoryVer1;*/
    if (pHandle->pStateBackend) {
      (*pTaskInfo)->streamInfo.pState = pHandle->pStateBackend;
    }
  }

  (*pTaskInfo)->sql = sql;
  sql = NULL;

  (*pTaskInfo)->pSubplan = pPlan;
  (*pTaskInfo)->pRoot =
      createOperatorTree(pPlan->pNode, *pTaskInfo, pHandle, pPlan->pTagCond, pPlan->pTagIndexCond, pPlan->user);

  if (NULL == (*pTaskInfo)->pRoot) {
    terrno = (*pTaskInfo)->code;
    goto _complete;
  }

  (*pTaskInfo)->cost.created = taosGetTimestampUs();
  return TSDB_CODE_SUCCESS;

_complete:
  taosMemoryFree(sql);
  doDestroyTask(*pTaskInfo);
  return terrno;
}

static void freeBlock(void* pParam) {
  SSDataBlock* pBlock = *(SSDataBlock**)pParam;
  blockDataDestroy(pBlock);
}

void doDestroyTask(SExecTaskInfo* pTaskInfo) {
  qDebug("%s execTask is freed", GET_TASKID(pTaskInfo));

  pTaskInfo->pTableInfoList = tableListDestroy(pTaskInfo->pTableInfoList);
  destroyOperatorInfo(pTaskInfo->pRoot);
  cleanupTableSchemaInfo(&pTaskInfo->schemaInfo);
  cleanupStreamInfo(&pTaskInfo->streamInfo);

  if (!pTaskInfo->localFetch.localExec) {
    nodesDestroyNode((SNode*)pTaskInfo->pSubplan);
  }

  taosArrayDestroyEx(pTaskInfo->pResultBlockList, freeBlock);
  taosArrayDestroy(pTaskInfo->stopInfo.pStopInfo);
  taosMemoryFreeClear(pTaskInfo->sql);
  taosMemoryFreeClear(pTaskInfo->id.str);
  taosMemoryFreeClear(pTaskInfo);
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

int32_t getOperatorExplainExecInfo(SOperatorInfo* operatorInfo, SArray* pExecInfoList) {
  SExplainExecInfo  execInfo = {0};
  SExplainExecInfo* pExplainInfo = taosArrayPush(pExecInfoList, &execInfo);

  pExplainInfo->numOfRows = operatorInfo->resultInfo.totalRows;
  pExplainInfo->startupCost = operatorInfo->cost.openCost;
  pExplainInfo->totalCost = operatorInfo->cost.totalCost;
  pExplainInfo->verboseLen = 0;
  pExplainInfo->verboseInfo = NULL;

  if (operatorInfo->fpSet.getExplainFn) {
    int32_t code =
        operatorInfo->fpSet.getExplainFn(operatorInfo, &pExplainInfo->verboseInfo, &pExplainInfo->verboseLen);
    if (code) {
      qError("%s operator getExplainFn failed, code:%s", GET_TASKID(operatorInfo->pTaskInfo), tstrerror(code));
      return code;
    }
  }

  int32_t code = 0;
  for (int32_t i = 0; i < operatorInfo->numOfDownstream; ++i) {
    code = getOperatorExplainExecInfo(operatorInfo->pDownstream[i], pExecInfoList);
    if (code != TSDB_CODE_SUCCESS) {
      //      taosMemoryFreeClear(*pRes);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setOutputBuf(SStreamState* pState, STimeWindow* win, SResultRow** pResult, int64_t tableGroupId,
                     SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup) {
  SWinKey key = {
      .ts = win->skey,
      .groupId = tableGroupId,
  };
  char*   value = NULL;
  int32_t size = pAggSup->resultRowSize;

  if (streamStateAddIfNotExist(pState, &key, (void**)&value, &size) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pResult = (SResultRow*)value;
  ASSERT(*pResult);
  // set time window for current result
  (*pResult)->win = (*win);
  setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

int32_t releaseOutputBuf(SStreamState* pState, SWinKey* pKey, SResultRow* pResult) {
  streamStateReleaseBuf(pState, pKey, pResult);
  return TSDB_CODE_SUCCESS;
}

int32_t saveOutputBuf(SStreamState* pState, SWinKey* pKey, SResultRow* pResult, int32_t resSize) {
  streamStatePut(pState, pKey, pResult, resSize);
  return TSDB_CODE_SUCCESS;
}

int32_t buildDataBlockFromGroupRes(SOperatorInfo* pOperator, SStreamState* pState, SSDataBlock* pBlock, SExprSupp* pSup,
                                   SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SResKeyPos* pPos = taosArrayGetP(pGroupResInfo->pRows, i);
    int32_t     size = 0;
    void*       pVal = NULL;
    SWinKey     key = {
            .ts = *(TSKEY*)pPos->key,
            .groupId = pPos->groupId,
    };
    int32_t code = streamStateGet(pState, &key, &pVal, &size);
    ASSERT(code == 0);
    SResultRow* pRow = (SResultRow*)pVal;
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      releaseOutputBuf(pState, &key, pRow);
      continue;
    }

    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = pPos->groupId;
      void* tbname = NULL;
      if (streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
        pBlock->info.parTbName[0] = 0;
      } else {
        memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      }
      tdbFree(tbname);
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.id.groupId != pPos->groupId) {
        releaseOutputBuf(pState, &key, pRow);
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      releaseOutputBuf(pState, &key, pRow);
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.finalize) {
        int32_t code1 = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(code1)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code1));
          T_LONG_JMP(pTaskInfo->env, code1);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          colDataAppend(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    pBlock->info.rows += pRow->numOfRows;
    releaseOutputBuf(pState, &key, pRow);
  }
  pBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pBlock, 0);
  return TSDB_CODE_SUCCESS;
}

int32_t saveSessionDiscBuf(SStreamState* pState, SSessionKey* key, void* buf, int32_t size) {
  streamStateSessionPut(pState, key, (const void*)buf, size);
  releaseOutputBuf(pState, NULL, (SResultRow*)buf);
  return TSDB_CODE_SUCCESS;
}

int32_t buildSessionResultDataBlock(SOperatorInfo* pOperator, SStreamState* pState, SSDataBlock* pBlock,
                                    SExprSupp* pSup, SGroupResInfo* pGroupResInfo) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SSessionKey* pKey = taosArrayGet(pGroupResInfo->pRows, i);
    int32_t      size = 0;
    void*        pVal = NULL;
    int32_t      code = streamStateSessionGet(pState, pKey, &pVal, &size);
    ASSERT(code == 0);
    if (code == -1) {
      // coverity scan
      pGroupResInfo->index += 1;
      continue;
    }
    SResultRow* pRow = (SResultRow*)pVal;
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      releaseOutputBuf(pState, NULL, pRow);
      continue;
    }

    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = pKey->groupId;

      void* tbname = NULL;
      if (streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
        pBlock->info.parTbName[0] = 0;
      } else {
        memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      }
      tdbFree(tbname);
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.id.groupId != pKey->groupId) {
        releaseOutputBuf(pState, NULL, pRow);
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      releaseOutputBuf(pState, NULL, pRow);
      break;
    }

    pGroupResInfo->index += 1;

    for (int32_t j = 0; j < numOfExprs; ++j) {
      int32_t slotId = pExprInfo[j].base.resSchema.slotId;

      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.finalize) {
        int32_t code1 = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
        if (TAOS_FAILED(code1)) {
          qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(code1));
          T_LONG_JMP(pTaskInfo->env, code1);
        }
      } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
        // do nothing, todo refactor
      } else {
        // expand the result into multiple rows. E.g., _wstart, top(k, 20)
        // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
        for (int32_t k = 0; k < pRow->numOfRows; ++k) {
          colDataAppend(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    pBlock->info.dataLoad = 1;
    pBlock->info.rows += pRow->numOfRows;
    // saveSessionDiscBuf(pState, pKey, pVal, size);
    releaseOutputBuf(pState, NULL, pRow);
  }
  blockDataUpdateTsWindow(pBlock, 0);
  return TSDB_CODE_SUCCESS;
}
