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
#include "tref.h"

#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tsort.h"
#include "ttime.h"

#include "executorimpl.h"
#include "index.h"
#include "query.h"
#include "tcompare.h"
#include "tcompression.h"
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

int32_t getMaximumIdleDurationSec() { return tsShellActivityTimer * 2; }

static void setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExpr, SSDataBlock* pBlock);

static void releaseQueryBuf(size_t numOfTables);

static void destroyFillOperatorInfo(void* param);
static void destroyProjectOperatorInfo(void* param);
static void destroyOrderOperatorInfo(void* param);
static void destroyAggOperatorInfo(void* param);

static void destroyIntervalOperatorInfo(void* param);
static void destroyExchangeOperatorInfo(void* param);

static void destroyOperatorInfo(SOperatorInfo* pOperator);

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

static int32_t doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                  SGroupResInfo* pGroupResInfo);

static void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void doSetTableGroupOutputBuf(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId);

#if 0
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
#endif

SResultRow* getNewResultRow(SDiskbasedBuf* pResultBuf, int32_t* currentPageId, int32_t interBufSize) {
  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  if (*currentPageId == -1) {
    pData = getNewBufPage(pResultBuf, &pageId);
    pData->num = sizeof(SFilePage);
  } else {
    pData = getBufPage(pResultBuf, *currentPageId);
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
    if (masterscan && p1 != NULL) {  // the *p1 may be NULL in case of sliding+offset exists.
      pResult = getResultRowByPos(pResultBuf, p1, true);
      ASSERT(pResult->pageId == p1->pageId && pResult->offset == p1->offset);
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existInCurrentResusltRowInfo in the
    // pResultRowInfo object.
    if (p1 != NULL) {
      // todo
      pResult = getResultRowByPos(pResultBuf, p1, true);
      ASSERT(pResult->pageId == p1->pageId && pResult->offset == p1->offset);
    }
  }

  // 1. close current opened time window
  if (pResultRowInfo->cur.pageId != -1 && ((pResult == NULL) || (pResult->pageId != pResultRowInfo->cur.pageId))) {
    SResultRowPosition pos = pResultRowInfo->cur;
    SFilePage*         pPage = getBufPage(pResultBuf, pos.pageId);
    releaseBufPage(pResultBuf, pPage);
  }

  // allocate a new buffer page
  if (pResult == NULL) {
    ASSERT(pSup->resultRowSize > 0);
    pResult = getNewResultRow(pResultBuf, &pSup->currentPageId, pSup->resultRowSize);

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
static int32_t addNewWindowResultBuf(SResultRow* pWindowRes, SDiskbasedBuf* pResultBuf, int32_t tid, uint32_t size) {
  if (pWindowRes->pageId != -1) {
    return 0;
  }

  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SIDList list = getDataBufPagesIdList(pResultBuf);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewBufPage(pResultBuf, &pageId);
    pData->num = sizeof(SFilePage);
  } else {
    SPageInfo* pi = getLastPageInfo(list);
    pData = getBufPage(pResultBuf, getPageId(pi));
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

  colInfoDataEnsureCapacity(pColData, 5);
  colDataAppendInt64(pColData, 0, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 1, &pQueryWindow->ekey);

  int64_t interval = 0;
  colDataAppendInt64(pColData, 2, &interval);  // this value may be variable in case of 'n' and 'y'.
  colDataAppendInt64(pColData, 3, &pQueryWindow->skey);
  colDataAppendInt64(pColData, 4, &pQueryWindow->ekey);
}

void cleanupExecTimeWindowInfo(SColumnInfoData* pColData) { colDataDestroy(pColData); }

typedef struct {
  bool    hasAgg;
  int32_t numOfRows;
  int32_t startOffset;
} SFunctionCtxStatus;

static void functionCtxSave(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus) {
  pStatus->hasAgg = pCtx->input.colDataAggIsSet;
  pStatus->numOfRows = pCtx->input.numOfRows;
  pStatus->startOffset = pCtx->input.startRowIndex;
}

static void functionCtxRestore(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus) {
  pCtx->input.colDataAggIsSet = pStatus->hasAgg;
  pCtx->input.numOfRows = pStatus->numOfRows;
  pCtx->input.startRowIndex = pStatus->startOffset;
}

void doApplyFunctions(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, SColumnInfoData* pTimeWindowData, int32_t offset,
                      int32_t forwardStep, int32_t numOfTotal, int32_t numOfOutput) {
  for (int32_t k = 0; k < numOfOutput; ++k) {
    // keep it temporarily
    SFunctionCtxStatus status = {0};
    functionCtxSave(&pCtx[k], &status);

    pCtx[k].input.startRowIndex = offset;
    pCtx[k].input.numOfRows = forwardStep;

    // not a whole block involved in query processing, statistics data can not be used
    // NOTE: the original value of isSet have been changed here
    if (pCtx[k].input.colDataAggIsSet && forwardStep < numOfTotal) {
      pCtx[k].input.colDataAggIsSet = false;
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

static int32_t doSetInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order,
                                   int32_t scanFlag, bool createDummyCol);

static void doSetInputDataBlockInfo(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock,
                                    int32_t order) {
  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;
    setBlockSMAInfo(&pCtx[i], &pOperator->exprSupp.pExprInfo[i], pBlock);
    pCtx[i].pSrcBlock = pBlock;
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

  colInfoDataEnsureCapacity(pColInfo, numOfRows);

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

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;

    pCtx[i].pSrcBlock = pBlock;
    pCtx[i].scanFlag = scanFlag;

    SInputColumnInfoData* pInput = &pCtx[i].input;
    pInput->uid = pBlock->info.uid;
    pInput->colDataAggIsSet = false;

    SExprInfo* pOneExpr = &pOperator->exprSupp.pExprInfo[i];
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

static void setPseudoOutputColInfo(SSDataBlock* pResult, SqlFunctionCtx* pCtx, SArray* pPseudoList) {
  size_t num = (pPseudoList != NULL) ? taosArrayGetSize(pPseudoList) : 0;
  for (int32_t i = 0; i < num; ++i) {
    pCtx[i].pOutput = taosArrayGet(pResult->pDataBlock, i);
  }
}

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList) {
  setPseudoOutputColInfo(pResult, pCtx, pPseudoList);

  if (pSrcBlock == NULL) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

      ASSERT(pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE);
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataAppendNNULL(pColInfoData, 0, 1);
      } else {
        colDataAppend(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
      }
    }

    pResult->info.rows = 1;
    return TSDB_CODE_SUCCESS;
  }

  pResult->info.groupId = pSrcBlock->info.groupId;

  // if the source equals to the destination, it is to create a new column as the result of scalar
  // function or some operators.
  bool createNewColModel = (pResult == pSrcBlock);

  int32_t numOfRows = 0;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t               outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx*       pfCtx = &pCtx[k];
    SInputColumnInfoData* pInputData = &pfCtx->input;

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_COLUMN) {  // it is a project query
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pResult->info.rows > 0 && !createNewColModel) {
        colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity, pInputData->pData[0],
                        pInputData->numOfRows);
      } else {
        colDataAssign(pColInfoData, pInputData->pData[0], pInputData->numOfRows, &pResult->info);
      }

      numOfRows = pInputData->numOfRows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);

      int32_t offset = createNewColModel ? 0 : pResult->info.rows;

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataAppendNNULL(pColInfoData, offset, pSrcBlock->info.rows);
      } else {
        for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
          colDataAppend(pColInfoData, i + offset, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
        }
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
      ASSERT(pResult->info.capacity > 0);
      colDataMergeCol(pResColData, startOffset, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
      colDataDestroy(&idata);

      numOfRows = dest.numOfRows;
      taosArrayDestroy(pBlockList);
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      // _rowts/_c0, not tbname column
      if (fmIsPseudoColumnFunc(pfCtx->functionId) && (!fmIsScanPseudoColumnFunc(pfCtx->functionId))) {
        // do nothing
      } else if (fmIsIndefiniteRowsFunc(pfCtx->functionId)) {
        SResultRowEntryInfo* pResInfo = GET_RES_INFO(pfCtx);
        pfCtx->fpSet.init(pfCtx, pResInfo);

        pfCtx->pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        pfCtx->offset = createNewColModel ? 0 : pResult->info.rows;  // set the start offset

        // set the timestamp(_rowts) output buffer
        if (taosArrayGetSize(pPseudoList) > 0) {
          int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
          pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
        }

        // link pDstBlock to set selectivity value
        if (pfCtx->subsidiaries.num > 0) {
          pfCtx->pDstBlock = pResult;
        }

        numOfRows = pfCtx->fpSet.process(pfCtx);
      } else if (fmIsAggFunc(pfCtx->functionId)) {
        // selective value output should be set during corresponding function execution
        if (fmIsSelectValueFunc(pfCtx->functionId)) {
          continue;
        }
        // _group_key function for "partition by tbname" + csum(col_name) query
        SColumnInfoData* pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        int32_t          slotId = pfCtx->param[0].pCol->slotId;

        // todo handle the json tag
        SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
        for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
          bool isNull = colDataIsNull_s(pInput, f);
          if (isNull) {
            colDataAppendNULL(pOutput, pResult->info.rows + f);
          } else {
            char* data = colDataGetData(pInput, f);
            colDataAppend(pOutput, pResult->info.rows + f, data, isNull);
          }
        }

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
        ASSERT(pResult->info.capacity > 0);
        colDataMergeCol(pResColData, startOffset, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
        colDataDestroy(&idata);

        numOfRows = dest.numOfRows;
        taosArrayDestroy(pBlockList);
      }
    } else {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  if (!createNewColModel) {
    pResult->info.rows += numOfRows;
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
    ASSERT(0);
  }

  return TSDB_CODE_SUCCESS;
}

void setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, SSDataBlock* pBlock) {
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

#if 0
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
#endif

static uint32_t doFilterByBlockTimeWindow(STableScanInfo* pTableScanInfo, SSDataBlock* pBlock) {
#if 0
  SqlFunctionCtx* pCtx = pTableScanInfo->pCtx;
  uint32_t        status = BLK_DATA_NOT_LOAD;

  int32_t numOfOutput = 0;  // pTableScanInfo->numOfOutput;
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
#endif
  return 0;
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
        if (setResultOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pBlock->info.uid, &win, masterScan, &pResult, groupId,
                                    pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                    pTableScanInfo->rowEntryInfoOffset) != TSDB_CODE_SUCCESS) {
          T_LONG_JMP(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
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
          if (setResultOutputBufByKey(pRuntimeEnv, pTableScanInfo->pResultRowInfo, pBlock->info.uid, &win, masterScan, &pResult, groupId,
                                      pTableScanInfo->pCtx, pTableScanInfo->numOfOutput,
                                      pTableScanInfo->rowEntryInfoOffset) != TSDB_CODE_SUCCESS) {
            T_LONG_JMP(pRuntimeEnv->env, TSDB_CODE_QRY_OUT_OF_MEMORY);
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

static void updateTableQueryInfoForReverseScan(STableQueryInfo* pTableQueryInfo) {
  if (pTableQueryInfo == NULL) {
    return;
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

static void extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, bool keep,
                                                int32_t status);

void doFilter(const SNode* pFilterNode, SSDataBlock* pBlock, const SArray* pColMatchInfo) {
  if (pFilterNode == NULL || pBlock->info.rows == 0) {
    return;
  }

  SFilterInfo* filter = NULL;

  // todo move to the initialization function
  int32_t            code = filterInitFromNode((SNode*)pFilterNode, &filter, 0);
  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param1);

  SColumnInfoData* p = NULL;
  int32_t          status = 0;

  // todo the keep seems never to be True??
  bool keep = filterExecute(filter, pBlock, &p, NULL, param1.numOfCols, &status);
  filterFreeInfo(filter);

  extractQualifiedTupleByFilterResult(pBlock, p, keep, status);

  if (pColMatchInfo != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pColMatchInfo); ++i) {
      SColMatchInfo* pInfo = taosArrayGet(pColMatchInfo, i);
      if (pInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pInfo->targetSlotId);
        if (pColData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
          blockDataUpdateTsWindow(pBlock, pInfo->targetSlotId);
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

  int32_t totalRows = pBlock->info.rows;

  if (status == FILTER_RESULT_ALL_QUALIFIED) {
    // here nothing needs to be done
  } else if (status == FILTER_RESULT_NONE_QUALIFIED) {
    pBlock->info.rows = 0;
  } else {
    SSDataBlock* px = createOneDataBlock(pBlock, true);

    size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pSrc = taosArrayGet(px->pDataBlock, i);
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      // it is a reserved column for scalar function, and no data in this column yet.
      if (pDst->pData == NULL || pSrc->pData == NULL) {
        continue;
      }

      colInfoDataCleanup(pDst, pBlock->info.rows);

      int32_t numOfRows = 0;
      for (int32_t j = 0; j < totalRows; ++j) {
        if (((int8_t*)p->pData)[j] == 0) {
          continue;
        }

        if (colDataIsNull_s(pSrc, j)) {
          colDataAppendNULL(pDst, numOfRows);
        } else {
          colDataAppend(pDst, numOfRows, colDataGetData(pSrc, j), false);
        }
        numOfRows += 1;
      }

      // todo this value can be assigned directly
      if (pBlock->info.rows == totalRows) {
        pBlock->info.rows = numOfRows;
      } else {
        ASSERT(pBlock->info.rows == numOfRows);
      }
    }

    blockDataDestroy(px);  // fix memory leak
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
    int32_t ret =
        addNewWindowResultBuf(pResultRow, pAggInfo->aggSup.pResultBuf, groupId, pAggInfo->binfo.pRes->info.rowSize);
    if (ret != TSDB_CODE_SUCCESS) {
      return;
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
                              const int32_t* rowCellOffset) {
  bool returnNotNull = false;
  for (int32_t j = 0; j < numOfExprs; ++j) {
    struct SResultRowEntryInfo* pResInfo = getResultEntryInfo(pRow, j, rowCellOffset);
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

    SResultRow* pRow = (SResultRow*)((char*)page + pPos->pos.offset);

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);

    // no results, continue to check the next one
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
         pBlock->info.groupId);

  blockDataUpdateTsWindow(pBlock, 0);
  return 0;
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
  pBlock->info.groupId = 0;
  if (!pbInfo->mergeResultBlock) {
    doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo);
  } else {
    while (hasRemainResults(pGroupResInfo)) {
      doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo);
      if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }

      // clearing group id to continue to merge data that belong to different groups
      pBlock->info.groupId = 0;
    }

    // clear the group id info in SSDataBlock, since the client does not need it
    pBlock->info.groupId = 0;
  }
}

static int32_t compressQueryColData(SColumnInfoData* pColRes, int32_t numOfRows, char* data, int8_t compressed) {
  int32_t colSize = pColRes->info.bytes * numOfRows;
  return (*(tDataTypes[pColRes->info.type].compFunc))(pColRes->pData, colSize, numOfRows, data,
                                                      colSize + COMP_OVERFLOW_BYTES, compressed, NULL, 0);
}

void queryCostStatis(SExecTaskInfo* pTaskInfo) {
  STaskCostInfo* pSummary = &pTaskInfo->cost;

  //  uint64_t hashSize = taosHashGetMemSize(pQInfo->runtimeEnv.pResultRowHashTable);
  //  hashSize += taosHashGetMemSize(pRuntimeEnv->tableqinfoGroupInfo.map);
  //  pSummary->hashSize = hashSize;

  //  SResultRowPool* p = pTaskInfo->pool;
  //  if (p != NULL) {
  //    pSummary->winInfoSize = getResultRowPoolMemSize(p);
  //    pSummary->numOfTimeWindows = getNumOfAllocatedResultRows(p);
  //  } else {
  //    pSummary->winInfoSize = 0;
  //    pSummary->numOfTimeWindows = 0;
  //  }

  SFileBlockLoadRecorder* pRecorder = pSummary->pRecoder;
  if (pSummary->pRecoder != NULL) {
    qDebug(
        "%s :cost summary: elapsed time:%.2f ms, total blocks:%d, load block SMA:%d, load data block:%d, total "
        "rows:%" PRId64 ", check rows:%" PRId64,
        GET_TASKID(pTaskInfo), pSummary->elapsedTime / 1000.0, pRecorder->totalBlocks, pRecorder->loadBlockStatis,
        pRecorder->loadBlocks, pRecorder->totalRows, pRecorder->totalCheckedRows);
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
//       T_LONG_JMP(pRuntimeEnv->env, TSDB_CODE_TSC_QUERY_CANCELLED);
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
//     T_LONG_JMP(pRuntimeEnv->env, terrno);
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

static void doDestroyTableList(STableListInfo* pTableqinfoList);

typedef struct SFetchRspHandleWrapper {
  uint32_t exchangeId;
  int32_t  sourceIndex;
} SFetchRspHandleWrapper;

int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SFetchRspHandleWrapper* pWrapper = (SFetchRspHandleWrapper*)param;

  SExchangeInfo* pExchangeInfo = taosAcquireRef(exchangeObjRefPool, pWrapper->exchangeId);
  if (pExchangeInfo == NULL) {
    qWarn("failed to acquire exchange operator, since it may have been released");
    taosMemoryFree(pMsg->pData);
    return TSDB_CODE_SUCCESS;
  }

  int32_t          index = pWrapper->sourceIndex;
  SSourceDataInfo* pSourceDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, index);

  if (code == TSDB_CODE_SUCCESS) {
    pSourceDataInfo->pRsp = pMsg->pData;

    SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->compLen = htonl(pRsp->compLen);
    pRsp->numOfCols = htonl(pRsp->numOfCols);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->numOfBlocks = htonl(pRsp->numOfBlocks);

    ASSERT(pRsp != NULL);
    qDebug("%s fetch rsp received, index:%d, blocks:%d, rows:%d", pSourceDataInfo->taskId, index, pRsp->numOfBlocks,
           pRsp->numOfRows);
  } else {
    taosMemoryFree(pMsg->pData);
    pSourceDataInfo->code = code;
    qDebug("%s fetch rsp received, index:%d, error:%d", pSourceDataInfo->taskId, index, tstrerror(code));
  }

  pSourceDataInfo->status = EX_SOURCE_DATA_READY;

  tsem_post(&pExchangeInfo->ready);
  taosReleaseRef(exchangeObjRefPool, pWrapper->exchangeId);

  return TSDB_CODE_SUCCESS;
}

void qProcessRspMsg(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
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

  SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, sourceIndex);
  SSourceDataInfo*       pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);

  ASSERT(pDataInfo->status == EX_SOURCE_DATA_NOT_READY);

  SFetchRspHandleWrapper* pWrapper = taosMemoryCalloc(1, sizeof(SFetchRspHandleWrapper));
  pWrapper->exchangeId = pExchangeInfo->self;
  pWrapper->sourceIndex = sourceIndex;

  if (pSource->localExec) {
    SDataBuf pBuf = {0};
    int32_t  code =
        (*pTaskInfo->localFetch.fp)(pTaskInfo->localFetch.handle, pSource->schedId, pTaskInfo->id.queryId,
                                    pSource->taskId, 0, pSource->execId, &pBuf.pData, pTaskInfo->localFetch.explainRes);
    loadRemoteDataCallback(pWrapper, &pBuf, code);
    taosMemoryFree(pWrapper);
  } else {
    SResFetchReq* pMsg = taosMemoryCalloc(1, sizeof(SResFetchReq));
    if (NULL == pMsg) {
      pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      return pTaskInfo->code;
    }

    qDebug("%s build fetch msg and send to vgId:%d, ep:%s, taskId:0x%" PRIx64 ", execId:%d, %d/%" PRIzu,
           GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->addr.epSet.eps[0].fqdn, pSource->taskId,
           pSource->execId, sourceIndex, totalSources);

    pMsg->header.vgId = htonl(pSource->addr.nodeId);
    pMsg->sId = htobe64(pSource->schedId);
    pMsg->taskId = htobe64(pSource->taskId);
    pMsg->queryId = htobe64(pTaskInfo->id.queryId);
    pMsg->execId = htonl(pSource->execId);

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      taosMemoryFreeClear(pMsg);
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      return pTaskInfo->code;
    }

    pMsgSendInfo->param = pWrapper;
    pMsgSendInfo->paramFreeFp = taosMemoryFree;
    pMsgSendInfo->msgInfo.pData = pMsg;
    pMsgSendInfo->msgInfo.len = sizeof(SResFetchReq);
    pMsgSendInfo->msgType = pSource->fetchMsgType;
    pMsgSendInfo->fp = loadRemoteDataCallback;

    int64_t transporterId = 0;
    int32_t code =
        asyncSendMsgToServer(pExchangeInfo->pTransporter, &pSource->addr.epSet, &transporterId, pMsgSendInfo);
  }

  return TSDB_CODE_SUCCESS;
}

void updateLoadRemoteInfo(SLoadRemoteDataInfo* pInfo, int32_t numOfRows, int32_t dataLen, int64_t startTs,
                          SOperatorInfo* pOperator) {
  pInfo->totalRows += numOfRows;
  pInfo->totalSize += dataLen;
  pInfo->totalElapsed += (taosGetTimestampUs() - startTs);
  pOperator->resultInfo.totalRows += numOfRows;
}

int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, char* pData, SArray* pColList, char** pNextStart) {
  if (pColList == NULL) {  // data from other sources
    blockDataCleanup(pRes);
    *pNextStart = (char*)blockDecode(pRes, pData);
  } else {  // extract data according to pColList
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

    SSDataBlock* pBlock = createDataBlock();
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData idata = createColumnInfoData(pSchema[i].type, pSchema[i].bytes, pSchema[i].colId);
      blockDataAppendColInfo(pBlock, &idata);
    }

    blockDecode(pBlock, pStart);
    blockDataEnsureCapacity(pRes, pBlock->info.rows);

    // data from mnode
    pRes->info.rows = pBlock->info.rows;
    relocateColumnData(pRes, pColList, pBlock->pDataBlock, false);
    blockDataDestroy(pBlock);
  }

  // todo move this to time window aggregator, since the primary timestamp may not be known by exchange operator.
  blockDataUpdateTsWindow(pRes, 0);
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

static void concurrentlyLoadRemoteDataImpl(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
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

      if (pDataInfo->code != TSDB_CODE_SUCCESS) {
        code = pDataInfo->code;
        goto _error;
      }

      SRetrieveTableRsp*     pRsp = pDataInfo->pRsp;
      SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, i);

      SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
      if (pRsp->numOfRows == 0) {
        qDebug("%s vgId:%d, taskId:0x%" PRIx64 " execId:%d index:%d completed, rowsOfSource:%" PRIu64
               ", totalRows:%" PRIu64 ", completed:%d try next %d/%" PRIzu,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, i, pDataInfo->totalRows,
               pExchangeInfo->loadInfo.totalRows, completed + 1, i + 1, totalSources);
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
        completed += 1;
        taosMemoryFreeClear(pDataInfo->pRsp);
        continue;
      }

      SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
      int32_t            index = 0;
      char*              pStart = pRetrieveRsp->data;
      while (index++ < pRetrieveRsp->numOfBlocks) {
        SSDataBlock* pb = createOneDataBlock(pExchangeInfo->pDummyBlock, false);
        code = extractDataBlockFromFetchRsp(pb, pStart, NULL, &pStart);
        if (code != 0) {
          taosMemoryFreeClear(pDataInfo->pRsp);
          goto _error;
        }

        taosArrayPush(pExchangeInfo->pResultBlockList, &pb);
      }

      updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, startTs, pOperator);

      if (pRsp->completed == 1) {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64
               " execId:%d"
               " index:%d completed, blocks:%d, numOfRows:%d, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64
               ", total:%.2f Kb,"
               " completed:%d try next %d/%" PRIzu,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, i, pRsp->numOfBlocks,
               pRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0,
               completed + 1, i + 1, totalSources);
        completed += 1;
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      } else {
        qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64
               " execId:%d blocks:%d, numOfRows:%d, totalRows:%" PRIu64 ", total:%.2f Kb",
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pRsp->numOfBlocks,
               pRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0);
      }

      taosMemoryFreeClear(pDataInfo->pRsp);

      if (pDataInfo->status != EX_SOURCE_DATA_EXHAUSTED) {
        pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFreeClear(pDataInfo->pRsp);
          goto _error;
        }
      }

      return;
    }

    if (completed == totalSources) {
      setAllSourcesCompleted(pOperator, startTs);
      return;
    }

    sched_yield();
  }

_error:
  pTaskInfo->code = code;
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
  qDebug("%s send all fetch requests to %" PRIzu " sources completed, elapsed:%.2fms", GET_TASKID(pTaskInfo),
         totalSources, (endTs - startTs) / 1000.0);

  pOperator->status = OP_RES_TO_RETURN;
  pOperator->cost.openCost = taosGetTimestampUs() - startTs;

  tsem_wait(&pExchangeInfo->ready);
  return TSDB_CODE_SUCCESS;
}

static int32_t seqLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  while (1) {
    if (pExchangeInfo->current >= totalSources) {
      setAllSourcesCompleted(pOperator, startTs);
      return TSDB_CODE_SUCCESS;
    }

    doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    tsem_wait(&pExchangeInfo->ready);

    SSourceDataInfo*       pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, taskID:0x%" PRIx64 " execId:%d error happens, code:%s", GET_TASKID(pTaskInfo),
             pSource->addr.nodeId, pSource->taskId, pSource->execId, tstrerror(pDataInfo->code));
      pOperator->pTaskInfo->code = pDataInfo->code;
      return pOperator->pTaskInfo->code;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
    if (pRsp->numOfRows == 0) {
      qDebug("%s vgId:%d, taskID:0x%" PRIx64 " execId:%d %d of total completed, rowsOfSource:%" PRIu64
             ", totalRows:%" PRIu64 " try next",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pExchangeInfo->current + 1,
             pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
      taosMemoryFreeClear(pDataInfo->pRsp);
      continue;
    }

    SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;

    char*   pStart = pRetrieveRsp->data;
    int32_t code = extractDataBlockFromFetchRsp(NULL, pStart, NULL, &pStart);

    if (pRsp->completed == 1) {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " execId:%d numOfRows:%d, rowsOfSource:%" PRIu64
             ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pRetrieveRsp->numOfRows,
             pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize, pExchangeInfo->current + 1,
             totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      pExchangeInfo->current += 1;
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, taskId:0x%" PRIx64 " execId:%d numOfRows:%d, totalRows:%" PRIu64
             ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pSource->execId, pRetrieveRsp->numOfRows,
             pLoadInfo->totalRows, pLoadInfo->totalSize);
    }

    updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, startTs, pOperator);
    pDataInfo->totalRows += pRetrieveRsp->numOfRows;

    taosMemoryFreeClear(pDataInfo->pRsp);
    return TSDB_CODE_SUCCESS;
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

static void freeBlock(void* pParam) {
  SSDataBlock* pBlock = *(SSDataBlock**)pParam;
  blockDataDestroy(pBlock);
}

static SSDataBlock* doLoadRemoteDataImpl(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    qDebug("%s all %" PRIzu " source(s) are exhausted, total rows:%" PRIu64 " bytes:%" PRIu64 ", elapsed:%.2f ms",
           GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize,
           pLoadInfo->totalElapsed / 1000.0);
    return NULL;
  }

  size_t size = taosArrayGetSize(pExchangeInfo->pResultBlockList);
  if (size == 0 || pExchangeInfo->rspBlockIndex >= size) {
    pExchangeInfo->rspBlockIndex = 0;
    taosArrayClearEx(pExchangeInfo->pResultBlockList, freeBlock);
    if (pExchangeInfo->seqLoadData) {
      seqLoadRemoteData(pOperator);
    } else {
      concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
    }

    if (taosArrayGetSize(pExchangeInfo->pResultBlockList) == 0) {
      return NULL;
    }
  }

  // we have buffered retrieved datablock, return it directly
  return taosArrayGetP(pExchangeInfo->pResultBlockList, pExchangeInfo->rspBlockIndex++);
}

static SSDataBlock* doLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  while (1) {
    SSDataBlock* pBlock = doLoadRemoteDataImpl(pOperator);
    if (pBlock == NULL) {
      return NULL;
    }

    SLimitInfo* pLimitInfo = &pExchangeInfo->limitInfo;
    if (hasLimitOffsetInfo(pLimitInfo)) {
      int32_t status = handleLimitOffset(pOperator, pLimitInfo, pBlock, false);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      } else if (status == PROJECT_RETRIEVE_DONE) {
        size_t rows = pBlock->info.rows;
        pExchangeInfo->limitInfo.numOfOutputRows += rows;

        if (rows == 0) {
          doSetOperatorCompleted(pOperator);
          return NULL;
        } else {
          return pBlock;
        }
      }
    } else {
      return pBlock;
    }
  }
}

static int32_t initDataSource(int32_t numOfSources, SExchangeInfo* pInfo, const char* id) {
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = id;
    dataInfo.index = i;
    SSourceDataInfo* pDs = taosArrayPush(pInfo->pSourceDataInfo, &dataInfo);
    if (pDs == NULL) {
      taosArrayDestroy(pInfo->pSourceDataInfo);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initExchangeOperator(SExchangePhysiNode* pExNode, SExchangeInfo* pInfo, const char* id) {
  size_t numOfSources = LIST_LENGTH(pExNode->pSrcEndPoints);

  if (numOfSources == 0) {
    qError("%s invalid number: %d of sources in exchange operator", id, (int32_t)numOfSources);
    return TSDB_CODE_INVALID_PARA;
  }

  pInfo->pSources = taosArrayInit(numOfSources, sizeof(SDownstreamSourceNode));
  if (pInfo->pSources == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)nodesListGetNode((SNodeList*)pExNode->pSrcEndPoints, i);
    taosArrayPush(pInfo->pSources, pNode);
  }

  initLimitInfo(pExNode->node.pLimit, pExNode->node.pSlimit, &pInfo->limitInfo);
  pInfo->self = taosAddRef(exchangeObjRefPool, pInfo);

  return initDataSource(numOfSources, pInfo, id);
}

SOperatorInfo* createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo) {
  SExchangeInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t code = initExchangeOperator(pExNode, pInfo, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  tsem_init(&pInfo->ready, 0, 0);
  pInfo->pDummyBlock = createResDataBlock(pExNode->node.pOutputDataBlockDesc);
  pInfo->pResultBlockList = taosArrayInit(1, POINTER_BYTES);

  pInfo->seqLoadData = false;
  pInfo->pTransporter = pTransporter;

  pOperator->name = "ExchangeOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pDummyBlock->pDataBlock);
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(prepareLoadRemoteData, doLoadRemoteData, NULL, NULL,
                                         destroyExchangeOperatorInfo, NULL, NULL, NULL);
  return pOperator;

_error:
  if (pInfo != NULL) {
    doDestroyExchangeOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                                const char* pKey);

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

int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag) {
  // todo add more information about exchange operation
  int32_t type = pOperator->operatorType;
  if (type == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE || type == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN) {
    *order = TSDB_ORDER_ASC;
    *scanFlag = MAIN_SCAN;
    return TSDB_CODE_SUCCESS;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = pOperator->info;
    *order = pTableScanInfo->cond.order;
    *scanFlag = pTableScanInfo->scanFlag;
    return TSDB_CODE_SUCCESS;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    STableMergeScanInfo* pTableScanInfo = pOperator->info;
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

  SExprSupp*     pSup = &pOperator->exprSupp;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

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
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // there is an scalar expression that needs to be calculated before apply the group aggregation.
    if (pAggInfo->scalarExprSup.pExprInfo != NULL) {
      SExprSupp* pSup1 = &pAggInfo->scalarExprSup;
      code = projectApplyFunctions(pSup1->pExprInfo, pBlock, pBlock, pSup1->pCtx, pSup1->numOfExprs, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    // the pDataBlock are always the same one, no need to call this again
    setExecutionContext(pOperator, pOperator->exprSupp.numOfExprs, pBlock->info.groupId);
    setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, scanFlag, true);
    code = doAggregateImpl(pOperator, pSup->pCtx);
    if (code != 0) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

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
  while (1) {
    doBuildResultDatablock(pOperator, pInfo, &pAggInfo->groupResInfo, pAggInfo->aggSup.pResultBuf);
    doFilter(pAggInfo->pCondition, pInfo->pRes, NULL);

    if (!hasRemainResults(&pAggInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
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

int32_t aggEncodeResultRow(SOperatorInfo* pOperator, char** result, int32_t* length) {
  if (result == NULL || length == NULL) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }
  SOptrBasicInfo* pInfo = (SOptrBasicInfo*)(pOperator->info);
  SAggSupporter*  pSup = (SAggSupporter*)POINTER_SHIFT(pOperator->info, sizeof(SOptrBasicInfo));
  int32_t         size = tSimpleHashGetSize(pSup->pResultRowHashTable);
  size_t          keyLen = sizeof(uint64_t) * 2;  // estimate the key length
  int32_t         totalSize =
      sizeof(int32_t) + sizeof(int32_t) + size * (sizeof(int32_t) + keyLen + sizeof(int32_t) + pSup->resultRowSize);

  // no result
  if (getTotalBufSize(pSup->pResultBuf) == 0) {
    *result = NULL;
    *length = 0;
    return TSDB_CODE_SUCCESS;
  }

  *result = (char*)taosMemoryCalloc(1, totalSize);
  if (*result == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t offset = sizeof(int32_t);
  *(int32_t*)(*result + offset) = size;
  offset += sizeof(int32_t);

  // prepare memory
  SResultRowPosition* pos = &pInfo->resultRowInfo.cur;
  void*               pPage = getBufPage(pSup->pResultBuf, pos->pageId);
  SResultRow*         pRow = (SResultRow*)((char*)pPage + pos->offset);
  setBufPageDirty(pPage, true);
  releaseBufPage(pSup->pResultBuf, pPage);

  int32_t iter = 0;
  void*   pIter = NULL;
  while ((pIter = tSimpleHashIterate(pSup->pResultRowHashTable, pIter, &iter))) {
    void*               key = tSimpleHashGetKey(pIter, &keyLen);
    SResultRowPosition* p1 = (SResultRowPosition*)pIter;

    pPage = (SFilePage*)getBufPage(pSup->pResultBuf, p1->pageId);
    pRow = (SResultRow*)((char*)pPage + p1->offset);
    setBufPageDirty(pPage, true);
    releaseBufPage(pSup->pResultBuf, pPage);

    // recalculate the result size
    int32_t realTotalSize = offset + sizeof(int32_t) + keyLen + sizeof(int32_t) + pSup->resultRowSize;
    if (realTotalSize > totalSize) {
      char* tmp = (char*)taosMemoryRealloc(*result, realTotalSize);
      if (tmp == NULL) {
        taosMemoryFree(*result);
        *result = NULL;
        return TSDB_CODE_OUT_OF_MEMORY;
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
  }

  *(int32_t*)(*result) = offset;
  *length = offset;

  return TDB_CODE_SUCCESS;
}

int32_t aggDecodeResultRow(SOperatorInfo* pOperator, char* result) {
  if (result == NULL) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }
  SOptrBasicInfo* pInfo = (SOptrBasicInfo*)(pOperator->info);
  SAggSupporter*  pSup = (SAggSupporter*)POINTER_SHIFT(pOperator->info, sizeof(SOptrBasicInfo));

  //  int32_t size = taosHashGetSize(pSup->pResultRowHashTable);
  int32_t length = *(int32_t*)(result);
  int32_t offset = sizeof(int32_t);

  int32_t count = *(int32_t*)(result + offset);
  offset += sizeof(int32_t);

  while (count-- > 0 && length > offset) {
    int32_t keyLen = *(int32_t*)(result + offset);
    offset += sizeof(int32_t);

    uint64_t    tableGroupId = *(uint64_t*)(result + offset);
    SResultRow* resultRow = getNewResultRow(pSup->pResultBuf, &pSup->currentPageId, pSup->resultRowSize);
    if (!resultRow) {
      return TSDB_CODE_TSC_INVALID_INPUT;
    }

    // add a new result set for a new group
    SResultRowPosition pos = {.pageId = resultRow->pageId, .offset = resultRow->offset};
    tSimpleHashPut(pSup->pResultRowHashTable, result + offset, keyLen, &pos, sizeof(SResultRowPosition));

    offset += keyLen;
    int32_t valueLen = *(int32_t*)(result + offset);
    if (valueLen != pSup->resultRowSize) {
      return TSDB_CODE_TSC_INVALID_INPUT;
    }
    offset += sizeof(int32_t);
    int32_t pageId = resultRow->pageId;
    int32_t pOffset = resultRow->offset;
    memcpy(resultRow, result + offset, valueLen);
    resultRow->pageId = pageId;
    resultRow->offset = pOffset;
    offset += valueLen;

    pInfo->resultRowInfo.cur = (SResultRowPosition){.pageId = resultRow->pageId, .offset = resultRow->offset};
    // releaseBufPage(pSup->pResultBuf, getBufPage(pSup->pResultBuf, pageId));
  }

  if (offset != length) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }
  return TDB_CODE_SUCCESS;
}

int32_t handleLimitOffset(SOperatorInfo* pOperator, SLimitInfo* pLimitInfo, SSDataBlock* pBlock, bool holdDataInBuf) {
  if (pLimitInfo->remainGroupOffset > 0) {
    if (pLimitInfo->currentGroupId == 0) {  // it is the first group
      pLimitInfo->currentGroupId = pBlock->info.groupId;
      blockDataCleanup(pBlock);
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pLimitInfo->currentGroupId != pBlock->info.groupId) {
      // now it is the data from a new group
      pLimitInfo->remainGroupOffset -= 1;

      // ignore data block in current group
      if (pLimitInfo->remainGroupOffset > 0) {
        blockDataCleanup(pBlock);
        return PROJECT_RETRIEVE_CONTINUE;
      }
    }

    // set current group id of the project operator
    pLimitInfo->currentGroupId = pBlock->info.groupId;
  }

  // here check for a new group data, we need to handle the data of the previous group.
  if (pLimitInfo->currentGroupId != 0 && pLimitInfo->currentGroupId != pBlock->info.groupId) {
    pLimitInfo->numOfOutputGroups += 1;
    if ((pLimitInfo->slimit.limit > 0) && (pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      pOperator->status = OP_EXEC_DONE;
      blockDataCleanup(pBlock);

      return PROJECT_RETRIEVE_DONE;
    }

    // reset the value for a new group data
    pLimitInfo->numOfOutputRows = 0;
    pLimitInfo->remainOffset = pLimitInfo->limit.offset;

    // existing rows that belongs to previous group.
    if (pBlock->info.rows > 0) {
      return PROJECT_RETRIEVE_DONE;
    }
  }

  // here we reach the start position, according to the limit/offset requirements.

  // set current group id
  pLimitInfo->currentGroupId = pBlock->info.groupId;

  if (pLimitInfo->remainOffset >= pBlock->info.rows) {
    pLimitInfo->remainOffset -= pBlock->info.rows;
    blockDataCleanup(pBlock);
    return PROJECT_RETRIEVE_CONTINUE;
  } else if (pLimitInfo->remainOffset < pBlock->info.rows && pLimitInfo->remainOffset > 0) {
    blockDataTrimFirstNRows(pBlock, pLimitInfo->remainOffset);
    pLimitInfo->remainOffset = 0;
  }

  // check for the limitation in each group
  if (pLimitInfo->limit.limit >= 0 && pLimitInfo->numOfOutputRows + pBlock->info.rows >= pLimitInfo->limit.limit) {
    int32_t keepRows = (int32_t)(pLimitInfo->limit.limit - pLimitInfo->numOfOutputRows);
    blockDataKeepFirstNRows(pBlock, keepRows);
    if (pLimitInfo->slimit.limit > 0 && pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups) {
      pOperator->status = OP_EXEC_DONE;
    }

    return PROJECT_RETRIEVE_DONE;
  }

  // todo optimize performance
  // If there are slimit/soffset value exists, multi-round result can not be packed into one group, since the
  // they may not belong to the same group the limit/offset value is not valid in this case.
  if ((!holdDataInBuf) || (pBlock->info.rows >= pOperator->resultInfo.threshold) || pLimitInfo->slimit.offset != -1 ||
      pLimitInfo->slimit.limit != -1) {
    return PROJECT_RETRIEVE_DONE;
  } else {  // not full enough, continue to accumulate the output data in the buffer.
    return PROJECT_RETRIEVE_CONTINUE;
  }
}

static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);
static void doHandleRemainBlockForNewGroupImpl(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                               SResultInfo* pResultInfo, SExecTaskInfo* pTaskInfo) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;
  SSDataBlock* pResBlock = pInfo->pFinalRes;

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;
  getTableScanInfo(pOperator, &order, &scanFlag);

  int64_t ekey =
      Q_STATUS_EQUAL(pTaskInfo->status, TASK_COMPLETED) ? pInfo->win.ekey : pInfo->existNewGroupBlock->info.window.ekey;
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  blockDataCleanup(pInfo->pRes);
  doApplyScalarCalculation(pOperator, pInfo->existNewGroupBlock, order, scanFlag);

  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ekey);
  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);

  int32_t numOfResultRows = pResultInfo->capacity - pResBlock->info.rows;
  taosFillResultDataBlock(pInfo->pFillInfo, pResBlock, numOfResultRows);

  pInfo->curGroupId = pInfo->existNewGroupBlock->info.groupId;
  pInfo->existNewGroupBlock = NULL;
}

static void doHandleRemainBlockFromNewGroup(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                            SResultInfo* pResultInfo, SExecTaskInfo* pTaskInfo) {
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    int32_t numOfResultRows = pResultInfo->capacity - pInfo->pFinalRes->info.rows;
    taosFillResultDataBlock(pInfo->pFillInfo, pInfo->pFinalRes, numOfResultRows);
    pInfo->pRes->info.groupId = pInfo->curGroupId;
    return;
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, pTaskInfo);
  }
}

static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExprSupp*         pSup = &pOperator->exprSupp;
  SSDataBlock*       pResBlock = pInfo->pFinalRes;

  setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, scanFlag, false);
  projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs, NULL);
  pInfo->pRes->info.groupId = pBlock->info.groupId;

  SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pInfo->primaryTsCol);
  SColumnInfoData* pSrc = taosArrayGet(pBlock->pDataBlock, pInfo->primarySrcSlotId);
  colDataAssign(pDst, pSrc, pInfo->pRes->info.rows, &pResBlock->info);

  for (int32_t i = 0; i < pInfo->numOfNotFillExpr; ++i) {
    SFillColInfo* pCol = &pInfo->pFillInfo->pFillCol[i + pInfo->numOfExpr];
    ASSERT(pCol->notFillCol);

    SExprInfo* pExpr = pCol->pExpr;
    int32_t    srcSlotId = pExpr->base.pParam[0].pCol->slotId;
    int32_t    dstSlotId = pExpr->base.resSchema.slotId;

    SColumnInfoData* pDst1 = taosArrayGet(pInfo->pRes->pDataBlock, dstSlotId);
    SColumnInfoData* pSrc1 = taosArrayGet(pBlock->pDataBlock, srcSlotId);
    colDataAssign(pDst1, pSrc1, pInfo->pRes->info.rows, &pResBlock->info);
  }
}

static SSDataBlock* doFillImpl(SOperatorInfo* pOperator) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  SSDataBlock* pResBlock = pInfo->pFinalRes;

  blockDataCleanup(pResBlock);

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;
  getTableScanInfo(pOperator, &order, &scanFlag);

  doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, pTaskInfo);
  if (pResBlock->info.rows > 0) {
    pResBlock->info.groupId = pInfo->curGroupId;
    return pResBlock;
  }

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = pDownstream->fpSet.getNextFn(pDownstream);
    if (pBlock == NULL) {
      if (pInfo->totalInputRows == 0) {
        doSetOperatorCompleted(pOperator);
        return NULL;
      }

      taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
    } else {
      blockDataUpdateTsWindow(pBlock, pInfo->primarySrcSlotId);

      blockDataCleanup(pInfo->pRes);
      doApplyScalarCalculation(pOperator, pBlock, order, scanFlag);

      if (pInfo->curGroupId == 0 || pInfo->curGroupId == pInfo->pRes->info.groupId) {
        pInfo->curGroupId = pInfo->pRes->info.groupId;  // the first data block
        pInfo->totalInputRows += pInfo->pRes->info.rows;

        if (order == pInfo->pFillInfo->order) {
          taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, pBlock->info.window.ekey);
        } else {
          taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, pBlock->info.window.skey);
        }
        taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);
      } else if (pInfo->curGroupId != pBlock->info.groupId) {  // the new group data block
        pInfo->existNewGroupBlock = pBlock;

        // Fill the previous group data block, before handle the data block of new group.
        // Close the fill operation for previous group data block
        taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
      }
    }

    int32_t numOfResultRows = pOperator->resultInfo.capacity - pResBlock->info.rows;
    taosFillResultDataBlock(pInfo->pFillInfo, pResBlock, numOfResultRows);

    // current group has no more result to return
    if (pResBlock->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pResBlock->info.rows > pResultInfo->threshold || pBlock == NULL || pInfo->existNewGroupBlock != NULL) {
        pResBlock->info.groupId = pInfo->curGroupId;
        return pResBlock;
      }

      doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, pTaskInfo);
      if (pResBlock->info.rows >= pOperator->resultInfo.threshold || pBlock == NULL) {
        pResBlock->info.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      assert(pBlock != NULL);

      blockDataCleanup(pResBlock);

      doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, pTaskInfo);
      if (pResBlock->info.rows > pResultInfo->threshold) {
        pResBlock->info.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else {
      return NULL;
    }
  }
}

static SSDataBlock* doFill(SOperatorInfo* pOperator) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSDataBlock* fillResult = NULL;
  while (true) {
    fillResult = doFillImpl(pOperator);
    if (fillResult == NULL) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    doFilter(pInfo->pCondition, fillResult, pInfo->pColMatchColInfo);
    if (fillResult->info.rows > 0) {
      break;
    }
  }

  if (fillResult != NULL) {
    pOperator->resultInfo.totalRows += fillResult->info.rows;
  }

  return fillResult;
}

void destroyExprInfo(SExprInfo* pExpr, int32_t numOfExprs) {
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExpr[i];
    for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
      if (pExprInfo->base.pParam[j].type == FUNC_PARAM_TYPE_COLUMN) {
        taosMemoryFreeClear(pExprInfo->base.pParam[j].pCol);
      }
    }

    taosMemoryFree(pExprInfo->base.pParam);
    taosMemoryFree(pExprInfo->pExpr);
  }
}

static void destroyOperatorInfo(SOperatorInfo* pOperator) {
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
  int32_t    code = 0;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  pAggSup->currentPageId = -1;
  pAggSup->resultRowSize = getResultRowSize(pCtx, numOfOutput);
  pAggSup->keyBuf = taosMemoryCalloc(1, keyBufSize + POINTER_BYTES + sizeof(int64_t));
  pAggSup->pResultRowHashTable = tSimpleHashInit(10, hashFn);

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

int32_t initAggInfo(SExprSupp* pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols, size_t keyBufSize,
                    const char* pkey) {
  int32_t code = initExprSupp(pSup, pExprInfo, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = doInitAggInfoSup(pAggSup, pSup->pCtx, numOfCols, keyBufSize, pkey);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    pSup->pCtx[i].saveHandle.pBuf = pAggSup->pResultBuf;
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
  taosMemoryFree(pSupp->rowEntryInfoOffset);
}

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, SNode* pCondition, SExprInfo* pScalarExprInfo,
                                           int32_t numOfScalarExpr, bool mergeResult, SExecTaskInfo* pTaskInfo) {
  SAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAggOperatorInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  int32_t code = initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pInfo->binfo, pResultBlock);
  code = initExprSupp(&pInfo->scalarExprSup, pScalarExprInfo, numOfScalarExpr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.mergeResultBlock = mergeResult;
  pInfo->groupId = UINT64_MAX;
  pInfo->pCondition = pCondition;
  pOperator->name = "TableAggregate";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_HASH_AGG;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(doOpenAggregateOptr, getAggregateResult, NULL, NULL, destroyAggOperatorInfo,
                                         aggEncodeResultRow, aggDecodeResultRow, NULL);

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = downstream->info;
    pTableScanInfo->pdInfo.pExprSup = &pOperator->exprSupp;
    pTableScanInfo->pdInfo.pAggSup = &pInfo->aggSup;
  }

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;
_error:
  destroyAggOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
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

void destroyFillOperatorInfo(void* param) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*)param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  pInfo->pFinalRes = blockDataDestroy(pInfo->pFinalRes);

  if (pInfo->pNotFillExprInfo != NULL) {
    destroyExprInfo(pInfo->pNotFillExprInfo, pInfo->numOfNotFillExpr);
    taosMemoryFree(pInfo->pNotFillExprInfo);
  }

  taosMemoryFreeClear(pInfo->p);
  taosArrayDestroy(pInfo->pColMatchColInfo);
  taosMemoryFreeClear(param);
}

void destroyExchangeOperatorInfo(void* param) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  taosRemoveRef(exchangeObjRefPool, pExInfo->self);
}

void freeSourceDataInfo(void* p) {
  SSourceDataInfo* pInfo = (SSourceDataInfo*)p;
  taosMemoryFreeClear(pInfo->pRsp);
}

void doDestroyExchangeOperatorInfo(void* param) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;

  taosArrayDestroy(pExInfo->pSources);
  taosArrayDestroyEx(pExInfo->pSourceDataInfo, freeSourceDataInfo);

  if (pExInfo->pResultBlockList != NULL) {
    taosArrayDestroyEx(pExInfo->pResultBlockList, freeBlock);
    pExInfo->pResultBlockList = NULL;
  }

  blockDataDestroy(pExInfo->pDummyBlock);

  tsem_destroy(&pExInfo->ready);
  taosMemoryFreeClear(param);
}

static int32_t initFillInfo(SFillOperatorInfo* pInfo, SExprInfo* pExpr, int32_t numOfCols, SExprInfo* pNotFillExpr,
                            int32_t numOfNotFillCols, SNodeListNode* pValNode, STimeWindow win, int32_t capacity,
                            const char* id, SInterval* pInterval, int32_t fillType, int32_t order) {
  SFillColInfo* pColInfo = createFillColInfo(pExpr, numOfCols, pNotFillExpr, numOfNotFillCols, pValNode);

  int64_t     startKey = (order == TSDB_ORDER_ASC) ? win.skey : win.ekey;
  STimeWindow w = getAlignQueryTimeWindow(pInterval, pInterval->precision, startKey);
  w = getFirstQualifiedTimeWindow(startKey, &w, pInterval, order);

  pInfo->pFillInfo = taosCreateFillInfo(w.skey, numOfCols, numOfNotFillCols, capacity, pInterval, fillType, pColInfo,
                                        pInfo->primaryTsCol, order, id);

  if (order == TSDB_ORDER_ASC) {
    pInfo->win.skey = win.skey;
    pInfo->win.ekey = win.ekey;
  } else {
    pInfo->win.skey = win.ekey;
    pInfo->win.ekey = win.skey;
  }
  pInfo->p = taosMemoryCalloc(numOfCols, POINTER_BYTES);

  if (pInfo->pFillInfo == NULL || pInfo->p == NULL) {
    taosMemoryFree(pInfo->pFillInfo);
    taosMemoryFree(pInfo->p);
    return TSDB_CODE_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

static bool isWstartColumnExist(SFillOperatorInfo* pInfo) {
  if (pInfo->numOfNotFillExpr == 0) {
    return false;
  }
  for (int32_t i = 0; i < pInfo->numOfNotFillExpr; ++i) {
    SExprInfo* exprInfo = pInfo->pNotFillExprInfo + i;
    if (exprInfo->pExpr->nodeType == QUERY_NODE_COLUMN && exprInfo->base.numOfParams == 1 &&
        exprInfo->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      return true;
    }
  }
  return false;
}

static int32_t createWStartTsAsNotFillExpr(SFillOperatorInfo* pInfo, SFillPhysiNode* pPhyFillNode) {
  bool wstartExist = isWstartColumnExist(pInfo);
  if (wstartExist == false) {
    if (pPhyFillNode->pWStartTs->type != QUERY_NODE_TARGET) {
      qError("pWStartTs of fill physical node is not a target node");
      return TSDB_CODE_QRY_SYS_ERROR;
    }

    SExprInfo* notFillExprs =
        taosMemoryRealloc(pInfo->pNotFillExprInfo, (pInfo->numOfNotFillExpr + 1) * sizeof(SExprInfo));
    if (notFillExprs == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    createExprFromTargetNode(notFillExprs + pInfo->numOfNotFillExpr, (STargetNode*)pPhyFillNode->pWStartTs);

    ++pInfo->numOfNotFillExpr;
    pInfo->pNotFillExprInfo = notFillExprs;
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SFillPhysiNode* pPhyFillNode,
                                      SExecTaskInfo* pTaskInfo) {
  SFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SFillOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SSDataBlock* pResBlock = createResDataBlock(pPhyFillNode->node.pOutputDataBlockDesc);
  SExprInfo*   pExprInfo = createExprInfo(pPhyFillNode->pFillExprs, NULL, &pInfo->numOfExpr);
  pInfo->pNotFillExprInfo = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &pInfo->numOfNotFillExpr);
  int32_t code = createWStartTsAsNotFillExpr(pInfo, pPhyFillNode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  SInterval* pInterval =
      QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL == downstream->operatorType
          ? &((SMergeAlignedIntervalAggOperatorInfo*)downstream->info)->intervalAggOperatorInfo->interval
          : &((SIntervalAggOperatorInfo*)downstream->info)->interval;

  int32_t order = (pPhyFillNode->inputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  int32_t type = convertFillType(pPhyFillNode->mode);

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);
  initExprSupp(&pOperator->exprSupp, pExprInfo, pInfo->numOfExpr);

  pInfo->primaryTsCol = ((STargetNode*)pPhyFillNode->pWStartTs)->slotId;
  pInfo->primarySrcSlotId = ((SColumnNode*)((STargetNode*)pPhyFillNode->pWStartTs)->pExpr)->slotId;

  int32_t numOfOutputCols = 0;
  SArray* pColMatchColInfo = extractColMatchInfo(pPhyFillNode->pFillExprs, pPhyFillNode->node.pOutputDataBlockDesc,
                                                 &numOfOutputCols, COL_MATCH_FROM_SLOT_ID);

  code = initFillInfo(pInfo, pExprInfo, pInfo->numOfExpr, pInfo->pNotFillExprInfo, pInfo->numOfNotFillExpr,
                      (SNodeListNode*)pPhyFillNode->pValues, pPhyFillNode->timeRange, pResultInfo->capacity,
                      pTaskInfo->id.str, pInterval, type, order);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pRes = pResBlock;
  pInfo->pFinalRes = createOneDataBlock(pResBlock, false);
  blockDataEnsureCapacity(pInfo->pFinalRes, pOperator->resultInfo.capacity);

  pInfo->pCondition = pPhyFillNode->node.pConditions;
  pInfo->pColMatchColInfo = pColMatchColInfo;
  pOperator->name = "FillOperator";
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_FILL;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = pInfo->numOfExpr;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doFill, NULL, NULL, destroyFillOperatorInfo, NULL, NULL, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  taosMemoryFreeClear(pOperator);
  taosMemoryFreeClear(pInfo);
  return NULL;
}

static SExecTaskInfo* createExecTaskInfo(uint64_t queryId, uint64_t taskId, EOPTR_EXEC_MODEL model, char* dbFName) {
  SExecTaskInfo* pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);

  pTaskInfo->schemaInfo.dbname = strdup(dbFName);
  pTaskInfo->cost.created = taosGetTimestampMs();
  pTaskInfo->id.queryId = queryId;
  pTaskInfo->execModel = model;

  char* p = taosMemoryCalloc(1, 128);
  snprintf(p, 128, "TID:0x%" PRIx64 " QID:0x%" PRIx64, taskId, queryId);
  pTaskInfo->id.str = p;

  return pTaskInfo;
}

static SArray* extractColumnInfo(SNodeList* pNodeList);

SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode);

int32_t extractTableSchemaInfo(SReadHandle* pHandle, SScanPhysiNode* pScanNode, SExecTaskInfo* pTaskInfo) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, pHandle->meta, 0);
  int32_t code = metaGetTableEntryByUid(&mr, pScanNode->uid);
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
    metaGetTableEntryByUid(&mr, suid);
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
    pSchema->type = pColNode->node.resType.bytes;
    strncpy(pSchema->name, pColNode->colName, tListLen(pSchema->name));
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
      pSchema->type = pColNode->node.resType.bytes;
      strncpy(pSchema->name, pColNode->colName, tListLen(pSchema->name));
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

static int32_t sortTableGroup(STableListInfo* pTableListInfo) {
  taosArrayClear(pTableListInfo->pGroupList);
  SArray* sortSupport = taosArrayInit(16, sizeof(uint64_t));
  if (sortSupport == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  for (int32_t i = 0; i < taosArrayGetSize(pTableListInfo->pTableList); i++) {
    STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);
    uint64_t*      groupId = taosHashGet(pTableListInfo->map, &info->uid, sizeof(uint64_t));

    int32_t index = taosArraySearchIdx(sortSupport, groupId, compareUint64Val, TD_EQ);
    if (index == -1) {
      void*   p = taosArraySearch(sortSupport, groupId, compareUint64Val, TD_GT);
      SArray* tGroup = taosArrayInit(8, sizeof(STableKeyInfo));
      if (tGroup == NULL) {
        taosArrayDestroy(sortSupport);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      if (taosArrayPush(tGroup, info) == NULL) {
        qError("taos push info array error");
        taosArrayDestroy(sortSupport);
        return TSDB_CODE_QRY_APP_ERROR;
      }
      if (p == NULL) {
        if (taosArrayPush(sortSupport, groupId) == NULL) {
          qError("taos push support array error");
          taosArrayDestroy(sortSupport);
          return TSDB_CODE_QRY_APP_ERROR;
        }
        if (taosArrayPush(pTableListInfo->pGroupList, &tGroup) == NULL) {
          qError("taos push group array error");
          taosArrayDestroy(sortSupport);
          return TSDB_CODE_QRY_APP_ERROR;
        }
      } else {
        int32_t pos = TARRAY_ELEM_IDX(sortSupport, p);
        if (taosArrayInsert(sortSupport, pos, groupId) == NULL) {
          qError("taos insert support array error");
          taosArrayDestroy(sortSupport);
          return TSDB_CODE_QRY_APP_ERROR;
        }
        if (taosArrayInsert(pTableListInfo->pGroupList, pos, &tGroup) == NULL) {
          qError("taos insert group array error");
          taosArrayDestroy(sortSupport);
          return TSDB_CODE_QRY_APP_ERROR;
        }
      }
    } else {
      SArray* tGroup = (SArray*)taosArrayGetP(pTableListInfo->pGroupList, index);
      if (taosArrayPush(tGroup, info) == NULL) {
        qError("taos push uid array error");
        taosArrayDestroy(sortSupport);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    }
  }
  taosArrayDestroy(sortSupport);
  return TDB_CODE_SUCCESS;
}

bool groupbyTbname(SNodeList* pGroupList) {
  bool bytbname = false;
  if (LIST_LENGTH(pGroupList) > 0) {
    SNode* p = nodesListGetNode(pGroupList, 0);
    if (p->type == QUERY_NODE_FUNCTION) {
      // partition by tbname/group by tbname
      bytbname = (strcmp(((struct SFunctionNode*)p)->functionName, "tbname") == 0);
    }
  }

  return bytbname;
}

int32_t generateGroupIdMap(STableListInfo* pTableListInfo, SReadHandle* pHandle, SNodeList* group) {
  if (group == NULL) {
    return TDB_CODE_SUCCESS;
  }

  pTableListInfo->map = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pTableListInfo->map == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  bool assignUid = groupbyTbname(group);

  size_t numOfTables = taosArrayGetSize(pTableListInfo->pTableList);

  if (assignUid) {
    for (int32_t i = 0; i < numOfTables; i++) {
      STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);
      info->groupId = info->uid;
      taosHashPut(pTableListInfo->map, &(info->uid), sizeof(uint64_t), &info->groupId, sizeof(uint64_t));
    }
  } else {
    int32_t code = getColInfoResultForGroupby(pHandle->meta, group, pTableListInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pTableListInfo->needSortTableByGroupId) {
    return sortTableGroup(pTableListInfo);
  }

  return TDB_CODE_SUCCESS;
}

static int32_t initTableblockDistQueryCond(uint64_t uid, SQueryTableDataCond* pCond) {
  memset(pCond, 0, sizeof(SQueryTableDataCond));

  pCond->order = TSDB_ORDER_ASC;
  pCond->numOfCols = 1;
  pCond->colList = taosMemoryCalloc(1, sizeof(SColumnInfo));
  if (pCond->colList == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return terrno;
  }

  pCond->colList->colId = 1;
  pCond->colList->type = TSDB_DATA_TYPE_TIMESTAMP;
  pCond->colList->bytes = sizeof(TSKEY);

  pCond->twindows = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pCond->suid = uid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createOperatorTree(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle,
                                  STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond,
                                  const char* pUser) {
  int32_t type = nodeType(pPhyNode);

  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    SOperatorInfo* pOperator = NULL;
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;

      int32_t code =
          createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort, pHandle,
                                  pTableListInfo, pTagCond, pTagIndexCond, GET_TASKID(pTaskInfo));
      if (code) {
        pTaskInfo->code = code;
        qError("failed to createScanTableListInfo, code:%s, %s", tstrerror(code), GET_TASKID(pTaskInfo));
        return NULL;
      }

      code = extractTableSchemaInfo(pHandle, &pTableScanNode->scan, pTaskInfo);
      if (code) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      pOperator = createTableScanOperatorInfo(pTableScanNode, pHandle, pTaskInfo);
      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == type) {
      STableMergeScanPhysiNode* pTableScanNode = (STableMergeScanPhysiNode*)pPhyNode;
      int32_t                   code =
          createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort, pHandle,
                                  pTableListInfo, pTagCond, pTagIndexCond, GET_TASKID(pTaskInfo));
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

      pOperator = createTableMergeScanOperatorInfo(pTableScanNode, pTableListInfo, pHandle, pTaskInfo);

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == type) {
      pOperator = createExchangeOperatorInfo(pHandle ? pHandle->pMsgCb->clientRpc : NULL, (SExchangePhysiNode*)pPhyNode,
                                             pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;
      if (pHandle->vnode) {
        int32_t code =
            createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort,
                                    pHandle, pTableListInfo, pTagCond, pTagIndexCond, GET_TASKID(pTaskInfo));
        if (code) {
          pTaskInfo->code = code;
          qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
          return NULL;
        }

#ifndef NDEBUG
        int32_t sz = taosArrayGetSize(pTableListInfo->pTableList);
        for (int32_t i = 0; i < sz; i++) {
          STableKeyInfo* pKeyInfo = taosArrayGet(pTableListInfo->pTableList, i);
          qDebug("creating stream task: add table %" PRId64, pKeyInfo->uid);
        }
#endif
      }

      pTaskInfo->schemaInfo.qsw = extractQueriedColumnSchema(&pTableScanNode->scan);
      pOperator = createStreamScanOperatorInfo(pHandle, pTableScanNode, pTagCond, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == type) {
      SSystemTableScanPhysiNode* pSysScanPhyNode = (SSystemTableScanPhysiNode*)pPhyNode;
      pOperator = createSysTableScanOperatorInfo(pHandle, pSysScanPhyNode, pUser, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN == type) {
      STagScanPhysiNode* pScanPhyNode = (STagScanPhysiNode*)pPhyNode;
      int32_t code = getTableList(pHandle->meta, pHandle->vnode, pScanPhyNode, pTagCond, pTagIndexCond, pTableListInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        qError("failed to getTableList, code: %s", tstrerror(code));
        return NULL;
      }

      pOperator = createTagScanOperatorInfo(pHandle, pScanPhyNode, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN == type) {
      SBlockDistScanPhysiNode* pBlockNode = (SBlockDistScanPhysiNode*)pPhyNode;
      pTableListInfo->pTableList = taosArrayInit(4, sizeof(STableKeyInfo));

      if (pBlockNode->tableType == TSDB_SUPER_TABLE) {
        int32_t code = vnodeGetAllTableList(pHandle->vnode, pBlockNode->uid, pTableListInfo->pTableList);
        if (code != TSDB_CODE_SUCCESS) {
          pTaskInfo->code = terrno;
          return NULL;
        }
      } else {  // Create one table group.
        STableKeyInfo info = {.uid = pBlockNode->uid, .groupId = 0};
        taosArrayPush(pTableListInfo->pTableList, &info);
      }

      SQueryTableDataCond cond = {0};
      int32_t             code = initTableblockDistQueryCond(pBlockNode->suid, &cond);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }

      STsdbReader* pReader = NULL;
      tsdbReaderOpen(pHandle->vnode, &cond, pTableListInfo->pTableList, &pReader, "");
      cleanupQueryTableDataCond(&cond);

      pOperator = createDataBlockInfoScanOperator(pReader, pHandle, cond.suid, pBlockNode, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN == type) {
      SLastRowScanPhysiNode* pScanNode = (SLastRowScanPhysiNode*)pPhyNode;

      int32_t code = createScanTableListInfo(&pScanNode->scan, pScanNode->pGroupTags, true, pHandle, pTableListInfo,
                                             pTagCond, pTagIndexCond, GET_TASKID(pTaskInfo));
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

  int32_t num = 0;
  size_t  size = LIST_LENGTH(pPhyNode->pChildren);

  SOperatorInfo** ops = taosMemoryCalloc(size, POINTER_BYTES);
  for (int32_t i = 0; i < size; ++i) {
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
    ops[i] = createOperatorTree(pChildNode, pTaskInfo, pHandle, pTableListInfo, pTagCond, pTagIndexCond, pUser);
    if (ops[i] == NULL) {
      taosMemoryFree(ops);
      return NULL;
    }

    ops[i]->resultDataBlockId = pChildNode->pOutputDataBlockDesc->dataBlockId;
  }

  SOperatorInfo* pOptr = NULL;
  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
    pOptr = createProjectOperatorInfo(ops[0], (SProjectPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_AGG == type) {
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
      pOptr = createAggregateOperatorInfo(ops[0], pExprInfo, num, pResBlock, pAggNode->node.pConditions,
                                          pScalarExprInfo, numOfScalarExpr, pAggNode->mergeDataBlock, pTaskInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL == type) {
    SIntervalPhysiNode* pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;

    SExprInfo*   pExprInfo = createExprInfo(pIntervalPhyNode->window.pFuncs, NULL, &num);
    SSDataBlock* pResBlock = createResDataBlock(pPhyNode->pOutputDataBlockDesc);

    SInterval interval = {.interval = pIntervalPhyNode->interval,
                          .sliding = pIntervalPhyNode->sliding,
                          .intervalUnit = pIntervalPhyNode->intervalUnit,
                          .slidingUnit = pIntervalPhyNode->slidingUnit,
                          .offset = pIntervalPhyNode->offset,
                          .precision = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->node.resType.precision};

    STimeWindowAggSupp as = {
        .waterMark = pIntervalPhyNode->window.watermark,
        .calTrigger = pIntervalPhyNode->window.triggerType,
        .maxTs = INT64_MIN,
    };
    ASSERT(as.calTrigger != STREAM_TRIGGER_MAX_DELAY);

    int32_t tsSlotId = ((SColumnNode*)pIntervalPhyNode->window.pTspk)->slotId;
    bool    isStream = (QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL == type);
    pOptr = createIntervalOperatorInfo(ops[0], pExprInfo, num, pResBlock, &interval, tsSlotId, &as, pIntervalPhyNode,
                                       pTaskInfo, isStream);

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

static int32_t extractTbscanInStreamOpTree(SOperatorInfo* pOperator, STableScanInfo** ppInfo) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    if (pOperator->numOfDownstream == 0) {
      qError("failed to find stream scan operator");
      return TSDB_CODE_QRY_APP_ERROR;
    }

    if (pOperator->numOfDownstream > 1) {
      qError("join not supported for stream block scan");
      return TSDB_CODE_QRY_APP_ERROR;
    }
    return extractTbscanInStreamOpTree(pOperator->pDownstream[0], ppInfo);
  } else {
    SStreamScanInfo* pInfo = pOperator->info;
    ASSERT(pInfo->pTableScanOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
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
      ASSERT(0);
      terrno = TSDB_CODE_QRY_APP_ERROR;
      return -1;
    }
  } else {
    if (LIST_LENGTH(pNode->pChildren) != 1) {
      ASSERT(0);
      terrno = TSDB_CODE_QRY_APP_ERROR;
      return -1;
    }
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pNode->pChildren, 0);
    return extractTableScanNode(pChildNode, ppNode);
  }
  return -1;
}

#if 0
int32_t rebuildReader(SOperatorInfo* pOperator, SSubplan* plan, SReadHandle* pHandle, int64_t uid, int64_t ts) {
  STableScanInfo* pTableScanInfo = NULL;
  if (extractTbscanInStreamOpTree(pOperator, &pTableScanInfo) < 0) {
    return -1;
  }

  STableScanPhysiNode* pNode = NULL;
  if (extractTableScanNode(plan->pNode, &pNode) < 0) {
    ASSERT(0);
  }

  tsdbReaderClose(pTableScanInfo->dataReader);

  STableListInfo info = {0};
  pTableScanInfo->dataReader = doCreateDataReader(pNode, pHandle, &info, NULL);
  if (pTableScanInfo->dataReader == NULL) {
    ASSERT(0);
    qError("failed to create data reader");
    return TSDB_CODE_QRY_APP_ERROR;
  }
  // TODO: set uid and ts to data reader
  return 0;
}
#endif

int32_t encodeOperator(SOperatorInfo* ops, char** result, int32_t* length, int32_t* nOptrWithVal) {
  int32_t code = TDB_CODE_SUCCESS;
  char*   pCurrent = NULL;
  int32_t currLength = 0;
  if (ops->fpSet.encodeResultRow) {
    if (result == NULL || length == NULL || nOptrWithVal == NULL) {
      return TSDB_CODE_TSC_INVALID_INPUT;
    }
    code = ops->fpSet.encodeResultRow(ops, &pCurrent, &currLength);

    if (code != TDB_CODE_SUCCESS) {
      if (*result != NULL) {
        taosMemoryFree(*result);
        *result = NULL;
      }
      return code;
    } else if (currLength == 0) {
      ASSERT(!pCurrent);
      goto _downstream;
    }

    ++(*nOptrWithVal);

    ASSERT(currLength >= 0);

    if (*result == NULL) {
      *result = (char*)taosMemoryCalloc(1, currLength + sizeof(int32_t));
      if (*result == NULL) {
        taosMemoryFree(pCurrent);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      memcpy(*result + sizeof(int32_t), pCurrent, currLength);
      *(int32_t*)(*result) = currLength + sizeof(int32_t);
    } else {
      int32_t sizePre = *(int32_t*)(*result);
      char*   tmp = (char*)taosMemoryRealloc(*result, sizePre + currLength);
      if (tmp == NULL) {
        taosMemoryFree(pCurrent);
        taosMemoryFree(*result);
        *result = NULL;
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      *result = tmp;
      memcpy(*result + sizePre, pCurrent, currLength);
      *(int32_t*)(*result) += currLength;
    }
    taosMemoryFree(pCurrent);
    *length = *(int32_t*)(*result);
  }

_downstream:
  for (int32_t i = 0; i < ops->numOfDownstream; ++i) {
    code = encodeOperator(ops->pDownstream[i], result, length, nOptrWithVal);
    if (code != TDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TDB_CODE_SUCCESS;
}

int32_t decodeOperator(SOperatorInfo* ops, const char* result, int32_t length) {
  int32_t code = TDB_CODE_SUCCESS;
  if (ops->fpSet.decodeResultRow) {
    if (result == NULL) {
      return TSDB_CODE_TSC_INVALID_INPUT;
    }

    ASSERT(length == *(int32_t*)result);

    const char* data = result + sizeof(int32_t);
    code = ops->fpSet.decodeResultRow(ops, (char*)data);
    if (code != TDB_CODE_SUCCESS) {
      return code;
    }

    int32_t totalLength = *(int32_t*)result;
    int32_t dataLength = *(int32_t*)data;

    if (totalLength == dataLength + sizeof(int32_t)) {  // the last data
      result = NULL;
      length = 0;
    } else {
      result += dataLength;
      *(int32_t*)(result) = totalLength - dataLength;
      length = totalLength - dataLength;
    }
  }

  for (int32_t i = 0; i < ops->numOfDownstream; ++i) {
    code = decodeOperator(ops->pDownstream[i], result, length);
    if (code != TDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TDB_CODE_SUCCESS;
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
      int32_t tbNum = taosArrayGetSize(pTask->tableqinfoList.pTableList);
      pDeleterParam->suid = pTask->tableqinfoList.suid;
      pDeleterParam->pUidList = taosArrayInit(tbNum, sizeof(uint64_t));
      if (NULL == pDeleterParam->pUidList) {
        taosMemoryFree(pDeleterParam);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < tbNum; ++i) {
        STableKeyInfo* pTable = taosArrayGet(pTask->tableqinfoList.pTableList, i);
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

  int32_t code = TSDB_CODE_SUCCESS;
  *pTaskInfo = createExecTaskInfo(queryId, taskId, model, pPlan->dbFName);
  if (*pTaskInfo == NULL) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _complete;
  }

  if (pHandle && pHandle->pStateBackend) {
    (*pTaskInfo)->streamInfo.pState = pHandle->pStateBackend;
  }

  (*pTaskInfo)->sql = sql;
  sql = NULL;
  (*pTaskInfo)->pSubplan = pPlan;
  (*pTaskInfo)->pRoot = createOperatorTree(pPlan->pNode, *pTaskInfo, pHandle, &(*pTaskInfo)->tableqinfoList,
                                           pPlan->pTagCond, pPlan->pTagIndexCond, pPlan->user);

  if (NULL == (*pTaskInfo)->pRoot) {
    code = (*pTaskInfo)->code;
    goto _complete;
  }

  return code;

_complete:
  taosMemoryFree(sql);
  doDestroyTask(*pTaskInfo);
  terrno = code;
  return code;
}

void doDestroyTableList(STableListInfo* pTableqinfoList) {
  taosArrayDestroy(pTableqinfoList->pTableList);
  taosHashCleanup(pTableqinfoList->map);
  if (pTableqinfoList->needSortTableByGroupId) {
    for (int32_t i = 0; i < taosArrayGetSize(pTableqinfoList->pGroupList); i++) {
      SArray* tmp = taosArrayGetP(pTableqinfoList->pGroupList, i);
      if (tmp == pTableqinfoList->pTableList) {
        continue;
      }
      taosArrayDestroy(tmp);
    }
  }
  taosArrayDestroy(pTableqinfoList->pGroupList);

  pTableqinfoList->pTableList = NULL;
  pTableqinfoList->map = NULL;
}

void doDestroyTask(SExecTaskInfo* pTaskInfo) {
  qDebug("%s execTask is freed", GET_TASKID(pTaskInfo));

  doDestroyTableList(&pTaskInfo->tableqinfoList);
  destroyOperatorInfo(pTaskInfo->pRoot);
  cleanupTableSchemaInfo(&pTaskInfo->schemaInfo);
  cleanupStreamInfo(&pTaskInfo->streamInfo);

  if (!pTaskInfo->localFetch.localExec) {
    nodesDestroyNode((SNode*)pTaskInfo->pSubplan);
  }

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
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, const char* pKey, SqlFunctionCtx* pCtx, int32_t numOfOutput,
                               int32_t size) {
  pSup->currentPageId = -1;
  pSup->resultRowSize = getResultRowSize(pCtx, numOfOutput);
  pSup->keySize = sizeof(int64_t) + sizeof(TSKEY);
  pSup->pKeyBuf = taosMemoryCalloc(1, pSup->keySize);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pSup->pResultRows = taosHashInit(1024, hashFn, false, HASH_NO_LOCK);
  if (pSup->pKeyBuf == NULL || pSup->pResultRows == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSup->valueSize = size;

  pSup->pScanBlock = createSpecialDataBlock(STREAM_CLEAR);
  int32_t pageSize = 4096;
  while (pageSize < pSup->resultRowSize * 4) {
    pageSize <<= 1u;
  }
  // at least four pages need to be in buffer
  int32_t bufSize = 4096 * 256;
  if (bufSize <= pageSize) {
    bufSize = pageSize * 4;
  }
  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_AVAIL_DISK;
    qError("Init stream agg supporter failed since %s", terrstr(terrno));
    return terrno;
  }
  int32_t code = createDiskbasedBuf(&pSup->pResultBuf, pageSize, bufSize, pKey, tsTempDir);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    pCtx[i].saveHandle.pBuf = pSup->pResultBuf;
  }

  return code;
}

int32_t setOutputBuf(STimeWindow* win, SResultRow** pResult, int64_t tableGroupId, SqlFunctionCtx* pCtx,
                     int32_t numOfOutput, int32_t* rowEntryInfoOffset, SAggSupporter* pAggSup,
                     SExecTaskInfo* pTaskInfo) {
  SWinKey key = {
      .ts = win->skey,
      .groupId = tableGroupId,
  };
  char*   value = NULL;
  int32_t size = pAggSup->resultRowSize;

  tSimpleHashPut(pAggSup->pResultRowHashTable, &key, sizeof(SWinKey), NULL, 0);
  if (streamStateAddIfNotExist(pTaskInfo->streamInfo.pState, &key, (void**)&value, &size) < 0) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  *pResult = (SResultRow*)value;
  ASSERT(*pResult);
  // set time window for current result
  (*pResult)->win = (*win);
  setResultRowInitCtx(*pResult, pCtx, numOfOutput, rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

int32_t releaseOutputBuf(SExecTaskInfo* pTaskInfo, SWinKey* pKey, SResultRow* pResult) {
  streamStateReleaseBuf(pTaskInfo->streamInfo.pState, pKey, pResult);
  /*taosMemoryFree((*(void**)pResult));*/
  return TSDB_CODE_SUCCESS;
}

int32_t saveOutputBuf(SExecTaskInfo* pTaskInfo, SWinKey* pKey, SResultRow* pResult, int32_t resSize) {
  streamStatePut(pTaskInfo->streamInfo.pState, pKey, pResult, resSize);
  return TSDB_CODE_SUCCESS;
}

int32_t buildDataBlockFromGroupRes(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup,
                                   SGroupResInfo* pGroupResInfo) {
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
    int32_t code = streamStateGet(pTaskInfo->streamInfo.pState, &key, &pVal, &size);
    ASSERT(code == 0);
    SResultRow* pRow = (SResultRow*)pVal;
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      releaseOutputBuf(pTaskInfo, &key, pRow);
      continue;
    }

    if (pBlock->info.groupId == 0) {
      pBlock->info.groupId = pPos->groupId;
    } else {
      // current value belongs to different group, it can't be packed into one datablock
      if (pBlock->info.groupId != pPos->groupId) {
        releaseOutputBuf(pTaskInfo, &key, pRow);
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      releaseOutputBuf(pTaskInfo, &key, pRow);
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
    releaseOutputBuf(pTaskInfo, &key, pRow);
  }
  blockDataUpdateTsWindow(pBlock, 0);
  return TSDB_CODE_SUCCESS;
}
