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

#define SET_REVERSE_SCAN_FLAG(runtime)    ((runtime)->scanFlag = REVERSE_SCAN)
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

#define CLEAR_QUERY_STATUS(q, st) ((q)->status &= (~(st)))

static void setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExpr, SSDataBlock* pBlock);

static void releaseQueryBuf(size_t numOfTables);

static void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);

static void    extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, bool keep,
                                                   int32_t status);
static int32_t doSetInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                                   bool createDummyCol);
static int32_t doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                  SGroupResInfo* pGroupResInfo);
static SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode);

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

  memset((char*)pResultRow, 0, interBufSize);
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
                                   bool isIntervalQuery, SAggSupporter* pSup, bool keepGroup) {
  SET_RES_WINDOW_KEY(pSup->keyBuf, pData, bytes, groupId);
  if (!keepGroup) {
    *(uint64_t*)pSup->keyBuf = calcGroupId(pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
  }

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

//  query_range_start, query_range_end, window_duration, window_start, window_end
void initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow) {
  pColData->info.type = TSDB_DATA_TYPE_TIMESTAMP;
  pColData->info.bytes = sizeof(int64_t);

  colInfoDataEnsureCapacity(pColData, 5, false);
  colDataSetInt64(pColData, 0, &pQueryWindow->skey);
  colDataSetInt64(pColData, 1, &pQueryWindow->ekey);

  int64_t interval = 0;
  colDataSetInt64(pColData, 2, &interval);  // this value may be variable in case of 'n' and 'y'.
  colDataSetInt64(pColData, 3, &pQueryWindow->skey);
  colDataSetInt64(pColData, 4, &pQueryWindow->ekey);
}

static void doSetInputDataBlockInfo(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag) {
  SqlFunctionCtx* pCtx = pExprSup->pCtx;
  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;
    setBlockSMAInfo(&pCtx[i], &pExprSup->pExprInfo[i], pBlock);
    pCtx[i].pSrcBlock = pBlock;
    pCtx[i].scanFlag = scanFlag;
  }
}

void setInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag, bool createDummyCol) {
  if (pBlock->pBlockAgg != NULL) {
    doSetInputDataBlockInfo(pExprSup, pBlock, order, scanFlag);
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
      colDataSetInt64(pColInfo, i, &v);
    }
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    double v = pFuncParam->param.d;
    for (int32_t i = 0; i < numOfRows; ++i) {
      colDataSetDouble(pColInfo, i, &v);
    }
  } else if (type == TSDB_DATA_TYPE_VARCHAR) {
    char* tmp = taosMemoryMalloc(pFuncParam->param.nLen + VARSTR_HEADER_SIZE);
    STR_WITH_SIZE_TO_VARSTR(tmp, pFuncParam->param.pz, pFuncParam->param.nLen);
    for (int32_t i = 0; i < numOfRows; ++i) {
      colDataSetVal(pColInfo, i, tmp, false);
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

bool functionNeedToExecute(SqlFunctionCtx* pCtx) {
  struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  // in case of timestamp column, always generated results.
  int32_t functionId = pCtx->functionId;
  if (functionId == -1) {
    return false;
  }

  if (pCtx->scanFlag == PRE_SCAN) {
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

bool isTaskKilled(SExecTaskInfo* pTaskInfo) { return (0 != pTaskInfo->code); }

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

    if (pCtx[i].isPseudoFunc) {
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

void clearResultRowInitFlag(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SResultRowEntryInfo* pResInfo = pCtx[i].resultInfo;
    if (pResInfo == NULL) {
      continue;
    }

    pResInfo->initialized = false;
    pResInfo->numOfRes = 0;
    pResInfo->isNullRes = 0;
    pResInfo->complete = false;
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

        while (j < totalRows) {
          if (pIndicator[j] == 0) {
            j += 1;
            continue;
          }

          if (colDataIsNull_var(pDst, j)) {
            colDataSetNull_var(pDst, numOfRows);
          } else {
            char* p1 = colDataGetVarData(pDst, j);
            colDataSetVal(pDst, numOfRows, p1, false);
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

void doUpdateNumOfRows(SqlFunctionCtx* pCtx, SResultRow* pRow, int32_t numOfExprs, const int32_t* rowEntryOffset) {
  bool returnNotNull = false;
  for (int32_t j = 0; j < numOfExprs; ++j) {
    SResultRowEntryInfo* pResInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
    if (!isRowEntryInitialized(pResInfo)) {
      continue;
    }

    if (pRow->numOfRows < pResInfo->numOfRes) {
      pRow->numOfRows = pResInfo->numOfRes;
    }

    if (pCtx[j].isNotNullFunc) {
      returnNotNull = true;
    }
  }
  // if all expr skips all blocks, e.g. all null inputs for max function, output one row in final result.
  //  except for first/last, which require not null output, output no rows
  if (pRow->numOfRows == 0 && !returnNotNull) {
    pRow->numOfRows = 1;
  }
}

void copyResultrowToDataBlock(SExprInfo* pExprInfo, int32_t numOfExprs, SResultRow* pRow, SqlFunctionCtx* pCtx,
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
        colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
      }
    }
  }
}

// todo refactor. SResultRow has direct pointer in miainfo
int32_t finalizeResultRows(SDiskbasedBuf* pBuf, SResultRowPosition* resultRowPosition, SExprSupp* pSup,
                           SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  SFilePage* page = getBufPage(pBuf, resultRowPosition->pageId);
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

  copyResultrowToDataBlock(pExprInfo, pSup->numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo);

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
      blockDataEnsureCapacity(pBlock, pBlock->info.rows + pRow->numOfRows);
      qDebug("datablock capacity not sufficient, expand to required:%" PRId64 ", current capacity:%d, %s",
             (pRow->numOfRows + pBlock->info.rows), pBlock->info.capacity, GET_TASKID(pTaskInfo));
      // todo set the pOperator->resultInfo size
    }

    pGroupResInfo->index += 1;
    copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo);

    releaseBufPage(pBuf, page);
    pBlock->info.rows += pRow->numOfRows;
  }

  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
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

int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num) {
  p->pDownstream = taosMemoryCalloc(1, num * POINTER_BYTES);
  if (p->pDownstream == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(p->pDownstream, pDownstream, num * POINTER_BYTES);
  p->numOfDownstream = num;
  return TSDB_CODE_SUCCESS;
}

int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag, bool inheritUsOrder) {
  // todo add more information about exchange operation
  int32_t type = pOperator->operatorType;
  if (type == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN) {
    *order = TSDB_ORDER_ASC;
    *scanFlag = MAIN_SCAN;
    return TSDB_CODE_SUCCESS;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    if (!inheritUsOrder) {
      *order = TSDB_ORDER_ASC;
    }
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
      return getTableScanInfo(pOperator->pDownstream[0], order, scanFlag, inheritUsOrder);
    }
  }
}

// QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN
SOperatorInfo* extractOperatorInTree(SOperatorInfo* pOperator, int32_t type, const char* id) {
  if (pOperator == NULL) {
    qError("invalid operator, failed to find tableScanOperator %s", id);
    return NULL;
  }

  if (pOperator->operatorType == type) {
    return pOperator;
  } else {
    if (pOperator->pDownstream == NULL || pOperator->pDownstream[0] == NULL) {
      qError("invalid operator, failed to find tableScanOperator %s", id);
      return NULL;
    }

    return extractOperatorInTree(pOperator->pDownstream[0], type, id);
  }
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

void initResultSizeInfo(SResultInfo* pResultInfo, int32_t numOfRows) {
  if (numOfRows == 0) {
    numOfRows = 4096;
  }

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

static void* destroySqlFunctionCtx(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
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

    if (pCtx[i].udfName != NULL) {
      taosMemoryFree(pCtx[i].udfName);
    }
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

void cleanupBasicInfo(SOptrBasicInfo* pInfo) { pInfo->pRes = blockDataDestroy(pInfo->pRes); }

char* buildTaskId(uint64_t taskId, uint64_t queryId) {
  char* p = taosMemoryMalloc(64);

  int32_t offset = 6;
  memcpy(p, "TID:0x", offset);
  offset += tintToHex(taskId, &p[offset]);

  memcpy(&p[offset], " QID:0x", 7);
  offset += 7;
  offset += tintToHex(queryId, &p[offset]);

  p[offset] = 0;
  return p;
}

SExecTaskInfo* doCreateExecTaskInfo(uint64_t queryId, uint64_t taskId, int32_t vgId, EOPTR_EXEC_MODEL model,
                                    char* dbFName) {
  SExecTaskInfo* pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  if (pTaskInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
  pTaskInfo->cost.created = taosGetTimestampUs();

  pTaskInfo->schemaInfo.dbname = taosStrdup(dbFName);
  pTaskInfo->execModel = model;
  pTaskInfo->stopInfo.pStopInfo = taosArrayInit(4, sizeof(SExchangeOpStopInfo));
  pTaskInfo->pResultBlockList = taosArrayInit(128, POINTER_BYTES);

  taosInitRWLatch(&pTaskInfo->lock);
  pTaskInfo->id.vgId = vgId;
  pTaskInfo->id.queryId = queryId;
  pTaskInfo->id.str = buildTaskId(taskId, queryId);
  return pTaskInfo;
}

int32_t extractTableSchemaInfo(SReadHandle* pHandle, SScanPhysiNode* pScanNode, SExecTaskInfo* pTaskInfo) {
  SMetaReader mr = {0};
  if (pHandle == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  metaReaderInit(&mr, pHandle->meta, 0);
  int32_t code = metaGetTableEntryByUidCache(&mr, pScanNode->uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get the table meta, uid:0x%" PRIx64 ", suid:0x%" PRIx64 ", %s", pScanNode->uid, pScanNode->suid,
           GET_TASKID(pTaskInfo));

    metaReaderClear(&mr);
    return terrno;
  }

  SSchemaInfo* pSchemaInfo = &pTaskInfo->schemaInfo;
  pSchemaInfo->tablename = taosStrdup(mr.me.name);

  if (mr.me.type == TSDB_SUPER_TABLE) {
    pSchemaInfo->sw = tCloneSSchemaWrapper(&mr.me.stbEntry.schemaRow);
    pSchemaInfo->tversion = mr.me.stbEntry.schemaTag.version;
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);

    tb_uid_t suid = mr.me.ctbEntry.suid;
    code = metaGetTableEntryByUidCache(&mr, suid);
    if (code != TSDB_CODE_SUCCESS) {
      metaReaderClear(&mr);
      return terrno;
    }

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
  int32_t     type = nodeType(pPhyNode);
  const char* idstr = GET_TASKID(pTaskInfo);

  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    SOperatorInfo* pOperator = NULL;
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;

      // NOTE: this is an patch to fix the physical plan
      // TODO remove it later
      if (pTableScanNode->scan.node.pLimit != NULL) {
        pTableScanNode->groupSort = true;
      }

      STableListInfo* pTableListInfo = tableListCreate();
      int32_t         code =
          createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort, pHandle,
                                  pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        tableListDestroy(pTableListInfo);
        qError("failed to createScanTableListInfo, code:%s, %s", tstrerror(code), idstr);
        return NULL;
      }

      code = extractTableSchemaInfo(pHandle, &pTableScanNode->scan, pTaskInfo);
      if (code) {
        pTaskInfo->code = terrno;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      pOperator = createTableScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo);
      if (NULL == pOperator) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->base.readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == type) {
      STableMergeScanPhysiNode* pTableScanNode = (STableMergeScanPhysiNode*)pPhyNode;
      STableListInfo*           pTableListInfo = tableListCreate();

      int32_t code = createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, true, pHandle,
                                             pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        tableListDestroy(pTableListInfo);
        qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
        return NULL;
      }

      code = extractTableSchemaInfo(pHandle, &pTableScanNode->scan, pTaskInfo);
      if (code) {
        pTaskInfo->code = terrno;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      pOperator = createTableMergeScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo);
      if (NULL == pOperator) {
        pTaskInfo->code = terrno;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->base.readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == type) {
      pOperator = createExchangeOperatorInfo(pHandle ? pHandle->pMsgCb->clientRpc : NULL, (SExchangePhysiNode*)pPhyNode,
                                             pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;
      STableListInfo*      pTableListInfo = tableListCreate();

      if (pHandle->vnode) {
        int32_t code =
            createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort,
                                    pHandle, pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
        if (code) {
          pTaskInfo->code = code;
          tableListDestroy(pTableListInfo);
          qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
          return NULL;
        }
      }

      pTaskInfo->schemaInfo.qsw = extractQueriedColumnSchema(&pTableScanNode->scan);
      pOperator = createStreamScanOperatorInfo(pHandle, pTableScanNode, pTagCond, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == type) {
      SSystemTableScanPhysiNode* pSysScanPhyNode = (SSystemTableScanPhysiNode*)pPhyNode;
      pOperator = createSysTableScanOperatorInfo(pHandle, pSysScanPhyNode, pUser, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN == type) {
      STableCountScanPhysiNode* pTblCountScanNode = (STableCountScanPhysiNode*)pPhyNode;
      pOperator = createTableCountScanOperatorInfo(pHandle, pTblCountScanNode, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN == type) {
      STagScanPhysiNode* pScanPhyNode = (STagScanPhysiNode*)pPhyNode;
      STableListInfo*    pTableListInfo = tableListCreate();
      int32_t            code = createScanTableListInfo(pScanPhyNode, NULL, false, pHandle, pTableListInfo, pTagCond,
                                                        pTagIndexCond, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        qError("failed to getTableList, code: %s", tstrerror(code));
        return NULL;
      }

      pOperator = createTagScanOperatorInfo(pHandle, pScanPhyNode, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN == type) {
      SBlockDistScanPhysiNode* pBlockNode = (SBlockDistScanPhysiNode*)pPhyNode;
      STableListInfo*          pTableListInfo = tableListCreate();

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

      pOperator = createDataBlockInfoScanOperator(pHandle, pBlockNode, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN == type) {
      SLastRowScanPhysiNode* pScanNode = (SLastRowScanPhysiNode*)pPhyNode;
      STableListInfo*        pTableListInfo = tableListCreate();

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

      pOperator = createCacherowsScanOperator(pScanNode, pHandle, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
      pOperator = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pPhyNode, pTaskInfo);
    } else {
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
    }

    if (pOperator != NULL) {  // todo moved away
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
    pOptr = createIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo);
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
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT == type) {
    pOptr = createEventwindowOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else {
    terrno = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(ops);
    return NULL;
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

int32_t createDataSinkParam(SDataSinkNode* pNode, void** pParam, STableListInfo* pTableListInfo,
                            SReadHandle* readHandle) {
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

      int32_t tbNum = tableListGetSize(pTableListInfo);
      pDeleterParam->suid = tableListGetSuid(pTableListInfo);

      // TODO extract uid list
      pDeleterParam->pUidList = taosArrayInit(tbNum, sizeof(uint64_t));
      if (NULL == pDeleterParam->pUidList) {
        taosMemoryFree(pDeleterParam);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      for (int32_t i = 0; i < tbNum; ++i) {
        STableKeyInfo* pTable = tableListGetInfo(pTableListInfo, i);
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

int32_t createExecTaskInfo(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                           int32_t vgId, char* sql, EOPTR_EXEC_MODEL model) {
  *pTaskInfo = doCreateExecTaskInfo(pPlan->id.queryId, taskId, vgId, model, pPlan->dbFName);
  if (*pTaskInfo == NULL) {
    goto _complete;
  }

  if (pHandle) {
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
    SWinKey* pKey = taosArrayGet(pGroupResInfo->pRows, i);
    int32_t  size = 0;
    void*    pVal = NULL;
    int32_t  code = streamStateGet(pState, pKey, &pVal, &size);
    ASSERT(code == 0);
    SResultRow* pRow = (SResultRow*)pVal;
    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);
    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      releaseOutputBuf(pState, pKey, pRow);
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
        releaseOutputBuf(pState, pKey, pRow);
        break;
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      ASSERT(pBlock->info.rows > 0);
      releaseOutputBuf(pState, pKey, pRow);
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
          colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        }
      }
    }

    pBlock->info.rows += pRow->numOfRows;
    releaseOutputBuf(pState, pKey, pRow);
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
          colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
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

void qStreamCloseTsdbReader(void* task) {
  if (task == NULL) {
    return;
  }

  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)task;
  SOperatorInfo* pOp = pTaskInfo->pRoot;

  qDebug("stream close tsdb reader, reset status uid:%" PRId64 " ts:%" PRId64, pTaskInfo->streamInfo.lastStatus.uid,
         pTaskInfo->streamInfo.lastStatus.ts);

  // todo refactor, other thread may already use this read to extract data.
  pTaskInfo->streamInfo.lastStatus = (STqOffsetVal){0};
  while (pOp->numOfDownstream == 1 && pOp->pDownstream[0]) {
    SOperatorInfo* pDownstreamOp = pOp->pDownstream[0];
    if (pDownstreamOp->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
      SStreamScanInfo* pInfo = pDownstreamOp->info;
      if (pInfo->pTableScanOp) {
        STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;

        setOperatorCompleted(pInfo->pTableScanOp);
        while (pTaskInfo->owner != 0) {
          taosMsleep(100);
          qDebug("wait for the reader stopping");
        }

        tsdbReaderClose(pTSInfo->base.dataReader);
        pTSInfo->base.dataReader = NULL;

        // restore the status, todo refactor.
        pInfo->pTableScanOp->status = OP_OPENED;
        pTaskInfo->status = TASK_NOT_COMPLETED;
        return;
      }
    }
  }
}

static void extractTableList(SArray* pList, const SOperatorInfo* pOperator) {
  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = pOperator->info;
    taosArrayPush(pList, &pScanInfo->base.pTableListInfo);
  } else {
    if (pOperator->pDownstream != NULL && pOperator->pDownstream[0] != NULL) {
      extractTableList(pList, pOperator->pDownstream[0]);
    }
  }
}

SArray* getTableListInfo(const SExecTaskInfo* pTaskInfo) {
  SArray*        pArray = taosArrayInit(0, POINTER_BYTES);
  SOperatorInfo* pOperator = pTaskInfo->pRoot;
  extractTableList(pArray, pOperator);
  return pArray;
}