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
#include "tmsg.h"
#include "ttime.h"

#include "executorInt.h"
#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

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

static int32_t setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExpr, SSDataBlock* pBlock);

static int32_t initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void    doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);

static int32_t doSetInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                                   bool createDummyCol);
static void    doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                  SGroupResInfo* pGroupResInfo, int32_t threshold, bool ignoreGroup);

SResultRow* getNewResultRow(SDiskbasedBuf* pResultBuf, int32_t* currentPageId, int32_t interBufSize) {
  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  if (*currentPageId == -1) {
    pData = getNewBufPage(pResultBuf, &pageId);
    if (pData == NULL) {
      qError("failed to get buffer, code:%s", tstrerror(terrno));
      return NULL;
    }
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
      if (pData == NULL) {
        qError("failed to get buffer, code:%s", tstrerror(terrno));
        return NULL;
      }
      pData->num = sizeof(SFilePage);
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
                                   int32_t bytes, bool masterscan, uint64_t groupId, SExecTaskInfo* pTaskInfo,
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
      if (pResult == NULL) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      if (pResult->pageId != p1->pageId || pResult->offset != p1->offset) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        pTaskInfo->code = terrno;
        return NULL;
      }
    }
  } else {
    // In case of group by column query, the required SResultRow object must be existInCurrentResusltRowInfo in the
    // pResultRowInfo object.
    if (p1 != NULL) {
      // todo
      pResult = getResultRowByPos(pResultBuf, p1, true);
      if (NULL == pResult) {
        pTaskInfo->code = terrno;
        return NULL;
      }

      if (pResult->pageId != p1->pageId || pResult->offset != p1->offset) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        pTaskInfo->code = terrno;
        return NULL;
      }
    }
  }

  // 1. close current opened time window
  if (pResultRowInfo->cur.pageId != -1 && ((pResult == NULL) || (pResult->pageId != pResultRowInfo->cur.pageId))) {
    SResultRowPosition pos = pResultRowInfo->cur;
    SFilePage*         pPage = getBufPage(pResultBuf, pos.pageId);
    if (pPage == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      pTaskInfo->code = terrno;
      return NULL;
    }
    releaseBufPage(pResultBuf, pPage);
  }

  // allocate a new buffer page
  if (pResult == NULL) {
    pResult = getNewResultRow(pResultBuf, &pSup->currentPageId, pSup->resultRowSize);
    if (pResult == NULL) {
      pTaskInfo->code = terrno;
      return NULL;
    }

    // add a new result set for a new group
    SResultRowPosition pos = {.pageId = pResult->pageId, .offset = pResult->offset};
    int32_t code = tSimpleHashPut(pSup->pResultRowHashTable, pSup->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), &pos,
                                  sizeof(SResultRowPosition));
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      return NULL;
    }
  }

  // 2. set the new time window to be the new active time window
  pResultRowInfo->cur = (SResultRowPosition){.pageId = pResult->pageId, .offset = pResult->offset};

  // too many time window in query
  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_BATCH &&
      tSimpleHashGetSize(pSup->pResultRowHashTable) > MAX_INTERVAL_TIME_WINDOW) {
    pTaskInfo->code = TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW;
    return NULL;
  }

  return pResult;
}

//  query_range_start, query_range_end, window_duration, window_start, window_end
int32_t initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow) {
  pColData->info.type = TSDB_DATA_TYPE_TIMESTAMP;
  pColData->info.bytes = sizeof(int64_t);

  int32_t code = colInfoDataEnsureCapacity(pColData, 5, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  colDataSetInt64(pColData, 0, &pQueryWindow->skey);
  colDataSetInt64(pColData, 1, &pQueryWindow->ekey);

  int64_t interval = 0;
  colDataSetInt64(pColData, 2, &interval);  // this value may be variable in case of 'n' and 'y'.
  colDataSetInt64(pColData, 3, &pQueryWindow->skey);
  colDataSetInt64(pColData, 4, &pQueryWindow->ekey);
  return TSDB_CODE_SUCCESS;
}

static int32_t doSetInputDataBlockInfo(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SqlFunctionCtx* pCtx = pExprSup->pCtx;
  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    pCtx[i].order = order;
    pCtx[i].input.numOfRows = pBlock->info.rows;
    code = setBlockSMAInfo(&pCtx[i], &pExprSup->pExprInfo[i], pBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    pCtx[i].pSrcBlock = pBlock;
    pCtx[i].scanFlag = scanFlag;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t setInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                          bool createDummyCol) {
  if (pBlock->pBlockAgg != NULL) {
    return doSetInputDataBlockInfo(pExprSup, pBlock, order, scanFlag);
  } else {
    return doSetInputDataBlock(pExprSup, pBlock, order, scanFlag, createDummyCol);
  }
}

static int32_t doCreateConstantValColumnInfo(SInputColumnInfoData* pInput, SFunctParam* pFuncParam, int32_t paramIndex,
                                             int32_t numOfRows) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* pColInfo = NULL;
  if (pInput->pData[paramIndex] == NULL) {
    pColInfo = taosMemoryCalloc(1, sizeof(SColumnInfoData));
    QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    // Set the correct column info (data type and bytes)
    pColInfo->info.type = pFuncParam->param.nType;
    pColInfo->info.bytes = pFuncParam->param.nLen;

    pInput->pData[paramIndex] = pColInfo;
  } else {
    pColInfo = pInput->pData[paramIndex];
  }

  code = colInfoDataEnsureCapacity(pColInfo, numOfRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

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
  } else if (type == TSDB_DATA_TYPE_VARCHAR || type == TSDB_DATA_TYPE_GEOMETRY) {
    char* tmp = taosMemoryMalloc(pFuncParam->param.nLen + VARSTR_HEADER_SIZE);
    STR_WITH_SIZE_TO_VARSTR(tmp, pFuncParam->param.pz, pFuncParam->param.nLen);
    for (int32_t i = 0; i < numOfRows; ++i) {
      code = colDataSetVal(pColInfo, i, tmp, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    taosMemoryFree(tmp);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                                   bool createDummyCol) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
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
    bool       hasPk = pOneExpr->pExpr->nodeType == QUERY_NODE_FUNCTION && pOneExpr->pExpr->_function.pFunctNode->hasPk;
    pCtx[i].hasPrimaryKey = hasPk;

    int16_t tsParamIdx = (!hasPk) ? pOneExpr->base.numOfParams - 1 : pOneExpr->base.numOfParams - 2;
    int16_t pkParamIdx = pOneExpr->base.numOfParams - 1;

    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        pInput->pData[j] = taosArrayGet(pBlock->pDataBlock, slotId);
        pInput->totalRows = pBlock->info.rows;
        pInput->numOfRows = pBlock->info.rows;
        pInput->startRowIndex = 0;
        pInput->blankFill = pBlock->info.blankFill;

        // NOTE: the last parameter is the primary timestamp column
        // todo: refactor this

        if (fmIsImplicitTsFunc(pCtx[i].functionId) && (j == tsParamIdx)) {
          pInput->pPTS = pInput->pData[j];  // in case of merge function, this is not always the ts column data.
        }
        if (hasPk && (j == pkParamIdx)) {
          pInput->pPrimaryKey = pInput->pData[j];
        }
        QUERY_CHECK_CONDITION((pInput->pData[j] != NULL), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        // todo avoid case: top(k, 12), 12 is the value parameter.
        // sum(11), 11 is also the value parameter.
        if (createDummyCol && pOneExpr->base.numOfParams == 1) {
          pInput->totalRows = pBlock->info.rows;
          pInput->numOfRows = pBlock->info.rows;
          pInput->startRowIndex = 0;
          pInput->blankFill = pBlock->info.blankFill;

          code = doCreateConstantValColumnInfo(pInput, pFuncParam, j, pBlock->info.rows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
      return terrno;
    }

    // Set the correct column info (data type and bytes)
    pInput->pData[paramIndex]->info.type = type;
    pInput->pData[paramIndex]->info.bytes = tDataTypes[type].bytes;
  }

  SColumnDataAgg* da = NULL;
  if (pInput->pColumnDataAgg[paramIndex] == NULL) {
    da = taosMemoryCalloc(1, sizeof(SColumnDataAgg));
    if (!da) {
      return terrno;
    }
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

int32_t setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
        pInput->pColumnDataAgg[j] = &pBlock->pBlockAgg[slotId];
        if (pInput->pColumnDataAgg[j]->colId == -1) {
          pInput->colDataSMAIsSet = false;
        }

        // Here we set the column info data since the data type for each column data is required, but
        // the data in the corresponding SColumnInfoData will not be used.
        pInput->pData[j] = taosArrayGet(pBlock->pDataBlock, slotId);
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        code = doCreateConstantValColumnSMAInfo(pInput, pFuncParam, pFuncParam->param.nType, j, pBlock->info.rows);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else {
    pInput->colDataSMAIsSet = false;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/////////////////////////////////////////////////////////////////////////////////////////////
STimeWindow getAlignQueryTimeWindow(const SInterval* pInterval, int64_t key) {
  STimeWindow win = {0};
  win.skey = taosTimeTruncate(key, pInterval);

  /*
   * if the realSkey > INT64_MAX - pInterval->interval, the query duration between
   * realSkey and realEkey must be less than one interval.Therefore, no need to adjust the query ranges.
   */
  win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);
  if (win.ekey < win.skey) {
    win.ekey = INT64_MAX;
  }

  return win;
}

int32_t setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput,
                            int32_t* rowEntryInfoOffset) {
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
        int32_t code = pCtx[i].fpSet.init(&pCtx[i], pResInfo);
        if (code != TSDB_CODE_SUCCESS && fmIsUserDefinedFunc(pCtx[i].functionId)) {
          pResInfo->initialized = false;
          return TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
        } else if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        pResInfo->initialized = true;
      }
    } else {
      init = true;
    }
  }
  return TSDB_CODE_SUCCESS;
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

int32_t doFilter(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SColMatchInfo* pColMatchInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pFilterInfo == NULL || pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  SColumnInfoData*   p = NULL;

  code = filterSetDataFromSlotId(pFilterInfo, &param1);
  QUERY_CHECK_CODE(code, lino, _err);

  int32_t status = 0;
  code = filterExecute(pFilterInfo, pBlock, &p, NULL, param1.numOfCols, &status);
  QUERY_CHECK_CODE(code, lino, _err);

  code = extractQualifiedTupleByFilterResult(pBlock, p, status);
  QUERY_CHECK_CODE(code, lino, _err);

  if (pColMatchInfo != NULL) {
    size_t size = taosArrayGetSize(pColMatchInfo->pList);
    for (int32_t i = 0; i < size; ++i) {
      SColMatchItem* pInfo = taosArrayGet(pColMatchInfo->pList, i);
      QUERY_CHECK_NULL(pInfo, code, lino, _err, terrno);
      if (pInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pInfo->dstSlotId);
        QUERY_CHECK_NULL(pColData, code, lino, _err, terrno);
        if (pColData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
          code = blockDataUpdateTsWindow(pBlock, pInfo->dstSlotId);
          QUERY_CHECK_CODE(code, lino, _err);
          break;
        }
      }
    }
  }
  code = TSDB_CODE_SUCCESS;

_err:
  colDataDestroy(p);
  taosMemoryFree(p);
  return code;
}

int32_t extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, int32_t status) {
  int32_t code = TSDB_CODE_SUCCESS;
  int8_t* pIndicator = (int8_t*)p->pData;
  if (status == FILTER_RESULT_ALL_QUALIFIED) {
    // here nothing needs to be done
  } else if (status == FILTER_RESULT_NONE_QUALIFIED) {
    code = trimDataBlock(pBlock, pBlock->info.rows, NULL);
    pBlock->info.rows = 0;
  } else if (status == FILTER_RESULT_PARTIAL_QUALIFIED) {
    code = trimDataBlock(pBlock, pBlock->info.rows, (bool*)pIndicator);
  } else {
    qError("unknown filter result type: %d", status);
  }
  return code;
}

void doUpdateNumOfRows(SqlFunctionCtx* pCtx, SResultRow* pRow, int32_t numOfExprs, const int32_t* rowEntryOffset) {
  bool returnNotNull = false;
  for (int32_t j = 0; j < numOfExprs; ++j) {
    SResultRowEntryInfo* pResInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
    if (!isRowEntryInitialized(pResInfo)) {
      continue;
    } else {
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  for (int32_t j = 0; j < numOfExprs; ++j) {
    int32_t slotId = pExprInfo[j].base.resSchema.slotId;

    pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
    if (pCtx[j].fpSet.finalize) {
      if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_group_key") == 0 ||
          strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_group_const_value") == 0) {
        // for groupkey along with functions that output multiple lines(e.g. Histogram)
        // need to match groupkey result for each output row of that function.
        if (pCtx[j].resultInfo->numOfRes != 0) {
          pCtx[j].resultInfo->numOfRes = pRow->numOfRows;
        }
      }

      code = blockDataEnsureCapacity(pBlock, pBlock->info.rows + pCtx[j].resultInfo->numOfRes);
      QUERY_CHECK_CODE(code, lino, _end);

      int32_t winCode = pCtx[j].fpSet.finalize(&pCtx[j], pBlock);
      if (TAOS_FAILED(winCode)) {
        qError("%s build result data block error, code %s", GET_TASKID(pTaskInfo), tstrerror(winCode));
        T_LONG_JMP(pTaskInfo->env, winCode);
      }
    } else if (strcmp(pCtx[j].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
      // do nothing
    } else {
      // expand the result into multiple rows. E.g., _wstart, top(k, 20)
      // the _wstart needs to copy to 20 following rows, since the results of top-k expands to 20 different rows.
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      char*            in = GET_ROWCELL_INTERBUF(pCtx[j].resultInfo);
      for (int32_t k = 0; k < pRow->numOfRows; ++k) {
        code = colDataSetVal(pColInfoData, pBlock->info.rows + k, in, pCtx[j].resultInfo->isNullRes);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

// todo refactor. SResultRow has direct pointer in miainfo
void finalizeResultRows(SDiskbasedBuf* pBuf, SResultRowPosition* resultRowPosition, SExprSupp* pSup,
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
    return;
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
}

void doCopyToSDataBlockByHash(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                              SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap, int32_t threshold, bool ignoreGroup) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SExprInfo*      pExprInfo = pSup->pExprInfo;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;

  size_t  keyLen = 0;
  int32_t numOfRows = tSimpleHashGetSize(pHashmap);

  // begin from last iter
  void*   pData = pGroupResInfo->dataPos;
  int32_t iter = pGroupResInfo->iter;
  while ((pData = tSimpleHashIterate(pHashmap, pData, &iter)) != NULL) {
    void*               key = tSimpleHashGetKey(pData, &keyLen);
    SResultRowPosition* pos = pData;
    uint64_t            groupId = *(uint64_t*)key;

    SFilePage* page = getBufPage(pBuf, pos->pageId);
    if (page == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    SResultRow* pRow = (SResultRow*)((char*)page + pos->offset);

    doUpdateNumOfRows(pCtx, pRow, numOfExprs, rowEntryOffset);

    // no results, continue to check the next one
    if (pRow->numOfRows == 0) {
      pGroupResInfo->index += 1;
      pGroupResInfo->iter = iter;
      pGroupResInfo->dataPos = pData;

      releaseBufPage(pBuf, page);
      continue;
    }

    if (!ignoreGroup) {
      if (pBlock->info.id.groupId == 0) {
        pBlock->info.id.groupId = groupId;
      } else {
        // current value belongs to different group, it can't be packed into one datablock
        if (pBlock->info.id.groupId != groupId) {
          releaseBufPage(pBuf, page);
          break;
        }
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + ((numOfRows - iter) > 1 ? 1 : 0);
      code = blockDataEnsureCapacity(pBlock, newSize);
      QUERY_CHECK_CODE(code, lino, _end);
      qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s", newSize,
             pBlock->info.capacity, GET_TASKID(pTaskInfo));
      // todo set the pOperator->resultInfo size
    }

    pGroupResInfo->index += 1;
    pGroupResInfo->iter = iter;
    pGroupResInfo->dataPos = pData;

    copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo);

    releaseBufPage(pBuf, page);
    pBlock->info.rows += pRow->numOfRows;
    if (pBlock->info.rows >= threshold) {
      break;
    }
  }

  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);
  pBlock->info.dataLoad = 1;
  code = blockDataUpdateTsWindow(pBlock, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

void doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                        SGroupResInfo* pGroupResInfo, int32_t threshold, bool ignoreGroup) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
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

    if (!ignoreGroup) {
      if (pBlock->info.id.groupId == 0) {
        pBlock->info.id.groupId = pPos->groupId;
      } else {
        // current value belongs to different group, it can't be packed into one datablock
        if (pBlock->info.id.groupId != pPos->groupId) {
          releaseBufPage(pBuf, page);
          break;
        }
      }
    }

    if (pBlock->info.rows + pRow->numOfRows > pBlock->info.capacity) {
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + ((numOfRows - i) > 1 ? 1 : 0);
      code = blockDataEnsureCapacity(pBlock, newSize);
      QUERY_CHECK_CODE(code, lino, _end);
      qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s", newSize,
             pBlock->info.capacity, GET_TASKID(pTaskInfo));
      // todo set the pOperator->resultInfo size
    }

    pGroupResInfo->index += 1;
    copyResultrowToDataBlock(pExprInfo, numOfExprs, pRow, pCtx, pBlock, rowEntryOffset, pTaskInfo);

    releaseBufPage(pBuf, page);
    pBlock->info.rows += pRow->numOfRows;
    if (pBlock->info.rows >= threshold) {
      break;
    }
  }

  qDebug("%s result generated, rows:%" PRId64 ", groupId:%" PRIu64, GET_TASKID(pTaskInfo), pBlock->info.rows,
         pBlock->info.id.groupId);
  pBlock->info.dataLoad = 1;
  code = blockDataUpdateTsWindow(pBlock, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
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
    doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo, pOperator->resultInfo.threshold,
                       false);
  } else {
    while (hasRemainResults(pGroupResInfo)) {
      doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo, pOperator->resultInfo.threshold,
                         true);
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

int32_t getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, uint32_t* defaultBufsz) {
  *defaultPgsz = 4096;
  uint32_t last = *defaultPgsz;
  while (*defaultPgsz < rowSize * 4) {
    *defaultPgsz <<= 1u;
    if (*defaultPgsz < last) {
      return TSDB_CODE_INVALID_PARA;
    }
    last = *defaultPgsz;
  }

  // The default buffer for each operator in query is 10MB.
  // at least four pages need to be in buffer
  // TODO: make this variable to be configurable.
  *defaultBufsz = 4096 * 2560;
  if ((*defaultBufsz) <= (*defaultPgsz)) {
    (*defaultBufsz) = (*defaultPgsz) * 4;
    if (*defaultBufsz < ((int64_t)(*defaultPgsz)) * 4) {
      return TSDB_CODE_INVALID_PARA;
    }
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

static void destroySqlFunctionCtx(SqlFunctionCtx* pCtx, SExprInfo* pExpr, int32_t numOfOutput) {
  if (pCtx == NULL) {
    return;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (pExpr != NULL) {
      SExprInfo* pExprInfo = &pExpr[i];
      for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
        if (pExprInfo->base.pParam[j].type == FUNC_PARAM_TYPE_VALUE) {
          taosMemoryFree(pCtx[i].input.pData[j]);
          taosMemoryFree(pCtx[i].input.pColumnDataAgg[j]);
        }
      }
    }
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
  return;
}

int32_t initExprSupp(SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfExpr, SFunctionStateStore* pStore) {
  pSup->pExprInfo = pExprInfo;
  pSup->numOfExprs = numOfExpr;
  if (pSup->pExprInfo != NULL) {
    pSup->pCtx = createSqlFunctionCtx(pExprInfo, numOfExpr, &pSup->rowEntryInfoOffset, pStore);
    if (pSup->pCtx == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void cleanupExprSupp(SExprSupp* pSupp) {
  destroySqlFunctionCtx(pSupp->pCtx, pSupp->pExprInfo, pSupp->numOfExprs);
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

void cleanupBasicInfo(SOptrBasicInfo* pInfo) {
  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;
}

bool groupbyTbname(SNodeList* pGroupList) {
  bool bytbname = false;
  if (LIST_LENGTH(pGroupList) == 1) {
    SNode* p = nodesListGetNode(pGroupList, 0);
    if (!p) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return false;
    }
    if (p->type == QUERY_NODE_FUNCTION) {
      // partition by tbname/group by tbname
      bytbname = (strcmp(((struct SFunctionNode*)p)->functionName, "tbname") == 0);
    }
  }

  return bytbname;
}

int32_t createDataSinkParam(SDataSinkNode* pNode, void** pParam, SExecTaskInfo* pTask, SReadHandle* readHandle) {
  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT: {
      SInserterParam* pInserterParam = taosMemoryCalloc(1, sizeof(SInserterParam));
      if (NULL == pInserterParam) {
        return terrno;
      }
      pInserterParam->readHandle = readHandle;

      *pParam = pInserterParam;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DELETE: {
      SDeleterParam* pDeleterParam = taosMemoryCalloc(1, sizeof(SDeleterParam));
      if (NULL == pDeleterParam) {
        return terrno;
      }

      SArray* pInfoList = NULL;
      int32_t code = getTableListInfo(pTask, &pInfoList);
      if (code != TSDB_CODE_SUCCESS || pInfoList == NULL) {
        taosMemoryFree(pDeleterParam);
        return code;
      }

      STableListInfo* pTableListInfo = taosArrayGetP(pInfoList, 0);
      taosArrayDestroy(pInfoList);

      pDeleterParam->suid = tableListGetSuid(pTableListInfo);

      // TODO extract uid list
      int32_t numOfTables = 0;
      code = tableListGetSize(pTableListInfo, &numOfTables);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        taosMemoryFree(pDeleterParam);
        return code;
      }

      pDeleterParam->pUidList = taosArrayInit(numOfTables, sizeof(uint64_t));
      if (NULL == pDeleterParam->pUidList) {
        taosMemoryFree(pDeleterParam);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      for (int32_t i = 0; i < numOfTables; ++i) {
        STableKeyInfo* pTable = tableListGetInfo(pTableListInfo, i);
        if (!pTable) {
          taosArrayDestroy(pDeleterParam->pUidList);
          taosMemoryFree(pDeleterParam);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        void*          tmp = taosArrayPush(pDeleterParam->pUidList, &pTable->uid);
        if (!tmp) {
          taosArrayDestroy(pDeleterParam->pUidList);
          taosMemoryFree(pDeleterParam);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }

      *pParam = pDeleterParam;
      break;
    }
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

void streamOpReleaseState(SOperatorInfo* pOperator) {
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamOpReloadState(SOperatorInfo* pOperator) {
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
}

void freeOperatorParamImpl(SOperatorParam* pParam, SOperatorParamType type) {
  int32_t childrenNum = taosArrayGetSize(pParam->pChildren);
  for (int32_t i = 0; i < childrenNum; ++i) {
    SOperatorParam* pChild = taosArrayGetP(pParam->pChildren, i);
    freeOperatorParam(pChild, type);
  }

  taosArrayDestroy(pParam->pChildren);

  taosMemoryFree(pParam->value);

  taosMemoryFree(pParam);
}

void freeExchangeGetBasicOperatorParam(void* pParam) {
  SExchangeOperatorBasicParam* pBasic = (SExchangeOperatorBasicParam*)pParam;
  taosArrayDestroy(pBasic->uidList);
}

void freeExchangeGetOperatorParam(SOperatorParam* pParam) {
  SExchangeOperatorParam* pExcParam = (SExchangeOperatorParam*)pParam->value;
  if (pExcParam->multiParams) {
    SExchangeOperatorBatchParam* pExcBatch = (SExchangeOperatorBatchParam*)pParam->value;
    tSimpleHashCleanup(pExcBatch->pBatchs);
  } else {
    freeExchangeGetBasicOperatorParam(&pExcParam->basic);
  }

  freeOperatorParamImpl(pParam, OP_GET_PARAM);
}

void freeExchangeNotifyOperatorParam(SOperatorParam* pParam) { freeOperatorParamImpl(pParam, OP_NOTIFY_PARAM); }

void freeGroupCacheGetOperatorParam(SOperatorParam* pParam) { freeOperatorParamImpl(pParam, OP_GET_PARAM); }

void freeGroupCacheNotifyOperatorParam(SOperatorParam* pParam) { freeOperatorParamImpl(pParam, OP_NOTIFY_PARAM); }

void freeMergeJoinGetOperatorParam(SOperatorParam* pParam) { freeOperatorParamImpl(pParam, OP_GET_PARAM); }

void freeMergeJoinNotifyOperatorParam(SOperatorParam* pParam) { freeOperatorParamImpl(pParam, OP_NOTIFY_PARAM); }

void freeTableScanGetOperatorParam(SOperatorParam* pParam) {
  STableScanOperatorParam* pTableScanParam = (STableScanOperatorParam*)pParam->value;
  taosArrayDestroy(pTableScanParam->pUidList);
  freeOperatorParamImpl(pParam, OP_GET_PARAM);
}

void freeTableScanNotifyOperatorParam(SOperatorParam* pParam) { freeOperatorParamImpl(pParam, OP_NOTIFY_PARAM); }

void freeOperatorParam(SOperatorParam* pParam, SOperatorParamType type) {
  if (NULL == pParam) {
    return;
  }

  switch (pParam->opType) {
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      type == OP_GET_PARAM ? freeExchangeGetOperatorParam(pParam) : freeExchangeNotifyOperatorParam(pParam);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:
      type == OP_GET_PARAM ? freeGroupCacheGetOperatorParam(pParam) : freeGroupCacheNotifyOperatorParam(pParam);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      type == OP_GET_PARAM ? freeMergeJoinGetOperatorParam(pParam) : freeMergeJoinNotifyOperatorParam(pParam);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      type == OP_GET_PARAM ? freeTableScanGetOperatorParam(pParam) : freeTableScanNotifyOperatorParam(pParam);
      break;
    default:
      qError("unsupported op %d param, type %d", pParam->opType, type);
      break;
  }
}

void freeResetOperatorParams(struct SOperatorInfo* pOperator, SOperatorParamType type, bool allFree) {
  SOperatorParam**  ppParam = NULL;
  SOperatorParam*** pppDownstramParam = NULL;
  switch (type) {
    case OP_GET_PARAM:
      ppParam = &pOperator->pOperatorGetParam;
      pppDownstramParam = &pOperator->pDownstreamGetParams;
      break;
    case OP_NOTIFY_PARAM:
      ppParam = &pOperator->pOperatorNotifyParam;
      pppDownstramParam = &pOperator->pDownstreamNotifyParams;
      break;
    default:
      return;
  }

  if (*ppParam) {
    freeOperatorParam(*ppParam, type);
    *ppParam = NULL;
  }

  if (*pppDownstramParam) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      if ((*pppDownstramParam)[i]) {
        freeOperatorParam((*pppDownstramParam)[i], type);
        (*pppDownstramParam)[i] = NULL;
      }
    }
    if (allFree) {
      taosMemoryFreeClear(*pppDownstramParam);
    }
  }
}

FORCE_INLINE int32_t getNextBlockFromDownstreamImpl(struct SOperatorInfo* pOperator, int32_t idx, bool clearParam,
                                                    SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);

  int32_t code = 0;
  if (pOperator->pDownstreamGetParams && pOperator->pDownstreamGetParams[idx]) {
    qDebug("DynOp: op %s start to get block from downstream %s", pOperator->name, pOperator->pDownstream[idx]->name);
    code = pOperator->pDownstream[idx]->fpSet.getNextExtFn(pOperator->pDownstream[idx],
                                                           pOperator->pDownstreamGetParams[idx], pResBlock);
    if (clearParam && (code == 0)) {
      freeOperatorParam(pOperator->pDownstreamGetParams[idx], OP_GET_PARAM);
      pOperator->pDownstreamGetParams[idx] = NULL;
    }
    return code;
  }

  code = pOperator->pDownstream[idx]->fpSet.getNextFn(pOperator->pDownstream[idx], pResBlock);
  return code;
}

bool compareVal(const char* v, const SStateKeys* pKey) {
  if (IS_VAR_DATA_TYPE(pKey->type)) {
    if (varDataLen(v) != varDataLen(pKey->pData)) {
      return false;
    } else {
      return memcmp(varDataVal(v), varDataVal(pKey->pData), varDataLen(v)) == 0;
    }
  } else {
    return memcmp(pKey->pData, v, pKey->bytes) == 0;
  }
}
