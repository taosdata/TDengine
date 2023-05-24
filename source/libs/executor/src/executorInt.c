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

static void setBlockSMAInfo(SqlFunctionCtx* pCtx, SExprInfo* pExpr, SSDataBlock* pBlock);

static void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size);
static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);

static void    extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, bool keep,
                                                   int32_t status);
static int32_t doSetInputDataBlock(SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                                   bool createDummyCol);
static int32_t doCopyToSDataBlock(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                  SGroupResInfo* pGroupResInfo, int32_t threshold, bool ignoreGroup);

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
  } else if (type == TSDB_DATA_TYPE_VARCHAR || type == TSDB_DATA_TYPE_GEOMETRY) {
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
                           SGroupResInfo* pGroupResInfo, int32_t threshold, bool ignoreGroup) {
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
      uint32_t newSize = pBlock->info.rows + pRow->numOfRows + (numOfRows - i) > 1 ? 1 : 0;
      blockDataEnsureCapacity(pBlock, newSize);
      qDebug("datablock capacity not sufficient, expand to required:%d, current capacity:%d, %s",
             newSize, pBlock->info.capacity, GET_TASKID(pTaskInfo));
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
  doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo, pOperator->resultInfo.threshold, false);

  void* tbname = NULL;
  if (streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
    pBlock->info.parTbName[0] = 0;
  } else {
    memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
  }
  streamFreeVal(tbname);
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
    doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo, pOperator->resultInfo.threshold, false);
  } else {
    while (hasRemainResults(pGroupResInfo)) {
      doCopyToSDataBlock(pTaskInfo, pBlock, &pOperator->exprSupp, pBuf, pGroupResInfo, pOperator->resultInfo.threshold, true);
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

int32_t createDataSinkParam(SDataSinkNode* pNode, void** pParam, SExecTaskInfo* pTask, SReadHandle* readHandle) {
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

      SArray*         pInfoList = getTableListInfo(pTask);
      STableListInfo* pTableListInfo = taosArrayGetP(pInfoList, 0);
      taosArrayDestroy(pInfoList);

      pDeleterParam->suid = tableListGetSuid(pTableListInfo);

      // TODO extract uid list
      int32_t numOfTables = tableListGetSize(pTableListInfo);
      pDeleterParam->pUidList = taosArrayInit(numOfTables, sizeof(uint64_t));
      if (NULL == pDeleterParam->pUidList) {
        taosMemoryFree(pDeleterParam);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      for (int32_t i = 0; i < numOfTables; ++i) {
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

int32_t resultRowEncode(void* k, int32_t* size, char* buf) {
  // SResultRow* key = k;
  // int         len = 0;
  // int         struLen = *size;
  // len += taosEncodeFixedI32((void**)&buf, key->pageId);

  // uint32_t offset = key->offset;
  // len += taosEncodeFixedU32((void**)&buf, offset);

  // len += taosEncodeFixedI8((void**)&buf, key->startInterp);
  // len += taosEncodeFixedI8((void**)&buf, key->endInterp);
  // len += taosEncodeFixedI8((void**)&buf, key->closed);
  // len += taosEncodeFixedU32((void**)&buf, key->numOfRows);

  // len += taosEncodeFixedI64((void**)&buf, key->win.skey);
  // len += taosEncodeFixedI64((void**)&buf, key->win.ekey);

  // int32_t numOfEntryInfo = (struLen - sizeof(SResultRow)) / sizeof(struct SResultRowEntryInfo);
  // len += taosEncodeFixedI32((void**)&buf, numOfEntryInfo);
  // for (int i = 0; i < numOfEntryInfo; i++) {
  //   SResultRowEntryInfo* p = &key->pEntryInfo[i];

  //   uint8_t value = p->initialized ? 1 : 0;
  //   len += taosEncodeFixedU8((void**)&buf, value);

  //   value = p->complete ? 1 : 0;
  //   len += taosEncodeFixedU8((void**)&buf, value);

  //   value = p->isNullRes;
  //   len += taosEncodeFixedU8((void**)&buf, value);

  //   len += taosEncodeFixedU16((void**)&buf, p->numOfRes);
  // }
  // {
  //   char* strBuf = taosMemoryCalloc(1, *size * 100);
  //   resultRowToString(key, *size, strBuf);
  //   qWarn("encode result row:%s", strBuf);
  // }

  // return len;
  return 0;
}

int32_t resultRowDecode(void** k, size_t size, char* buf) {
  // char*    p1 = buf;
  // int32_t  numOfEntryInfo = 0;
  // uint32_t entryOffset = sizeof(int32_t) + sizeof(uint32_t) + sizeof(int8_t) + sizeof(int8_t) + sizeof(int8_t) +
  //                        sizeof(uint32_t) + sizeof(int64_t) + sizeof(int64_t);
  // taosDecodeFixedI32(p1 + entryOffset, &numOfEntryInfo);

  // char* p = buf;
  // size = sizeof(SResultRow) + numOfEntryInfo * sizeof(SResultRowEntryInfo);
  // SResultRow* key = taosMemoryCalloc(1, size);

  // p = taosDecodeFixedI32(p, (int32_t*)&key->pageId);
  // uint32_t offset = 0;
  // p = taosDecodeFixedU32(p, &offset);
  // key->offset = offset;

  // p = taosDecodeFixedI8(p, (int8_t*)(&key->startInterp));
  // p = taosDecodeFixedI8(p, (int8_t*)(&key->endInterp));
  // p = taosDecodeFixedI8(p, (int8_t*)&key->closed);
  // p = taosDecodeFixedU32(p, &key->numOfRows);

  // p = taosDecodeFixedI64(p, &key->win.skey);
  // p = taosDecodeFixedI64(p, &key->win.ekey);
  // p = taosDecodeFixedI32(p, &numOfEntryInfo);
  // for (int i = 0; i < numOfEntryInfo; i++) {
  //   SResultRowEntryInfo* pInfo = &key->pEntryInfo[i];
  //   uint8_t              value = 0;
  //   p = taosDecodeFixedU8(p, &value);
  //   pInfo->initialized = (value == 1) ? true : false;

  //   p = taosDecodeFixedU8(p, &value);
  //   pInfo->complete = (value == 1) ? true : false;

  //   p = taosDecodeFixedU8(p, &value);
  //   pInfo->isNullRes = value;

  //   p = taosDecodeFixedU16(p, &pInfo->numOfRes);
  // }
  // *k = key;

  // {
  //   char* strBuf = taosMemoryCalloc(1, size * 100);
  //   resultRowToString(key, size, strBuf);
  //   qWarn("decode result row:%s", strBuf);
  // }
  // return size;
  return 0;
}

int32_t saveOutputBuf(SStreamState* pState, SWinKey* pKey, SResultRow* pResult, int32_t resSize) {
  // char* buf = taosMemoryCalloc(1, resSize * 10);
  // int   len = resultRowEncode((void*)pResult, &resSize, buf);
  // char* buf = taosMemoryCalloc(1, resSize);
  // memcpy(buf, pResult, resSize);
  streamStatePut(pState, pKey, (char*)pResult, resSize);
  // taosMemoryFree(buf);
  return TSDB_CODE_SUCCESS;
}

int32_t releaseOutputBuf(SStreamState* pState, SWinKey* pKey, SResultRow* pResult) {
  streamStateReleaseBuf(pState, pKey, pResult);
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
      streamFreeVal(tbname);
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

  qDebug("stream close tsdb reader, reset status uid:%" PRId64 " ts:%" PRId64, pTaskInfo->streamInfo.currentOffset.uid,
         pTaskInfo->streamInfo.currentOffset.ts);

  // todo refactor, other thread may already use this read to extract data.
  pTaskInfo->streamInfo.currentOffset = (STqOffsetVal){0};
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
