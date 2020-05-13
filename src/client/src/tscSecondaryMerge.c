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

#include "tscSecondaryMerge.h"
#include "os.h"
#include "tlosertree.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tutil.h"
#include "tscLog.h"

typedef struct SCompareParam {
  SLocalDataSource **pLocalData;
  tOrderDescriptor * pDesc;
  int32_t            numOfElems;
  int32_t            groupOrderType;
} SCompareParam;

int32_t treeComparator(const void *pLeft, const void *pRight, void *param) {
  int32_t pLeftIdx = *(int32_t *)pLeft;
  int32_t pRightIdx = *(int32_t *)pRight;

  SCompareParam *    pParam = (SCompareParam *)param;
  tOrderDescriptor * pDesc = pParam->pDesc;
  SLocalDataSource **pLocalData = pParam->pLocalData;

  /* this input is exhausted, set the special value to denote this */
  if (pLocalData[pLeftIdx]->rowIdx == -1) {
    return 1;
  }

  if (pLocalData[pRightIdx]->rowIdx == -1) {
    return -1;
  }

  if (pParam->groupOrderType == TSDB_ORDER_DESC) {  // desc
    return compare_d(pDesc, pParam->numOfElems, pLocalData[pLeftIdx]->rowIdx, pLocalData[pLeftIdx]->filePage.data,
                     pParam->numOfElems, pLocalData[pRightIdx]->rowIdx, pLocalData[pRightIdx]->filePage.data);
  } else {
    return compare_a(pDesc, pParam->numOfElems, pLocalData[pLeftIdx]->rowIdx, pLocalData[pLeftIdx]->filePage.data,
                     pParam->numOfElems, pLocalData[pRightIdx]->rowIdx, pLocalData[pRightIdx]->filePage.data);
  }
}

static void tscInitSqlContext(SSqlCmd *pCmd, SLocalReducer *pReducer, tOrderDescriptor *pDesc) {
  /*
   * the fields and offset attributes in pCmd and pModel may be different due to
   * merge requirement. So, the final result in pRes structure is formatted in accordance with the pCmd object.
   */
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = 0; i < size; ++i) {
    SQLFunctionCtx *pCtx = &pReducer->pCtx[i];
    SSqlExpr *      pExpr = tscSqlExprGet(pQueryInfo, i);

    pCtx->aOutputBuf =
        pReducer->pResultBuf->data + tscFieldInfoGetOffset(pQueryInfo, i) * pReducer->resColModel->capacity;
    pCtx->order = pQueryInfo->order.order;
    pCtx->functionId = pExpr->functionId;

    // input buffer hold only one point data
    int16_t  offset = getColumnModelOffset(pDesc->pColumnModel, i);
    SSchema *pSchema = getColumnModelSchema(pDesc->pColumnModel, i);

    pCtx->aInputElemBuf = pReducer->pTempBuffer->data + offset;

    // input data format comes from pModel
    pCtx->inputType = pSchema->type;
    pCtx->inputBytes = pSchema->bytes;

    // output data format yet comes from pCmd.
    pCtx->outputBytes = pExpr->resBytes;
    pCtx->outputType = pExpr->resType;

    pCtx->startOffset = 0;
    pCtx->size = 1;
    pCtx->hasNull = true;
    pCtx->currentStage = SECONDARY_STAGE_MERGE;

    // for top/bottom function, the output of timestamp is the first column
    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      pCtx->ptsOutputBuf = pReducer->pCtx[0].aOutputBuf;
      pCtx->param[2].i64Key = pQueryInfo->order.order;
      pCtx->param[2].nType  = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[1].i64Key = pQueryInfo->order.orderColId;
    }

    SResultInfo *pResInfo = &pReducer->pResInfo[i];
    pResInfo->bufLen = pExpr->interBytes;
    pResInfo->interResultBuf = calloc(1, (size_t) pResInfo->bufLen);

    pCtx->resultInfo = &pReducer->pResInfo[i];
    pCtx->resultInfo->superTableQ = true;
  }

  int16_t          n = 0;
  int16_t          tagLen = 0;
  SQLFunctionCtx **pTagCtx = calloc(pQueryInfo->fieldsInfo.numOfOutput, POINTER_BYTES);

  SQLFunctionCtx *pCtx = NULL;
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_TAG_DUMMY || pExpr->functionId == TSDB_FUNC_TS_DUMMY) {
      tagLen += pExpr->resBytes;
      pTagCtx[n++] = &pReducer->pCtx[i];
    } else if ((aAggs[pExpr->functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      pCtx = &pReducer->pCtx[i];
    }
  }

  if (n == 0) {
    free(pTagCtx);
  } else {
    pCtx->tagInfo.pTagCtxList = pTagCtx;
    pCtx->tagInfo.numOfTagCols = n;
    pCtx->tagInfo.tagsLen = tagLen;
  }
}

void tscCreateLocalReducer(tExtMemBuffer **pMemBuffer, int32_t numOfBuffer, tOrderDescriptor *pDesc,
                           SColumnModel *finalmodel, SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  
  if (pMemBuffer == NULL) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);
  
    tscError("%p pMemBuffer is NULL", pMemBuffer);
    pRes->code = TSDB_CODE_APP_ERROR;
    return;
  }
 
  if (pDesc->pColumnModel == NULL) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);

    tscError("%p no local buffer or intermediate result format model", pSql);
    pRes->code = TSDB_CODE_APP_ERROR;
    return;
  }

  int32_t numOfFlush = 0;
  for (int32_t i = 0; i < numOfBuffer; ++i) {
    int32_t len = pMemBuffer[i]->fileMeta.flushoutData.nLength;
    if (len == 0) {
      tscTrace("%p no data retrieved from orderOfVnode:%d", pSql, i + 1);
      continue;
    }

    numOfFlush += len;
  }

  if (numOfFlush == 0 || numOfBuffer == 0) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);
    tscTrace("%p retrieved no data", pSql);

    return;
  }

  if (pDesc->pColumnModel->capacity >= pMemBuffer[0]->pageSize) {
    tscError("%p Invalid value of buffer capacity %d and page size %d ", pSql, pDesc->pColumnModel->capacity,
             pMemBuffer[0]->pageSize);

    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);
    pRes->code = TSDB_CODE_APP_ERROR;
    return;
  }

  size_t size = sizeof(SLocalReducer) + POINTER_BYTES * numOfFlush;
  
  SLocalReducer *pReducer = (SLocalReducer *) calloc(1, size);
  if (pReducer == NULL) {
    tscError("%p failed to create local merge structure, out of memory", pSql);

    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return;
  }

  pReducer->pExtMemBuffer = pMemBuffer;
  pReducer->pLocalDataSrc = (SLocalDataSource **)&pReducer[1];
  assert(pReducer->pLocalDataSrc != NULL);

  pReducer->numOfBuffer = numOfFlush;
  pReducer->numOfVnode = numOfBuffer;

  pReducer->pDesc = pDesc;
  tscTrace("%p the number of merged leaves is: %d", pSql, pReducer->numOfBuffer);

  int32_t idx = 0;
  for (int32_t i = 0; i < numOfBuffer; ++i) {
    int32_t numOfFlushoutInFile = pMemBuffer[i]->fileMeta.flushoutData.nLength;

    for (int32_t j = 0; j < numOfFlushoutInFile; ++j) {
      SLocalDataSource *ds = (SLocalDataSource *)malloc(sizeof(SLocalDataSource) + pMemBuffer[0]->pageSize);
      if (ds == NULL) {
        tscError("%p failed to create merge structure", pSql);
        pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        return;
      }
      
      pReducer->pLocalDataSrc[idx] = ds;

      ds->pMemBuffer = pMemBuffer[i];
      ds->flushoutIdx = j;
      ds->filePage.numOfElems = 0;
      ds->pageId = 0;
      ds->rowIdx = 0;

      tscTrace("%p load data from disk into memory, orderOfVnode:%d, total:%d", pSql, i + 1, idx + 1);
      tExtMemBufferLoadData(pMemBuffer[i], &(ds->filePage), j, 0);
#ifdef _DEBUG_VIEW
      printf("load data page into mem for build loser tree: %" PRIu64 " rows\n", ds->filePage.numOfElems);
      SSrcColumnInfo colInfo[256] = {0};
      SQueryInfo *   pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

      tscGetSrcColumnInfo(colInfo, pQueryInfo);

      tColModelDisplayEx(pDesc->pColumnModel, ds->filePage.data, ds->filePage.numOfElems,
                         pMemBuffer[0]->numOfElemsPerPage, colInfo);
#endif
      
      if (ds->filePage.numOfElems == 0) {  // no data in this flush, the index does not increase
        tscTrace("%p flush data is empty, ignore %d flush record", pSql, idx);
        tfree(ds);
        continue;
      }
      
      idx += 1;
    }
  }
  
  // no data actually, no need to merge result.
  if (idx == 0) {
    return;
  }

  pReducer->numOfBuffer = idx;

  SCompareParam *param = malloc(sizeof(SCompareParam));
  param->pLocalData = pReducer->pLocalDataSrc;
  param->pDesc = pReducer->pDesc;
  param->numOfElems = pReducer->pLocalDataSrc[0]->pMemBuffer->numOfElemsPerPage;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  param->groupOrderType = pQueryInfo->groupbyExpr.orderType;

  pRes->code = tLoserTreeCreate(&pReducer->pLoserTree, pReducer->numOfBuffer, param, treeComparator);
  if (pReducer->pLoserTree == NULL || pRes->code != 0) {
    return;
  }

  // the input data format follows the old format, but output in a new format.
  // so, all the input must be parsed as old format
  pReducer->pCtx = (SQLFunctionCtx *)calloc(tscSqlExprNumOfExprs(pQueryInfo), sizeof(SQLFunctionCtx));
  pReducer->rowSize = pMemBuffer[0]->nElemSize;

  tscRestoreSQLFuncForSTableQuery(pQueryInfo);
  tscFieldInfoUpdateOffset(pQueryInfo);

  if (pReducer->rowSize > pMemBuffer[0]->pageSize) {
    assert(false);  // todo fixed row size is larger than the minimum page size;
  }

  pReducer->hasPrevRow = false;
  pReducer->hasUnprocessedRow = false;

  pReducer->prevRowOfInput = (char *)calloc(1, pReducer->rowSize);

  // used to keep the latest input row
  pReducer->pTempBuffer = (tFilePage *)calloc(1, pReducer->rowSize + sizeof(tFilePage));
  pReducer->discardData = (tFilePage *)calloc(1, pReducer->rowSize + sizeof(tFilePage));
  pReducer->discard = false;

  pReducer->nResultBufSize = pMemBuffer[0]->pageSize * 16;
  pReducer->pResultBuf = (tFilePage *)calloc(1, pReducer->nResultBufSize + sizeof(tFilePage));

  int32_t finalRowLength = tscGetResRowLength(pQueryInfo->exprList);
  pReducer->resColModel = finalmodel;
  pReducer->resColModel->capacity = pReducer->nResultBufSize / finalRowLength;
  assert(finalRowLength <= pReducer->rowSize);

  pReducer->pFinalRes = calloc(1, pReducer->rowSize * pReducer->resColModel->capacity);
  pReducer->pBufForInterpo = calloc(1, pReducer->nResultBufSize);

  if (pReducer->pTempBuffer == NULL || pReducer->discardData == NULL || pReducer->pResultBuf == NULL ||
      pReducer->pBufForInterpo == NULL || pReducer->pFinalRes == NULL || pReducer->prevRowOfInput == NULL) {
    tfree(pReducer->pTempBuffer);
    tfree(pReducer->discardData);
    tfree(pReducer->pResultBuf);
    tfree(pReducer->pFinalRes);
    tfree(pReducer->pBufForInterpo);
    tfree(pReducer->prevRowOfInput);

    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return;
  }

  size = tscSqlExprNumOfExprs(pQueryInfo);
  
  pReducer->pTempBuffer->numOfElems = 0;
  pReducer->pResInfo = calloc(size, sizeof(SResultInfo));

  tscCreateResPointerInfo(pRes, pQueryInfo);
  tscInitSqlContext(pCmd, pReducer, pDesc);

  // we change the capacity of schema to denote that there is only one row in temp buffer
  pReducer->pDesc->pColumnModel->capacity = 1;

  // restore the limitation value at the last stage
  if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    pQueryInfo->limit.limit = pQueryInfo->clauseLimit;
    pQueryInfo->limit.offset = pQueryInfo->prjOffset;
  }

  pReducer->offset = pQueryInfo->limit.offset;

  pRes->pLocalReducer = pReducer;
  pRes->numOfGroups = 0;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int16_t prec = tinfo.precision;
  int64_t stime = (pQueryInfo->window.skey < pQueryInfo->window.ekey) ? pQueryInfo->window.skey : pQueryInfo->window.ekey;
  int64_t revisedSTime =
      taosGetIntervalStartTimestamp(stime, pQueryInfo->intervalTime, pQueryInfo->slidingTimeUnit, prec);

  SInterpolationInfo *pInterpoInfo = &pReducer->interpolationInfo;
  taosInitInterpoInfo(pInterpoInfo, pQueryInfo->order.order, revisedSTime, pQueryInfo->groupbyExpr.numOfGroupCols,
                      pReducer->rowSize);

  int32_t startIndex = pQueryInfo->fieldsInfo.numOfOutput - pQueryInfo->groupbyExpr.numOfGroupCols;

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    pInterpoInfo->pTags[0] = (char *)pInterpoInfo->pTags + POINTER_BYTES * pQueryInfo->groupbyExpr.numOfGroupCols;
    for (int32_t i = 1; i < pQueryInfo->groupbyExpr.numOfGroupCols; ++i) {
      SSchema *pSchema = getColumnModelSchema(pReducer->resColModel, startIndex + i - 1);
      pInterpoInfo->pTags[i] = pSchema->bytes + pInterpoInfo->pTags[i - 1];
    }
  } else {
    assert(pInterpoInfo->pTags == NULL);
  }
}

static int32_t tscFlushTmpBufferImpl(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage,
                                     int32_t orderType) {
  if (pPage->numOfElems == 0) {
    return 0;
  }

  assert(pPage->numOfElems <= pDesc->pColumnModel->capacity);

  // sort before flush to disk, the data must be consecutively put on tFilePage.
  if (pDesc->orderIdx.numOfCols > 0) {
    tColDataQSort(pDesc, pPage->numOfElems, 0, pPage->numOfElems - 1, pPage->data, orderType);
  }

#ifdef _DEBUG_VIEW
  printf("%" PRIu64 " rows data flushed to disk after been sorted:\n", pPage->numOfElems);
  tColModelDisplay(pDesc->pColumnModel, pPage->data, pPage->numOfElems, pPage->numOfElems);
#endif

  // write to cache after being sorted
  if (tExtMemBufferPut(pMemoryBuf, pPage->data, pPage->numOfElems) < 0) {
    tscError("failed to save data in temporary buffer");
    return -1;
  }

  pPage->numOfElems = 0;
  return 0;
}

int32_t tscFlushTmpBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, int32_t orderType) {
  int32_t ret = tscFlushTmpBufferImpl(pMemoryBuf, pDesc, pPage, orderType);
  if (ret != 0) {
    return -1;
  }

  if (!tExtMemBufferFlush(pMemoryBuf)) {
    return -1;
  }

  return 0;
}

int32_t saveToBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, void *data,
                     int32_t numOfRows, int32_t orderType) {
  SColumnModel *pModel = pDesc->pColumnModel;

  if (pPage->numOfElems + numOfRows <= pModel->capacity) {
    tColModelAppend(pModel, pPage, data, 0, numOfRows, numOfRows);
    return 0;
  }

  // current buffer is overflow, flush data to extensive buffer
  int32_t numOfRemainEntries = pModel->capacity - pPage->numOfElems;
  tColModelAppend(pModel, pPage, data, 0, numOfRemainEntries, numOfRows);

  // current buffer is full, need to flushed to disk
  assert(pPage->numOfElems == pModel->capacity);
  int32_t ret = tscFlushTmpBuffer(pMemoryBuf, pDesc, pPage, orderType);
  if (ret != 0) {
    return -1;
  }

  int32_t remain = numOfRows - numOfRemainEntries;

  while (remain > 0) {
    int32_t numOfWriteElems = 0;
    if (remain > pModel->capacity) {
      numOfWriteElems = pModel->capacity;
    } else {
      numOfWriteElems = remain;
    }

    tColModelAppend(pModel, pPage, data, numOfRows - remain, numOfWriteElems, numOfRows);

    if (pPage->numOfElems == pModel->capacity) {
      if (tscFlushTmpBuffer(pMemoryBuf, pDesc, pPage, orderType) != TSDB_CODE_SUCCESS) {
        return -1;
      }
    } else {
      pPage->numOfElems = numOfWriteElems;
    }

    remain -= numOfWriteElems;
    numOfRemainEntries += numOfWriteElems;
  }

  return 0;
}

void tscDestroyLocalReducer(SSqlObj *pSql) {
  if (pSql == NULL) {
    return;
  }

  tscTrace("%p start to free local reducer", pSql);
  SSqlRes *pRes = &(pSql->res);
  if (pRes->pLocalReducer == NULL) {
    tscTrace("%p local reducer has been freed, abort", pSql);
    return;
  }

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  // there is no more result, so we release all allocated resource
  SLocalReducer *pLocalReducer = (SLocalReducer *)atomic_exchange_ptr(&pRes->pLocalReducer, NULL);
  if (pLocalReducer != NULL) {
    int32_t status = 0;
    while ((status = atomic_val_compare_exchange_32(&pLocalReducer->status, TSC_LOCALREDUCE_READY,
                                                    TSC_LOCALREDUCE_TOBE_FREED)) == TSC_LOCALREDUCE_IN_PROGRESS) {
      taosMsleep(100);
      tscTrace("%p waiting for delete procedure, status: %d", pSql, status);
    }

    taosDestoryInterpoInfo(&pLocalReducer->interpolationInfo);

    if (pLocalReducer->pCtx != NULL) {
      for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
        SQLFunctionCtx *pCtx = &pLocalReducer->pCtx[i];

        tVariantDestroy(&pCtx->tag);
        if (pCtx->tagInfo.pTagCtxList != NULL) {
          tfree(pCtx->tagInfo.pTagCtxList);
        }
      }

      tfree(pLocalReducer->pCtx);
    }

    tfree(pLocalReducer->prevRowOfInput);

    tfree(pLocalReducer->pTempBuffer);
    tfree(pLocalReducer->pResultBuf);

    if (pLocalReducer->pResInfo != NULL) {
      for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
        tfree(pLocalReducer->pResInfo[i].interResultBuf);
      }

      tfree(pLocalReducer->pResInfo);
    }

    if (pLocalReducer->pLoserTree) {
      tfree(pLocalReducer->pLoserTree->param);
      tfree(pLocalReducer->pLoserTree);
    }

    tfree(pLocalReducer->pBufForInterpo);

    tfree(pLocalReducer->pFinalRes);
    tfree(pLocalReducer->discardData);

    tscLocalReducerEnvDestroy(pLocalReducer->pExtMemBuffer, pLocalReducer->pDesc, pLocalReducer->resColModel,
                              pLocalReducer->numOfVnode);
    for (int32_t i = 0; i < pLocalReducer->numOfBuffer; ++i) {
      tfree(pLocalReducer->pLocalDataSrc[i]);
    }

    pLocalReducer->numOfBuffer = 0;
    pLocalReducer->numOfCompleted = 0;
    free(pLocalReducer);
  } else {
    tscTrace("%p already freed or another free function is invoked", pSql);
  }

  tscTrace("%p free local reducer finished", pSql);
}

static int32_t createOrderDescriptor(tOrderDescriptor **pOrderDesc, SSqlCmd *pCmd, SColumnModel *pModel) {
  int32_t     numOfGroupByCols = 0;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    numOfGroupByCols = pQueryInfo->groupbyExpr.numOfGroupCols;
  }

  // primary timestamp column is involved in final result
  if (pQueryInfo->intervalTime != 0 || tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    numOfGroupByCols++;
  }

  int32_t *orderIdx = (int32_t *)calloc(numOfGroupByCols, sizeof(int32_t));
  if (orderIdx == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  if (numOfGroupByCols > 0) {
    int32_t startCols = pQueryInfo->fieldsInfo.numOfOutput - pQueryInfo->groupbyExpr.numOfGroupCols;

    // tags value locate at the last columns
    for (int32_t i = 0; i < pQueryInfo->groupbyExpr.numOfGroupCols; ++i) {
      orderIdx[i] = startCols++;
    }

    if (pQueryInfo->intervalTime != 0) {
      // the first column is the timestamp, handles queries like "interval(10m) group by tags"
      orderIdx[numOfGroupByCols - 1] = PRIMARYKEY_TIMESTAMP_COL_INDEX;
    }
  }

  *pOrderDesc = tOrderDesCreate(orderIdx, numOfGroupByCols, pModel, pQueryInfo->order.order);
  tfree(orderIdx);

  if (*pOrderDesc == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

bool isSameGroup(SSqlCmd *pCmd, SLocalReducer *pReducer, char *pPrev, tFilePage *tmpBuffer) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  int16_t functionId = tscSqlExprGet(pQueryInfo, 0)->functionId;

  // disable merge procedure for column projection query
  assert(functionId != TSDB_FUNC_ARITHM);

  if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    return true;
  }

  if (functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_ARITHM) {
    return false;
  }

  tOrderDescriptor *pOrderDesc = pReducer->pDesc;
  int32_t           numOfCols = pOrderDesc->orderIdx.numOfCols;

  // no group by columns, all data belongs to one group
  if (numOfCols <= 0) {
    return true;
  }

  if (pOrderDesc->orderIdx.pData[numOfCols - 1] == PRIMARYKEY_TIMESTAMP_COL_INDEX) {  //<= 0
    // super table interval query
    assert(pQueryInfo->intervalTime > 0);
    pOrderDesc->orderIdx.numOfCols -= 1;
  } else {  // simple group by query
    assert(pQueryInfo->intervalTime == 0);
  }

  // only one row exists
  int32_t ret = compare_a(pOrderDesc, 1, 0, pPrev, 1, 0, tmpBuffer->data);
  pOrderDesc->orderIdx.numOfCols = numOfCols;

  return (ret == 0);
}

int32_t tscLocalReducerEnvCreate(SSqlObj *pSql, tExtMemBuffer ***pMemBuffer, tOrderDescriptor **pOrderDesc,
                                 SColumnModel **pFinalModel, uint32_t nBufferSizes) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SSchema *     pSchema = NULL;
  SColumnModel *pModel = NULL;
  *pFinalModel = NULL;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  (*pMemBuffer) = (tExtMemBuffer **)malloc(POINTER_BYTES * 1);
  if (*pMemBuffer == NULL) {
    tscError("%p failed to allocate memory", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;
  }
  
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  pSchema = (SSchema *)calloc(1, sizeof(SSchema) * size);
  if (pSchema == NULL) {
    tscError("%p failed to allocate memory", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;
  }

  int32_t rlen = 0;
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);

    pSchema[i].bytes = pExpr->resBytes;
    pSchema[i].type = pExpr->resType;

    rlen += pExpr->resBytes;
  }

  int32_t capacity = 0;
  if (rlen != 0) {
    capacity = nBufferSizes / rlen;
  }
  
  pModel = createColumnModel(pSchema, size, capacity);

  size_t numOfSubs = pTableMetaInfo->vgroupList->numOfVgroups;
  for (int32_t i = 0; i < numOfSubs; ++i) {
    (*pMemBuffer)[i] = createExtMemBuffer(nBufferSizes, rlen, pModel);
    (*pMemBuffer)[i]->flushModel = MULTIPLE_APPEND_MODEL;
  }

  if (createOrderDescriptor(pOrderDesc, pCmd, pModel) != TSDB_CODE_SUCCESS) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;
  }

  // final result depends on the fields number
  memset(pSchema, 0, sizeof(SSchema) * size);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);

    SSchema *p1 = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pExpr->colInfo.colIndex);

    int16_t inter = 0;
    int16_t type = -1;
    int16_t bytes = 0;

    //    if ((pExpr->functionId >= TSDB_FUNC_FIRST_DST && pExpr->functionId <= TSDB_FUNC_LAST_DST) ||
    //        (pExpr->functionId >= TSDB_FUNC_SUM && pExpr->functionId <= TSDB_FUNC_MAX) ||
    //        pExpr->functionId == TSDB_FUNC_LAST_ROW) {
    // the final result size and type in the same as query on single table.
    // so here, set the flag to be false;

    int32_t functionId = pExpr->functionId;
    if (functionId >= TSDB_FUNC_TS && functionId <= TSDB_FUNC_DIFF) {
      type = pModel->pFields[i].field.type;
      bytes = pModel->pFields[i].field.bytes;
    } else {
      if (functionId == TSDB_FUNC_FIRST_DST) {
        functionId = TSDB_FUNC_FIRST;
      } else if (functionId == TSDB_FUNC_LAST_DST) {
        functionId = TSDB_FUNC_LAST;
      }

      getResultDataInfo(p1->type, p1->bytes, functionId, 0, &type, &bytes, &inter, 0, false);
    }

    pSchema[i].type = type;
    pSchema[i].bytes = bytes;
    strcpy(pSchema[i].name, pModel->pFields[i].field.name);
  }
  
  *pFinalModel = createColumnModel(pSchema, size, capacity);
  tfree(pSchema);

  return TSDB_CODE_SUCCESS;
}

/**
 * @param pMemBuffer
 * @param pDesc
 * @param pFinalModel
 * @param numOfVnodes
 */
void tscLocalReducerEnvDestroy(tExtMemBuffer **pMemBuffer, tOrderDescriptor *pDesc, SColumnModel *pFinalModel,
                               int32_t numOfVnodes) {
  destroyColumnModel(pFinalModel);
  tOrderDescDestroy(pDesc);
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pMemBuffer[i] = destoryExtMemBuffer(pMemBuffer[i]);
  }

  tfree(pMemBuffer);
}

/**
 *
 * @param pLocalReducer
 * @param pOneInterDataSrc
 * @param treeList
 * @return the number of remain input source. if ret == 0, all data has been handled
 */
int32_t loadNewDataFromDiskFor(SLocalReducer *pLocalReducer, SLocalDataSource *pOneInterDataSrc,
                               bool *needAdjustLoserTree) {
  pOneInterDataSrc->rowIdx = 0;
  pOneInterDataSrc->pageId += 1;

  if (pOneInterDataSrc->pageId <
      pOneInterDataSrc->pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[pOneInterDataSrc->flushoutIdx].numOfPages) {
    tExtMemBufferLoadData(pOneInterDataSrc->pMemBuffer, &(pOneInterDataSrc->filePage), pOneInterDataSrc->flushoutIdx,
                          pOneInterDataSrc->pageId);

#if defined(_DEBUG_VIEW)
    printf("new page load to buffer\n");
    tColModelDisplay(pOneInterDataSrc->pMemBuffer->pColumnModel, pOneInterDataSrc->filePage.data,
                     pOneInterDataSrc->filePage.numOfElems, pOneInterDataSrc->pMemBuffer->pColumnModel->capacity);
#endif
    *needAdjustLoserTree = true;
  } else {
    pLocalReducer->numOfCompleted += 1;

    pOneInterDataSrc->rowIdx = -1;
    pOneInterDataSrc->pageId = -1;
    *needAdjustLoserTree = true;
  }

  return pLocalReducer->numOfBuffer;
}

void adjustLoserTreeFromNewData(SLocalReducer *pLocalReducer, SLocalDataSource *pOneInterDataSrc,
                                SLoserTreeInfo *pTree) {
  /*
   * load a new data page into memory for intermediate dataset source,
   * since it's last record in buffer has been chosen to be processed, as the winner of loser-tree
   */
  bool needToAdjust = true;
  if (pOneInterDataSrc->filePage.numOfElems <= pOneInterDataSrc->rowIdx) {
    loadNewDataFromDiskFor(pLocalReducer, pOneInterDataSrc, &needToAdjust);
  }

  /*
   * adjust loser tree otherwise, according to new candidate data
   * if the loser tree is rebuild completed, we do not need to adjust
   */
  if (needToAdjust) {
    int32_t leafNodeIdx = pTree->pNode[0].index + pLocalReducer->numOfBuffer;

#ifdef _DEBUG_VIEW
    printf("before adjust:\t");
    tLoserTreeDisplay(pTree);
#endif

    tLoserTreeAdjust(pTree, leafNodeIdx);

#ifdef _DEBUG_VIEW
    printf("\nafter adjust:\t");
    tLoserTreeDisplay(pTree);
    printf("\n");
#endif
  }
}

void savePrevRecordAndSetupInterpoInfo(SLocalReducer *pLocalReducer, SQueryInfo *pQueryInfo,
                                       SInterpolationInfo *pInterpoInfo) {
  // discard following dataset in the same group and reset the interpolation information
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int16_t prec = tinfo.precision;
  int64_t stime = (pQueryInfo->window.skey < pQueryInfo->window.ekey) ? pQueryInfo->window.skey : pQueryInfo->window.ekey;
  int64_t revisedSTime =
      taosGetIntervalStartTimestamp(stime, pQueryInfo->intervalTime, pQueryInfo->slidingTimeUnit, prec);

  taosInitInterpoInfo(pInterpoInfo, pQueryInfo->order.order, revisedSTime, pQueryInfo->groupbyExpr.numOfGroupCols,
                      pLocalReducer->rowSize);

  pLocalReducer->discard = true;
  pLocalReducer->discardData->numOfElems = 0;

  SColumnModel *pModel = pLocalReducer->pDesc->pColumnModel;
  tColModelAppend(pModel, pLocalReducer->discardData, pLocalReducer->prevRowOfInput, 0, 1, 1);
}

// todo merge with following function
// static void reversedCopyResultToDstBuf(SQueryInfo* pQueryInfo, SSqlRes *pRes, tFilePage *pFinalDataPage) {
//
//  for (int32_t i = 0; i < pQueryInfo->exprList.numOfExprs; ++i) {
//    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
//
//    int32_t offset = tscFieldInfoGetOffset(pQueryInfo, i);
//    char *  src = pFinalDataPage->data + (pRes->numOfRows - 1) * pField->bytes + pRes->numOfRows * offset;
//    char *  dst = pRes->data + pRes->numOfRows * offset;
//
//    for (int32_t j = 0; j < pRes->numOfRows; ++j) {
//      memcpy(dst, src, (size_t)pField->bytes);
//      dst += pField->bytes;
//      src -= pField->bytes;
//    }
//  }
//}

static void reversedCopyFromInterpolationToDstBuf(SQueryInfo *pQueryInfo, SSqlRes *pRes, tFilePage **pResPages,
                                                  SLocalReducer *pLocalReducer) {
  assert(0);
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = 0; i < size; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

    int32_t offset = tscFieldInfoGetOffset(pQueryInfo, i);
    assert(offset == getColumnModelOffset(pLocalReducer->resColModel, i));

    char *src = pResPages[i]->data + (pRes->numOfRows - 1) * pField->bytes;
    char *dst = pRes->data + pRes->numOfRows * offset;

    for (int32_t j = 0; j < pRes->numOfRows; ++j) {
      memcpy(dst, src, (size_t)pField->bytes);
      dst += pField->bytes;
      src -= pField->bytes;
    }
  }
}

/*
 * Note: pRes->pLocalReducer may be null, due to the fact that "tscDestroyLocalReducer" is called
 * by "interuptHandler" function in shell
 */
static void doInterpolateResult(SSqlObj *pSql, SLocalReducer *pLocalReducer, bool doneOutput) {
  SSqlCmd *   pCmd = &pSql->cmd;
  SSqlRes *   pRes = &pSql->res;
  tFilePage * pFinalDataPage = pLocalReducer->pResultBuf;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  if (pRes->pLocalReducer != pLocalReducer) {
    /*
     * Release the SSqlObj is called, and it is int destroying function invoked by other thread.
     * However, the other thread will WAIT until current process fully completes.
     * Since the flag of release struct is set by doLocalReduce function
     */
    assert(pRes->pLocalReducer == NULL);
  }

  if (pQueryInfo->intervalTime == 0 || pQueryInfo->interpoType == TSDB_INTERPO_NONE) {
    // no interval query, no interpolation
    pRes->data = pLocalReducer->pFinalRes;
    pRes->numOfRows = pFinalDataPage->numOfElems;
    pRes->numOfTotalInCurrentClause += pRes->numOfRows;

    if (pQueryInfo->limit.offset > 0) {
      if (pQueryInfo->limit.offset < pRes->numOfRows) {
        int32_t prevSize = pFinalDataPage->numOfElems;
        tColModelErase(pLocalReducer->resColModel, pFinalDataPage, prevSize, 0, pQueryInfo->limit.offset - 1);

        /* remove the hole in column model */
        tColModelCompact(pLocalReducer->resColModel, pFinalDataPage, prevSize);

        pRes->numOfRows -= pQueryInfo->limit.offset;
        pRes->numOfTotalInCurrentClause -= pQueryInfo->limit.offset;
        pQueryInfo->limit.offset = 0;
      } else {
        pQueryInfo->limit.offset -= pRes->numOfRows;
        pRes->numOfRows = 0;
        pRes->numOfTotalInCurrentClause = 0;
      }
    }

    if (pQueryInfo->limit.limit >= 0 && pRes->numOfTotalInCurrentClause > pQueryInfo->limit.limit) {
      /* impose the limitation of output rows on the final result */
      int32_t prevSize = pFinalDataPage->numOfElems;
      int32_t overFlow = pRes->numOfTotalInCurrentClause - pQueryInfo->limit.limit;

      assert(overFlow < pRes->numOfRows);

      pRes->numOfTotalInCurrentClause = pQueryInfo->limit.limit;
      pRes->numOfRows -= overFlow;
      pFinalDataPage->numOfElems -= overFlow;

      tColModelCompact(pLocalReducer->resColModel, pFinalDataPage, prevSize);

      /* set remain data to be discarded, and reset the interpolation information */
      savePrevRecordAndSetupInterpoInfo(pLocalReducer, pQueryInfo, &pLocalReducer->interpolationInfo);
    }

    int32_t rowSize = tscGetResRowLength(pQueryInfo->exprList);
    memcpy(pRes->data, pFinalDataPage->data, pRes->numOfRows * rowSize);

    pFinalDataPage->numOfElems = 0;
    return;
  }

  int64_t *pPrimaryKeys = (int64_t *)pLocalReducer->pBufForInterpo;

  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;

  int64_t actualETime = (pQueryInfo->window.skey < pQueryInfo->window.ekey) ? pQueryInfo->window.ekey : pQueryInfo->window.skey;

  tFilePage **pResPages = malloc(POINTER_BYTES * pQueryInfo->fieldsInfo.numOfOutput);
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
    pResPages[i] = calloc(1, sizeof(tFilePage) + pField->bytes * pLocalReducer->resColModel->capacity);
  }

  char **  srcData = (char **)malloc((POINTER_BYTES + sizeof(int32_t)) * pQueryInfo->fieldsInfo.numOfOutput);
  int32_t *functions = (int32_t *)((char *)srcData + pQueryInfo->fieldsInfo.numOfOutput * sizeof(void *));

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    srcData[i] =
        pLocalReducer->pBufForInterpo + tscFieldInfoGetOffset(pQueryInfo, i) * pInterpoInfo->numOfRawDataInRows;
    functions[i] = tscSqlExprGet(pQueryInfo, i)->functionId;
  }

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int8_t precision = tinfo.precision;

  while (1) {
    int32_t remains = taosNumOfRemainPoints(pInterpoInfo);
    TSKEY   etime = taosGetRevisedEndKey(actualETime, pQueryInfo->order.order, pQueryInfo->intervalTime,
                                       pQueryInfo->slidingTimeUnit, precision);
    int32_t nrows = taosGetNumOfResultWithInterpo(pInterpoInfo, pPrimaryKeys, remains, pQueryInfo->intervalTime, etime,
                                                  pLocalReducer->resColModel->capacity);

    int32_t newRows = taosDoInterpoResult(pInterpoInfo, pQueryInfo->interpoType, pResPages, remains, nrows,
                                          pQueryInfo->intervalTime, pPrimaryKeys, pLocalReducer->resColModel, srcData,
                                          pQueryInfo->defaultVal, functions, pLocalReducer->resColModel->capacity);
    assert(newRows <= nrows);

    if (pQueryInfo->limit.offset < newRows) {
      newRows -= pQueryInfo->limit.offset;

      if (pQueryInfo->limit.offset > 0) {
        for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
          TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
          memmove(pResPages[i]->data, pResPages[i]->data + pField->bytes * pQueryInfo->limit.offset,
                  newRows * pField->bytes);
        }
      }

      pRes->data = pLocalReducer->pFinalRes;
      pRes->numOfRows = newRows;
      pRes->numOfTotalInCurrentClause += newRows;

      pQueryInfo->limit.offset = 0;
      break;
    } else {
      pQueryInfo->limit.offset -= newRows;
      pRes->numOfRows = 0;

      int32_t rpoints = taosNumOfRemainPoints(pInterpoInfo);
      if (rpoints <= 0) {
        if (!doneOutput) {
          /* reduce procedure is not completed, but current results for interpolation are exhausted */
          break;
        }

        /* all output for current group are completed */
        int32_t totalRemainRows =
            taosGetNumOfResWithoutLimit(pInterpoInfo, pPrimaryKeys, rpoints, pQueryInfo->intervalTime, actualETime);
        if (totalRemainRows <= 0) {
          break;
        }
      }
    }
  }

  if (pRes->numOfRows > 0) {
    if (pQueryInfo->limit.limit >= 0 && pRes->numOfTotalInCurrentClause > pQueryInfo->limit.limit) {
      int32_t overFlow = pRes->numOfTotalInCurrentClause - pQueryInfo->limit.limit;
      pRes->numOfRows -= overFlow;

      assert(pRes->numOfRows >= 0);

      pRes->numOfTotalInCurrentClause = pQueryInfo->limit.limit;
      pFinalDataPage->numOfElems -= overFlow;

      /* set remain data to be discarded, and reset the interpolation information */
      savePrevRecordAndSetupInterpoInfo(pLocalReducer, pQueryInfo, pInterpoInfo);
    }

    if (pQueryInfo->order.order == TSDB_ORDER_ASC) {
      for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
        TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
        int16_t     offset = getColumnModelOffset(pLocalReducer->resColModel, i);
        memcpy(pRes->data + offset * pRes->numOfRows, pResPages[i]->data, pField->bytes * pRes->numOfRows);
      }
    } else {  // todo bug??
      reversedCopyFromInterpolationToDstBuf(pQueryInfo, pRes, pResPages, pLocalReducer);
    }
  }

  pFinalDataPage->numOfElems = 0;
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    tfree(pResPages[i]);
  }
  tfree(pResPages);

  free(srcData);
}

static void savePreviousRow(SLocalReducer *pLocalReducer, tFilePage *tmpBuffer) {
  SColumnModel *pColumnModel = pLocalReducer->pDesc->pColumnModel;
  assert(pColumnModel->capacity == 1 && tmpBuffer->numOfElems == 1);

  // copy to previous temp buffer
  for (int32_t i = 0; i < pColumnModel->numOfCols; ++i) {
    SSchema *pSchema = getColumnModelSchema(pColumnModel, i);
    int16_t  offset = getColumnModelOffset(pColumnModel, i);

    memcpy(pLocalReducer->prevRowOfInput + offset, tmpBuffer->data + offset, pSchema->bytes);
  }

  tmpBuffer->numOfElems = 0;
  pLocalReducer->hasPrevRow = true;
}

static void doExecuteSecondaryMerge(SSqlCmd *pCmd, SLocalReducer *pLocalReducer, bool needInit) {
  // the tag columns need to be set before all functions execution
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);

  for (int32_t j = 0; j < size; ++j) {
    SSqlExpr *      pExpr = tscSqlExprGet(pQueryInfo, j);
    SQLFunctionCtx *pCtx = &pLocalReducer->pCtx[j];

    tVariantAssign(&pCtx->param[0], &pExpr->param[0]);

    // tags/tags_dummy function, the tag field of SQLFunctionCtx is from the input buffer
    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS_DUMMY) {
      tVariantDestroy(&pCtx->tag);
      tVariantCreateFromBinary(&pCtx->tag, pCtx->aInputElemBuf, pCtx->inputBytes, pCtx->inputType);
    }

    pCtx->currentStage = SECONDARY_STAGE_MERGE;

    if (needInit) {
      aAggs[pExpr->functionId].init(pCtx);
    }
  }

  for (int32_t j = 0; j < size; ++j) {
    int32_t functionId = tscSqlExprGet(pQueryInfo, j)->functionId;
    if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    aAggs[functionId].distSecondaryMergeFunc(&pLocalReducer->pCtx[j]);
  }
}

static void handleUnprocessedRow(SSqlCmd *pCmd, SLocalReducer *pLocalReducer, tFilePage *tmpBuffer) {
  if (pLocalReducer->hasUnprocessedRow) {
    pLocalReducer->hasUnprocessedRow = false;
    doExecuteSecondaryMerge(pCmd, pLocalReducer, true);
    savePreviousRow(pLocalReducer, tmpBuffer);
  }
}

static int64_t getNumOfResultLocal(SQueryInfo *pQueryInfo, SQLFunctionCtx *pCtx) {
  int64_t maxOutput = 0;
  
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t j = 0; j < size; ++j) {
    //    SSqlExpr* pExpr = pQueryInfo->fieldsInfo.pSqlExpr[j];
    //    if (pExpr == NULL) {
    //      assert(pQueryInfo->fieldsInfo.pExpr[j] != NULL);
    //
    //      maxOutput = 1;
    //      continue;
    //    }

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, j);
    int32_t   functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    if (maxOutput < GET_RES_INFO(&pCtx[j])->numOfRes) {
      maxOutput = GET_RES_INFO(&pCtx[j])->numOfRes;
    }
  }

  return maxOutput;
}

/*
 * in handling the top/bottom query, which produce more than one rows result,
 * the tsdb_func_tags only fill the first row of results, the remain rows need to
 * filled with the same result, which is the tags, specified in group by clause
 *
 */
static void fillMultiRowsOfTagsVal(SQueryInfo *pQueryInfo, int32_t numOfRes, SLocalReducer *pLocalReducer) {
  int32_t maxBufSize = 0;  // find the max tags column length to prepare the buffer
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t k = 0; k < size; ++k) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, k);
    if (maxBufSize < pExpr->resBytes && pExpr->functionId == TSDB_FUNC_TAG) {
      maxBufSize = pExpr->resBytes;
    }
  }

  assert(maxBufSize >= 0);

  char *buf = malloc((size_t)maxBufSize);
  for (int32_t k = 0; k < size; ++k) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, k);
    if (pExpr->functionId != TSDB_FUNC_TAG) {
      continue;
    }

    int32_t inc = numOfRes - 1;  // tsdb_func_tag function only produce one row of result
    memset(buf, 0, (size_t)maxBufSize);

    SQLFunctionCtx *pCtx = &pLocalReducer->pCtx[k];
    memcpy(buf, pCtx->aOutputBuf, (size_t)pCtx->outputBytes);

    for (int32_t i = 0; i < inc; ++i) {
      pCtx->aOutputBuf += pCtx->outputBytes;
      memcpy(pCtx->aOutputBuf, buf, (size_t)pCtx->outputBytes);
    }
  }

  free(buf);
}

int32_t finalizeRes(SQueryInfo *pQueryInfo, SLocalReducer *pLocalReducer) {
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t k = 0; k < size; ++k) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, k);
    aAggs[pExpr->functionId].xFinalize(&pLocalReducer->pCtx[k]);
  }

  pLocalReducer->hasPrevRow = false;

  int32_t numOfRes = (int32_t)getNumOfResultLocal(pQueryInfo, pLocalReducer->pCtx);
  pLocalReducer->pResultBuf->numOfElems += numOfRes;

  fillMultiRowsOfTagsVal(pQueryInfo, numOfRes, pLocalReducer);
  return numOfRes;
}

/*
 * points merge:
 * points are merged according to the sort info, which is tags columns and timestamp column.
 * In case of points without either tags columns or timestamp, such as
 * results generated by simple aggregation function, we merge them all into one points
 * *Exception*: column projection query, required no merge procedure
 */
bool needToMerge(SQueryInfo *pQueryInfo, SLocalReducer *pLocalReducer, tFilePage *tmpBuffer) {
  int32_t ret = 0;  // merge all result by default
  int16_t functionId = tscSqlExprGet(pQueryInfo, 0)->functionId;

  if (functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_ARITHM) {  // column projection query
    ret = 1;                                                            // disable merge procedure
  } else {
    tOrderDescriptor *pDesc = pLocalReducer->pDesc;
    if (pDesc->orderIdx.numOfCols > 0) {
      if (pDesc->tsOrder == TSDB_ORDER_ASC) {  // asc
        // todo refactor comparator
        ret = compare_a(pLocalReducer->pDesc, 1, 0, pLocalReducer->prevRowOfInput, 1, 0, tmpBuffer->data);
      } else {  // desc
        ret = compare_d(pLocalReducer->pDesc, 1, 0, pLocalReducer->prevRowOfInput, 1, 0, tmpBuffer->data);
      }
    }
  }

  /* if ret == 0, means the result belongs to the same group */
  return (ret == 0);
}

static bool reachGroupResultLimit(SQueryInfo *pQueryInfo, SSqlRes *pRes) {
  return (pRes->numOfGroups >= pQueryInfo->slimit.limit && pQueryInfo->slimit.limit >= 0);
}

static bool saveGroupResultInfo(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  pRes->numOfGroups += 1;

  // the output group is limited by the slimit clause
  if (reachGroupResultLimit(pQueryInfo, pRes)) {
    return true;
  }

  //    pRes->pGroupRec = realloc(pRes->pGroupRec, pRes->numOfGroups*sizeof(SResRec));
  //    pRes->pGroupRec[pRes->numOfGroups-1].numOfRows = pRes->numOfRows;
  //    pRes->pGroupRec[pRes->numOfGroups-1].numOfTotalInCurrentClause = pRes->numOfTotalInCurrentClause;

  return false;
}

/**
 *
 * @param pSql
 * @param pLocalReducer
 * @param noMoreCurrentGroupRes
 * @return if current group is skipped, return false, and do NOT record it into pRes->numOfGroups
 */
bool doGenerateFinalResults(SSqlObj *pSql, SLocalReducer *pLocalReducer, bool noMoreCurrentGroupRes) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SQueryInfo *  pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  tFilePage *   pResBuf = pLocalReducer->pResultBuf;
  SColumnModel *pModel = pLocalReducer->resColModel;

  pRes->code = TSDB_CODE_SUCCESS;

  /*
   * ignore the output of the current group since this group is skipped by user
   * We set the numOfRows to be 0 and discard the possible remain results.
   */
  if (pQueryInfo->slimit.offset > 0) {
    pRes->numOfRows = 0;
    pQueryInfo->slimit.offset -= 1;
    pLocalReducer->discard = !noMoreCurrentGroupRes;
    return false;
  }

  tColModelCompact(pModel, pResBuf, pModel->capacity);
  memcpy(pLocalReducer->pBufForInterpo, pResBuf->data, pLocalReducer->nResultBufSize);

#ifdef _DEBUG_VIEW
  printf("final result before interpo:\n");
  tColModelDisplay(pLocalReducer->resColModel, pLocalReducer->pBufForInterpo, pResBuf->numOfElems, pResBuf->numOfElems);
#endif

  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;
  int32_t             startIndex = pQueryInfo->fieldsInfo.numOfOutput - pQueryInfo->groupbyExpr.numOfGroupCols;

  for (int32_t i = 0; i < pQueryInfo->groupbyExpr.numOfGroupCols; ++i) {
    int16_t  offset = getColumnModelOffset(pModel, startIndex + i);
    SSchema *pSchema = getColumnModelSchema(pModel, startIndex + i);

    memcpy(pInterpoInfo->pTags[i], pLocalReducer->pBufForInterpo + offset * pResBuf->numOfElems, pSchema->bytes);
  }

  taosInterpoSetStartInfo(&pLocalReducer->interpolationInfo, pResBuf->numOfElems, pQueryInfo->interpoType);
  doInterpolateResult(pSql, pLocalReducer, noMoreCurrentGroupRes);

  return true;
}

void resetOutputBuf(SQueryInfo *pQueryInfo, SLocalReducer *pLocalReducer) {  // reset output buffer to the beginning
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    pLocalReducer->pCtx[i].aOutputBuf =
        pLocalReducer->pResultBuf->data + tscFieldInfoGetOffset(pQueryInfo, i) * pLocalReducer->resColModel->capacity;
  }

  memset(pLocalReducer->pResultBuf, 0, pLocalReducer->nResultBufSize + sizeof(tFilePage));
}

static void resetEnvForNewResultset(SSqlRes *pRes, SSqlCmd *pCmd, SLocalReducer *pLocalReducer) {
  // In handling data in other groups, we need to reset the interpolation information for a new group data
  pRes->numOfRows = 0;
  pRes->numOfTotalInCurrentClause = 0;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pQueryInfo->limit.offset = pLocalReducer->offset;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int8_t precision = tinfo.precision;

  // for group result interpolation, do not return if not data is generated
  if (pQueryInfo->interpoType != TSDB_INTERPO_NONE) {
    int64_t stime = (pQueryInfo->window.skey < pQueryInfo->window.ekey) ? pQueryInfo->window.skey : pQueryInfo->window.ekey;
    int64_t newTime =
        taosGetIntervalStartTimestamp(stime, pQueryInfo->intervalTime, pQueryInfo->slidingTimeUnit, precision);

    taosInitInterpoInfo(&pLocalReducer->interpolationInfo, pQueryInfo->order.order, newTime,
                        pQueryInfo->groupbyExpr.numOfGroupCols, pLocalReducer->rowSize);
  }
}

static bool isAllSourcesCompleted(SLocalReducer *pLocalReducer) {
  return (pLocalReducer->numOfBuffer == pLocalReducer->numOfCompleted);
}

static bool doInterpolationForCurrentGroup(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SQueryInfo *        pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  SLocalReducer *     pLocalReducer = pRes->pLocalReducer;
  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int8_t p = tinfo.precision;

  if (taosHasRemainsDataForInterpolation(pInterpoInfo)) {
    assert(pQueryInfo->interpoType != TSDB_INTERPO_NONE);

    tFilePage *pFinalDataBuf = pLocalReducer->pResultBuf;
    int64_t    etime = *(int64_t *)(pFinalDataBuf->data + TSDB_KEYSIZE * (pInterpoInfo->numOfRawDataInRows - 1));

    int32_t remain = taosNumOfRemainPoints(pInterpoInfo);
    TSKEY   ekey =
        taosGetRevisedEndKey(etime, pQueryInfo->order.order, pQueryInfo->intervalTime, pQueryInfo->slidingTimeUnit, p);
    int32_t rows = taosGetNumOfResultWithInterpo(pInterpoInfo, (TSKEY *)pLocalReducer->pBufForInterpo, remain,
                                                 pQueryInfo->intervalTime, ekey, pLocalReducer->resColModel->capacity);
    if (rows > 0) {  // do interpo
      doInterpolateResult(pSql, pLocalReducer, false);
    }

    return true;
  } else {
    return false;
  }
}

static bool doHandleLastRemainData(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SLocalReducer *     pLocalReducer = pRes->pLocalReducer;
  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;

  bool prevGroupCompleted = (!pLocalReducer->discard) && pLocalReducer->hasUnprocessedRow;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  int8_t precision = tinfo.precision;

  if ((isAllSourcesCompleted(pLocalReducer) && !pLocalReducer->hasPrevRow) || pLocalReducer->pLocalDataSrc[0] == NULL ||
      prevGroupCompleted) {
    // if interpoType == TSDB_INTERPO_NONE, return directly
    if (pQueryInfo->interpoType != TSDB_INTERPO_NONE) {
      int64_t etime = (pQueryInfo->window.skey < pQueryInfo->window.ekey) ? pQueryInfo->window.ekey : pQueryInfo->window.skey;

      etime = taosGetRevisedEndKey(etime, pQueryInfo->order.order, pQueryInfo->intervalTime,
                                   pQueryInfo->slidingTimeUnit, precision);
      int32_t rows = taosGetNumOfResultWithInterpo(pInterpoInfo, NULL, 0, pQueryInfo->intervalTime, etime,
                                                   pLocalReducer->resColModel->capacity);
      if (rows > 0) {  // do interpo
        doInterpolateResult(pSql, pLocalReducer, true);
      }
    }

    /*
     * 1. numOfRows == 0, means no interpolation results are generated.
     * 2. if all local data sources are consumed, and no un-processed rows exist.
     *
     * No results will be generated and query completed.
     */
    if (pRes->numOfRows > 0 || (isAllSourcesCompleted(pLocalReducer) && (!pLocalReducer->hasUnprocessedRow))) {
      return true;
    }

    // start to process result for a new group and save the result info of previous group
    if (saveGroupResultInfo(pSql)) {
      return true;
    }

    resetEnvForNewResultset(pRes, pCmd, pLocalReducer);
  }

  return false;
}

static void doProcessResultInNextWindow(SSqlObj *pSql, int32_t numOfRes) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SLocalReducer *pLocalReducer = pRes->pLocalReducer;
  SQueryInfo *   pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);

  for (int32_t k = 0; k < size; ++k) {
    SSqlExpr *      pExpr = tscSqlExprGet(pQueryInfo, k);
    SQLFunctionCtx *pCtx = &pLocalReducer->pCtx[k];

    pCtx->aOutputBuf += pCtx->outputBytes * numOfRes;

    // set the correct output timestamp column position
    if (pExpr->functionId == TSDB_FUNC_TOP || pExpr->functionId == TSDB_FUNC_BOTTOM) {
      pCtx->ptsOutputBuf = ((char *)pCtx->ptsOutputBuf + TSDB_KEYSIZE * numOfRes);
    }
  }

  doExecuteSecondaryMerge(pCmd, pLocalReducer, true);
}

int32_t tscDoLocalMerge(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  tscResetForNextRetrieve(pRes);

  if (pSql->signature != pSql || pRes == NULL || pRes->pLocalReducer == NULL) {  // all data has been processed
    tscTrace("%p %s call the drop local reducer", pSql, __FUNCTION__);
    tscDestroyLocalReducer(pSql);
    return 0;
  }

  SLocalReducer *pLocalReducer = pRes->pLocalReducer;
  SQueryInfo *   pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  // set the data merge in progress
  int32_t prevStatus =
      atomic_val_compare_exchange_32(&pLocalReducer->status, TSC_LOCALREDUCE_READY, TSC_LOCALREDUCE_IN_PROGRESS);
  if (prevStatus != TSC_LOCALREDUCE_READY) {
    assert(prevStatus == TSC_LOCALREDUCE_TOBE_FREED);  // it is in tscDestroyLocalReducer function already
    return TSDB_CODE_SUCCESS;
  }

  tFilePage *tmpBuffer = pLocalReducer->pTempBuffer;

  if (doHandleLastRemainData(pSql)) {
    pLocalReducer->status = TSC_LOCALREDUCE_READY;  // set the flag, taos_free_result can release this result.
    return TSDB_CODE_SUCCESS;
  }

  if (doInterpolationForCurrentGroup(pSql)) {
    pLocalReducer->status = TSC_LOCALREDUCE_READY;  // set the flag, taos_free_result can release this result.
    return TSDB_CODE_SUCCESS;
  }

  SLoserTreeInfo *pTree = pLocalReducer->pLoserTree;

  // clear buffer
  handleUnprocessedRow(pCmd, pLocalReducer, tmpBuffer);
  SColumnModel *pModel = pLocalReducer->pDesc->pColumnModel;

  while (1) {
    if (isAllSourcesCompleted(pLocalReducer)) {
      break;
    }

#ifdef _DEBUG_VIEW
    printf("chosen data in pTree[0] = %d\n", pTree->pNode[0].index);
#endif
    assert((pTree->pNode[0].index < pLocalReducer->numOfBuffer) && (pTree->pNode[0].index >= 0) &&
           tmpBuffer->numOfElems == 0);

    // chosen from loser tree
    SLocalDataSource *pOneDataSrc = pLocalReducer->pLocalDataSrc[pTree->pNode[0].index];

    tColModelAppend(pModel, tmpBuffer, pOneDataSrc->filePage.data, pOneDataSrc->rowIdx, 1,
                    pOneDataSrc->pMemBuffer->pColumnModel->capacity);

#if defined(_DEBUG_VIEW)
    printf("chosen row:\t");
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pQueryInfo);

    tColModelDisplayEx(pModel, tmpBuffer->data, tmpBuffer->numOfElems, pModel->capacity, colInfo);
#endif

    if (pLocalReducer->discard) {
      assert(pLocalReducer->hasUnprocessedRow == false);

      /* current record belongs to the same group of previous record, need to discard it */
      if (isSameGroup(pCmd, pLocalReducer, pLocalReducer->discardData->data, tmpBuffer)) {
        tmpBuffer->numOfElems = 0;
        pOneDataSrc->rowIdx += 1;

        adjustLoserTreeFromNewData(pLocalReducer, pOneDataSrc, pTree);

        // all inputs are exhausted, abort current process
        if (isAllSourcesCompleted(pLocalReducer)) {
          break;
        }

        // data belongs to the same group needs to be discarded
        continue;
      } else {
        pLocalReducer->discard = false;
        pLocalReducer->discardData->numOfElems = 0;

        if (saveGroupResultInfo(pSql)) {
          pLocalReducer->status = TSC_LOCALREDUCE_READY;
          return TSDB_CODE_SUCCESS;
        }

        resetEnvForNewResultset(pRes, pCmd, pLocalReducer);
      }
    }

    if (pLocalReducer->hasPrevRow) {
      if (needToMerge(pQueryInfo, pLocalReducer, tmpBuffer)) {
        // belong to the group of the previous row, continue process it
        doExecuteSecondaryMerge(pCmd, pLocalReducer, false);

        // copy to buffer
        savePreviousRow(pLocalReducer, tmpBuffer);
      } else {
        /*
         * current row does not belong to the group of previous row.
         * so the processing of previous group is completed.
         */
        int32_t numOfRes = finalizeRes(pQueryInfo, pLocalReducer);

        bool       sameGroup = isSameGroup(pCmd, pLocalReducer, pLocalReducer->prevRowOfInput, tmpBuffer);
        tFilePage *pResBuf = pLocalReducer->pResultBuf;

        /*
         * if the previous group does NOT generate any result (pResBuf->numOfElems == 0),
         * continue to process results instead of return results.
         */
        if ((!sameGroup && pResBuf->numOfElems > 0) || (pResBuf->numOfElems == pLocalReducer->resColModel->capacity)) {
          // does not belong to the same group
          bool notSkipped = doGenerateFinalResults(pSql, pLocalReducer, !sameGroup);

          // this row needs to discard, since it belongs to the group of previous
          if (pLocalReducer->discard && sameGroup) {
            pLocalReducer->hasUnprocessedRow = false;
            tmpBuffer->numOfElems = 0;
          } else {
            // current row does not belongs to the previous group, so it is not be handled yet.
            pLocalReducer->hasUnprocessedRow = true;
          }

          resetOutputBuf(pQueryInfo, pLocalReducer);
          pOneDataSrc->rowIdx += 1;

          // here we do not check the return value
          adjustLoserTreeFromNewData(pLocalReducer, pOneDataSrc, pTree);
          assert(pLocalReducer->status == TSC_LOCALREDUCE_IN_PROGRESS);

          if (pRes->numOfRows == 0) {
            handleUnprocessedRow(pCmd, pLocalReducer, tmpBuffer);

            if (!sameGroup) {
              /*
               * previous group is done, prepare for the next group
               * If previous group is not skipped, keep it in pRes->numOfGroups
               */
              if (notSkipped && saveGroupResultInfo(pSql)) {
                pLocalReducer->status = TSC_LOCALREDUCE_READY;
                return TSDB_CODE_SUCCESS;
              }

              resetEnvForNewResultset(pRes, pCmd, pLocalReducer);
            }
          } else {
            /*
             * if next record belongs to a new group, we do not handle this record here.
             * We start the process in a new round.
             */
            if (sameGroup) {
              handleUnprocessedRow(pCmd, pLocalReducer, tmpBuffer);
            }
          }

          // current group has no result,
          if (pRes->numOfRows == 0) {
            continue;
          } else {
            pLocalReducer->status = TSC_LOCALREDUCE_READY;  // set the flag, taos_free_result can release this result.
            return TSDB_CODE_SUCCESS;
          }
        } else {  // result buffer is not full
          doProcessResultInNextWindow(pSql, numOfRes);
          savePreviousRow(pLocalReducer, tmpBuffer);
        }
      }
    } else {
      doExecuteSecondaryMerge(pCmd, pLocalReducer, true);
      savePreviousRow(pLocalReducer, tmpBuffer);  // copy the processed row to buffer
    }

    pOneDataSrc->rowIdx += 1;
    adjustLoserTreeFromNewData(pLocalReducer, pOneDataSrc, pTree);
  }

  if (pLocalReducer->hasPrevRow) {
    finalizeRes(pQueryInfo, pLocalReducer);
  }

  if (pLocalReducer->pResultBuf->numOfElems) {
    doGenerateFinalResults(pSql, pLocalReducer, true);
  }

  assert(pLocalReducer->status == TSC_LOCALREDUCE_IN_PROGRESS && pRes->row == 0);
  pLocalReducer->status = TSC_LOCALREDUCE_READY;  // set the flag, taos_free_result can release this result.

  return TSDB_CODE_SUCCESS;
}

void tscInitResObjForLocalQuery(SSqlObj *pObj, int32_t numOfRes, int32_t rowLen) {
  SSqlRes *pRes = &pObj->res;
  if (pRes->pLocalReducer != NULL) {
    tscDestroyLocalReducer(pObj);
  }

  pRes->qhandle = 1;  // hack to pass the safety check in fetch_row function
  pRes->numOfRows = 0;
  pRes->row = 0;

  pRes->rspType = 0;  // used as a flag to denote if taos_retrieved() has been called yet
  pRes->pLocalReducer = (SLocalReducer *)calloc(1, sizeof(SLocalReducer));

  /*
   * we need one additional byte space
   * the sprintf function needs one additional space to put '\0' at the end of string
   */
  size_t allocSize = numOfRes * rowLen + sizeof(tFilePage) + 1;
  pRes->pLocalReducer->pResultBuf = (tFilePage *)calloc(1, allocSize);

  pRes->pLocalReducer->pResultBuf->numOfElems = numOfRes;
  pRes->data = pRes->pLocalReducer->pResultBuf->data;
}
