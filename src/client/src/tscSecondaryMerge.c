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

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

#include "tlosertree.h"
#include "tlosertree.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tutil.h"

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

  if (pParam->groupOrderType == TSQL_SO_DESC) {  // desc
    return compare_d(pDesc, pParam->numOfElems, pLocalData[pLeftIdx]->rowIdx, pLocalData[pLeftIdx]->filePage.data,
                     pParam->numOfElems, pLocalData[pRightIdx]->rowIdx, pLocalData[pRightIdx]->filePage.data);
  } else {
    return compare_a(pDesc, pParam->numOfElems, pLocalData[pLeftIdx]->rowIdx, pLocalData[pLeftIdx]->filePage.data,
                     pParam->numOfElems, pLocalData[pRightIdx]->rowIdx, pLocalData[pRightIdx]->filePage.data);
  }
}

static void tscInitSqlContext(SSqlCmd *pCmd, SSqlRes *pRes, SLocalReducer *pReducer, tOrderDescriptor *pDesc) {
  /*
   * the fields and offset attributes in pCmd and pModel may be different due to
   * merge requirement. So, the final result in STscRes structure is formatted in accordance with the pCmd object.
   */
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SQLFunctionCtx *pCtx = &pReducer->pCtx[i];

    pCtx->aOutputBuf = pReducer->pResultBuf->data + tscFieldInfoGetOffset(pCmd, i) * pReducer->resColModel->maxCapacity;

    pCtx->order = pCmd->order.order;
    pCtx->aInputElemBuf =
        pReducer->pTempBuffer->data + pDesc->pSchema->colOffset[i];  // input buffer hold only one point data

    // input data format comes from pModel
    pCtx->inputType = pDesc->pSchema->pFields[i].type;
    pCtx->inputBytes = pDesc->pSchema->pFields[i].bytes;

    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);
    // output data format yet comes from pCmd.
    pCtx->outputBytes = pField->bytes;
    pCtx->outputType = pField->type;
    pCtx->numOfOutputElems = 0;

    pCtx->numOfIteratedElems = 0;
    pCtx->startOffset = 0;
    pCtx->size = 1;
    pCtx->hasNullValue = true;
    pCtx->currentStage = SECONDARY_STAGE_MERGE;

    pRes->bytes[i] = pField->bytes;

    int32_t sqlFunction = tscSqlExprGet(pCmd, i)->sqlFuncId;
    if (sqlFunction == TSDB_FUNC_TOP_DST || sqlFunction == TSDB_FUNC_BOTTOM_DST) {
      /* for top_dst/bottom_dst function, the output of timestamp is the first column */
      pCtx->ptsOutputBuf = pReducer->pCtx[0].aOutputBuf;

      pCtx->param[2].i64Key = pCmd->order.order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i64Key = sqlFunction;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i64Key = pCmd->order.orderColId;
    }
  }
}

/*
 * todo release allocated memory process with async process
 */
void tscCreateLocalReducer(tExtMemBuffer **pMemBuffer, int32_t numOfBuffer, tOrderDescriptor *pDesc,
                           tColModel *finalmodel, SSqlCmd *pCmd, SSqlRes *pRes) {
  // offset of cmd in SSqlObj structure
  char *pSqlObjAddr = (char *)pCmd - offsetof(SSqlObj, cmd);

  if (pMemBuffer == NULL || pDesc->pSchema == NULL) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);

    tscError("%p no local buffer or intermediate result format model", pSqlObjAddr);
    pRes->code = TSDB_CODE_APP_ERROR;
    return;
  }

  int32_t numOfFlush = 0;
  for (int32_t i = 0; i < numOfBuffer; ++i) {
    int32_t len = pMemBuffer[i]->fileMeta.flushoutData.nLength;
    if (len == 0) {
      tscTrace("%p no data retrieved from orderOfVnode:%d", pSqlObjAddr, i + 1);
      continue;
    }

    numOfFlush += len;
  }

  if (numOfFlush == 0 || numOfBuffer == 0) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);
    tscTrace("%p retrieved no data", pSqlObjAddr);

    return;
  }

  if (pDesc->pSchema->maxCapacity >= pMemBuffer[0]->nPageSize) {
    tscError("%p Invalid value of buffer capacity %d and page size %d ", pSqlObjAddr, pDesc->pSchema->maxCapacity,
             pMemBuffer[0]->nPageSize);

    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, numOfBuffer);
    pRes->code = TSDB_CODE_APP_ERROR;
    return;
  }

  size_t         nReducerSize = sizeof(SLocalReducer) + sizeof(void *) * numOfFlush;
  SLocalReducer *pReducer = (SLocalReducer *)calloc(1, nReducerSize);
  if (pReducer == NULL) {
    tscError("%p failed to create merge structure", pSqlObjAddr);

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
  pTrace("%p the number of merged leaves is: %d", pSqlObjAddr, pReducer->numOfBuffer);

  int32_t idx = 0;
  for (int32_t i = 0; i < numOfBuffer; ++i) {
    int32_t numOfFlushoutInFile = pMemBuffer[i]->fileMeta.flushoutData.nLength;

    for (int32_t j = 0; j < numOfFlushoutInFile; ++j) {
      SLocalDataSource *pDS = (SLocalDataSource *)malloc(sizeof(SLocalDataSource) + pMemBuffer[0]->nPageSize);
      if (pDS == NULL) {
        tscError("%p failed to create merge structure", pSqlObjAddr);
        pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        return;
      }
      pReducer->pLocalDataSrc[idx] = pDS;

      pDS->pMemBuffer = pMemBuffer[i];
      pDS->flushoutIdx = j;
      pDS->filePage.numOfElems = 0;
      pDS->pageId = 0;
      pDS->rowIdx = 0;

      tscTrace("%p load data from disk into memory, orderOfVnode:%d, total:%d", pSqlObjAddr, i + 1, idx + 1);
      tExtMemBufferLoadData(pMemBuffer[i], &(pDS->filePage), j, 0);
#ifdef _DEBUG_VIEW
      printf("load data page into mem for build loser tree: %ld rows\n", pDS->filePage.numOfElems);
      SSrcColumnInfo colInfo[256] = {0};
      tscGetSrcColumnInfo(colInfo, pCmd);

      tColModelDisplayEx(pDesc->pSchema, pDS->filePage.data, pDS->filePage.numOfElems, pMemBuffer[0]->numOfElemsPerPage,
                         colInfo);
#endif
      if (pDS->filePage.numOfElems == 0) {  // no data in this flush
        tscTrace("%p flush data is empty, ignore %d flush record", pSqlObjAddr, idx);
        tfree(pDS);
        continue;
      }
      idx += 1;
    }
  }
  assert(idx >= pReducer->numOfBuffer);
  if (idx == 0) {
    return;
  }

  pReducer->numOfBuffer = idx;

  SCompareParam *param = malloc(sizeof(SCompareParam));
  param->pLocalData = pReducer->pLocalDataSrc;
  param->pDesc = pReducer->pDesc;
  param->numOfElems = pReducer->pLocalDataSrc[0]->pMemBuffer->numOfElemsPerPage;
  param->groupOrderType = pCmd->groupbyExpr.orderType;

  pRes->code = tLoserTreeCreate(&pReducer->pLoserTree, pReducer->numOfBuffer, param, treeComparator);
  if (pReducer->pLoserTree == NULL || pRes->code != 0) {
    return;
  }

  // the input data format follows the old format, but output in a new format.
  // so, all the input must be parsed as old format
  pReducer->pCtx = (SQLFunctionCtx *)calloc(pCmd->fieldsInfo.numOfOutputCols, sizeof(SQLFunctionCtx));

  pReducer->rowSize = pMemBuffer[0]->nElemSize;

  tscRestoreSQLFunctionForMetricQuery(pCmd);
  tscFieldInfoCalOffset(pCmd);

  if (pReducer->rowSize > pMemBuffer[0]->nPageSize) {
    assert(false);  // todo fixed row size is larger than the minimum page size;
  }

  pReducer->hasPrevRow = false;
  pReducer->hasUnprocessedRow = false;

  pReducer->prevRowOfInput = (char *)calloc(1, pReducer->rowSize);
  if (pReducer->prevRowOfInput == 0) {
    // todo release previously allocated memory
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return;
  }

  /* used to keep the latest input row */
  pReducer->pTempBuffer = (tFilePage *)calloc(1, pReducer->rowSize + sizeof(tFilePage));

  pReducer->discardData = (tFilePage *)calloc(1, pReducer->rowSize + sizeof(tFilePage));
  pReducer->discard = false;

  pReducer->nResultBufSize = pMemBuffer[0]->nPageSize * 16;
  pReducer->pResultBuf = (tFilePage *)calloc(1, pReducer->nResultBufSize + sizeof(tFilePage));

  int32_t finalRowLength = tscGetResRowLength(pCmd);
  pReducer->resColModel = finalmodel;
  pReducer->resColModel->maxCapacity = pReducer->nResultBufSize / finalRowLength;
  assert(finalRowLength <= pReducer->rowSize);

  pReducer->pBufForInterpo = calloc(1, pReducer->nResultBufSize);

  if (pReducer->pTempBuffer == NULL || pReducer->pResultBuf == NULL || pReducer->pBufForInterpo == NULL) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return;
  }

  pReducer->pTempBuffer->numOfElems = 0;

  tscCreateResPointerInfo(pCmd, pRes);
  tscInitSqlContext(pCmd, pRes, pReducer, pDesc);

  /* we change the maxCapacity of schema to denote that there is only one row in temp buffer */
  pReducer->pDesc->pSchema->maxCapacity = 1;
  pReducer->offset = pCmd->limit.offset;

  pRes->pLocalReducer = pReducer;
  pRes->numOfGroups = 0;

  int64_t stime = (pCmd->stime < pCmd->etime) ? pCmd->stime : pCmd->etime;
  int64_t revisedSTime = taosGetIntervalStartTimestamp(stime, pCmd->nAggTimeInterval, pCmd->intervalTimeUnit);

  SInterpolationInfo *pInterpoInfo = &pReducer->interpolationInfo;
  taosInitInterpoInfo(pInterpoInfo, pCmd->order.order, revisedSTime, pCmd->groupbyExpr.numOfGroupbyCols,
                      pReducer->rowSize);

  int32_t startIndex = pCmd->fieldsInfo.numOfOutputCols - pCmd->groupbyExpr.numOfGroupbyCols;

  if (pCmd->groupbyExpr.numOfGroupbyCols > 0) {
    pInterpoInfo->pTags[0] = (char *)pInterpoInfo->pTags + POINTER_BYTES * pCmd->groupbyExpr.numOfGroupbyCols;
    for (int32_t i = 1; i < pCmd->groupbyExpr.numOfGroupbyCols; ++i) {
      pInterpoInfo->pTags[i] = pReducer->resColModel->pFields[startIndex + i - 1].bytes + pInterpoInfo->pTags[i - 1];
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

  assert(pPage->numOfElems <= pDesc->pSchema->maxCapacity);

  // sort before flush to disk, the data must be consecutively put on tFilePage.
  if (pDesc->orderIdx.numOfOrderedCols > 0) {
    tColDataQSort(pDesc, pPage->numOfElems, 0, pPage->numOfElems - 1, pPage->data, orderType);
  }

#ifdef _DEBUG_VIEW
  printf("%ld rows data flushed to disk after been sorted:\n", pPage->numOfElems);
  tColModelDisplay(pDesc->pSchema, pPage->data, pPage->numOfElems, pPage->numOfElems);
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
  if (pPage->numOfElems + numOfRows <= pDesc->pSchema->maxCapacity) {
    tColModelAppend(pDesc->pSchema, pPage, data, 0, numOfRows, numOfRows);
    return 0;
  }

  tColModel *pModel = pDesc->pSchema;

  int32_t numOfRemainEntries = pDesc->pSchema->maxCapacity - pPage->numOfElems;
  tColModelAppend(pModel, pPage, data, 0, numOfRemainEntries, numOfRows);

  /* current buffer is full, need to flushed to disk */
  assert(pPage->numOfElems == pDesc->pSchema->maxCapacity);
  int32_t ret = tscFlushTmpBuffer(pMemoryBuf, pDesc, pPage, orderType);
  if (ret != 0) {
    return -1;
  }

  int32_t remain = numOfRows - numOfRemainEntries;

  while (remain > 0) {
    int32_t numOfWriteElems = 0;
    if (remain > pModel->maxCapacity) {
      numOfWriteElems = pModel->maxCapacity;
    } else {
      numOfWriteElems = remain;
    }

    tColModelAppend(pModel, pPage, data, numOfRows - remain, numOfWriteElems, numOfRows);

    if (pPage->numOfElems == pModel->maxCapacity) {
      int32_t ret = tscFlushTmpBuffer(pMemoryBuf, pDesc, pPage, orderType);
      if (ret != 0) {
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

  // there is no more result, so we release all allocated resource
  SLocalReducer *pLocalReducer =
      (SLocalReducer *)__sync_val_compare_and_swap_64(&pRes->pLocalReducer, pRes->pLocalReducer, 0);
  if (pLocalReducer != NULL) {
    int32_t status = 0;
    while ((status = __sync_val_compare_and_swap_32(&pLocalReducer->status, TSC_LOCALREDUCE_READY,
                                                    TSC_LOCALREDUCE_TOBE_FREED)) == TSC_LOCALREDUCE_IN_PROGRESS) {
      taosMsleep(100);
      tscTrace("%p waiting for delete procedure, status: %d", pSql, status);
    }

    tfree(pLocalReducer->interpolationInfo.prevValues);
    tfree(pLocalReducer->interpolationInfo.pTags);

    tfree(pLocalReducer->pCtx);
    tfree(pLocalReducer->prevRowOfInput);

    tfree(pLocalReducer->pTempBuffer);
    tfree(pLocalReducer->pResultBuf);

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

static int32_t createOrderDescriptor(tOrderDescriptor **pOrderDesc, SSqlCmd *pCmd, tColModel *pModel) {
  int32_t numOfGroupByCols = 0;
  if (pCmd->groupbyExpr.numOfGroupbyCols > 0) {
    numOfGroupByCols = pCmd->groupbyExpr.numOfGroupbyCols;
  }

  // primary timestamp column is involved in final result
  if (pCmd->nAggTimeInterval != 0) {
    numOfGroupByCols++;
  }

  int32_t *orderIdx = (int32_t *)calloc(numOfGroupByCols, sizeof(int32_t));
  if (orderIdx == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  if (numOfGroupByCols > 0) {
    int32_t startCols = pCmd->fieldsInfo.numOfOutputCols - pCmd->groupbyExpr.numOfGroupbyCols;

    // tags value locate at the last columns
    for (int32_t i = 0; i < pCmd->groupbyExpr.numOfGroupbyCols; ++i) {
      orderIdx[i] = startCols++;
    }

    if (pCmd->nAggTimeInterval != 0) {
      //the first column is the timestamp, handles queries like "interval(10m) group by tags"
      orderIdx[numOfGroupByCols - 1] = PRIMARYKEY_TIMESTAMP_COL_INDEX;
    }
  }

  *pOrderDesc = tOrderDesCreate(orderIdx, numOfGroupByCols, pModel, pCmd->order.order);
  tfree(orderIdx);

  if (*pOrderDesc == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

bool isSameGroup(SSqlCmd *pCmd, SLocalReducer *pReducer, char *pPrev, tFilePage *tmpBuffer) {
  int16_t functionId = tscSqlExprGet(pCmd, 0)->sqlFuncId;

  // disable merge procedure for column projection query
  if (functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_ARITHM) {
    return false;
  }

  tOrderDescriptor *pOrderDesc = pReducer->pDesc;
  int32_t           numOfCols = pOrderDesc->orderIdx.numOfOrderedCols;

  // no group by columns, all data belongs to one group
  if (numOfCols <= 0) {
    return true;
  }

  if (pOrderDesc->orderIdx.pData[numOfCols - 1] == PRIMARYKEY_TIMESTAMP_COL_INDEX) {  //<= 0
    // super table interval query
    assert(pCmd->nAggTimeInterval > 0);
    pOrderDesc->orderIdx.numOfOrderedCols -= 1;
  } else {  // simple group by query
    assert(pCmd->nAggTimeInterval == 0);
  }

  // only one row exists
  int32_t ret = compare_a(pOrderDesc, 1, 0, pPrev, 1, 0, tmpBuffer->data);
  pOrderDesc->orderIdx.numOfOrderedCols = numOfCols;

  return (ret == 0);
}

int32_t tscLocalReducerEnvCreate(SSqlObj *pSql, tExtMemBuffer ***pMemBuffer, tOrderDescriptor **pOrderDesc,
                                 tColModel **pFinalModel, uint32_t nBufferSizes) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SSchema *  pSchema = NULL;
  tColModel *pModel = NULL;
  *pFinalModel = NULL;

  (*pMemBuffer) = (tExtMemBuffer **)malloc(POINTER_BYTES * pCmd->pMetricMeta->numOfVnodes);
  if (*pMemBuffer == NULL) {
    tscError("%p failed to allocate memory", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;
  }

  pSchema = (SSchema *)calloc(1, sizeof(SSchema) * pCmd->fieldsInfo.numOfOutputCols);
  if (pSchema == NULL) {
    tscError("%p failed to allocate memory", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;
  }

  int32_t rlen = 0;
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, i);

    pSchema[i].bytes = pExpr->resBytes;
    pSchema[i].type = pExpr->resType;

    rlen += pExpr->resBytes;
  }

  int32_t capacity = nBufferSizes / rlen;
  pModel = tColModelCreate(pSchema, pCmd->fieldsInfo.numOfOutputCols, capacity);

  for (int32_t i = 0; i < pCmd->pMetricMeta->numOfVnodes; ++i) {
    char tmpPath[512] = {0};
    getExtTmpfilePath("/tv_bf_db_%lld_%lld_%d.d", taosGetPthreadId(), i, 0, tmpPath);
    tscTrace("%p create tmp file:%s", pSql, tmpPath);

    tExtMemBufferCreate(&(*pMemBuffer)[i], nBufferSizes, rlen, tmpPath, pModel);
    (*pMemBuffer)[i]->flushModel = MULTIPLE_APPEND_MODEL;
  }

  if (createOrderDescriptor(pOrderDesc, pCmd, pModel) != TSDB_CODE_SUCCESS) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;
  }

  memset(pSchema, 0, sizeof(SSchema) * pCmd->fieldsInfo.numOfOutputCols);
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

    pSchema[i].type = pField->type;
    pSchema[i].bytes = pField->bytes;
    strcpy(pSchema[i].name, pField->name);
  }

  *pFinalModel = tColModelCreate(pSchema, pCmd->fieldsInfo.numOfOutputCols, capacity);
  tfree(pSchema);

  return TSDB_CODE_SUCCESS;
}

/**
 * @param pMemBuffer
 * @param pDesc
 * @param pFinalModel
 * @param numOfVnodes
 */
void tscLocalReducerEnvDestroy(tExtMemBuffer **pMemBuffer, tOrderDescriptor *pDesc, tColModel *pFinalModel,
                               int32_t numOfVnodes) {
  tColModelDestroy(pFinalModel);
  tOrderDescDestroy(pDesc);
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    tExtMemBufferDestroy(&pMemBuffer[i]);
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
    tColModelDisplay(pOneInterDataSrc->pMemBuffer->pColModel, pOneInterDataSrc->filePage.data,
                     pOneInterDataSrc->filePage.numOfElems, pOneInterDataSrc->pMemBuffer->pColModel->maxCapacity);
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

void savePrevRecordAndSetupInterpoInfo(
    SLocalReducer *pLocalReducer, SSqlCmd *pCmd,
    SInterpolationInfo
        *pInterpoInfo) {  // discard following dataset in the same group and reset the interpolation information
  int64_t stime = (pCmd->stime < pCmd->etime) ? pCmd->stime : pCmd->etime;
  int64_t revisedSTime = taosGetIntervalStartTimestamp(stime, pCmd->nAggTimeInterval, pCmd->intervalTimeUnit);

  taosInitInterpoInfo(pInterpoInfo, pCmd->order.order, revisedSTime, pCmd->groupbyExpr.numOfGroupbyCols,
                      pLocalReducer->rowSize);

  pLocalReducer->discard = true;
  pLocalReducer->discardData->numOfElems = 0;

  tColModel *pModel = pLocalReducer->pDesc->pSchema;
  tColModelAppend(pModel, pLocalReducer->discardData, pLocalReducer->prevRowOfInput, 0, 1, 1);
}

// todo merge with following function
static void reversedCopyResultToDstBuf(SSqlCmd *pCmd, SSqlRes *pRes, tFilePage *pFinalDataPage) {
  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

    int32_t offset = tscFieldInfoGetOffset(pCmd, i);
    char *  src = pFinalDataPage->data + (pRes->numOfRows - 1) * pField->bytes + pRes->numOfRows * offset;
    char *  dst = pRes->data + pRes->numOfRows * offset;

    for (int32_t j = 0; j < pRes->numOfRows; ++j) {
      memcpy(dst, src, (size_t)pField->bytes);
      dst += pField->bytes;
      src -= pField->bytes;
    }
  }
}

static void reversedCopyFromInterpolationToDstBuf(SSqlCmd *pCmd, SSqlRes *pRes, tFilePage **pResPages,
                                                  SLocalReducer *pLocalReducer) {
  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

    int32_t offset = tscFieldInfoGetOffset(pCmd, i);
    assert(offset == pLocalReducer->resColModel->colOffset[i]);

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
  SSqlCmd *  pCmd = &pSql->cmd;
  SSqlRes *  pRes = &pSql->res;
  tFilePage *pFinalDataPage = pLocalReducer->pResultBuf;

  if (pRes->pLocalReducer != pLocalReducer) {
    /*
     * Release the SSqlObj is called, and it is int destroying function invoked by other thread.
     * However, the other thread will WAIT until current process fully completes.
     * Since the flag of release struct is set by doLocalReduce function
     */
    assert(pRes->pLocalReducer == NULL);
  }

  if (pLocalReducer->pFinalRes == NULL) {
    pLocalReducer->pFinalRes = malloc(pLocalReducer->rowSize * pLocalReducer->resColModel->maxCapacity);
  }

  if (pCmd->nAggTimeInterval == 0 || pCmd->interpoType == TSDB_INTERPO_NONE) {
    // no interval query, no interpolation
    pRes->data = pLocalReducer->pFinalRes;
    pRes->numOfRows = pFinalDataPage->numOfElems;
    pRes->numOfTotal += pRes->numOfRows;

    if (pCmd->limit.offset > 0) {
      if (pCmd->limit.offset < pRes->numOfRows) {
        int32_t prevSize = pFinalDataPage->numOfElems;
        tColModelErase(pLocalReducer->resColModel, pFinalDataPage, prevSize, 0, pCmd->limit.offset - 1);

        /* remove the hole in column model */
        tColModelCompact(pLocalReducer->resColModel, pFinalDataPage, prevSize);

        pRes->numOfRows -= pCmd->limit.offset;
        pRes->numOfTotal -= pCmd->limit.offset;
        pCmd->limit.offset = 0;
      } else {
        pCmd->limit.offset -= pRes->numOfRows;
        pRes->numOfRows = 0;
        pRes->numOfTotal = 0;
      }
    }

    if (pCmd->limit.limit >= 0 && pRes->numOfTotal > pCmd->limit.limit) {
      /* impose the limitation of output rows on the final result */
      int32_t prevSize = pFinalDataPage->numOfElems;
      int32_t overFlow = pRes->numOfTotal - pCmd->limit.limit;

      assert(overFlow < pRes->numOfRows);

      pRes->numOfTotal = pCmd->limit.limit;
      pRes->numOfRows -= overFlow;
      pFinalDataPage->numOfElems -= overFlow;

      tColModelCompact(pLocalReducer->resColModel, pFinalDataPage, prevSize);

      /* set remain data to be discarded, and reset the interpolation information */
      savePrevRecordAndSetupInterpoInfo(pLocalReducer, pCmd, &pLocalReducer->interpolationInfo);
    }

    int32_t rowSize = tscGetResRowLength(pCmd);
    // handle the descend order output
    if (pCmd->order.order == TSQL_SO_ASC) {
      memcpy(pRes->data, pFinalDataPage->data, pRes->numOfRows * rowSize);
    } else {
      reversedCopyResultToDstBuf(pCmd, pRes, pFinalDataPage);
    }

    pFinalDataPage->numOfElems = 0;
    return;
  }

  int64_t *           pPrimaryKeys = (int64_t *)pLocalReducer->pBufForInterpo;
  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;

  int64_t actualETime = (pCmd->stime < pCmd->etime) ? pCmd->etime : pCmd->stime;

  tFilePage **pResPages = malloc(POINTER_BYTES * pCmd->fieldsInfo.numOfOutputCols);
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);
    pResPages[i] = calloc(1, sizeof(tFilePage) + pField->bytes * pLocalReducer->resColModel->maxCapacity);
  }

  char **  srcData = (char **)malloc((POINTER_BYTES + sizeof(int32_t)) * pCmd->fieldsInfo.numOfOutputCols);
  int32_t *functions = (int32_t *)((char *)srcData + pCmd->fieldsInfo.numOfOutputCols * sizeof(void *));

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    srcData[i] = pLocalReducer->pBufForInterpo + tscFieldInfoGetOffset(pCmd, i) * pInterpoInfo->numOfRawDataInRows;
    functions[i] = tscSqlExprGet(pCmd, i)->sqlFuncId;
  }

  while (1) {
    int32_t remains = taosNumOfRemainPoints(pInterpoInfo);
    TSKEY etime = taosGetRevisedEndKey(actualETime, pCmd->order.order, pCmd->nAggTimeInterval, pCmd->intervalTimeUnit);
    int32_t nrows = taosGetNumOfResultWithInterpo(pInterpoInfo, pPrimaryKeys, remains, pCmd->nAggTimeInterval, etime,
                                                  pLocalReducer->resColModel->maxCapacity);

    int32_t newRows = taosDoInterpoResult(pInterpoInfo, pCmd->interpoType, pResPages, remains, nrows,
                                          pCmd->nAggTimeInterval, pPrimaryKeys, pLocalReducer->resColModel, srcData,
                                          pCmd->defaultVal, functions, pLocalReducer->resColModel->maxCapacity);
    assert(newRows <= nrows);

    if (pCmd->limit.offset < newRows) {
      newRows -= pCmd->limit.offset;

      if (pCmd->limit.offset > 0) {
        for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
          TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);
          memmove(pResPages[i]->data, pResPages[i]->data + pField->bytes * pCmd->limit.offset, newRows * pField->bytes);
        }
      }

      pRes->data = pLocalReducer->pFinalRes;
      pRes->numOfRows = newRows;
      pRes->numOfTotal += newRows;

      pCmd->limit.offset = 0;
      break;
    } else {
      pCmd->limit.offset -= newRows;
      pRes->numOfRows = 0;

      int32_t rpoints = taosNumOfRemainPoints(pInterpoInfo);
      if (rpoints <= 0) {
        if (!doneOutput) {
          /* reduce procedure is not completed, but current results for interpolation are exhausted */
          break;
        }

        /* all output for current group are completed */
        int32_t totalRemainRows =
            taosGetNumOfResWithoutLimit(pInterpoInfo, pPrimaryKeys, rpoints, pCmd->nAggTimeInterval, actualETime);
        if (totalRemainRows <= 0) {
          break;
        }
      }
    }
  }

  if (pRes->numOfRows > 0) {
    if (pCmd->limit.limit >= 0 && pRes->numOfTotal > pCmd->limit.limit) {
      int32_t overFlow = pRes->numOfTotal - pCmd->limit.limit;
      pRes->numOfRows -= overFlow;

      assert(pRes->numOfRows >= 0);

      pRes->numOfTotal = pCmd->limit.limit;
      pFinalDataPage->numOfElems -= overFlow;

      /* set remain data to be discarded, and reset the interpolation information */
      savePrevRecordAndSetupInterpoInfo(pLocalReducer, pCmd, pInterpoInfo);
    }

    if (pCmd->order.order == TSQL_SO_ASC) {
      for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
        TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

        memcpy(pRes->data + pLocalReducer->resColModel->colOffset[i] * pRes->numOfRows, pResPages[i]->data,
               pField->bytes * pRes->numOfRows);
      }
    } else {
      reversedCopyFromInterpolationToDstBuf(pCmd, pRes, pResPages, pLocalReducer);
    }
  }

  pFinalDataPage->numOfElems = 0;
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    tfree(pResPages[i]);
  }
  tfree(pResPages);

  free(srcData);
}

static void savePreviousRow(SLocalReducer *pLocalReducer, tFilePage *tmpBuffer) {
  tColModel *pColModel = pLocalReducer->pDesc->pSchema;
  assert(pColModel->maxCapacity == 1 && tmpBuffer->numOfElems == 1);

  // copy to previous temp buffer
  for (int32_t i = 0; i < pLocalReducer->pDesc->pSchema->numOfCols; ++i) {
    memcpy(pLocalReducer->prevRowOfInput + pColModel->colOffset[i], tmpBuffer->data + pColModel->colOffset[i],
           pColModel->pFields[i].bytes);
  }

  tmpBuffer->numOfElems = 0;
  pLocalReducer->hasPrevRow = true;
}

static void handleUnprocessedRow(SLocalReducer *pLocalReducer, SSqlCmd *pCmd, tFilePage *tmpBuffer) {
  if (pLocalReducer->hasUnprocessedRow) {
    for (int32_t j = 0; j < pCmd->fieldsInfo.numOfOutputCols; ++j) {
      SSqlExpr *pExpr = tscSqlExprGet(pCmd, j);

      tVariantAssign(&pLocalReducer->pCtx[j].param[0], &pExpr->param[0]);
      aAggs[pExpr->sqlFuncId].init(&pLocalReducer->pCtx[j]);

      pLocalReducer->pCtx[j].currentStage = SECONDARY_STAGE_MERGE;
      pLocalReducer->pCtx[j].numOfIteratedElems = 0;
      aAggs[pExpr->sqlFuncId].distSecondaryMergeFunc(&pLocalReducer->pCtx[j]);
    }

    pLocalReducer->hasUnprocessedRow = false;

    // copy to previous temp buffer
    savePreviousRow(pLocalReducer, tmpBuffer);
  }
}

static int64_t getNumOfResultLocal(SSqlCmd *pCmd, SQLFunctionCtx *pCtx) {
  int64_t maxOutput = 0;

  for (int32_t j = 0; j < pCmd->exprsInfo.numOfExprs; ++j) {
    int32_t functionId = tscSqlExprGet(pCmd, j)->sqlFuncId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    if (maxOutput < pCtx[j].numOfOutputElems) {
      maxOutput = pCtx[j].numOfOutputElems;
    }
  }
  return maxOutput;
}

/*
 * in handling the to/bottom query, which produce more than one rows result,
 * the tsdb_func_tags only fill the first row of results, the remain rows need to
 * filled with the same result, which is the tags, specified in group by clause
 */
static void fillMultiRowsOfTagsVal(SSqlCmd *pCmd, int32_t numOfRes, SLocalReducer *pLocalReducer) {
  int32_t startIndex = pCmd->fieldsInfo.numOfOutputCols - pCmd->groupbyExpr.numOfGroupbyCols;

  int32_t maxBufSize = 0;
  for (int32_t k = startIndex; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, k);
    if (maxBufSize < pExpr->resBytes) {
      maxBufSize = pExpr->resBytes;
    }
    assert(pExpr->sqlFuncId == TSDB_FUNC_TAG);
  }

  assert(maxBufSize >= 0);

  char *buf = malloc((size_t)maxBufSize);
  for (int32_t k = startIndex; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
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

int32_t finalizeRes(SSqlCmd *pCmd, SLocalReducer *pLocalReducer) {
  for (int32_t k = 0; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, k);
    aAggs[pExpr->sqlFuncId].xFinalize(&pLocalReducer->pCtx[k]);
  }

  pLocalReducer->hasPrevRow = false;

  int32_t numOfRes = (int32_t)getNumOfResultLocal(pCmd, pLocalReducer->pCtx);
  pLocalReducer->pResultBuf->numOfElems += numOfRes;

  fillMultiRowsOfTagsVal(pCmd, numOfRes, pLocalReducer);
  return numOfRes;
}

/*
 * points merge:
 * points are merged according to the sort info, which is tags columns and timestamp column.
 * In case of points without either tags columns or timestamp, such as
 * results generated by simple aggregation function, we merge them all into one points
 * *Exception*: column projection query, required no merge procedure
 */
bool needToMerge(SSqlCmd *pCmd, SLocalReducer *pLocalReducer, tFilePage *tmpBuffer) {
  int32_t ret = 0;  // merge all result by default
  int16_t functionId = tscSqlExprGet(pCmd, 0)->sqlFuncId;

  if (functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_ARITHM) {  // column projection query
    ret = 1;                                                            // disable merge procedure
  } else {
    tOrderDescriptor *pDesc = pLocalReducer->pDesc;
    if (pDesc->orderIdx.numOfOrderedCols > 0) {
      if (pDesc->tsOrder == TSQL_SO_ASC) {  // asc
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

static bool reachGroupResultLimit(SSqlCmd *pCmd, SSqlRes *pRes) {
  return (pRes->numOfGroups >= pCmd->glimit.limit && pCmd->glimit.limit >= 0);
}

static bool saveGroupResultInfo(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfGroups += 1;

  // the output group is limited by the glimit clause
  if (reachGroupResultLimit(pCmd, pRes)) {
    return true;
  }

  //    pRes->pGroupRec = realloc(pRes->pGroupRec, pRes->numOfGroups*sizeof(SResRec));
  //    pRes->pGroupRec[pRes->numOfGroups-1].numOfRows = pRes->numOfRows;
  //    pRes->pGroupRec[pRes->numOfGroups-1].numOfTotal = pRes->numOfTotal;

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
  SSqlCmd *  pCmd = &pSql->cmd;
  SSqlRes *  pRes = &pSql->res;
  tFilePage *pResBuf = pLocalReducer->pResultBuf;
  tColModel *pModel = pLocalReducer->resColModel;

  pRes->code = TSDB_CODE_SUCCESS;

  /*
   * ignore the output of the current group since this group is skipped by user
   * We set the numOfRows to be 0 and discard the possible remain results.
   */
  if (pCmd->glimit.offset > 0) {
    pRes->numOfRows = 0;
    pCmd->glimit.offset -= 1;
    pLocalReducer->discard = !noMoreCurrentGroupRes;
    return false;
  }

  tColModelCompact(pModel, pResBuf, pModel->maxCapacity);
  memcpy(pLocalReducer->pBufForInterpo, pResBuf->data, pLocalReducer->nResultBufSize);

#ifdef _DEBUG_VIEW
  printf("final result before interpo:\n");
  tColModelDisplay(pLocalReducer->resColModel, pLocalReducer->pBufForInterpo, pResBuf->numOfElems, pResBuf->numOfElems);
#endif

  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;
  int32_t             startIndex = pCmd->fieldsInfo.numOfOutputCols - pCmd->groupbyExpr.numOfGroupbyCols;

  for (int32_t i = 0; i < pCmd->groupbyExpr.numOfGroupbyCols; ++i) {
    memcpy(pInterpoInfo->pTags[i],
           pLocalReducer->pBufForInterpo + pModel->colOffset[startIndex + i] * pResBuf->numOfElems,
           pModel->pFields[startIndex + i].bytes);
  }

  taosInterpoSetStartInfo(&pLocalReducer->interpolationInfo, pResBuf->numOfElems, pCmd->interpoType);
  doInterpolateResult(pSql, pLocalReducer, noMoreCurrentGroupRes);

  return true;
}

void resetOutputBuf(SSqlCmd *pCmd, SLocalReducer *pLocalReducer) {  // reset output buffer to the beginning
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    pLocalReducer->pCtx[i].aOutputBuf =
        pLocalReducer->pResultBuf->data + tscFieldInfoGetOffset(pCmd, i) * pLocalReducer->resColModel->maxCapacity;
  }

  memset(pLocalReducer->pResultBuf, 0, pLocalReducer->nResultBufSize + sizeof(tFilePage));
}

static void resetEnvForNewResultset(SSqlRes *pRes, SSqlCmd *pCmd, SLocalReducer *pLocalReducer) {
  //In handling data in other groups, we need to reset the interpolation information for a new group data
  pRes->numOfRows = 0;
  pRes->numOfTotal = 0;
  pCmd->limit.offset = pLocalReducer->offset;

  if (pCmd->interpoType != TSDB_INTERPO_NONE) {
    /* for group result interpolation, do not return if not data is generated */
    int64_t stime = (pCmd->stime < pCmd->etime) ? pCmd->stime : pCmd->etime;
    int64_t newTime = taosGetIntervalStartTimestamp(stime, pCmd->nAggTimeInterval, pCmd->intervalTimeUnit);

    taosInitInterpoInfo(&pLocalReducer->interpolationInfo, pCmd->order.order, newTime,
                        pCmd->groupbyExpr.numOfGroupbyCols, pLocalReducer->rowSize);
  }
}

static bool isAllSourcesCompleted(SLocalReducer *pLocalReducer) {
  return (pLocalReducer->numOfBuffer == pLocalReducer->numOfCompleted);
}

static bool doInterpolationForCurrentGroup(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SLocalReducer *     pLocalReducer = pRes->pLocalReducer;
  SInterpolationInfo *pInterpoInfo = &pLocalReducer->interpolationInfo;

  if (taosHasRemainsDataForInterpolation(pInterpoInfo)) {
    assert(pCmd->interpoType != TSDB_INTERPO_NONE);

    tFilePage *pFinalDataBuf = pLocalReducer->pResultBuf;
    int64_t    etime = *(int64_t *)(pFinalDataBuf->data + TSDB_KEYSIZE * (pInterpoInfo->numOfRawDataInRows - 1));

    int32_t remain = taosNumOfRemainPoints(pInterpoInfo);
    TSKEY   ekey = taosGetRevisedEndKey(etime, pCmd->order.order, pCmd->nAggTimeInterval, pCmd->intervalTimeUnit);
    int32_t rows = taosGetNumOfResultWithInterpo(pInterpoInfo, (TSKEY *)pLocalReducer->pBufForInterpo, remain,
                                                 pCmd->nAggTimeInterval, ekey, pLocalReducer->resColModel->maxCapacity);
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

  if ((isAllSourcesCompleted(pLocalReducer) && !pLocalReducer->hasPrevRow) || pLocalReducer->pLocalDataSrc[0] == NULL ||
      prevGroupCompleted) {
    // if interpoType == TSDB_INTERPO_NONE, return directly
    if (pCmd->interpoType != TSDB_INTERPO_NONE) {
      int64_t etime = (pCmd->stime < pCmd->etime) ? pCmd->etime : pCmd->stime;

      etime = taosGetRevisedEndKey(etime, pCmd->order.order, pCmd->nAggTimeInterval, pCmd->intervalTimeUnit);
      int32_t rows = taosGetNumOfResultWithInterpo(pInterpoInfo, NULL, 0, pCmd->nAggTimeInterval, etime,
                                                   pLocalReducer->resColModel->maxCapacity);
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

static void doMergeWithPrevRows(SSqlObj *pSql, int32_t numOfRes) {
  SSqlCmd *      pCmd = &pSql->cmd;
  SSqlRes *      pRes = &pSql->res;
  SLocalReducer *pLocalReducer = pRes->pLocalReducer;

  for (int32_t k = 0; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, k);

    pLocalReducer->pCtx[k].aOutputBuf += pLocalReducer->pCtx[k].outputBytes * numOfRes;

    // set the correct output timestamp column position
    if (pExpr->sqlFuncId == TSDB_FUNC_TOP_DST || pExpr->sqlFuncId == TSDB_FUNC_BOTTOM_DST) {
      pLocalReducer->pCtx[k].ptsOutputBuf = ((char *)pLocalReducer->pCtx[k].ptsOutputBuf + TSDB_KEYSIZE * numOfRes);
    }

    /* set the parameters for the SQLFunctionCtx */
    tVariantAssign(&pLocalReducer->pCtx[k].param[0], &pExpr->param[0]);

    aAggs[pExpr->sqlFuncId].init(&pLocalReducer->pCtx[k]);
    pLocalReducer->pCtx[k].currentStage = SECONDARY_STAGE_MERGE;
    aAggs[pExpr->sqlFuncId].distSecondaryMergeFunc(&pLocalReducer->pCtx[k]);
  }
}

static void doExecuteSecondaryMerge(SSqlObj *pSql) {
  SSqlCmd *      pCmd = &pSql->cmd;
  SSqlRes *      pRes = &pSql->res;
  SLocalReducer *pLocalReducer = pRes->pLocalReducer;

  for (int32_t j = 0; j < pCmd->fieldsInfo.numOfOutputCols; ++j) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, j);

    tVariantAssign(&pLocalReducer->pCtx[j].param[0], &pExpr->param[0]);
    pLocalReducer->pCtx[j].numOfIteratedElems = 0;
    pLocalReducer->pCtx[j].currentStage = 0;

    aAggs[pExpr->sqlFuncId].init(&pLocalReducer->pCtx[j]);
    pLocalReducer->pCtx[j].currentStage = SECONDARY_STAGE_MERGE;

    aAggs[pExpr->sqlFuncId].distSecondaryMergeFunc(&pLocalReducer->pCtx[j]);
  }
}

int32_t tscLocalDoReduce(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pSql->signature != pSql || pRes == NULL || pRes->pLocalReducer == NULL) {  // all data has been processed
    tscTrace("%s call the drop local reducer", __FUNCTION__);

    tscDestroyLocalReducer(pSql);
    pRes->numOfRows = 0;
    pRes->row = 0;
    return 0;
  }

  pRes->row = 0;
  pRes->numOfRows = 0;

  SLocalReducer *pLocalReducer = pRes->pLocalReducer;

  // set the data merge in progress
  int32_t prevStatus =
      __sync_val_compare_and_swap_32(&pLocalReducer->status, TSC_LOCALREDUCE_READY, TSC_LOCALREDUCE_IN_PROGRESS);
  if (prevStatus != TSC_LOCALREDUCE_READY || pLocalReducer == NULL) {
    assert(prevStatus == TSC_LOCALREDUCE_TOBE_FREED);
    /* it is in tscDestroyLocalReducer function already */
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
  handleUnprocessedRow(pLocalReducer, pCmd, tmpBuffer);
  tColModel *pModel = pLocalReducer->pDesc->pSchema;

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
                    pOneDataSrc->pMemBuffer->pColModel->maxCapacity);

#if defined(_DEBUG_VIEW)
    printf("chosen row:\t");
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pCmd);

    tColModelDisplayEx(pModel, tmpBuffer->data, tmpBuffer->numOfElems, pModel->maxCapacity, colInfo);
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
      if (needToMerge(pCmd, pLocalReducer, tmpBuffer)) {
        // belong to the group of the previous row, continue process it
        for (int32_t j = 0; j < pCmd->fieldsInfo.numOfOutputCols; ++j) {
          SSqlExpr *pExpr = tscSqlExprGet(pCmd, j);
          tVariantAssign(&pLocalReducer->pCtx[j].param[0], &pExpr->param[0]);

          aAggs[pExpr->sqlFuncId].distSecondaryMergeFunc(&pLocalReducer->pCtx[j]);
        }

        // copy to buffer
        savePreviousRow(pLocalReducer, tmpBuffer);
      } else {
        /*
         * current row does not belong to the group of previous row.
         * so the processing of previous group is completed.
         */
        int32_t numOfRes = finalizeRes(pCmd, pLocalReducer);

        bool      sameGroup = isSameGroup(pCmd, pLocalReducer, pLocalReducer->prevRowOfInput, tmpBuffer);
        tFilePage *pResBuf = pLocalReducer->pResultBuf;

        /*
         * if the previous group does NOT generate any result (pResBuf->numOfElems == 0),
         * continue to process results instead of return results.
         */
        if ((!sameGroup && pResBuf->numOfElems > 0) ||
            (pResBuf->numOfElems == pLocalReducer->resColModel->maxCapacity)) {
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

          resetOutputBuf(pCmd, pLocalReducer);
          pOneDataSrc->rowIdx += 1;

          // here we do not check the return value
          adjustLoserTreeFromNewData(pLocalReducer, pOneDataSrc, pTree);
          assert(pLocalReducer->status == TSC_LOCALREDUCE_IN_PROGRESS);

          if (pRes->numOfRows == 0) {
            handleUnprocessedRow(pLocalReducer, pCmd, tmpBuffer);

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
              handleUnprocessedRow(pLocalReducer, pCmd, tmpBuffer);
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
          doMergeWithPrevRows(pSql, numOfRes);
          savePreviousRow(pLocalReducer, tmpBuffer);
        }
      }
    } else {
      doExecuteSecondaryMerge(pSql);
      savePreviousRow(pLocalReducer, tmpBuffer);  // copy the processed row to buffer
    }

    pOneDataSrc->rowIdx += 1;
    adjustLoserTreeFromNewData(pLocalReducer, pOneDataSrc, pTree);
  }

  if (pLocalReducer->hasPrevRow) {
    finalizeRes(pCmd, pLocalReducer);
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
