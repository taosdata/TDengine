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

#include "tscLocalMerge.h"
#include "tscSubquery.h"
#include "os.h"
#include "texpr.h"
#include "tlosertree.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "qUtil.h"

typedef struct SCompareParam {
  SLocalDataSource **pLocalData;
  tOrderDescriptor * pDesc;
  int32_t            num;
  int32_t            groupOrderType;
} SCompareParam;

bool needToMergeRv(SSDataBlock* pBlock, SArray* columnIndex, int32_t index, char **buf);

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
    return compare_d(pDesc, pParam->num, pLocalData[pLeftIdx]->rowIdx, pLocalData[pLeftIdx]->filePage.data,
                     pParam->num, pLocalData[pRightIdx]->rowIdx, pLocalData[pRightIdx]->filePage.data);
  } else {
    return compare_a(pDesc, pParam->num, pLocalData[pLeftIdx]->rowIdx, pLocalData[pLeftIdx]->filePage.data,
                     pParam->num, pLocalData[pRightIdx]->rowIdx, pLocalData[pRightIdx]->filePage.data);
  }
}

// todo merge with vnode side function
void tsCreateSQLFunctionCtx(SQueryInfo* pQueryInfo, SQLFunctionCtx* pCtx, SSchema* pSchema) {
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo *pExpr = tscSqlExprGet(pQueryInfo, i);

    pCtx[i].order = pQueryInfo->order.order;
    pCtx[i].functionId = pExpr->base.functionId;

    pCtx[i].order = pQueryInfo->order.order;
    pCtx[i].functionId = pExpr->base.functionId;

    // input data format comes from pModel
    pCtx[i].inputType = pSchema[i].type;
    pCtx[i].inputBytes = pSchema[i].bytes;

    pCtx[i].outputBytes = pExpr->base.resBytes;
    pCtx[i].outputType  = pExpr->base.resType;

    // input buffer hold only one point data
    pCtx[i].size = 1;
    pCtx[i].hasNull = true;
    pCtx[i].currentStage = MERGE_STAGE;

    // for top/bottom function, the output of timestamp is the first column
    int32_t functionId = pExpr->base.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx[i].ptsOutputBuf = pCtx[0].pOutput;
      pCtx[i].param[2].i64 = pQueryInfo->order.order;
      pCtx[i].param[2].nType  = TSDB_DATA_TYPE_BIGINT;
      pCtx[i].param[1].i64 = pQueryInfo->order.orderColId;
      pCtx[i].param[0].i64 = pExpr->base.param[0].i64;  // top/bot parameter
    } else if (functionId == TSDB_FUNC_APERCT) {
      pCtx[i].param[0].i64 = pExpr->base.param[0].i64;
      pCtx[i].param[0].nType  = pExpr->base.param[0].nType;
    } else if (functionId == TSDB_FUNC_BLKINFO) {
      pCtx[i].param[0].i64 = pExpr->base.param[0].i64;
      pCtx[i].param[0].nType = pExpr->base.param[0].nType;
      pCtx[i].numOfParams = 1;
    }

    pCtx[i].interBufBytes = pExpr->base.interBytes;
    pCtx[i].stableQuery = true;
  }
}

void tscCreateLocalMerger(tExtMemBuffer **pMemBuffer, int32_t numOfBuffer, tOrderDescriptor *pDesc,
                          SColumnModel *finalmodel, SColumnModel *pFFModel, SSqlObj *pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  
  if (pMemBuffer == NULL) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, pFFModel, numOfBuffer);
    tscError("pMemBuffer:%p is NULL", pMemBuffer);
    pRes->code = TSDB_CODE_TSC_APP_ERROR;
    return;
  }
 
  if (pDesc->pColumnModel == NULL) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, pFFModel, numOfBuffer);
    tscError("0x%"PRIx64" no local buffer or intermediate result format model", pSql->self);
    pRes->code = TSDB_CODE_TSC_APP_ERROR;
    return;
  }

  int32_t numOfFlush = 0;
  for (int32_t i = 0; i < numOfBuffer; ++i) {
    int32_t len = pMemBuffer[i]->fileMeta.flushoutData.nLength;
    if (len == 0) {
      tscDebug("0x%"PRIx64" no data retrieved from orderOfVnode:%d", pSql->self, i + 1);
      continue;
    }

    numOfFlush += len;
  }

  if (numOfFlush == 0 || numOfBuffer == 0) {
    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, pFFModel, numOfBuffer);
    pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT; // no result, set the result empty
    tscDebug("0x%"PRIx64" retrieved no data", pSql->self);
    return;
  }

  if (pDesc->pColumnModel->capacity >= pMemBuffer[0]->pageSize) {
    tscError("0x%"PRIx64" Invalid value of buffer capacity %d and page size %d ", pSql->self, pDesc->pColumnModel->capacity,
             pMemBuffer[0]->pageSize);

    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, pFFModel, numOfBuffer);
    pRes->code = TSDB_CODE_TSC_APP_ERROR;
    return;
  }

  size_t size = sizeof(SLocalMerger) + POINTER_BYTES * numOfFlush;
  
  SLocalMerger *pMerger = (SLocalMerger *) calloc(1, size);
  if (pMerger == NULL) {
    tscError("0x%"PRIx64" failed to create local merge structure, out of memory", pSql->self);

    tscLocalReducerEnvDestroy(pMemBuffer, pDesc, finalmodel, pFFModel, numOfBuffer);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return;
  }

  pMerger->pExtMemBuffer = pMemBuffer;
  pMerger->pLocalDataSrc = (SLocalDataSource **)&pMerger[1];
  assert(pMerger->pLocalDataSrc != NULL);

  pMerger->numOfBuffer = numOfFlush;
  pMerger->numOfVnode = numOfBuffer;

  pMerger->pDesc = pDesc;
  tscDebug("0x%"PRIx64" the number of merged leaves is: %d", pSql->self, pMerger->numOfBuffer);

  int32_t idx = 0;
  for (int32_t i = 0; i < numOfBuffer; ++i) {
    int32_t numOfFlushoutInFile = pMemBuffer[i]->fileMeta.flushoutData.nLength;

    for (int32_t j = 0; j < numOfFlushoutInFile; ++j) {
      SLocalDataSource *ds = (SLocalDataSource *)malloc(sizeof(SLocalDataSource) + pMemBuffer[0]->pageSize);
      if (ds == NULL) {
        tscError("0x%"PRIx64" failed to create merge structure", pSql->self);
        pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
        tfree(pMerger);
        return;
      }
      
      pMerger->pLocalDataSrc[idx] = ds;

      ds->pMemBuffer = pMemBuffer[i];
      ds->flushoutIdx = j;
      ds->filePage.num = 0;
      ds->pageId = 0;
      ds->rowIdx = 0;

      tscDebug("0x%"PRIx64" load data from disk into memory, orderOfVnode:%d, total:%d", pSql->self, i + 1, idx + 1);
      tExtMemBufferLoadData(pMemBuffer[i], &(ds->filePage), j, 0);
#ifdef _DEBUG_VIEW
      printf("load data page into mem for build loser tree: %" PRIu64 " rows\n", ds->filePage.num);
      SSrcColumnInfo colInfo[256] = {0};
      SQueryInfo *   pQueryInfo = tscGetQueryInfo(pCmd, pCmd->clauseIndex);

      tscGetSrcColumnInfo(colInfo, pQueryInfo);

      tColModelDisplayEx(pDesc->pColumnModel, ds->filePage.data, ds->filePage.num,
                         pMemBuffer[0]->numOfElemsPerPage, colInfo);
#endif
      
      if (ds->filePage.num == 0) {  // no data in this flush, the index does not increase
        tscDebug("0x%"PRIx64" flush data is empty, ignore %d flush record", pSql->self, idx);
        tfree(ds);
        continue;
      }
      
      idx += 1;
    }
  }
  
  // no data actually, no need to merge result.
  if (idx == 0) {
    tfree(pMerger);
    return;
  }

  pMerger->numOfBuffer = idx;

  SCompareParam *param = malloc(sizeof(SCompareParam));
  if (param == NULL) {
    tfree(pMerger);
    return;
  }

  param->pLocalData = pMerger->pLocalDataSrc;
  param->pDesc = pMerger->pDesc;
  param->num = pMerger->pLocalDataSrc[0]->pMemBuffer->numOfElemsPerPage;
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd, pCmd->clauseIndex);

  param->groupOrderType = pQueryInfo->groupbyExpr.orderType;
  pMerger->orderPrjOnSTable = tscOrderedProjectionQueryOnSTable(pQueryInfo, 0);

  pRes->code = tLoserTreeCreate(&pMerger->pLoserTree, pMerger->numOfBuffer, param, treeComparator);
  if (pMerger->pLoserTree == NULL || pRes->code != 0) {
    tfree(param);
    tfree(pMerger);
    return;
  }

  // the input data format follows the old format, but output in a new format.
  // so, all the input must be parsed as old format
  pMerger->pCtx = (SQLFunctionCtx *)calloc(tscSqlExprNumOfExprs(pQueryInfo), sizeof(SQLFunctionCtx));
  pMerger->rowSize = pMemBuffer[0]->nElemSize;

  tscFieldInfoUpdateOffset(pQueryInfo);

  if (pMerger->rowSize > pMemBuffer[0]->pageSize) {
    assert(false);  // todo fixed row size is larger than the minimum page size;
  }

  // used to keep the latest input row
  pMerger->pTempBuffer = (tFilePage *)calloc(1, pMerger->rowSize + sizeof(tFilePage));

  pMerger->nResultBufSize = pMemBuffer[0]->pageSize * 16;
  pMerger->pResultBuf = (tFilePage *)calloc(1, pMerger->nResultBufSize + sizeof(tFilePage));

  pMerger->resColModel = finalmodel;
  pMerger->resColModel->capacity = pMerger->nResultBufSize;
  pMerger->finalModel = pFFModel;

  if (finalmodel->rowSize > 0) {
    pMerger->resColModel->capacity /= finalmodel->rowSize;
  }

  assert(finalmodel->rowSize > 0 && finalmodel->rowSize <= pMerger->rowSize);

  if (pMerger->pTempBuffer == NULL || pMerger->pLoserTree == NULL) {
    tfree(pMerger->pTempBuffer);
    tfree(pMerger->pLoserTree);
    tfree(param);
    tfree(pMerger);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return;
  }
  
  pMerger->pTempBuffer->num = 0;
  tscCreateResPointerInfo(pRes, pQueryInfo);

  SSchema* pschema = calloc(pDesc->pColumnModel->numOfCols, sizeof(SSchema));
  for(int32_t i = 0; i < pDesc->pColumnModel->numOfCols; ++i) {
    pschema[i] = pDesc->pColumnModel->pFields[i].field;
  }

  tsCreateSQLFunctionCtx(pQueryInfo, pMerger->pCtx, pschema);
//  setCtxInputOutputBuffer(pQueryInfo, pMerger->pCtx, pMerger, pDesc);

  tfree(pschema);

  int32_t maxBufSize = 0;
  for (int32_t k = 0; k < tscSqlExprNumOfExprs(pQueryInfo); ++k) {
    SExprInfo *pExpr = tscSqlExprGet(pQueryInfo, k);
    if (maxBufSize < pExpr->base.resBytes && pExpr->base.functionId == TSDB_FUNC_TAG) {
      maxBufSize = pExpr->base.resBytes;
    }
  }

  // we change the capacity of schema to denote that there is only one row in temp buffer
  pMerger->pDesc->pColumnModel->capacity = 1;

  // restore the limitation value at the last stage
  if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    pQueryInfo->limit.limit = pQueryInfo->clauseLimit;
    pQueryInfo->limit.offset = pQueryInfo->prjOffset;
  }

  pRes->pLocalMerger = pMerger;
  pRes->numOfGroups = 0;

//  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
//  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
//  TSKEY stime = (pQueryInfo->order.order == TSDB_ORDER_ASC)? pQueryInfo->window.skey : pQueryInfo->window.ekey;
//  int64_t revisedSTime = taosTimeTruncate(stime, &pQueryInfo->interval, tinfo.precision);
  
//  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
//    SFillColInfo* pFillCol = createFillColInfo(pQueryInfo);
//    pMerger->pFillInfo =
//        taosCreateFillInfo(pQueryInfo->order.order, revisedSTime, pQueryInfo->groupbyExpr.numOfGroupCols, 4096,
//                           (int32_t)pQueryInfo->fieldsInfo.numOfOutput, pQueryInfo->interval.sliding,
//                           pQueryInfo->interval.slidingUnit, tinfo.precision, pQueryInfo->fillType, pFillCol, pSql);
//  }
}

static int32_t tscFlushTmpBufferImpl(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage,
                                     int32_t orderType) {
  if (pPage->num == 0) {
    return 0;
  }

  assert(pPage->num <= pDesc->pColumnModel->capacity);

  // sort before flush to disk, the data must be consecutively put on tFilePage.
  if (pDesc->orderInfo.numOfCols > 0) {
    tColDataQSort(pDesc, (int32_t)pPage->num, 0, (int32_t)pPage->num - 1, pPage->data, orderType);
  }

#ifdef _DEBUG_VIEW
  printf("%" PRIu64 " rows data flushed to disk after been sorted:\n", pPage->num);
  tColModelDisplay(pDesc->pColumnModel, pPage->data, pPage->num, pPage->num);
#endif

  // write to cache after being sorted
  if (tExtMemBufferPut(pMemoryBuf, pPage->data, (int32_t)pPage->num) < 0) {
    tscError("failed to save data in temporary buffer");
    return -1;
  }

  pPage->num = 0;
  return 0;
}

int32_t tscFlushTmpBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, int32_t orderType) {
  int32_t ret = 0;
  if ((ret = tscFlushTmpBufferImpl(pMemoryBuf, pDesc, pPage, orderType)) != 0) {
    return ret;
  }

  if ((ret = tExtMemBufferFlush(pMemoryBuf)) != 0) {
    return ret;
  }

  return 0;
}

int32_t saveToBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, void *data,
                     int32_t numOfRows, int32_t orderType) {
  SColumnModel *pModel = pDesc->pColumnModel;

  if (pPage->num + numOfRows <= pModel->capacity) {
    tColModelAppend(pModel, pPage, data, 0, numOfRows, numOfRows);
    return 0;
  }

  // current buffer is overflow, flush data to extensive buffer
  int32_t numOfRemainEntries = pModel->capacity - (int32_t)pPage->num;
  tColModelAppend(pModel, pPage, data, 0, numOfRemainEntries, numOfRows);

  // current buffer is full, need to flushed to disk
  assert(pPage->num == pModel->capacity);
  int32_t code = tscFlushTmpBuffer(pMemoryBuf, pDesc, pPage, orderType);
  if (code != 0) {
    return code;
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

    if (pPage->num == pModel->capacity) {
      if ((code = tscFlushTmpBuffer(pMemoryBuf, pDesc, pPage, orderType)) != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      pPage->num = numOfWriteElems;
    }

    remain -= numOfWriteElems;
    numOfRemainEntries += numOfWriteElems;
  }

  return 0;
}

void tscDestroyLocalMerger(SSqlObj *pSql) {
  if (pSql == NULL) {
    return;
  }

  SSqlRes *pRes = &(pSql->res);
  if (pRes->pLocalMerger == NULL) {
    return;
  }

  // there is no more result, so we release all allocated resource
  SLocalMerger *pLocalMerge = (SLocalMerger *)atomic_exchange_ptr(&pRes->pLocalMerger, NULL);
  tfree(pLocalMerge->pResultBuf);
  tfree(pLocalMerge->pCtx);

  if (pLocalMerge->pLoserTree) {
    tfree(pLocalMerge->pLoserTree->param);
    tfree(pLocalMerge->pLoserTree);
  }

  tscLocalReducerEnvDestroy(pLocalMerge->pExtMemBuffer, pLocalMerge->pDesc, pLocalMerge->resColModel,
                            pLocalMerge->finalModel, pLocalMerge->numOfVnode);
  for (int32_t i = 0; i < pLocalMerge->numOfBuffer; ++i) {
    tfree(pLocalMerge->pLocalDataSrc[i]);
  }

  pLocalMerge->numOfBuffer = 0;
  pLocalMerge->numOfCompleted = 0;
  tfree(pLocalMerge->pTempBuffer);

  free(pLocalMerge);

  tscDebug("0x%"PRIx64" free local reducer finished", pSql->self);
}

static int32_t createOrderDescriptor(tOrderDescriptor **pOrderDesc, SSqlCmd *pCmd, SColumnModel *pModel) {
  int32_t     numOfGroupByCols = 0;
  SQueryInfo *pQueryInfo = tscGetActiveQueryInfo(pCmd);

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    numOfGroupByCols = pQueryInfo->groupbyExpr.numOfGroupCols;
  }

  // primary timestamp column is involved in final result
  if (pQueryInfo->interval.interval != 0 || tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    numOfGroupByCols++;
  }

  int32_t *orderColIndexList = (int32_t *)calloc(numOfGroupByCols, sizeof(int32_t));
  if (orderColIndexList == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  if (numOfGroupByCols > 0) {

    if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
      int32_t numOfInternalOutput = (int32_t) tscSqlExprNumOfExprs(pQueryInfo);

      // the last "pQueryInfo->groupbyExpr.numOfGroupCols" columns are order-by columns
      for (int32_t i = 0; i < pQueryInfo->groupbyExpr.numOfGroupCols; ++i) {
        SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, i);
        for(int32_t j = 0; j < numOfInternalOutput; ++j) {
          SExprInfo* pExprInfo = tscSqlExprGet(pQueryInfo, j);

          int32_t functionId = pExprInfo->base.functionId;
          if (pColIndex->colId == pExprInfo->base.colInfo.colId && (functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TAG)) {
            orderColIndexList[i] = j;
            break;
          }
        }
      }

      if (pQueryInfo->interval.interval != 0) {
        // the first column is the timestamp, handles queries like "interval(10m) group by tags"
        orderColIndexList[numOfGroupByCols - 1] = PRIMARYKEY_TIMESTAMP_COL_INDEX; //TODO ???
      }
    } else {
      /*
       * 1. the orderby ts asc/desc projection query for the super table
       * 2. interval query without groupby clause
       */
      if (pQueryInfo->interval.interval != 0) {
        orderColIndexList[0] = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      } else {
        size_t size = tscSqlExprNumOfExprs(pQueryInfo);
        for (int32_t i = 0; i < size; ++i) {
          SExprInfo *pExpr = tscSqlExprGet(pQueryInfo, i);
          if (pExpr->base.functionId == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
            orderColIndexList[0] = i;
          }
        }
      }

      assert(pQueryInfo->order.orderColId == PRIMARYKEY_TIMESTAMP_COL_INDEX);
    }
  }

  *pOrderDesc = tOrderDesCreate(orderColIndexList, numOfGroupByCols, pModel, pQueryInfo->order.order);
  tfree(orderColIndexList);

  if (*pOrderDesc == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t tscLocalReducerEnvCreate(SSqlObj *pSql, tExtMemBuffer ***pMemBuffer, tOrderDescriptor **pOrderDesc,
                                 SColumnModel **pFinalModel, SColumnModel** pFFModel, uint32_t nBufferSizes) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SSchema *     pSchema = NULL;
  SColumnModel *pModel = NULL;
  *pFinalModel = NULL;

  SQueryInfo *    pQueryInfo = tscGetActiveQueryInfo(pCmd);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  (*pMemBuffer) = (tExtMemBuffer **)malloc(POINTER_BYTES * pSql->subState.numOfSub);
  if (*pMemBuffer == NULL) {
    tscError("0x%"PRIx64" failed to allocate memory", pSql->self);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pRes->code;
  }
  
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  pSchema = (SSchema *)calloc(1, sizeof(SSchema) * size);
  if (pSchema == NULL) {
    tscError("0x%"PRIx64" failed to allocate memory", pSql->self);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pRes->code;
  }

  int32_t rlen = 0;
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo *pExpr = tscSqlExprGet(pQueryInfo, i);

    pSchema[i].bytes = pExpr->base.resBytes;
    pSchema[i].type = (int8_t)pExpr->base.resType;
    tstrncpy(pSchema[i].name, pExpr->base.aliasName, tListLen(pSchema[i].name));

    rlen += pExpr->base.resBytes;
  }

  int32_t capacity = 0;
  if (rlen != 0) {
    capacity = nBufferSizes / rlen;
  }
  
  pModel = createColumnModel(pSchema, (int32_t)size, capacity);

  int32_t pg = DEFAULT_PAGE_SIZE;
  int32_t overhead = sizeof(tFilePage);
  while((pg - overhead) < pModel->rowSize * 2) {
    pg *= 2;
  }

  size_t numOfSubs = pSql->subState.numOfSub;
  assert(numOfSubs <= pTableMetaInfo->vgroupList->numOfVgroups);
  for (int32_t i = 0; i < numOfSubs; ++i) {
    (*pMemBuffer)[i] = createExtMemBuffer(nBufferSizes, rlen, pg, pModel);
    (*pMemBuffer)[i]->flushModel = MULTIPLE_APPEND_MODEL;
  }

  if (createOrderDescriptor(pOrderDesc, pCmd, pModel) != TSDB_CODE_SUCCESS) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tfree(pSchema);
    return pRes->code;
  }

  // final result depends on the fields number
  memset(pSchema, 0, sizeof(SSchema) * size);

  for (int32_t i = 0; i < size; ++i) {
    SExprInfo *pExpr = tscSqlExprGet(pQueryInfo, i);

    SSchema p1 = {0};
    if (pExpr->base.colInfo.colIndex == TSDB_TBNAME_COLUMN_INDEX) {
      p1 = *tGetTbnameColumnSchema();
    } else if (TSDB_COL_IS_UD_COL(pExpr->base.colInfo.flag)) {
      p1.bytes = pExpr->base.resBytes;
      p1.type  = (uint8_t) pExpr->base.resType;
      tstrncpy(p1.name, pExpr->base.aliasName, tListLen(p1.name));
    } else {
      p1 = *tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pExpr->base.colInfo.colIndex);
    }

    int32_t inter = 0;
    int16_t type = -1;
    int16_t bytes = 0;

    // the final result size and type in the same as query on single table.
    // so here, set the flag to be false;
    int32_t functionId = pExpr->base.functionId;
    if (functionId >= TSDB_FUNC_TS && functionId <= TSDB_FUNC_DIFF) {
      type = pModel->pFields[i].field.type;
      bytes = pModel->pFields[i].field.bytes;
    } else {
      if (functionId == TSDB_FUNC_FIRST_DST) {
        functionId = TSDB_FUNC_FIRST;
      } else if (functionId == TSDB_FUNC_LAST_DST) {
        functionId = TSDB_FUNC_LAST;
      } else if (functionId == TSDB_FUNC_STDDEV_DST) {
        functionId = TSDB_FUNC_STDDEV;
      }

      int32_t ret = getResultDataInfo(p1.type, p1.bytes, functionId, 0, &type, &bytes, &inter, 0, false);
      assert(ret == TSDB_CODE_SUCCESS);
    }

    pSchema[i].type = (uint8_t)type;
    pSchema[i].bytes = bytes;
    strcpy(pSchema[i].name, pModel->pFields[i].field.name);
  }
  
  *pFinalModel = createColumnModel(pSchema, (int32_t)size, capacity);

  memset(pSchema, 0, sizeof(SSchema) * size);
  size = tscNumOfFields(pQueryInfo);

  for(int32_t i = 0; i < size; ++i) {
    SInternalField* pField = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, i);
    pSchema[i].bytes = pField->field.bytes;
    pSchema[i].type = pField->field.type;
    tstrncpy(pSchema[i].name, pField->field.name, tListLen(pSchema[i].name));
  }

  *pFFModel = createColumnModel(pSchema, (int32_t) size, capacity);

   tfree(pSchema);
  return TSDB_CODE_SUCCESS;
}

/**
 * @param pMemBuffer
 * @param pDesc
 * @param pFinalModel
 * @param numOfVnodes
 */
void tscLocalReducerEnvDestroy(tExtMemBuffer **pMemBuffer, tOrderDescriptor *pDesc, SColumnModel *pFinalModel, SColumnModel *pFFModel,
                               int32_t numOfVnodes) {
  destroyColumnModel(pFinalModel);
  destroyColumnModel(pFFModel);

  tOrderDescDestroy(pDesc);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pMemBuffer[i] = destoryExtMemBuffer(pMemBuffer[i]);
  }

  tfree(pMemBuffer);
}

/**
 *
 * @param pLocalMerge
 * @param pOneInterDataSrc
 * @param treeList
 * @return the number of remain input source. if ret == 0, all data has been handled
 */
int32_t loadNewDataFromDiskFor(SLocalMerger *pLocalMerge, SLocalDataSource *pOneInterDataSrc,
                               bool *needAdjustLoserTree) {
  pOneInterDataSrc->rowIdx = 0;
  pOneInterDataSrc->pageId += 1;

  if ((uint32_t)pOneInterDataSrc->pageId <
      pOneInterDataSrc->pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[pOneInterDataSrc->flushoutIdx].numOfPages) {
    tExtMemBufferLoadData(pOneInterDataSrc->pMemBuffer, &(pOneInterDataSrc->filePage), pOneInterDataSrc->flushoutIdx,
                          pOneInterDataSrc->pageId);

#if defined(_DEBUG_VIEW)
    printf("new page load to buffer\n");
    tColModelDisplay(pOneInterDataSrc->pMemBuffer->pColumnModel, pOneInterDataSrc->filePage.data,
                     pOneInterDataSrc->filePage.num, pOneInterDataSrc->pMemBuffer->pColumnModel->capacity);
#endif
    *needAdjustLoserTree = true;
  } else {
    pLocalMerge->numOfCompleted += 1;

    pOneInterDataSrc->rowIdx = -1;
    pOneInterDataSrc->pageId = -1;
    *needAdjustLoserTree = true;
  }

  return pLocalMerge->numOfBuffer;
}

void adjustLoserTreeFromNewData(SLocalMerger *pLocalMerge, SLocalDataSource *pOneInterDataSrc,
                                SLoserTreeInfo *pTree) {
  /*
   * load a new data page into memory for intermediate dataset source,
   * since it's last record in buffer has been chosen to be processed, as the winner of loser-tree
   */
  bool needToAdjust = true;
  if (pOneInterDataSrc->filePage.num <= pOneInterDataSrc->rowIdx) {
    loadNewDataFromDiskFor(pLocalMerge, pOneInterDataSrc, &needToAdjust);
  }

  /*
   * adjust loser tree otherwise, according to new candidate data
   * if the loser tree is rebuild completed, we do not need to adjust
   */
  if (needToAdjust) {
    int32_t leafNodeIdx = pTree->pNode[0].index + pLocalMerge->numOfBuffer;

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

//TODO it is not ordered, fix it
static void savePrevOrderColumns(char** prevRow, SArray* pColumnList, SSDataBlock* pBlock, int32_t rowIndex, bool* hasPrev) {
  int32_t size = (int32_t) taosArrayGetSize(pColumnList);

  for(int32_t i = 0; i < size; ++i) {
    SColIndex* index = taosArrayGet(pColumnList, i);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, index->colIndex);
    assert(index->colId == pColInfo->info.colId);

    memcpy(prevRow[i], pColInfo->pData + pColInfo->info.bytes * rowIndex, pColInfo->info.bytes);
  }

  (*hasPrev) = true;
}

static void setTagValueForMultipleRows(SQLFunctionCtx* pCtx, int32_t numOfOutput, int32_t numOfRows) {
  if (numOfRows <= 1) {
    return ;
  }

  for (int32_t k = 0; k < numOfOutput; ++k) {
    if (pCtx[k].functionId != TSDB_FUNC_TAG) {
      continue;
    }

    int32_t inc = numOfRows - 1;  // tsdb_func_tag function only produce one row of result
    char* src = pCtx[k].pOutput;

    for (int32_t i = 0; i < inc; ++i) {
      pCtx[k].pOutput += pCtx[k].outputBytes;
      memcpy(pCtx[k].pOutput, src, (size_t)pCtx[k].outputBytes);
    }
  }
}

static void doExecuteFinalMergeRv(SOperatorInfo* pOperator, int32_t numOfExpr, SSDataBlock* pBlock) {
  SMultiwayMergeInfo* pInfo = pOperator->info;
  SQLFunctionCtx* pCtx = pInfo->binfo.pCtx;

  char** add = calloc(pBlock->info.numOfCols, POINTER_BYTES);
  for(int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    add[i] = pCtx[i].pInput;
    pCtx[i].size = 1;
  }

  for(int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (pInfo->hasPrev) {
      if (needToMergeRv(pBlock, pInfo->orderColumnList, i, pInfo->prevRow)) {
        for (int32_t j = 0; j < numOfExpr; ++j) {
          pCtx[j].pInput = add[j] + pCtx[j].inputBytes * i;
        }

        for (int32_t j = 0; j < numOfExpr; ++j) {
          int32_t functionId = pCtx[j].functionId;
          if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
            continue;
          }
          aAggs[functionId].mergeFunc(&pCtx[j]);
        }
      } else {
        for(int32_t j = 0; j < numOfExpr; ++j) {  // TODO refactor
          int32_t functionId = pCtx[j].functionId;
          if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
            continue;
          }
          aAggs[functionId].xFinalize(&pCtx[j]);
        }

        int32_t numOfRows = getNumOfResult(pOperator->pRuntimeEnv, pInfo->binfo.pCtx, pOperator->numOfOutput);
        setTagValueForMultipleRows(pCtx, pOperator->numOfOutput, numOfRows);

        pInfo->binfo.pRes->info.rows += numOfRows;

        for(int32_t j = 0; j < numOfExpr; ++j) {
          pCtx[j].pOutput += (pCtx[j].outputBytes * numOfRows);
          if (pCtx[j].functionId == TSDB_FUNC_TOP || pCtx[j].functionId == TSDB_FUNC_BOTTOM) {
            pCtx[j].ptsOutputBuf = pCtx[0].pOutput;
          }
        }

        for(int32_t j = 0; j < numOfExpr; ++j) {
          aAggs[pCtx[j].functionId].init(&pCtx[j]);
        }

        for (int32_t j = 0; j < numOfExpr; ++j) {
          pCtx[j].pInput = add[j] + pCtx[j].inputBytes * i;
        }

        for (int32_t j = 0; j < numOfExpr; ++j) {
          int32_t functionId = pCtx[j].functionId;
          if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
            continue;
          }
          aAggs[functionId].mergeFunc(&pCtx[j]);
        }
      }
    } else {
      for (int32_t j = 0; j < numOfExpr; ++j) {
        pCtx[j].pInput = add[j] + pCtx[j].inputBytes * i;
      }

      for (int32_t j = 0; j < numOfExpr; ++j) {
        int32_t functionId = pCtx[j].functionId;
        if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
          continue;
        }
        aAggs[functionId].mergeFunc(&pCtx[j]);
      }
    }

    savePrevOrderColumns(pInfo->prevRow, pInfo->orderColumnList, pBlock, i, &pInfo->hasPrev);
  }

  {
    for(int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
      pCtx[i].pInput = add[i];
    }
  }

  tfree(add);
}

bool needToMergeRv(SSDataBlock* pBlock, SArray* columnIndexList, int32_t index, char **buf) {
  int32_t ret = 0;
  size_t  size = taosArrayGetSize(columnIndexList);
  if (size > 0) {
    ret = compare_aRv(pBlock, columnIndexList, (int32_t) size, index, buf, TSDB_ORDER_ASC);
  }

  // if ret == 0, means the result belongs to the same group
  return (ret == 0);
}

static bool isAllSourcesCompleted(SLocalMerger *pLocalMerge) {
  return (pLocalMerge->numOfBuffer == pLocalMerge->numOfCompleted);
}

void tscInitResObjForLocalQuery(SSqlObj *pObj, int32_t numOfRes, int32_t rowLen) {
  SSqlRes *pRes = &pObj->res;
  if (pRes->pLocalMerger != NULL) {
    tscDestroyLocalMerger(pObj);
  }

  pRes->qId = 1;  // hack to pass the safety check in fetch_row function
  pRes->numOfRows = 0;
  pRes->row = 0;

  pRes->rspType = 0;  // used as a flag to denote if taos_retrieved() has been called yet
  pRes->pLocalMerger = (SLocalMerger *)calloc(1, sizeof(SLocalMerger));

  /*
   * we need one additional byte space
   * the sprintf function needs one additional space to put '\0' at the end of string
   */
  size_t allocSize = numOfRes * rowLen + sizeof(tFilePage) + 1;
  pRes->pLocalMerger->pResultBuf = (tFilePage *)calloc(1, allocSize);

  pRes->pLocalMerger->pResultBuf->num = numOfRes;
  pRes->data = pRes->pLocalMerger->pResultBuf->data;
}

int32_t doArithmeticCalculate(SQueryInfo* pQueryInfo, tFilePage* pOutput, int32_t rowSize, int32_t finalRowSize) {
  int32_t maxRowSize = MAX(rowSize, finalRowSize);
  char* pbuf = calloc(1, (size_t)(pOutput->num * maxRowSize));

  size_t size = tscNumOfFields(pQueryInfo);
  SArithmeticSupport arithSup = {0};

  // todo refactor
  arithSup.offset     = 0;
  arithSup.numOfCols  = (int32_t) tscSqlExprNumOfExprs(pQueryInfo);
  arithSup.exprList   = pQueryInfo->exprList;
  arithSup.data       = calloc(arithSup.numOfCols, POINTER_BYTES);

  for(int32_t k = 0; k < arithSup.numOfCols; ++k) {
    SExprInfo* pExpr = tscSqlExprGet(pQueryInfo, k);
    arithSup.data[k] = (pOutput->data + pOutput->num* pExpr->base.offset);
  }

  int32_t offset = 0;

  for (int i = 0; i < size; ++i) {
    SInternalField* pSup = TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, i);
    
    // calculate the result from several other columns
    if (pSup->pExpr->pExpr != NULL) {
      arithSup.pExprInfo = pSup->pExpr;
      arithmeticTreeTraverse(arithSup.pExprInfo->pExpr, (int32_t) pOutput->num, pbuf + pOutput->num*offset, &arithSup, TSDB_ORDER_ASC, getArithmeticInputSrc);
    } else {
      SExprInfo* pExpr = pSup->pExpr;
      memcpy(pbuf + pOutput->num * offset, pExpr->base.offset * pOutput->num + pOutput->data, (size_t)(pExpr->base.resBytes * pOutput->num));
    }

    offset += pSup->field.bytes;
  }

  memcpy(pOutput->data, pbuf, (size_t)(pOutput->num * offset));

  tfree(pbuf);
  tfree(arithSup.data);

  return offset;
}

#define COLMODEL_GET_VAL(data, schema, allrow, rowId, colId) \
  (data + (schema)->pFields[colId].offset * (allrow) + (rowId) * (schema)->pFields[colId].field.bytes)

static void appendOneRowToDataBlock(SSDataBlock *pBlock, char *buf, SColumnModel *pModel, int32_t rowIndex,
                                    int32_t maxRows) {
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    char* p = pColInfo->pData + pBlock->info.rows * pColInfo->info.bytes;

    char *src = COLMODEL_GET_VAL(buf, pModel, maxRows, rowIndex, i);
    memmove(p, src, pColInfo->info.bytes);
  }

  pBlock->info.rows += 1;
}

SSDataBlock* doMultiwayMergeSort(void* param, bool* newgroup) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SMultiwayMergeInfo *pInfo = pOperator->info;

  SLocalMerger   *pMerger = pInfo->pMerge;
  SLoserTreeInfo *pTree   = pMerger->pLoserTree;
  SColumnModel   *pModel  = pMerger->pDesc->pColumnModel;
  tFilePage      *tmpBuffer = pMerger->pTempBuffer;

  pInfo->binfo.pRes->info.rows = 0;

  while(1) {
    if (isAllSourcesCompleted(pMerger)) {
      break;
    }

#ifdef _DEBUG_VIEW
    printf("chosen data in pTree[0] = %d\n", pTree->pNode[0].index);
#endif

    assert((pTree->pNode[0].index < pMerger->numOfBuffer) && (pTree->pNode[0].index >= 0) && tmpBuffer->num == 0);

    // chosen from loser tree
    SLocalDataSource *pOneDataSrc = pMerger->pLocalDataSrc[pTree->pNode[0].index];
    bool sameGroup = true;
    if (pInfo->hasPrev) {
      int32_t numOfCols = (int32_t)taosArrayGetSize(pInfo->orderColumnList);

      // if this row belongs to current result set group
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColIndex *      pIndex = taosArrayGet(pInfo->orderColumnList, i);
        SColumnInfoData *pColInfo = taosArrayGet(pInfo->binfo.pRes->pDataBlock, pIndex->colIndex);

        char *newRow =
            COLMODEL_GET_VAL(pOneDataSrc->filePage.data, pModel, pOneDataSrc->pMemBuffer->pColumnModel->capacity,
                             pOneDataSrc->rowIdx, pIndex->colIndex);

        char *  data = pInfo->prevRow[i];
        int32_t ret = columnValueAscendingComparator(data, newRow, pColInfo->info.type, pColInfo->info.bytes);
        if (ret == 0) {
          continue;
        } else {
          sameGroup = false;
          *newgroup = true;
          break;
        }
      }
    }

    if (!sameGroup || !pInfo->hasPrev) { //save the data
      int32_t numOfCols = (int32_t)taosArrayGetSize(pInfo->orderColumnList);

      for (int32_t i = 0; i < numOfCols; ++i) {
        SColIndex *      pIndex = taosArrayGet(pInfo->orderColumnList, i);
        SColumnInfoData *pColInfo = taosArrayGet(pInfo->binfo.pRes->pDataBlock, pIndex->colIndex);

        char *curCol =
            COLMODEL_GET_VAL(pOneDataSrc->filePage.data, pModel, pOneDataSrc->pMemBuffer->pColumnModel->capacity,
                             pOneDataSrc->rowIdx, pIndex->colIndex);
        memcpy(pInfo->prevRow[i], curCol, pColInfo->info.bytes);
      }

      pInfo->hasPrev = true;
    }

    if (!sameGroup && pInfo->binfo.pRes->info.rows > 0) {
      return pInfo->binfo.pRes;
    }

    appendOneRowToDataBlock(pInfo->binfo.pRes, pOneDataSrc->filePage.data, pModel, pOneDataSrc->rowIdx, pOneDataSrc->pMemBuffer->pColumnModel->capacity);

#if defined(_DEBUG_VIEW)
    printf("chosen row:\t");
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pQueryInfo);

    tColModelDisplayEx(pModel, tmpBuffer->data, tmpBuffer->num, pModel->capacity, colInfo);
#endif

    pOneDataSrc->rowIdx += 1;
    adjustLoserTreeFromNewData(pMerger, pOneDataSrc, pTree);

    if (pInfo->binfo.pRes->info.rows >= pInfo->bufCapacity) {
      return pInfo->binfo.pRes;
    }
  }

  pOperator->status = OP_EXEC_DONE;
  return (pInfo->binfo.pRes->info.rows > 0)? pInfo->binfo.pRes:NULL;
}

static bool isSameGroupRv(SArray* orderColumnList, SSDataBlock* pBlock, char** dataCols) {
  int32_t numOfCols = (int32_t) taosArrayGetSize(orderColumnList);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColIndex *pIndex = taosArrayGet(orderColumnList, i);

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, pIndex->colIndex);
    assert(pIndex->colId == pColInfo->info.colId);

    char *data = dataCols[i];
    int32_t ret = columnValueAscendingComparator(data, pColInfo->pData, pColInfo->info.type, pColInfo->info.bytes);
    if (ret == 0) {
      continue;
    } else {
      return false;
    }
  }

  return true;
}

SSDataBlock* doGlobalAggregate(void* param, bool* newgroup) {
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SMultiwayMergeInfo *pAggInfo = pOperator->info;
  SOperatorInfo      *upstream = pOperator->upstream;

  *newgroup = false;
  bool handleData = false;
  pAggInfo->binfo.pRes->info.rows = 0;

  {
    if (pAggInfo->hasDataBlockForNewGroup) {
      pAggInfo->binfo.pRes->info.rows = 0;
      pAggInfo->hasPrev = false; // now we start from a new group data set.

      // not belongs to the same group, return the result of current group;
      setInputDataBlock(pOperator, pAggInfo->binfo.pCtx, pAggInfo->pExistBlock, TSDB_ORDER_ASC);
      updateOutputBuf(&pAggInfo->binfo, &pAggInfo->bufCapacity, pAggInfo->pExistBlock->info.rows);

      { // reset output buffer
        for(int32_t j = 0; j < pOperator->numOfOutput; ++j) {
          aAggs[pAggInfo->binfo.pCtx[j].functionId].init(&pAggInfo->binfo.pCtx[j]);
        }
      }

      doExecuteFinalMergeRv(pOperator, pOperator->numOfOutput, pAggInfo->pExistBlock);

      savePrevOrderColumns(pAggInfo->currentGroupColData, pAggInfo->groupColumnList, pAggInfo->pExistBlock, 0,
                           &pAggInfo->hasGroupColData);
      pAggInfo->pExistBlock = NULL;
      pAggInfo->hasDataBlockForNewGroup = false;
      handleData = true;
      *newgroup = true;
    }
  }

  SSDataBlock* pBlock = NULL;
  while(1) {
    bool prev = *newgroup;
    pBlock = upstream->exec(upstream, newgroup);
    if (pBlock == NULL) {
      *newgroup = prev;
      break;
    }

    if (pAggInfo->hasGroupColData) {
      bool sameGroup = isSameGroupRv(pAggInfo->groupColumnList, pBlock, pAggInfo->currentGroupColData);
      if (!sameGroup) {
        *newgroup = true;
        pAggInfo->hasDataBlockForNewGroup = true;
        pAggInfo->pExistBlock = pBlock;
        savePrevOrderColumns(pAggInfo->prevRow, pAggInfo->groupColumnList, pBlock, 0, &pAggInfo->hasPrev);
        break;
      }
    }

    // not belongs to the same group, return the result of current group
    setInputDataBlock(pOperator, pAggInfo->binfo.pCtx, pBlock, TSDB_ORDER_ASC);
    updateOutputBuf(&pAggInfo->binfo, &pAggInfo->bufCapacity, pBlock->info.rows * pAggInfo->resultRowFactor);

    doExecuteFinalMergeRv(pOperator, pOperator->numOfOutput, pBlock);
    savePrevOrderColumns(pAggInfo->currentGroupColData, pAggInfo->groupColumnList, pBlock, 0, &pAggInfo->hasGroupColData);
    handleData = true;
  }

  if (handleData) { // data in current group is all handled
    for(int32_t j = 0; j < pOperator->numOfOutput; ++j) {
      int32_t functionId = pAggInfo->binfo.pCtx[j].functionId;
      if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
        continue;
      }

      aAggs[functionId].xFinalize(&pAggInfo->binfo.pCtx[j]);
    }

    int32_t numOfRows = getNumOfResult(pOperator->pRuntimeEnv, pAggInfo->binfo.pCtx, pOperator->numOfOutput);
    pAggInfo->binfo.pRes->info.rows += numOfRows;

    setTagValueForMultipleRows(pAggInfo->binfo.pCtx, pOperator->numOfOutput, numOfRows);
  }

  SSDataBlock* pRes = pAggInfo->binfo.pRes;
  {
    SColumnInfoData* pInfoData = taosArrayGet(pRes->pDataBlock, 0);

    if (pInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP && pRes->info.rows > 0) {
      STimeWindow* w = &pRes->info.window;

      // TODO in case of desc order, swap it
      w->skey = *(int64_t*)pInfoData->pData;
      w->ekey = *(int64_t*)(((char*)pInfoData->pData) + TSDB_KEYSIZE * (pRes->info.rows - 1));

      if (pOperator->pRuntimeEnv->pQueryAttr->order.order == TSDB_ORDER_DESC) {
        SWAP(w->skey, w->ekey, TSKEY);
        assert(w->skey <= w->ekey);
      }
    }
  }

  return (pRes->info.rows != 0)? pRes:NULL;
}

static SSDataBlock* skipGroupBlock(SOperatorInfo* pOperator, bool* newgroup) {
  SSLimitOperatorInfo *pInfo = pOperator->info;
  assert(pInfo->currentGroupOffset >= 0);

  SSDataBlock* pBlock = NULL;
  if (pInfo->currentGroupOffset == 0) {
    pBlock = pOperator->upstream->exec(pOperator->upstream, newgroup);
    if (pBlock == NULL) {
      setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
      pOperator->status = OP_EXEC_DONE;
    }

    if (*newgroup == false && pInfo->limit.limit > 0 && pInfo->rowsTotal >= pInfo->limit.limit) {
      while ((*newgroup) == false) {  // ignore the remain blocks
        pBlock = pOperator->upstream->exec(pOperator->upstream, newgroup);
        if (pBlock == NULL) {
          setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
          pOperator->status = OP_EXEC_DONE;
          return NULL;
        }
      }
    }

    return pBlock;
  }

    pBlock = pOperator->upstream->exec(pOperator->upstream, newgroup);
    if (pBlock == NULL) {
      setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }

    while(1) {
      if (*newgroup) {
        pInfo->currentGroupOffset -= 1;
        *newgroup = false;
      }

      while ((*newgroup) == false) {
        pBlock = pOperator->upstream->exec(pOperator->upstream, newgroup);
        if (pBlock == NULL) {
          setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
          pOperator->status = OP_EXEC_DONE;
          return NULL;
        }
      }

      // now we have got the first data block of the next group.
      if (pInfo->currentGroupOffset == 0) {
        return pBlock;
      }
    }

  return NULL;
}

SSDataBlock* doSLimit(void* param, bool* newgroup) {
  SOperatorInfo *pOperator = (SOperatorInfo *)param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSLimitOperatorInfo *pInfo = pOperator->info;

  SSDataBlock *pBlock = NULL;
  while (1) {
    pBlock = skipGroupBlock(pOperator, newgroup);
    if (pBlock == NULL) {
      setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }

    if (*newgroup) {  // a new group arrives
      pInfo->groupTotal += 1;
      pInfo->rowsTotal = 0;
      pInfo->currentOffset = pInfo->limit.offset;
    }

    assert(pInfo->currentGroupOffset == 0);

    if (pInfo->currentOffset >= pBlock->info.rows) {
      pInfo->currentOffset -= pBlock->info.rows;
    } else {
      if (pInfo->currentOffset == 0) {
        break;
      }

      int32_t remain = (int32_t)(pBlock->info.rows - pInfo->currentOffset);
      pBlock->info.rows = remain;

      // move the remain rows of this data block to the front.
      for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
        SColumnInfoData *pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

        int16_t bytes = pColInfoData->info.bytes;
        memmove(pColInfoData->pData, pColInfoData->pData + bytes * pInfo->currentOffset, remain * bytes);
      }

      pInfo->currentOffset = 0;
      break;
    }
  }

  if (pInfo->slimit.limit > 0 && pInfo->groupTotal > pInfo->slimit.limit) {  // reach the group limit, abort
    return NULL;
  }

  if (pInfo->limit.limit > 0 && (pInfo->rowsTotal + pBlock->info.rows >= pInfo->limit.limit)) {
    pBlock->info.rows = (int32_t)(pInfo->limit.limit - pInfo->rowsTotal);
    pInfo->rowsTotal = pInfo->limit.limit;

    if (pInfo->slimit.limit > 0 && pInfo->groupTotal >= pInfo->slimit.limit) {
      pOperator->status = OP_EXEC_DONE;
    }

    //    setQueryStatus(pOperator->pRuntimeEnv, QUERY_COMPLETED);
  } else {
    pInfo->rowsTotal += pBlock->info.rows;
  }

  return pBlock;
}
