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

#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "streamexecutorInt.h"
#include "tdatablock.h"
#include "ttime.h"
#include "virtualtablescan.h"
#include "tsort.h"

#define STREAM_VTABLE_MERGE_OP_NAME "StreamVtableMergeOperator"
#define STREAM_VTABLE_MERGE_OP_CHECKPOINT_NAME "StreamVtableMergeOperator_Checkpoint"

typedef struct SVirtualTableScanInfo {
  STableScanBase base;
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SHashObj*      dataSlotMap;
  int32_t        tsSlotId;
  bool           scanAllCols;
  SArray*        pSortCtxList;
} SVirtualTableScanInfo;

typedef struct SVirtualScanMergeOperatorInfo {
  SOptrBasicInfo        binfo;
  EMergeType            type;
  SVirtualTableScanInfo virtualScanInfo;
  SLimitInfo            limitInfo;
  bool                  ignoreGroupId;
  uint64_t              groupId;
  STupleHandle*         pSavedTuple;
} SVirtualScanMergeOperatorInfo;

typedef struct SLoadNextCtx {
  SOperatorInfo*  pOperator;
  SOperatorParam* pOperatorGetParam;
  int32_t         blockId;
  STimeWindow     window;
  SSDataBlock*    pIntermediateBlock;
  col_id_t        tsSlotId;
} SLoadNextCtx;

int32_t virtualScanloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  int32_t        code = TSDB_CODE_SUCCESS;

  VTS_ERR_JRET(pOperator->fpSet.getNextFn(pOperator, ppBlock));
  VTS_ERR_JRET(blockDataCheck(*ppBlock));

  return code;
_return:
  qError("failed to load data block from downstream, %s code:%s", __func__, tstrerror(code));
  return code;
}

int32_t getTimeWindowOfBlock(SSDataBlock *pBlock, col_id_t tsSlotId, int64_t *startTs, int64_t *endTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tsIndex = -1;
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
    if (((SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i))->info.colId == tsSlotId) {
      tsIndex = i;
      break;
    }
  }

  if (tsIndex == -1) {
    tsIndex = (int32_t)taosArrayGetSize(pBlock->pDataBlock) - 1;
  }

  SColumnInfoData *pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, tsIndex);
  QUERY_CHECK_NULL(pColData, code, lino, _return, terrno);

  GET_TYPED_DATA(*startTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, 0), 0);
  GET_TYPED_DATA(*endTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, pBlock->info.rows - 1), 0);

  return code;
_return:
  qError("failed to get time window of block, %s code:%s", __func__, tstrerror(code));
  return code;
}

int32_t virtualScanloadNextDataBlockFromParam(void* param, SSDataBlock** ppBlock) {
  SLoadNextCtx*           pCtx = (SLoadNextCtx*)param;
  SOperatorInfo*          pOperator = pCtx->pOperator;
  SOperatorParam*         pOperatorGetParam = pCtx->pOperatorGetParam;
  int32_t                 code = TSDB_CODE_SUCCESS;
  SSDataBlock*            pRes = NULL;
  SExchangeOperatorParam* pParam = (SExchangeOperatorParam*)pOperatorGetParam->value;

  pParam->basic.window = pCtx->window;
  pOperator->status = OP_NOT_OPENED;
  if (pCtx->pIntermediateBlock) {
    blockDataDestroy(pCtx->pIntermediateBlock);
    pCtx->pIntermediateBlock = NULL;
  }

  VTS_ERR_JRET(pOperator->fpSet.getNextExtFn(pOperator, pOperatorGetParam, &pRes));

  VTS_ERR_JRET(blockDataCheck(pRes));
  if ((pRes)) {
    qInfo("load from downstream, blockId:%d", pCtx->blockId);
    //printDataBlock(pRes, "load from downstream", "task");
    (pRes)->info.id.blockId = pCtx->blockId;
    getTimeWindowOfBlock(pRes, pCtx->tsSlotId, &pCtx->window.skey, &pCtx->window.ekey);
    VTS_ERR_JRET(createOneDataBlock(pRes, true, &pCtx->pIntermediateBlock));
    *ppBlock = pCtx->pIntermediateBlock;
  } else {
    pCtx->window.ekey = INT64_MAX;
    *ppBlock = NULL;
  }

  return code;
_return:
  qError("failed to load data block from downstream, %s code:%s", __func__, tstrerror(code));
  return code;
}

int32_t makeTSMergeKey(SNodeList** pMergeKeys, col_id_t tsSlotId) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SNodeList        *pNodeList = NULL;
  SColumnNode      *pColumnNode = NULL;
  SOrderByExprNode *pOrderByExprNode = NULL;

  VTS_ERR_JRET(nodesMakeList(&pNodeList));

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumnNode));
  pColumnNode->slotId = tsSlotId;

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExprNode));
  pOrderByExprNode->pExpr = (SNode*)pColumnNode;
  pOrderByExprNode->order = ORDER_ASC;
  pOrderByExprNode->nullOrder = NULL_ORDER_FIRST;

  VTS_ERR_JRET(nodesListAppend(pNodeList, (SNode*)pOrderByExprNode));

  *pMergeKeys = pNodeList;
  return code;
_return:
  nodesDestroyNode((SNode*)pColumnNode);
  nodesDestroyNode((SNode*)pOrderByExprNode);
  nodesDestroyList(pNodeList);
  return code;
}

void cleanUpVirtualScanInfo(SVirtualTableScanInfo* pVirtualScanInfo) {
  if (pVirtualScanInfo->pSortInfo) {
    taosArrayDestroy(pVirtualScanInfo->pSortInfo);
    pVirtualScanInfo->pSortInfo = NULL;
  }
  if (pVirtualScanInfo->pSortHandle) {
    tsortDestroySortHandle(pVirtualScanInfo->pSortHandle);
    pVirtualScanInfo->pSortHandle = NULL;
  }
  if (pVirtualScanInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pVirtualScanInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pVirtualScanInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pVirtualScanInfo->pSortCtxList);
  }
}

int32_t createSortHandleFromParam(SOperatorInfo* pOperator) {
  int32_t                         code = TSDB_CODE_SUCCESS;
  int32_t                         lino = 0;
  SVirtualScanMergeOperatorInfo*  pInfo = pOperator->info;
  SVirtualTableScanInfo*          pVirtualScanInfo = &pInfo->virtualScanInfo;
  SVTableScanOperatorParam *      pParam = (SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  pVirtualScanInfo->sortBufSize = pVirtualScanInfo->bufPageSize * (taosArrayGetSize((pParam)->pOpParamArray) + 1);
  int32_t                         numOfBufPage = (int32_t)pVirtualScanInfo->sortBufSize / pVirtualScanInfo->bufPageSize;
  SNodeList*                      pMergeKeys = NULL;
  SSortSource*                    ps = NULL;

  cleanUpVirtualScanInfo(pVirtualScanInfo);
  VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, pVirtualScanInfo->tsSlotId));
  pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno);
  nodesDestroyList(pMergeKeys);

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pVirtualScanInfo->bufPageSize,
                                     numOfBufPage, pVirtualScanInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle, virtualScanloadNextDataBlockFromParam, NULL, NULL);

  pOperator->pDownstream[0]->status = OP_NOT_OPENED;
  pVirtualScanInfo->pSortCtxList = taosArrayInit(taosArrayGetSize((pParam)->pOpParamArray), POINTER_BYTES);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortCtxList, code, lino, _return, terrno);
  for (int32_t i = 0; i < taosArrayGetSize((pParam)->pOpParamArray); i++) {
    SOperatorParam* pOpParam = *(SOperatorParam**)taosArrayGet((pParam)->pOpParamArray, i);
    SLoadNextCtx*   pCtx = NULL;
    ps = NULL;

    pCtx = taosMemoryMalloc(sizeof(SLoadNextCtx));
    QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno);
    pCtx->blockId = i;
    pCtx->pOperator = pOperator->pDownstream[0];
    pCtx->pOperatorGetParam = pOpParam;
    pCtx->window = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};
    pCtx->pIntermediateBlock = NULL;
    pCtx->tsSlotId = (col_id_t)pVirtualScanInfo->tsSlotId;

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    QUERY_CHECK_NULL(ps, code, lino, _return, terrno);

    ps->param = pCtx;
    ps->onlyRef = true;

    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno);
  }

  VTS_ERR_JRET(tsortOpen(pVirtualScanInfo->pSortHandle));

  return code;
_return:
  if (code != 0){
    qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  nodesDestroyList(pMergeKeys);
  if (ps != NULL) {
    taosMemoryFree(ps);
  }
  return code;
}

int32_t createSortHandle(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo * pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  SVirtualTableScanInfo*          pVirtualScanInfo = &pInfo->virtualScanInfo;
  int32_t                         numOfBufPage = (int32_t)pVirtualScanInfo->sortBufSize / pVirtualScanInfo->bufPageSize;
  SSortSource*                    ps = NULL;
  int32_t                         code = 0;
  int32_t                         lino = 0;

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pVirtualScanInfo->bufPageSize,
                                     numOfBufPage, pVirtualScanInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle, virtualScanloadNextDataBlock, NULL, NULL);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      VTS_ERR_JRET(pDownstream->fpSet._openFn(pDownstream));
    } else {
      VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INVALID_DOWNSTREAM);
    }

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    TSDB_CHECK_NULL(ps, code, lino, _return, terrno);

    ps->param = pDownstream;
    ps->onlyRef = true;

    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    ps = NULL;
  }

  VTS_ERR_JRET(tsortOpen(pVirtualScanInfo->pSortHandle));

_return:
  if (code != 0){
    qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  if (ps != NULL) {
    taosMemoryFree(ps);
  }
  return code;
}

int32_t openVirtualTableScanOperatorImpl(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo * pInfo = pOperator->info;
  int32_t                         code = 0;
  int32_t                         lino = 0;

  if (pOperator->numOfDownstream == 0) {
    return code;
  }

  if (pOperator->pOperatorGetParam) {
    VTS_ERR_JRET(createSortHandleFromParam(pOperator));
  } else {
    VTS_ERR_JRET(createSortHandle(pOperator));
  }

  return code;

_return:
  qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  return code;
}

int32_t openVirtualTableScanOperator(SOperatorInfo* pOperator) {
  int32_t code = 0;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t startTs = taosGetTimestampUs();

  code = openVirtualTableScanOperatorImpl(pOperator);

  pOperator->cost.openCost = (double)(taosGetTimestampUs() - startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  VTS_ERR_RET(code);

  OPTR_SET_OPENED(pOperator);
  return code;
}

static int32_t doGetVtableMergedBlockData(SVirtualScanMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                          SSDataBlock* p) {
  int32_t code = 0;
  int64_t lastTs = 0;
  int64_t rowNums = -1;
  blockDataEmpty(p);
  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (!pInfo->pSavedTuple) {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      if (pTupleHandle == NULL || (code != 0)) {
        break;
      }
    } else {
      pTupleHandle = pInfo->pSavedTuple;
      pInfo->pSavedTuple = NULL;
    }

    SDataBlockInfo info = {0};
    tsortGetBlockInfo(pTupleHandle, &info);
    int32_t blockId = (int32_t)info.id.blockId;

    for (int32_t i = 0; i < (pInfo->virtualScanInfo.scanAllCols ? 1 : tsortGetColNum(pTupleHandle)); i++) {
      bool isNull = tsortIsNullVal(pTupleHandle, i);
      if (isNull) {
        colDataSetNULL(taosArrayGet(p->pDataBlock, i), rowNums);
      } else {
        char* pData = NULL;
        tsortGetValue(pTupleHandle, i, (void**)&pData);

        if (pData != NULL) {
          if (i == 0) {
            if (lastTs != *(int64_t*)pData) {
              if (rowNums >= capacity - 1) {
                pInfo->pSavedTuple = pTupleHandle;
                goto _return;
              }
              rowNums++;
              for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
                colDataSetNULL(taosArrayGet(p->pDataBlock, j), rowNums);
              }
              if (pInfo->virtualScanInfo.tsSlotId != -1) {
                VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
              }
              lastTs = *(int64_t*)pData;
            }
            continue;
          }
          int32_t slotKey = blockId << 16 | i;
          void *slotId = taosHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
          if (slotId == NULL) {
            qError("failed to get slotId from dataSlotMap, blockId:%d, slotId:%d", blockId, i);
            VTS_ERR_RET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
          }
          VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, *(int32_t *)slotId), rowNums, pData, false));
        }
      }
    }
  }
_return:
  p->info.rows = rowNums + 1;
  p->info.dataLoad = 1;
  p->info.scanFlag = MAIN_SCAN;
  return code;
}

static int32_t doGetVStableMergedBlockData(SVirtualScanMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                           SSDataBlock* p) {
  int32_t code = 0;
  int64_t lastTs = 0;
  int64_t rowNums = -1;
  blockDataEmpty(p);
  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (!pInfo->pSavedTuple) {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      if (pTupleHandle == NULL || (code != 0)) {
        break;
      }
    } else {
      pTupleHandle = pInfo->pSavedTuple;
      pInfo->pSavedTuple = NULL;
    }

    int32_t tsIndex = -1;

    for (int32_t i = 0; i < tsortGetColNum(pTupleHandle); i++) {
      if (tsortIsNullVal(pTupleHandle, i)) {
        continue;
      } else {
        SColumnInfoData *pColInfo = NULL;
        tsortGetColumnInfo(pTupleHandle, i, &pColInfo);
        if (pColInfo->info.slotId ==  pInfo->virtualScanInfo.tsSlotId) {
          tsIndex = i;
          break;
        }
      }
    }

    if (tsIndex == -1) {
      tsIndex = (int32_t)tsortGetColNum(pTupleHandle) - 1;
    }

    char* pData = NULL;
    // first, set ts slot's data
    // then, set other slots' data
    tsortGetValue(pTupleHandle, tsIndex, (void**)&pData);

    if (pData != NULL) {
      if (lastTs != *(int64_t*)pData) {
        if (rowNums >= capacity - 1) {
          pInfo->pSavedTuple = pTupleHandle;
          goto _return;
        }
        rowNums++;
        for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
          colDataSetNULL(taosArrayGet(p->pDataBlock, j), rowNums);
        }
        if (pInfo->virtualScanInfo.tsSlotId != -1) {
          VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
        }
        lastTs = *(int64_t*)pData;
      }
    }
    if (pInfo->virtualScanInfo.scanAllCols) {
      continue;
    }

    for (int32_t i = 0; i < tsortGetColNum(pTupleHandle); i++) {
      if (i == tsIndex || tsortIsNullVal(pTupleHandle, i)) {
        continue;
      }

      SColumnInfoData *pColInfo = NULL;
      tsortGetColumnInfo(pTupleHandle, i, &pColInfo);
      tsortGetValue(pTupleHandle, i, (void**)&pData);

      if (pData != NULL) {
        VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, i), rowNums, pData, false));
      }
    }
  }
_return:
  p->info.rows = rowNums + 1;
  p->info.dataLoad = 1;
  p->info.scanFlag = MAIN_SCAN;
  return code;
}

int32_t doVirtualTableMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = 0;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SSortHandle*                   pHandle = pVirtualScanInfo->pSortHandle;
  SSDataBlock*                   pDataBlock = pInfo->binfo.pRes;
  int32_t                        capacity = pOperator->resultInfo.capacity;

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  blockDataCleanup(pDataBlock);

  if (pHandle == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pVirtualScanInfo->pIntermediateBlock == NULL) {
    VTS_ERR_RET(tsortGetSortedDataBlock(pHandle, &pVirtualScanInfo->pIntermediateBlock));
    if (pVirtualScanInfo->pIntermediateBlock == NULL) {
      return TSDB_CODE_SUCCESS;
    }

    VTS_ERR_RET(blockDataEnsureCapacity(pVirtualScanInfo->pIntermediateBlock, capacity));
  } else {
    blockDataCleanup(pVirtualScanInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pVirtualScanInfo->pIntermediateBlock;
  if (pOperator->pOperatorGetParam) {
    VTS_ERR_RET(doGetVStableMergedBlockData(pInfo, pHandle, capacity, p));
  } else {
    VTS_ERR_RET(doGetVtableMergedBlockData(pInfo, pHandle, capacity, p));
  }

  VTS_ERR_RET(copyDataBlock(pDataBlock, p));
  qDebug("%s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64 , GET_TASKID(pTaskInfo), pDataBlock->info.id.groupId,
         pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

static int32_t doSetTagColumnData(STableScanBase * pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo,
                                  int32_t rows) {
  int32_t    code = 0;
  SExprSupp* pSup = &pTableScanInfo->pseudoSup;
  if (pSup->numOfExprs > 0) {
    code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock, rows,
                                  pTaskInfo, &pTableScanInfo->metaCache);
    // ignore the table not exists error, since this table may have been dropped during the scan procedure.
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      if (pTaskInfo->streamInfo.pState) blockDataCleanup(pBlock);
      code = 0;
    }
  }

  return code;
}

int32_t virtualTableGetNext(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    pResBlock = NULL;
    return code;
  }

  VTS_ERR_JRET(pOperator->fpSet._openFn(pOperator));

  while(1) {
    VTS_ERR_JRET(doVirtualTableMerge(pOperator, pResBlock));
    if (*pResBlock == NULL) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pOperator->pOperatorGetParam) {
      uint64_t uid = ((SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value)->uid;
      STableKeyInfo* tbInfo = tableListGetInfo(pVirtualScanInfo->base.pTableListInfo, tableListFind(pVirtualScanInfo->base.pTableListInfo, uid, 0));
      QUERY_CHECK_NULL(tbInfo, code, lino, _return, terrno);
      (*pResBlock)->info.id.uid = tbInfo->uid;
    } else {
      STableKeyInfo* tbInfo = tableListGetInfo(pVirtualScanInfo->base.pTableListInfo, 0);
      QUERY_CHECK_NULL(tbInfo, code, lino, _return, terrno);
      (*pResBlock)->info.id.uid = tbInfo->uid;
    }

    VTS_ERR_JRET(doSetTagColumnData(&pVirtualScanInfo->base, (*pResBlock), pTaskInfo, (*pResBlock)->info.rows));
    VTS_ERR_JRET(doFilter(*pResBlock, pOperator->exprSupp.pFilterInfo, NULL));
    if ((*pResBlock)->info.rows > 0) {
      break;
    }
  }

  return code;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static void destroyTableScanBase(STableScanBase* pBase, TsdReader* pAPI) {
  cleanupQueryTableDataCond(&pBase->cond);

  if (pAPI->tsdReaderClose) {
    pAPI->tsdReaderClose(pBase->dataReader);
  }
  pBase->dataReader = NULL;

  if (pBase->matchInfo.pList != NULL) {
    taosArrayDestroy(pBase->matchInfo.pList);
  }

  tableListDestroy(pBase->pTableListInfo);
  taosLRUCacheCleanup(pBase->metaCache.pTableMetaEntryCache);
  cleanupExprSupp(&pBase->pseudoSup);
}

void destroyVirtualTableScanOperatorInfo(void* param) {
  if (!param) {
    return;
  }
  SVirtualScanMergeOperatorInfo* pOperatorInfo = (SVirtualScanMergeOperatorInfo*)param;
  SVirtualTableScanInfo* pInfo = &pOperatorInfo->virtualScanInfo;
  blockDataDestroy(pOperatorInfo->binfo.pRes);
  pOperatorInfo->binfo.pRes = NULL;

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
  taosArrayDestroy(pInfo->pSortInfo);
  pInfo->pSortHandle = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = NULL;
  destroyTableScanBase(&pInfo->base, &pInfo->base.readerAPI);

  taosHashCleanup(pInfo->dataSlotMap);

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
  }
  taosMemoryFreeClear(param);
}

int32_t extractColMap(SNodeList* pNodeList, SHashObj** pSlotMap, int32_t *tsSlotId) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (numOfCols == 0) {
    return code;
  }

  *tsSlotId = -1;
  *pSlotMap = taosHashInit(numOfCols, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(*pSlotMap, code, lino, _return, terrno);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pNodeList, i);
    TSDB_CHECK_NULL(pColNode, code, lino, _return, terrno);

    if (pColNode->isPrimTs) {
      *tsSlotId = i;
    } else if (pColNode->hasRef) {
      int32_t slotKey = pColNode->dataBlockId << 16 | pColNode->slotId;
      VTS_ERR_JRET(taosHashPut(*pSlotMap, &slotKey, sizeof(slotKey), &i, sizeof(i)));
    }
  }

  return code;
_return:
  taosHashCleanup(*pSlotMap);
  *pSlotMap = NULL;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createVirtualTableMergeOperatorInfo(SOperatorInfo** pDownstream, SReadHandle* readHandle,
                                            STableListInfo* pTableListInfo, int32_t numOfDownstream,
                                            SVirtualScanPhysiNode* pVirtualScanPhyNode, SExecTaskInfo* pTaskInfo,
                                            SOperatorInfo** pOptrInfo) {
   SPhysiNode*                    pPhyNode = (SPhysiNode*)pVirtualScanPhyNode;
   int32_t                        lino = 0;
   int32_t                        code = TSDB_CODE_SUCCESS;
   SVirtualScanMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SVirtualScanMergeOperatorInfo));
   SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
   SDataBlockDescNode*            pDescNode = pPhyNode->pOutputDataBlockDesc;
   SNodeList*                     pMergeKeys = NULL;

   QUERY_CHECK_NULL(pInfo, code, lino, _return, terrno);
   QUERY_CHECK_NULL(pOperator, code, lino, _return, terrno);

   pInfo->binfo.inputTsOrder = pVirtualScanPhyNode->scan.node.inputTsOrder;
   pInfo->binfo.outputTsOrder = pVirtualScanPhyNode->scan.node.outputTsOrder;

   SVirtualTableScanInfo* pVirtualScanInfo = &pInfo->virtualScanInfo;
   pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
   TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _return, terrno);

   SSDataBlock* pInputBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
   TSDB_CHECK_NULL(pInputBlock, code, lino, _return, terrno);
   pVirtualScanInfo->pInputBlock = pInputBlock;

   if (pVirtualScanPhyNode->scan.pScanPseudoCols != NULL) {
     SExprSupp* pSup = &pVirtualScanInfo->base.pseudoSup;
     pSup->pExprInfo = NULL;
     VTS_ERR_JRET(createExprInfo(pVirtualScanPhyNode->scan.pScanPseudoCols, NULL, &pSup->pExprInfo, &pSup->numOfExprs));

     pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset,
                                       &pTaskInfo->storageAPI.functionStore);
     TSDB_CHECK_NULL(pSup->pCtx, code, lino, _return, terrno);
   }

   initResultSizeInfo(&pOperator->resultInfo, 1024);
   TSDB_CHECK_CODE(blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity), lino, _return);

   size_t     numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
   int32_t    rowSize = pInfo->binfo.pRes->info.rowSize;

   if (!pVirtualScanPhyNode->scan.node.dynamicOp) {
     VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, 0));
     pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
     TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno);
   }
   pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
   pVirtualScanInfo->sortBufSize =
       pVirtualScanInfo->bufPageSize * (numOfDownstream + 1);  // one additional is reserved for merged result.
   VTS_ERR_JRET(extractColMap(pVirtualScanPhyNode->pTargets, &pVirtualScanInfo->dataSlotMap, &pVirtualScanInfo->tsSlotId));

   pVirtualScanInfo->scanAllCols = pVirtualScanPhyNode->scanAllCols;

   VTS_ERR_JRET(filterInitFromNode((SNode*)pVirtualScanPhyNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0));

   pVirtualScanInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
   QUERY_CHECK_NULL(pVirtualScanInfo->base.metaCache.pTableMetaEntryCache, code, lino, _return, terrno);
   pVirtualScanInfo->base.readHandle = *readHandle;
   pVirtualScanInfo->base.pTableListInfo = pTableListInfo;

   setOperatorInfo(pOperator, "VirtualTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                   OP_NOT_OPENED, pInfo, pTaskInfo);
   pOperator->fpSet =
       createOperatorFpSet(openVirtualTableScanOperator, virtualTableGetNext, NULL, destroyVirtualTableScanOperatorInfo,
                           optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

   if (NULL != pDownstream) {
     VTS_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));
   }


   nodesDestroyList(pMergeKeys);
   *pOptrInfo = pOperator;
   return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
   if (pInfo != NULL) {
     destroyVirtualTableScanOperatorInfo(pInfo);
   }
   nodesDestroyList(pMergeKeys);
   pTaskInfo->code = code;
   destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
   return code;
}

static int32_t doStreamVtableMergeNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  // NOTE: this operator does never check if current status is done or not
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  const char*    id = GET_TASKID(pTaskInfo);

  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;

  qDebug("stream scan started, %s", id);

  // TODO(kjq): add fill history recover step

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
// TODO: refactor
FETCH_NEXT_BLOCK:
  if (pInfo->blockType == STREAM_INPUT__DATA_BLOCK) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      (*ppRes) = NULL;
      return code;
    }

    int32_t current = pInfo->validBlockIndex++;
    qDebug("process %d/%d input data blocks, %s", current, (int32_t)total, id);

    SPackedData* pPacked = taosArrayGet(pInfo->pBlockLists, current);
    QUERY_CHECK_NULL(pPacked, code, lino, _end, terrno);

    SSDataBlock* pBlock = pPacked->pDataBlock;
    if (pBlock->info.parTbName[0]) {
      code =
          pAPI->stateStore.streamStatePutParName(pStreamInfo->pState, pBlock->info.id.groupId, pBlock->info.parTbName);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // TODO move into scan
    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    pBlock->info.dataLoad = 1;
    if (pInfo->pUpdateInfo) {
      pInfo->pUpdateInfo->maxDataVersion = TMAX(pInfo->pUpdateInfo->maxDataVersion, pBlock->info.version);
    }

    code = blockDataUpdateTsWindow(pBlock, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_GET_ALL:
        printDataBlock(pBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        setStreamOperatorState(&pInfo->basic, pBlock->info.type);
        (*ppRes) = pBlock;
        return code;
      case STREAM_RETRIEVE: {
        pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RETRIEVE;
        code = copyDataBlock(pInfo->pUpdateRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        pInfo->updateResIndex = 0;
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
        pAPI->stateStore.updateInfoAddCloseWindowSBF(pInfo->pUpdateInfo);
      } break;
      case STREAM_DELETE_DATA: {
        printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "delete recv", GET_TASKID(pTaskInfo));
        SSDataBlock* pDelBlock = NULL;
        if (pInfo->tqReader) {
          code = createSpecialDataBlock(STREAM_DELETE_DATA, &pDelBlock);
          QUERY_CHECK_CODE(code, lino, _end);

          code = filterDelBlockByUid(pDelBlock, pBlock, pInfo->tqReader, &pInfo->readerFn);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          pDelBlock = pBlock;
        }

        code = setBlockGroupIdByUid(pInfo, pDelBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        code = rebuildDeleteBlockData(pDelBlock, &pStreamInfo->fillHistoryWindow, id);
        QUERY_CHECK_CODE(code, lino, _end);
        printSpecDataBlock(pDelBlock, getStreamOpName(pOperator->operatorType), "delete recv filtered",
                           GET_TASKID(pTaskInfo));
        if (pDelBlock->info.rows == 0) {
          if (pInfo->tqReader) {
            blockDataDestroy(pDelBlock);
          }
          goto FETCH_NEXT_BLOCK;
        }

        if (!isStreamWindow(pInfo)) {
          code = generateDeleteResultBlock(pInfo, pDelBlock, pInfo->pDeleteDataRes);
          QUERY_CHECK_CODE(code, lino, _end);
          if (pInfo->partitionSup.needCalc) {
            pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
          } else {
            pInfo->pDeleteDataRes->info.type = STREAM_DELETE_RESULT;
          }
          blockDataDestroy(pDelBlock);

          if (pInfo->pDeleteDataRes->info.rows > 0) {
            printSpecDataBlock(pInfo->pDeleteDataRes, getStreamOpName(pOperator->operatorType), "delete result",
                               GET_TASKID(pTaskInfo));
            setStreamOperatorState(&pInfo->basic, pInfo->pDeleteDataRes->info.type);
            (*ppRes) = pInfo->pDeleteDataRes;
            return code;
          } else {
            goto FETCH_NEXT_BLOCK;
          }
        } else {
          pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
          pInfo->updateResIndex = 0;
          code = generateScanRange(pInfo, pDelBlock, pInfo->pUpdateRes, STREAM_DELETE_DATA);
          QUERY_CHECK_CODE(code, lino, _end);
          prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
          code = copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
          QUERY_CHECK_CODE(code, lino, _end);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
          if (pInfo->tqReader) {
            blockDataDestroy(pDelBlock);
          }
          if (pInfo->pDeleteDataRes->info.rows > 0) {
            pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
            printSpecDataBlock(pInfo->pDeleteDataRes, getStreamOpName(pOperator->operatorType), "delete result",
                               GET_TASKID(pTaskInfo));
            setStreamOperatorState(&pInfo->basic, pInfo->pDeleteDataRes->info.type);
            (*ppRes) = pInfo->pDeleteDataRes;
            return code;
          } else {
            goto FETCH_NEXT_BLOCK;
          }
        }
      } break;
      case STREAM_GET_RESULT: {
        pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
        pInfo->updateResIndex = 0;
        pInfo->lastScanRange = pBlock->info.window;
        TSKEY endKey = taosTimeGetIntervalEnd(pBlock->info.window.skey, &pInfo->interval);
        if (pInfo->useGetResultRange == true) {
          endKey = pBlock->info.window.ekey;
        }
        code = copyGetResultBlock(pInfo->pUpdateRes, pBlock->info.window.skey, endKey);
        QUERY_CHECK_CODE(code, lino, _end);
        pInfo->pUpdateInfo->maxDataVersion = -1;
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
      } break;
      case STREAM_DROP_CHILD_TABLE: {
        int32_t deleteNum = 0;
        code = deletePartName(&pInfo->stateStore, pInfo->pStreamScanOp->pTaskInfo->streamInfo.pState, pBlock, &deleteNum);
        QUERY_CHECK_CODE(code, lino, _end);
        if (deleteNum == 0) {
          printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "block recv", GET_TASKID(pTaskInfo));
          qDebug("===stream=== ignore block type 18, delete num is 0");
          goto FETCH_NEXT_BLOCK;
        }
      } break;
      case STREAM_CHECKPOINT: {
        qError("stream check point error. msg type: STREAM_INPUT__DATA_BLOCK");
      } break;
      default:
        break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "block recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);
    (*ppRes) = pBlock;
    return code;
  } else if (pInfo->blockType == STREAM_INPUT__DATA_SUBMIT) {
    qDebug("stream scan mode:%d, %s", pInfo->scanMode, id);
    switch (pInfo->scanMode) {
      case STREAM_SCAN_FROM_RES: {
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
        code = doCheckUpdate(pInfo, pInfo->pRes->info.window.ekey, pInfo->pRes);
        QUERY_CHECK_CODE(code, lino, _end);
        setStreamOperatorState(&pInfo->basic, pInfo->pRes->info.type);
        code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
        pInfo->pRes->info.dataLoad = 1;
        code = blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pInfo->pRes->info.rows > 0) {
          printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
          (*ppRes) = pInfo->pRes;
          return code;
        }
      } break;
      case STREAM_SCAN_FROM_DELETE_DATA: {
        code = generateScanRange(pInfo, pInfo->pUpdateDataRes, pInfo->pUpdateRes, STREAM_PARTITION_DELETE_DATA);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pInfo->pUpdateRes->info.rows > 0) {
          prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
          pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
          code = copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
          QUERY_CHECK_CODE(code, lino, _end);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
          (*ppRes) = pInfo->pDeleteDataRes;
          return code;
        }
        qError("%s===stream=== %s failed at line %d since pInfo->pUpdateRes is empty", GET_TASKID(pTaskInfo), __func__,
               __LINE__);
        blockDataCleanup(pInfo->pUpdateDataRes);
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      } break;
      case STREAM_SCAN_FROM_UPDATERES: {
        code = generateScanRange(pInfo, pInfo->pUpdateDataRes, pInfo->pUpdateRes, STREAM_CLEAR);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pInfo->pUpdateRes->info.rows > 0) {
          prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
          pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
          (*ppRes) = pInfo->pUpdateRes;
          return code;
        }
        qError("%s===stream=== %s failed at line %d since pInfo->pUpdateRes is empty", GET_TASKID(pTaskInfo), __func__,
               __LINE__);
        blockDataCleanup(pInfo->pUpdateDataRes);
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      } break;
      case STREAM_SCAN_FROM_DATAREADER_RANGE:
      case STREAM_SCAN_FROM_DATAREADER_RETRIEVE: {
        if (pInfo->pRangeScanRes != NULL) {
          (*ppRes) = pInfo->pRangeScanRes;
          pInfo->pRangeScanRes = NULL;
          return code;
        }
        SSDataBlock* pSDB = NULL;
        code = doRangeScan(pInfo, pInfo->pUpdateRes, pInfo->primaryTsIndex, &pInfo->updateResIndex, &pSDB);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pSDB) {
          STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
          pSDB->info.type = pInfo->scanMode == STREAM_SCAN_FROM_DATAREADER_RANGE ? STREAM_NORMAL : STREAM_PULL_DATA;
          if (!pInfo->igCheckUpdate && pInfo->pUpdateInfo) {
            code = checkUpdateData(pInfo, true, pSDB, false);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          printSpecDataBlock(pSDB, getStreamOpName(pOperator->operatorType), "update", GET_TASKID(pTaskInfo));
          code = calBlockTbName(pInfo, pSDB, 0);
          QUERY_CHECK_CODE(code, lino, _end);

          if (pInfo->pCreateTbRes->info.rows > 0) {
            printSpecDataBlock(pInfo->pCreateTbRes, getStreamOpName(pOperator->operatorType), "update",
                               GET_TASKID(pTaskInfo));
            (*ppRes) = pInfo->pCreateTbRes;
            pInfo->pRangeScanRes = pSDB;
            return code;
          }

          (*ppRes) = pSDB;
          return code;
        }
        blockDataCleanup(pInfo->pUpdateDataRes);
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      } break;
      default:
        break;
    }

    if (hasScanRange(pInfo)) {
      pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
      pInfo->updateResIndex = 0;
      SStreamAggSupporter* pSup = pInfo->windowSup.pStreamAggSup;
      code = copyDataBlock(pInfo->pUpdateRes, pSup->pScanBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      blockDataCleanup(pSup->pScanBlock);
      prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
      pInfo->pUpdateRes->info.type = STREAM_DELETE_DATA;
      printSpecDataBlock(pInfo->pUpdateRes, getStreamOpName(pOperator->operatorType), "rebuild", GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pUpdateRes;
      return code;
    }

    SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
    int32_t         totalBlocks = taosArrayGetSize(pInfo->pBlockLists);

  NEXT_SUBMIT_BLK:
    while (1) {
      if (pInfo->readerFn.tqReaderCurrentBlockConsumed(pInfo->tqReader)) {
        if (pInfo->validBlockIndex >= totalBlocks) {
          pAPI->stateStore.updateInfoDestoryColseWinSBF(pInfo->pUpdateInfo);
          doClearBufferedBlocks(pInfo);

          qDebug("stream scan return empty, all %d submit blocks consumed, %s", totalBlocks, id);
          (*ppRes) = NULL;
          return code;
        }

        int32_t      current = pInfo->validBlockIndex++;
        SPackedData* pSubmit = taosArrayGet(pInfo->pBlockLists, current);
        QUERY_CHECK_NULL(pSubmit, code, lino, _end, terrno);

        qDebug("set %d/%d as the input submit block, %s", current + 1, totalBlocks, id);
        if (pAPI->tqReaderFn.tqReaderSetSubmitMsg(pInfo->tqReader, pSubmit->msgStr, pSubmit->msgLen, pSubmit->ver,
                                                  NULL) < 0) {
          qError("submit msg messed up when initializing stream submit block %p, current %d/%d, %s", pSubmit, current,
                 totalBlocks, id);
          continue;
        }
      }

      blockDataCleanup(pInfo->pRes);

      while (pAPI->tqReaderFn.tqNextBlockImpl(pInfo->tqReader, id)) {
        SSDataBlock* pRes = NULL;

        code = pAPI->tqReaderFn.tqRetrieveBlock(pInfo->tqReader, &pRes, id);
        qDebug("retrieve data from submit completed code:%s rows:%" PRId64 " %s", tstrerror(code), pRes->info.rows, id);

        if (code != TSDB_CODE_SUCCESS || pRes->info.rows == 0) {
          qDebug("retrieve data failed, try next block in submit block, %s", id);
          continue;
        }

        code = setBlockIntoRes(pInfo, pRes, &pStreamInfo->fillHistoryWindow, false);
        if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
          pInfo->pRes->info.rows = 0;
          code = TSDB_CODE_SUCCESS;
        } else {
          QUERY_CHECK_CODE(code, lino, _end);
        }

        if (pInfo->pRes->info.rows == 0) {
          continue;
        }

        if (pInfo->pCreateTbRes->info.rows > 0) {
          pInfo->scanMode = STREAM_SCAN_FROM_RES;
          qDebug("create table res exists, rows:%" PRId64 " return from stream scan, %s",
                 pInfo->pCreateTbRes->info.rows, id);
          (*ppRes) = pInfo->pCreateTbRes;
          return code;
        }

        code = doCheckUpdate(pInfo, pBlockInfo->window.ekey, pInfo->pRes);
        QUERY_CHECK_CODE(code, lino, _end);
        setStreamOperatorState(&pInfo->basic, pInfo->pRes->info.type);
        code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
        QUERY_CHECK_CODE(code, lino, _end);

        code = blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
        QUERY_CHECK_CODE(code, lino, _end);

        int64_t numOfUpdateRes = pInfo->pUpdateDataRes->info.rows;
        qDebug("%s %" PRId64 " rows in datablock, update res:%" PRId64, id, pBlockInfo->rows, numOfUpdateRes);
        if (pBlockInfo->rows > 0 || numOfUpdateRes > 0) {
          break;
        }
      }

      if (pBlockInfo->rows > 0 || pInfo->pUpdateDataRes->info.rows > 0) {
        break;
      } else {
        continue;
      }
    }

    // record the scan action.
    pInfo->numOfExec++;
    pOperator->resultInfo.totalRows += pBlockInfo->rows;

    qDebug("stream scan completed, and return source rows:%" PRId64 ", %s", pBlockInfo->rows, id);
    if (pBlockInfo->rows > 0) {
      printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pRes;
      return code;
    }

    if (pInfo->pUpdateDataRes->info.rows > 0) {
      goto FETCH_NEXT_BLOCK;
    }

    goto NEXT_SUBMIT_BLK;
  } else if (pInfo->blockType == STREAM_INPUT__CHECKPOINT) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      (*ppRes) = NULL;
      return code;
    }

    int32_t current = pInfo->validBlockIndex++;
    qDebug("process %d/%d input data blocks, %s", current, (int32_t)total, id);

    SPackedData* pData = taosArrayGet(pInfo->pBlockLists, current);
    QUERY_CHECK_NULL(pData, code, lino, _end, terrno);
    SSDataBlock* pBlock = taosArrayGet(pData->pDataBlock, 0);
    QUERY_CHECK_NULL(pBlock, code, lino, _end, terrno);

    if (pBlock->info.type == STREAM_CHECKPOINT) {
      // todo(kjq): serialize checkpoint
    }
    // printDataBlock(pInfo->pCheckpointRes, "stream scan ck", GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pCheckpointRes;
    return code;
  } else {
    qError("stream scan error, invalid block type %d, %s", pInfo->blockType, id);
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = NULL;
  return code;
}

int32_t createStreamVtableMergeOperatorInfo(SReadHandle* pHandle, SVirtualScanPhysiNode* pVirtualScanNode,
                                            SNode* pTagCond, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamScanInfo));
  SOperatorInfo*   pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  SScanPhysiNode*     pScanPhyNode = &pVirtualScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;

  pInfo->pTagCond = pTagCond;
  pInfo->pGroupTags = pVirtualScanNode->pGroupTags;

  int32_t numOfCols = 0;
  code = extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SDataType pkType = {0};
  pInfo->primaryKeyIndex = -1;
  pInfo->basic.primaryPkIndex = -1;
  int32_t numOfOutput = taosArrayGetSize(pInfo->matchInfo.pList);

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColMatchItem* id = taosArrayGet(pInfo->matchInfo.pList, i);
    QUERY_CHECK_NULL(id, code, lino, _error, terrno);
    if (id->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pInfo->primaryTsIndex = id->dstSlotId;
    }
    if (id->isPk) {
      pInfo->primaryKeyIndex = id->dstSlotId;
      pInfo->basic.primaryPkIndex = id->dstSlotId;
      pkType = id->dataType;
    }
  }

  pInfo->pPartTbnameSup = NULL;
  if (pVirtualScanNode->pSubtable != NULL) {
    SExprInfo* pSubTableExpr = taosMemoryCalloc(1, sizeof(SExprInfo));
    if (pSubTableExpr == NULL) {
      code = terrno;
      goto _error;
    }

    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    code = createExprFromOneNode(pSubTableExpr, pVirtualScanNode->pSubtable, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    if (initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1, &pTaskInfo->storageAPI.functionStore) != 0) {
      goto _error;
    }
  }

  if (pVirtualScanNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExpr(pVirtualScanNode->pTags, &numOfTags);
    if (pTagExpr == NULL) {
      goto _error;
    }
    code = initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags, &pTaskInfo->storageAPI.functionStore);
    if (code != 0) {
      goto _error;
    }
  }
  // todo(kjq): add partition table name generation
  // todo(kjq): add tag column generation

  pInfo->pBlockLists = taosArrayInit(4, sizeof(SPackedData));
  QUERY_CHECK_NULL(pInfo->pBlockLists, code, lino, _error, terrno);

  // TODO(kjq): support virtual table re-scan from tsdb

  if (pTaskInfo->streamInfo.pState) {
    pAPI->stateStore.streamStateSetNumber(pTaskInfo->streamInfo.pState, -1, pInfo->primaryTsIndex);
  }

  pInfo->readHandle = *pHandle;
  pTaskInfo->streamInfo.snapshotVer = pHandle->version;
  pInfo->pCreateTbRes = buildCreateTableBlock(&pInfo->tbnameCalSup, &pInfo->tagCalSup);
  QUERY_CHECK_NULL(pInfo->pCreateTbRes, code, lino, _error, terrno);

  // create the pseduo columns info
  if (pVirtualScanNode->scan.pScanPseudoCols != NULL) {
    code = createExprInfo(pVirtualScanNode->scan.pScanPseudoCols, NULL, &pInfo->pPseudoExpr, &pInfo->numOfPseudoExpr);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pScanPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  code = createSpecialDataBlock(STREAM_CLEAR, &pInfo->pUpdateRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
  pInfo->windowSup = (SWindowSupporter){.pStreamAggSup = NULL, .gap = -1, .parentType = QUERY_NODE_PHYSICAL_PLAN};
  pInfo->groupId = 0;
  pInfo->igCheckGroupId = false;
  pInfo->pStreamScanOp = pOperator;
  pInfo->deleteDataIndex = 0;
  code = createSpecialDataBlock(STREAM_DELETE_DATA, &pInfo->pDeleteDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->updateWin = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MAX};
  code = createSpecialDataBlock(STREAM_CLEAR, &pInfo->pUpdateDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pInfo->primaryKeyIndex != -1) {
    pInfo->pUpdateDataRes->info.rowSize += pkType.bytes;
    SColumnInfoData infoData = {0};
    infoData.info.type = pkType.type;
    infoData.info.bytes = pkType.bytes;
    void* px = taosArrayPush(pInfo->pUpdateDataRes->pDataBlock, &infoData);
    QUERY_CHECK_NULL(px, code, lino, _error, terrno);

    pInfo->pkColType = pkType.type;
    pInfo->pkColLen = pkType.bytes;
  }

  pInfo->partitionSup.needCalc = false;
  pInfo->igCheckUpdate = pVirtualScanNode->igCheckUpdate;
  pInfo->igExpired = pVirtualScanNode->igExpired;
  pInfo->twAggSup.maxTs = INT64_MIN;
  pInfo->pState = pTaskInfo->streamInfo.pState;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->readerFn = pTaskInfo->storageAPI.tqReaderFn;
  pInfo->pFillSup = NULL;
  pInfo->useGetResultRange = false;
  pInfo->pRangeScanRes = NULL;

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->lastScanRange.skey = INT64_MIN;
  pInfo->lastScanRange.ekey = INT64_MIN;
  // for stream
  if (pTaskInfo->streamInfo.pState) {
    void*   buff = NULL;
    int32_t len = 0;
    int32_t res =
        pAPI->stateStore.streamStateGetInfo(pTaskInfo->streamInfo.pState, STREAM_VTABLE_MERGE_OP_CHECKPOINT_NAME,
                                            strlen(STREAM_VTABLE_MERGE_OP_CHECKPOINT_NAME), &buff, &len);
    if (res == TSDB_CODE_SUCCESS) {
      // todo(kjq): deserialize checkpoint here
      taosMemoryFree(buff);
    }
  }

  setOperatorInfo(pOperator, STREAM_VTABLE_MERGE_OP_NAME, QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamVtableMergeNext, NULL, destroyStreamScanOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  // TODO(kjq): save and load fill history state

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroyStreamScanOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}
