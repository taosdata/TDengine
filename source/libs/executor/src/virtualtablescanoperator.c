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
    qDebug("%s load from downstream, blockId:%d", __func__, pCtx->blockId);
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
          }
          int32_t slotKey = blockId << 16 | i;
          void*   slotId = taosHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
          if (slotId == NULL) {
            if (i == 0) {
              continue;
            } else {
              qError("failed to get slotId from dataSlotMap, blockId:%d, slotId:%d", blockId, i);
              VTS_ERR_RET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
            }
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

  size_t  numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  int32_t rowSize = pInfo->binfo.pRes->info.rowSize;

  if (!pVirtualScanPhyNode->scan.node.dynamicOp) {
    VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, 0));
    pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
    TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno);
  }
  pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
  pVirtualScanInfo->sortBufSize =
      pVirtualScanInfo->bufPageSize * (numOfDownstream + 1);  // one additional is reserved for merged result.
  VTS_ERR_JRET(
      extractColMap(pVirtualScanPhyNode->pTargets, &pVirtualScanInfo->dataSlotMap, &pVirtualScanInfo->tsSlotId));

  pVirtualScanInfo->scanAllCols = pVirtualScanPhyNode->scanAllCols;

  VTS_ERR_JRET(
      filterInitFromNode((SNode*)pVirtualScanPhyNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0));

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
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  const char*      id = GET_TASKID(pTaskInfo);
  SStreamScanInfo* pInfo = pOperator->info;
  SSDataBlock*     pResBlock = pInfo->pRes;
  SArray*          pVTables = pTaskInfo->streamInfo.pVTables;
  void*            pIter = NULL;

  (*ppRes) = NULL;
  if (pOperator->status == OP_EXEC_DONE) {
    goto _end;
  }

  qDebug("===stream=== stream vtable merge next, taskId:%s", id);

  // TODO(kjq): add fill history recover step

  if (pInfo->pVtableMergeHandles == NULL) {
    pInfo->pVtableMergeHandles = taosHashInit(taosArrayGetSize(pVTables),
                                              taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    QUERY_CHECK_NULL(pInfo->pVtableMergeHandles, code, lino, _end, terrno);
    taosHashSetFreeFp(pInfo->pVtableMergeHandles, streamVtableMergeDestroyHandle);

    int32_t nTables = taosArrayGetSize(pVTables);
    int32_t numPagePerTable = getNumOfInMemBufPages(pInfo->pVtableMergeBuf) / nTables;
    for (int32_t i = 0; i < nTables; ++i) {
      SVCTableMergeInfo* pTableInfo = taosArrayGet(pVTables, i);
      if (pTableInfo == NULL || pTableInfo->numOfSrcTbls == 0) {
        continue;
      }
      QUERY_CHECK_CONDITION(pTableInfo->numOfSrcTbls <= numPagePerTable, code, lino, _end, terrno);
      SStreamVtableMergeHandle* pMergeHandle = NULL;
      code = streamVtableMergeCreateHandle(&pMergeHandle, pTableInfo->uid, pTableInfo->numOfSrcTbls, numPagePerTable,
                                           pInfo->primaryTsIndex, pInfo->pVtableMergeBuf, pInfo->pRes, id);
      QUERY_CHECK_CODE(code, lino, _end);
      code = taosHashPut(pInfo->pVtableMergeHandles, &pTableInfo->uid, sizeof(pTableInfo->uid), &pMergeHandle,
                         POINTER_BYTES);
      if (code != TSDB_CODE_SUCCESS) {
        streamVtableMergeDestroyHandle(&pMergeHandle);
      }
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  while (pOperator->status != OP_RES_TO_RETURN) {
    SSDataBlock*   pBlock = NULL;
    SOperatorInfo* downStream = pOperator->pDownstream[0];

    code = downStream->fpSet.getNextFn(downStream, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      break;
    }

    if (pBlock->info.type == STREAM_NORMAL) {
      int32_t inputNCols = taosArrayGetSize(pBlock->pDataBlock);
      int32_t resNCols = taosArrayGetSize(pResBlock->pDataBlock);
      QUERY_CHECK_CONDITION(inputNCols <= resNCols, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      for (int32_t i = 0; i < inputNCols; ++i) {
        SColumnInfoData* p1 = taosArrayGet(pResBlock->pDataBlock, i);
        QUERY_CHECK_NULL(p1, code, lino, _end, terrno);
        SColumnInfoData* p2 = taosArrayGet(pBlock->pDataBlock, i);
        QUERY_CHECK_CODE(code, lino, _end);
        QUERY_CHECK_CONDITION(p1->info.type == p2->info.type, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        QUERY_CHECK_CONDITION(p1->info.bytes == p2->info.bytes, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      }
      for (int32_t i = inputNCols; i < resNCols; ++i) {
        SColumnInfoData* p = taosArrayGet(pResBlock->pDataBlock, i);
        QUERY_CHECK_NULL(p, code, lino, _end, terrno);
        SColumnInfoData colInfo = {.hasNull = true, .info = p->info};
        code = blockDataAppendColInfo(pBlock, &colInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        SColumnInfoData* pNewCol = taosArrayGet(pBlock->pDataBlock, i);
        QUERY_CHECK_NULL(pNewCol, code, lino, _end, terrno);
        code = colInfoDataEnsureCapacity(pNewCol, pBlock->info.rows, false);
        QUERY_CHECK_CODE(code, lino, _end);
        colDataSetNNULL(pNewCol, 0, pBlock->info.rows);
      }
      SStreamVtableMergeHandle** ppHandle =
          taosHashGet(pInfo->pVtableMergeHandles, &pBlock->info.id.uid, sizeof(int64_t));
      if (ppHandle == NULL) {
        // skip table that is not needed
        continue;
      }

      code = streamVtableMergeAddBlock(*ppHandle, pBlock, id);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      code = copyDataBlock(pInfo->pCheckpointRes, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      (*ppRes) = pInfo->pCheckpointRes;
      goto _end;
    } else {
      qError("unexpected block type %d, id:%s", pBlock->info.type, id);
      code = TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (taosArrayGetSize(pInfo->pVtableReadyHandles) == 0) {
    void* pIter = taosHashIterate(pInfo->pVtableMergeHandles, NULL);
    while (pIter != NULL) {
      SStreamVtableMergeHandle* pHandle = *(SStreamVtableMergeHandle**)pIter;
      SVM_NEXT_RESULT           res = SVM_NEXT_NOT_READY;
      code = streamVtableMergeMoveNext(pHandle, &res, id);
      QUERY_CHECK_CODE(code, lino, _end);
      if (res == SVM_NEXT_FOUND) {
        void* px = taosArrayPush(pInfo->pVtableReadyHandles, &pHandle);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
      pIter = taosHashIterate(pInfo->pVtableMergeHandles, pIter);
    }
  }

  blockDataCleanup(pResBlock);
  while (true) {
    void* px = taosArrayGetLast(pInfo->pVtableReadyHandles);
    if (px == NULL) {
      break;
    }

    SStreamVtableMergeHandle* pHandle = *(SStreamVtableMergeHandle**)px;
    QUERY_CHECK_NULL(pHandle, code, lino, _end, terrno);

    SVM_NEXT_RESULT res = SVM_NEXT_FOUND;
    int32_t         nCols = taosArrayGetSize(pResBlock->pDataBlock);
    while (res == SVM_NEXT_FOUND) {
      SSDataBlock* pBlock = NULL;
      int32_t      idx = 0;
      code = streamVtableMergeCurrent(pHandle, &pBlock, &idx, id);
      QUERY_CHECK_CODE(code, lino, _end);

      bool newTuple = true;
      if (pResBlock->info.rows > 0) {
        SColumnInfoData* pResTsCol = taosArrayGet(pResBlock->pDataBlock, pInfo->primaryTsIndex);
        int64_t          lastResTs = *(int64_t*)colDataGetNumData(pResTsCol, pResBlock->info.rows - 1);
        SColumnInfoData* pMergeTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
        int64_t          mergeTs = *(int64_t*)colDataGetNumData(pMergeTsCol, idx);
        QUERY_CHECK_CONDITION(mergeTs >= lastResTs, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        newTuple = (mergeTs > lastResTs);
      }
      if (newTuple) {
        if (pResBlock->info.rows >= pResBlock->info.capacity) {
          break;
        }
        pResBlock->info.rows++;
        for (int32_t i = 0; i < nCols; ++i) {
          SColumnInfoData* pResCol = taosArrayGet(pResBlock->pDataBlock, i);
          colDataSetNULL(pResCol, pResBlock->info.rows - 1);
        }
      }
      for (int32_t i = 0; i < nCols; ++i) {
        SColumnInfoData* pMergeCol = taosArrayGet(pBlock->pDataBlock, i);
        if (!colDataIsNull_s(pMergeCol, idx)) {
          SColumnInfoData* pResCol = taosArrayGet(pResBlock->pDataBlock, i);
          code = colDataAssignNRows(pResCol, pResBlock->info.rows - 1, pMergeCol, idx, 1);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      code = streamVtableMergeMoveNext(pHandle, &res, id);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (res == SVM_NEXT_NOT_READY) {
      px = taosArrayPop(pInfo->pVtableReadyHandles);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }

    if (pResBlock->info.rows > 0) {
      pResBlock->info.id.uid = streamVtableMergeHandleGetVuid(pHandle);
      break;
    }
  }

  if (taosArrayGetSize(pInfo->pVtableReadyHandles) == 0) {
    pOperator->status = OP_EXEC_DONE;
  }

  pInfo->numOfExec++;
  if (pResBlock->info.rows > 0) {
    pResBlock->info.id.groupId = tableListGetTableGroupId(pInfo->pTableListInfo, pResBlock->info.id.uid);
    code = blockDataUpdateTsWindow(pResBlock, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    code = addTagPseudoColumnData(&pInfo->readHandle, pInfo->pPseudoExpr, pInfo->numOfPseudoExpr, pResBlock,
                                  pResBlock->info.rows, pTaskInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pResBlock->info.rows > 0) {
      (*ppRes) = pResBlock;
      pOperator->resultInfo.totalRows += pResBlock->info.rows;
    }
  }

_end:
  if (pIter != NULL) {
    taosHashCancelIterate(pInfo->pVtableMergeHandles, pIter);
    pIter = NULL;
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

int32_t createStreamVtableMergeOperatorInfo(SOperatorInfo* pDownstream, SReadHandle* pHandle,
                                            SVirtualScanPhysiNode* pVirtualScanNode, SNode* pTagCond,
                                            STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                            SOperatorInfo** pOptrInfo) {
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

  // create the pseudo columns info
  if (pVirtualScanNode->scan.pScanPseudoCols != NULL) {
    code = createExprInfo(pVirtualScanNode->scan.pScanPseudoCols, NULL, &pInfo->pPseudoExpr, &pInfo->numOfPseudoExpr);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pScanPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pInfo->pRes, TMAX(pOperator->resultInfo.capacity, 4096));
  QUERY_CHECK_CODE(code, lino, _error);
  pInfo->pRes->info.type = STREAM_NORMAL;
  code = createSpecialDataBlock(STREAM_CLEAR, &pInfo->pUpdateRes);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t pageSize = getProperSortPageSize(pInfo->pRes->info.rowSize, taosArrayGetSize(pInfo->pRes->pDataBlock));
  code = createDiskbasedBuf(&pInfo->pVtableMergeBuf, pageSize, tsStreamVirtualMergeMaxMemKb * 1024,
                            "streamVtableMergeBuf", tsTempDir);
  QUERY_CHECK_CODE(code, lino, _error);
  pInfo->pVtableReadyHandles = taosArrayInit(0, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pVtableReadyHandles, code, lino, _error, terrno);
  pInfo->pTableListInfo = pTableListInfo;
  pTableListInfo = NULL;

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

  code = appendDownstream(pOperator, &pDownstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroyStreamScanOperatorInfo(pInfo);
  }

  if (pTableListInfo != NULL) {
    tableListDestroy(pTableListInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}
