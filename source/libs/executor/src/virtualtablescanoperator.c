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
#include "tdatablock.h"
#include "virtualtablescan.h"
#include "tsort.h"

typedef struct SVirtualTableScanInfo {
  STableScanBase base;
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SSHashObj*     dataSlotMap;
  int32_t        tsSlotId;
  int32_t        orgTsSlotId; // ts slot id of block from origin table, only used for virtual super table to make TS merge key
  int32_t        tagBlockId;
  int32_t        tagDownStreamId;
  bool           scanAllCols;
  SArray*        pSortCtxList;
  tb_uid_t       vtableUid;  // virtual table uid, used to identify the vtable scan operator
} SVirtualTableScanInfo;

typedef struct SVirtualScanMergeOperatorInfo {
  SOptrBasicInfo        binfo;
  EMergeType            type;
  SVirtualTableScanInfo virtualScanInfo;
  bool                  ignoreGroupId;
  uint64_t              groupId;
  STupleHandle*         pSavedTuple;
  SSDataBlock*          pSavedTagBlock;
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
  int32_t        line = 0;

  VTS_ERR_JRET(pOperator->fpSet.getNextFn(pOperator, ppBlock));
  VTS_ERR_JRET(blockDataCheck(*ppBlock));
  if (*ppBlock) {
    SColumnInfoData* p = taosArrayGet((*ppBlock)->pDataBlock, 0);
    QUERY_CHECK_NULL(p, code, line, _return, terrno);
    // ts column will never have null value. set hasNull = false here can accelerate the sort
    p->hasNull = false;
  }

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
  QUERY_CHECK_NULL(pColData, code, lino, _return, terrno)
  // ts column will never have null value. set hasNull = false here can accelerate the sort
  pColData->hasNull = false;

  GET_TYPED_DATA(*startTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, 0), 0);
  GET_TYPED_DATA(*endTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, pBlock->info.rows - 1), 0);

  return code;
_return:
  qError("failed to get time window of block, %s code:%s, line:%d", __func__, tstrerror(code), lino);
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
    VTS_ERR_JRET(getTimeWindowOfBlock(pRes, pCtx->tsSlotId, &pCtx->window.skey, &pCtx->window.ekey));
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
  int32_t                         scanOpIndex = 0;

  cleanUpVirtualScanInfo(pVirtualScanInfo);
  VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, pVirtualScanInfo->orgTsSlotId));
  pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno)
  nodesDestroyList(pMergeKeys);

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_TS_MERGE, pVirtualScanInfo->bufPageSize,
                                     numOfBufPage, pVirtualScanInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle, virtualScanloadNextDataBlockFromParam, NULL, NULL);

  if (pOperator->numOfDownstream > 2) {
    qError("virtual scan operator should not have more than 2 downstreams, current numOfDownstream:%d", pOperator->numOfDownstream);
    VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INVALID_DOWNSTREAM);
  }

  pVirtualScanInfo->tagDownStreamId = -1;
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->resultDataBlockId == pVirtualScanInfo->tagBlockId) {
      // tag block do not need sort
      pVirtualScanInfo->tagDownStreamId = i;
      pInfo->pSavedTagBlock = NULL;
      continue;
    }
  }
  scanOpIndex = pVirtualScanInfo->tagDownStreamId == -1 ? 0 : 1 - pVirtualScanInfo->tagDownStreamId;

  pOperator->pDownstream[scanOpIndex]->status = OP_NOT_OPENED;
  pVirtualScanInfo->pSortCtxList = taosArrayInit(taosArrayGetSize((pParam)->pOpParamArray), POINTER_BYTES);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortCtxList, code, lino, _return, terrno)
  for (int32_t i = 0; i < taosArrayGetSize((pParam)->pOpParamArray); i++) {
    SOperatorParam* pOpParam = *(SOperatorParam**)taosArrayGet((pParam)->pOpParamArray, i);
    SLoadNextCtx*   pCtx = NULL;
    ps = NULL;

    pCtx = taosMemoryMalloc(sizeof(SLoadNextCtx));
    QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno)
    pCtx->blockId = i;
    pCtx->pOperator = pOperator->pDownstream[scanOpIndex];
    pCtx->pOperatorGetParam = pOpParam;
    pCtx->window = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};
    pCtx->pIntermediateBlock = NULL;
    pCtx->tsSlotId = (col_id_t)pVirtualScanInfo->tsSlotId;

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    QUERY_CHECK_NULL(ps, code, lino, _return, terrno)

    ps->param = pCtx;
    ps->onlyRef = true;

    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno)
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

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_TS_MERGE, pVirtualScanInfo->bufPageSize,
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

    if (pDownstream->resultDataBlockId == pVirtualScanInfo->tagBlockId) {
      // tag block do not need sort
      pVirtualScanInfo->tagDownStreamId = i;
      continue;
    }

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    TSDB_CHECK_NULL(ps, code, lino, _return, terrno)

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
  for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
    colDataSetNItemsNull(taosArrayGet(p->pDataBlock, j), 0, capacity);
  }
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

    int32_t blockId = (int32_t)tsortGetBlockId(pTupleHandle);
    int32_t colNum = pInfo->virtualScanInfo.scanAllCols ? 1 : tsortGetColNum(pTupleHandle);
    for (int32_t i = 0; i < colNum; i++) {
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
            if (pInfo->virtualScanInfo.tsSlotId != -1) {
              VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
            }
            lastTs = *(int64_t*)pData;
          }
          continue;
        }
        if (tsortIsNullVal(pTupleHandle, i)) {
          continue;
        }
        int32_t slotKey = blockId << 16 | i;
        void*   slotId = tSimpleHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
        if (slotId == NULL) {
          qError("failed to get slotId from dataSlotMap, blockId:%d, slotId:%d", blockId, i);
          VTS_ERR_RET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
        }
        VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, *(int32_t *)slotId), rowNums, pData, false));
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
  for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
    colDataSetNItemsNull(taosArrayGet(p->pDataBlock, j), 0, capacity);
  }
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
    int32_t colNum = tsortGetColNum(pTupleHandle);

    for (int32_t i = 0; i < colNum; i++) {
      SColumnInfoData *pColInfo = NULL;
      tsortGetColumnInfo(pTupleHandle, i, &pColInfo);
      if (pColInfo && pColInfo->info.slotId ==  pInfo->virtualScanInfo.tsSlotId) {
        tsIndex = i;
        break;
      }
    }

    if (tsIndex == -1) {
      tsIndex = colNum - 1;
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
        if (pInfo->virtualScanInfo.tsSlotId != -1) {
          VTS_ERR_RET(colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
        }
        lastTs = *(int64_t*)pData;
      }
    }
    if (pInfo->virtualScanInfo.scanAllCols) {
      continue;
    }

    for (int32_t i = 0; i < colNum; i++) {
      if (i == tsIndex || tsortIsNullVal(pTupleHandle, i)) {
        continue;
      }

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
  qDebug("%s %s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64 , GET_TASKID(pTaskInfo), __func__, pDataBlock->info.id.groupId,
         pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

int32_t vtableAddTagPseudoColumnData(const SExprInfo* pExpr, int32_t numOfExpr, SSDataBlock* tagBlock, SSDataBlock* pBlock, int32_t rows) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  int64_t          backupRows;
  // currently only the tbname pseudo column
  if (numOfExpr <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (tagBlock == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (tagBlock->info.rows != 1) {
    qError("tag block should have only one row, current rows:%" PRId64, tagBlock->info.rows);
    VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
  }

  backupRows = pBlock->info.rows;
  pBlock->info.rows = rows;
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExpr[j];
    int32_t          dstSlotId = pExpr1->base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    TSDB_CHECK_NULL(pColInfoData, code, lino, _return, terrno)
    colInfoDataCleanup(pColInfoData, pBlock->info.rows);

    SColumnInfoData* pTagInfoData = taosArrayGet(tagBlock->pDataBlock, j);
    TSDB_CHECK_NULL(pTagInfoData, code, lino, _return, terrno)

    if (colDataIsNull_s(pTagInfoData, 0) || IS_JSON_NULL(pTagInfoData->info.type, colDataGetData(pTagInfoData, 0))) {
      colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
      continue;
    }

    char* data = colDataGetData(pTagInfoData, 0);

    if (pColInfoData->info.type != TSDB_DATA_TYPE_JSON) {
      code = colDataSetNItems(pColInfoData, 0, data, pBlock->info.rows, 1, false);
      QUERY_CHECK_CODE(code, lino, _return);
    } else {  // todo opt for json tag
      for (int32_t i = 0; i < pBlock->info.rows; ++i) {
        code = colDataSetVal(pColInfoData, i, data, false);
        QUERY_CHECK_CODE(code, lino, _return);
      }
    }
  }

  // restore the rows
  pBlock->info.rows = backupRows;

_return:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetTagColumnData(SVirtualTableScanInfo* pInfo, SSDataBlock* pTagBlock, SSDataBlock* pBlock, int32_t rows) {
  int32_t         code = 0;
  STableScanBase* pTableScanInfo = &pInfo->base;
  SExprSupp*      pSup = &pTableScanInfo->pseudoSup;
  if (pSup->numOfExprs > 0) {
    VTS_ERR_RET(vtableAddTagPseudoColumnData(pSup->pExprInfo, pSup->numOfExprs, pTagBlock, pBlock, rows));
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

  if (pVirtualScanInfo->tagBlockId != -1 && pVirtualScanInfo->tagDownStreamId != -1 && !pInfo->pSavedTagBlock) {
    SSDataBlock*   pTagBlock = NULL;
    SOperatorInfo *pTagScanOp = pOperator->pDownstream[pVirtualScanInfo->tagDownStreamId];
    if (pOperator->pOperatorGetParam) {
      SOperatorParam* pTagOp = ((SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value)->pTagScanOp;
      VTS_ERR_JRET(pTagScanOp->fpSet.getNextExtFn(pTagScanOp, pTagOp, &pTagBlock));
    } else {
      VTS_ERR_JRET(pTagScanOp->fpSet.getNextFn(pTagScanOp, &pTagBlock));
    }

    if (pTagBlock == NULL || pTagBlock->info.rows != 1) {
      VTS_ERR_JRET(TSDB_CODE_FAILED);
    }
    pInfo->pSavedTagBlock = pTagBlock;
  }

  while(1) {
    VTS_ERR_JRET(doVirtualTableMerge(pOperator, pResBlock));
    if (*pResBlock == NULL) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pOperator->pOperatorGetParam) {
      uint64_t uid = ((SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value)->uid;
      (*pResBlock)->info.id.uid = uid;
    } else {
      (*pResBlock)->info.id.uid = pInfo->virtualScanInfo.vtableUid;
    }

    VTS_ERR_JRET(doSetTagColumnData(pVirtualScanInfo, pInfo->pSavedTagBlock, (*pResBlock), (*pResBlock)->info.rows));
    VTS_ERR_JRET(doFilter(*pResBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL));
    if ((*pResBlock)->info.rows > 0) {
      break;
    }
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  pTaskInfo->code = code;
  T_LONG_JMP(pTaskInfo->env, code);
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
  pInfo->pSortInfo = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = NULL;
  destroyTableScanBase(&pInfo->base, &pInfo->base.readerAPI);

  tSimpleHashCleanup(pInfo->dataSlotMap);

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
    pInfo->pSortCtxList = NULL;
  }
  taosMemoryFreeClear(param);
}

int32_t extractColMap(SNodeList* pNodeList, SSHashObj** pSlotMap, int32_t *tsSlotId, int32_t *orgTsSlotId, int32_t *tagBlockId) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (numOfCols == 0) {
    return code;
  }

  *tsSlotId = -1;
  *orgTsSlotId = numOfCols;
  *tagBlockId = -1;
  *pSlotMap = tSimpleHashInit(numOfCols, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  TSDB_CHECK_NULL(*pSlotMap, code, lino, _return, terrno);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pNodeList, i);
    TSDB_CHECK_NULL(pColNode, code, lino, _return, terrno)

    if (pColNode->isPrimTs || pColNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      *tsSlotId = i;
      *orgTsSlotId = i;
    } else if (pColNode->hasRef) {
      int32_t slotKey = pColNode->dataBlockId << 16 | pColNode->slotId;
      VTS_ERR_JRET(tSimpleHashPut(*pSlotMap, &slotKey, sizeof(slotKey), &i, sizeof(i)));
    } else if (pColNode->colType == COLUMN_TYPE_TAG || '\0' == pColNode->tableAlias[0]) {
      // tag column or pseudo column's function
      *tagBlockId = pColNode->dataBlockId;
    }
  }

  return code;
_return:
  tSimpleHashCleanup(*pSlotMap);
  *pSlotMap = NULL;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t resetVirtualTableMergeOperState(SOperatorInfo* pOper) {
  int32_t code = 0, lino = 0;
  SVirtualScanMergeOperatorInfo* pMergeInfo = pOper->info;
  SVirtualScanPhysiNode* pPhynode = (SVirtualScanPhysiNode*)pOper->pPhyNode;
  SVirtualTableScanInfo* pInfo = &pMergeInfo->virtualScanInfo;
  
  pOper->status = OP_NOT_OPENED;
  resetBasicOperatorState(&pMergeInfo->binfo);

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
  // taosArrayDestroy(pInfo->pSortInfo);
  // pInfo->pSortInfo = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = createDataBlockFromDescNode(((SPhysiNode*)pPhynode)->pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pInfo->pInputBlock, code, lino, _exit, terrno)

  pInfo->tagDownStreamId = -1;

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
    pInfo->pSortCtxList = NULL;
  }

  pMergeInfo->pSavedTuple = NULL;
  pMergeInfo->pSavedTagBlock = NULL;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t createVirtualTableMergeOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                            SVirtualScanPhysiNode* pVirtualScanPhyNode,
                                            SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  SPhysiNode*                    pPhyNode = (SPhysiNode*)pVirtualScanPhyNode;
  int32_t                        lino = 0;
  int32_t                        code = TSDB_CODE_SUCCESS;
  SVirtualScanMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SVirtualScanMergeOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*            pDescNode = pPhyNode->pOutputDataBlockDesc;
  SNodeList*                     pMergeKeys = NULL;

  QUERY_CHECK_NULL(pInfo, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pOperator, code, lino, _return, terrno)

  pOperator->pPhyNode = pVirtualScanPhyNode;

  pInfo->binfo.inputTsOrder = pVirtualScanPhyNode->scan.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pVirtualScanPhyNode->scan.node.outputTsOrder;

  SVirtualTableScanInfo* pVirtualScanInfo = &pInfo->virtualScanInfo;
  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _return, terrno)

  SSDataBlock* pInputBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pInputBlock, code, lino, _return, terrno)
  pVirtualScanInfo->pInputBlock = pInputBlock;
  pVirtualScanInfo->tagDownStreamId = -1;
  pVirtualScanInfo->vtableUid = (tb_uid_t)pVirtualScanPhyNode->scan.uid;
  if (pVirtualScanPhyNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pVirtualScanInfo->base.pseudoSup;
    pSup->pExprInfo = NULL;
    VTS_ERR_JRET(createExprInfo(pVirtualScanPhyNode->scan.pScanPseudoCols, NULL, &pSup->pExprInfo, &pSup->numOfExprs));

    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset,
                                      &pTaskInfo->storageAPI.functionStore);
    TSDB_CHECK_NULL(pSup->pCtx, code, lino, _return, terrno)
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  TSDB_CHECK_CODE(blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity), lino, _return);

  size_t  numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  int32_t rowSize = pInfo->binfo.pRes->info.rowSize;

  if (!pVirtualScanPhyNode->scan.node.dynamicOp) {
    VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, 0));
    pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
    TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno)
  } else {
    pTaskInfo->dynamicTask = true;
  }
  pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
  pVirtualScanInfo->sortBufSize =
      pVirtualScanInfo->bufPageSize * (numOfDownstream + 1);  // one additional is reserved for merged result.
  VTS_ERR_JRET(
      extractColMap(pVirtualScanPhyNode->pTargets, &pVirtualScanInfo->dataSlotMap, &pVirtualScanInfo->tsSlotId, &pVirtualScanInfo->orgTsSlotId, &pVirtualScanInfo->tagBlockId));

  pVirtualScanInfo->scanAllCols = pVirtualScanPhyNode->scanAllCols;

  VTS_ERR_JRET(filterInitFromNode((SNode*)pVirtualScanPhyNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo,
                                  0, pTaskInfo->pStreamRuntimeInfo));

  pVirtualScanInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
  QUERY_CHECK_NULL(pVirtualScanInfo->base.metaCache.pTableMetaEntryCache, code, lino, _return, terrno)

  setOperatorInfo(pOperator, "VirtualTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(openVirtualTableScanOperator, virtualTableGetNext, NULL, destroyVirtualTableScanOperatorInfo,
                          optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetVirtualTableMergeOperState);

  if (NULL != pDownstream) {
    VTS_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));
  } else {
    pVirtualScanInfo->tagDownStreamId = -1;
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
