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
  SHashObj*      dataSlotMap;
  int32_t        tsSlotId;
  bool           onlyTs;
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

int32_t virtualScanloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  int32_t        code = TSDB_CODE_SUCCESS;

  VTS_ERR_JRET(pOperator->fpSet.getNextFn(pOperator, ppBlock));
  VTS_ERR_JRET(blockDataCheck(*ppBlock));

  return code;
_return:
  qError("failed to check data block got from upstream, %s code:%s", __func__, tstrerror(code));
  return code;
}

int32_t openVirtualTableScanOperatorImpl(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo * pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  SVirtualTableScanInfo*          pVirtualScanInfo = &pInfo->virtualScanInfo;
  int32_t                         numOfBufPage = pVirtualScanInfo->sortBufSize / pVirtualScanInfo->bufPageSize;
  SSortSource*                    ps = NULL;
  int32_t                         code = 0;
  int32_t                         lino = 0;

  pVirtualScanInfo->pSortHandle = NULL;
  if (pOperator->numOfDownstream == 0) {
    return TSDB_CODE_SUCCESS;
  }

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
  return code;
_return:
  if (code != 0){
    qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  if (ps != NULL) {
    taosMemoryFree(ps);
  }
  return code;
}

int32_t openVirtualTableScanOperator(SOperatorInfo* pOperator) {
  int32_t code = 0;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t startTs = taosGetTimestampUs();

  code = openVirtualTableScanOperatorImpl(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - startTs) / 1000.0;
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

    for (int32_t i = 0; i < (pInfo->virtualScanInfo.onlyTs ? 1 : tsortGetColNum(pTupleHandle)); i++) {
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
            VTS_ERR_RET(TSDB_CODE_VTABLE_SCAN_UNMATCHED_COLUMN);
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
  VTS_ERR_RET(doGetVtableMergedBlockData(pInfo, pHandle, capacity, p));

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
  if (pOperator->status == OP_EXEC_DONE) {
    pResBlock = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  VTS_ERR_JRET(pOperator->fpSet._openFn(pOperator));

  while(1) {
    VTS_ERR_JRET(doVirtualTableMerge(pOperator, pResBlock));
    if (*pResBlock == NULL) {
      setOperatorCompleted(pOperator);
      break;
    }
    STableKeyInfo* tbInfo = tableListGetInfo(pVirtualScanInfo->base.pTableListInfo, 0);
    QUERY_CHECK_NULL(tbInfo, code, lino, _return, terrno);
    (*pResBlock)->info.id.uid = tbInfo->uid;

    VTS_ERR_JRET(doSetTagColumnData(&pVirtualScanInfo->base, (*pResBlock), pTaskInfo, (*pResBlock)->info.rows));
    VTS_ERR_JRET(doFilter(*pResBlock, pOperator->exprSupp.pFilterInfo, NULL));
    if ((*pResBlock)->info.rows > 0) {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
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
  SVirtualScanMergeOperatorInfo* pOperatorInfo = (SVirtualScanMergeOperatorInfo*)param;
  SVirtualTableScanInfo* pInfo = &pOperatorInfo->virtualScanInfo;
  blockDataDestroy(pOperatorInfo->binfo.pRes);
  pOperatorInfo->binfo.pRes = NULL;

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = NULL;
  destroyTableScanBase(&pInfo->base, &pInfo->base.readerAPI);

  taosHashCleanup(pInfo->dataSlotMap);
  taosMemoryFreeClear(param);
}

int32_t getVirtualTableScanExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  int32_t code = 0;
  return code;
}

int32_t makeTSMergeKey(SNodeList** pMergeKeys) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SNodeList        *pNodeList = NULL;
  SColumnNode      *pColumnNode = NULL;
  SOrderByExprNode *pOrderByExprNode = NULL;

  VTS_ERR_JRET(nodesMakeList(&pNodeList));

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumnNode));
  pColumnNode->slotId = 0;

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
   SPhysiNode*                 pPhyNode = (SPhysiNode*)pVirtualScanPhyNode;
   int32_t                     lino = 0;
   int32_t                     code = TSDB_CODE_SUCCESS;
   SVirtualScanMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SVirtualScanMergeOperatorInfo));
   SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
   SDataBlockDescNode*         pDescNode = pPhyNode->pOutputDataBlockDesc;
   SNodeList                  *pMergeKeys = NULL;
   if (pInfo == NULL || pOperator == NULL) {
     VTS_ERR_JRET(terrno);
   }


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

   VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys));
   pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
   TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno);
   pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
   pVirtualScanInfo->sortBufSize =
       pVirtualScanInfo->bufPageSize * (numOfDownstream + 1);  // one additional is reserved for merged result.
   VTS_ERR_JRET(extractColMap(pVirtualScanPhyNode->pTargets, &pVirtualScanInfo->dataSlotMap, &pVirtualScanInfo->tsSlotId));

   pVirtualScanInfo->onlyTs = pVirtualScanPhyNode->onlyTs;

   VTS_ERR_JRET(filterInitFromNode((SNode*)pVirtualScanPhyNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0));

   pVirtualScanInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
   QUERY_CHECK_NULL(pVirtualScanInfo->base.metaCache.pTableMetaEntryCache, code, lino, _return, terrno);
   pVirtualScanInfo->base.readHandle = *readHandle;
   pVirtualScanInfo->base.pTableListInfo = pTableListInfo;

   setOperatorInfo(pOperator, "VirtualTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                   OP_NOT_OPENED, pInfo, pTaskInfo);
   pOperator->fpSet =
       createOperatorFpSet(openVirtualTableScanOperator, virtualTableGetNext, NULL, destroyVirtualTableScanOperatorInfo,
                           optrDefaultBufFn, getVirtualTableScanExplainExecInfo, optrDefaultGetNextExtFn, NULL);

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
