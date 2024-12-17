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
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  STupleHandle*  prefetchedTuple;
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SColMatchInfo  matchInfo;
} SVirtualTableScanInfo;

typedef struct SVirtualScanMergeOperatorInfo {
  SOptrBasicInfo        binfo;
  EMergeType            type;
  SVirtualTableScanInfo virtualScanInfo;
  SLimitInfo            limitInfo;
  bool                  ignoreGroupId;
  uint64_t              groupId;
  bool                  inputWithGroupId;
} SVirtualScanMergeOperatorInfo;

int32_t sortMergeloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  int32_t        code = pOperator->fpSet.getNextFn(pOperator, ppBlock);
  if (code) {
    qError("failed to get next data block from upstream, %s code:%s", __func__, tstrerror(code));
  }
  code = blockDataCheck(*ppBlock);
  if (code) {
    qError("failed to check data block got from upstream, %s code:%s", __func__, tstrerror(code));
  }
  return code;
}

int32_t openVirtualTableScanOperatorImpl(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo * pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  SVirtualTableScanInfo*          pSortMergeInfo = &pInfo->virtualScanInfo;

  int32_t numOfBufPage = pSortMergeInfo->sortBufSize / pSortMergeInfo->bufPageSize;

  pSortMergeInfo->pSortHandle = NULL;
  int32_t code = tsortCreateSortHandle(pSortMergeInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pSortMergeInfo->bufPageSize,
                                       numOfBufPage, pSortMergeInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0, &pSortMergeInfo->pSortHandle);
  if (code) {
    return code;
  }

  tsortSetForceUsePQSort(pSortMergeInfo->pSortHandle);
  tsortSetFetchRawDataFp(pSortMergeInfo->pSortHandle, sortMergeloadNextDataBlock, NULL, NULL);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      code = pDownstream->fpSet._openFn(pDownstream);
      if (code) {
        return code;
      }
    }

    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    if (ps == NULL) {
      return terrno;
    }

    ps->param = pDownstream;
    ps->onlyRef = true;

    code = tsortAddSource(pSortMergeInfo->pSortHandle, ps);
    if (code) {
      return code;
    }
  }

  return tsortOpen(pSortMergeInfo->pSortHandle);
}

int32_t openVirtualTableScanOperator(SOperatorInfo* pOperator) {
  int32_t                        code = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t startTs = taosGetTimestampUs();

  code = openVirtualTableScanOperatorImpl(pOperator);

  pOperator->cost.openCost = (taosGetTimestampUs() - startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  OPTR_SET_OPENED(pOperator);
  return code;
}

static int32_t doGetSortedBlockData(SSortHandle* pHandle, int32_t capacity,
                                    SSDataBlock* p) {
  int32_t code = 0;

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    code = tsortNextTuple(pHandle, &pTupleHandle);

    if (pTupleHandle == NULL || (code != 0)) {
      break;
    }

    code = appendOneRowToDataBlock(p, pTupleHandle);
    if (code) {
      return code;
    }

    if (p->info.rows >= capacity) {
      break;
    }
  }

  return code;
}

int32_t doSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = 0;
  code = openVirtualTableScanOperator(pOperator);
  if (code) {
    return code;
  }
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pSortMergeInfo = &pInfo->virtualScanInfo;
  SSortHandle*                   pHandle = pSortMergeInfo->pSortHandle;
  SSDataBlock*                   pDataBlock = pInfo->binfo.pRes;
  SArray*                        pColMatchInfo = pSortMergeInfo->matchInfo.pList;
  int32_t                        capacity = pOperator->resultInfo.capacity;

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  blockDataCleanup(pDataBlock);

  if (pSortMergeInfo->pIntermediateBlock == NULL) {
    pSortMergeInfo->pIntermediateBlock = NULL;
    code = tsortGetSortedDataBlock(pHandle, &pSortMergeInfo->pIntermediateBlock);
    if (pSortMergeInfo->pIntermediateBlock == NULL || code != 0) {
      return code;
    }

    code = blockDataEnsureCapacity(pSortMergeInfo->pIntermediateBlock, capacity);
    if (code) {
      return code;
    }

  } else {
    blockDataCleanup(pSortMergeInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pSortMergeInfo->pIntermediateBlock;
  while (1) {
    code = doGetSortedBlockData(pHandle, capacity, p);
    if (code) {
      return code;
    }

    if (p->info.rows == 0) {
      break;
    }
  }

  if (p->info.rows > 0) {
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);
      if (pmInfo == NULL) {
        code = terrno;
        return code;
      }

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      if (pSrc == NULL) {
        code = terrno;
        return code;
      }

      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->dstSlotId);
      if (pDst == NULL) {
        code = terrno;
        return code;
      }

      code = colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
      if (code) {
        return code;
      }
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    pDataBlock->info.id.groupId = 0; // Ignore groupid

    pDataBlock->info.dataLoad = 1;
  }

  qDebug("%s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64 , GET_TASKID(pTaskInfo), pDataBlock->info.id.groupId,
         pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

int32_t doMultiwayMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pOperator->status == OP_EXEC_DONE) {
    return 0;
  }

  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  code = doSortMerge(pOperator, pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  if ((*pResBlock) != NULL) {
    pOperator->resultInfo.totalRows += (*pResBlock)->info.rows;
    code = blockDataCheck(*pResBlock);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    setOperatorCompleted(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

void destroyVirtualTableScanOperatorInfo(void* param) {
  SVirtualScanMergeOperatorInfo* pInfo = (SVirtualScanMergeOperatorInfo*)param;
  blockDataDestroy(pInfo->binfo.pRes);
  pInfo->binfo.pRes = NULL;

  taosMemoryFreeClear(param);
}

int32_t getVirtualTableScanExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  int32_t code = 0;
  return code;
}

int32_t makeTSMergeKey(SNodeList** pMergeKeys) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SNodeList        *pNodeList = NULL;
  SColumnNode      *pColumnNode = NULL;
  SOrderByExprNode *pOrderByExprNode = NULL;

  TSDB_CHECK_CODE(nodesMakeList(&pNodeList), lino, _return);

  TSDB_CHECK_CODE(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumnNode), lino, _return);
  pColumnNode->slotId = 0;

  TSDB_CHECK_CODE(nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExprNode), lino, _return);
  pOrderByExprNode->pExpr = (SNode*)pColumnNode;
  pOrderByExprNode->order = ORDER_ASC;
  pOrderByExprNode->nullOrder = NULL_ORDER_FIRST;

  code = nodesListAppend(pNodeList, (SNode*)pOrderByExprNode);
  TSDB_CHECK_CODE(code, lino, _return);

  *pMergeKeys = pNodeList;
  return code;
_return:
  nodesDestroyNode((SNode*)pColumnNode);
  nodesDestroyNode((SNode*)pOrderByExprNode);
  nodesDestroyList(pNodeList);
  return code;
}
int32_t createVirtualTableMergeOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, 
                                            SVirtualScanPhysiNode* pVirtualScanPhyNode, SExecTaskInfo* pTaskInfo,
                                            SOperatorInfo** pOptrInfo) {
   SPhysiNode*                 pPhyNode = (SPhysiNode*)pVirtualScanPhyNode;
   int32_t                     lino = 0;
   int32_t                     code = TSDB_CODE_SUCCESS;
   SVirtualScanMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SVirtualScanMergeOperatorInfo));
   SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
   SDataBlockDescNode*         pDescNode = pPhyNode->pOutputDataBlockDesc;
   if (pInfo == NULL || pOperator == NULL) {
     VTS_ERR_JRET(terrno);
   }

   pInfo->binfo.inputTsOrder = pVirtualScanPhyNode->scan.node.inputTsOrder;
   pInfo->binfo.outputTsOrder = pVirtualScanPhyNode->scan.node.outputTsOrder;

   SVirtualTableScanInfo* pVirtualScanInfo = &pInfo->virtualScanInfo;
   pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
   TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _return, terrno);

   SPhysiNode*  pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
   SSDataBlock* pInputBlock = createDataBlockFromDescNode(pChildNode->pOutputDataBlockDesc);
   TSDB_CHECK_NULL(pInputBlock, code, lino, _return, terrno);
   pVirtualScanInfo->pInputBlock = pInputBlock;

   initResultSizeInfo(&pOperator->resultInfo, 1024);
   TSDB_CHECK_CODE(blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity), lino, _return);

   size_t     numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
   int32_t    rowSize = pInfo->binfo.pRes->info.rowSize;
   int32_t    numOfOutputCols = 0;
   SNodeList *pMergeKeys = NULL;

   TSDB_CHECK_CODE(makeTSMergeKey(&pMergeKeys), lino, _return);
   pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
   pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
   pVirtualScanInfo->sortBufSize =
       pVirtualScanInfo->bufPageSize * (numOfDownstream + 1);  // one additional is reserved for merged result.
   code = extractColMatchInfo(pVirtualScanPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                              &pVirtualScanInfo->matchInfo);
   if (code != TSDB_CODE_SUCCESS) {
     goto _return;
   }

   setOperatorInfo(pOperator, "VirtualTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                   OP_NOT_OPENED, pInfo, pTaskInfo);
   pOperator->fpSet =
       createOperatorFpSet(optrDummyOpenFn, doSortMerge, NULL, destroyVirtualTableScanOperatorInfo,
                           optrDefaultBufFn, getVirtualTableScanExplainExecInfo, optrDefaultGetNextExtFn, NULL);

   VTS_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));

   *pOptrInfo = pOperator;
   return TSDB_CODE_SUCCESS;

_return:
   if (pInfo != NULL) {
     destroyVirtualTableScanOperatorInfo(pInfo);
   }
   pTaskInfo->code = code;
   destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
   return code;
}
