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

typedef struct SSortMergeInfo {
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  STupleHandle*  prefetchedTuple;
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SColMatchInfo  matchInfo;
} SSortMergeInfo;

typedef struct SNonSortMergeInfo {
  int32_t  lastSourceIdx;
  int32_t  sourceWorkIdx;
  int32_t  sourceNum;
  int32_t* pSourceStatus;
} SNonSortMergeInfo;

typedef struct SColsMergeInfo {
  SNodeList* pTargets;
  uint64_t   srcBlkIds[2]; 
} SColsMergeInfo;

typedef struct SMultiwayMergeOperatorInfo {
  SOptrBasicInfo binfo;
  EMergeType     type;
  union {
    SSortMergeInfo    sortMergeInfo;
    SNonSortMergeInfo nsortMergeInfo;
    SColsMergeInfo    colsMergeInfo;
  };
  SLimitInfo     limitInfo;
  bool           groupMerge;
  bool           ignoreGroupId;
  uint64_t       groupId;
  bool           inputWithGroupId;
} SMultiwayMergeOperatorInfo;

SSDataBlock* sortMergeloadNextDataBlock(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  SSDataBlock*   pBlock = pOperator->fpSet.getNextFn(pOperator);
  return pBlock;
}

int32_t openSortMergeOperator(SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SSortMergeInfo*             pSortMergeInfo = &pInfo->sortMergeInfo;

  int32_t numOfBufPage = pSortMergeInfo->sortBufSize / pSortMergeInfo->bufPageSize;

  pSortMergeInfo->pSortHandle = tsortCreateSortHandle(pSortMergeInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pSortMergeInfo->bufPageSize, numOfBufPage,
                                             pSortMergeInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0);

  tsortSetFetchRawDataFp(pSortMergeInfo->pSortHandle, sortMergeloadNextDataBlock, NULL, NULL);
  tsortSetCompareGroupId(pSortMergeInfo->pSortHandle, pInfo->groupMerge);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      pDownstream->fpSet._openFn(pDownstream);
    }

    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    ps->param = pDownstream;
    ps->onlyRef = true;

    tsortAddSource(pSortMergeInfo->pSortHandle, ps);
  }

  return tsortOpen(pSortMergeInfo->pSortHandle);
}

static void doGetSortedBlockData(SMultiwayMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                 SSDataBlock* p, bool* newgroup) {
  SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;
  *newgroup = false;

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (pInfo->groupMerge || pInfo->inputWithGroupId) {
      if (pSortMergeInfo->prefetchedTuple == NULL) {
        pTupleHandle = tsortNextTuple(pHandle);
      } else {
        pTupleHandle = pSortMergeInfo->prefetchedTuple;
        pSortMergeInfo->prefetchedTuple = NULL;
        uint64_t gid = tsortGetGroupId(pTupleHandle);
        if (gid != pInfo->groupId) {
          *newgroup = true;
          pInfo->groupId = gid;
        }
      }
    } else {
      pTupleHandle = tsortNextTuple(pHandle);
      pInfo->groupId = 0;
    }

    if (pTupleHandle == NULL) {
      break;
    }

    if (pInfo->groupMerge || pInfo->inputWithGroupId) {
      uint64_t tupleGroupId = tsortGetGroupId(pTupleHandle);
      if (pInfo->groupId == 0 || pInfo->groupId == tupleGroupId) {
        appendOneRowToDataBlock(p, pTupleHandle);
        p->info.id.groupId = tupleGroupId;
        pInfo->groupId = tupleGroupId;
      } else {
        if (p->info.rows == 0) {
          appendOneRowToDataBlock(p, pTupleHandle);
          p->info.id.groupId = pInfo->groupId = tupleGroupId;
        } else {
          pSortMergeInfo->prefetchedTuple = pTupleHandle;
          break;
        }
      }
    } else {
      appendOneRowToDataBlock(p, pTupleHandle);
    }

    if (p->info.rows >= capacity) {
      break;
    }
  }
}

SSDataBlock* doSortMerge(SOperatorInfo* pOperator) {
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSortMergeInfo*             pSortMergeInfo = &pInfo->sortMergeInfo;
  SSortHandle*                pHandle = pSortMergeInfo->pSortHandle;
  SSDataBlock*                pDataBlock = pInfo->binfo.pRes;
  SArray*                     pColMatchInfo = pSortMergeInfo->matchInfo.pList;
  int32_t                     capacity = pOperator->resultInfo.capacity;

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));

  blockDataCleanup(pDataBlock);

  if (pSortMergeInfo->pIntermediateBlock == NULL) {
    pSortMergeInfo->pIntermediateBlock = tsortGetSortedDataBlock(pHandle);
    if (pSortMergeInfo->pIntermediateBlock == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    blockDataEnsureCapacity(pSortMergeInfo->pIntermediateBlock, capacity);
  } else {
    blockDataCleanup(pSortMergeInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pSortMergeInfo->pIntermediateBlock;
  bool         newgroup = false;

  while (1) {
    doGetSortedBlockData(pInfo, pHandle, capacity, p, &newgroup);
    if (p->info.rows == 0) {
      break;
    }

    if (newgroup) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

    applyLimitOffset(&pInfo->limitInfo, p, pTaskInfo);

    if (p->info.rows > 0) {
      break;
    }
  }

  if (p->info.rows > 0) {
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->dstSlotId);
      colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    if (pInfo->ignoreGroupId) {
      pDataBlock->info.id.groupId = 0;
    } else {
      pDataBlock->info.id.groupId = pInfo->groupId;
    }
    pDataBlock->info.dataLoad = 1;
  }

  qDebug("%s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64 , GET_TASKID(pTaskInfo), pDataBlock->info.id.groupId,
         pDataBlock->info.rows);

  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}


int32_t getSortMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pSortExecInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;
  SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;

  *pSortExecInfo = tsortGetSortExecInfo(pSortMergeInfo->pSortHandle);
  *pOptrExplain = pSortExecInfo;

  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}


void destroySortMergeOperatorInfo(void* param) {
  SSortMergeInfo* pSortMergeInfo = param;
  pSortMergeInfo->pInputBlock = blockDataDestroy(pSortMergeInfo->pInputBlock);
  pSortMergeInfo->pIntermediateBlock = blockDataDestroy(pSortMergeInfo->pIntermediateBlock);

  taosArrayDestroy(pSortMergeInfo->matchInfo.pList);

  tsortDestroySortHandle(pSortMergeInfo->pSortHandle);
  taosArrayDestroy(pSortMergeInfo->pSortInfo);
}

#define NON_SORT_NEXT_SRC(_info, _idx) ((++(_idx) >= (_info)->sourceNum) ? ((_info)->sourceWorkIdx) : (_idx))

int32_t openNonSortMergeOperator(SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SNonSortMergeInfo*          pNonSortMergeInfo = &pInfo->nsortMergeInfo;

  pNonSortMergeInfo->sourceWorkIdx = 0;
  pNonSortMergeInfo->sourceNum = pOperator->numOfDownstream;
  pNonSortMergeInfo->lastSourceIdx = -1;
  pNonSortMergeInfo->pSourceStatus = taosMemoryCalloc(pOperator->numOfDownstream, sizeof(*pNonSortMergeInfo->pSourceStatus));
  if (NULL == pNonSortMergeInfo->pSourceStatus) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    pNonSortMergeInfo->pSourceStatus[i] = i;
  }

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doNonSortMerge(SOperatorInfo* pOperator) {
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SNonSortMergeInfo*          pNonSortMerge = &pInfo->nsortMergeInfo;
  SSDataBlock*                pBlock = NULL;

  qDebug("start to merge no sorted rows, %s", GET_TASKID(pTaskInfo));

  int32_t idx = NON_SORT_NEXT_SRC(pNonSortMerge, pNonSortMerge->lastSourceIdx);
  while (idx < pNonSortMerge->sourceNum) {
    pBlock = getNextBlockFromDownstream(pOperator, pNonSortMerge->pSourceStatus[idx]);
    if (NULL == pBlock) {
      TSWAP(pNonSortMerge->pSourceStatus[pNonSortMerge->sourceWorkIdx], pNonSortMerge->pSourceStatus[idx]);
      pNonSortMerge->sourceWorkIdx++;
      idx = NON_SORT_NEXT_SRC(pNonSortMerge, idx);
      continue;
    }
    break;
  }

  return pBlock;
}

void destroyNonSortMergeOperatorInfo(void* param) {
  SNonSortMergeInfo* pNonSortMerge = param;
  taosMemoryFree(pNonSortMerge->pSourceStatus);
}

int32_t getNonSortMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  return TSDB_CODE_SUCCESS;
}


int32_t openColsMergeOperator(SOperatorInfo* pOperator) {
  return TSDB_CODE_SUCCESS;
}

int32_t copyColumnsValue(SNodeList* pNodeList, uint64_t targetBlkId, SSDataBlock* pDst, SSDataBlock* pSrc) {
  bool isNull = (NULL == pSrc || pSrc->info.rows <= 0);
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN && ((SColumnNode*)pNode->pExpr)->dataBlockId == targetBlkId) {
      SColumnInfoData* pDstCol = taosArrayGet(pDst->pDataBlock, pNode->slotId);
      if (isNull) {
        colDataSetVal(pDstCol, 0, NULL, true);
      } else {
        SColumnInfoData* pSrcCol = taosArrayGet(pSrc->pDataBlock, ((SColumnNode*)pNode->pExpr)->slotId);
        colDataAssign(pDstCol, pSrcCol, 1, &pDst->info);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doColsMerge(SOperatorInfo* pOperator) {
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*                pBlock = NULL;
  SColsMergeInfo*             pColsMerge = &pInfo->colsMergeInfo;
  int32_t                     nullBlkNum = 0;

  qDebug("start to merge columns, %s", GET_TASKID(pTaskInfo));

  for (int32_t i = 0; i < 2; ++i) {
    pBlock = getNextBlockFromDownstream(pOperator, i);
    if (pBlock && pBlock->info.rows > 1) {
      qError("more than 1 row returned from downstream, rows:%" PRId64, pBlock->info.rows);
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    } else if (NULL == pBlock) {
      nullBlkNum++;
    }
    
    copyColumnsValue(pColsMerge->pTargets, pColsMerge->srcBlkIds[i], pInfo->binfo.pRes, pBlock);    
  }

  setOperatorCompleted(pOperator);

  if (2 == nullBlkNum) {
    return NULL;
  }

  pInfo->binfo.pRes->info.rows = 1;

  return pInfo->binfo.pRes;
}

void destroyColsMergeOperatorInfo(void* param) {
}

int32_t getColsMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  return TSDB_CODE_SUCCESS;
}


SOperatorFpSet gMultiwayMergeFps[MERGE_TYPE_MAX_VALUE] = {
  {0},
  {._openFn = openSortMergeOperator, .getNextFn = doSortMerge, .closeFn = destroySortMergeOperatorInfo, .getExplainFn = getSortMergeExplainExecInfo},
  {._openFn = openNonSortMergeOperator, .getNextFn = doNonSortMerge, .closeFn = destroyNonSortMergeOperatorInfo, .getExplainFn = getNonSortMergeExplainExecInfo},
  {._openFn = openColsMergeOperator, .getNextFn = doColsMerge, .closeFn = destroyColsMergeOperatorInfo, .getExplainFn = getColsMergeExplainExecInfo},
};


int32_t openMultiwayMergeOperator(SOperatorInfo* pOperator) {
  int32_t code = 0;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t startTs = taosGetTimestampUs();
  
  if (NULL != gMultiwayMergeFps[pInfo->type]._openFn) {
    code = (*gMultiwayMergeFps[pInfo->type]._openFn)(pOperator);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  OPTR_SET_OPENED(pOperator);
  return code;
}

SSDataBlock* doMultiwayMerge(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSDataBlock* pBlock = NULL;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (NULL != gMultiwayMergeFps[pInfo->type].getNextFn) {
    pBlock = (*gMultiwayMergeFps[pInfo->type].getNextFn)(pOperator);
  }
  if (pBlock != NULL) {
    pOperator->resultInfo.totalRows += pBlock->info.rows;
  } else {
    setOperatorCompleted(pOperator);
  }

  return pBlock;
}

void destroyMultiwayMergeOperatorInfo(void* param) {
  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  if (NULL != gMultiwayMergeFps[pInfo->type].closeFn) {
    (*gMultiwayMergeFps[pInfo->type].closeFn)(&pInfo->sortMergeInfo);
  }

  taosMemoryFreeClear(param);
}

int32_t getMultiwayMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  int32_t code = 0;
  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;

  if (NULL != gMultiwayMergeFps[pInfo->type].getExplainFn) {
    code = (*gMultiwayMergeFps[pInfo->type].getExplainFn)(pOptr, pOptrExplain, len);
  }

  return code;
}

SOperatorInfo* createMultiwayMergeOperatorInfo(SOperatorInfo** downStreams, size_t numStreams,
                                               SMergePhysiNode* pMergePhyNode, SExecTaskInfo* pTaskInfo) {
  SPhysiNode* pPhyNode = (SPhysiNode*)pMergePhyNode;

  SMultiwayMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMultiwayMergeOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*         pDescNode = pPhyNode->pOutputDataBlockDesc;

  int32_t code = TSDB_CODE_SUCCESS;
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->groupMerge = pMergePhyNode->groupSort;
  pInfo->ignoreGroupId = pMergePhyNode->ignoreGroupId;
  pInfo->binfo.inputTsOrder = pMergePhyNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pMergePhyNode->node.outputTsOrder;
  pInfo->inputWithGroupId = pMergePhyNode->inputWithGroupId;

  pInfo->type = pMergePhyNode->type;
  switch (pInfo->type) {
    case MERGE_TYPE_SORT: {
      SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;
      initLimitInfo(pMergePhyNode->node.pLimit, pMergePhyNode->node.pSlimit, &pInfo->limitInfo);
      pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
      
      SPhysiNode*  pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
      SSDataBlock* pInputBlock = createDataBlockFromDescNode(pChildNode->pOutputDataBlockDesc);
      
      initResultSizeInfo(&pOperator->resultInfo, 1024);
      blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

      size_t numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
      int32_t rowSize = pInfo->binfo.pRes->info.rowSize;
      int32_t numOfOutputCols = 0;
      pSortMergeInfo->pSortInfo = createSortInfo(pMergePhyNode->pMergeKeys);
      pSortMergeInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
      pSortMergeInfo->sortBufSize = pSortMergeInfo->bufPageSize * (numStreams + 1);  // one additional is reserved for merged result.
      pSortMergeInfo->pInputBlock = pInputBlock;
      code = extractColMatchInfo(pMergePhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                                 &pSortMergeInfo->matchInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
      break;
    }
    case MERGE_TYPE_NON_SORT: {
      SNonSortMergeInfo* pNonSortMerge = &pInfo->nsortMergeInfo;
      break;
    }
    case MERGE_TYPE_COLUMNS: {
      SColsMergeInfo* pColsMerge = &pInfo->colsMergeInfo;
      pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
      initResultSizeInfo(&pOperator->resultInfo, 1);
      blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

      pColsMerge->pTargets = pMergePhyNode->pTargets;
      pColsMerge->srcBlkIds[0] = getOperatorResultBlockId(downStreams[0], 0);
      pColsMerge->srcBlkIds[1] = getOperatorResultBlockId(downStreams[1], 0);
      break;
    }
    default:
      qError("Invalid merge type: %d", pInfo->type);
      code = TSDB_CODE_INVALID_PARA;
      goto _error;
  }

  setOperatorInfo(pOperator, "MultiwayMergeOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(openMultiwayMergeOperator, doMultiwayMerge, NULL,
                                         destroyMultiwayMergeOperatorInfo, optrDefaultBufFn, getMultiwayMergeExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, downStreams, numStreams);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyMultiwayMergeOperatorInfo(pInfo);
  }

  pTaskInfo->code = code;
  taosMemoryFree(pOperator);
  return NULL;
}
