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
#include "executorimpl.h"
#include "tdatablock.h"

typedef struct SSortOperatorInfo {
  SOptrBasicInfo binfo;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  SColMatchInfo  matchInfo;
  int32_t        bufPageSize;
  int64_t        startTs;      // sort start time
  uint64_t       sortElapsed;  // sort elapsed time, time to flush to disk not included.
  SLimitInfo     limitInfo;
} SSortOperatorInfo;

static SSDataBlock* doSort(SOperatorInfo* pOperator);
static int32_t      doOpenSortOperator(SOperatorInfo* pOperator);
static int32_t      getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);

static void destroySortOperatorInfo(void* param);

// todo add limit/offset impl
SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortNode, SExecTaskInfo* pTaskInfo) {
  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  SDataBlockDescNode* pDescNode = pSortNode->node.pOutputDataBlockDesc;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pSortNode->pExprs, NULL, &numOfCols);

  int32_t numOfOutputCols = 0;
  int32_t code =
      extractColMatchInfo(pSortNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->exprSupp.pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset);
  initResultSizeInfo(&pOperator->resultInfo, 1024);
  code = filterInitFromNode((SNode*)pSortNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  pInfo->pSortInfo = createSortInfo(pSortNode->pSortKeys);
  initLimitInfo(pSortNode->node.pLimit, pSortNode->node.pSlimit, &pInfo->limitInfo);

  setOperatorInfo(pOperator, "SortOperator", QUERY_NODE_PHYSICAL_PLAN_SORT, true, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;

  // lazy evaluation for the following parameter since the input datablock is not known till now.
  //  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;
  //  there are headers, so pageSize = rowSize + header pInfo->sortBufSize  = pInfo->bufPageSize * 16;
  // TODO dynamic set the available sort buffer

  pOperator->fpSet =
      createOperatorFpSet(doOpenSortOperator, doSort, NULL, destroySortOperatorInfo, optrDefaultBufFn, getExplainExecInfo);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  if (pInfo != NULL) {
    destroySortOperatorInfo(pInfo);
  }

  taosMemoryFree(pOperator);
  return NULL;
}

void appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    bool             isNull = tsortIsNullVal(pTupleHandle, i);
    if (isNull) {
      colDataAppendNULL(pColInfo, pBlock->info.rows);
    } else {
      char* pData = tsortGetValue(pTupleHandle, i);
      if (pData != NULL) {
        colDataAppend(pColInfo, pBlock->info.rows, pData, false);
      }
    }
  }

  pBlock->info.dataLoad = 1;
  pBlock->info.rows += 1;
}

SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo,
                                SSortOperatorInfo* pInfo) {
  blockDataCleanup(pDataBlock);

  SSDataBlock* p = tsortGetSortedDataBlock(pHandle);
  if (p == NULL) {
    return NULL;
  }

  blockDataEnsureCapacity(p, capacity);

  while (1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(p, pTupleHandle);
    if (p->info.rows >= capacity) {
      break;
    }
  }

  if (p->info.rows > 0) {
    blockDataEnsureCapacity(pDataBlock, capacity);

    // todo extract function to handle this
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->dstSlotId);
      colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
    }

    pDataBlock->info.rows = p->info.rows;
  }

  blockDataDestroy(p);
  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

SSDataBlock* loadNextDataBlock(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  SSDataBlock*   pBlock = pOperator->fpSet.getNextFn(pOperator);
  return pBlock;
}

// todo refactor: merged with fetch fp
void applyScalarFunction(SSDataBlock* pBlock, void* param) {
  SOperatorInfo*     pOperator = param;
  SSortOperatorInfo* pSort = pOperator->info;
  if (pOperator->exprSupp.pExprInfo != NULL && pOperator->exprSupp.numOfExprs > 0) {
    int32_t code = projectApplyFunctions(pOperator->exprSupp.pExprInfo, pBlock, pBlock, pOperator->exprSupp.pCtx,
                                         pOperator->exprSupp.numOfExprs, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }
  }
}

int32_t doOpenSortOperator(SOperatorInfo* pOperator) {
  SSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->startTs = taosGetTimestampUs();

  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, applyScalarFunction, pOperator);

  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  ps->param = pOperator->pDownstream[0];
  ps->onlyRef = true;
  tsortAddSource(pInfo->pSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pSortHandle);

  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - pInfo->startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doSort(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSortOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  // multi-group case not handle here
  SSDataBlock* pBlock = NULL;
  while (1) {
    pBlock = getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                pInfo->matchInfo.pList, pInfo);
    if (pBlock == NULL) {
      setOperatorCompleted(pOperator);
      return NULL;
    }

    doFilter(pBlock, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo);
    if (blockDataGetNumOfRows(pBlock) == 0) {
      continue;
    }

    // there are bugs?
    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
    if (limitReached) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

    pOperator->resultInfo.totalRows += pBlock->info.rows;
    if (pBlock->info.rows > 0) {
      break;
    }
  }

  return blockDataGetNumOfRows(pBlock) > 0 ? pBlock : NULL;
}

void destroySortOperatorInfo(void* param) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(param);
}

int32_t getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SSortOperatorInfo* pOperatorInfo = (SSortOperatorInfo*)pOptr->info;

  *pInfo = tsortGetSortExecInfo(pOperatorInfo->pSortHandle);
  *pOptrExplain = pInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

//=====================================================================================
// Group Sort Operator
typedef enum EChildOperatorStatus { CHILD_OP_NEW_GROUP, CHILD_OP_SAME_GROUP, CHILD_OP_FINISHED } EChildOperatorStatus;

typedef struct SGroupSortOperatorInfo {
  SOptrBasicInfo       binfo;
  SArray*              pSortInfo;
  SColMatchInfo        matchInfo;
  int64_t              startTs;
  uint64_t             sortElapsed;
  bool                 hasGroupId;
  uint64_t             currGroupId;
  SSDataBlock*         prefetchedSortInput;
  SSortHandle*         pCurrSortHandle;
  EChildOperatorStatus childOpStatus;
  SSortExecInfo        sortExecInfo;
} SGroupSortOperatorInfo;

SSDataBlock* getGroupSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity,
                                     SArray* pColMatchInfo, SGroupSortOperatorInfo* pInfo) {
  blockDataCleanup(pDataBlock);
  blockDataEnsureCapacity(pDataBlock, capacity);

  SSDataBlock* p = tsortGetSortedDataBlock(pHandle);
  if (p == NULL) {
    return NULL;
  }

  blockDataEnsureCapacity(p, capacity);

  while (1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(p, pTupleHandle);
    if (p->info.rows >= capacity) {
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
    pDataBlock->info.capacity = p->info.rows;
  }

  blockDataDestroy(p);
  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

typedef struct SGroupSortSourceParam {
  SOperatorInfo*          childOpInfo;
  SGroupSortOperatorInfo* grpSortOpInfo;
} SGroupSortSourceParam;

SSDataBlock* fetchNextGroupSortDataBlock(void* param) {
  SGroupSortSourceParam*  source = param;
  SGroupSortOperatorInfo* grpSortOpInfo = source->grpSortOpInfo;
  if (grpSortOpInfo->prefetchedSortInput) {
    SSDataBlock* block = grpSortOpInfo->prefetchedSortInput;
    grpSortOpInfo->prefetchedSortInput = NULL;
    return block;
  } else {
    SOperatorInfo* childOp = source->childOpInfo;
    SSDataBlock*   block = childOp->fpSet.getNextFn(childOp);
    if (block != NULL) {
      if (block->info.id.groupId == grpSortOpInfo->currGroupId) {
        grpSortOpInfo->childOpStatus = CHILD_OP_SAME_GROUP;
        return block;
      } else {
        grpSortOpInfo->childOpStatus = CHILD_OP_NEW_GROUP;
        grpSortOpInfo->prefetchedSortInput = block;
        return NULL;
      }
    } else {
      grpSortOpInfo->childOpStatus = CHILD_OP_FINISHED;
      return NULL;
    }
  }
}

int32_t beginSortGroup(SOperatorInfo* pOperator) {
  SGroupSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pCurrSortHandle =
      tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pCurrSortHandle, fetchNextGroupSortDataBlock, applyScalarFunction, pOperator);

  SSortSource*           ps = taosMemoryCalloc(1, sizeof(SSortSource));
  SGroupSortSourceParam* param = taosMemoryCalloc(1, sizeof(SGroupSortSourceParam));
  param->childOpInfo = pOperator->pDownstream[0];
  param->grpSortOpInfo = pInfo;
  ps->param = param;
  ps->onlyRef = false;
  tsortAddSource(pInfo->pCurrSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pCurrSortHandle);

  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t finishSortGroup(SOperatorInfo* pOperator) {
  SGroupSortOperatorInfo* pInfo = pOperator->info;

  SSortExecInfo sortExecInfo = tsortGetSortExecInfo(pInfo->pCurrSortHandle);

  pInfo->sortExecInfo.sortMethod = sortExecInfo.sortMethod;
  pInfo->sortExecInfo.sortBuffer = sortExecInfo.sortBuffer;
  pInfo->sortExecInfo.loops += sortExecInfo.loops;
  pInfo->sortExecInfo.readBytes += sortExecInfo.readBytes;
  pInfo->sortExecInfo.writeBytes += sortExecInfo.writeBytes;

  tsortDestroySortHandle(pInfo->pCurrSortHandle);
  pInfo->pCurrSortHandle = NULL;

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doGroupSort(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SGroupSortOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    pInfo->prefetchedSortInput = pOperator->pDownstream[0]->fpSet.getNextFn(pOperator->pDownstream[0]);
    if (pInfo->prefetchedSortInput == NULL) {
      setOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->currGroupId = pInfo->prefetchedSortInput->info.id.groupId;
    pInfo->childOpStatus = CHILD_OP_NEW_GROUP;
    beginSortGroup(pOperator);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->pCurrSortHandle != NULL) {
    // beginSortGroup would fetch all child blocks of pInfo->currGroupId;
    ASSERT(pInfo->childOpStatus != CHILD_OP_SAME_GROUP);
    pBlock = getGroupSortedBlockData(pInfo->pCurrSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                     pInfo->matchInfo.pList, pInfo);
    if (pBlock != NULL) {
      pBlock->info.id.groupId = pInfo->currGroupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      return pBlock;
    } else {
      if (pInfo->childOpStatus == CHILD_OP_NEW_GROUP) {
        finishSortGroup(pOperator);
        pInfo->currGroupId = pInfo->prefetchedSortInput->info.id.groupId;
        beginSortGroup(pOperator);
      } else if (pInfo->childOpStatus == CHILD_OP_FINISHED) {
        finishSortGroup(pOperator);
        setOperatorCompleted(pOperator);
        return NULL;
      }
    }
  }
  return NULL;
}

int32_t getGroupSortExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SGroupSortOperatorInfo* pInfo = (SGroupSortOperatorInfo*)pOptr->info;
  *pOptrExplain = &pInfo->sortExecInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

void destroyGroupSortOperatorInfo(void* param) {
  SGroupSortOperatorInfo* pInfo = (SGroupSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);

  tsortDestroySortHandle(pInfo->pCurrSortHandle);
  pInfo->pCurrSortHandle = NULL;

  taosMemoryFreeClear(param);
}

SOperatorInfo* createGroupSortOperatorInfo(SOperatorInfo* downstream, SGroupSortPhysiNode* pSortPhyNode,
                                           SExecTaskInfo* pTaskInfo) {
  SGroupSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupSortOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SExprSupp* pSup = &pOperator->exprSupp;
  SDataBlockDescNode* pDescNode = pSortPhyNode->node.pOutputDataBlockDesc;

  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pSortPhyNode->pExprs, NULL, &numOfCols);

  pSup->pExprInfo = pExprInfo;
  pSup->numOfExprs = numOfCols;

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  pOperator->exprSupp.pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset);

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  int32_t numOfOutputCols = 0;
  int32_t code = extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                                     &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto  _error;
  }

  pInfo->pSortInfo = createSortInfo(pSortPhyNode->pSortKeys);
  setOperatorInfo(pOperator, "GroupSortOperator", QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doGroupSort, NULL, destroyGroupSortOperatorInfo,
                                         optrDefaultBufFn, getGroupSortExplainExecInfo);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = code;
  if (pInfo != NULL) {
    destroyGroupSortOperatorInfo(pInfo);
  }
  taosMemoryFree(pOperator);
  return NULL;
}

//=====================================================================================
// Multiway Sort Merge operator
typedef struct SMultiwayMergeOperatorInfo {
  SOptrBasicInfo binfo;
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SLimitInfo     limitInfo;
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  SColMatchInfo  matchInfo;
  SSDataBlock*   pInputBlock;
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  int64_t        startTs;  // sort start time
  bool           groupSort;
  uint64_t       groupId;
  STupleHandle*  prefetchedTuple;
} SMultiwayMergeOperatorInfo;

int32_t openMultiwayMergeOperator(SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->startTs = taosGetTimestampUs();
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;

  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->pInputBlock, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, NULL, NULL);
  tsortSetCompareGroupId(pInfo->pSortHandle, pInfo->groupSort);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      pDownstream->fpSet._openFn(pDownstream);
    }

    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    ps->param = pDownstream;
    ps->onlyRef = true;

    tsortAddSource(pInfo->pSortHandle, ps);
  }

  int32_t code = tsortOpen(pInfo->pSortHandle);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - pInfo->startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

static void doGetSortedBlockData(SMultiwayMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                 SSDataBlock* p, bool* newgroup) {
  *newgroup = false;

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (pInfo->groupSort) {
      if (pInfo->prefetchedTuple == NULL) {
        pTupleHandle = tsortNextTuple(pHandle);
      } else {
        pTupleHandle = pInfo->prefetchedTuple;
        pInfo->prefetchedTuple = NULL;
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

    if (pInfo->groupSort) {
      uint64_t tupleGroupId = tsortGetGroupId(pTupleHandle);
      if (pInfo->groupId == 0 || pInfo->groupId == tupleGroupId) {
        appendOneRowToDataBlock(p, pTupleHandle);
        p->info.id.groupId = tupleGroupId;
        pInfo->groupId = tupleGroupId;
      } else {
        pInfo->prefetchedTuple = pTupleHandle;
        break;
      }
    } else {
      appendOneRowToDataBlock(p, pTupleHandle);
    }

    if (p->info.rows >= capacity) {
      break;
    }
  }
}

SSDataBlock* getMultiwaySortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, SArray* pColMatchInfo,
                                        SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;

  int32_t capacity = pOperator->resultInfo.capacity;

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  blockDataCleanup(pDataBlock);

  if (pInfo->pIntermediateBlock == NULL) {
    pInfo->pIntermediateBlock = tsortGetSortedDataBlock(pHandle);
    if (pInfo->pIntermediateBlock == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    blockDataEnsureCapacity(pInfo->pIntermediateBlock, capacity);
  } else {
    blockDataCleanup(pInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pInfo->pIntermediateBlock;
  bool         newgroup = false;

  while (1) {
    doGetSortedBlockData(pInfo, pHandle, capacity, p, &newgroup);
    if (p->info.rows == 0) {
      break;
    }

    if (newgroup) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

    bool limitReached = applyLimitOffset(&pInfo->limitInfo, p, pTaskInfo);
    if (limitReached) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

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
    pDataBlock->info.id.groupId = pInfo->groupId;
    pDataBlock->info.dataLoad = 1;
  }

  qDebug("%s get sorted block, groupId:0x%" PRIx64 " rows:%d", GET_TASKID(pTaskInfo), pDataBlock->info.id.groupId,
         pDataBlock->info.rows);

  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

SSDataBlock* doMultiwayMerge(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  SSDataBlock* pBlock = getMultiwaySortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pInfo->matchInfo.pList, pOperator);
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
  pInfo->pInputBlock = blockDataDestroy(pInfo->pInputBlock);
  pInfo->pIntermediateBlock = blockDataDestroy(pInfo->pIntermediateBlock);

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);

  taosMemoryFreeClear(param);
}

int32_t getMultiwayMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pSortExecInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;

  *pSortExecInfo = tsortGetSortExecInfo(pInfo->pSortHandle);
  *pOptrExplain = pSortExecInfo;

  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
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

  initLimitInfo(pMergePhyNode->node.pLimit, pMergePhyNode->node.pSlimit, &pInfo->limitInfo);
  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);

  int32_t rowSize = pInfo->binfo.pRes->info.rowSize;
  ASSERT(rowSize < 100 * 1024 * 1024);

  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pMergePhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                             &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SPhysiNode*  pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
  SSDataBlock* pInputBlock = createDataBlockFromDescNode(pChildNode->pOutputDataBlockDesc);

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  pInfo->groupSort = pMergePhyNode->groupSort;
  pInfo->pSortInfo = createSortInfo(pMergePhyNode->pMergeKeys);
  pInfo->pInputBlock = pInputBlock;
  size_t numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  pInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
  pInfo->sortBufSize = pInfo->bufPageSize * (numStreams + 1);  // one additional is reserved for merged result.

  setOperatorInfo(pOperator, "MultiwayMergeOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(openMultiwayMergeOperator, doMultiwayMerge, NULL,
                                         destroyMultiwayMergeOperatorInfo, optrDefaultBufFn, getMultiwayMergeExplainExecInfo);

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
