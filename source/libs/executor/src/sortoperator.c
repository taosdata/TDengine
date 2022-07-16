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

#include "executorimpl.h"
#include "tdatablock.h"

static SSDataBlock* doSort(SOperatorInfo* pOperator);
static int32_t      doOpenSortOperator(SOperatorInfo* pOperator);
static int32_t      getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);

static void destroyOrderOperatorInfo(void* param, int32_t numOfOutput);

SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortPhyNode,
                                      SExecTaskInfo* pTaskInfo) {
  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL /* || rowSize > 100 * 1024 * 1024*/) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pSortPhyNode->node.pOutputDataBlockDesc;

  int32_t      numOfCols = 0;
  SSDataBlock* pResBlock = createResDataBlock(pDescNode);
  SExprInfo*   pExprInfo = createExprInfo(pSortPhyNode->pExprs, NULL, &numOfCols);

  int32_t numOfOutputCols = 0;
  SArray* pColMatchColInfo =
      extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID);

  pOperator->exprSupp.pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset);
  pInfo->binfo.pRes = pResBlock;

  initResultSizeInfo(pOperator, 1024);

  pInfo->pSortInfo = createSortInfo(pSortPhyNode->pSortKeys);
  pInfo->pCondition = pSortPhyNode->node.pConditions;
  pInfo->pColMatchInfo = pColMatchColInfo;
  pOperator->name = "SortOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SORT;
  pOperator->blocking = true;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  // lazy evaluation for the following parameter since the input datablock is not known till now.
  //  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;
  //  there are headers, so pageSize = rowSize + header pInfo->sortBufSize  = pInfo->bufPageSize * 16;
  // TODO dynamic set the available sort buffer

  pOperator->fpSet = createOperatorFpSet(doOpenSortOperator, doSort, NULL, NULL, destroyOrderOperatorInfo, NULL, NULL,
                                         getExplainExecInfo);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
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
      SColMatchInfo* pmInfo = taosArrayGet(pColMatchInfo, i);
      ASSERT(pmInfo->matchType == COL_MATCH_FROM_SLOT_ID);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->targetSlotId);
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
  if (pOperator->exprSupp.pExprInfo != NULL) {
    int32_t code = projectApplyFunctions(pOperator->exprSupp.pExprInfo, pBlock, pBlock, pOperator->exprSupp.pCtx,
                                         pOperator->exprSupp.numOfExprs, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pOperator->pTaskInfo->env, code);
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
  tsortAddSource(pInfo->pSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pSortHandle);
  taosMemoryFreeClear(ps);

  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
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
    longjmp(pTaskInfo->env, code);
  }

  SSDataBlock* pBlock = NULL;
  while (1) {
    pBlock = getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                             pInfo->pColMatchInfo, pInfo);
    if (pBlock != NULL) {
      doFilter(pInfo->pCondition, pBlock);
    }

    if (pBlock == NULL) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    if (blockDataGetNumOfRows(pBlock) > 0) {
      break;
    }
  }

  if (pBlock != NULL) {
    pOperator->resultInfo.totalRows += pBlock->info.rows;
  }

  return pBlock;
}

void destroyOrderOperatorInfo(void* param, int32_t numOfOutput) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->pColMatchInfo);
  
  taosMemoryFreeClear(param);
}

int32_t getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  ASSERT(pOptr != NULL);
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
  SOptrBasicInfo binfo;
  SArray*        pSortInfo;
  SArray*        pColMatchInfo;

  int64_t  startTs;
  uint64_t sortElapsed;
  bool     hasGroupId;
  uint64_t currGroupId;

  SSDataBlock*         prefetchedSortInput;
  SSortHandle*         pCurrSortHandle;
  EChildOperatorStatus childOpStatus;

  SSortExecInfo sortExecInfo;
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
      SColMatchInfo* pmInfo = taosArrayGet(pColMatchInfo, i);
      ASSERT(pmInfo->matchType == COL_MATCH_FROM_SLOT_ID);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->targetSlotId);
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
      if (block->info.groupId == grpSortOpInfo->currGroupId) {
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
  tsortAddSource(pInfo->pCurrSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pCurrSortHandle);
  taosMemoryFreeClear(ps);

  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
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
  if (pInfo->pCurrSortHandle != NULL) {
    tsortDestroySortHandle(pInfo->pCurrSortHandle);
  }
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
    longjmp(pTaskInfo->env, code);
  }

  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    pInfo->prefetchedSortInput = pOperator->pDownstream[0]->fpSet.getNextFn(pOperator->pDownstream[0]);
    if (pInfo->prefetchedSortInput == NULL) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->currGroupId = pInfo->prefetchedSortInput->info.groupId;
    pInfo->childOpStatus = CHILD_OP_NEW_GROUP;
    beginSortGroup(pOperator);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->pCurrSortHandle != NULL) {
    // beginSortGroup would fetch all child blocks of pInfo->currGroupId;
    ASSERT(pInfo->childOpStatus != CHILD_OP_SAME_GROUP);
    pBlock = getGroupSortedBlockData(pInfo->pCurrSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                     pInfo->pColMatchInfo, pInfo);
    if (pBlock != NULL) {
      pBlock->info.groupId = pInfo->currGroupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      return pBlock;
    } else {
      if (pInfo->childOpStatus == CHILD_OP_NEW_GROUP) {
        finishSortGroup(pOperator);
        pInfo->currGroupId = pInfo->prefetchedSortInput->info.groupId;
        beginSortGroup(pOperator);
      } else if (pInfo->childOpStatus == CHILD_OP_FINISHED) {
        finishSortGroup(pOperator);
        doSetOperatorCompleted(pOperator);
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

void destroyGroupSortOperatorInfo(void* param, int32_t numOfOutput) {
  SGroupSortOperatorInfo* pInfo = (SGroupSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->pColMatchInfo);
  
  taosMemoryFreeClear(param);
}

SOperatorInfo* createGroupSortOperatorInfo(SOperatorInfo* downstream, SGroupSortPhysiNode* pSortPhyNode,
                                           SExecTaskInfo* pTaskInfo) {
  SGroupSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupSortOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL /* || rowSize > 100 * 1024 * 1024*/) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pSortPhyNode->node.pOutputDataBlockDesc;

  int32_t      numOfCols = 0;
  SSDataBlock* pResBlock = createResDataBlock(pDescNode);
  SExprInfo*   pExprInfo = createExprInfo(pSortPhyNode->pExprs, NULL, &numOfCols);

  int32_t numOfOutputCols = 0;
  SArray* pColMatchColInfo =
      extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID);

  pOperator->exprSupp.pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset);
  pInfo->binfo.pRes = pResBlock;

  initResultSizeInfo(pOperator, 1024);

  pInfo->pSortInfo = createSortInfo(pSortPhyNode->pSortKeys);
  ;
  pInfo->pColMatchInfo = pColMatchColInfo;
  pOperator->name = "GroupSortOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doGroupSort, NULL, NULL, destroyGroupSortOperatorInfo,
                                         NULL, NULL, getGroupSortExplainExecInfo);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

//=====================================================================================
// Multiway Sort Merge operator
typedef struct SMultiwayMergeOperatorInfo {
  SOptrBasicInfo binfo;

  int32_t  bufPageSize;
  uint32_t sortBufSize;  // max buffer size for in-memory sort

  SArray*      pSortInfo;
  SSortHandle* pSortHandle;
  SArray*      pColMatchInfo;  // for index map from table scan output

  SSDataBlock*  pInputBlock;
  int64_t       startTs;  // sort start time
  bool          groupSort;
  bool          hasGroupId;
  uint64_t      groupId;
  STupleHandle* prefetchedTuple;
} SMultiwayMergeOperatorInfo;

int32_t doOpenMultiwayMergeOperator(SOperatorInfo* pOperator) {
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
    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    ps->param = pOperator->pDownstream[i];
    tsortAddSource(pInfo->pSortHandle, ps);
  }

  int32_t code = tsortOpen(pInfo->pSortHandle);

  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - pInfo->startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* getMultiwaySortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity,
                                        SArray* pColMatchInfo, SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  blockDataCleanup(pDataBlock);

  SSDataBlock* p = tsortGetSortedDataBlock(pHandle);
  if (p == NULL) {
    return NULL;
  }

  blockDataEnsureCapacity(p, capacity);

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (pInfo->groupSort) {
      if (pInfo->prefetchedTuple == NULL) {
        pTupleHandle = tsortNextTuple(pHandle);
      } else {
        pTupleHandle = pInfo->prefetchedTuple;
        pInfo->groupId = tsortGetGroupId(pTupleHandle);
        pInfo->prefetchedTuple = NULL;
      }
    }
    else {
      pTupleHandle = tsortNextTuple(pHandle);
      pInfo->groupId = 0;
    }

    if (pTupleHandle == NULL) {
      break;
    }

    if (pInfo->groupSort) {
      uint64_t tupleGroupId = tsortGetGroupId(pTupleHandle);
      if (!pInfo->hasGroupId) {
        pInfo->groupId = tupleGroupId;
        pInfo->hasGroupId = true;
        appendOneRowToDataBlock(p, pTupleHandle);
      } else if (pInfo->groupId == tupleGroupId) {
        appendOneRowToDataBlock(p, pTupleHandle);
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
  if (pInfo->groupSort) {
    pInfo->hasGroupId = false;
  }
  if (p->info.rows > 0) {  // todo extract method
    blockDataEnsureCapacity(pDataBlock, p->info.rows);
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchInfo* pmInfo = taosArrayGet(pColMatchInfo, i);
      ASSERT(pmInfo->matchType == COL_MATCH_FROM_SLOT_ID);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->targetSlotId);
      colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.groupId = pInfo->groupId;
  }

  blockDataDestroy(p);

  qDebug("%s get sorted row blocks, rows:%d", GET_TASKID(pTaskInfo), pDataBlock->info.rows);
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
    longjmp(pTaskInfo->env, code);
  }

  SSDataBlock* pBlock = getMultiwaySortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes,
                                                   pOperator->resultInfo.capacity, pInfo->pColMatchInfo, pOperator);
  if (pBlock != NULL) {
    pOperator->resultInfo.totalRows += pBlock->info.rows;
  } else {
    doSetOperatorCompleted(pOperator);
  }
  return pBlock;
}

void destroyMultiwayMergeOperatorInfo(void* param, int32_t numOfOutput) {
  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);
  pInfo->pInputBlock = blockDataDestroy(pInfo->pInputBlock);

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->pColMatchInfo);
  
  taosMemoryFreeClear(param);
}

int32_t getMultiwayMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  ASSERT(pOptr != NULL);
  SSortExecInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SMultiwayMergeOperatorInfo* pOperatorInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;

  *pInfo = tsortGetSortExecInfo(pOperatorInfo->pSortHandle);
  *pOptrExplain = pInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createMultiwayMergeOperatorInfo(SOperatorInfo** downStreams, size_t numStreams,
                                                SMergePhysiNode* pMergePhyNode, SExecTaskInfo* pTaskInfo) {
  SPhysiNode* pPhyNode = (SPhysiNode*)pMergePhyNode;

  SMultiwayMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMultiwayMergeOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*         pDescNode = pPhyNode->pOutputDataBlockDesc;
  SSDataBlock*                pResBlock = createResDataBlock(pDescNode);

  int32_t rowSize = pResBlock->info.rowSize;

  if (pInfo == NULL || pOperator == NULL || rowSize > 100 * 1024 * 1024) {
    goto _error;
  }

  SArray* pSortInfo = createSortInfo(pMergePhyNode->pMergeKeys);
  int32_t numOfOutputCols = 0;
  SArray* pColMatchColInfo =
      extractColMatchInfo(pMergePhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID);
  SPhysiNode*  pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
  SSDataBlock* pInputBlock = createResDataBlock(pChildNode->pOutputDataBlockDesc);
  initResultSizeInfo(pOperator, 1024);

  pInfo->groupSort = pMergePhyNode->groupSort;
  pInfo->binfo.pRes = pResBlock;
  pInfo->pSortInfo = pSortInfo;
  pInfo->pColMatchInfo = pColMatchColInfo;
  pInfo->pInputBlock = pInputBlock;
  pOperator->name = "MultiwayMerge";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pInfo->bufPageSize = getProperSortPageSize(rowSize);

  // one additional is reserved for merged result.
  pInfo->sortBufSize = pInfo->bufPageSize * (numStreams + 1);

  pOperator->fpSet = createOperatorFpSet(doOpenMultiwayMergeOperator, doMultiwayMerge, NULL, NULL,
                                         destroyMultiwayMergeOperatorInfo, NULL, NULL, getMultiwayMergeExplainExecInfo);

  int32_t code = appendDownstream(pOperator, downStreams, numStreams);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}
