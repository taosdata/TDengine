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

SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortPhyNode, SExecTaskInfo* pTaskInfo) {
  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL/* || rowSize > 100 * 1024 * 1024*/) {
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

  pInfo->pSortInfo        = createSortInfo(pSortPhyNode->pSortKeys);;
  pInfo->pColMatchInfo    = pColMatchColInfo;
  pOperator->name         = "SortOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SORT;
  pOperator->blocking     = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->exprSupp.pExprInfo        = pExprInfo;
  pOperator->exprSupp.numOfExprs   = numOfCols;
  pOperator->pTaskInfo    = pTaskInfo;

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
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
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
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchInfo* pmInfo = taosArrayGet(pColMatchInfo, i);
      ASSERT(pmInfo->matchType == COL_MATCH_FROM_SLOT_ID);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->targetSlotId);
      colDataAssign(pDst, pSrc, p->info.rows);
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.capacity = p->info.rows;
  }

  blockDataDestroy(p);
  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

SSDataBlock* loadNextDataBlock(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  return pOperator->fpSet.getNextFn(pOperator);
}

// todo refactor: merged with fetch fp
void applyScalarFunction(SSDataBlock* pBlock, void* param) {
  SOperatorInfo*     pOperator = param;
  SSortOperatorInfo* pSort = pOperator->info;
  if (pOperator->exprSupp.pExprInfo != NULL) {
    int32_t code =
        projectApplyFunctions(pOperator->exprSupp.pExprInfo, pBlock, pBlock, pOperator->exprSupp.pCtx, pOperator->exprSupp.numOfExprs, NULL);
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
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1,
                                             NULL, pTaskInfo->id.str);

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

  SSDataBlock* pBlock = getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                           pInfo->pColMatchInfo, pInfo);

  if (pBlock != NULL) {
    pOperator->resultInfo.totalRows += pBlock->info.rows;
  } else {
    doSetOperatorCompleted(pOperator);
  }
  return pBlock;
}

void destroyOrderOperatorInfo(void* param, int32_t numOfOutput) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->pColMatchInfo);
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

typedef struct SMultiwaySortMergeOperatorInfo {
  SOptrBasicInfo binfo;

  int32_t  bufPageSize;
  uint32_t sortBufSize;  // max buffer size for in-memory sort

  SArray*      pSortInfo;
  SSortHandle* pSortHandle;
  SArray*      pColMatchInfo;  // for index map from table scan output

  SSDataBlock* pInputBlock;
  int64_t      startTs;  // sort start time
  uint64_t     groupId;
} SMultiwaySortMergeOperatorInfo;

int32_t doOpenMultiwaySortMergeOperator(SOperatorInfo* pOperator) {
  SMultiwaySortMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->startTs = taosGetTimestampUs();

  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;

  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_MULTISOURCE_MERGE,
                                             pInfo->bufPageSize, numOfBufPage, pInfo->pInputBlock, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, NULL, NULL);

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
  SMultiwaySortMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

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
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchInfo* pmInfo = taosArrayGet(pColMatchInfo, i);
      ASSERT(pmInfo->matchType == COL_MATCH_FROM_SLOT_ID);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->targetSlotId);
      colDataAssign(pDst, pSrc, p->info.rows);
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.capacity = p->info.rows;
    pDataBlock->info.groupId = pInfo->groupId;
  }

  blockDataDestroy(p);

  qDebug("%s get sorted row blocks, rows:%d", GET_TASKID(pTaskInfo), pDataBlock->info.rows);
  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

SSDataBlock* doMultiwaySortMerge(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  SMultiwaySortMergeOperatorInfo* pInfo = pOperator->info;

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

void destroyMultiwaySortMergeOperatorInfo(void* param, int32_t numOfOutput) {
  SMultiwaySortMergeOperatorInfo* pInfo = (SMultiwaySortMergeOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);
  pInfo->pInputBlock = blockDataDestroy(pInfo->pInputBlock);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->pColMatchInfo);
}

int32_t getMultiwaySortMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  ASSERT(pOptr != NULL);
  SSortExecInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SMultiwaySortMergeOperatorInfo* pOperatorInfo = (SMultiwaySortMergeOperatorInfo*)pOptr->info;

  *pInfo = tsortGetSortExecInfo(pOperatorInfo->pSortHandle);
  *pOptrExplain = pInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createMultiwaySortMergeOperatorInfo(SOperatorInfo** downStreams, int32_t numStreams,
                                                   SSDataBlock* pInputBlock, SSDataBlock* pResBlock, SArray* pSortInfo,
                                                   SArray* pColMatchColInfo, SExecTaskInfo* pTaskInfo) {
  SMultiwaySortMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMultiwaySortMergeOperatorInfo));
  SOperatorInfo*                  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  int32_t                         rowSize = pResBlock->info.rowSize;

  if (pInfo == NULL || pOperator == NULL || rowSize > 100 * 1024 * 1024) {
    goto _error;
  }


  initResultSizeInfo(pOperator, 1024);

  pInfo->binfo.pRes    = pResBlock;
  pInfo->pSortInfo     = pSortInfo;
  pInfo->pColMatchInfo = pColMatchColInfo;
  pInfo->pInputBlock   = pInputBlock;
  pOperator->name      = "MultiwaySortMerge";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  pOperator->blocking  = false;
  pOperator->status    = OP_NOT_OPENED;
  pOperator->info      = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pInfo->bufPageSize   = getProperSortPageSize(rowSize);

  // one additional is reserved for merged result.
  pInfo->sortBufSize   = pInfo->bufPageSize * (numStreams + 1);

  pOperator->fpSet =
      createOperatorFpSet(doOpenMultiwaySortMergeOperator, doMultiwaySortMerge, NULL, NULL,
                          destroyMultiwaySortMergeOperatorInfo, NULL, NULL, getMultiwaySortMergeExplainExecInfo);

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