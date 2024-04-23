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

typedef struct SSortOpGroupIdCalc {
  STupleHandle* pSavedTuple;
  SArray*       pSortColsArr;
  char*         keyBuf;
  int32_t       lastKeysLen; // default to be 0
  uint64_t      lastGroupId;
  bool          excludePKCol;
} SSortOpGroupIdCalc;

typedef struct SSortOperatorInfo {
  SOptrBasicInfo      binfo;
  uint32_t            sortBufSize;  // max buffer size for in-memory sort
  SArray*             pSortInfo;
  SSortHandle*        pSortHandle;
  SColMatchInfo       matchInfo;
  int32_t             bufPageSize;
  int64_t             startTs;      // sort start time
  uint64_t            sortElapsed;  // sort elapsed time, time to flush to disk not included.
  SLimitInfo          limitInfo;
  uint64_t            maxTupleLength;
  int64_t             maxRows;
  SSortOpGroupIdCalc* pGroupIdCalc;
} SSortOperatorInfo;

static SSDataBlock* doSort(SOperatorInfo* pOperator);
static int32_t      doOpenSortOperator(SOperatorInfo* pOperator);
static int32_t      getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);

static void destroySortOperatorInfo(void* param);
static int32_t calcSortOperMaxTupleLength(SSortOperatorInfo* pSortOperInfo, SNodeList* pSortKeys);

static void destroySortOpGroupIdCalc(SSortOpGroupIdCalc* pCalc);

// todo add limit/offset impl
SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortNode, SExecTaskInfo* pTaskInfo) {
  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;
  SDataBlockDescNode* pDescNode = pSortNode->node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  pOperator->exprSupp.pExprInfo = createExprInfo(pSortNode->pExprs, NULL, &numOfCols);
  pOperator->exprSupp.numOfExprs = numOfCols;
  int32_t numOfOutputCols = 0;
  int32_t code =
      extractColMatchInfo(pSortNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  
  calcSortOperMaxTupleLength(pInfo, pSortNode->pSortKeys);
  pInfo->maxRows = -1;
  if (pSortNode->node.pLimit) {
    SLimitNode* pLimit = (SLimitNode*)pSortNode->node.pLimit;
    if (pLimit->limit > 0) pInfo->maxRows = pLimit->limit + pLimit->offset;
  }

  pOperator->exprSupp.pCtx =
      createSqlFunctionCtx(pOperator->exprSupp.pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);
  initResultSizeInfo(&pOperator->resultInfo, 1024);
  code = filterInitFromNode((SNode*)pSortNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  pInfo->pSortInfo = createSortInfo(pSortNode->pSortKeys);

  if (pSortNode->calcGroupId) {
    int32_t keyLen;
    SSortOpGroupIdCalc* pGroupIdCalc = pInfo->pGroupIdCalc = taosMemoryCalloc(1, sizeof(SSortOpGroupIdCalc));
    if (!pGroupIdCalc) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    SNodeList* pSortColsNodeArr = makeColsNodeArrFromSortKeys(pSortNode->pSortKeys);
    if (!pSortColsNodeArr) code = TSDB_CODE_OUT_OF_MEMORY;
    if (TSDB_CODE_SUCCESS == code) {
      pGroupIdCalc->pSortColsArr = makeColumnArrayFromList(pSortColsNodeArr);
      if (!pGroupIdCalc->pSortColsArr) code = TSDB_CODE_OUT_OF_MEMORY;
      nodesClearList(pSortColsNodeArr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      // PK ts col should always at last, see partColOptCreateSort
      if (pSortNode->excludePkCol) taosArrayPop(pGroupIdCalc->pSortColsArr);
      keyLen = extractKeysLen(pGroupIdCalc->pSortColsArr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pGroupIdCalc->lastKeysLen = 0;
      pGroupIdCalc->keyBuf = taosMemoryCalloc(1, keyLen);
      if (!pGroupIdCalc->keyBuf) code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (code != TSDB_CODE_SUCCESS) goto _error;

  pInfo->binfo.inputTsOrder = pSortNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pSortNode->node.outputTsOrder;
  initLimitInfo(pSortNode->node.pLimit, pSortNode->node.pSlimit, &pInfo->limitInfo);

  setOperatorInfo(pOperator, "SortOperator", QUERY_NODE_PHYSICAL_PLAN_SORT, true, OP_NOT_OPENED, pInfo, pTaskInfo);


  // lazy evaluation for the following parameter since the input datablock is not known till now.
  //  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;
  //  there are headers, so pageSize = rowSize + header pInfo->sortBufSize  = pInfo->bufPageSize * 16;
  // TODO dynamic set the available sort buffer

  pOperator->fpSet =
      createOperatorFpSet(doOpenSortOperator, doSort, NULL, destroySortOperatorInfo, optrDefaultBufFn, getExplainExecInfo, optrDefaultGetNextExtFn, NULL);

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
      colDataSetNULL(pColInfo, pBlock->info.rows);
    } else {
      char* pData = tsortGetValue(pTupleHandle, i);
      if (pData != NULL) {
        colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
      }
    }
  }

  pBlock->info.dataLoad = 1;
  pBlock->info.scanFlag = ((SDataBlockInfo*)tsortGetBlockInfo(pTupleHandle))->scanFlag;
  pBlock->info.rows += 1;
}

/**
 * @brief get next tuple with group id attached, here assume that all tuples are sorted by group keys
 * @param [in, out] pBlock the output block, the group id will be saved in it
 * @retval NULL if next group tuple arrived and this new group tuple will be saved in pInfo.pSavedTuple
 * @retval NULL if no more tuples
 */
static STupleHandle* nextTupleWithGroupId(SSortHandle* pHandle, SSortOperatorInfo* pInfo, SSDataBlock* pBlock) {
  STupleHandle* retTuple = pInfo->pGroupIdCalc->pSavedTuple;
  if (!retTuple) {
    retTuple = tsortNextTuple(pHandle);
  }

  if (retTuple) {
    int32_t newGroup;
    if (pInfo->pGroupIdCalc->pSavedTuple) {
      newGroup = true;
      pInfo->pGroupIdCalc->pSavedTuple = NULL;
    } else {
      newGroup = tsortCompAndBuildKeys(pInfo->pGroupIdCalc->pSortColsArr, pInfo->pGroupIdCalc->keyBuf,
                                       &pInfo->pGroupIdCalc->lastKeysLen, retTuple);
    }
    bool emptyBlock = pBlock->info.rows == 0;
    if (newGroup) {
      if (!emptyBlock) {
        // new group arrived, and we have already copied some tuples for cur group, save the new group tuple, return
        // NULL. Note that the keyBuf and lastKeysLen has been updated to new value
        pInfo->pGroupIdCalc->pSavedTuple = retTuple;
        retTuple = NULL;
      } else {
        // new group with empty block
        pInfo->pGroupIdCalc->lastGroupId = pBlock->info.id.groupId =
            calcGroupId(pInfo->pGroupIdCalc->keyBuf, pInfo->pGroupIdCalc->lastKeysLen);
      }
    } else {
      if (emptyBlock) {
        // new block but not new group, assign last group id to it
        pBlock->info.id.groupId = pInfo->pGroupIdCalc->lastGroupId;
      } else {
        // not new group and not empty block and ret NOT NULL, just return the tuple
      }
    }
  }

  return retTuple;
}

SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo,
                                SSortOperatorInfo* pInfo) {
  blockDataCleanup(pDataBlock);

  SSDataBlock* p = tsortGetSortedDataBlock(pHandle);
  if (p == NULL) {
    return NULL;
  }

  blockDataEnsureCapacity(p, capacity);

  STupleHandle* pTupleHandle;
  while (1) {
    if (pInfo->pGroupIdCalc) {
      pTupleHandle = nextTupleWithGroupId(pHandle, pInfo, p);
    } else {
      pTupleHandle = tsortNextTuple(pHandle);
    }
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

    pDataBlock->info.dataLoad = 1;
    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    pDataBlock->info.id.groupId = p->info.id.groupId;
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
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str,
                                             pInfo->maxRows, pInfo->maxTupleLength, tsPQSortMemThreshold * 1024 * 1024);

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
    if (tsortIsClosed(pInfo->pSortHandle)) {
      terrno = TSDB_CODE_TSC_QUERY_CANCELLED;
      T_LONG_JMP(pOperator->pTaskInfo->env, terrno);
    }

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
  destroySortOpGroupIdCalc(pInfo->pGroupIdCalc);
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

static int32_t calcSortOperMaxTupleLength(SSortOperatorInfo* pSortOperInfo, SNodeList* pSortKeys) {
  SColMatchInfo* pColItem = &pSortOperInfo->matchInfo;
  size_t         size = taosArrayGetSize(pColItem->pList);
  for (size_t i = 0; i < size; ++i) {
    pSortOperInfo->maxTupleLength += ((SColMatchItem*)taosArrayGet(pColItem->pList, i))->dataType.bytes;
  }
  size = LIST_LENGTH(pSortKeys);
  for (size_t i = 0; i < size; ++i) {
    SOrderByExprNode* pOrderExprNode = (SOrderByExprNode*)nodesListGetNode(pSortKeys, i);
    pSortOperInfo->maxTupleLength += ((SColumnNode*)pOrderExprNode->pExpr)->node.resType.bytes;
  }
  return TSDB_CODE_SUCCESS;
}

static void destroySortOpGroupIdCalc(SSortOpGroupIdCalc* pCalc) {
  if (pCalc) {
    taosArrayDestroy(pCalc->pSortColsArr);
    taosMemoryFree(pCalc->keyBuf);
    taosMemoryFree(pCalc);
  }
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
    pDataBlock->info.scanFlag = p->info.scanFlag;
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
      tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str, 0, 0, 0);

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

    pInfo->prefetchedSortInput = getNextBlockFromDownstream(pOperator, 0);
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
    if (tsortIsClosed(pInfo->pCurrSortHandle)) {
      terrno = TSDB_CODE_TSC_QUERY_CANCELLED;
      T_LONG_JMP(pOperator->pTaskInfo->env, terrno);
    }

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
  pOperator->exprSupp.pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  pInfo->binfo.inputTsOrder = pSortPhyNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pSortPhyNode->node.outputTsOrder;

  int32_t numOfOutputCols = 0;
  int32_t code = extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                                     &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto  _error;
  }

  pInfo->pSortInfo = createSortInfo(pSortPhyNode->pSortKeys);
  setOperatorInfo(pOperator, "GroupSortOperator", QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doGroupSort, NULL, destroyGroupSortOperatorInfo,
                                         optrDefaultBufFn, getGroupSortExplainExecInfo, optrDefaultGetNextExtFn, NULL);

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



