/*
 * Copyright (c) 2019 TAOS Data, Inc.
 */

#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

#define THETA_JOIN_BLOCK_ROWS 1

typedef struct SThetaJoinInfo {
  EJoinType      joinType;
  EJoinSubType   subType;
  SNodeList*     pTargets;
  SFilterInfo*   pOnFilter;
  SFilterInfo*   pFinFilter;
  SArray*        pBlocks[2];
  SSDataBlock*   pCandidates;
  int32_t        leftBlock;
  int32_t        rightBlock;
  int32_t        leftRow;
  int32_t        rightRow;
  int64_t        inputBlockId[2];
  bool*          matched[2];
  int32_t        totalRows[2];
  int32_t        lastRow[2];
  int32_t        emitSide;
  int32_t        emitRow;
  bool           loaded;
  bool           done;
} SThetaJoinInfo;

static int32_t thetaFlatRow(SThetaJoinInfo* pInfo, int32_t side, int32_t blockIdx, int32_t rowIdx) {
  int32_t flatRow = rowIdx;
  for (int32_t i = 0; i < blockIdx; ++i) {
    SSDataBlock* pBlock = *(SSDataBlock**)taosArrayGet(pInfo->pBlocks[side], i);
    flatRow += pBlock->info.rows;
  }
  return flatRow;
}

static void thetaGetRow(SThetaJoinInfo* pInfo, int32_t side, int32_t flatRow, SSDataBlock** ppBlock,
                        int32_t* pRow) {
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pBlocks[side]); ++i) {
    SSDataBlock* pBlock = *(SSDataBlock**)taosArrayGet(pInfo->pBlocks[side], i);
    if (flatRow < pBlock->info.rows) {
      *ppBlock = pBlock;
      *pRow = flatRow;
      return;
    }
    flatRow -= pBlock->info.rows;
  }
  *ppBlock = NULL;
}

// Match the executor's downstream validation and explain accounting contract.
static int32_t thetaGetNextDownstreamBlock(SOperatorInfo* pOperator, int32_t side, SSDataBlock** ppBlock) {
  *ppBlock = NULL;
  if (isTaskKilled(pOperator->pTaskInfo)) {
    return pOperator->pTaskInfo->code;
  }
  recordOpExecBeforeDownstream(pOperator);
  int32_t code = getNextBlockFromDownstreamImpl(pOperator, side, true, ppBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = blockDataCheck(*ppBlock);
  }
  recordOpExecAfterDownstream(pOperator, *ppBlock != NULL && code == TSDB_CODE_SUCCESS ? (*ppBlock)->info.rows : 0);
  return code;
}

static int32_t thetaLoadInput(SOperatorInfo* pOperator, SThetaJoinInfo* pInfo, int32_t side) {
  pInfo->pBlocks[side] = taosArrayInit(4, sizeof(SSDataBlock*));
  if (NULL == pInfo->pBlocks[side]) return terrno;
  while (true) {
    SSDataBlock* pBlock = NULL;
    int32_t code = thetaGetNextDownstreamBlock(pOperator, side, &pBlock);
    if (code) return code;
    if (NULL == pBlock) break;
    if (0 == pBlock->info.rows) continue;
    SSDataBlock* pCopy = NULL;
    code = createOneDataBlock(pBlock, true, &pCopy);
    if (code) return code;
    if (NULL == taosArrayPush(pInfo->pBlocks[side], &pCopy)) {
      blockDataDestroy(pCopy);
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t thetaAppendPair(SThetaJoinInfo* pInfo, SSDataBlock* pLeft, int32_t leftRow,
                               SSDataBlock* pRight, int32_t rightRow) {
  int32_t dstRow = pInfo->pCandidates->info.rows;
  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pTargets) {
    STargetNode* pTarget = (STargetNode*)pNode;
    if (QUERY_NODE_COLUMN != nodeType(pTarget->pExpr)) continue;
    SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
    bool leftSide = pCol->dataBlockId == pInfo->inputBlockId[0];
    SSDataBlock* pSrc = leftSide ? pLeft : pRight;
    int32_t srcRow = pSrc == pLeft ? leftRow : rightRow;
    SColumnInfoData* pSrcCol = taosArrayGet(pSrc->pDataBlock, pCol->slotId);
    SColumnInfoData* pDstCol = taosArrayGet(pInfo->pCandidates->pDataBlock, pTarget->slotId);
    if (NULL == pSrcCol || NULL == pDstCol) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    bool isNull = colDataIsNull_s(pSrcCol, srcRow);
    int32_t code = colDataSetVal(pDstCol, dstRow, isNull ? NULL : colDataGetData(pSrcCol, srcRow), isNull);
    if (code) return code;
  }
  pInfo->pCandidates->info.rows++;
  return TSDB_CODE_SUCCESS;
}

static int32_t thetaAppendUnmatched(SThetaJoinInfo* pInfo, int32_t side, int32_t flatRow) {
  blockDataCleanup(pInfo->pCandidates);
  SSDataBlock* pSrcBlock = NULL;
  int32_t srcRow = 0;
  thetaGetRow(pInfo, side, flatRow, &pSrcBlock, &srcRow);
  if (NULL == pSrcBlock) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pTargets) {
    STargetNode* pTarget = (STargetNode*)pNode;
    if (QUERY_NODE_COLUMN != nodeType(pTarget->pExpr)) continue;
    SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
    bool targetOnSide = pCol->dataBlockId == pInfo->inputBlockId[side];
    SColumnInfoData* pDstCol = taosArrayGet(pInfo->pCandidates->pDataBlock, pTarget->slotId);
    if (NULL == pDstCol) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    if (!targetOnSide) {
      int32_t code = colDataSetVal(pDstCol, 0, NULL, true);
      if (code) return code;
      continue;
    }
    SColumnInfoData* pSrcCol = taosArrayGet(pSrcBlock->pDataBlock, pCol->slotId);
    if (NULL == pSrcCol) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    bool isNull = colDataIsNull_s(pSrcCol, srcRow);
    int32_t code = colDataSetVal(pDstCol, 0, isNull ? NULL : colDataGetData(pSrcCol, srcRow), isNull);
    if (code) return code;
  }
  pInfo->pCandidates->info.rows = 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t thetaBuildBatch(SThetaJoinInfo* pInfo) {
  blockDataCleanup(pInfo->pCandidates);
  int32_t leftBlocks = taosArrayGetSize(pInfo->pBlocks[0]);
  int32_t rightBlocks = taosArrayGetSize(pInfo->pBlocks[1]);
  while (pInfo->leftBlock < leftBlocks && pInfo->pCandidates->info.rows < THETA_JOIN_BLOCK_ROWS) {
    SSDataBlock* pLeft = *(SSDataBlock**)taosArrayGet(pInfo->pBlocks[0], pInfo->leftBlock);
    if (pInfo->leftRow >= pLeft->info.rows) {
      pInfo->leftRow = 0;
      pInfo->leftBlock++;
      continue;
    }

    SSDataBlock* pRight = *(SSDataBlock**)taosArrayGet(pInfo->pBlocks[1], pInfo->rightBlock);
    pInfo->lastRow[0] = thetaFlatRow(pInfo, 0, pInfo->leftBlock, pInfo->leftRow);
    pInfo->lastRow[1] = thetaFlatRow(pInfo, 1, pInfo->rightBlock, pInfo->rightRow);
    int32_t code = thetaAppendPair(pInfo, pLeft, pInfo->leftRow, pRight, pInfo->rightRow++);
    if (code) return code;

    if (pInfo->rightRow >= pRight->info.rows) {
      pInfo->rightRow = 0;
      pInfo->rightBlock++;
      if (pInfo->rightBlock >= rightBlocks) {
        pInfo->rightBlock = 0;
        pInfo->leftRow++;
      }
    }
    if (pInfo->leftRow >= pLeft->info.rows) {
      pInfo->leftRow = 0;
      pInfo->leftBlock++;
    }
  }
  pInfo->done = pInfo->leftBlock >= leftBlocks;
  return TSDB_CODE_SUCCESS;
}

static int32_t thetaJoinNext(SOperatorInfo* pOperator, SSDataBlock** ppResult) {
  SThetaJoinInfo* pInfo = pOperator->info;
  *ppResult = NULL;
  if (isTaskKilled(pOperator->pTaskInfo)) return pOperator->pTaskInfo->code;
  if (!pInfo->loaded) {
    int32_t code = thetaLoadInput(pOperator, pInfo, 0);
    if (code) return code;
    code = thetaLoadInput(pOperator, pInfo, 1);
    if (code) return code;
    for (int32_t side = 0; side < 2; ++side) {
      for (int32_t i = 0; i < taosArrayGetSize(pInfo->pBlocks[side]); ++i) {
        int32_t rows = (*(SSDataBlock**)taosArrayGet(pInfo->pBlocks[side], i))->info.rows;
        if (rows > INT32_MAX - pInfo->totalRows[side]) return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        pInfo->totalRows[side] += rows;
      }
      pInfo->matched[side] = taosMemoryCalloc(pInfo->totalRows[side], sizeof(bool));
      if (pInfo->totalRows[side] > 0 && NULL == pInfo->matched[side]) return terrno;
    }
    pInfo->loaded = true;
    if (0 == pInfo->totalRows[0] || 0 == pInfo->totalRows[1]) pInfo->done = true;
  }
  while (!pInfo->done) {
    if (isTaskKilled(pOperator->pTaskInfo)) return pOperator->pTaskInfo->code;
    int32_t code = thetaBuildBatch(pInfo);
    if (code) return code;
    if (pInfo->pCandidates->info.rows > 0 && pInfo->pOnFilter) {
      code = doFilter(pInfo->pCandidates, pInfo->pOnFilter, NULL, NULL);
      if (code) return code;
    }
    if (pInfo->pCandidates->info.rows > 0) {
      for (int32_t side = 0; side < 2; ++side) {
        if (pInfo->lastRow[side] < 0 || pInfo->lastRow[side] >= pInfo->totalRows[side]) {
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
      }
      bool leftAlreadyMatched = pInfo->matched[0][pInfo->lastRow[0]];
      bool rightAlreadyMatched = pInfo->matched[1][pInfo->lastRow[1]];

      // A row is matched as soon as it satisfies the JOIN ... ON predicate.
      // The final filter is a post-join filter and must not turn an actual
      // match into an unmatched outer-join row.
      pInfo->matched[0][pInfo->lastRow[0]] = true;
      pInfo->matched[1][pInfo->lastRow[1]] = true;
      if (pInfo->pFinFilter) {
        code = doFilter(pInfo->pCandidates, pInfo->pFinFilter, NULL, NULL);
        if (code) return code;
      }
      if (pInfo->pCandidates->info.rows > 0) {
        if ((JOIN_STYPE_SEMI == pInfo->subType &&
             ((JOIN_TYPE_LEFT == pInfo->joinType && leftAlreadyMatched) ||
              (JOIN_TYPE_RIGHT == pInfo->joinType && rightAlreadyMatched))) ||
            JOIN_STYPE_ANTI == pInfo->subType) {
          blockDataCleanup(pInfo->pCandidates);
          continue;
        }
        *ppResult = pInfo->pCandidates;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  int32_t side = -1;
  if (JOIN_STYPE_ANTI == pInfo->subType) {
    side = JOIN_TYPE_RIGHT == pInfo->joinType ? 1 : 0;
  } else if (JOIN_STYPE_SEMI == pInfo->subType) {
    side = -1;
  } else if (JOIN_TYPE_LEFT == pInfo->joinType || JOIN_TYPE_FULL == pInfo->joinType) {
    side = pInfo->emitSide;
  } else if (JOIN_TYPE_RIGHT == pInfo->joinType) {
    side = 1;
  }
  while (side >= 0) {
    while (pInfo->emitRow < pInfo->totalRows[side]) {
      if (isTaskKilled(pOperator->pTaskInfo)) return pOperator->pTaskInfo->code;
      int32_t row = pInfo->emitRow++;
      bool emit = JOIN_STYPE_SEMI == pInfo->subType ? pInfo->matched[side][row] : !pInfo->matched[side][row];
      if (!emit) continue;
      int32_t code = thetaAppendUnmatched(pInfo, side, row);
      if (code) return code;
      if (pInfo->pFinFilter) {
        code = doFilter(pInfo->pCandidates, pInfo->pFinFilter, NULL, NULL);
        if (code) return code;
      }
      if (pInfo->pCandidates->info.rows > 0) {
        *ppResult = pInfo->pCandidates;
        return TSDB_CODE_SUCCESS;
      }
    }
    if (JOIN_TYPE_FULL == pInfo->joinType && 0 == side) {
      pInfo->emitSide = 1;
      pInfo->emitRow = 0;
      side = 1;
    } else {
      side = -1;
    }
  }
  setOperatorCompleted(pOperator);
  return TSDB_CODE_SUCCESS;
}

static void thetaClearInputs(SThetaJoinInfo* pInfo) {
  for (int32_t side = 0; side < 2; ++side) {
    if (pInfo->pBlocks[side]) {
      for (int32_t i = 0; i < taosArrayGetSize(pInfo->pBlocks[side]); ++i) {
        blockDataDestroy(*(SSDataBlock**)taosArrayGet(pInfo->pBlocks[side], i));
      }
      taosArrayDestroy(pInfo->pBlocks[side]);
      pInfo->pBlocks[side] = NULL;
    }
    taosMemoryFreeClear(pInfo->matched[side]);
    pInfo->totalRows[side] = 0;
    pInfo->lastRow[side] = 0;
  }
}

static int32_t resetThetaJoinOperState(SOperatorInfo* pOperator) {
  SThetaJoinInfo* pInfo = pOperator->info;
  thetaClearInputs(pInfo);
  blockDataCleanup(pInfo->pCandidates);
  pInfo->leftBlock = 0;
  pInfo->rightBlock = 0;
  pInfo->leftRow = 0;
  pInfo->rightRow = 0;
  pInfo->emitSide = 0;
  pInfo->emitRow = 0;
  pInfo->loaded = false;
  pInfo->done = false;
  return TSDB_CODE_SUCCESS;
}

static void thetaDestroy(void* param) {
  SThetaJoinInfo* pInfo = param;
  if (NULL == pInfo) return;
  thetaClearInputs(pInfo);
  filterFreeInfo(pInfo->pOnFilter);
  filterFreeInfo(pInfo->pFinFilter);
  blockDataDestroy(pInfo->pCandidates);
  nodesDestroyList(pInfo->pTargets);
  taosMemoryFree(pInfo);
}

int32_t createThetaJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                    SThetaJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo,
                                    SOperatorInfo** pOptrInfo) {
  if (numOfDownstream != 2) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  *pOptrInfo = NULL;
  SThetaJoinInfo* pInfo = taosMemoryCalloc(1, sizeof(SThetaJoinInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (NULL == pInfo || NULL == pOperator) {
    code = terrno;
    goto _error;
  }
  pInfo->joinType = pJoinNode->joinType;
  pInfo->subType = pJoinNode->subType;
  // Theta reuses these fields for direct-child DataBlock IDs, not column slots.
  pInfo->inputBlockId[0] = pJoinNode->leftPrimSlotId;
  pInfo->inputBlockId[1] = pJoinNode->rightPrimSlotId;
  // Projection expressions are evaluated above the JOIN; its targets must
  // identify columns from one of the two input blocks.
  SNode* pTargetNode = NULL;
  FOREACH(pTargetNode, pJoinNode->pTargets) {
    if (QUERY_NODE_TARGET != nodeType(pTargetNode)) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _error;
    }
    SNode* pExpr = ((STargetNode*)pTargetNode)->pExpr;
    if (NULL == pExpr || QUERY_NODE_COLUMN != nodeType(pExpr) ||
        (((SColumnNode*)pExpr)->dataBlockId != pInfo->inputBlockId[0] &&
         ((SColumnNode*)pExpr)->dataBlockId != pInfo->inputBlockId[1])) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _error;
    }
  }
  code = nodesCloneList(pJoinNode->pTargets, &pInfo->pTargets);
  if (code) goto _error;
  pInfo->pCandidates = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  if (NULL == pInfo->pCandidates) { code = terrno; goto _error; }
  code = blockDataEnsureCapacity(pInfo->pCandidates, THETA_JOIN_BLOCK_ROWS);
  if (code) goto _error;
  code = filterInitFromNode(pJoinNode->pFullOnCond, &pInfo->pOnFilter, 0, pTaskInfo->pStreamRuntimeInfo);
  if (code) goto _error;
  filterSetExecContext(pInfo->pOnFilter, pTaskInfo, isTaskKilled);
  code = filterInitFromNode(pJoinNode->node.pConditions, &pInfo->pFinFilter, 0, pTaskInfo->pStreamRuntimeInfo);
  if (code) goto _error;
  filterSetExecContext(pInfo->pFinFilter, pTaskInfo, isTaskKilled);
  setOperatorInfo(pOperator, "ThetaJoinOperator", QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code) goto _error;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, thetaJoinNext, NULL, thetaDestroy, optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetThetaJoinOperState);
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  thetaDestroy(pInfo);
  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  pTaskInfo->code = code;
  return code;
}
