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
#include "function.h"
#include "os.h"
#include "querynodes.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"

static void         setJoinColumnInfo(SColumnInfo* pColumn, const SColumnNode* pColumnNode);
static SSDataBlock* doMergeJoin(struct SOperatorInfo* pOperator);
static void         destroyMergeJoinOperator(void* param, int32_t numOfOutput);
static void         extractTimeCondition(SJoinOperatorInfo* Info, SLogicConditionNode* pLogicConditionNode);

SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SJoinPhysiNode* pJoinNode,
    SExecTaskInfo* pTaskInfo) {
  SJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    goto _error;
  }

  SSDataBlock*    pResBlock = createResDataBlock(pJoinNode->node.pOutputDataBlockDesc);

  int32_t numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pJoinNode->pTargets, NULL, &numOfCols);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pRes             = pResBlock;
  pOperator->name         = "MergeJoinOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN;
  pOperator->blocking     = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->exprSupp.pExprInfo   = pExprInfo;
  pOperator->exprSupp.numOfExprs  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;

  SNode* pMergeCondition = pJoinNode->pMergeCondition;
  if (nodeType(pMergeCondition) == QUERY_NODE_OPERATOR) {
    SOperatorNode* pNode = (SOperatorNode*)pMergeCondition;
    setJoinColumnInfo(&pInfo->leftCol, (SColumnNode*)pNode->pLeft);
    setJoinColumnInfo(&pInfo->rightCol, (SColumnNode*)pNode->pRight);
  } else {
    ASSERT(false);
  }

  if (pJoinNode->pOnConditions != NULL && pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterMerge = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pInfo->pCondAfterMerge);
    pLogicCond->pParameterList = nodesMakeList();
    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->pOnConditions));
    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->node.pConditions));
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
  } else if (pJoinNode->pOnConditions != NULL) {
    pInfo->pCondAfterMerge = nodesCloneNode(pJoinNode->pOnConditions);
  } else if (pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterMerge = nodesCloneNode(pJoinNode->node.pConditions);
  } else {
    pInfo->pCondAfterMerge = NULL;
  }

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doMergeJoin, NULL, NULL, destroyMergeJoinOperator, NULL, NULL, NULL);
  int32_t code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

void setJoinColumnInfo(SColumnInfo* pColumn, const SColumnNode* pColumnNode) {
  pColumn->slotId = pColumnNode->slotId;
  pColumn->type = pColumnNode->node.resType.type;
  pColumn->bytes = pColumnNode->node.resType.bytes;
  pColumn->precision = pColumnNode->node.resType.precision;
  pColumn->scale = pColumnNode->node.resType.scale;
}

void destroyMergeJoinOperator(void* param, int32_t numOfOutput) {
  SJoinOperatorInfo* pJoinOperator = (SJoinOperatorInfo*)param;
  nodesDestroyNode(pJoinOperator->pCondAfterMerge);
  
  taosMemoryFreeClear(param);
}

static void doMergeJoinImpl(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

  int32_t nrows = 0;

  while (1) {
    // todo extract method
    if (pJoinInfo->pLeft == NULL || pJoinInfo->leftPos >= pJoinInfo->pLeft->info.rows) {
      SOperatorInfo* ds1 = pOperator->pDownstream[0];
      pJoinInfo->pLeft = ds1->fpSet.getNextFn(ds1);

      pJoinInfo->leftPos = 0;
      if (pJoinInfo->pLeft == NULL) {
        setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
        break;
      }
    }

    if (pJoinInfo->pRight == NULL || pJoinInfo->rightPos >= pJoinInfo->pRight->info.rows) {
      SOperatorInfo* ds2 = pOperator->pDownstream[1];
      pJoinInfo->pRight = ds2->fpSet.getNextFn(ds2);

      pJoinInfo->rightPos = 0;
      if (pJoinInfo->pRight == NULL) {
        setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
        break;
      }
    }

    SColumnInfoData* pLeftCol = taosArrayGet(pJoinInfo->pLeft->pDataBlock, pJoinInfo->leftCol.slotId);
    char*            pLeftVal = colDataGetData(pLeftCol, pJoinInfo->leftPos);

    SColumnInfoData* pRightCol = taosArrayGet(pJoinInfo->pRight->pDataBlock, pJoinInfo->rightCol.slotId);
    char*            pRightVal = colDataGetData(pRightCol, pJoinInfo->rightPos);

    // only the timestamp match support for ordinary table
    ASSERT(pLeftCol->info.type == TSDB_DATA_TYPE_TIMESTAMP);
    if (*(int64_t*)pLeftVal == *(int64_t*)pRightVal) {
      for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, i);

        SExprInfo* pExprInfo = &pOperator->exprSupp.pExprInfo[i];

        int32_t blockId = pExprInfo->base.pParam[0].pCol->dataBlockId;
        int32_t slotId = pExprInfo->base.pParam[0].pCol->slotId;
        int32_t rowIndex = -1;

        SColumnInfoData* pSrc = NULL;
        if (pJoinInfo->pLeft->info.blockId == blockId) {
          pSrc = taosArrayGet(pJoinInfo->pLeft->pDataBlock, slotId);
          rowIndex = pJoinInfo->leftPos;
        } else {
          pSrc = taosArrayGet(pJoinInfo->pRight->pDataBlock, slotId);
          rowIndex = pJoinInfo->rightPos;
        }

        if (colDataIsNull_s(pSrc, rowIndex)) {
          colDataAppendNULL(pDst, nrows);
        } else {
          char* p = colDataGetData(pSrc, rowIndex);
          colDataAppend(pDst, nrows, p, false);
        }
      }

      pJoinInfo->leftPos += 1;
      pJoinInfo->rightPos += 1;

      nrows += 1;
    } else if (*(int64_t*)pLeftVal < *(int64_t*)pRightVal) {
      pJoinInfo->leftPos += 1;

      if (pJoinInfo->leftPos >= pJoinInfo->pLeft->info.rows) {
        continue;
      }
    } else if (*(int64_t*)pLeftVal > *(int64_t*)pRightVal) {
      pJoinInfo->rightPos += 1;
      if (pJoinInfo->rightPos >= pJoinInfo->pRight->info.rows) {
        continue;
      }
    }

    // the pDataBlock are always the same one, no need to call this again
    pRes->info.rows = nrows;
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
}

SSDataBlock* doMergeJoin(struct SOperatorInfo* pOperator) {
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

  SSDataBlock* pRes = pJoinInfo->pRes;
  blockDataCleanup(pRes);
  blockDataEnsureCapacity(pRes, 4096);
  while (true) {
    int32_t numOfRowsBefore = pRes->info.rows;
    doMergeJoinImpl(pOperator, pRes);
    int32_t numOfNewRows = pRes->info.rows - numOfRowsBefore;
    if (numOfNewRows == 0) {
      break;
    }
    if (pJoinInfo->pCondAfterMerge != NULL) {
      doFilter(pJoinInfo->pCondAfterMerge, pRes, NULL);
    }
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
  return (pRes->info.rows > 0) ? pRes : NULL;
}

static void extractTimeCondition(SJoinOperatorInfo* pInfo, SLogicConditionNode* pLogicConditionNode) {
  int32_t len = LIST_LENGTH(pLogicConditionNode->pParameterList);

  for (int32_t i = 0; i < len; ++i) {
    SNode* pNode = nodesListGetNode(pLogicConditionNode->pParameterList, i);
    if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
      SOperatorNode* pn1 = (SOperatorNode*)pNode;
      setJoinColumnInfo(&pInfo->leftCol, (SColumnNode*)pn1->pLeft);
      setJoinColumnInfo(&pInfo->rightCol, (SColumnNode*)pn1->pRight);
      break;
    }
  }
}
