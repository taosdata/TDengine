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
#include "function.h"
#include "operator.h"
#include "os.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"

typedef struct SMJoinRowCtx {
  bool    rowRemains;
  int64_t ts;
  SArray* leftRowLocations;
  SArray* leftCreatedBlocks;
  SArray* rightCreatedBlocks;
  int32_t leftRowIdx;
  int32_t rightRowIdx;

  bool    rightUseBuildTable;
  SArray* rightRowLocations;
} SMJoinRowCtx;

typedef struct SMJoinOperatorInfo {
  SSDataBlock* pRes;
  int32_t      joinType;
  int32_t      inputOrder;
  bool         downstreamInitDone[2];
  bool         downstreamFetchDone[2];
  int16_t      downstreamResBlkId[2];

  SSDataBlock* pLeft;
  int32_t      leftPos;
  SColumnInfo  leftCol;

  SSDataBlock* pRight;
  int32_t      rightPos;
  SColumnInfo  rightCol;
  SNode*       pCondAfterMerge;
  SNode*       pColEqualOnConditions;

  SArray*      leftEqOnCondCols;
  char*        leftEqOnCondKeyBuf;
  int32_t      leftEqOnCondKeyLen;

  SArray*      rightEqOnCondCols;
  char*        rightEqOnCondKeyBuf;
  int32_t      rightEqOnCondKeyLen;

  SSHashObj*   rightBuildTable;
  SMJoinRowCtx  rowCtx;

  int64_t       resRows;
} SMJoinOperatorInfo;

static void         setJoinColumnInfo(SColumnInfo* pColumn, const SColumnNode* pColumnNode);
static SSDataBlock* doMergeJoin(struct SOperatorInfo* pOperator);
static void         destroyMergeJoinOperator(void* param);
static void         extractTimeCondition(SMJoinOperatorInfo* pInfo,        SSortMergeJoinPhysiNode* pJoinNode, const char* idStr);

static void extractTimeCondition(SMJoinOperatorInfo* pInfo,        SSortMergeJoinPhysiNode* pJoinNode, const char* idStr) {
  SNode* pPrimKeyCond = pJoinNode->pPrimKeyCond;
  if (nodeType(pPrimKeyCond) != QUERY_NODE_OPERATOR) {
    qError("not support this in join operator, %s", idStr);
    return;  // do not handle this
  }

  SOperatorNode* pNode = (SOperatorNode*)pPrimKeyCond;
  SColumnNode*   col1 = (SColumnNode*)pNode->pLeft;
  SColumnNode*   col2 = (SColumnNode*)pNode->pRight;
  SColumnNode*   leftTsCol = NULL;
  SColumnNode*   rightTsCol = NULL;
  if (col1->dataBlockId == col2->dataBlockId) {
    leftTsCol = col1;
    rightTsCol = col2;
  } else {
    if (col1->dataBlockId == pInfo->downstreamResBlkId[0]) {
      ASSERT(col2->dataBlockId == pInfo->downstreamResBlkId[1]);
      leftTsCol = col1;
      rightTsCol = col2;
    } else {
      ASSERT(col1->dataBlockId == pInfo->downstreamResBlkId[1]);
      ASSERT(col2->dataBlockId == pInfo->downstreamResBlkId[0]);
      leftTsCol = col2;
      rightTsCol = col1;
    }
  }
  setJoinColumnInfo(&pInfo->leftCol, leftTsCol);
  setJoinColumnInfo(&pInfo->rightCol, rightTsCol);
}

static void extractEqualOnCondColsFromOper(SMJoinOperatorInfo* pInfo, SOperatorNode* pOperNode,
                                       SColumn* pLeft, SColumn* pRight) {
  SColumnNode* pLeftNode = (SColumnNode*)pOperNode->pLeft;
  SColumnNode* pRightNode = (SColumnNode*)pOperNode->pRight;
  if (pLeftNode->dataBlockId == pRightNode->dataBlockId || pLeftNode->dataBlockId == pInfo->downstreamResBlkId[0]) {
    *pLeft = extractColumnFromColumnNode((SColumnNode*)pOperNode->pLeft);
    *pRight = extractColumnFromColumnNode((SColumnNode*)pOperNode->pRight);
  } else {
    *pLeft = extractColumnFromColumnNode((SColumnNode*)pOperNode->pRight);
    *pRight = extractColumnFromColumnNode((SColumnNode*)pOperNode->pLeft);
  }
}

static void extractEqualOnCondCols(SMJoinOperatorInfo* pInfo, SNode* pEqualOnCondNode,
                                    SArray* leftTagEqCols, SArray* rightTagEqCols) {
  SColumn left = {0};
  SColumn right = {0};
  if (nodeType(pEqualOnCondNode) == QUERY_NODE_LOGIC_CONDITION && ((SLogicConditionNode*)pEqualOnCondNode)->condType == LOGIC_COND_TYPE_AND) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SLogicConditionNode*)pEqualOnCondNode)->pParameterList) {
      SOperatorNode* pOperNode = (SOperatorNode*)pNode;
      extractEqualOnCondColsFromOper(pInfo, pOperNode, &left, &right);
      taosArrayPush(leftTagEqCols, &left);
      taosArrayPush(rightTagEqCols, &right);
    }
    return;
  }

  if (nodeType(pEqualOnCondNode) == QUERY_NODE_OPERATOR) {
    SOperatorNode* pOperNode = (SOperatorNode*)pEqualOnCondNode;
    extractEqualOnCondColsFromOper(pInfo, pOperNode, &left, &right);
    taosArrayPush(leftTagEqCols, &left);
    taosArrayPush(rightTagEqCols, &right);
  }
}

static int32_t initTagColskeyBuf(int32_t* keyLen, char** keyBuf, const SArray* pGroupColList) {
  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = (SColumn*)taosArrayGet(pGroupColList, i);
    (*keyLen) += pCol->bytes;  // actual data + null_flag
  }

  int32_t nullFlagSize = sizeof(int8_t) * numOfGroupCols;
  (*keyLen) += nullFlagSize;

  if (*keyLen >= 0) {

    (*keyBuf) = taosMemoryCalloc(1, (*keyLen));
    if ((*keyBuf) == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fillKeyBufFromTagCols(SArray* pCols, SSDataBlock* pBlock, int32_t rowIndex, void* pKey) {
  SColumnDataAgg* pColAgg = NULL;
  size_t numOfGroupCols = taosArrayGetSize(pCols);
  char* isNull = (char*)pKey;
  char* pStart = (char*)pKey + sizeof(int8_t) * numOfGroupCols;

  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = (SColumn*) taosArrayGet(pCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

    // valid range check. todo: return error code.
    if (pCol->slotId > taosArrayGetSize(pBlock->pDataBlock)) {
      continue;
    }

    if (pBlock->pBlockAgg != NULL) {
      pColAgg = pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      isNull[i] = 1;
    } else {
      isNull[i] = 0;
      char* val = colDataGetData(pColInfoData, rowIndex);
      if (pCol->type == TSDB_DATA_TYPE_JSON) {
        int32_t dataLen = getJsonValueLen(val);
        memcpy(pStart, val, dataLen);
        pStart += dataLen;
      } else if (IS_VAR_DATA_TYPE(pCol->type)) {
        varDataCopy(pStart, val);
        pStart += varDataTLen(val);
      } else {
        memcpy(pStart, val, pCol->bytes);
        pStart += pCol->bytes;
      }
    }
  }
  return (int32_t)(pStart - (char*)pKey);
}

SOperatorInfo** buildMergeJoinDownstreams(SMJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream) {
  SOperatorInfo** p = taosMemoryMalloc(2 * POINTER_BYTES);
  if (p) {
    p[0] = pDownstream[0];
    p[1] = pDownstream[0];
    pInfo->downstreamResBlkId[0] = getOperatorResultBlockId(p[0], 0);
    pInfo->downstreamResBlkId[1] = getOperatorResultBlockId(p[1], 1);
  }

  return p;
}


SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SMJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  bool newDownstreams = false;
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  if (1 == numOfDownstream) {
    newDownstreams = true;
    pDownstream = buildMergeJoinDownstreams(pInfo, pDownstream);
    if (NULL == pDownstream) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    numOfDownstream = 2;
  } else {
    pInfo->downstreamResBlkId[0] = getOperatorResultBlockId(pDownstream[0], 0);
    pInfo->downstreamResBlkId[1] = getOperatorResultBlockId(pDownstream[1], 0);
  }

  int32_t      numOfCols = 0;
  pInfo->pRes = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);

  SExprInfo*   pExprInfo = createExprInfo(pJoinNode->pTargets, NULL, &numOfCols);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;

  extractTimeCondition(pInfo, pJoinNode, GET_TASKID(pTaskInfo));

  if (pJoinNode->pOtherOnCond != NULL && pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterMerge = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (pInfo->pCondAfterMerge == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pInfo->pCondAfterMerge);
    pLogicCond->pParameterList = nodesMakeList();
    if (pLogicCond->pParameterList == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->pOtherOnCond));
    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->node.pConditions));
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
  } else if (pJoinNode->pOtherOnCond != NULL) {
    pInfo->pCondAfterMerge = nodesCloneNode(pJoinNode->pOtherOnCond);
  } else if (pJoinNode->pColEqCond != NULL) {
    pInfo->pCondAfterMerge = nodesCloneNode(pJoinNode->pColEqCond);
  } else if (pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterMerge = nodesCloneNode(pJoinNode->node.pConditions);
  } else {
    pInfo->pCondAfterMerge = NULL;
  }

  code = filterInitFromNode(pInfo->pCondAfterMerge, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->inputOrder = TSDB_ORDER_ASC;
  if (pJoinNode->node.inputTsOrder == ORDER_ASC) {
    pInfo->inputOrder = TSDB_ORDER_ASC;
  } else if (pJoinNode->node.inputTsOrder == ORDER_DESC) {
    pInfo->inputOrder = TSDB_ORDER_DESC;
  }

  pInfo->pColEqualOnConditions = pJoinNode->pColEqCond;
  if (pInfo->pColEqualOnConditions != NULL) {
    pInfo->leftEqOnCondCols = taosArrayInit(4, sizeof(SColumn));
    pInfo->rightEqOnCondCols = taosArrayInit(4, sizeof(SColumn));
    extractEqualOnCondCols(pInfo, pInfo->pColEqualOnConditions, pInfo->leftEqOnCondCols, pInfo->rightEqOnCondCols);
    initTagColskeyBuf(&pInfo->leftEqOnCondKeyLen, &pInfo->leftEqOnCondKeyBuf, pInfo->leftEqOnCondCols);
    initTagColskeyBuf(&pInfo->rightEqOnCondKeyLen, &pInfo->rightEqOnCondKeyBuf, pInfo->rightEqOnCondCols);
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->rightBuildTable = tSimpleHashInit(256,  hashFn);
  }
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doMergeJoin, NULL, destroyMergeJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  if (newDownstreams) {
    taosMemoryFree(pDownstream);
  }

  pOperator->numOfRealDownstream = newDownstreams ? 1 : 2;

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyMergeJoinOperator(pInfo);
  }
  if (newDownstreams) {
    taosMemoryFree(pDownstream);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void setJoinColumnInfo(SColumnInfo* pColumn, const SColumnNode* pColumnNode) {
  pColumn->slotId = pColumnNode->slotId;
  pColumn->type = pColumnNode->node.resType.type;
  pColumn->bytes = pColumnNode->node.resType.bytes;
  pColumn->precision = pColumnNode->node.resType.precision;
  pColumn->scale = pColumnNode->node.resType.scale;
}

static void mergeJoinDestoryBuildTable(SSHashObj* pBuildTable) {
  void* p = NULL;
  int32_t iter = 0;

  while ((p = tSimpleHashIterate(pBuildTable, p, &iter)) != NULL) {
    SArray* rows = (*(SArray**)p);
    taosArrayDestroy(rows);
  }

  tSimpleHashCleanup(pBuildTable);
}

void destroyMergeJoinOperator(void* param) {
  SMJoinOperatorInfo* pJoinOperator = (SMJoinOperatorInfo*)param;
  if (pJoinOperator->pColEqualOnConditions != NULL) {
    mergeJoinDestoryBuildTable(pJoinOperator->rightBuildTable);
    taosMemoryFreeClear(pJoinOperator->rightEqOnCondKeyBuf);
    taosArrayDestroy(pJoinOperator->rightEqOnCondCols);

    taosMemoryFreeClear(pJoinOperator->leftEqOnCondKeyBuf);
    taosArrayDestroy(pJoinOperator->leftEqOnCondCols);
  }
  nodesDestroyNode(pJoinOperator->pCondAfterMerge);

  taosArrayDestroy(pJoinOperator->rowCtx.leftCreatedBlocks);
  taosArrayDestroy(pJoinOperator->rowCtx.rightCreatedBlocks);
  taosArrayDestroy(pJoinOperator->rowCtx.leftRowLocations);
  taosArrayDestroy(pJoinOperator->rowCtx.rightRowLocations);

  pJoinOperator->pRes = blockDataDestroy(pJoinOperator->pRes);
  taosMemoryFreeClear(param);
}

static void mergeJoinJoinLeftRight(struct SOperatorInfo* pOperator, SSDataBlock* pRes, int32_t currRow,
                                   SSDataBlock* pLeftBlock, int32_t leftPos, SSDataBlock* pRightBlock,
                                   int32_t rightPos) {
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, i);

    SExprInfo* pExprInfo = &pOperator->exprSupp.pExprInfo[i];

    int32_t blockId = pExprInfo->base.pParam[0].pCol->dataBlockId;
    int32_t slotId = pExprInfo->base.pParam[0].pCol->slotId;
    int32_t rowIndex = -1;

    SColumnInfoData* pSrc = NULL;
    if (pLeftBlock->info.id.blockId == blockId) {
      pSrc = taosArrayGet(pLeftBlock->pDataBlock, slotId);
      rowIndex = leftPos;
    } else {
      pSrc = taosArrayGet(pRightBlock->pDataBlock, slotId);
      rowIndex = rightPos;
    }

    if (colDataIsNull_s(pSrc, rowIndex)) {
      colDataSetNULL(pDst, currRow);
    } else {
      char* p = colDataGetData(pSrc, rowIndex);
      colDataSetVal(pDst, currRow, p, false);
    }
  }
}
typedef struct SRowLocation {
  SSDataBlock* pDataBlock;
  int32_t      pos;
} SRowLocation;

// pBlock[tsSlotId][startPos, endPos) == timestamp,
static int32_t mergeJoinGetBlockRowsEqualTs(SSDataBlock* pBlock, int16_t tsSlotId, int32_t startPos, int64_t timestamp,
                                            int32_t* pEndPos, SArray* rowLocations, SArray* createdBlocks) {
  int32_t numRows = pBlock->info.rows;
  ASSERT(startPos < numRows);
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, tsSlotId);

  int32_t i = startPos;
  for (; i < numRows; ++i) {
    char* pNextVal = colDataGetData(pCol, i);
    if (timestamp != *(int64_t*)pNextVal) {
      break;
    }
  }
  int32_t endPos = i;
  *pEndPos = endPos;

  if (endPos - startPos == 0) {
    return 0;
  }

  SSDataBlock* block = pBlock;
  bool         createdNewBlock = false;
  if (endPos == numRows) {
    block = blockDataExtractBlock(pBlock, startPos, endPos - startPos);
    taosArrayPush(createdBlocks, &block);
    createdNewBlock = true;
  }
  SRowLocation location = {0};
  for (int32_t j = startPos; j < endPos; ++j) {
    location.pDataBlock = block;
    location.pos = (createdNewBlock ? j - startPos : j);
    taosArrayPush(rowLocations, &location);
  }
  return 0;
}

// whichChild == 0, left child of join; whichChild ==1, right child of join
static int32_t mergeJoinGetDownStreamRowsEqualTimeStamp(SOperatorInfo* pOperator, int32_t whichChild, int16_t tsSlotId,
                                                        SSDataBlock* startDataBlock, int32_t startPos,
                                                        int64_t timestamp, SArray* rowLocations,
                                                        SArray* createdBlocks) {
  ASSERT(whichChild == 0 || whichChild == 1);

  SMJoinOperatorInfo* pJoinInfo = pOperator->info;
  int32_t            endPos = -1;
  SSDataBlock*       dataBlock = startDataBlock;
  mergeJoinGetBlockRowsEqualTs(dataBlock, tsSlotId, startPos, timestamp, &endPos, rowLocations, createdBlocks);
  while (endPos == dataBlock->info.rows) {
    SOperatorInfo* ds = pOperator->pDownstream[whichChild];
    dataBlock = getNextBlockFromDownstreamRemain(pOperator, whichChild);
    qError("merge join %s got block for same ts, rows:%" PRId64, whichChild == 0 ? "left" : "right", dataBlock ? dataBlock->info.rows : 0);
    if (whichChild == 0) {
      pJoinInfo->leftPos = 0;
      pJoinInfo->pLeft = dataBlock;
    } else if (whichChild == 1) {
      pJoinInfo->rightPos = 0;
      pJoinInfo->pRight = dataBlock;
    }

    if (dataBlock == NULL) {
      pJoinInfo->downstreamFetchDone[whichChild] = true;
      endPos = -1;
      break;
    }

    mergeJoinGetBlockRowsEqualTs(dataBlock, tsSlotId, 0, timestamp, &endPos, rowLocations, createdBlocks);
  }
  if (endPos != -1) {
    if (whichChild == 0) {
      pJoinInfo->leftPos = endPos;
    } else if (whichChild == 1) {
      pJoinInfo->rightPos = endPos;
    }
  }
  return 0;
}

static int32_t mergeJoinFillBuildTable(SMJoinOperatorInfo* pInfo, SArray* rightRowLocations) {
  for (int32_t i = 0; i < taosArrayGetSize(rightRowLocations); ++i) {
    SRowLocation* rightRow = taosArrayGet(rightRowLocations, i);
    int32_t keyLen = fillKeyBufFromTagCols(pInfo->rightEqOnCondCols, rightRow->pDataBlock, rightRow->pos, pInfo->rightEqOnCondKeyBuf);
    SArray** ppRows = tSimpleHashGet(pInfo->rightBuildTable, pInfo->rightEqOnCondKeyBuf, keyLen);
    if (!ppRows) {
      SArray* rows = taosArrayInit(4, sizeof(SRowLocation));
      taosArrayPush(rows, rightRow);
      tSimpleHashPut(pInfo->rightBuildTable, pInfo->rightEqOnCondKeyBuf, keyLen, &rows, POINTER_BYTES);
    } else {
      taosArrayPush(*ppRows, rightRow);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mergeJoinLeftRowsRightRows(SOperatorInfo* pOperator, SSDataBlock* pRes, int32_t* nRows,
                                         const SArray* leftRowLocations, int32_t leftRowIdx,
                                       int32_t rightRowIdx, bool useBuildTableTSRange, SArray* rightRowLocations, bool* pReachThreshold) {
  *pReachThreshold = false;
  uint32_t limitRowNum = pOperator->resultInfo.threshold;
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;
  size_t leftNumJoin = taosArrayGetSize(leftRowLocations);

  int32_t i,j;

  for (i = leftRowIdx; i < leftNumJoin; ++i, rightRowIdx = 0) {
    SRowLocation* leftRow = taosArrayGet(leftRowLocations, i);
    SArray* pRightRows = NULL;
    if (useBuildTableTSRange) {
      int32_t  keyLen = fillKeyBufFromTagCols(pJoinInfo->leftEqOnCondCols, leftRow->pDataBlock, leftRow->pos, pJoinInfo->leftEqOnCondKeyBuf);
      SArray** ppRightRows = tSimpleHashGet(pJoinInfo->rightBuildTable, pJoinInfo->leftEqOnCondKeyBuf, keyLen);
      if (!ppRightRows) {
        continue;
      }
      pRightRows = *ppRightRows;
    } else {
      pRightRows = rightRowLocations;
    }
    size_t rightRowsSize = taosArrayGetSize(pRightRows);
    for (j = rightRowIdx; j < rightRowsSize; ++j) {
      if (*nRows >= limitRowNum) {
        *pReachThreshold = true;
        break;
      }

      SRowLocation* rightRow = taosArrayGet(pRightRows, j);
      mergeJoinJoinLeftRight(pOperator, pRes, *nRows, leftRow->pDataBlock, leftRow->pos, rightRow->pDataBlock,
                             rightRow->pos);
      ++*nRows;
    }
    if (*pReachThreshold) {
      break;
    }
  }

  if (*pReachThreshold) {
    pJoinInfo->rowCtx.rowRemains = true;
    pJoinInfo->rowCtx.leftRowIdx = i;
    pJoinInfo->rowCtx.rightRowIdx = j;
  }
  return TSDB_CODE_SUCCESS;
}

static void mergeJoinDestroyTSRangeCtx(SMJoinOperatorInfo* pJoinInfo, SArray* leftRowLocations, SArray* leftCreatedBlocks,
                                       SArray* rightCreatedBlocks, bool rightUseBuildTable, SArray* rightRowLocations) {
  for (int i = 0; i < taosArrayGetSize(rightCreatedBlocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(rightCreatedBlocks, i);
    blockDataDestroy(pBlock);
  }
  taosArrayDestroy(rightCreatedBlocks);
  for (int i = 0; i < taosArrayGetSize(leftCreatedBlocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(leftCreatedBlocks, i);
    blockDataDestroy(pBlock);
  }
  if (rightRowLocations != NULL) {
    taosArrayDestroy(rightRowLocations);
  }
  if (rightUseBuildTable) {
    void* p = NULL;
    int32_t iter = 0;
    while ((p = tSimpleHashIterate(pJoinInfo->rightBuildTable, p, &iter)) != NULL) {
      SArray* rows = (*(SArray**)p);
      taosArrayDestroy(rows);
    }
    tSimpleHashClear(pJoinInfo->rightBuildTable);
  }

  taosArrayDestroy(leftCreatedBlocks);
  taosArrayDestroy(leftRowLocations);

  pJoinInfo->rowCtx.rowRemains = false;
  pJoinInfo->rowCtx.leftRowLocations = NULL;
  pJoinInfo->rowCtx.leftCreatedBlocks = NULL;
  pJoinInfo->rowCtx.rightCreatedBlocks = NULL;
  pJoinInfo->rowCtx.rightUseBuildTable = false;
  pJoinInfo->rowCtx.rightRowLocations = NULL;
}

static int32_t mergeJoinJoinDownstreamTsRanges(SOperatorInfo* pOperator, int64_t timestamp, SSDataBlock* pRes,
                                               int32_t* nRows) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;
  SArray* leftRowLocations = NULL;
  SArray* rightRowLocations = NULL;
  SArray* leftCreatedBlocks = NULL;
  SArray* rightCreatedBlocks = NULL;
  int32_t leftRowIdx = 0;
  int32_t rightRowIdx = 0;
  SSHashObj* rightTableHash = NULL;
  bool rightUseBuildTable = false;

  if (pJoinInfo->rowCtx.rowRemains) {
    leftRowLocations = pJoinInfo->rowCtx.leftRowLocations;
    leftCreatedBlocks = pJoinInfo->rowCtx.leftCreatedBlocks;
    rightUseBuildTable = pJoinInfo->rowCtx.rightUseBuildTable;
    rightRowLocations = pJoinInfo->rowCtx.rightRowLocations;
    rightCreatedBlocks = pJoinInfo->rowCtx.rightCreatedBlocks;
    leftRowIdx = pJoinInfo->rowCtx.leftRowIdx;
    rightRowIdx = pJoinInfo->rowCtx.rightRowIdx;
  } else {
    leftRowLocations = taosArrayInit(8, sizeof(SRowLocation));
    leftCreatedBlocks = taosArrayInit(8, POINTER_BYTES);

    rightRowLocations = taosArrayInit(8, sizeof(SRowLocation));
    rightCreatedBlocks = taosArrayInit(8, POINTER_BYTES);

    mergeJoinGetDownStreamRowsEqualTimeStamp(pOperator, 0, pJoinInfo->leftCol.slotId, pJoinInfo->pLeft,
                                             pJoinInfo->leftPos, timestamp, leftRowLocations, leftCreatedBlocks);
    mergeJoinGetDownStreamRowsEqualTimeStamp(pOperator, 1, pJoinInfo->rightCol.slotId, pJoinInfo->pRight,
                                             pJoinInfo->rightPos, timestamp, rightRowLocations, rightCreatedBlocks);
    if (pJoinInfo->pColEqualOnConditions != NULL && taosArrayGetSize(rightRowLocations) > 16) {
      mergeJoinFillBuildTable(pJoinInfo, rightRowLocations);
      rightUseBuildTable = true;
      taosArrayDestroy(rightRowLocations);
      rightRowLocations = NULL;
    }
  }
  
  size_t leftNumJoin = taosArrayGetSize(leftRowLocations);
  code = blockDataEnsureCapacity(pRes, pOperator->resultInfo.threshold);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s can not ensure block capacity for join. left: %zu", GET_TASKID(pOperator->pTaskInfo),
           leftNumJoin);
  }

  bool reachThreshold = false;

  if (code == TSDB_CODE_SUCCESS) {
    mergeJoinLeftRowsRightRows(pOperator, pRes, nRows, leftRowLocations, leftRowIdx,
                                                rightRowIdx, rightUseBuildTable, rightRowLocations, &reachThreshold);
  }

  if (!reachThreshold) {
    mergeJoinDestroyTSRangeCtx(pJoinInfo, leftRowLocations, leftCreatedBlocks, rightCreatedBlocks,
                               rightUseBuildTable, rightRowLocations);

  } else {
      pJoinInfo->rowCtx.rowRemains = true;
      pJoinInfo->rowCtx.ts = timestamp;
      pJoinInfo->rowCtx.leftRowLocations = leftRowLocations;
      pJoinInfo->rowCtx.leftCreatedBlocks = leftCreatedBlocks;
      pJoinInfo->rowCtx.rightCreatedBlocks = rightCreatedBlocks;
      pJoinInfo->rowCtx.rightUseBuildTable = rightUseBuildTable;
      pJoinInfo->rowCtx.rightRowLocations = rightRowLocations;
  }
  return TSDB_CODE_SUCCESS;
}

static void setMergeJoinDone(SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);
  if (pOperator->pDownstreamGetParams) {
    freeOperatorParam(pOperator->pDownstreamGetParams[0], OP_GET_PARAM);
    freeOperatorParam(pOperator->pDownstreamGetParams[1], OP_GET_PARAM);
    pOperator->pDownstreamGetParams[0] = NULL;
    pOperator->pDownstreamGetParams[1] = NULL;
  }
}

static bool mergeJoinGetNextTimestamp(SOperatorInfo* pOperator, int64_t* pLeftTs, int64_t* pRightTs) {
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;
  bool leftEmpty = false;
  
  if (pJoinInfo->pLeft == NULL || pJoinInfo->leftPos >= pJoinInfo->pLeft->info.rows) {
    if (!pJoinInfo->downstreamFetchDone[0]) {
      pJoinInfo->pLeft = getNextBlockFromDownstreamRemain(pOperator, 0);
      pJoinInfo->downstreamInitDone[0] = true;

      pJoinInfo->leftPos = 0;
      qError("merge join left got block, rows:%" PRId64, pJoinInfo->pLeft ? pJoinInfo->pLeft->info.rows : 0);
    } else {
      pJoinInfo->pLeft = NULL;
    }
    
    if (pJoinInfo->pLeft == NULL) {
      if (pOperator->pOperatorGetParam && ((SSortMergeJoinOperatorParam*)pOperator->pOperatorGetParam->value)->initDownstream && !pJoinInfo->downstreamInitDone[1]) {
        leftEmpty = true;
      } else {
        setMergeJoinDone(pOperator);
        return false;
      }
    }
  }

  if (pJoinInfo->pRight == NULL || pJoinInfo->rightPos >= pJoinInfo->pRight->info.rows) {
    if (!pJoinInfo->downstreamFetchDone[1]) {
      pJoinInfo->pRight = getNextBlockFromDownstreamRemain(pOperator, 1);
      pJoinInfo->downstreamInitDone[1] = true;

      pJoinInfo->rightPos = 0;
      qError("merge join right got block, rows:%" PRId64, pJoinInfo->pRight ? pJoinInfo->pRight->info.rows : 0);
    } else {
      pJoinInfo->pRight = NULL;
    }
    
    if (pJoinInfo->pRight == NULL) {
      setMergeJoinDone(pOperator);
      return false;
    } else {
      if (leftEmpty) {
        setMergeJoinDone(pOperator);
        return false;
      }
    }
  }

  if (NULL == pJoinInfo->pLeft || NULL == pJoinInfo->pRight) {
    setMergeJoinDone(pOperator);
    return false;
  }
  
  // only the timestamp match support for ordinary table
  SColumnInfoData* pLeftCol = taosArrayGet(pJoinInfo->pLeft->pDataBlock, pJoinInfo->leftCol.slotId);
  char*            pLeftVal = colDataGetData(pLeftCol, pJoinInfo->leftPos);
  *pLeftTs = *(int64_t*)pLeftVal;

  SColumnInfoData* pRightCol = taosArrayGet(pJoinInfo->pRight->pDataBlock, pJoinInfo->rightCol.slotId);
  char*            pRightVal = colDataGetData(pRightCol, pJoinInfo->rightPos);
  *pRightTs = *(int64_t*)pRightVal;

  return true;
}

static void doMergeJoinImpl(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;

  int32_t nrows = pRes->info.rows;

  bool asc = (pJoinInfo->inputOrder == TSDB_ORDER_ASC) ? true : false;

  while (1) {
    int64_t leftTs = 0;
    int64_t rightTs = 0;
    if (pJoinInfo->rowCtx.rowRemains) {
      leftTs = pJoinInfo->rowCtx.ts;
      rightTs = pJoinInfo->rowCtx.ts;
    } else {
      bool    hasNextTs = mergeJoinGetNextTimestamp(pOperator, &leftTs, &rightTs);
      if (!hasNextTs) {
        break;
      }
    }

    if (leftTs == rightTs) {
      mergeJoinJoinDownstreamTsRanges(pOperator, leftTs, pRes, &nrows);
    } else if ((asc && leftTs < rightTs) || (!asc && leftTs > rightTs)) {
      pJoinInfo->leftPos += 1;

      if (pJoinInfo->leftPos >= pJoinInfo->pLeft->info.rows && pRes->info.rows < pOperator->resultInfo.threshold) {
        continue;
      }
    } else if ((asc && leftTs > rightTs) || (!asc && leftTs < rightTs)) {
      pJoinInfo->rightPos += 1;
      if (pJoinInfo->rightPos >= pJoinInfo->pRight->info.rows && pRes->info.rows < pOperator->resultInfo.threshold) {
        continue;
      }
    }

    // the pDataBlock are always the same one, no need to call this again
    pRes->info.rows = nrows;
    pRes->info.dataLoad = 1;
    pRes->info.scanFlag = MAIN_SCAN;
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
}

void resetMergeJoinOperator(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;
  if (pJoinInfo->rowCtx.rowRemains) {
    mergeJoinDestroyTSRangeCtx(pJoinInfo, pJoinInfo->rowCtx.leftRowLocations, pJoinInfo->rowCtx.leftCreatedBlocks, pJoinInfo->rowCtx.rightCreatedBlocks,
                               pJoinInfo->rowCtx.rightUseBuildTable, pJoinInfo->rowCtx.rightRowLocations);
  }
  pJoinInfo->pLeft = NULL;
  pJoinInfo->leftPos = 0;
  pJoinInfo->pRight = NULL;
  pJoinInfo->rightPos = 0;
  pJoinInfo->downstreamFetchDone[0] = false;
  pJoinInfo->downstreamFetchDone[1] = false;
  pJoinInfo->downstreamInitDone[0] = false;
  pJoinInfo->downstreamInitDone[1] = false;
  pJoinInfo->resRows = 0;
  pOperator->status = OP_OPENED;
}

SSDataBlock* doMergeJoin(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoinInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    if (NULL == pOperator->pDownstreamGetParams || NULL == pOperator->pDownstreamGetParams[0] || NULL == pOperator->pDownstreamGetParams[1]) {
      qError("total merge join res rows:%" PRId64, pJoinInfo->resRows);
      return NULL;
    } else {
      resetMergeJoinOperator(pOperator);
      qError("start new merge join");
    }
  }

  int64_t st = 0;
  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SSDataBlock* pRes = pJoinInfo->pRes;
  blockDataCleanup(pRes);

  while (true) {
    int32_t numOfRowsBefore = pRes->info.rows;
    doMergeJoinImpl(pOperator, pRes);
    int32_t numOfNewRows = pRes->info.rows - numOfRowsBefore;
    if (numOfNewRows == 0) {
      break;
    }
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    }
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
    if (pOperator->status == OP_EXEC_DONE) {
      break;
    }
  }

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }
  
  if (pRes->info.rows > 0) {
    pJoinInfo->resRows += pRes->info.rows;
    qError("merge join returns res rows:%" PRId64, pRes->info.rows);
    return pRes;
  } else {
    qError("total merge join res rows:%" PRId64, pJoinInfo->resRows);
    return NULL;
  }
}



