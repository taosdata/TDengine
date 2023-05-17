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

typedef struct SJoinRowCtx {
  bool    rowRemains;
  int64_t ts;
  SArray* leftRowLocations;
  SArray* leftCreatedBlocks;
  SArray* rightCreatedBlocks;
  int32_t leftRowIdx;
  int32_t rightRowIdx;

  SSHashObj* buildTableTSRange;
  SArray* rightRowLocations;
} SJoinRowCtx;

typedef struct SJoinOperatorInfo {
  SSDataBlock* pRes;
  int32_t      joinType;
  int32_t      inputOrder;

  SSDataBlock* pLeft;
  int32_t      leftPos;
  SColumnInfo  leftCol;

  SSDataBlock* pRight;
  int32_t      rightPos;
  SColumnInfo  rightCol;
  SNode*       pCondAfterMerge;
  SNode*       pTagEqualConditions;

  SArray*      leftTagCols;
  SArray*      leftTagKeys;
  char*        leftTagKeyBuf;
  int32_t      leftTagKeyLen;

  SArray*      rightTagCols;
  SArray*      rightTagKeys;
  char*        rightTagKeyBuf;
  int32_t      rightTagKeyLen;

  SJoinRowCtx  rowCtx;
} SJoinOperatorInfo;

static void         setJoinColumnInfo(SColumnInfo* pColumn, const SColumnNode* pColumnNode);
static SSDataBlock* doMergeJoin(struct SOperatorInfo* pOperator);
static void         destroyMergeJoinOperator(void* param);
static void         extractTimeCondition(SJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream, int32_t num,
                                         SSortMergeJoinPhysiNode* pJoinNode, const char* idStr);

static void extractTimeCondition(SJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream,  int32_t num,
                                 SSortMergeJoinPhysiNode* pJoinNode, const char* idStr) {
  SNode* pMergeCondition = pJoinNode->pMergeCondition;
  if (nodeType(pMergeCondition) != QUERY_NODE_OPERATOR) {
    qError("not support this in join operator, %s", idStr);
    return;  // do not handle this
  }

  SOperatorNode* pNode = (SOperatorNode*)pMergeCondition;
  SColumnNode*   col1 = (SColumnNode*)pNode->pLeft;
  SColumnNode*   col2 = (SColumnNode*)pNode->pRight;
  SColumnNode*   leftTsCol = NULL;
  SColumnNode*   rightTsCol = NULL;
  if (col1->dataBlockId == col2->dataBlockId) {
    leftTsCol = col1;
    rightTsCol = col2;
  } else {
    if (col1->dataBlockId == pDownstream[0]->resultDataBlockId) {
      ASSERT(col2->dataBlockId == pDownstream[1]->resultDataBlockId);
      leftTsCol = col1;
      rightTsCol = col2;
    } else {
      ASSERT(col1->dataBlockId == pDownstream[1]->resultDataBlockId);
      ASSERT(col2->dataBlockId == pDownstream[0]->resultDataBlockId);
      leftTsCol = col2;
      rightTsCol = col1;
    }
  }
  setJoinColumnInfo(&pInfo->leftCol, leftTsCol);
  setJoinColumnInfo(&pInfo->rightCol, rightTsCol);
}

static void extractTagEqualColsFromOper(SJoinOperatorInfo* pInfo, SOperatorInfo** pDownstreams, SOperatorNode* pOperNode,
                                       SColumn* pLeft, SColumn* pRight) {
  SColumnNode* pLeftNode = (SColumnNode*)pOperNode->pLeft;
  SColumnNode* pRightNode = (SColumnNode*)pOperNode->pRight;
  if (pLeftNode->dataBlockId == pRightNode->dataBlockId || pLeftNode->dataBlockId == pDownstreams[0]->resultDataBlockId) {
    *pLeft = extractColumnFromColumnNode((SColumnNode*)pOperNode->pLeft);
    *pRight = extractColumnFromColumnNode((SColumnNode*)pOperNode->pRight);
  } else {
    *pLeft = extractColumnFromColumnNode((SColumnNode*)pOperNode->pRight);
    *pRight = extractColumnFromColumnNode((SColumnNode*)pOperNode->pLeft);
  }
}

static void extractTagEqualCondCols(SJoinOperatorInfo* pInfo, SOperatorInfo** pDownStream, SNode* pTagEqualNode,
                                    SArray* leftTagEqCols, SArray* rightTagEqCols) {
  SColumn left = {0};
  SColumn right = {0};
  if (nodeType(pTagEqualNode) == QUERY_NODE_LOGIC_CONDITION && ((SLogicConditionNode*)pTagEqualNode)->condType == LOGIC_COND_TYPE_AND) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SLogicConditionNode*)pTagEqualNode)->pParameterList) {
      SOperatorNode* pOperNode = (SOperatorNode*)pNode;
      extractTagEqualColsFromOper(pInfo, pDownStream, pOperNode, &left, &right);
      taosArrayPush(leftTagEqCols, &left);
      taosArrayPush(rightTagEqCols, &right);
    }
    return;
  }

  if (nodeType(pTagEqualNode) == QUERY_NODE_OPERATOR) {
    SOperatorNode* pOperNode = (SOperatorNode*)pTagEqualNode;
    extractTagEqualColsFromOper(pInfo, pDownStream, pOperNode, &left, &right);
    taosArrayPush(leftTagEqCols, &left);
    taosArrayPush(rightTagEqCols, &right);
  }
}

static int32_t initTagColsGroupkeys(SArray** pGroupColVals, int32_t* keyLen, char** keyBuf, const SArray* pGroupColList) {
  *pGroupColVals = taosArrayInit(4, sizeof(SGroupKeys));
  if ((*pGroupColVals) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = (SColumn*)taosArrayGet(pGroupColList, i);
    (*keyLen) += pCol->bytes;  // actual data + null_flag

    SGroupKeys key = {0};
    key.bytes = pCol->bytes;
    key.type = pCol->type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pCol->bytes);
    if (key.pData == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayPush((*pGroupColVals), &key);
  }

  int32_t nullFlagSize = sizeof(int8_t) * numOfGroupCols;
  (*keyLen) += nullFlagSize;

  (*keyBuf) = taosMemoryCalloc(1, (*keyLen));
  if ((*keyBuf) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static void fillGroupKeyValsFromTagCols(SArray* pCols, SArray* pGroupKeys, SSDataBlock* pBlock, int32_t rowIndex) {
  SColumnDataAgg* pColAgg = NULL;

  size_t numOfGroupCols = taosArrayGetSize(pCols);

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

    SGroupKeys* pkey = taosArrayGet(pGroupKeys, i);
    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      pkey->isNull = true;
    } else {
      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      if (pkey->type == TSDB_DATA_TYPE_JSON) {
        if (tTagIsJson(val)) {
          terrno = TSDB_CODE_QRY_JSON_IN_GROUP_ERROR;
          return;
        }
        int32_t dataLen = getJsonValueLen(val);
        memcpy(pkey->pData, val, dataLen);
      } else if (IS_VAR_DATA_TYPE(pkey->type)) {
        memcpy(pkey->pData, val, varDataTLen(val));
        ASSERT(varDataTLen(val) <= pkey->bytes);
      } else {
        memcpy(pkey->pData, val, pkey->bytes);
      }
    }
  }
}

static int32_t combineGroupKeysIntoBuf(void* pKey, const SArray* pGroupKeys) {
  size_t numOfGroupCols = taosArrayGetSize(pGroupKeys);

  char* isNull = (char*)pKey;
  char* pStart = (char*)pKey + sizeof(int8_t) * numOfGroupCols;
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SGroupKeys* pkey = taosArrayGet(pGroupKeys, i);
    if (pkey->isNull) {
      isNull[i] = 1;
      continue;
    }

    isNull[i] = 0;
    if (pkey->type == TSDB_DATA_TYPE_JSON) {
      int32_t dataLen = getJsonValueLen(pkey->pData);
      memcpy(pStart, (pkey->pData), dataLen);
      pStart += dataLen;
    } else if (IS_VAR_DATA_TYPE(pkey->type)) {
      varDataCopy(pStart, pkey->pData);
      pStart += varDataTLen(pkey->pData);
      ASSERT(varDataTLen(pkey->pData) <= pkey->bytes);
    } else {
      memcpy(pStart, pkey->pData, pkey->bytes);
      pStart += pkey->bytes;
    }
  }

  return (int32_t)(pStart - (char*)pKey);
}

SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  int32_t      numOfCols = 0;
  pInfo->pRes = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);

  SExprInfo*   pExprInfo = createExprInfo(pJoinNode->pTargets, NULL, &numOfCols);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;

  extractTimeCondition(pInfo, pDownstream, numOfDownstream, pJoinNode, GET_TASKID(pTaskInfo));

  if (pJoinNode->pOnConditions != NULL && pJoinNode->node.pConditions != NULL) {
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

  code = filterInitFromNode(pInfo->pCondAfterMerge, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->inputOrder = TSDB_ORDER_ASC;
  if (pJoinNode->inputTsOrder == ORDER_ASC) {
    pInfo->inputOrder = TSDB_ORDER_ASC;
  } else if (pJoinNode->inputTsOrder == ORDER_DESC) {
    pInfo->inputOrder = TSDB_ORDER_DESC;
  }

  pInfo->pTagEqualConditions = pJoinNode->pTagEqualCondtions;
  if (pInfo->pTagEqualConditions != NULL) {
    pInfo->leftTagCols = taosArrayInit(4, sizeof(SColumn));
    pInfo->rightTagCols = taosArrayInit(4, sizeof(SColumn));
    extractTagEqualCondCols(pInfo, pDownstream, pInfo->pTagEqualConditions, pInfo->leftTagCols, pInfo->rightTagCols);
    initTagColsGroupkeys(&pInfo->leftTagKeys, &pInfo->leftTagKeyLen, &pInfo->leftTagKeyBuf, pInfo->leftTagCols);
    initTagColsGroupkeys(&pInfo->rightTagKeys, &pInfo->rightTagKeyLen, &pInfo->rightTagKeyBuf, pInfo->rightTagCols);
  }
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doMergeJoin, NULL, destroyMergeJoinOperator, optrDefaultBufFn, NULL);
  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyMergeJoinOperator(pInfo);
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

static void freeGroupKeyData(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

void destroyMergeJoinOperator(void* param) {
  SJoinOperatorInfo* pJoinOperator = (SJoinOperatorInfo*)param;
  if (pJoinOperator->pTagEqualConditions != NULL) {
    taosMemoryFreeClear(pJoinOperator->rightTagKeyBuf);
    taosArrayDestroyEx(pJoinOperator->rightTagKeys, freeGroupKeyData);
    taosArrayDestroy(pJoinOperator->rightTagCols);

    taosMemoryFreeClear(pJoinOperator->leftTagKeyBuf);
    taosArrayDestroyEx(pJoinOperator->leftTagKeys, freeGroupKeyData);
    taosArrayDestroy(pJoinOperator->leftTagCols);
  }
  nodesDestroyNode(pJoinOperator->pCondAfterMerge);

  pJoinOperator->pRes = blockDataDestroy(pJoinOperator->pRes);
  taosMemoryFreeClear(param);
}

static void mergeJoinJoinLeftRight(struct SOperatorInfo* pOperator, SSDataBlock* pRes, int32_t currRow,
                                   SSDataBlock* pLeftBlock, int32_t leftPos, SSDataBlock* pRightBlock,
                                   int32_t rightPos) {
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

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

  SJoinOperatorInfo* pJoinInfo = pOperator->info;
  int32_t            endPos = -1;
  SSDataBlock*       dataBlock = startDataBlock;
  mergeJoinGetBlockRowsEqualTs(dataBlock, tsSlotId, startPos, timestamp, &endPos, rowLocations, createdBlocks);
  while (endPos == dataBlock->info.rows) {
    SOperatorInfo* ds = pOperator->pDownstream[whichChild];
    dataBlock = ds->fpSet.getNextFn(ds);
    if (whichChild == 0) {
      pJoinInfo->leftPos = 0;
      pJoinInfo->pLeft = dataBlock;
    } else if (whichChild == 1) {
      pJoinInfo->rightPos = 0;
      pJoinInfo->pRight = dataBlock;
    }

    if (dataBlock == NULL) {
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
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

static int32_t mergeJoinCreateBuildTable(SJoinOperatorInfo* pInfo, SArray* rightRowLocations,SSHashObj** ppHashObj) {
  int32_t buildTableCap = TMIN(taosArrayGetSize(rightRowLocations), 256);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SSHashObj* buildTable = tSimpleHashInit(buildTableCap,  hashFn);
  for (int32_t i = 0; i < taosArrayGetSize(rightRowLocations); ++i) {
    SRowLocation* rightRow = taosArrayGet(rightRowLocations, i);
    fillGroupKeyValsFromTagCols(pInfo->rightTagCols, pInfo->rightTagKeys, rightRow->pDataBlock, rightRow->pos);
    int32_t keyLen = combineGroupKeysIntoBuf(pInfo->rightTagKeyBuf, pInfo->rightTagKeys);
    SArray** ppRows = tSimpleHashGet(buildTable, pInfo->rightTagKeyBuf, keyLen);
    if (!ppRows) {
      SArray* rows = taosArrayInit(4, sizeof(SRowLocation));
      taosArrayPush(rows, rightRow);
      tSimpleHashPut(buildTable, pInfo->rightTagKeyBuf, keyLen, &rows, POINTER_BYTES);
    } else {
      taosArrayPush(*ppRows, rightRow);
    }
  }
  *ppHashObj = buildTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t mergeJoinLeftRowsRightRows(SOperatorInfo* pOperator, SSDataBlock* pRes, int32_t* nRows,
                                         const SArray* leftRowLocations, int32_t leftRowIdx,
                                       int32_t rightRowIdx, SSHashObj* rightTableHash, SArray* rightRowLocations, bool* pReachThreshold) {
  *pReachThreshold = false;
  uint32_t limitRowNum = pOperator->resultInfo.threshold;
  SJoinOperatorInfo* pJoinInfo = pOperator->info;
  size_t leftNumJoin = taosArrayGetSize(leftRowLocations);

  int32_t i,j;

  for (i = leftRowIdx; i < leftNumJoin; ++i, rightRowIdx = 0) {
    SRowLocation* leftRow = taosArrayGet(leftRowLocations, i);
    SArray* pRightRows = NULL;
    if (rightTableHash != NULL) {
      fillGroupKeyValsFromTagCols(pJoinInfo->leftTagCols, pJoinInfo->leftTagKeys, leftRow->pDataBlock, leftRow->pos);
      int32_t  keyLen = combineGroupKeysIntoBuf(pJoinInfo->leftTagKeyBuf, pJoinInfo->leftTagKeys);
      SArray** ppRightRows = tSimpleHashGet(rightTableHash, pJoinInfo->leftTagKeyBuf, keyLen);
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

static void mergeJoinDestoryBuildTable(SSHashObj* pBuildTable) {
  void* p = NULL;
  int32_t iter = 0;

  while ((p = tSimpleHashIterate(pBuildTable, p, &iter)) != NULL) {
    SArray* rows = (*(SArray**)p);
    taosArrayDestroy(rows);
  }

  tSimpleHashCleanup(pBuildTable);
}

static void mergeJoinDestroyTSRangeCtx(SJoinOperatorInfo* pJoinInfo, SArray* leftRowLocations, SArray* leftCreatedBlocks,
                                       SArray* rightCreatedBlocks, SSHashObj* rightTableHash, SArray* rightRowLocations) {
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
  if (rightTableHash != NULL) {
    mergeJoinDestoryBuildTable(rightTableHash);
  }

  taosArrayDestroy(leftCreatedBlocks);
  taosArrayDestroy(leftRowLocations);

  pJoinInfo->rowCtx.rowRemains = false;
  pJoinInfo->rowCtx.leftRowLocations = NULL;
  pJoinInfo->rowCtx.leftCreatedBlocks = NULL;
  pJoinInfo->rowCtx.rightCreatedBlocks = NULL;
  pJoinInfo->rowCtx.buildTableTSRange = NULL;
  pJoinInfo->rowCtx.rightRowLocations = NULL;
}

static int32_t mergeJoinJoinDownstreamTsRanges(SOperatorInfo* pOperator, int64_t timestamp, SSDataBlock* pRes,
                                               int32_t* nRows) {
  int32_t code = TSDB_CODE_SUCCESS;
  SJoinOperatorInfo* pJoinInfo = pOperator->info;
  SArray* leftRowLocations = NULL;
  SArray* rightRowLocations = NULL;
  SArray* leftCreatedBlocks = NULL;
  SArray* rightCreatedBlocks = NULL;
  int32_t leftRowIdx = 0;
  int32_t rightRowIdx = 0;
  SSHashObj* rightTableHash = NULL;

  if (pJoinInfo->rowCtx.rowRemains) {
    leftRowLocations = pJoinInfo->rowCtx.leftRowLocations;
    leftCreatedBlocks = pJoinInfo->rowCtx.leftCreatedBlocks;
    rightTableHash = pJoinInfo->rowCtx.buildTableTSRange;
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
    if (pJoinInfo->pTagEqualConditions != NULL && taosArrayGetSize(rightRowLocations) > 16) {
      mergeJoinCreateBuildTable(pJoinInfo, rightRowLocations, &rightTableHash);
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
                                                rightRowIdx, rightTableHash, rightRowLocations, &reachThreshold);
  }

  if (!reachThreshold) {
    mergeJoinDestroyTSRangeCtx(pJoinInfo, leftRowLocations, leftCreatedBlocks, rightCreatedBlocks,
                               rightTableHash, rightRowLocations);

  } else {
      pJoinInfo->rowCtx.rowRemains = true;
      pJoinInfo->rowCtx.ts = timestamp;
      pJoinInfo->rowCtx.leftRowLocations = leftRowLocations;
      pJoinInfo->rowCtx.leftCreatedBlocks = leftCreatedBlocks;
      pJoinInfo->rowCtx.rightCreatedBlocks = rightCreatedBlocks;
      pJoinInfo->rowCtx.buildTableTSRange = rightTableHash;
      pJoinInfo->rowCtx.rightRowLocations = rightRowLocations;
  }
  return TSDB_CODE_SUCCESS;
}

static bool mergeJoinGetNextTimestamp(SOperatorInfo* pOperator, int64_t* pLeftTs, int64_t* pRightTs) {
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

  if (pJoinInfo->pLeft == NULL || pJoinInfo->leftPos >= pJoinInfo->pLeft->info.rows) {
    SOperatorInfo* ds1 = pOperator->pDownstream[0];
    pJoinInfo->pLeft = ds1->fpSet.getNextFn(ds1);

    pJoinInfo->leftPos = 0;
    if (pJoinInfo->pLeft == NULL) {
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      return false;
    }
  }

  if (pJoinInfo->pRight == NULL || pJoinInfo->rightPos >= pJoinInfo->pRight->info.rows) {
    SOperatorInfo* ds2 = pOperator->pDownstream[1];
    pJoinInfo->pRight = ds2->fpSet.getNextFn(ds2);

    pJoinInfo->rightPos = 0;
    if (pJoinInfo->pRight == NULL) {
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      return false;
    }
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
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

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
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
}

SSDataBlock* doMergeJoin(struct SOperatorInfo* pOperator) {
  SJoinOperatorInfo* pJoinInfo = pOperator->info;

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
  }
  return (pRes->info.rows > 0) ? pRes : NULL;
}
