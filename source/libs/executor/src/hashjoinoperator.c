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
#include "hashjoin.h"

int32_t initJoinKeyBufInfo(SColBufInfo** ppInfo, int32_t* colNum, SNodeList* pList, char** ppBuf) {
  *colNum = LIST_LENGTH(pList);
  
  (*ppInfo) = taosMemoryMalloc((*colNum) * sizeof(SColBufInfo));
  if (NULL == (*ppInfo)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int64_t bufSize = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    (*ppInfo)->slotId = pColNode->slotId;
    (*ppInfo)->vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    (*ppInfo)->bytes = pColNode->node.resType.bytes;
    bufSize += pColNode->node.resType.bytes;
  }  

  *ppBuf = taosMemoryMalloc(bufSize);
  if (NULL == *ppBuf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

void getJoinValColNum(SNodeList* pList, int32_t blkId, int32_t* colNum) {
  *colNum = 0;
  
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->dataBlockId == blkId) {
      (*colNum)++;
    }
  }
}

int32_t initJoinValBufInfo(SJoinTableInfo* pTable, SNodeList* pList) {
  getJoinValColNum(pList, pTable->blkId, &pTable->valNum);
  if (pTable->valNum <= 0) {
    qError("fail to get join value column, num:%d", pTable->valNum);
    return TSDB_CODE_INVALID_MSG;
  }
  
  pTable->valCols = taosMemoryMalloc(pTable->valNum * sizeof(SColBufInfo));
  if (NULL == pTable->valCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    if (pColNode->dataBlockId == pTable->blkId) {
      pTable->valCols->slotId = pColNode->slotId;
      pTable->valCols->vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
      if (pTable->valCols->vardata) {
        pTable->valVarData = true;
      }
      pTable->valCols->bytes = pColNode->node.resType.bytes;
      pTable->valBufSize += pColNode->node.resType.bytes;
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t initJoinTableInfo(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SNodeList* pKeyList = NULL;
  SJoinTableInfo* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  if (0 == idx) {
    pKeyList = pJoinNode->pOnLeft;
  } else {
    pKeyList = pJoinNode->pOnRight;
  }
  
  int32_t code = initJoinKeyBufInfo(&pTable->keyCols, &pTable->keyNum, pKeyList, &pTable->keyBuf);
  if (code) {
    return code;
  }
  int32_t code = initJoinValBufInfo(&pTable->keyCols, &pTable->keyNum, pJoinNode->pTargets, &pTable->keyBuf, pTable->blkId, pTable);
  if (code) {
    return code;
  }

  memcpy(&pTable->inputStat, pStat, sizeof(*pStat));

  return TSDB_CODE_SUCCESS;
}

void setJoinBuildAndProbeTable(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
  pInfo->pBuild = &pInfo->tbs[1];
  pInfo->pProbe = &pInfo->tbs[0];
}

FORCE_INLINE int32_t addPageToJoinBuf(SArray* pRowBufs) {
  SBufPageInfo page;
  page.pageSize = HASH_JOIN_DEFAULT_PAGE_SIZE;
  page.offset = 0;
  page.data = taosMemoryMalloc(page.pageSize);
  if (NULL == page.data) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosArrayPush(pRowBufs, &page);
  return TSDB_CODE_SUCCESS;
}

int32_t initJoinBufPages(SHJoinOperatorInfo* pInfo) {
  pInfo->pRowBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pRowBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return addPageToJoinBuf(pInfo->pRowBufs);
}


SOperatorInfo* createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SHJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SHJoinOperatorInfo));
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

  setOperatorInfo(pOperator, "HashJoinOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;

  initJoinTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  initJoinTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);

  setJoinBuildAndProbeTable(pInfo, pJoinNode);

  code = initJoinBufPages(pInfo);
  if (code) {
    goto _error;
  }

  size_t hashCap = pInfo->pBuild->inputStat.inputRowNum > 0 ? (pInfo->pBuild->inputStat.inputRowNum * 1.5) : 1024;
  pInfo->pKeyHash = tSimpleHashInit(hashCap, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pInfo->pKeyHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  if (pJoinNode->pFilterConditions != NULL && pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterJoin = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (pInfo->pCondAfterJoin == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pInfo->pCondAfterJoin);
    pLogicCond->pParameterList = nodesMakeList();
    if (pLogicCond->pParameterList == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->pFilterConditions));
    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->node.pConditions));
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
  } else if (pJoinNode->pFilterConditions != NULL) {
    pInfo->pCondAfterJoin = nodesCloneNode(pJoinNode->pFilterConditions);
  } else if (pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterJoin = nodesCloneNode(pJoinNode->node.pConditions);
  } else {
    pInfo->pCondAfterJoin = NULL;
  }

  code = filterInitFromNode(pInfo->pCondAfterJoin, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doHashJoin, NULL, destroHashJoinOperator, optrDefaultBufFn, NULL);
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

void destroHashJoinOperator(void* param) {
  SHJoinOperatorInfo* pJoinOperator = (SHJoinOperatorInfo*)param;
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

static void doHashJoinImpl(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SHJoinOperatorInfo* pJoinInfo = pOperator->info;

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

int32_t setColBufInfo(SSDataBlock* pBlock, int32_t colNum, SColBufInfo* pColList) {
  for (int32_t i = 0; i < colNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pColList[i].slotId);
    if (pColList[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pColList[i].slotId, pCol->info.type, pColList[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pColList[i].bytes != IS_VAR_DATA_TYPE(pCol->info.bytes))  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pColList[i].slotId, pCol->info.bytes, pColList[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    pColList[i].data = pCol->pData;
    if (pColList[i].vardata) {
      pColList[i].offset = pCol->varmeta.offset;
    }
  }

  return TSDB_CODE_SUCCESS;
}

FORCE_INLINE void copyColDataToBuf(int32_t colNum, int32_t rowIdx, SColBufInfo* pColList, char* pBuf, size_t *pBufLen) {
  char *pData = NULL;

  size_t bufLen = 0;
  for (int32_t i = 0; i < colNum; ++i) {
    if (pColList[i].vardata) {
      pData = pColList[i].data + pColList[i].offset[rowIdx];
      memcpy(pBuf + bufLen, pData, varDataTLen(pData));
      bufLen += varDataTLen(pData);
    } else {
      pData = pColList[i].data + pColList[i].bytes * rowIdx;
      memcpy(pBuf + bufLen, pColList[i].data, pColList[i].bytes);
      bufLen += pColList[i].bytes;
    }
  }

  if (pBufLen) {
    *pBufLen = bufLen;
  }
}

FORCE_INLINE int32_t getValBufFromPages(SArray* pPages, int32_t bufSize, char** pBuf) {
  do {
    SBufPageInfo* page = taosArrayGetLast(pPages);
    if ((page->pageSize - page->offset) >= bufSize) {
      *pBuf = page->data + page->offset;
      page->offset += bufSize;
      return TSDB_CODE_SUCCESS;
    }

    int32_t code = addPageToJoinBuf(pPages);
    if (code) {
      return code;
    }
  } while (true);
}

int32_t getJoinValBuf(SHJoinOperatorInfo* pJoin, SSHashObj* pHash, SResRowData* pRes, SJoinTableInfo* pTable, char** pBuf, size_t keyLen) {
  SResRowData res = {0};

  if (NULL == pRes) {
    res.rows = taosMemoryMalloc(sizeof(SBufRowInfo));
    if (NULL == res.rows) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  int32_t code = getValBufFromPages(pJoin->pRowBufs, getJoinValBufSize(), pBuf);
  if (code) {
    return code;
  }

  if (NULL == pRes && tSimpleHashPut(pHash, pTable->keyBuf, keyLen, &res, sizeof(res))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

}

int32_t addRowToHash(SHJoinOperatorInfo* pJoin, SSDataBlock* pBlock, char* pKey, size_t keyLen, int32_t rowIdx) {
  SJoinTableInfo* pBuild = pJoin->pBuild;
  int32_t code = setColBufInfo(pBlock, pBuild->valNum, pBuild->valCols);
  if (code) {
    return code;
  }

  char *valBuf = NULL;
  SResRowData* pRes = tSimpleHashGet(pJoin->pKeyHash, pBuild->keyBuf, keyLen);
  code = getJoinValBuf(pJoin->pKeyHash, pRes, pBuild, &valBuf, keyLen);
  if (code) {
    return code;
  }
  
  copyColDataToBuf(pBuild->valNum, rowIdx, pBuild->valCols, valBuf, NULL);

  return TSDB_CODE_SUCCESS;
}

int32_t addBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin) {
  SJoinTableInfo* pBuild = pJoin->pBuild;
  int32_t code = setColBufInfo(pBlock, pBuild->keyNum, pBuild->keyCols);
  if (code) {
    return code;
  }

  size_t bufLen = 0;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    copyColDataToBuf(pBuild->keyNum, i, pBuild->keyCols, pBuild->keyBuf, &bufLen);
    code = addRowToHash(pJoin, pBlock, pBuild->keyBuf, bufLen, i);
    if (code) {
      return code;
    }
  }

  return code;
}

int32_t buildJoinKeyHash(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SSDataBlock* pBlock = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  
  while (true) {
    pBlock = pJoin->pBuild->downStream->fpSet.getNextFn(pJoin->pBuild->downStream);
    if (NULL == pBlock) {
      break;
    }

    code = addBlockRowsToHash(pBlock, pJoin->pKeyHash, pJoin->pBuild);
    if (code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doHashJoin(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoinInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pRes = pJoinInfo->pRes;
  blockDataCleanup(pRes);

  if (NULL == pJoinInfo->pKeyHash) {
    code = buildJoinKeyHash(pJoinInfo);
    if (code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (tSimpleHashGetSize(pJoinInfo->pKeyHash) <= 0) {
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      return NULL;
    }
  }

  while (true) {
    int32_t numOfRowsBefore = pRes->info.rows;
    doHashJoinImpl(pOperator, pRes);
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
