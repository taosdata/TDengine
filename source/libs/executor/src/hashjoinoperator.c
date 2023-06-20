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
    (*ppInfo)->srcSlot = pColNode->slotId;
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
      pTable->valCols->srcSlot = pColNode->slotId;
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
  pInfo->pResColNum = pJoinNode->pTargets->length;
  pInfo->pResColMap = taosMemoryCalloc(pJoinNode->pTargets->length, sizeof(int8_t));
  if (NULL == pInfo->pResColMap) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  pInfo->pBuild = &pInfo->tbs[1];
  pInfo->pProbe = &pInfo->tbs[0];

  SNode* pNode = NULL;
  int32_t i = 0;
  FOREACH(pNode, pJoinNode->pTargets) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    if (pColNode->dataBlockId == pInfo->pBuild->blkId) {
      pInfo->pResColMap[i] = 1;
    }
    
    i++;
  }

  return TSDB_CODE_SUCCESS;
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

  code = setJoinBuildAndProbeTable(pInfo, pJoinNode);
  if (code) {
    goto _error;
  }

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

FORCE_INLINE char* getColDataFromRowBufs(SArray* pRowBufs, SBufRowInfo* pRow) {
  SBufPageInfo *pPage = taosArrayGet(pRowBufs, pRow->pageId);
  return pPage->data + pRow->offset;
}

FORCE_INLINE int32_t copyJoinResRowsToBlock(SHJoinOperatorInfo* pJoin, int32_t rowNum, SBufRowInfo* pStart, SSDataBlock* pRes) {
  SJoinTableInfo* pBuild = pJoin->pBuild;
  SJoinTableInfo* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0;
  int32_t probeIdx = 0;
  SBufRowInfo* pRow = pStart;
  int32_t code = 0;
  
  for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
    if (pJoin->pResColMap[i]) {
      SColumnInfoData* pCol = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);
      for (int32_t r = 0; r < rowNum; ++r) {
        code = colDataSetVal(pCol, pRes->info.rows + r, pRow->isNull ? NULL : getColDataFromRowBufs(pJoin->pRowBufs, pRow), pRow->isNull);
        if (code) {
          return code;
        }
        pRow = pRow->next;
      }
      buildIdx++;
    } else {
      SColumnInfoData* pSrc = taosArrayGet(pJoin->ctx.pProbeData, pProbe->valCols[probeIdx].srcSlot);
      SColumnInfoData* pDst = taosArrayGet(pJoin->ctx.pProbeData, pProbe->valCols[probeIdx].dstSlot);

      code = colDataCopyNItems(pDst, pRes->info.rows, colDataGetData(pSrc, pJoin->ctx.probeIdx), rowNum, colDataIsNull_s(pSrc, pJoin->ctx.probeIdx));
      if (code) {
        return code;
      }
      probeIdx++;
    }
  }

  return TSDB_CODE_SUCCESS;
}


void appendJoinResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;
  SBufRowInfo* pStart = pCtx->pBuildRow;
  int32_t rowNum = 0;
  int32_t resNum = pRes.info.rows;
  
  while (pCtx->pBuildRow && resNum < pRes.info.capacity) {
    rowNum++;
    resNum++;
    pCtx->pBuildRow = pCtx->pBuildRow->next;
  }

  int32_t code = copyJoinResRowsToBlock(pJoin, rowNum, pStart, pRes);
  if (code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  pRes->info.rows = resNum;
  pCtx->rowRemains = pCtx->pBuildRow ? true : false;
}

void doHashJoinImpl(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SJoinTableInfo* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  SSDataBlock* pRes = pJoin->pRes;
  size_t bufLen = 0;

  if (pJoin->ctx.pBuildRow) {
    appendJoinResToBlock(pOperator, pRes);
    return;
  }

  for (int32_t i = pCtx->probeIdx; i < pCtx->pProbeData->info.rows; ++i) {
    copyColDataToBuf(pProbe->keyNum, i, pProbe->keyCols, pProbe->keyBuf, &bufLen);
    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyBuf, bufLen);
    if (pGroup) {
      pCtx->pBuildRow = pGroup->rows;
      appendJoinResToBlock(pOperator, pRes);
      if (pRes->info.rows >= pRes.info.capacity) {
        break;
      }
    }
  }
}

int32_t setColBufInfo(SSDataBlock* pBlock, int32_t colNum, SColBufInfo* pColList) {
  for (int32_t i = 0; i < colNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pColList[i].srcSlot);
    if (pColList[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pColList[i].srcSlot, pCol->info.type, pColList[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pColList[i].bytes != IS_VAR_DATA_TYPE(pCol->info.bytes))  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pColList[i].srcSlot, pCol->info.bytes, pColList[i].bytes);
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

FORCE_INLINE int32_t getValBufFromPages(SArray* pPages, int32_t bufSize, char** pBuf, SBufRowInfo* pRow) {
  do {
    SBufPageInfo* page = taosArrayGetLast(pPages);
    if ((page->pageSize - page->offset) >= bufSize) {
      *pBuf = page->data + page->offset;
      pRow->pageId = taosArrayGetSize(pPages) - 1;
      pRow->offset = page->offset;
      page->offset += bufSize;
      return TSDB_CODE_SUCCESS;
    }

    int32_t code = addPageToJoinBuf(pPages);
    if (code) {
      return code;
    }
  } while (true);
}

FORCE_INLINE int32_t getJoinValBufSize(SJoinTableInfo* pTable, int32_t rowIdx) {
  if (!pTable->valVarData) {
    return pTable->valBufSize;
  }

  int32_t bufLen = 0;
  for (int32_t i = 0; i < pTable->valNum; ++i) {
    if (pTable->valCols[i].vardata) {
      char* pData = pTable->valCols[i].data + pTable->valCols[i].offset[rowIdx];
      bufLen += varDataTLen(pData);
    } else {
      bufLen += pTable->valCols[i].bytes;
    }
  }

  return bufLen;
}


int32_t getJoinValBuf(SHJoinOperatorInfo* pJoin, SSHashObj* pHash, SGroupData* pGroup, SJoinTableInfo* pTable, char** pBuf, size_t keyLen, int32_t rowIdx) {
  SGroupData group = {0};
  SBufRowInfo* pRow = NULL;

  if (NULL == pGroup) {
    group.rows = taosMemoryMalloc(sizeof(SBufRowInfo));
    if (NULL == group.rows) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pRow = group.rows;
  } else {
    pRow = taosMemoryMalloc(sizeof(SBufRowInfo))
    if (NULL == pRow) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  int32_t code = getValBufFromPages(pJoin->pRowBufs, getJoinValBufSize(pTable, rowIdx), pBuf, pRow);
  if (code) {
    return code;
  }

  if (NULL == pGroup) {
    pRow->next = NULL;
    if (tSimpleHashPut(pHash, pTable->keyBuf, keyLen, &group, sizeof(group))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pRow->next = pGroup->rows;
    pGroup->rows = pRow;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t addRowToHash(SHJoinOperatorInfo* pJoin, SSDataBlock* pBlock, char* pKey, size_t keyLen, int32_t rowIdx) {
  SJoinTableInfo* pBuild = pJoin->pBuild;
  int32_t code = setColBufInfo(pBlock, pBuild->valNum, pBuild->valCols);
  if (code) {
    return code;
  }

  char *valBuf = NULL;
  SGroupData* pRes = tSimpleHashGet(pJoin->pKeyHash, pBuild->keyBuf, keyLen);
  code = getJoinValBuf(pJoin->pKeyHash, pRes, pBuild, &valBuf, keyLen, rowIdx);
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

void launchBlockHashJoin(struct SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SJoinTableInfo* pProbe = pJoin->pProbe;
  int32_t code = setColBufInfo(pBlock, pProbe->keyNum, pProbe->keyCols);
  if (code) {
    return code;
  }
  code = setColBufInfo(pBlock, pProbe->valNum, pProbe->valCols);
  if (code) {
    return code;
  }


  pJoin->ctx.probeIdx = 0;
  pJoin->ctx.pBuildRow = NULL;
  pJoin->ctx.pProbeData = pBlock;

  doHashJoinImpl(pOperator);
}

SSDataBlock* doHashJoin(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pRes = pJoin->pRes;
  blockDataCleanup(pRes);

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  
  if (NULL == pJoin->pKeyHash) {
    code = buildJoinKeyHash(pJoin);
    if (code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (tSimpleHashGetSize(pJoin->pKeyHash) <= 0) {
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }
  }

  if (pJoin->ctx.rowRemains) {
    doHashJoinImpl(pOperator);
    
    if (pRes->info.rows >= pOperator->resultInfo.threshold && pOperator->exprSupp.pFilterInfo != NULL) {
      doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    }
    if (pRes->info.rows > 0) {
      return pRes;
    }
  }

  while (true) {
    SSDataBlock* pBlock = pJoin->pProbe->downStream->fpSet.getNextFn(pJoin->pProbe->downStream);
    if (NULL == pBlock) {
      break;
    }
    
    launchBlockHashJoin(pOperator, pBlock);

    if (pRes->info.rows < pOperator->resultInfo.threshold) {
      continue;
    }
    
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    }
    if (pRes->info.rows > 0) {
      break;
    }
  }
  
  return (pRes->info.rows > 0) ? pRes : NULL;
}
