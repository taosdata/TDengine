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


static int64_t getSingleKeyRowsNum(SBufRowInfo* pRow) {
  int64_t rows = 0;
  while (pRow) {
    rows++;
    pRow = pRow->next;
  }
  return rows;
}

static int64_t getRowsNumOfKeyHash(SSHashObj* pHash) {
  SGroupData* pGroup = NULL;
  int32_t iter = 0;
  int64_t rowsNum = 0;
  
  while (NULL != (pGroup = tSimpleHashIterate(pHash, pGroup, &iter))) {
    int32_t* pKey = tSimpleHashGetKey(pGroup, NULL);
    int64_t rows = getSingleKeyRowsNum(pGroup->rows);
    qTrace("build_key:%d, rows:%" PRId64, *pKey, rows);
    rowsNum += rows;
  }

  return rowsNum;
}

static int32_t initHJoinKeyColsInfo(SHJoinTableInfo* pTable, SNodeList* pList) {
  pTable->keyNum = LIST_LENGTH(pList);
  
  pTable->keyCols = taosMemoryMalloc(pTable->keyNum * sizeof(SHJoinColInfo));
  if (NULL == pTable->keyCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int64_t bufSize = 0;
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    pTable->keyCols[i].srcSlot = pColNode->slotId;
    pTable->keyCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    pTable->keyCols[i].bytes = pColNode->node.resType.bytes;
    bufSize += pColNode->node.resType.bytes;
    ++i;
  }  

  if (pTable->keyNum > 1) {
    pTable->keyBuf = taosMemoryMalloc(bufSize);
    if (NULL == pTable->keyBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void getHJoinValColNum(SNodeList* pList, int32_t blkId, int32_t* colNum) {
  *colNum = 0;
  
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
    if (pCol->dataBlockId == blkId) {
      (*colNum)++;
    }
  }
}

static bool valColInKeyCols(int16_t slotId, int32_t keyNum, SHJoinColInfo* pKeys, int32_t* pKeyIdx) {
  for (int32_t i = 0; i < keyNum; ++i) {
    if (pKeys[i].srcSlot == slotId) {
      *pKeyIdx = i;
      return true;
    }
  }

  return false;
}

static int32_t initJoinValColsInfo(SHJoinTableInfo* pTable, SNodeList* pList) {
  getHJoinValColNum(pList, pTable->blkId, &pTable->valNum);
  if (pTable->valNum == 0) {
    return TSDB_CODE_SUCCESS;
  }
  
  pTable->valCols = taosMemoryMalloc(pTable->valNum * sizeof(SHJoinColInfo));
  if (NULL == pTable->valCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t i = 0;
  int32_t colNum = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pColNode = (SColumnNode*)pTarget->pExpr;
    if (pColNode->dataBlockId == pTable->blkId) {
      if (valColInKeyCols(pColNode->slotId, pTable->keyNum, pTable->keyCols, &pTable->valCols[i].srcSlot)) {
        pTable->valCols[i].keyCol = true;
      } else {
        pTable->valCols[i].keyCol = false;
        pTable->valCols[i].srcSlot = pColNode->slotId;
        pTable->valColExist = true;
        colNum++;
      }
      pTable->valCols[i].dstSlot = pTarget->slotId;
      pTable->valCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
      if (pTable->valCols[i].vardata) {
        if (NULL == pTable->valVarCols) {
          pTable->valVarCols = taosArrayInit(pTable->valNum, sizeof(int32_t));
          if (NULL == pTable->valVarCols) {
            return TSDB_CODE_OUT_OF_MEMORY;
          }
        }
        taosArrayPush(pTable->valVarCols, &i);
      }
      pTable->valCols[i].bytes = pColNode->node.resType.bytes;
      if (!pTable->valCols[i].keyCol && !pTable->valCols[i].vardata) {
        pTable->valBufSize += pColNode->node.resType.bytes;
      }
      i++;
    }
  }

  pTable->valBitMapSize = BitmapLen(colNum);
  pTable->valBufSize += pTable->valBitMapSize;

  return TSDB_CODE_SUCCESS;
}


static int32_t initHJoinTableInfo(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SNodeList* pKeyList = NULL;
  SHJoinTableInfo* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  if (0 == idx) {
    pKeyList = pJoinNode->pOnLeft;
  } else {
    pKeyList = pJoinNode->pOnRight;
  }
  
  int32_t code = initHJoinKeyColsInfo(pTable, pKeyList);
  if (code) {
    return code;
  }
  code = initJoinValColsInfo(pTable, pJoinNode->pTargets);
  if (code) {
    return code;
  }

  memcpy(&pTable->inputStat, pStat, sizeof(*pStat));

  return TSDB_CODE_SUCCESS;
}

static void setHJoinBuildAndProbeTable(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
  int32_t buildIdx = 0;
  int32_t probeIdx = 1;

  pInfo->joinType = pJoinNode->joinType;
  
  switch (pInfo->joinType) {
    case JOIN_TYPE_INNER:
      if (pInfo->tbs[0].inputStat.inputRowNum <= pInfo->tbs[1].inputStat.inputRowNum) {
        buildIdx = 0;
        probeIdx = 1;
      } else {
        buildIdx = 1;
        probeIdx = 0;
      }
      break;
    case JOIN_TYPE_LEFT:
      buildIdx = 1;
      probeIdx = 0;
      break;
    case JOIN_TYPE_RIGHT:
      buildIdx = 0;
      probeIdx = 1;
      break;
    default:
      break;
  } 
  
  pInfo->pBuild = &pInfo->tbs[buildIdx];
  pInfo->pProbe = &pInfo->tbs[probeIdx];
  
  pInfo->pBuild->downStreamIdx = buildIdx;
  pInfo->pProbe->downStreamIdx = probeIdx;
}

static int32_t buildHJoinResColMap(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
  pInfo->pResColNum = pJoinNode->pTargets->length;
  pInfo->pResColMap = taosMemoryCalloc(pJoinNode->pTargets->length, sizeof(int8_t));
  if (NULL == pInfo->pResColMap) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  SNode* pNode = NULL;
  int32_t i = 0;
  FOREACH(pNode, pJoinNode->pTargets) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
    if (pCol->dataBlockId == pInfo->pBuild->blkId) {
      pInfo->pResColMap[i] = 1;
    }
    
    i++;
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE int32_t addPageToHJoinBuf(SArray* pRowBufs) {
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

static int32_t initHJoinBufPages(SHJoinOperatorInfo* pInfo) {
  pInfo->pRowBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pRowBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return addPageToHJoinBuf(pInfo->pRowBufs);
}

static void freeHJoinTableInfo(SHJoinTableInfo* pTable) {
  taosMemoryFreeClear(pTable->keyCols);
  taosMemoryFreeClear(pTable->keyBuf);
  taosMemoryFreeClear(pTable->valCols);
  taosArrayDestroy(pTable->valVarCols);
}

static void freeHJoinBufPage(void* param) {
  SBufPageInfo* pInfo = (SBufPageInfo*)param;
  taosMemoryFree(pInfo->data);
}

static void destroyHJoinKeyHash(SSHashObj** ppHash) {
  if (NULL == ppHash || NULL == (*ppHash)) {
    return;
  }

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(*ppHash, pIte, &iter)) != NULL) {
    SGroupData* pGroup = pIte;
    SBufRowInfo* pRow = pGroup->rows;
    SBufRowInfo* pNext = NULL;
    while (pRow) {
      pNext = pRow->next;
      taosMemoryFree(pRow);
      pRow = pNext;
    }
  }

  tSimpleHashCleanup(*ppHash);
  *ppHash = NULL;
}

static void destroyHashJoinOperator(void* param) {
  SHJoinOperatorInfo* pJoinOperator = (SHJoinOperatorInfo*)param;
  qError("hashJoin exec info, buildBlk:%" PRId64 ", buildRows:%" PRId64 ", probeBlk:%" PRId64 ", probeRows:%" PRId64 ", resRows:%" PRId64, 
         pJoinOperator->execInfo.buildBlkNum, pJoinOperator->execInfo.buildBlkRows, pJoinOperator->execInfo.probeBlkNum, 
         pJoinOperator->execInfo.probeBlkRows, pJoinOperator->execInfo.resRows);

  destroyHJoinKeyHash(&pJoinOperator->pKeyHash);

  freeHJoinTableInfo(&pJoinOperator->tbs[0]);
  freeHJoinTableInfo(&pJoinOperator->tbs[1]);
  pJoinOperator->pRes = blockDataDestroy(pJoinOperator->pRes);
  taosMemoryFreeClear(pJoinOperator->pResColMap);
  taosArrayDestroyEx(pJoinOperator->pRowBufs, freeHJoinBufPage);
  nodesDestroyNode(pJoinOperator->pCond);

  taosMemoryFreeClear(param);
}

static FORCE_INLINE char* retrieveColDataFromRowBufs(SArray* pRowBufs, SBufRowInfo* pRow) {
  SBufPageInfo *pPage = taosArrayGet(pRowBufs, pRow->pageId);
  return pPage->data + pRow->offset;
}

static FORCE_INLINE int32_t copyHJoinResRowsToBlock(SHJoinOperatorInfo* pJoin, int32_t rowNum, SBufRowInfo* pStart, SSDataBlock* pRes) {
  SHJoinTableInfo* pBuild = pJoin->pBuild;
  SHJoinTableInfo* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0, buildValIdx = 0;
  int32_t probeIdx = 0;
  SBufRowInfo* pRow = pStart;
  int32_t code = 0;

  for (int32_t r = 0; r < rowNum; ++r) {
    char* pData = retrieveColDataFromRowBufs(pJoin->pRowBufs, pRow);
    char* pValData = pData + pBuild->valBitMapSize;
    char* pKeyData = pProbe->keyData;
    buildIdx = buildValIdx = probeIdx = 0;
    for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
      if (pJoin->pResColMap[i]) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);
        if (pBuild->valCols[buildIdx].keyCol) {
          code = colDataSetVal(pDst, pRes->info.rows + r, pKeyData, false);
          if (code) {
            return code;
          }
          pKeyData += pBuild->valCols[buildIdx].vardata ? varDataTLen(pKeyData) : pBuild->valCols[buildIdx].bytes;
        } else {
          if (colDataIsNull_f(pData, buildValIdx)) {
            code = colDataSetVal(pDst, pRes->info.rows + r, NULL, true);
            if (code) {
              return code;
            }
          } else {
            code = colDataSetVal(pDst, pRes->info.rows + r, pValData, false);
            if (code) {
              return code;
            }
            pValData += pBuild->valCols[buildIdx].vardata ? varDataTLen(pValData) : pBuild->valCols[buildIdx].bytes;
          }
          buildValIdx++;
        }
        buildIdx++;
      } else if (0 == r) {
        SColumnInfoData* pSrc = taosArrayGet(pJoin->ctx.pProbeData->pDataBlock, pProbe->valCols[probeIdx].srcSlot);
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pProbe->valCols[probeIdx].dstSlot);
    
        code = colDataCopyNItems(pDst, pRes->info.rows, colDataGetData(pSrc, pJoin->ctx.probeIdx), rowNum, colDataIsNull_s(pSrc, pJoin->ctx.probeIdx));
        if (code) {
          return code;
        }
        probeIdx++;
      }
    }
    pRow = pRow->next;
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE void appendHJoinResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;
  SBufRowInfo* pStart = pCtx->pBuildRow;
  int32_t rowNum = 0;
  int32_t resNum = pRes->info.rows;
  
  while (pCtx->pBuildRow && (resNum < pRes->info.capacity)) {
    rowNum++;
    resNum++;
    pCtx->pBuildRow = pCtx->pBuildRow->next;
  }

  pJoin->execInfo.resRows += rowNum;

  int32_t code = copyHJoinResRowsToBlock(pJoin, rowNum, pStart, pRes);
  if (code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  pRes->info.rows = resNum;
  *allFetched = pCtx->pBuildRow ? false : true;
}


static FORCE_INLINE void copyKeyColsDataToBuf(SHJoinTableInfo* pTable, int32_t rowIdx, size_t *pBufLen) {
  char *pData = NULL;
  size_t bufLen = 0;
  
  if (1 == pTable->keyNum) {
    if (pTable->keyCols[0].vardata) {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].offset[rowIdx];
      bufLen = varDataTLen(pData);
    } else {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].bytes * rowIdx;
      bufLen = pTable->keyCols[0].bytes;
    }
    pTable->keyData = pData;
  } else {
    for (int32_t i = 0; i < pTable->keyNum; ++i) {
      if (pTable->keyCols[i].vardata) {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].offset[rowIdx];
        memcpy(pTable->keyBuf + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      } else {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].bytes * rowIdx;
        memcpy(pTable->keyBuf + bufLen, pData, pTable->keyCols[i].bytes);
        bufLen += pTable->keyCols[i].bytes;
      }
    }
    pTable->keyData = pTable->keyBuf;
  }

  if (pBufLen) {
    *pBufLen = bufLen;
  }
}


static void doHashJoinImpl(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableInfo* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  SSDataBlock* pRes = pJoin->pRes;
  size_t bufLen = 0;
  bool allFetched = false;

  if (pJoin->ctx.pBuildRow) {
    appendHJoinResToBlock(pOperator, pRes, &allFetched);
    if (pRes->info.rows >= pRes->info.capacity) {
      if (allFetched) {
        ++pCtx->probeIdx;
      }
      
      return;
    } else {
      ++pCtx->probeIdx;
    }
  }

  for (; pCtx->probeIdx < pCtx->pProbeData->info.rows; ++pCtx->probeIdx) {
    copyKeyColsDataToBuf(pProbe, pCtx->probeIdx, &bufLen);
    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
/*
    size_t keySize = 0;
    int32_t* pKey = tSimpleHashGetKey(pGroup, &keySize);
    ASSERT(keySize == bufLen && 0 == memcmp(pKey, pProbe->keyData, bufLen));
    int64_t rows = getSingleKeyRowsNum(pGroup->rows);
    pJoin->execInfo.expectRows += rows;    
    qTrace("hash_key:%d, rows:%" PRId64, *pKey, rows);
*/
    if (pGroup) {
      pCtx->pBuildRow = pGroup->rows;
      appendHJoinResToBlock(pOperator, pRes, &allFetched);
      if (pRes->info.rows >= pRes->info.capacity) {
        if (allFetched) {
          ++pCtx->probeIdx;
        }
        
        return;
      }
    } else {
      qTrace("no key matched");
    }
  }

  pCtx->rowRemains = false;
}

static int32_t setKeyColsData(SSDataBlock* pBlock, SHJoinTableInfo* pTable) {
  for (int32_t i = 0; i < pTable->keyNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->keyCols[i].srcSlot);
    if (pTable->keyCols[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pTable->keyCols[i].srcSlot, pCol->info.type, pTable->keyCols[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pTable->keyCols[i].bytes != pCol->info.bytes)  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pTable->keyCols[i].srcSlot, pCol->info.bytes, pTable->keyCols[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    pTable->keyCols[i].data = pCol->pData;
    if (pTable->keyCols[i].vardata) {
      pTable->keyCols[i].offset = pCol->varmeta.offset;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setValColsData(SSDataBlock* pBlock, SHJoinTableInfo* pTable) {
  if (!pTable->valColExist) {
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < pTable->valNum; ++i) {
    if (pTable->valCols[i].keyCol) {
      continue;
    }
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->valCols[i].srcSlot);
    if (pTable->valCols[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pTable->valCols[i].srcSlot, pCol->info.type, pTable->valCols[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pTable->valCols[i].bytes != pCol->info.bytes)  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pTable->valCols[i].srcSlot, pCol->info.bytes, pTable->valCols[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    if (!pTable->valCols[i].vardata) {
      pTable->valCols[i].bitMap = pCol->nullbitmap;
    }
    pTable->valCols[i].data = pCol->pData;
    if (pTable->valCols[i].vardata) {
      pTable->valCols[i].offset = pCol->varmeta.offset;
    }
  }

  return TSDB_CODE_SUCCESS;
}



static FORCE_INLINE void copyValColsDataToBuf(SHJoinTableInfo* pTable, int32_t rowIdx) {
  if (!pTable->valColExist) {
    return;
  }

  char *pData = NULL;
  size_t bufLen = pTable->valBitMapSize;
  memset(pTable->valData, 0, pTable->valBitMapSize);
  for (int32_t i = 0, m = 0; i < pTable->valNum; ++i) {
    if (pTable->valCols[i].keyCol) {
      continue;
    }
    if (pTable->valCols[i].vardata) {
      if (-1 == pTable->valCols[i].offset[rowIdx]) {
        colDataSetNull_f(pTable->valData, m);
      } else {
        pData = pTable->valCols[i].data + pTable->valCols[i].offset[rowIdx];
        memcpy(pTable->valData + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      }
    } else {
      if (colDataIsNull_f(pTable->valCols[i].bitMap, rowIdx)) {
        colDataSetNull_f(pTable->valData, m);
      } else {
        pData = pTable->valCols[i].data + pTable->valCols[i].bytes * rowIdx;
        memcpy(pTable->valData + bufLen, pData, pTable->valCols[i].bytes);
        bufLen += pTable->valCols[i].bytes;
      }
    }
    m++;
  }
}


static FORCE_INLINE int32_t getValBufFromPages(SArray* pPages, int32_t bufSize, char** pBuf, SBufRowInfo* pRow) {
  if (0 == bufSize) {
    pRow->pageId = -1;
    return TSDB_CODE_SUCCESS;
  }

  if (bufSize > HASH_JOIN_DEFAULT_PAGE_SIZE) {
    qError("invalid join value buf size:%d", bufSize);
    return TSDB_CODE_INVALID_PARA;
  }
  
  do {
    SBufPageInfo* page = taosArrayGetLast(pPages);
    if ((page->pageSize - page->offset) >= bufSize) {
      *pBuf = page->data + page->offset;
      pRow->pageId = taosArrayGetSize(pPages) - 1;
      pRow->offset = page->offset;
      page->offset += bufSize;
      return TSDB_CODE_SUCCESS;
    }

    int32_t code = addPageToHJoinBuf(pPages);
    if (code) {
      return code;
    }
  } while (true);
}

static FORCE_INLINE int32_t getHJoinValBufSize(SHJoinTableInfo* pTable, int32_t rowIdx) {
  if (NULL == pTable->valVarCols) {
    return pTable->valBufSize;
  }

  int32_t* varColIdx = NULL;
  int32_t bufLen = pTable->valBufSize;
  int32_t varColNum = taosArrayGetSize(pTable->valVarCols);
  for (int32_t i = 0; i < varColNum; ++i) {
    varColIdx = taosArrayGet(pTable->valVarCols, i);
    char* pData = pTable->valCols[*varColIdx].data + pTable->valCols[*varColIdx].offset[rowIdx];
    bufLen += varDataTLen(pData);
  }

  return bufLen;
}


static int32_t addRowToHashImpl(SHJoinOperatorInfo* pJoin, SGroupData* pGroup, SHJoinTableInfo* pTable, size_t keyLen, int32_t rowIdx) {
  SGroupData group = {0};
  SBufRowInfo* pRow = NULL;

  if (NULL == pGroup) {
    group.rows = taosMemoryMalloc(sizeof(SBufRowInfo));
    if (NULL == group.rows) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pRow = group.rows;
  } else {
    pRow = taosMemoryMalloc(sizeof(SBufRowInfo));
    if (NULL == pRow) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  int32_t code = getValBufFromPages(pJoin->pRowBufs, getHJoinValBufSize(pTable, rowIdx), &pTable->valData, pRow);
  if (code) {
    taosMemoryFree(pRow);
    return code;
  }

  if (NULL == pGroup) {
    pRow->next = NULL;
    if (tSimpleHashPut(pJoin->pKeyHash, pTable->keyData, keyLen, &group, sizeof(group))) {
      taosMemoryFree(pRow);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pRow->next = pGroup->rows;
    pGroup->rows = pRow;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t addRowToHash(SHJoinOperatorInfo* pJoin, SSDataBlock* pBlock, size_t keyLen, int32_t rowIdx) {
  SHJoinTableInfo* pBuild = pJoin->pBuild;
  int32_t code = setValColsData(pBlock, pBuild);
  if (code) {
    return code;
  }

  SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pBuild->keyData, keyLen);
  code = addRowToHashImpl(pJoin, pGroup, pBuild, keyLen, rowIdx);
  if (code) {
    return code;
  }
  
  copyValColsDataToBuf(pBuild, rowIdx);

  return TSDB_CODE_SUCCESS;
}

static int32_t addBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin) {
  SHJoinTableInfo* pBuild = pJoin->pBuild;
  int32_t code = setKeyColsData(pBlock, pBuild);
  if (code) {
    return code;
  }

  size_t bufLen = 0;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    copyKeyColsDataToBuf(pBuild, i, &bufLen);
    code = addRowToHash(pJoin, pBlock, bufLen, i);
    if (code) {
      return code;
    }
  }

  return code;
}

static int32_t buildHJoinKeyHash(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SSDataBlock* pBlock = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  
  while (true) {
    pBlock = getNextBlockFromDownstream(pOperator, pJoin->pBuild->downStreamIdx);
    if (NULL == pBlock) {
      break;
    }

    pJoin->execInfo.buildBlkNum++;
    pJoin->execInfo.buildBlkRows += pBlock->info.rows;

    code = addBlockRowsToHash(pBlock, pJoin);
    if (code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t launchBlockHashJoin(struct SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableInfo* pProbe = pJoin->pProbe;
  int32_t code = setKeyColsData(pBlock, pProbe);
  if (code) {
    return code;
  }
  code = setValColsData(pBlock, pProbe);
  if (code) {
    return code;
  }

  pJoin->ctx.probeIdx = 0;
  pJoin->ctx.pBuildRow = NULL;
  pJoin->ctx.pProbeData = pBlock;
  pJoin->ctx.rowRemains = true;

  doHashJoinImpl(pOperator);

  return TSDB_CODE_SUCCESS;
}

static void setHJoinDone(struct SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);

  SHJoinOperatorInfo* pInfo = pOperator->info;
  destroyHJoinKeyHash(&pInfo->pKeyHash);

  qError("hash Join done");  
}

static SSDataBlock* doHashJoin(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pRes = pJoin->pRes;
  pRes->info.rows = 0;
  int64_t st = 0;

  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  if (pOperator->status == OP_EXEC_DONE) {
    goto _return;
  }
  
  if (!pJoin->keyHashBuilt) {
    pJoin->keyHashBuilt = true;
    
    code = buildHJoinKeyHash(pOperator);
    if (code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (tSimpleHashGetSize(pJoin->pKeyHash) <= 0) {
      setHJoinDone(pOperator);
      goto _return;
    }

    //qTrace("build table rows:%" PRId64, getRowsNumOfKeyHash(pJoin->pKeyHash));
  }

  if (pJoin->ctx.rowRemains) {
    doHashJoinImpl(pOperator);
    
    if (pRes->info.rows >= pRes->info.capacity && pOperator->exprSupp.pFilterInfo != NULL) {
      doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    }
    if (pRes->info.rows > 0) {
      return pRes;
    }
  }

  while (true) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, pJoin->pProbe->downStreamIdx);
    if (NULL == pBlock) {
      setHJoinDone(pOperator);
      break;
    }

    pJoin->execInfo.probeBlkNum++;
    pJoin->execInfo.probeBlkRows += pBlock->info.rows;
    
    code = launchBlockHashJoin(pOperator, pBlock);
    if (code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

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

_return:

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }
  
  return (pRes->info.rows > 0) ? pRes : NULL;
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
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  setOperatorInfo(pOperator, "HashJoinOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  initHJoinTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  initHJoinTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);

  setHJoinBuildAndProbeTable(pInfo, pJoinNode);
  code = buildHJoinResColMap(pInfo, pJoinNode);
  if (code) {
    goto _error;
  }

  code = initHJoinBufPages(pInfo);
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
    pInfo->pCond = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (pInfo->pCond == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pInfo->pCond);
    pLogicCond->pParameterList = nodesMakeList();
    if (pLogicCond->pParameterList == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->pFilterConditions));
    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->node.pConditions));
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
  } else if (pJoinNode->pFilterConditions != NULL) {
    pInfo->pCond = nodesCloneNode(pJoinNode->pFilterConditions);
  } else if (pJoinNode->node.pConditions != NULL) {
    pInfo->pCond = nodesCloneNode(pJoinNode->node.pConditions);
  } else {
    pInfo->pCond = NULL;
  }

  code = filterInitFromNode(pInfo->pCond, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doHashJoin, NULL, destroyHashJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  qError("create hash Join operator done");

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyHashJoinOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


