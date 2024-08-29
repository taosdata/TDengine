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
#include "functionMgt.h"


bool hJoinBlkReachThreshold(SHJoinOperatorInfo* pInfo, int64_t blkRows) {
  if (INT64_MAX == pInfo->ctx.limit || pInfo->pFinFilter != NULL) {
    return blkRows >= pInfo->blkThreshold;
  }
  
  return (pInfo->execInfo.resRows + blkRows) >= pInfo->ctx.limit;
}

int32_t hJoinHandleMidRemains(SHJoinOperatorInfo* pJoin, SHJoinCtx* pCtx) {
  TSWAP(pJoin->midBlk, pJoin->finBlk);

  pCtx->midRemains = false;

  return TSDB_CODE_SUCCESS;
}

int32_t hJoinCopyMergeMidBlk(SHJoinCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin) {
  SSDataBlock* pLess = *ppMid;
  SSDataBlock* pMore = *ppFin;

/*
  if ((*ppMid)->info.rows < (*ppFin)->info.rows) {
    pLess = (*ppMid);
    pMore = (*ppFin);
  } else {
    pLess = (*ppFin);
    pMore = (*ppMid);
  }
*/

  int32_t totalRows = pMore->info.rows + pLess->info.rows;
  if (totalRows <= pMore->info.capacity) {
    HJ_ERR_RET(blockDataMerge(pMore, pLess));
    blockDataCleanup(pLess);
    pCtx->midRemains = false;
  } else {
    int32_t copyRows = pMore->info.capacity - pMore->info.rows;
    HJ_ERR_RET(blockDataMergeNRows(pMore, pLess, pLess->info.rows - copyRows, copyRows));
    blockDataShrinkNRows(pLess, copyRows);
    pCtx->midRemains = true;
  }

/*
  if (pMore != (*ppFin)) {
    TSWAP(*ppMid, *ppFin);
  }
*/

  return TSDB_CODE_SUCCESS;
}

int32_t hJoinSetImplFp(SHJoinOperatorInfo* pJoin) {
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER:
      pJoin->joinFp = hInnerJoinDo;
      break;
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT: {
      switch (pJoin->subType) {
        case JOIN_STYPE_OUTER:          
          pJoin->joinFp = hLeftJoinDo;
          break;
        default:
          break;
      }
      break;
    }      
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t hJoinLaunchPrimExpr(SSDataBlock* pBlock, SHJoinTableCtx* pTable, int32_t startIdx, int32_t endIdx) {
  if (NULL == pTable->primExpr) {
    return TSDB_CODE_SUCCESS;
  }

  SHJoinPrimExprCtx* pCtx = &pTable->primCtx;
  SColumnInfoData* pPrimIn = taosArrayGet(pBlock->pDataBlock, pTable->primCol->srcSlot);
  SColumnInfoData* pPrimOut = taosArrayGet(pBlock->pDataBlock, pTable->primCtx.targetSlotId);
  if (0 != pCtx->timezoneUnit) {
    for (int32_t i = startIdx; i <= endIdx; ++i) {
      ((int64_t*)pPrimOut->pData)[i] = ((int64_t*)pPrimIn->pData)[i] - (((int64_t*)pPrimIn->pData)[i] - pCtx->timezoneUnit) % pCtx->truncateUnit;
    }
  } else {
    for (int32_t i = startIdx; i <= endIdx; ++i) {
      ((int64_t*)pPrimOut->pData)[i] = ((int64_t*)pPrimIn->pData)[i] / pCtx->truncateUnit * pCtx->truncateUnit;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int64_t hJoinGetSingleKeyRowsNum(SBufRowInfo* pRow) {
  int64_t rows = 0;
  while (pRow) {
    rows++;
    pRow = pRow->next;
  }
  return rows;
}

static int64_t hJoinGetRowsNumOfKeyHash(SSHashObj* pHash) {
  SGroupData* pGroup = NULL;
  int32_t iter = 0;
  int64_t rowsNum = 0;
  
  while (NULL != (pGroup = tSimpleHashIterate(pHash, pGroup, &iter))) {
    int32_t* pKey = tSimpleHashGetKey(pGroup, NULL);
    int64_t rows = hJoinGetSingleKeyRowsNum(pGroup->rows);
    //qTrace("build_key:%d, rows:%" PRId64, *pKey, rows);
    rowsNum += rows;
  }

  return rowsNum;
}

static int32_t hJoinInitKeyColsInfo(SHJoinTableCtx* pTable, SNodeList* pList) {
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

static void hJoinGetValColsNum(SNodeList* pList, int32_t blkId, int32_t* colNum) {
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

static bool hJoinIsValColInKeyCols(int16_t slotId, int32_t keyNum, SHJoinColInfo* pKeys, int32_t* pKeyIdx) {
  for (int32_t i = 0; i < keyNum; ++i) {
    if (pKeys[i].srcSlot == slotId) {
      *pKeyIdx = i;
      return true;
    }
  }

  return false;
}

static int32_t hJoinInitValColsInfo(SHJoinTableCtx* pTable, SNodeList* pList) {
  hJoinGetValColsNum(pList, pTable->blkId, &pTable->valNum);
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
      if (hJoinIsValColInKeyCols(pColNode->slotId, pTable->keyNum, pTable->keyCols, &pTable->valCols[i].srcSlot)) {
        pTable->valCols[i].keyCol = true;
      } else {
        pTable->valCols[i].keyCol = false;
        pTable->valCols[i].srcSlot = pColNode->slotId;
        pTable->valColExist = true;
        colNum++;
      }
      pTable->valCols[i].dstSlot = pTarget->slotId;
      pTable->valCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
      if (pTable->valCols[i].vardata && !pTable->valCols[i].keyCol) {
        if (NULL == pTable->valVarCols) {
          pTable->valVarCols = taosArrayInit(pTable->valNum, sizeof(int32_t));
          if (NULL == pTable->valVarCols) {
            return terrno;
          }
        }
        if (NULL == taosArrayPush(pTable->valVarCols, &i)) {
          return terrno;
        }
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

static int32_t hJoinInitPrimKeyInfo(SHJoinTableCtx* pTable, int32_t slotId) {
  pTable->primCol = taosMemoryMalloc(sizeof(SHJoinColMap));
  if (NULL == pTable->primCol) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTable->primCol->srcSlot = slotId;

  return TSDB_CODE_SUCCESS;
}


static int32_t hJoinInitPrimExprCtx(SNode* pNode, SHJoinPrimExprCtx* pCtx, SHJoinTableCtx* pTable) {
  if (NULL == pNode) {
    pCtx->targetSlotId = pTable->primCol->srcSlot;
    return TSDB_CODE_SUCCESS;
  }
  
  if (QUERY_NODE_TARGET != nodeType(pNode)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }  

  STargetNode* pTarget = (STargetNode*)pNode;
  if (QUERY_NODE_FUNCTION != nodeType(pTarget->pExpr)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SFunctionNode* pFunc = (SFunctionNode*)pTarget->pExpr;
  if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (4 != pFunc->pParameterList->length && 5 != pFunc->pParameterList->length) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SValueNode* pUnit = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  SValueNode* pCurrTz = (5 == pFunc->pParameterList->length) ? (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2) : NULL;
  SValueNode* pTimeZone = (5 == pFunc->pParameterList->length) ? (SValueNode*)nodesListGetNode(pFunc->pParameterList, 4) : (SValueNode*)nodesListGetNode(pFunc->pParameterList, 3);

  pCtx->truncateUnit = pUnit->typeData;
  if ((NULL == pCurrTz || 1 == pCurrTz->typeData) && pCtx->truncateUnit >= (86400 * TSDB_TICK_PER_SECOND(pFunc->node.resType.precision))) {
    pCtx->timezoneUnit = offsetFromTz(varDataVal(pTimeZone->datum.p), TSDB_TICK_PER_SECOND(pFunc->node.resType.precision));
  }

  pCtx->targetSlotId = pTarget->slotId;

  return TSDB_CODE_SUCCESS;
}


static int32_t hJoinInitTableInfo(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SNodeList* pKeyList = NULL;
  SHJoinTableCtx* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  if (0 == idx) {
    pKeyList = pJoinNode->pOnLeft;
    pTable->hasTimeRange = pJoinNode->timeRangeTarget & 0x1;
  } else {
    pKeyList = pJoinNode->pOnRight;
    pTable->hasTimeRange = pJoinNode->timeRangeTarget & 0x2;
  }

  HJ_ERR_RET(hJoinInitPrimKeyInfo(pTable, (0 == idx) ? pJoinNode->leftPrimSlotId : pJoinNode->rightPrimSlotId));
  
  int32_t code = hJoinInitKeyColsInfo(pTable, pKeyList);
  if (code) {
    return code;
  }
  code = hJoinInitValColsInfo(pTable, pJoinNode->pTargets);
  if (code) {
    return code;
  }

  TAOS_MEMCPY(&pTable->inputStat, pStat, sizeof(*pStat));

  HJ_ERR_RET(hJoinInitPrimExprCtx(pTable->primExpr, &pTable->primCtx, pTable));

  return TSDB_CODE_SUCCESS;
}

static void hJoinSetBuildAndProbeTable(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
  int32_t buildIdx = 0;
  int32_t probeIdx = 1;

  pInfo->joinType = pJoinNode->joinType;
  pInfo->subType = pJoinNode->subType;
  
  switch (pInfo->joinType) {
    case JOIN_TYPE_INNER:
    case JOIN_TYPE_FULL:
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

  if (0 == buildIdx) {
    pInfo->pBuild->primExpr = pJoinNode->leftPrimExpr;
    pInfo->pProbe->primExpr = pJoinNode->rightPrimExpr;
  } else {
    pInfo->pBuild->primExpr = pJoinNode->rightPrimExpr;
    pInfo->pProbe->primExpr = pJoinNode->leftPrimExpr;
  }
}

static int32_t hJoinBuildResColsMap(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
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


static FORCE_INLINE int32_t hJoinAddPageToBufs(SArray* pRowBufs) {
  SBufPageInfo page;
  page.pageSize = HASH_JOIN_DEFAULT_PAGE_SIZE;
  page.offset = 0;
  page.data = taosMemoryMalloc(page.pageSize);
  if (NULL == page.data) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (NULL == taosArrayPush(pRowBufs, &page)) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinInitBufPages(SHJoinOperatorInfo* pInfo) {
  pInfo->pRowBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pRowBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return hJoinAddPageToBufs(pInfo->pRowBufs);
}

static void hJoinFreeTableInfo(SHJoinTableCtx* pTable) {
  taosMemoryFreeClear(pTable->keyCols);
  taosMemoryFreeClear(pTable->keyBuf);
  taosMemoryFreeClear(pTable->valCols);
  taosArrayDestroy(pTable->valVarCols);
  taosMemoryFree(pTable->primCol);
}

static void hJoinFreeBufPage(void* param) {
  SBufPageInfo* pInfo = (SBufPageInfo*)param;
  taosMemoryFree(pInfo->data);
}

static void hJoinDestroyKeyHash(SSHashObj** ppHash) {
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

static FORCE_INLINE int32_t hJoinRetrieveColDataFromRowBufs(SArray* pRowBufs, SBufRowInfo* pRow, char** ppData) {
  *ppData = NULL;
  
  if ((uint16_t)-1 == pRow->pageId) {
    return TSDB_CODE_SUCCESS;
  }
  SBufPageInfo *pPage = taosArrayGet(pRowBufs, pRow->pageId);
  if (NULL == pPage) {
    qError("fail to get %d page, total:%d", pRow->pageId, (int32_t)taosArrayGetSize(pRowBufs));
    QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  
  *ppData = pPage->data + pRow->offset;

  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinCopyResRowsToBlock(SHJoinOperatorInfo* pJoin, int32_t rowNum, SBufRowInfo* pStart, SSDataBlock* pRes) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0, buildValIdx = 0;
  int32_t probeIdx = 0;
  SBufRowInfo* pRow = pStart;
  int32_t code = 0;
  char* pData = NULL;

  for (int32_t r = 0; r < rowNum; ++r) {
    HJ_ERR_RET(hJoinRetrieveColDataFromRowBufs(pJoin->pRowBufs, pRow, &pData));
    
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
    
        code = colDataCopyNItems(pDst, pRes->info.rows, colDataGetData(pSrc, pJoin->ctx.probeStartIdx), rowNum, colDataIsNull_s(pSrc, pJoin->ctx.probeStartIdx));
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

int32_t hJoinCopyNMatchRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, int32_t startIdx, int32_t rows) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0;
  int32_t probeIdx = 0;
  int32_t code = 0;

  for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
    if (pJoin->pResColMap[i]) {
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);
      colDataSetNItemsNull(pDst, pRes->info.rows, rows);

      buildIdx++;
    } else {
      SColumnInfoData* pSrc = taosArrayGet(pJoin->ctx.pProbeData->pDataBlock, pProbe->valCols[probeIdx].srcSlot);
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pProbe->valCols[probeIdx].dstSlot);

      QRY_ERR_RET(colDataAssignNRows(pDst, pRes->info.rows, pSrc, startIdx, rows));

      probeIdx++;
    }
  }

  pRes->info.rows += rows;

  return TSDB_CODE_SUCCESS;
}



void hJoinAppendResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched) {
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

  int32_t code = hJoinCopyResRowsToBlock(pJoin, rowNum, pStart, pRes);
  if (code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  pRes->info.rows = resNum;
  *allFetched = pCtx->pBuildRow ? false : true;
}


bool hJoinCopyKeyColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen) {
  char *pData = NULL;
  size_t bufLen = 0;
  
  if (1 == pTable->keyNum) {
    if (colDataIsNull_s(pTable->keyCols[0].colData, rowIdx)) {
      return true;
    }
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
      if (colDataIsNull_s(pTable->keyCols[i].colData, rowIdx)) {
        return true;
      }
      if (pTable->keyCols[i].vardata) {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].offset[rowIdx];
        TAOS_MEMCPY(pTable->keyBuf + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      } else {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].bytes * rowIdx;
        TAOS_MEMCPY(pTable->keyBuf + bufLen, pData, pTable->keyCols[i].bytes);
        bufLen += pTable->keyCols[i].bytes;
      }
    }
    pTable->keyData = pTable->keyBuf;
  }

  if (pBufLen) {
    *pBufLen = bufLen;
  }

  return false;
}

static int32_t hJoinSetKeyColsData(SSDataBlock* pBlock, SHJoinTableCtx* pTable) {
  for (int32_t i = 0; i < pTable->keyNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->keyCols[i].srcSlot);
    if (NULL == pCol) {
      qError("fail to get %d col, total:%d", pTable->keyCols[i].srcSlot, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
      QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }
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
    pTable->keyCols[i].colData = pCol;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinSetValColsData(SSDataBlock* pBlock, SHJoinTableCtx* pTable) {
  if (!pTable->valColExist) {
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < pTable->valNum; ++i) {
    if (pTable->valCols[i].keyCol) {
      continue;
    }
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->valCols[i].srcSlot);
    if (NULL == pCol) {
      qError("fail to get %d col, total:%d", pTable->valCols[i].srcSlot, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
      QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }
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



static FORCE_INLINE void hJoinCopyValColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx) {
  if (!pTable->valColExist) {
    return;
  }

  char *pData = NULL;
  size_t bufLen = pTable->valBitMapSize;
  TAOS_MEMSET(pTable->valData, 0, pTable->valBitMapSize);
  for (int32_t i = 0, m = 0; i < pTable->valNum; ++i) {
    if (pTable->valCols[i].keyCol) {
      continue;
    }
    if (pTable->valCols[i].vardata) {
      if (-1 == pTable->valCols[i].offset[rowIdx]) {
        colDataSetNull_f(pTable->valData, m);
      } else {
        pData = pTable->valCols[i].data + pTable->valCols[i].offset[rowIdx];
        TAOS_MEMCPY(pTable->valData + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      }
    } else {
      if (colDataIsNull_f(pTable->valCols[i].bitMap, rowIdx)) {
        colDataSetNull_f(pTable->valData, m);
      } else {
        pData = pTable->valCols[i].data + pTable->valCols[i].bytes * rowIdx;
        TAOS_MEMCPY(pTable->valData + bufLen, pData, pTable->valCols[i].bytes);
        bufLen += pTable->valCols[i].bytes;
      }
    }
    m++;
  }
}


static FORCE_INLINE int32_t hJoinGetValBufFromPages(SArray* pPages, int32_t bufSize, char** pBuf, SBufRowInfo* pRow) {
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

    int32_t code = hJoinAddPageToBufs(pPages);
    if (code) {
      return code;
    }
  } while (true);
}

static FORCE_INLINE int32_t hJoinGetValBufSize(SHJoinTableCtx* pTable, int32_t rowIdx) {
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


static int32_t hJoinAddRowToHashImpl(SHJoinOperatorInfo* pJoin, SGroupData* pGroup, SHJoinTableCtx* pTable, size_t keyLen, int32_t rowIdx) {
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

  int32_t code = hJoinGetValBufFromPages(pJoin->pRowBufs, hJoinGetValBufSize(pTable, rowIdx), &pTable->valData, pRow);
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

static int32_t hJoinAddRowToHash(SHJoinOperatorInfo* pJoin, SSDataBlock* pBlock, size_t keyLen, int32_t rowIdx) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  int32_t code = hJoinSetValColsData(pBlock, pBuild);
  if (code) {
    return code;
  }

  SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pBuild->keyData, keyLen);
  code = hJoinAddRowToHashImpl(pJoin, pGroup, pBuild, keyLen, rowIdx);
  if (code) {
    return code;
  }
  
  hJoinCopyValColsDataToBuf(pBuild, rowIdx);

  return TSDB_CODE_SUCCESS;
}

static bool hJoinFilterTimeRange(SSDataBlock* pBlock, STimeWindow* pRange, int32_t primSlot, int32_t* startIdx, int32_t* endIdx) {
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, primSlot);
  if (NULL == pCol) {
    qError("hash join can't get prim col, slot:%d, slotNum:%d", primSlot, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
    return false;
  }

  TSKEY skey = *(TSKEY*)colDataGetData(pCol, 0);
  TSKEY ekey = *(TSKEY*)colDataGetData(pCol, (pBlock->info.rows - 1));

  if (ekey < pRange->skey || skey > pRange->ekey) {
    return false;
  }

  if (skey >= pRange->skey && ekey <= pRange->ekey) {
    *startIdx = 0;
    *endIdx = pBlock->info.rows - 1;
    return true;
  }

  if (skey < pRange->skey && ekey > pRange->ekey) {
    TSKEY *pStart = (TSKEY*)taosbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
    TSKEY *pEnd = (TSKEY*)taosbsearch(&pRange->ekey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_LE);
    *startIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
    *endIdx = ((uint64_t)pEnd - (uint64_t)pCol->pData) / sizeof(int64_t);
    return true;
  }

  if (skey >= pRange->skey) {
    TSKEY *pEnd = (TSKEY*)taosbsearch(&pRange->ekey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_LE);
    *startIdx = 0;
    *endIdx = ((uint64_t)pEnd - (uint64_t)pCol->pData) / sizeof(int64_t);
    return true;
  }

  TSKEY *pStart = (TSKEY*)taosbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
  *startIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
  *endIdx = pBlock->info.rows - 1;
  
  return true;
}

static int32_t hJoinAddBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  int32_t startIdx = 0, endIdx = pBlock->info.rows - 1;
  if (pBuild->hasTimeRange && !hJoinFilterTimeRange(pBlock, &pJoin->tblTimeRange, pBuild->primCol->srcSlot, &startIdx, &endIdx)) {
    return TSDB_CODE_SUCCESS;
  }

  HJ_ERR_RET(hJoinLaunchPrimExpr(pBlock, pBuild, startIdx, endIdx));

  int32_t code = hJoinSetKeyColsData(pBlock, pBuild);
  if (code) {
    return code;
  }

  size_t bufLen = 0;
  for (int32_t i = startIdx; i <= endIdx; ++i) {
    if (hJoinCopyKeyColsDataToBuf(pBuild, i, &bufLen)) {
      continue;
    }
    code = hJoinAddRowToHash(pJoin, pBlock, bufLen, i);
    if (code) {
      return code;
    }
  }

  return code;
}

static int32_t hJoinBuildHash(struct SOperatorInfo* pOperator, bool* queryDone) {
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

    code = hJoinAddBlockRowsToHash(pBlock, pJoin);
    if (code) {
      return code;
    }
  }

  if (IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && tSimpleHashGetSize(pJoin->pKeyHash) <= 0) {
    hJoinSetDone(pOperator);
    *queryDone = true;
  }
  
  //qTrace("build table rows:%" PRId64, hJoinGetRowsNumOfKeyHash(pJoin->pKeyHash));

  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinPrepareStart(struct SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t startIdx = 0, endIdx = pBlock->info.rows - 1;
  if (pProbe->hasTimeRange && !hJoinFilterTimeRange(pBlock, &pJoin->tblTimeRange, pProbe->primCol->srcSlot, &startIdx, &endIdx)) {
    if (!IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType)) {
      pJoin->ctx.probeEndIdx = -1;
      pJoin->ctx.probePostIdx = 0;
      pJoin->ctx.pProbeData = pBlock;
      pJoin->ctx.rowRemains = true;
      pJoin->ctx.probePhase = E_JOIN_PHASE_POST;
      
      HJ_ERR_RET((*pJoin->joinFp)(pOperator));
    }
    
    return TSDB_CODE_SUCCESS;
  }

  HJ_ERR_RET(hJoinLaunchPrimExpr(pBlock, pProbe, startIdx, endIdx));

  int32_t code = hJoinSetKeyColsData(pBlock, pProbe);
  if (code) {
    return code;
  }
  code = hJoinSetValColsData(pBlock, pProbe);
  if (code) {
    return code;
  }

  pJoin->ctx.probeStartIdx = startIdx;
  pJoin->ctx.probeEndIdx = endIdx;
  pJoin->ctx.pBuildRow = NULL;
  pJoin->ctx.pProbeData = pBlock;
  pJoin->ctx.rowRemains = true;
  pJoin->ctx.probePreIdx = 0;
  pJoin->ctx.probePostIdx = endIdx + 1;

  if (!IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) && startIdx > 0) {
    pJoin->ctx.probePhase = E_JOIN_PHASE_PRE;
  } else {
    pJoin->ctx.probePhase = E_JOIN_PHASE_CUR;
  }

  HJ_ERR_RET((*pJoin->joinFp)(pOperator));

  return TSDB_CODE_SUCCESS;
}

void hJoinSetDone(struct SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);

  SHJoinOperatorInfo* pInfo = pOperator->info;
  hJoinDestroyKeyHash(&pInfo->pKeyHash);

  qDebug("hash Join done");  
}

static int32_t hJoinMainProcess(struct SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  int32_t             code = TSDB_CODE_SUCCESS;
  SSDataBlock*        pRes = pJoin->finBlk;
  int64_t             st = 0;

  QRY_OPTR_CHECK(pResBlock);
  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  if (pOperator->status == OP_EXEC_DONE) {
    pRes->info.rows = 0;
    goto _return;
  }

  if (!pJoin->keyHashBuilt) {
    pJoin->keyHashBuilt = true;

    bool queryDone = false;
    code = hJoinBuildHash(pOperator, &queryDone);
    if (code) {
      pTaskInfo->code = code;
      return code;
    }

    if (queryDone) {
      goto _return;
    }
  }

  blockDataCleanup(pRes);

  if (pJoin->ctx.rowRemains) {
    code = (*pJoin->joinFp)(pOperator);
    if (code) {
      pTaskInfo->code = code;
      return pTaskInfo->code;
    }

    if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
      code = doFilter(pRes, pJoin->pFinFilter, NULL);
      if (code) {
        pTaskInfo->code = code;
        return pTaskInfo->code;
      }
    }

    if (pRes->info.rows > 0) {
      *pResBlock = pRes;
      return code;
    }
  }

  while (true) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, pJoin->pProbe->downStreamIdx);
    if (NULL == pBlock) {
      hJoinSetDone(pOperator);
      break;
    }

    pJoin->execInfo.probeBlkNum++;
    pJoin->execInfo.probeBlkRows += pBlock->info.rows;

    code = hJoinPrepareStart(pOperator, pBlock);
    if (code) {
      pTaskInfo->code = code;
      return pTaskInfo->code;
    }

    if (!hJoinBlkReachThreshold(pJoin, pRes->info.rows)) {
      continue;
    }

    if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
      code = doFilter(pRes, pJoin->pFinFilter, NULL);
      if (code) {
        pTaskInfo->code = code;
        return pTaskInfo->code;
      }
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

_return:
  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  if (pRes->info.rows > 0) {
    *pResBlock = pRes;
  }

  return code;
}

static void destroyHashJoinOperator(void* param) {
  SHJoinOperatorInfo* pJoinOperator = (SHJoinOperatorInfo*)param;
  qDebug("hashJoin exec info, buildBlk:%" PRId64 ", buildRows:%" PRId64 ", probeBlk:%" PRId64 ", probeRows:%" PRId64 ", resRows:%" PRId64, 
         pJoinOperator->execInfo.buildBlkNum, pJoinOperator->execInfo.buildBlkRows, pJoinOperator->execInfo.probeBlkNum, 
         pJoinOperator->execInfo.probeBlkRows, pJoinOperator->execInfo.resRows);

  hJoinDestroyKeyHash(&pJoinOperator->pKeyHash);

  hJoinFreeTableInfo(&pJoinOperator->tbs[0]);
  hJoinFreeTableInfo(&pJoinOperator->tbs[1]);
  blockDataDestroy(pJoinOperator->finBlk);
  pJoinOperator->finBlk = NULL;
  taosMemoryFreeClear(pJoinOperator->pResColMap);
  taosArrayDestroyEx(pJoinOperator->pRowBufs, hJoinFreeBufPage);

  taosMemoryFreeClear(param);
}

int32_t hJoinHandleConds(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode) {
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER: {
      SNode* pCond = NULL;
      if (pJoinNode->pFullOnCond != NULL) {
        if (pJoinNode->node.pConditions != NULL) {
          HJ_ERR_RET(mergeJoinConds(&pJoinNode->pFullOnCond, &pJoinNode->node.pConditions));
        }
        pCond = pJoinNode->pFullOnCond;
      } else if (pJoinNode->node.pConditions != NULL) {
        pCond = pJoinNode->node.pConditions;
      }
      
      HJ_ERR_RET(filterInitFromNode(pCond, &pJoin->pFinFilter, 0));
      break;
    }
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT:
    case JOIN_TYPE_FULL:
      if (pJoinNode->pFullOnCond != NULL) {
        HJ_ERR_RET(filterInitFromNode(pJoinNode->pFullOnCond, &pJoin->pPreFilter, 0));
      }
      if (pJoinNode->node.pConditions != NULL) {
        HJ_ERR_RET(filterInitFromNode(pJoinNode->node.pConditions, &pJoin->pFinFilter, 0));
      }
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static uint32_t hJoinGetFinBlkCapacity(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode) {
  uint32_t maxRows = TMAX(HJOIN_DEFAULT_BLK_ROWS_NUM, HJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc->totalRowSize);
  if (INT64_MAX != pJoin->ctx.limit && NULL == pJoin->pFinFilter) {
    uint32_t limitMaxRows = pJoin->ctx.limit / HJOIN_BLK_THRESHOLD_RATIO + 1;
    return (maxRows > limitMaxRows) ? limitMaxRows : maxRows;
  }

  return maxRows;
}


int32_t hJoinInitResBlocks(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode) {
  pJoin->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  if (NULL == pJoin->finBlk) {
    QRY_ERR_RET(terrno);
  }

  int32_t code = blockDataEnsureCapacity(pJoin->finBlk, hJoinGetFinBlkCapacity(pJoin, pJoinNode));
  if (TSDB_CODE_SUCCESS != code) {
    QRY_ERR_RET(code);
  }
  
  if (NULL != pJoin->pPreFilter) {
    pJoin->midBlk = NULL;
    code = createOneDataBlock(pJoin->finBlk, false, &pJoin->midBlk);
    if (code) {
      QRY_ERR_RET(code);
    }

    code = blockDataEnsureCapacity(pJoin->midBlk, pJoin->finBlk->info.capacity);
    if (TSDB_CODE_SUCCESS != code) {
      QRY_ERR_RET(code);
    }
  }

  pJoin->blkThreshold = pJoin->finBlk->info.capacity * HJOIN_BLK_THRESHOLD_RATIO;
  return TSDB_CODE_SUCCESS;
}


int32_t createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t             code = TSDB_CODE_SUCCESS;
  SHJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SHJoinOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  pInfo->tblTimeRange.skey = pJoinNode->timeRange.skey;
  pInfo->tblTimeRange.ekey = pJoinNode->timeRange.ekey;
  
  pInfo->ctx.limit = pJoinNode->node.pLimit ? ((SLimitNode*)pJoinNode->node.pLimit)->limit : INT64_MAX;

  setOperatorInfo(pOperator, "HashJoinOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  HJ_ERR_JRET(hJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]));
  HJ_ERR_JRET(hJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]));

  hJoinSetBuildAndProbeTable(pInfo, pJoinNode);
  
  HJ_ERR_JRET(hJoinBuildResColsMap(pInfo, pJoinNode));

  HJ_ERR_JRET(hJoinInitBufPages(pInfo));

  size_t hashCap = pInfo->pBuild->inputStat.inputRowNum > 0 ? (pInfo->pBuild->inputStat.inputRowNum * 1.5) : 1024;
  pInfo->pKeyHash = tSimpleHashInit(hashCap, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pInfo->pKeyHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  HJ_ERR_JRET(hJoinHandleConds(pInfo, pJoinNode));

  HJ_ERR_JRET(hJoinInitResBlocks(pInfo, pJoinNode));

  HJ_ERR_JRET(hJoinSetImplFp(pInfo));

  HJ_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hJoinMainProcess, NULL, destroyHashJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  qDebug("create hash Join operator done");

  *pOptrInfo = pOperator;
  return code;

_return:

  if (pInfo != NULL) {
    destroyHashJoinOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return code;
}


