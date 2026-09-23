/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "functionMgt.h"
#include "hashjoin.h"
#include "joinUtil.h"
#include "operator.h"
#include "os.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttime.h"
#include "ttypes.h"

#if defined(BUILD_TEST)
typedef struct SHJoinTestFailureHooks {
  bool failHashInitOnce;
  bool failPagePushOnce;
  bool failAfterPageGrowthOnce;
} SHJoinTestFailureHooks;

static SHJoinTestFailureHooks gHJoinTestFailureHooks = {0};

void hJoinTestFailHashInitOnce(void) { gHJoinTestFailureHooks.failHashInitOnce = true; }

void hJoinTestFailPagePushOnce(void) { gHJoinTestFailureHooks.failPagePushOnce = true; }

void hJoinTestFailAfterPageGrowthOnce(void) { gHJoinTestFailureHooks.failAfterPageGrowthOnce = true; }

void hJoinTestResetFailureHooks(void) { gHJoinTestFailureHooks = (SHJoinTestFailureHooks){0}; }

static bool hJoinTestTakeFailure(bool* pFailure) {
  bool fail = *pFailure;
  *pFailure = false;
  if (fail) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return fail;
}
#endif

static SSHashObj* hJoinCreateKeyHash(size_t capacity) {
#if defined(BUILD_TEST)
  if (hJoinTestTakeFailure(&gHJoinTestFailureHooks.failHashInitOnce)) {
    return NULL;
  }
#endif
  return tSimpleHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
}

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
          //pJoin->joinFp = hLeftJoinDo; TOOPEN
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
  if (NULL == pPrimIn || NULL == pPrimOut) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (pCtx->tz != NULL) {
    for (int32_t i = startIdx; i <= endIdx; ++i) {
      int64_t ts = ((int64_t*)pPrimIn->pData)[i];
      ((int64_t*)pPrimOut->pData)[i] = taosTimeTruncateIANA(ts, pCtx->truncateUnit,
                                                            pCtx->fdow, pCtx->precision, pCtx->tz);
    }
  } else {
    int64_t factor = TSDB_TICK_PER_SECOND(pCtx->precision);
    int64_t weekShift = (pCtx->truncateUnit == 604800LL * factor) ?
                        ((int64_t)pCtx->fdow - UNIX_EPOCH_WDAY) * 86400LL * factor : 0;
    if (0 != pCtx->timezoneUnit || 0 != weekShift) {
      for (int32_t i = startIdx; i <= endIdx; ++i) {
        int64_t rem = (((int64_t*)pPrimIn->pData)[i] + pCtx->timezoneUnit - weekShift) % pCtx->truncateUnit;
        if (rem < 0) {
          rem += pCtx->truncateUnit;
        }
        ((int64_t*)pPrimOut->pData)[i] = ((int64_t*)pPrimIn->pData)[i] - rem;
      }
    } else {
      for (int32_t i = startIdx; i <= endIdx; ++i) {
        ((int64_t*)pPrimOut->pData)[i] = ((int64_t*)pPrimIn->pData)[i] / pCtx->truncateUnit * pCtx->truncateUnit;
      }
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
    return terrno;
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

  if (pTable->multiGroupJoin) {
    bufSize += sizeof(pTable->baseGId);
  }

  if (pTable->multiGroupJoin || pTable->keyNum > 1) {
    pTable->keyBuf = taosMemoryMalloc(bufSize);
    if (NULL == pTable->keyBuf) {
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void hJoinGetValColsNum(SNodeList* pList, int64_t blkId, int32_t* colNum) {
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

  *pKeyIdx = -1;
  return false;
}

static int32_t hJoinInitValColsInfo(SHJoinTableCtx* pTable, SNodeList* pList) {
  hJoinGetValColsNum(pList, pTable->blkId, &pTable->valNum);
  if (pTable->valNum == 0) {
    return TSDB_CODE_SUCCESS;
  }
  
  pTable->valCols = taosMemoryMalloc(pTable->valNum * sizeof(SHJoinColInfo));
  if (NULL == pTable->valCols) {
    return terrno;
  }

  int32_t i = 0;
  int32_t colNum = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pColNode = (SColumnNode*)pTarget->pExpr;
    if (pColNode->dataBlockId == pTable->blkId) {
      if (!hJoinIsValColInKeyCols(pColNode->slotId, pTable->keyNum, pTable->keyCols, &pTable->valCols[i].keyColIdx)) {
        pTable->valColExist = true;
        colNum++;
      }
      pTable->valCols[i].srcSlot = pColNode->slotId;
      pTable->valCols[i].dstSlot = pTarget->slotId;
      pTable->valCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
      if (pTable->valCols[i].vardata && !IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx)) {
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
      if (!IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx) && !pTable->valCols[i].vardata) {
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
    return terrno;
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

  if (pFunc->pParameterList->length != 7) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  SValueNode* pUnit     = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  SValueNode* pCurrTz   = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
  SValueNode* pTimeZone = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 4);
  if (NULL == pUnit || NULL == pTimeZone) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pCtx->truncateUnit = pUnit->typeData;
  pCtx->precision = pFunc->node.resType.precision;
  {
    SValueNode* pFdow = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 5);
    pCtx->fdow = pFdow ? (int8_t)(pFdow->typeData & 0x07) : 0;
  }
  HJ_ERR_RET(joinResolveTruncateTimezone(pCurrTz, pTimeZone, pCtx->truncateUnit,
                                         pCtx->precision, &pCtx->tz, &pCtx->timezoneUnit));

  pCtx->targetSlotId = pTarget->slotId;

  return TSDB_CODE_SUCCESS;
}


static int32_t hJoinInitTableInfo(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SNodeList* pKeyList = NULL;
  SHJoinTableCtx* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  pTable->multiGroupJoin = pJoin->multiGroupJoin;
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
    return terrno;
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

static void hJoinUpdateHashMemPeak(SHJoinOperatorInfo* pJoin);

static FORCE_INLINE void* hJoinArrayPush(SArray* pArray, const void* pData) {
#if defined(BUILD_TEST)
  if (hJoinTestTakeFailure(&gHJoinTestFailureHooks.failPagePushOnce)) {
    return NULL;
  }
#endif
  return taosArrayPush(pArray, pData);
}

static FORCE_INLINE int32_t hJoinAddPageToBufs(SHJoinOperatorInfo* pJoin) {
  SArray*      pRowBufs = pJoin->pRowBufs;
  SBufPageInfo page;
  page.pageSize = HASH_JOIN_DEFAULT_PAGE_SIZE;
  page.offset = 0;
  page.data = taosMemoryMalloc(page.pageSize);
  if (NULL == page.data) {
    return terrno;
  }

  if (NULL == hJoinArrayPush(pRowBufs, &page)) {
    int32_t code = terrno;
    taosMemoryFree(page.data);
    return code;
  }
  hJoinUpdateHashMemPeak(pJoin);
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinInitBufPages(SHJoinOperatorInfo* pInfo) {
  pInfo->pRowBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pRowBufs) {
    return terrno;
  }

  return hJoinAddPageToBufs(pInfo);
}

static void hJoinFreeTableInfo(SHJoinTableCtx* pTable) {
  if (pTable->primCtx.tz != NULL) {
    tzfree(pTable->primCtx.tz);
    pTable->primCtx.tz = NULL;
  }
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

static int32_t hJoinEnsureLineageArray(SArray** ppLineage) {
  if (*ppLineage == NULL) {
    *ppLineage = taosArrayInit(4, sizeof(int64_t));
    if (*ppLineage == NULL) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void hJoinDecodeLineageSegment(const int64_t* pValue, int32_t* pWinOutIdx, int32_t* pRowStartIdx) {
  const int32_t* pPair = (const int32_t*)pValue;
  *pWinOutIdx = pPair[0];
  *pRowStartIdx = pPair[1];
}

static int32_t hJoinAppendLineageSegment(SArray* pLineage, int32_t winOutIdx, int32_t rowStartIdx) {
  int32_t size = taosArrayGetSize(pLineage);
  if (size > 0) {
    int64_t* pLast = taosArrayGet(pLineage, size - 1);
    if (pLast == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    int32_t lastWinOutIdx = 0;
    int32_t lastRowStartIdx = 0;
    hJoinDecodeLineageSegment(pLast, &lastWinOutIdx, &lastRowStartIdx);
    if (lastWinOutIdx == winOutIdx) {
      return TSDB_CODE_SUCCESS;
    }
    if (rowStartIdx <= lastRowStartIdx) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }

  int64_t  value = 0;
  int32_t* pPair = (int32_t*)&value;
  pPair[0] = winOutIdx;
  pPair[1] = rowStartIdx;
  return taosArrayPush(pLineage, &value) == NULL ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t hJoinValidateLineage(const SArray* pLineage, int32_t rows) {
  if (rows < 0 || (rows == 0 && pLineage != NULL && taosArrayGetSize(pLineage) != 0) ||
      (rows > 0 && (pLineage == NULL || taosArrayGetSize(pLineage) == 0))) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t previousStart = -1;
  int32_t size = taosArrayGetSize(pLineage);
  for (int32_t i = 0; i < size; ++i) {
    int64_t* pValue = taosArrayGet(pLineage, i);
    if (pValue == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    int32_t winOutIdx = 0;
    int32_t rowStartIdx = 0;
    hJoinDecodeLineageSegment(pValue, &winOutIdx, &rowStartIdx);
    if ((i == 0 && rowStartIdx != 0) || rowStartIdx <= previousStart || rowStartIdx >= rows) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    previousStart = rowStartIdx;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinSnapshotProbeLineage(SHJoinOperatorInfo* pJoin, const SArray* pChildLineage, int32_t rows) {
  HJ_ERR_RET(hJoinValidateLineage(pChildLineage, rows));
  HJ_ERR_RET(hJoinEnsureLineageArray(&pJoin->pProbeWinIdx));
  taosArrayClear(pJoin->pProbeWinIdx);
  int32_t size = taosArrayGetSize(pChildLineage);
  if (size > 0 && taosArrayAddBatch(pJoin->pProbeWinIdx, TARRAY_DATA(pChildLineage), size) == NULL) {
    taosArrayClear(pJoin->pProbeWinIdx);
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinGetProbeWinOutIdx(const SHJoinOperatorInfo* pJoin, int32_t probeRow, int32_t* pWinOutIdx) {
  if (pJoin->pProbeWinIdx == NULL || pJoin->ctx.pProbeData == NULL || probeRow < 0 ||
      probeRow >= pJoin->ctx.pProbeData->info.rows) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t segmentCount = taosArrayGetSize(pJoin->pProbeWinIdx);
  for (int32_t i = segmentCount - 1; i >= 0; --i) {
    int64_t* pValue = taosArrayGet(pJoin->pProbeWinIdx, i);
    int32_t  winOutIdx = 0;
    int32_t  rowStartIdx = 0;
    hJoinDecodeLineageSegment(pValue, &winOutIdx, &rowStartIdx);
    if (rowStartIdx <= probeRow) {
      *pWinOutIdx = winOutIdx;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
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
        if (IS_HASH_JOIN_KEY_COL(pBuild->valCols[buildIdx].keyColIdx)) {
          int32_t bufOffset = pProbe->keyCols[pBuild->valCols[buildIdx].keyColIdx].bufOffset;
          code = colDataSetVal(pDst, pRes->info.rows + r, pKeyData + bufOffset, false);
          if (code) {
            return code;
          }
        } else {
          if (BMIsNull(pData, buildValIdx)) {
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

        if (colDataIsNull_s(pSrc, pJoin->ctx.probeStartIdx)) {
          code = colDataCopyNItems(pDst, pRes->info.rows, NULL, rowNum, true);
        } else {
          code = colDataCopyNItems(pDst, pRes->info.rows, colDataGetData(pSrc, pJoin->ctx.probeStartIdx), rowNum, false);
        }
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

int32_t hJoinAppendResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched) {
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

  if (rowNum > 0 && pJoin->pProbeWinIdx != NULL) {
    int32_t winOutIdx = 0;
    HJ_ERR_RET(hJoinGetProbeWinOutIdx(pJoin, pCtx->probeStartIdx, &winOutIdx));
    HJ_ERR_RET(hJoinEnsureLineageArray(&pJoin->pOutputWinIdx));
    HJ_ERR_RET(hJoinAppendLineageSegment(pJoin->pOutputWinIdx, winOutIdx, pRes->info.rows));
  }

  int32_t code = hJoinCopyResRowsToBlock(pJoin, rowNum, pStart, pRes);
  if (code) {
    return code;
  }

  pJoin->execInfo.resRows += rowNum;
  pRes->info.rows = resNum;
  *allFetched = pCtx->pBuildRow ? false : true;
  return TSDB_CODE_SUCCESS;
}

bool hJoinCopyKeyColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx, size_t* pBufLen) {
  char*  pData = NULL;
  size_t bufLen = 0;

  if (pTable->multiGroupJoin) {
    TAOS_MEMCPY(pTable->keyBuf, &pTable->baseGId, sizeof(pTable->baseGId));
    bufLen = sizeof(pTable->baseGId);
    for (int32_t i = 0; i < pTable->keyNum; ++i) {
      if (colDataIsNull_s(pTable->keyCols[i].colData, rowIdx)) {
        return true;
      }

      pTable->keyCols[i].bufOffset = bufLen;
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
  } else if (1 == pTable->keyNum) {
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
      
      pTable->keyCols[i].bufOffset = bufLen;
      
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
    if (IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx)) {
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
    if (IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx)) {
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
      if (BMIsNull(pTable->valCols[i].bitMap, rowIdx)) {
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

static FORCE_INLINE int32_t hJoinGetValBufFromPages(SHJoinOperatorInfo* pJoin, int32_t bufSize, char** pBuf,
                                                    SBufRowInfo* pRow) {
  SArray* pPages = pJoin->pRowBufs;
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

    int32_t code = hJoinAddPageToBufs(pJoin);
    if (code) {
      return code;
    }
#if defined(BUILD_TEST)
    if (hJoinTestTakeFailure(&gHJoinTestFailureHooks.failAfterPageGrowthOnce)) {
      return terrno;
    }
#endif
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
    if (-1 == pTable->valCols[*varColIdx].offset[rowIdx]) {
      continue;
    }
    char* pData = pTable->valCols[*varColIdx].data + pTable->valCols[*varColIdx].offset[rowIdx];
    bufLen += varDataTLen(pData);
  }

  return bufLen;
}

static void hJoinUpdateHashMemPeak(SHJoinOperatorInfo* pJoin) {
  uint64_t pages =
      pJoin->pRowBufs == NULL ? 0 : (uint64_t)taosArrayGetSize(pJoin->pRowBufs) * HASH_JOIN_DEFAULT_PAGE_SIZE;
  uint64_t bytes = pages + pJoin->hashOwnedBytes + tSimpleHashGetMemSize(pJoin->pKeyHash);
  if (bytes > pJoin->execInfo.hashMemPeakBytes) {
    pJoin->execInfo.hashMemPeakBytes = bytes;
  }
}

static int32_t hJoinAddRowToHashImpl(SHJoinOperatorInfo* pJoin, SGroupData* pGroup, SHJoinTableCtx* pTable,
                                     size_t keyLen, int32_t rowIdx) {
  SGroupData   group = {0};
  SBufRowInfo* pRow = NULL;

  if (NULL == pGroup) {
    group.rows = taosMemoryMalloc(sizeof(SBufRowInfo));
    if (NULL == group.rows) {
      return terrno;
    }
    pRow = group.rows;
  } else {
    pRow = taosMemoryMalloc(sizeof(SBufRowInfo));
    if (NULL == pRow) {
      return terrno;
    }
  }

  int32_t code = hJoinGetValBufFromPages(pJoin, hJoinGetValBufSize(pTable, rowIdx), &pTable->valData, pRow);
  if (code) {
    taosMemoryFree(pRow);
    return code;
  }

  if (NULL == pGroup) {
    pRow->next = NULL;
    code = tSimpleHashPut(pJoin->pKeyHash, pTable->keyData, keyLen, &group, sizeof(group));
    if (code) {
      hJoinUpdateHashMemPeak(pJoin);
      taosMemoryFree(pRow);
      return code;
    }
    pJoin->hashOwnedBytes += keyLen + sizeof(group);
  } else {
    pRow->next = pGroup->rows;
    pGroup->rows = pRow;
  }

  pJoin->hashOwnedBytes += sizeof(*pRow);
  hJoinUpdateHashMemPeak(pJoin);

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

  TSKEY* pStart =
      (TSKEY*)taosbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
  *startIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
  *endIdx = pBlock->info.rows - 1;

  return true;
}

static int32_t hJoinBindLegacyMultiGroupBlock(SHJoinOperatorInfo* pJoin, SHJoinTableCtx* pTable,
                                              const SSDataBlock* pBlock) {
  if (!pJoin->multiGroupJoin || pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  const SBlockID* pId = &pBlock->info.id;
  if (pId->baseGId == 0 || (pId->groupId != 0 && pId->groupId != pId->baseGId)) {
    qError("invalid multi-group hash join block identity, uid:%" PRIu64 ", blockId:%" PRId64 ", groupId:%" PRIu64
           ", baseGId:%" PRIu64,
           pId->uid, pId->blockId, pId->groupId, pId->baseGId);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pTable->baseGId = pId->baseGId;
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinBindMultiGroupBlock(SHJoinTableCtx* pTable, const SSDataBlock* pBlock, uint64_t expectedGid) {
  const SBlockID* pId = &pBlock->info.id;
  if (expectedGid == 0 || pId->baseGId != expectedGid || pId->groupId != expectedGid) {
    qError("invalid ordered multi-group hash join block identity, expected:%" PRIu64 ", uid:%" PRIu64
           ", blockId:%" PRId64 ", groupId:%" PRIu64 ", baseGId:%" PRIu64,
           expectedGid, pId->uid, pId->blockId, pId->groupId, pId->baseGId);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pTable->baseGId = expectedGid;
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinAddBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin) {
  if (pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SHJoinTableCtx* pBuild = pJoin->pBuild;
  int32_t         startIdx = 0, endIdx = pBlock->info.rows - 1;
  if (pBuild->hasTimeRange &&
      !hJoinFilterTimeRange(pBlock, &pJoin->tblTimeRange, pBuild->primCol->srcSlot, &startIdx, &endIdx)) {
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
  SSDataBlock*        pBlock = NULL;
  int32_t             code = TSDB_CODE_SUCCESS;

  while (true) {
    pBlock = getNextBlockFromDownstream(pOperator, pJoin->pBuild->downStreamIdx);
    if (NULL == pBlock) {
      break;
    }

    code = hJoinBindLegacyMultiGroupBlock(pJoin, pJoin->pBuild, pBlock);
    if (code) {
      return code;
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

static void hJoinReleaseHashMemory(SHJoinOperatorInfo* pJoin) {
  hJoinDestroyKeyHash(&pJoin->pKeyHash);
  taosArrayDestroyEx(pJoin->pRowBufs, hJoinFreeBufPage);
  pJoin->pRowBufs = NULL;
  pJoin->hashOwnedBytes = 0;
}

static int32_t hJoinInitCurrentGroupHash(SHJoinOperatorInfo* pJoin) {
  if (pJoin->pKeyHash != NULL || pJoin->pRowBufs != NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pJoin->pKeyHash = hJoinCreateKeyHash(0);
  if (pJoin->pKeyHash == NULL) {
    return terrno;
  }
  hJoinUpdateHashMemPeak(pJoin);

  int32_t code = hJoinInitBufPages(pJoin);
  if (code != TSDB_CODE_SUCCESS) {
    hJoinReleaseHashMemory(pJoin);
  }
  return code;
}

static int32_t hJoinBuildCurrentGroup(SOperatorInfo* pOperator, bool* pBuildComplete) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  *pBuildComplete = false;
  while (!pJoin->buildEogSeen) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, pJoin->pBuild->downStreamIdx);
    if (pBlock == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    HJ_ERR_RET(hJoinBindMultiGroupBlock(pJoin->pBuild, pBlock, pJoin->currentBaseGId));
    pJoin->execInfo.buildBlkNum++;
    pJoin->execInfo.buildBlkRows += pBlock->info.rows;
    if (qIsMultiGroupEogBlock(pBlock)) {
      pJoin->buildEogSeen = true;
      *pBuildComplete = true;
      return TSDB_CODE_SUCCESS;
    }
    if (pBlock->info.rows <= 0) {
      continue;
    }

    if (pJoin->pKeyHash == NULL) {
      HJ_ERR_RET(hJoinInitCurrentGroupHash(pJoin));
    }
    HJ_ERR_RET(hJoinAddBlockRowsToHash(pBlock, pJoin));
  }

  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
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

static bool hJoinBlockIdEqual(const SBlockID* pLeft, const SBlockID* pRight) {
  return pLeft->uid == pRight->uid && pLeft->blockId == pRight->blockId && pLeft->groupId == pRight->groupId &&
         pLeft->baseGId == pRight->baseGId;
}

static bool hJoinHasCurrentGroupState(const SHJoinOperatorInfo* pJoin) {
  return pJoin->finBlk->info.rows > 0 || (pJoin->midBlk != NULL && pJoin->midBlk->info.rows > 0) ||
         pJoin->ctx.rowRemains || pJoin->ctx.midRemains || pJoin->ctx.pBuildRow != NULL || pJoin->pPendingProbe != NULL;
}

static int32_t hJoinReplaceLineage(SArray* pDest, SArray* pSource) {
  taosArrayClear(pDest);
  int32_t size = taosArrayGetSize(pSource);
  if (size > 0 && taosArrayAddBatch(pDest, TARRAY_DATA(pSource), size) == NULL) {
    taosArrayClear(pDest);
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinRebuildLineageByFilter(SHJoinOperatorInfo* pJoin, int32_t rowsBefore,
                                           const SColumnInfoData* pFilterResult) {
  if (pJoin->pOutputWinIdx == NULL || pFilterResult == NULL || rowsBefore <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  HJ_ERR_RET(hJoinValidateLineage(pJoin->pOutputWinIdx, rowsBefore));

  SArray* pRebuilt = taosArrayInit(taosArrayGetSize(pJoin->pOutputWinIdx), sizeof(int64_t));
  if (pRebuilt == NULL) {
    return terrno;
  }

  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       outputRow = 0;
  const int8_t* pIndicator = (const int8_t*)pFilterResult->pData;
  int32_t       segmentCount = taosArrayGetSize(pJoin->pOutputWinIdx);
  for (int32_t i = 0; i < segmentCount && code == TSDB_CODE_SUCCESS; ++i) {
    int64_t* pCurrent = taosArrayGet(pJoin->pOutputWinIdx, i);
    int32_t  winOutIdx = 0;
    int32_t  rowStartIdx = 0;
    hJoinDecodeLineageSegment(pCurrent, &winOutIdx, &rowStartIdx);
    int32_t rowEndIdx = rowsBefore;
    if (i + 1 < segmentCount) {
      int64_t* pNext = taosArrayGet(pJoin->pOutputWinIdx, i + 1);
      int32_t  nextWinOutIdx = 0;
      hJoinDecodeLineageSegment(pNext, &nextWinOutIdx, &rowEndIdx);
    }
    for (int32_t row = rowStartIdx; row < rowEndIdx; ++row) {
      if (pIndicator[row]) {
        code = hJoinAppendLineageSegment(pRebuilt, winOutIdx, outputRow++);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = hJoinReplaceLineage(pJoin->pOutputWinIdx, pRebuilt);
  }
  taosArrayDestroy(pRebuilt);
  return code;
}

static int32_t hJoinPublishLineage(SOperatorInfo* pOperator, int32_t rows) {
  SHJoinOperatorInfo*     pJoin = pOperator->info;
  SStreamRuntimeFuncInfo* pRuntime = GET_STM_RTINFO(pOperator->pTaskInfo);
  if (pRuntime == NULL || pJoin->pOutputWinIdx == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  HJ_ERR_RET(hJoinValidateLineage(pJoin->pOutputWinIdx, rows));
  pRuntime->pStreamBlkWinIdx = pJoin->pOutputWinIdx;
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinFinalizeMultiGroupBlock(SOperatorInfo* pOperator, bool* pHasOutput) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SColumnInfoData*    pFilterResult = NULL;
  int32_t             code = TSDB_CODE_SUCCESS;
  *pHasOutput = false;
  if (pJoin->finBlk->info.rows > 0 && pJoin->pFinFilter != NULL) {
    int32_t rowsBefore = pJoin->finBlk->info.rows;
    code = doFilter(pJoin->finBlk, pJoin->pFinFilter, NULL, &pFilterResult);
    if (code == TSDB_CODE_SUCCESS && pJoin->finBlk->info.rows < rowsBefore) {
      code = hJoinRebuildLineageByFilter(pJoin, rowsBefore, pFilterResult);
    }
    colDataDestroy(pFilterResult);
    taosMemoryFree(pFilterResult);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  if (pJoin->finBlk->info.rows > 0) {
    if (!pJoin->probeIdSet || pJoin->probeId.baseGId != pJoin->currentBaseGId) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pJoin->finBlk->info.id = pJoin->probeId;
    pJoin->finBlk->info.dataLoad = 1;
    HJ_ERR_RET(hJoinPublishLineage(pOperator, pJoin->finBlk->info.rows));
    *pHasOutput = true;
  } else if (pJoin->pOutputWinIdx != NULL) {
    taosArrayClear(pJoin->pOutputWinIdx);
  }
  return TSDB_CODE_SUCCESS;
}

static void hJoinReleaseCompletedPendingProbe(SHJoinOperatorInfo* pJoin) {
  if (pJoin->pPendingProbe != NULL && pJoin->ctx.pProbeData == pJoin->pPendingProbe && !pJoin->ctx.rowRemains &&
      pJoin->ctx.pBuildRow == NULL) {
    blockDataDestroy(pJoin->pPendingProbe);
    pJoin->pPendingProbe = NULL;
    pJoin->ctx.pProbeData = NULL;
  }
}

static int32_t hJoinDrainMultiGroupRemains(SOperatorInfo* pOperator, bool* pHasOutput) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  *pHasOutput = false;
  while (pJoin->ctx.rowRemains || pJoin->ctx.pBuildRow != NULL) {
    HJ_ERR_RET((*pJoin->joinFp)(pOperator));
    if (!hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      continue;
    }

    HJ_ERR_RET(hJoinFinalizeMultiGroupBlock(pOperator, pHasOutput));
    if (*pHasOutput) {
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinFinishCurrentGroup(SHJoinOperatorInfo* pJoin) {
  if (!pJoin->buildEogSeen || !pJoin->probeEogSeen || hJoinHasCurrentGroupState(pJoin)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  HJ_ERR_RET(hJoinEnsureLineageArray(&pJoin->pOutputWinIdx));
  HJ_ERR_RET(qSetMultiGroupEogBlock(pJoin->finBlk, pJoin->currentBaseGId));
  taosArrayClear(pJoin->pOutputWinIdx);
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinValidateChildrenExhausted(SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  if (getNextBlockFromDownstream(pOperator, pJoin->pBuild->downStreamIdx) != NULL ||
      getNextBlockFromDownstream(pOperator, pJoin->pProbe->downStreamIdx) != NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static void hJoinReleaseCurrentGroup(SHJoinOperatorInfo* pJoin) {
  int64_t limit = pJoin->ctx.limit;
  hJoinReleaseHashMemory(pJoin);
  blockDataDestroy(pJoin->pPendingProbe);
  pJoin->pPendingProbe = NULL;
  pJoin->ctx = (SHJoinCtx){0};
  pJoin->ctx.limit = limit;
  blockDataCleanup(pJoin->midBlk);
  taosArrayDestroy(pJoin->pProbeWinIdx);
  pJoin->pProbeWinIdx = NULL;
  if (pJoin->pOutputWinIdx != NULL) {
    taosArrayClear(pJoin->pOutputWinIdx);
  }
  pJoin->keyHashBuilt = false;
  pJoin->probeId = (SBlockID){0};
  pJoin->probeIdSet = false;
  pJoin->pBuild->baseGId = 0;
  pJoin->pProbe->baseGId = 0;
  pJoin->currentBaseGId = 0;
  pJoin->buildEogSeen = false;
  pJoin->probeEogSeen = false;
}

static int32_t hJoinResolveCurrentGroup(SOperatorInfo* pOperator) {
  SHJoinOperatorInfo*     pJoin = pOperator->info;
  SStreamRuntimeFuncInfo* pRuntime = GET_STM_RTINFO(pOperator->pTaskInfo);
  if (pRuntime == NULL || !pRuntime->isMultiGroupCalc || pRuntime->pGroupCalcOrder == NULL ||
      taosArrayGetSize(pRuntime->pGroupCalcOrder) <= 0 || pJoin->groupOrderIdx < 0 ||
      pJoin->groupOrderIdx >= taosArrayGetSize(pRuntime->pGroupCalcOrder)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  uint64_t* pGid = taosArrayGet(pRuntime->pGroupCalcOrder, pJoin->groupOrderIdx);
  if (pGid == NULL || *pGid == 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  pJoin->currentBaseGId = *pGid;
  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinMultiGroupMainProcess(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*        pRes = pJoin->finBlk;
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  bool                hasOutput = false;

  QRY_PARAM_CHECK(pResBlock);
  *pResBlock = NULL;
  if (pJoin->multiGroupPhase == HJOIN_MULTI_GROUP_DONE || pOperator->status == OP_EXEC_DONE) {
    pRes->info.rows = 0;
    return TSDB_CODE_SUCCESS;
  }

  SStreamRuntimeFuncInfo* pRuntime = GET_STM_RTINFO(pOperator->pTaskInfo);
  if (pRuntime == NULL || !pRuntime->isMultiGroupCalc || pRuntime->pGroupCalcOrder == NULL ||
      taosArrayGetSize(pRuntime->pGroupCalcOrder) <= 0) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  blockDataCleanup(pRes);
  if (pJoin->pOutputWinIdx != NULL) {
    taosArrayClear(pJoin->pOutputWinIdx);
  }
  code = hJoinDrainMultiGroupRemains(pOperator, &hasOutput);
  QUERY_CHECK_CODE(code, lino, _end);
  hJoinReleaseCompletedPendingProbe(pJoin);
  if (hasOutput) {
    *pResBlock = pRes;
    return TSDB_CODE_SUCCESS;
  }

  while (true) {
    switch (pJoin->multiGroupPhase) {
      case HJOIN_MULTI_GROUP_BUILD: {
        if (pJoin->currentBaseGId == 0) {
          code = hJoinResolveCurrentGroup(pOperator);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        bool buildComplete = false;
        code = hJoinBuildCurrentGroup(pOperator, &buildComplete);
        QUERY_CHECK_CODE(code, lino, _end);
        if (!buildComplete) {
          code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        pJoin->multiGroupPhase = HJOIN_MULTI_GROUP_PROBE;
        continue;
      }
      case HJOIN_MULTI_GROUP_PROBE: {
        bool         pending = pJoin->pPendingProbe != NULL;
        SSDataBlock* pBlock =
            pending ? pJoin->pPendingProbe : getNextBlockFromDownstream(pOperator, pJoin->pProbe->downStreamIdx);
        if (pBlock == NULL) {
          code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }

        if (!pending) {
          pJoin->execInfo.probeBlkNum++;
          pJoin->execInfo.probeBlkRows += pBlock->info.rows;
        }
        code = hJoinBindMultiGroupBlock(pJoin->pProbe, pBlock, pJoin->currentBaseGId);
        QUERY_CHECK_CODE(code, lino, _end);
        if (qIsMultiGroupEogBlock(pBlock)) {
          if (pending || pJoin->probeEogSeen) {
            code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          pJoin->probeEogSeen = true;
          pJoin->multiGroupPhase = HJOIN_MULTI_GROUP_EMIT_EOG;
          code = hJoinFinalizeMultiGroupBlock(pOperator, &hasOutput);
          QUERY_CHECK_CODE(code, lino, _end);
          if (hasOutput) {
            *pResBlock = pRes;
            return TSDB_CODE_SUCCESS;
          }
          continue;
        }
        if (pBlock->info.rows <= 0) {
          if (pending) {
            blockDataDestroy(pJoin->pPendingProbe);
            pJoin->pPendingProbe = NULL;
          }
          continue;
        }

        if (!pending) {
          code = hJoinSnapshotProbeLineage(pJoin, pRuntime->pStreamBlkWinIdx, pBlock->info.rows);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        if (pJoin->probeIdSet && !hJoinBlockIdEqual(&pJoin->probeId, &pBlock->info.id) && pRes->info.rows > 0) {
          if (pending) {
            code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = blockDataExtractBlock(pBlock, 0, pBlock->info.rows, &pJoin->pPendingProbe);
          QUERY_CHECK_CODE(code, lino, _end);
          code = hJoinFinalizeMultiGroupBlock(pOperator, &hasOutput);
          QUERY_CHECK_CODE(code, lino, _end);
          if (hasOutput) {
            *pResBlock = pRes;
            return TSDB_CODE_SUCCESS;
          }
          continue;
        }

        pJoin->probeId = pBlock->info.id;
        pJoin->probeIdSet = true;
        if (pJoin->pKeyHash == NULL) {
          if (pending) {
            blockDataDestroy(pJoin->pPendingProbe);
            pJoin->pPendingProbe = NULL;
          }
          continue;
        }

        code = hJoinPrepareStart(pOperator, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        if (hJoinBlkReachThreshold(pJoin, pRes->info.rows)) {
          code = hJoinFinalizeMultiGroupBlock(pOperator, &hasOutput);
          QUERY_CHECK_CODE(code, lino, _end);
          if (hasOutput) {
            hJoinReleaseCompletedPendingProbe(pJoin);
            *pResBlock = pRes;
            return TSDB_CODE_SUCCESS;
          }
        }

        code = hJoinDrainMultiGroupRemains(pOperator, &hasOutput);
        QUERY_CHECK_CODE(code, lino, _end);
        hJoinReleaseCompletedPendingProbe(pJoin);
        if (hasOutput) {
          *pResBlock = pRes;
          return TSDB_CODE_SUCCESS;
        }
        continue;
      }
      case HJOIN_MULTI_GROUP_EMIT_EOG: {
        bool finalGroup = pJoin->groupOrderIdx + 1 >= taosArrayGetSize(pRuntime->pGroupCalcOrder);
        if (finalGroup) {
          code = hJoinValidateChildrenExhausted(pOperator);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        code = hJoinFinishCurrentGroup(pJoin);
        QUERY_CHECK_CODE(code, lino, _end);
        hJoinReleaseCurrentGroup(pJoin);
        ++pJoin->groupOrderIdx;
        if (finalGroup) {
          pJoin->multiGroupPhase = HJOIN_MULTI_GROUP_DONE;
          setOperatorCompleted(pOperator);
        } else {
          pJoin->multiGroupPhase = HJOIN_MULTI_GROUP_BUILD;
        }
        code = hJoinPublishLineage(pOperator, 0);
        QUERY_CHECK_CODE(code, lino, _end);
        *pResBlock = pRes;
        return TSDB_CODE_SUCCESS;
      }
      case HJOIN_MULTI_GROUP_DONE:
        return TSDB_CODE_SUCCESS;
      default:
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t hJoinSingleGroupMainProcess(struct SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SSDataBlock*        pRes = pJoin->finBlk;

  QRY_PARAM_CHECK(pResBlock);

  if (pOperator->status == OP_EXEC_DONE) {
    pRes->info.rows = 0;
    goto _end;
  }

  if (!pJoin->keyHashBuilt) {
    pJoin->keyHashBuilt = true;

    bool queryDone = false;
    code = hJoinBuildHash(pOperator, &queryDone);
    QUERY_CHECK_CODE(code, lino, _end);

    if (queryDone) {
      goto _end;
    }
  }

  blockDataCleanup(pRes);

  if (pJoin->ctx.rowRemains) {
    code = (*pJoin->joinFp)(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
      code = doFilter(pRes, pJoin->pFinFilter, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
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
    QUERY_CHECK_CODE(code, lino, _end);

    if (!hJoinBlkReachThreshold(pJoin, pRes->info.rows)) {
      continue;
    }

    if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
      code = doFilter(pRes, pJoin->pFinFilter, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (pRes->info.rows > 0) {
    *pResBlock = pRes;
    qDebug("%s %s output %" PRId64 " rows final res", GET_TASKID(pTaskInfo), __func__, pRes->info.rows);
  }

  return code;
}

static int32_t hJoinMainProcess(struct SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SStreamRuntimeFuncInfo* pRuntime = pOperator->pTaskInfo == NULL ? NULL : GET_STM_RTINFO(pOperator->pTaskInfo);
  bool                    runtimeMultiGroup = pRuntime != NULL && pRuntime->isMultiGroupCalc;
  if (pJoin->multiGroupJoin != runtimeMultiGroup) {
    pOperator->pTaskInfo->code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pJoin->multiGroupJoin && runtimeMultiGroup) {
    return hJoinMultiGroupMainProcess(pOperator, pResBlock);
  }
  return hJoinSingleGroupMainProcess(pOperator, pResBlock);
}

static void destroyHashJoinOperator(void* param) {
  SHJoinOperatorInfo* pJoinOperator = (SHJoinOperatorInfo*)param;
  qDebug("hashJoin exec info, buildBlk:%" PRId64 ", buildRows:%" PRId64 ", probeBlk:%" PRId64 ", probeRows:%" PRId64
         ", resRows:%" PRId64 ", hashMemPeakBytes:%" PRIu64,
         pJoinOperator->execInfo.buildBlkNum, pJoinOperator->execInfo.buildBlkRows, pJoinOperator->execInfo.probeBlkNum,
         pJoinOperator->execInfo.probeBlkRows, pJoinOperator->execInfo.resRows,
         pJoinOperator->execInfo.hashMemPeakBytes);

  hJoinReleaseHashMemory(pJoinOperator);

  hJoinFreeTableInfo(&pJoinOperator->tbs[0]);
  hJoinFreeTableInfo(&pJoinOperator->tbs[1]);
  filterFreeInfo(pJoinOperator->pPreFilter);
  pJoinOperator->pPreFilter = NULL;
  filterFreeInfo(pJoinOperator->pFinFilter);
  pJoinOperator->pFinFilter = NULL;
  blockDataDestroy(pJoinOperator->pPendingProbe);
  pJoinOperator->pPendingProbe = NULL;
  blockDataDestroy(pJoinOperator->midBlk);
  pJoinOperator->midBlk = NULL;
  blockDataDestroy(pJoinOperator->finBlk);
  pJoinOperator->finBlk = NULL;
  taosMemoryFreeClear(pJoinOperator->pResColMap);
  taosArrayDestroy(pJoinOperator->pProbeWinIdx);
  pJoinOperator->pProbeWinIdx = NULL;
  taosArrayDestroy(pJoinOperator->pOutputWinIdx);
  pJoinOperator->pOutputWinIdx = NULL;

  taosMemoryFreeClear(param);
}

int32_t hJoinHandleConds(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
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

      HJ_ERR_RET(filterInitFromNode(pCond, &pJoin->pFinFilter, 0, pTaskInfo->pStreamRuntimeInfo));
      filterSetExecContext(pJoin->pFinFilter, pTaskInfo, isTaskKilled);
      break;
    }
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT:
    case JOIN_TYPE_FULL:
      if (pJoinNode->pFullOnCond != NULL) {
        HJ_ERR_RET(filterInitFromNode(pJoinNode->pFullOnCond, &pJoin->pPreFilter, 0,
                                      pTaskInfo->pStreamRuntimeInfo));
        filterSetExecContext(pJoin->pPreFilter, pTaskInfo, isTaskKilled);
      }
      if (pJoinNode->node.pConditions != NULL) {
        HJ_ERR_RET(filterInitFromNode(pJoinNode->node.pConditions, &pJoin->pFinFilter, 0,
                                      pTaskInfo->pStreamRuntimeInfo));
        filterSetExecContext(pJoin->pFinFilter, pTaskInfo, isTaskKilled);
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

static int32_t resetHashJoinOperState(SOperatorInfo* pOper) {
  SHJoinOperatorInfo* pHjOper = pOper->info;
  SStreamRuntimeFuncInfo* pRuntime = GET_STM_RTINFO(pOper->pTaskInfo);
  int64_t                 limit = pHjOper->ctx.limit;
  pHjOper->ctx = (SHJoinCtx){0};
  pHjOper->ctx.limit = limit;
  pHjOper->keyHashBuilt = false;
  pOper->status = OP_EXEC_DONE;
  blockDataDestroy(pHjOper->pPendingProbe);
  pHjOper->pPendingProbe = NULL;
  blockDataCleanup(pHjOper->midBlk);
  blockDataCleanup(pHjOper->finBlk);
  if (pHjOper->midBlk != NULL) {
    pHjOper->midBlk->info.id = (SBlockID){0};
  }
  pHjOper->finBlk->info.id = (SBlockID){0};

  pHjOper->execInfo = (SHJoinExecInfo){0};
  pHjOper->hashOwnedBytes = 0;
  pHjOper->probeId = (SBlockID){0};
  pHjOper->probeIdSet = false;
  pHjOper->pBuild->baseGId = 0;
  pHjOper->pProbe->baseGId = 0;
  pHjOper->multiGroupPhase = HJOIN_MULTI_GROUP_BUILD;
  pHjOper->groupOrderIdx = 0;
  pHjOper->currentBaseGId = 0;
  pHjOper->buildEogSeen = false;
  pHjOper->probeEogSeen = false;

  if (pRuntime != NULL &&
      (pRuntime->pStreamBlkWinIdx == pHjOper->pProbeWinIdx || pRuntime->pStreamBlkWinIdx == pHjOper->pOutputWinIdx)) {
    pRuntime->pStreamBlkWinIdx = NULL;
  }
  taosArrayDestroy(pHjOper->pProbeWinIdx);
  pHjOper->pProbeWinIdx = NULL;
  taosArrayDestroy(pHjOper->pOutputWinIdx);
  pHjOper->pOutputWinIdx = NULL;

  hJoinReleaseHashMemory(pHjOper);

  if (pHjOper->multiGroupJoin) {
    pOper->status = OP_NOT_OPENED;
    return TSDB_CODE_SUCCESS;
  }

  size_t hashCap = pHjOper->pBuild->inputStat.inputRowNum > 0 ? (pHjOper->pBuild->inputStat.inputRowNum * 1.5) : 1024;
  pHjOper->pKeyHash = hJoinCreateKeyHash(hashCap);
  if (pHjOper->pKeyHash == NULL) {
    return terrno;
  }
  hJoinUpdateHashMemPeak(pHjOper);

  int32_t code = hJoinInitBufPages(pHjOper);
  if (code == TSDB_CODE_SUCCESS) {
    pOper->status = OP_NOT_OPENED;
  }
  return code;
}

int32_t createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t             code = TSDB_CODE_SUCCESS;
  SHJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SHJoinOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _return;
  }
  initOperatorCostInfo(pOperator);

  pInfo->tblTimeRange.skey = pJoinNode->timeRange.skey;
  pInfo->tblTimeRange.ekey = pJoinNode->timeRange.ekey;
  pInfo->multiGroupJoin = pJoinNode->multiGroupJoin;
  pInfo->multiGroupPhase = HJOIN_MULTI_GROUP_BUILD;

  pInfo->ctx.limit = (pJoinNode->node.pLimit && ((SLimitNode*)pJoinNode->node.pLimit)->limit)
                         ? ((SLimitNode*)pJoinNode->node.pLimit)->limit->datum.i
                         : INT64_MAX;

  setOperatorInfo(pOperator, "HashJoinOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  HJ_ERR_JRET(hJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]));
  HJ_ERR_JRET(hJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]));

  hJoinSetBuildAndProbeTable(pInfo, pJoinNode);

  HJ_ERR_JRET(hJoinInitPrimExprCtx(pInfo->pBuild->primExpr, &pInfo->pBuild->primCtx, pInfo->pBuild));
  HJ_ERR_JRET(hJoinInitPrimExprCtx(pInfo->pProbe->primExpr, &pInfo->pProbe->primCtx, pInfo->pProbe));

  HJ_ERR_JRET(hJoinBuildResColsMap(pInfo, pJoinNode));

  if (!pInfo->multiGroupJoin) {
    HJ_ERR_JRET(hJoinInitBufPages(pInfo));

    size_t hashCap = pInfo->pBuild->inputStat.inputRowNum > 0 ? (pInfo->pBuild->inputStat.inputRowNum * 1.5) : 1024;
    pInfo->pKeyHash = hJoinCreateKeyHash(hashCap);
    if (pInfo->pKeyHash == NULL) {
      code = terrno;
      goto _return;
    }
    hJoinUpdateHashMemPeak(pInfo);
  }

  HJ_ERR_JRET(hJoinHandleConds(pInfo, pJoinNode, pTaskInfo));

  HJ_ERR_JRET(hJoinInitResBlocks(pInfo, pJoinNode));

  HJ_ERR_JRET(hJoinSetImplFp(pInfo));

  HJ_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hJoinMainProcess, NULL, destroyHashJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetHashJoinOperState);

  qDebug("create hash Join operator done");

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_return:

  if (pInfo != NULL) {
    destroyHashJoinOperator(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  pTaskInfo->code = code;
  return code;
}
